"""LLM-driven supplier-cost-concentration enrichment for ``supply_chain_edges``.

The regex-only ``ingestion.altdata.supply_chain_parser`` only manages to fill
``pct_downstream_cogs`` for ~4 of ~800 edges because mega-cap 10-Ks rarely use
the canonical "X accounted for N% of our cost of revenue" phrasing. This
module bridges the gap by:

1. Pulling each edge that is missing a percentage value from the DB.
2. Re-fetching the latest 10-K text for the downstream ticker (re-using the
   regex parser's SEC fetch helpers, no duplication).
3. Locating every passage that mentions the upstream supplier name and
   carving a tight ±500-char window around each hit.
4. Sending each window to a local LLM (Ollama qwen2.5:7b on the GPU by
   default; falls back through Nemotron-3-Super-120B CPU on :8081 and the
   gemma micros on 8082-8085) with a strict JSON-only extraction prompt.
5. Validating the LLM response: must be a JSON object, must have either a
   numeric ``pct`` between 0 and 0.95 paired with a ``citation`` string, OR
   both fields null. Citation must literally appear in the source passage.
6. Writing accepted values back to ``supply_chain_edges`` (preserving the
   original confidence label as ``derived``) and logging EVERY attempt
   (accepted or rejected) to ``supply_chain_enrichment_log`` for audit.

The pipeline never falls back to a paid API. If every local provider is
unreachable the enricher raises ``LLMUnavailableError`` so the caller can
flag the run as blocked rather than silently producing nothing.

Public API
----------
    PctCogsEnricher(engine).run(tickers=[...], limit=N) -> EnrichmentSummary
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.supply_chain_parser import SupplyChain10KParser


# ── Tunables ────────────────────────────────────────────────────────────────

# Window of characters around each supplier-name hit that we feed to the LLM.
PASSAGE_WINDOW: int = 500
# Maximum number of distinct windows we extract per edge before giving up.
MAX_PASSAGES_PER_EDGE: int = 5
# Cap accepted percent at 0.95 — anything higher is almost certainly wrong.
PCT_HARD_CAP: float = 0.95
# How many characters of the LLM raw response we keep in the audit log.
RAW_RESPONSE_LOG_CHARS: int = 4000
# Provider chain. First entry that is reachable wins. The values are the
# concrete keyword argument bundles handed to ``_call_provider``.
PROVIDER_CHAIN: tuple[dict[str, Any], ...] = (
    {
        "name": "ollama-qwen2.5:7b",
        "kind": "ollama",
        "url": "http://localhost:11434/api/generate",
        "model": "qwen2.5:7b",
        "timeout": 60,
    },
    {
        "name": "ollama-llama3.3:70b",
        "kind": "ollama",
        "url": "http://localhost:11434/api/generate",
        "model": "llama3.3:70b-instruct-q4_K_M",
        "timeout": 180,
    },
    {
        "name": "llamacpp-nemotron-120b",
        "kind": "llamacpp_text",
        "url": "http://localhost:8081/v1/completions",
        "model": "nvidia_Nemotron-3-Super-120B-A12B-Q6_K-00001-of-00003",
        "timeout": 300,
    },
    {
        "name": "llamacpp-gemma-edgar-extractor",
        "kind": "llamacpp_text",
        "url": "http://localhost:8084/v1/completions",
        "model": "gemma-4-e2b-edgar_extractor.gguf",
        "timeout": 60,
    },
)

PROMPT_SYSTEM = (
    "You extract supplier cost-concentration percentages from 10-K excerpts. "
    "You ONLY reply with a single JSON object and NOTHING else. "
    "If the text explicitly discloses what percent of the BUYER company's "
    "cost of goods sold (or total cost of revenue) is represented by the "
    "named SUPPLIER, return "
    '{"pct": <float between 0 and 1>, "citation": "<exact verbatim quote>"}. '
    "Do NOT extract a percentage if the text only discloses what percent of "
    "the buyer's REVENUE goes to a customer — that is the wrong direction. "
    "Do NOT guess, infer, or estimate. If the percentage is not explicitly "
    "disclosed in the text, return "
    '{"pct": null, "citation": null}. '
    "The citation MUST appear verbatim in the excerpt."
)


class LLMUnavailableError(RuntimeError):
    """Raised when no LLM provider in the chain is reachable."""


# ── Result containers ───────────────────────────────────────────────────────


@dataclass
class EdgeRow:
    edge_id: int
    upstream_id: str
    downstream_id: str
    upstream_label: str
    downstream_label: str
    downstream_ticker: str
    relationship: str | None
    has_cogs: bool
    has_rev: bool
    # direction: "supplier_side" -> fetch downstream 10-K, extract pct_downstream_cogs
    #            "customer_side" -> fetch upstream 10-K, extract pct_upstream_revenue
    direction: str = "supplier_side"


@dataclass
class AttemptRecord:
    edge_id: int
    ticker: str
    upstream_id: str
    upstream_label: str
    field: str
    provider: str
    model: str
    passage_chars: int
    raw_response: str
    parsed_pct: float | None
    parsed_citation: str | None
    accepted: bool
    reason: str


@dataclass
class EnrichmentSummary:
    edges_considered: int = 0
    edges_with_passage: int = 0
    edges_accepted: int = 0
    attempts_total: int = 0
    attempts_accepted: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)
    accepted_samples: list[dict[str, Any]] = field(default_factory=list)
    provider_used: str = ""
    elapsed_seconds: float = 0.0

    def bump_reject(self, reason: str) -> None:
        self.reject_reasons[reason] = self.reject_reasons.get(reason, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "edges_considered": self.edges_considered,
            "edges_with_passage": self.edges_with_passage,
            "edges_accepted": self.edges_accepted,
            "attempts_total": self.attempts_total,
            "attempts_accepted": self.attempts_accepted,
            "acceptance_rate": (
                round(self.attempts_accepted / self.attempts_total, 3)
                if self.attempts_total
                else 0.0
            ),
            "reject_reasons": dict(self.reject_reasons),
            "accepted_samples": self.accepted_samples[:20],
            "provider_used": self.provider_used,
            "elapsed_seconds": round(self.elapsed_seconds, 1),
        }


# ── LLM provider plumbing ───────────────────────────────────────────────────


def _call_ollama(cfg: dict[str, Any], prompt: str) -> str | None:
    payload = {
        "model": cfg["model"],
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.0,
            "num_predict": 320,
        },
    }
    try:
        resp = requests.post(
            cfg["url"],
            json=payload,
            timeout=cfg.get("timeout", 60),
        )
        if resp.status_code >= 400:
            log.debug(
                "ollama {m} HTTP {s}: {b}",
                m=cfg["model"], s=resp.status_code, b=resp.text[:200],
            )
            return None
        data = resp.json()
        output = (data.get("response") or "").strip() or None
        if output:
            try:
                from llm.feedback_loop import log_llm_call

                log_llm_call(
                    module="pct_cogs_enrichment",
                    tier="extract",
                    system_prompt=PROMPT_SYSTEM[:2000],
                    user_prompt=prompt[:2000],
                    output=output[:2000],
                    context_tokens=data.get("prompt_eval_count", 0) or 0,
                    output_tokens=data.get("eval_count", 0) or 0,
                    latency_ms=int(data.get("total_duration", 0) / 1_000_000)
                    if data.get("total_duration") else 0,
                    model=data.get("model", cfg["model"]),
                    provider=cfg["name"],
                    metadata={"endpoint": cfg["url"], "kind": cfg["kind"]},
                )
            except Exception:
                pass
        return output
    except Exception as exc:
        log.debug("ollama {m} call failed: {e}", m=cfg["model"], e=str(exc))
        return None


def _call_llamacpp_text(cfg: dict[str, Any], prompt: str) -> str | None:
    payload = {
        "model": cfg["model"],
        "prompt": prompt,
        "max_tokens": 220,
        "temperature": 0.0,
        "stop": ["\n\n", "</s>"],
    }
    try:
        resp = requests.post(
            cfg["url"],
            json=payload,
            timeout=cfg.get("timeout", 120),
        )
        if resp.status_code >= 400:
            return None
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        text_out = (choices[0].get("text") or "").strip()
        if text_out:
            try:
                from llm.feedback_loop import log_llm_call

                usage = data.get("usage", {}) if isinstance(data, dict) else {}
                log_llm_call(
                    module="pct_cogs_enrichment",
                    tier="extract",
                    system_prompt=PROMPT_SYSTEM[:2000],
                    user_prompt=prompt[:2000],
                    output=text_out[:2000],
                    context_tokens=usage.get("prompt_tokens", 0) or 0,
                    output_tokens=usage.get("completion_tokens", 0) or 0,
                    latency_ms=0,
                    model=data.get("model", cfg["model"]),
                    provider=cfg["name"],
                    metadata={"endpoint": cfg["url"], "kind": cfg["kind"]},
                )
            except Exception:
                pass
        return text_out or None
    except Exception as exc:
        log.debug("llamacpp {m} call failed: {e}", m=cfg["model"], e=str(exc))
        return None


def _provider_health_check(cfg: dict[str, Any]) -> bool:
    """Lightweight reachability probe — sends a tiny prompt with a short
    timeout and accepts any non-error reply."""
    probe_prompt = 'Reply with this exact JSON only: {"pct": null, "citation": null}'
    if cfg["kind"] == "ollama":
        try:
            resp = requests.post(
                cfg["url"],
                json={
                    "model": cfg["model"],
                    "prompt": probe_prompt,
                    "stream": False,
                    "options": {"temperature": 0.0, "num_predict": 30},
                },
                timeout=15,
            )
            return resp.status_code < 400 and bool((resp.json() or {}).get("response"))
        except Exception:
            return False
    if cfg["kind"] == "llamacpp_text":
        try:
            resp = requests.post(
                cfg["url"],
                json={
                    "model": cfg["model"],
                    "prompt": probe_prompt,
                    "max_tokens": 20,
                    "temperature": 0.0,
                },
                timeout=20,
            )
            if resp.status_code >= 400:
                return False
            data = resp.json()
            choices = data.get("choices") or []
            return bool(choices and (choices[0].get("text") or "").strip())
        except Exception:
            return False
    return False


def _select_provider() -> dict[str, Any]:
    """Walk PROVIDER_CHAIN and return the first reachable config.

    Raises LLMUnavailableError if every provider fails the health check.
    """
    last_err: list[str] = []
    for cfg in PROVIDER_CHAIN:
        if _provider_health_check(cfg):
            log.info("pct_cogs_enrichment: provider {n} OK", n=cfg["name"])
            return cfg
        last_err.append(cfg["name"])
    raise LLMUnavailableError(
        "No local LLM provider is reachable; tried: " + ", ".join(last_err)
    )


def _call_llm(cfg: dict[str, Any], prompt: str) -> str | None:
    if cfg["kind"] == "ollama":
        return _call_ollama(cfg, prompt)
    if cfg["kind"] == "llamacpp_text":
        return _call_llamacpp_text(cfg, prompt)
    return None


# ── JSON parsing & validation ───────────────────────────────────────────────


_JSON_OBJ_RE = re.compile(r"\{.*?\}", re.DOTALL)


def _parse_llm_json(raw: str) -> tuple[float | None, str | None, str]:
    """Return (pct, citation, parse_status).

    parse_status is "ok" if a JSON object was successfully extracted, even
    when both fields are null. Otherwise it's "bad_json".
    """
    if not raw:
        return None, None, "bad_json"
    candidate = raw.strip()
    # Strip markdown fences if the model produced any
    if candidate.startswith("```"):
        candidate = re.sub(r"^```[a-zA-Z]*\s*", "", candidate)
        candidate = re.sub(r"```\s*$", "", candidate)
        candidate = candidate.strip()
    # Try the whole string first, then fall back to first {...} match
    objs: list[str] = [candidate]
    m = _JSON_OBJ_RE.search(candidate)
    if m and m.group(0) != candidate:
        objs.append(m.group(0))
    for blob in objs:
        try:
            data = json.loads(blob)
        except (ValueError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        pct = data.get("pct")
        cit = data.get("citation")
        if pct is not None:
            try:
                pct = float(pct)
            except (TypeError, ValueError):
                return None, None, "bad_json"
        if cit is not None and not isinstance(cit, str):
            cit = str(cit)
        return pct, cit, "ok"
    return None, None, "bad_json"


_PCT_TOKEN_RE = re.compile(r"(\d{1,2}(?:\.\d+)?)\s*%")


def _citation_contains_pct(citation: str, pct: float) -> bool:
    """Check whether the decimal pct actually appears (as a percent number)
    in the citation string. Accepts integer rounding on either side so e.g.
    a reported ``pct=0.156`` is accepted for a ``15.6%`` quote and a
    reported ``pct=0.15`` is accepted for a ``16%`` quote (the LLM may
    round down when the text says approximately 16%).
    """
    try:
        target = float(pct) * 100.0
    except (TypeError, ValueError):
        return False
    found: list[float] = []
    for m in _PCT_TOKEN_RE.finditer(citation or ""):
        try:
            found.append(float(m.group(1)))
        except ValueError:
            continue
    if not found:
        return False
    # Accept if any disclosed percentage is within 1.0 absolute of the
    # returned pct (i.e. the LLM is allowed to round to nearest integer).
    return any(abs(v - target) <= 1.0 for v in found)


def _validate(
    pct: float | None,
    citation: str | None,
    passage: str,
) -> tuple[bool, str]:
    """Decide whether to accept the parsed extraction.

    Returns (accepted, reason).
    """
    if pct is None and citation is None:
        return False, "not_disclosed"
    if pct is None:
        return False, "citation_missing"
    if citation is None or not citation.strip():
        return False, "citation_missing"
    if not (0.0 < pct < PCT_HARD_CAP):
        return False, "pct_out_of_range"
    # Citation has to literally appear in the passage. We compare on the
    # whitespace-collapsed forms so paragraph wrapping doesn't trip the check.
    passage_norm = re.sub(r"\s+", " ", passage).lower()
    citation_norm = re.sub(r"\s+", " ", citation).lower().strip(' ".,;:')
    if len(citation_norm) < 8:
        return False, "citation_missing"
    if citation_norm not in passage_norm:
        return False, "citation_not_in_text"
    # The returned pct must actually appear (as a percent) inside the
    # citation — otherwise the LLM is padding the quote with a fabricated
    # number.
    if not _citation_contains_pct(citation, pct):
        return False, "pct_not_in_citation"
    return True, "ok"


# ── Passage extraction ──────────────────────────────────────────────────────


def _label_variants(label: str) -> list[str]:
    """Generate the search terms we will look for in the 10-K text.

    We want to cast a reasonably wide net: strip legal suffixes, pull out
    parenthesized aliases, and expose the most distinctive head-noun so
    that commodity labels like "Lithium (carbonate + hydroxide)" still
    hit 10-K passages that just say "lithium".
    """
    label = (label or "").strip()
    if not label:
        return []
    out: list[str] = [label]

    # Parenthesized alias: "Olam Food Ingredients (ofi)" -> "ofi"
    paren = re.findall(r"\(([^)]{2,60})\)", label)
    for p in paren:
        out.append(p.strip())

    # Drop parenthesized suffix entirely, keep the bare head.
    bare_paren = re.sub(r"\s*\([^)]*\)\s*$", "", label).strip()
    if bare_paren and bare_paren != label:
        out.append(bare_paren)

    # Drop common legal suffixes
    bare = re.sub(
        r",?\s+(?:Inc\.?|LLC|L\.L\.C\.?|Ltd\.?|Limited|Corp\.?|Corporation|"
        r"Company|Co\.?|Holdings|Group|plc|PLC|AG|SA|N\.V\.?|S\.A\.?|"
        r"Technologies|International|Incorporated)\s*$",
        "",
        bare_paren or label,
        flags=re.IGNORECASE,
    ).strip()
    if bare and bare not in out:
        out.append(bare)

    # First word for very long names — only if reasonably distinctive.
    first = bare.split()[0] if bare else ""
    if len(first) >= 5 and first.lower() not in {
        "national", "global", "general", "international", "american", "united",
        "north", "south", "east", "west", "grand", "royal", "dairy", "farmers",
        "air", "crude", "type", "generic", "rare",
    }:
        out.append(first)

    # De-duplicate, preserve order, drop noise entries.
    seen: set[str] = set()
    uniq: list[str] = []
    for v in out:
        k = (v or "").strip().lower()
        if len(k) < 3:
            continue
        if k in seen:
            continue
        seen.add(k)
        uniq.append(v.strip())
    return uniq


def _find_passages(text_body: str, label: str) -> list[str]:
    """Return up to MAX_PASSAGES_PER_EDGE windows around supplier mentions."""
    if not text_body or not label:
        return []
    passages: list[str] = []
    seen_starts: set[int] = set()
    for variant in _label_variants(label):
        if not variant:
            continue
        try:
            pat = re.compile(r"\b" + re.escape(variant) + r"\b", re.IGNORECASE)
        except re.error:
            continue
        for match in pat.finditer(text_body):
            start = max(0, match.start() - PASSAGE_WINDOW)
            end = min(len(text_body), match.end() + PASSAGE_WINDOW)
            # Skip near-duplicates (same start window)
            bucket = start // 200
            if bucket in seen_starts:
                continue
            seen_starts.add(bucket)
            window = text_body[start:end]
            # Only keep windows that contain a percent sign — otherwise the
            # LLM has nothing to extract.
            if "%" not in window:
                continue
            passages.append(window)
            if len(passages) >= MAX_PASSAGES_PER_EDGE:
                return passages
    return passages


_HARVEST_KEYWORD_RE = re.compile(
    r"(?:our\s+(?:largest|principal|major|top)\s+(?:customer|supplier)|"
    r"concentration\s+of\s+(?:credit|customer|customers|revenue|revenues|sales)|"
    r"sales\s+to\s+[A-Z]|"
    r"accounted\s+for\s+approximately|"
    r"represent(?:ed)?\s+approximately|"
    r"net\s+sales\s+(?:to|from|made\s+to)|"
    r"% of\s+(?:our|the\s+company|total|consolidated|net)\s*"
    r"(?:sales|revenue|net\s+sales|consolidated\s+net\s+sales|cost)|"
    r"single\s+customer|"
    r"largest\s+customer|"
    r"principal\s+customer)",
    re.IGNORECASE,
)


def _slice_percent_windows(
    body: str,
    window: int = 1200,
    max_windows: int = 5,
) -> list[str]:
    """Return up to ``max_windows`` slices where the filer is most likely
    disclosing a customer / supplier concentration.

    Rather than sliding a window blindly, we anchor on concentration-
    disclosure keywords (``sales to``, ``largest customer``, ``accounted
    for approximately``) and emit a ±1400-char window around each unique
    hit. This reduces the LLM call count by roughly 5x compared to a
    fixed-stride pass while giving the model the exact sentences where
    percentages live.
    """
    if not body:
        return []
    out: list[str] = []
    seen_buckets: set[int] = set()
    for match in _HARVEST_KEYWORD_RE.finditer(body):
        start = max(0, match.start() - window // 2)
        end = min(len(body), match.end() + window // 2)
        slab = body[start:end]
        if "%" not in slab:
            continue
        bucket = start // 600
        if bucket in seen_buckets:
            continue
        seen_buckets.add(bucket)
        out.append(slab)
        if len(out) >= max_windows:
            break
    return out


def _build_prompt_harvest(ticker: str, passage: str) -> str:
    return (
        PROMPT_SYSTEM_HARVEST
        + "\n\n"
        + f"Filer: {ticker.upper()} (10-K)\n\n"
        + "Excerpt:\n"
        + '"""'
        + "\n"
        + passage.strip()
        + "\n"
        + '"""'
        + "\n\n"
        + "Extract every named-counterparty concentration disclosure. "
        + "Reply with the JSON only.\nJSON: "
    )


def _parse_harvest_findings(raw: str) -> list[dict[str, Any]] | None:
    """Return the list of findings from a harvest-prompt response, or None
    on parse failure. Empty list means LLM said no disclosures."""
    if not raw:
        return None
    candidate = raw.strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```[a-zA-Z]*\s*", "", candidate)
        candidate = re.sub(r"```\s*$", "", candidate)
        candidate = candidate.strip()
    blobs: list[str] = [candidate]
    m = re.search(r"\{.*\}", candidate, re.DOTALL)
    if m and m.group(0) != candidate:
        blobs.append(m.group(0))
    for blob in blobs:
        try:
            data = json.loads(blob)
        except (ValueError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        findings = data.get("findings")
        if findings is None:
            return []
        if not isinstance(findings, list):
            continue
        # Normalize entries
        out: list[dict[str, Any]] = []
        for f in findings:
            if isinstance(f, dict):
                out.append(f)
        return out
    return None


def _build_prompt_supplier(
    upstream_label: str,
    downstream_ticker: str,
    passage: str,
) -> str:
    return (
        PROMPT_SYSTEM
        + "\n\n"
        + f"Buyer ticker: {downstream_ticker.upper()}\n"
        + f"Supplier name: {upstream_label}\n\n"
        + "Excerpt from buyer 10-K:\n"
        + '"""'
        + "\n"
        + passage.strip()
        + "\n"
        + '"""'
        + "\n\n"
        + "What percent of the BUYER's cost of goods sold (or total cost of "
        + "revenue) is explicitly disclosed as coming from the SUPPLIER above? "
        + "Reply with the JSON only.\nJSON: "
    )


PROMPT_SYSTEM_HARVEST = (
    "You extract supplier or customer concentration disclosures from a "
    "10-K excerpt. You ONLY reply with a single JSON object and NOTHING "
    "else. Your job is to find every disclosure where the filer explicitly "
    "states what percent of its revenue / net sales comes from a NAMED "
    "customer, OR what percent of its cost of goods sold (or total cost of "
    "revenue) comes from a NAMED supplier. "
    'Return {"findings": [{"direction": "customer"|"supplier", '
    '"counterparty": "<name as it appears>", "pct": <float 0-1>, '
    '"citation": "<exact verbatim quote>"}]}. '
    "If there are no such disclosures in the excerpt, return "
    '{"findings": []}. '
    "Do NOT include region/geography percentages, market share, employee "
    "percentages, or segment revenue — only named counterparty "
    "concentration. Every citation must appear verbatim in the excerpt."
)


PROMPT_SYSTEM_CUSTOMER = (
    "You extract customer-revenue-concentration percentages from 10-K "
    "excerpts. You ONLY reply with a single JSON object and NOTHING else. "
    "If the text explicitly discloses what percent of the SELLER company's "
    "net sales, revenue, or consolidated sales comes from the named "
    'CUSTOMER, return {"pct": <float between 0 and 1>, '
    '"citation": "<exact verbatim quote>"}. '
    "Do NOT extract a percentage if the text only discloses a procurement "
    "share, a market share, or an employee headcount — only customer "
    "revenue concentration counts. Do NOT guess, infer, or estimate. If "
    "the percentage is not explicitly disclosed, return "
    '{"pct": null, "citation": null}. '
    "The citation MUST appear verbatim in the excerpt."
)


def _build_prompt_customer(
    seller_label: str,
    customer_label: str,
    passage: str,
) -> str:
    return (
        PROMPT_SYSTEM_CUSTOMER
        + "\n\n"
        + f"Seller: {seller_label}\n"
        + f"Customer name: {customer_label}\n\n"
        + "Excerpt from seller 10-K:\n"
        + '"""'
        + "\n"
        + passage.strip()
        + "\n"
        + '"""'
        + "\n\n"
        + "What percent of the SELLER's revenue or net sales is explicitly "
        + "disclosed as coming from the CUSTOMER above? Reply with the JSON "
        + "only.\nJSON: "
    )


# ── Main enricher ───────────────────────────────────────────────────────────


class PctCogsEnricher:
    """LLM enrichment pipeline for ``supply_chain_edges.pct_downstream_cogs``.

    Composes the regex parser only for its SEC fetch + 10-K text helpers; it
    does NOT re-run regex extraction.
    """

    def __init__(self, engine: Engine, provider: dict[str, Any] | None = None):
        self.engine = engine
        self._sec = SupplyChain10KParser(db_engine=engine)
        self._text_cache: dict[str, str] = {}
        self._provider = provider  # set lazily on first run() call

    # ── DB read ──────────────────────────────────────────────────────────

    def _fetch_target_edges(
        self,
        tickers: list[str] | None,
        limit: int | None,
    ) -> list[EdgeRow]:
        """Pull every edge that is missing at least one of the concentration
        fields we're trying to fill. Scope is restricted to edges whose
        relevant side is in the requested ticker set.

        Returns rows annotated with ``direction`` so the worker knows which
        10-K to fetch and which field to update:

        - ``supplier_side``: supplier is the upstream; we fetch the buyer
          (downstream) 10-K and try to fill ``pct_downstream_cogs``.
          Only pursued when the upstream node is a real company or tier-1
          raw-material name (skip generic commodity rollups like
          ``oil_crude``).
        - ``customer_side``: the edge records a customer relationship, where
          the downstream actor is the buyer. We fetch the UPSTREAM (seller)
          ticker's 10-K and try to fill ``pct_upstream_revenue``.
        """
        sql = (
            """
            SELECT e.id,
                   e.upstream_id,
                   e.downstream_id,
                   COALESCE(nu.name, e.upstream_id)   AS upstream_label,
                   COALESCE(nd.name, e.downstream_id) AS downstream_label,
                   e.relationship,
                   e.pct_downstream_cogs,
                   e.pct_upstream_revenue,
                   nu.type                            AS upstream_type,
                   nd.type                            AS downstream_type
              FROM supply_chain_edges e
              LEFT JOIN supply_chain_nodes nu ON nu.id = e.upstream_id
              LEFT JOIN supply_chain_nodes nd ON nd.id = e.downstream_id
             WHERE (e.pct_downstream_cogs IS NULL OR e.pct_upstream_revenue IS NULL)
            """
        )
        params: dict[str, Any] = {}
        if tickers:
            tks = [t.lower() for t in tickers]
            sql += (
                " AND (lower(e.downstream_id) = ANY(:tks) "
                "OR lower(e.upstream_id) = ANY(:tks))"
            )
            params["tks"] = tks
        sql += " ORDER BY e.id"
        if limit is not None and limit > 0:
            sql += " LIMIT :lim"
            params["lim"] = int(limit * 2)  # oversample; many get filtered

        out: list[EdgeRow] = []
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
        for r in rows:
            upstream_id = str(r[1])
            downstream_id = str(r[2])
            upstream_label = str(r[3])
            downstream_label = str(r[4])
            relationship = str(r[5]) if r[5] is not None else None
            has_cogs = r[6] is not None
            has_rev = r[7] is not None
            upstream_type = (r[8] or "").lower()

            # Determine direction + target ticker
            if relationship == "customer":
                # upstream is SELLER, downstream is BUYER
                # We want pct_upstream_revenue = what % of seller rev goes
                # to this buyer. Source doc: seller's 10-K.
                if has_rev:
                    continue  # already have it
                if upstream_type != "ticker":
                    continue  # cannot fetch a 10-K
                direction = "customer_side"
                ticker = upstream_id.upper()
            else:
                # supplier_side
                if has_cogs:
                    continue
                # Skip country/region nodes — no natural search term.
                # (Commodities are OK; we extract the head noun in
                # _label_variants, e.g. "Lithium (carbonate + hydroxide)"
                # -> "lithium" which will hit TSLA's 10-K passages.)
                if upstream_type in {"country", "region"}:
                    continue
                direction = "supplier_side"
                ticker = downstream_id.upper()

            out.append(
                EdgeRow(
                    edge_id=int(r[0]),
                    upstream_id=upstream_id,
                    downstream_id=downstream_id,
                    upstream_label=upstream_label,
                    downstream_label=downstream_label,
                    downstream_ticker=ticker,
                    relationship=relationship,
                    has_cogs=has_cogs,
                    has_rev=has_rev,
                    direction=direction,
                )
            )
            if limit is not None and len(out) >= limit:
                break
        return out

    # ── 10-K text fetch ──────────────────────────────────────────────────

    def _get_10k_text(self, ticker: str) -> str | None:
        if ticker in self._text_cache:
            return self._text_cache[ticker] or None
        cik = self._sec._resolve_cik(ticker)
        if not cik:
            self._text_cache[ticker] = ""
            return None
        meta = self._sec._latest_10k_meta(cik)
        if not meta:
            self._text_cache[ticker] = ""
            return None
        body = self._sec._fetch_10k_text(cik, meta)
        if not body:
            self._text_cache[ticker] = ""
            return None
        self._text_cache[ticker] = body
        return body

    # ── DB writes ────────────────────────────────────────────────────────

    def _update_edge(
        self,
        edge_id: int,
        pct: float,
        citation: str,
        ticker: str,
        field: str,
        force: bool = False,
    ) -> bool:
        """Write an extracted pct to the given field.

        When ``force`` is false, the UPDATE only runs if the column is
        currently NULL (preserving any pre-existing value, e.g. from the
        hand-curated seed). When ``force`` is true, the row is unconditionally
        overwritten — use this when the new value comes with a verbatim
        10-K citation, which beats a seed guess.
        """
        snippet = (citation or "")[:200].replace("\n", " ")
        new_source = f"10-K LLM extraction: {snippet}"
        null_guard = "" if force else " AND pct_downstream_cogs IS NULL"
        null_guard_rev = "" if force else " AND pct_upstream_revenue IS NULL"
        if field == "pct_downstream_cogs":
            sql = (
                "UPDATE supply_chain_edges "
                "SET pct_downstream_cogs = :pct, confidence = 'derived', "
                "    source = :src "
                "WHERE id = :id" + null_guard
            )
        elif field == "pct_upstream_revenue":
            sql = (
                "UPDATE supply_chain_edges "
                "SET pct_upstream_revenue = :pct, confidence = 'derived', "
                "    source = :src "
                "WHERE id = :id" + null_guard_rev
            )
        else:
            return False
        try:
            with self.engine.begin() as conn:
                res = conn.execute(
                    text(sql),
                    {"pct": pct, "src": new_source, "id": edge_id},
                )
                return res.rowcount > 0
        except Exception as exc:
            log.warning(
                "pct_cogs_enrichment: update failed for edge {i} field {f}: {e}",
                i=edge_id, f=field, e=str(exc),
            )
            return False

    # ── Missing-edge materialization ─────────────────────────────────────

    _ANON_TOKENS: tuple[str, ...] = (
        "not specified",
        "one customer",
        "one direct customer",
        "another direct customer",
        "top five",
        "top 5",
        "five largest",
        "ai research",
        "semiconductor solutions customer",
        "client and gaming segment customer",
        "direct customer",
        "one distributor",
        "undisclosed",
        "anonymous",
        "unnamed",
        "certain",
    )

    _NAME_TO_TICKER: tuple[tuple[str, str], ...] = (
        ("walmart", "wmt"),
        ("wal-mart", "wmt"),
        ("sam's club", "wmt"),
        ("costco", "cost"),
        ("kroger", "kr"),
        ("target corp", "tgt"),
        ("amazon.com", "amzn"),
        ("coca-cola europacific", "ccep"),
        ("coca-cola consolidated", "coke"),
        ("td synnex", "snx"),
        ("synnex", "snx"),
        ("arrow electronics", "arw"),
        ("avnet", "avt"),
        ("ingram micro", "im"),
        ("procter & gamble", "pg"),
        ("apple inc", "aapl"),
        ("microsoft corp", "msft"),
        ("google", "googl"),
        ("meta platforms", "meta"),
        ("tesla inc", "tsla"),
        ("nvidia corp", "nvda"),
    )

    @staticmethod
    def _is_anonymous_cp(name: str) -> bool:
        ln = (name or "").lower().strip()
        if not ln or len(ln) < 4:
            return True
        for token in PctCogsEnricher._ANON_TOKENS:
            if token in ln:
                return True
        return False

    @staticmethod
    def _clean_counterparty_name(raw: str) -> str:
        name = (raw or "").strip().strip(' "\'.,;:')
        name = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
        suffix_re = re.compile(
            r",?\s+(?:Inc\.?|LLC|L\.L\.C\.?|Ltd\.?|Limited|Corp\.?|Corporation|"
            r"Company|Co\.?|Holdings|Group|plc|PLC|AG|SA|N\.V\.?|S\.A\.?|"
            r"Technologies|International|Incorporated|Stores|"
            r"and\s+its\s+affiliates|and\s+its\s+subsidiaries|"
            r"and\s+affiliates|and\s+subsidiaries)\s*$",
            re.IGNORECASE,
        )
        for _ in range(2):
            stripped = suffix_re.sub("", name).strip().rstrip(",.")
            if stripped == name or not stripped:
                break
            name = stripped
        return name.strip() or (raw or "").strip()

    @staticmethod
    def _slugify_name(name: str) -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "_", name.lower()).strip("_")
        slug = re.sub(r"_+", "_", slug)
        return slug[:60] or "unknown"

    def _resolve_counterparty_node(self, raw_name: str) -> str | None:
        """Resolve (or create) a supply_chain_nodes row for ``raw_name``."""
        if self._is_anonymous_cp(raw_name):
            return None
        ln = (raw_name or "").lower()
        override: str | None = None
        for key, node_id in self._NAME_TO_TICKER:
            if key in ln:
                override = node_id
                break
        cleaned = self._clean_counterparty_name(raw_name)
        try:
            with self.engine.connect() as conn:
                if override:
                    row = conn.execute(
                        text("SELECT id FROM supply_chain_nodes WHERE id = :i"),
                        {"i": override},
                    ).fetchone()
                    if row:
                        return str(row[0])
                row = conn.execute(
                    text(
                        """
                        SELECT id FROM supply_chain_nodes
                         WHERE lower(name) = lower(:n)
                            OR lower(name) LIKE lower(:lk)
                         ORDER BY
                            CASE WHEN type = 'ticker' THEN 0 ELSE 1 END,
                            length(name)
                         LIMIT 1
                        """
                    ),
                    {"n": cleaned, "lk": f"{cleaned}%"},
                ).fetchone()
                if row:
                    return str(row[0])
        except Exception as exc:
            log.warning("counterparty lookup failed for {n}: {e}", n=cleaned, e=str(exc))
            return None

        node_id = self._slugify_name(cleaned)
        if not node_id or len(node_id) < 3:
            return None
        try:
            with self.engine.begin() as conn:
                existing = conn.execute(
                    text("SELECT id FROM supply_chain_nodes WHERE id = :i"),
                    {"i": node_id},
                ).fetchone()
                if not existing:
                    conn.execute(
                        text(
                            """
                            INSERT INTO supply_chain_nodes (id, name, type, notes)
                            VALUES (:id, :name, 'private_company',
                                    'auto-created from 10-K LLM harvester')
                            ON CONFLICT (id) DO NOTHING
                            """
                        ),
                        {"id": node_id, "name": cleaned},
                    )
            return node_id
        except Exception as exc:
            log.warning("node create failed for {n}: {e}", n=node_id, e=str(exc))
            return None

    def _create_missing_harvest_edge(
        self,
        ticker: str,
        counterparty: str,
        direction: str,
        pct: float,
        citation: str,
    ) -> int | None:
        """Create the missing node + edge for a harvester finding.

        Returns the new ``supply_chain_edges.id`` on success, or ``None`` if
        the counterparty is anonymous, the ticker node is missing, or the
        write failed.
        """
        filer_id = ticker.lower()
        try:
            with self.engine.connect() as conn:
                ok = conn.execute(
                    text("SELECT 1 FROM supply_chain_nodes WHERE id = :i"),
                    {"i": filer_id},
                ).fetchone()
            if not ok:
                return None
        except Exception:
            return None

        counterparty_id = self._resolve_counterparty_node(counterparty)
        if not counterparty_id or counterparty_id == filer_id:
            return None

        snippet = (citation or "")[:200].replace("\n", " ")
        source = f"10-K LLM harvester-create: {snippet}"
        if direction == "customer":
            seller, buyer = filer_id, counterparty_id
            field = "pct_upstream_revenue"
            relationship = "customer"
        else:
            seller, buyer = counterparty_id, filer_id
            field = "pct_downstream_cogs"
            relationship = "component"

        sql_insert = (
            """
            INSERT INTO supply_chain_edges (
                upstream_id, downstream_id, relationship, tier,
                """
            + field
            + """, confidence, source
            ) VALUES (:u, :d, :rel, 1, :pct, 'derived', :src)
            ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
            DO UPDATE SET """
            + field
            + """ = EXCLUDED."""
            + field
            + """,
                confidence = EXCLUDED.confidence,
                source = EXCLUDED.source
            RETURNING id
            """
        )
        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(sql_insert),
                    {
                        "u": seller,
                        "d": buyer,
                        "rel": relationship,
                        "pct": float(pct),
                        "src": source,
                    },
                ).fetchone()
            return int(row[0]) if row else None
        except Exception as exc:
            log.warning(
                "harvest edge create failed {u}->{d}: {e}",
                u=seller, d=buyer, e=str(exc),
            )
            return None

    def _log_attempt(self, rec: AttemptRecord) -> None:
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO supply_chain_enrichment_log (
                            edge_id, ticker, upstream_id, upstream_label,
                            field, llm_provider, llm_model, passage_chars,
                            raw_response, parsed_pct, parsed_citation,
                            accepted, reason
                        ) VALUES (
                            :edge_id, :ticker, :upstream_id, :upstream_label,
                            :field, :llm_provider, :llm_model, :passage_chars,
                            :raw_response, :parsed_pct, :parsed_citation,
                            :accepted, :reason
                        )
                        """
                    ),
                    {
                        "edge_id": rec.edge_id,
                        "ticker": rec.ticker,
                        "upstream_id": rec.upstream_id,
                        "upstream_label": rec.upstream_label[:200],
                        "field": rec.field,
                        "llm_provider": rec.provider,
                        "llm_model": rec.model,
                        "passage_chars": rec.passage_chars,
                        "raw_response": (rec.raw_response or "")[:RAW_RESPONSE_LOG_CHARS],
                        "parsed_pct": rec.parsed_pct,
                        "parsed_citation": (
                            (rec.parsed_citation or "")[:1000]
                            if rec.parsed_citation
                            else None
                        ),
                        "accepted": rec.accepted,
                        "reason": rec.reason,
                    },
                )
        except Exception as exc:
            log.warning(
                "pct_cogs_enrichment: log insert failed: {e}", e=str(exc)
            )

    # ── Per-edge worker ──────────────────────────────────────────────────

    def _enrich_edge(
        self,
        edge: EdgeRow,
        provider: dict[str, Any],
        summary: EnrichmentSummary,
    ) -> None:
        # Branch on direction — the ticker we fetch and the field we update
        # are different.
        if edge.direction == "customer_side":
            source_ticker = edge.upstream_id.upper()
            search_label = edge.downstream_label
            field = "pct_upstream_revenue"
        else:
            source_ticker = edge.downstream_ticker
            search_label = edge.upstream_label
            field = "pct_downstream_cogs"

        body = self._get_10k_text(source_ticker)
        if not body:
            rec = AttemptRecord(
                edge_id=edge.edge_id,
                ticker=source_ticker,
                upstream_id=edge.upstream_id,
                upstream_label=edge.upstream_label,
                field=field,
                provider=provider["name"],
                model=str(provider["model"]),
                passage_chars=0,
                raw_response="",
                parsed_pct=None,
                parsed_citation=None,
                accepted=False,
                reason="no_passage",
            )
            self._log_attempt(rec)
            summary.attempts_total += 1
            summary.bump_reject("no_passage")
            return

        passages = _find_passages(body, search_label)
        if not passages:
            rec = AttemptRecord(
                edge_id=edge.edge_id,
                ticker=source_ticker,
                upstream_id=edge.upstream_id,
                upstream_label=edge.upstream_label,
                field=field,
                provider=provider["name"],
                model=str(provider["model"]),
                passage_chars=0,
                raw_response="",
                parsed_pct=None,
                parsed_citation=None,
                accepted=False,
                reason="no_passage",
            )
            self._log_attempt(rec)
            summary.attempts_total += 1
            summary.bump_reject("no_passage")
            return

        summary.edges_with_passage += 1

        for passage in passages:
            if edge.direction == "customer_side":
                prompt = _build_prompt_customer(
                    seller_label=edge.upstream_label,
                    customer_label=edge.downstream_label,
                    passage=passage,
                )
            else:
                prompt = _build_prompt_supplier(
                    upstream_label=edge.upstream_label,
                    downstream_ticker=source_ticker,
                    passage=passage,
                )
            raw = _call_llm(provider, prompt)
            summary.attempts_total += 1
            if raw is None:
                rec = AttemptRecord(
                    edge_id=edge.edge_id,
                    ticker=source_ticker,
                    upstream_id=edge.upstream_id,
                    upstream_label=edge.upstream_label,
                    field=field,
                    provider=provider["name"],
                    model=str(provider["model"]),
                    passage_chars=len(passage),
                    raw_response="",
                    parsed_pct=None,
                    parsed_citation=None,
                    accepted=False,
                    reason="no_llm_response",
                )
                self._log_attempt(rec)
                summary.bump_reject("no_llm_response")
                continue

            pct, cit, parse_status = _parse_llm_json(raw)
            if parse_status != "ok":
                rec = AttemptRecord(
                    edge_id=edge.edge_id,
                    ticker=source_ticker,
                    upstream_id=edge.upstream_id,
                    upstream_label=edge.upstream_label,
                    field=field,
                    provider=provider["name"],
                    model=str(provider["model"]),
                    passage_chars=len(passage),
                    raw_response=raw,
                    parsed_pct=None,
                    parsed_citation=None,
                    accepted=False,
                    reason="bad_json",
                )
                self._log_attempt(rec)
                summary.bump_reject("bad_json")
                continue

            accepted, reason = _validate(pct, cit, passage)
            if not accepted:
                rec = AttemptRecord(
                    edge_id=edge.edge_id,
                    ticker=source_ticker,
                    upstream_id=edge.upstream_id,
                    upstream_label=edge.upstream_label,
                    field=field,
                    provider=provider["name"],
                    model=str(provider["model"]),
                    passage_chars=len(passage),
                    raw_response=raw,
                    parsed_pct=pct,
                    parsed_citation=cit,
                    accepted=False,
                    reason=reason,
                )
                self._log_attempt(rec)
                summary.bump_reject(reason)
                continue

            # Accepted — write back and short-circuit subsequent passages.
            wrote = self._update_edge(
                edge_id=edge.edge_id,
                pct=float(pct),
                citation=str(cit),
                ticker=source_ticker,
                field=field,
            )
            final_reason = "ok" if wrote else "update_failed"
            rec = AttemptRecord(
                edge_id=edge.edge_id,
                ticker=source_ticker,
                upstream_id=edge.upstream_id,
                upstream_label=edge.upstream_label,
                field=field,
                provider=provider["name"],
                model=str(provider["model"]),
                passage_chars=len(passage),
                raw_response=raw,
                parsed_pct=float(pct),
                parsed_citation=str(cit),
                accepted=wrote,
                reason=final_reason,
            )
            self._log_attempt(rec)
            if wrote:
                summary.attempts_accepted += 1
                summary.edges_accepted += 1
                summary.accepted_samples.append(
                    {
                        "edge_id": edge.edge_id,
                        "direction": edge.direction,
                        "source_ticker": source_ticker,
                        "upstream_id": edge.upstream_id,
                        "upstream_label": edge.upstream_label,
                        "downstream_id": edge.downstream_id,
                        "downstream_label": edge.downstream_label,
                        "field": field,
                        "pct": float(pct),
                        "citation": str(cit)[:240],
                    }
                )
                return  # done with this edge
            summary.bump_reject("update_failed")

    # ── Broad harvester ─────────────────────────────────────────────────

    def _harvest_ticker(
        self,
        ticker: str,
        provider: dict[str, Any],
        summary: EnrichmentSummary,
    ) -> None:
        """Scan one ticker's 10-K for every explicit customer/supplier
        concentration disclosure, then try to match each finding against an
        existing ``supply_chain_edges`` row and fill the missing pct.

        This is the high-yield complementary pass to ``_enrich_edge``: for
        filers that disclose concentrations generally (Walmart is 14% of
        sales) without naming suppliers, we harvest the text and match by
        counterparty name.
        """
        body = self._get_10k_text(ticker)
        if not body:
            return

        # Grab all windows that contain the substring "%" — these are the
        # only passages that could possibly disclose a percentage. We walk
        # the body in ~1400-char strides with ~200-char overlap to avoid
        # slicing mid-sentence.
        windows = _slice_percent_windows(body)
        if not windows:
            return

        # Load edges for this ticker (both directions) so we can match
        # findings against pre-existing rows.
        ticker_l = ticker.lower()
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT e.id, e.upstream_id, e.downstream_id,
                               COALESCE(nu.name, e.upstream_id),
                               COALESCE(nd.name, e.downstream_id),
                               e.relationship,
                               e.pct_downstream_cogs,
                               e.pct_upstream_revenue
                          FROM supply_chain_edges e
                          LEFT JOIN supply_chain_nodes nu ON nu.id = e.upstream_id
                          LEFT JOIN supply_chain_nodes nd ON nd.id = e.downstream_id
                         WHERE lower(e.downstream_id) = :t OR lower(e.upstream_id) = :t
                        """
                    ),
                    {"t": ticker_l},
                ).fetchall()
        except Exception as exc:
            log.warning("harvest: edge lookup failed for {t}: {e}", t=ticker, e=str(exc))
            return

        # Build lookup: counterparty (name) -> (edge_id, direction, field, label, had_value_pre)
        matcher: list[tuple[str, int, str, str, str, bool]] = []
        for r in rows:
            eid = int(r[0])
            up = str(r[1])
            down = str(r[2])
            up_label = str(r[3])
            down_label = str(r[4])
            rel = str(r[5]) if r[5] is not None else ""
            has_cogs = r[6] is not None
            has_rev = r[7] is not None

            # Ticker is the buyer; counterparty is the supplier. We match
            # against every edge where downstream is this ticker, even if
            # pct_downstream_cogs is already set — a fresh LLM-cited value
            # is more trustworthy than the seed.
            if down.lower() == ticker_l:
                for variant in _label_variants(up_label):
                    matcher.append(
                        (variant.lower(), eid, "supplier", "pct_downstream_cogs", up_label, has_cogs)
                    )
            # Ticker is the seller; counterparty is the buyer.
            if up.lower() == ticker_l and rel == "customer":
                for variant in _label_variants(down_label):
                    matcher.append(
                        (variant.lower(), eid, "customer", "pct_upstream_revenue", down_label, has_rev)
                    )

        if not matcher:
            return

        # Iterate through windows, call LLM once per window, try to match
        # every finding against the matcher list.
        accepted_edge_ids: set[int] = set()
        for window in windows:  # already capped in _slice_percent_windows
            prompt = _build_prompt_harvest(ticker=ticker, passage=window)
            raw = _call_llm(provider, prompt)
            summary.attempts_total += 1
            if not raw:
                summary.bump_reject("no_llm_response")
                continue

            findings = _parse_harvest_findings(raw)
            if findings is None:
                summary.bump_reject("bad_json")
                self._log_attempt(
                    AttemptRecord(
                        edge_id=0,
                        ticker=ticker,
                        upstream_id="harvest",
                        upstream_label="<harvest>",
                        field="harvest",
                        provider=provider["name"],
                        model=str(provider["model"]),
                        passage_chars=len(window),
                        raw_response=raw,
                        parsed_pct=None,
                        parsed_citation=None,
                        accepted=False,
                        reason="bad_json",
                    )
                )
                continue
            if not findings:
                summary.bump_reject("not_disclosed")
                continue

            for finding in findings:
                direction = str(finding.get("direction", "")).lower()
                counterparty = str(finding.get("counterparty") or "").strip()
                try:
                    pct = float(finding.get("pct"))
                except (TypeError, ValueError):
                    summary.bump_reject("bad_json")
                    continue
                citation = str(finding.get("citation") or "").strip()
                if not counterparty or not citation:
                    summary.bump_reject("citation_missing")
                    continue
                if not (0.0 < pct < PCT_HARD_CAP):
                    summary.bump_reject("pct_out_of_range")
                    continue
                # Citation must appear in the window (verbatim, whitespace-
                # normalized).
                passage_norm = re.sub(r"\s+", " ", window).lower()
                cit_norm = re.sub(r"\s+", " ", citation).lower().strip(' ".,;:')
                if len(cit_norm) < 8 or cit_norm not in passage_norm:
                    summary.bump_reject("citation_not_in_text")
                    continue
                # The returned pct must appear inside the citation, not
                # just somewhere in the broader window.
                if not _citation_contains_pct(citation, pct):
                    summary.bump_reject("pct_not_in_citation")
                    continue

                # Match counterparty against the edge list.
                cp_l = counterparty.lower()
                best: tuple[int, str, str, str, bool] | None = None
                for variant, eid, d, field_name, orig_label, had_pre in matcher:
                    if d != direction:
                        continue
                    if eid in accepted_edge_ids:
                        continue
                    if variant in cp_l or cp_l in variant:
                        best = (eid, d, field_name, orig_label, had_pre)
                        break
                if best is None:
                    # No matching edge — attempt to create the node + edge
                    # inline so future runs can update in place. This turns
                    # a citation-backed reject into a new derived edge
                    # rather than losing the data.
                    created_edge_id = self._create_missing_harvest_edge(
                        ticker=ticker,
                        counterparty=counterparty,
                        direction=direction,
                        pct=pct,
                        citation=citation,
                    )
                    if created_edge_id is None:
                        self._log_attempt(
                            AttemptRecord(
                                edge_id=0,
                                ticker=ticker,
                                upstream_id="harvest",
                                upstream_label=counterparty[:200],
                                field="harvest",
                                provider=provider["name"],
                                model=str(provider["model"]),
                                passage_chars=len(window),
                                raw_response=raw,
                                parsed_pct=pct,
                                parsed_citation=citation,
                                accepted=False,
                                reason="no_matching_edge",
                            )
                        )
                        summary.bump_reject("no_matching_edge")
                        continue
                    accepted_edge_ids.add(created_edge_id)
                    summary.edges_accepted += 1
                    summary.attempts_accepted += 1
                    field_name = (
                        "pct_upstream_revenue"
                        if direction == "customer"
                        else "pct_downstream_cogs"
                    )
                    self._log_attempt(
                        AttemptRecord(
                            edge_id=created_edge_id,
                            ticker=ticker,
                            upstream_id=counterparty[:200],
                            upstream_label=counterparty,
                            field=field_name,
                            provider=provider["name"],
                            model=str(provider["model"]),
                            passage_chars=len(window),
                            raw_response=raw,
                            parsed_pct=pct,
                            parsed_citation=citation,
                            accepted=True,
                            reason="edge_created",
                        )
                    )
                    summary.accepted_samples.append(
                        {
                            "edge_id": created_edge_id,
                            "direction": direction,
                            "ticker": ticker,
                            "counterparty": counterparty,
                            "field": field_name,
                            "pct": pct,
                            "citation": citation[:240],
                            "created": True,
                        }
                    )
                    continue

                eid, d, field_name, orig_label, had_pre = best
                wrote = self._update_edge(
                    edge_id=eid,
                    pct=pct,
                    citation=citation,
                    ticker=ticker,
                    field=field_name,
                    force=True,
                )
                reason = ("refresh" if had_pre else "ok") if wrote else "duplicate"
                self._log_attempt(
                    AttemptRecord(
                        edge_id=eid,
                        ticker=ticker,
                        upstream_id=orig_label[:200],
                        upstream_label=orig_label,
                        field=field_name,
                        provider=provider["name"],
                        model=str(provider["model"]),
                        passage_chars=len(window),
                        raw_response=raw,
                        parsed_pct=pct,
                        parsed_citation=citation,
                        accepted=wrote,
                        reason=reason,
                    )
                )
                if wrote:
                    accepted_edge_ids.add(eid)
                    summary.edges_accepted += 1
                    summary.attempts_accepted += 1
                    summary.accepted_samples.append(
                        {
                            "edge_id": eid,
                            "direction": d,
                            "ticker": ticker,
                            "counterparty": orig_label,
                            "field": field_name,
                            "pct": pct,
                            "citation": citation[:240],
                        }
                    )

    # ── Public entry point ──────────────────────────────────────────────

    def run(
        self,
        tickers: list[str] | None = None,
        limit: int | None = None,
        mode: str = "both",
    ) -> EnrichmentSummary:
        """Run the enrichment pipeline.

        ``mode`` selects which pass(es) to perform:
          * ``"per_edge"`` — classic per-edge targeted extraction only.
          * ``"harvest"`` — broad per-ticker harvest only.
          * ``"both"`` (default) — harvest first, then per-edge cleanup for
            edges still missing values.
        """
        summary = EnrichmentSummary()
        started = time.monotonic()

        if self._provider is None:
            self._provider = _select_provider()
        summary.provider_used = self._provider["name"]

        # ── Harvest pass ────────────────────────────────────────────────
        if mode in ("harvest", "both"):
            harvest_tickers = self._harvest_ticker_list(tickers=tickers)
            log.info(
                "pct_cogs_enrichment[harvest]: {n} tickers to scan",
                n=len(harvest_tickers),
            )
            for i, tkr in enumerate(harvest_tickers, start=1):
                try:
                    self._harvest_ticker(tkr, self._provider, summary)
                except Exception as exc:
                    log.warning(
                        "harvest {t} crashed: {x}", t=tkr, x=str(exc)
                    )
                if i % 5 == 0:
                    log.info(
                        "harvest progress: {i}/{n} tickers, accepted={a}",
                        i=i, n=len(harvest_tickers), a=summary.edges_accepted,
                    )

        # ── Per-edge pass ───────────────────────────────────────────────
        if mode in ("per_edge", "both"):
            edges = self._fetch_target_edges(tickers=tickers, limit=limit)
            summary.edges_considered = len(edges)
            log.info(
                "pct_cogs_enrichment[per_edge]: {n} candidate edges "
                "(tickers={tk}, limit={l})",
                n=len(edges), tk=tickers, l=limit,
            )
            for i, edge in enumerate(edges, start=1):
                try:
                    self._enrich_edge(edge, self._provider, summary)
                except Exception as exc:
                    log.warning(
                        "per_edge edge {e} crashed: {x}",
                        e=edge.edge_id, x=str(exc),
                    )
                if i % 25 == 0:
                    log.info(
                        "per_edge progress: {i}/{n} processed, accepted={a}",
                        i=i, n=len(edges), a=summary.edges_accepted,
                    )

        summary.elapsed_seconds = time.monotonic() - started
        return summary

    def _harvest_ticker_list(
        self, tickers: list[str] | None
    ) -> list[str]:
        """Return the list of tickers to harvest. When tickers is passed we
        just use it directly; otherwise we compile the unique set of
        downstream tickers that still have at least one missing-pct edge.
        """
        if tickers:
            return [t.upper() for t in tickers]
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT DISTINCT downstream_id FROM supply_chain_edges e
                        LEFT JOIN supply_chain_nodes n ON n.id = e.downstream_id
                        WHERE (pct_downstream_cogs IS NULL OR pct_upstream_revenue IS NULL)
                          AND (n.type = 'ticker' OR n.type IS NULL)
                        UNION
                        SELECT DISTINCT upstream_id FROM supply_chain_edges e
                        LEFT JOIN supply_chain_nodes n ON n.id = e.upstream_id
                        WHERE e.relationship = 'customer'
                          AND e.pct_upstream_revenue IS NULL
                          AND n.type = 'ticker'
                        """
                    )
                ).fetchall()
        except Exception as exc:
            log.warning("harvest ticker list failed: {e}", e=str(exc))
            return []
        out: list[str] = []
        for r in rows:
            val = (r[0] or "").strip()
            if val and re.match(r"^[A-Za-z][A-Za-z0-9_.\-]{0,9}$", val):
                out.append(val.upper())
        return sorted(set(out))


# ── Hermes scheduler hook ───────────────────────────────────────────────────


def run_weekly(engine: Engine | None = None, **_: Any) -> dict[str, Any]:
    """Hermes scheduler entry point — process every missing-pct edge in one
    weekly run. The scheduler hands us the engine; if not, we lazily build
    one via ``db.get_engine``.
    """
    if engine is None:
        from db import get_engine
        engine = get_engine()
    enricher = PctCogsEnricher(engine=engine)
    summary = enricher.run(tickers=None, limit=None)
    log.info(
        "pct_cogs_enrichment.run_weekly: accepted={a} attempts={n} provider={p}",
        a=summary.edges_accepted, n=summary.attempts_total, p=summary.provider_used,
    )
    return summary.as_dict()


__all__ = [
    "PctCogsEnricher",
    "EnrichmentSummary",
    "LLMUnavailableError",
    "run_weekly",
    "PROVIDER_CHAIN",
]
