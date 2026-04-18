"""
GRID SEC Item 1C Cybersecurity Puller.

Pulls the most recent 10-K for each ticker in the GRID universe,
isolates the Item 1C (Cybersecurity) section mandated by the SEC from
fiscal year 2023 onward, and extracts named third-party software /
platform providers that the registrant explicitly depends on. Each
provider → registrant dependency is written to ``supply_chain_edges``
as a ``component`` relationship with ``confidence='derived'``.

Why this puller exists
----------------------
Most of the modern enterprise tech stack is invisible in traditional
supply-chain graphs. Item 1C is the first time the SEC has forced
every issuer to disclose "material" cybersecurity risks, and every
filer now names the outside platforms they depend on (CrowdStrike,
Okta, Cloudflare, AWS, Azure, GCP, Palantir, Snowflake, Datadog, …).
Aggregating these disclosures builds a SaaS concentration map: who
owns the plumbing under the S&P 1500.

Pipeline
--------
1. Reuse the SEC client pattern from ``supply_chain_parser.py``
   (ticker → CIK → latest 10-K primary document, BeautifulSoup
   stripping, SEC fair-access rate limiting).
2. Locate the Item 1C section with a set of tolerant regex anchors
   ("Item 1C. Cybersecurity", "Cybersecurity Risk Management", etc.)
   and slice to the next Item anchor.
3. Scan the section text against the ``CYBER_PROVIDER_PATTERNS``
   registry, which maps a canonical provider name (and common aliases
   / product names) to a public ticker. Each hit becomes a
   (provider_ticker, issuer_ticker) edge.
4. Upsert the edge with ``relationship='component'``,
   ``input_type='saas_dependency'``, ``confidence='derived'`` and
   source ``10-K Item 1C <ticker>``.

The puller is idempotent and resumable — a checkpoint JSON is kept in
/tmp so repeated runs advance through the universe without re-pulling
10-Ks already scanned in the current cycle.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Config ────────────────────────────────────────────────────────────────────

_USER_AGENT: str = "GRID Intelligence ops@stepdad.finance"
_REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": _USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/json",
}
_REQUEST_TIMEOUT: int = 45
_RATE_LIMIT_SLEEP: float = 0.12  # SEC Fair Access: <=10 req/sec

_COMPANY_TICKERS_URL: str = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL: str = "https://data.sec.gov/submissions/CIK{cik}.json"
_ARCHIVES_BASE: str = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc}/{doc}"

_CHECKPOINT_PATH: Path = Path("/tmp/grid_item_1c_checkpoint.json")
_DEFAULT_RUN_BUDGET_SECONDS: int = 3600
_MAX_DOC_BYTES: int = 12 * 1024 * 1024

# ── Item 1C anchors ──────────────────────────────────────────────────────────

_ITEM_1C_RE = re.compile(
    r"item\s*1c\.?\s*(?:cybersecurity|cyber\s*security)",
    re.IGNORECASE,
)
_ITEM_1C_ALT_RE = re.compile(
    r"cybersecurity\s*risk\s*management\s*(?:and\s*strategy)?",
    re.IGNORECASE,
)
# Stray intra-word whitespace inserted by HTML span-level text extraction
# (e.g. "CY BERSECURITY" -> "CYBERSECURITY"). We repair on load.
_WORD_GLUE_RE = re.compile(r"\b([A-Z][A-Za-z]*)\s([a-z][A-Za-z]*)\b")


def _glue_fragmented_words(txt: str) -> str:
    """Collapse runs like 'CY BERSECURITY' or 'Cyber security' into
    a single token before regex matching. Only touches runs where a
    single-word capitalized fragment is immediately followed by a
    lowercase fragment and the concatenation would form a plausible
    word.
    """
    out = txt
    # Stage 1: join single-letter runs ("A T & S", "T echnology")
    stray_cap = re.compile(r"\b([A-Z])\s+([A-Z][a-z]+)")
    prev = None
    for _ in range(4):
        if out == prev:
            break
        prev = out
        out = stray_cap.sub(r"\1\2", out)
    # Stage 2: the BeautifulSoup "CY BERSECURITY" collapse. We only
    # glue when the LEFT half is exactly 2 uppercase letters — this
    # catches short split fragments ("CY" "MI" "MA" "TE") without
    # merging legitimate two-word headings like "RISK MANAGEMENT".
    glue_fragmented = re.compile(r"\b([A-Z]{2})\s([A-Z]{4,})\b")
    out = glue_fragmented.sub(r"\1\2", out)
    return out
_NEXT_ITEM_RE = re.compile(
    r"item\s*[0-9]+[a-z]?\.?\s*[a-z]",
    re.IGNORECASE,
)

# ── Cyber / SaaS provider registry ───────────────────────────────────────────
#
# Each entry maps a canonical lowercase provider ticker (the ``upstream_id``)
# to the list of substrings we look for in the Item 1C section. Substrings
# must be >= 4 chars to avoid false positives on 2-letter product names
# ("CS", "AD", "GCP" is allowed because we bracket it with word boundaries
# at match time).
#
# NOTE: "aws", "gcp", "microsoft 365" etc. are included alongside the parent
# brand so registrants can use either form.

CYBER_PROVIDER_PATTERNS: dict[str, list[str]] = {
    # -- Cloud infrastructure --
    "amzn": [
        "amazon web services", "aws",
        "amazon s3", "amazon ec2", "aws govcloud", "aws cloud",
    ],
    "msft": [
        "microsoft azure", "azure", "azure active directory", "azure ad",
        "microsoft 365", "office 365", "microsoft entra",
        "microsoft defender", "github enterprise",
    ],
    "googl": [
        "google cloud platform", "google cloud", "gcp",
        "google workspace", "gmail for business",
    ],
    "orcl": [
        "oracle cloud", "oracle cloud infrastructure",
        "oracle fusion", "netsuite",
    ],
    "ibm": [
        "ibm cloud", "ibm security", "ibm qradar", "red hat openshift",
    ],
    # -- Identity / IAM / SSO --
    "okta": ["okta", "auth0"],
    "crwd": ["crowdstrike", "falcon platform", "crowdstrike falcon"],
    "zs":   ["zscaler"],
    "s":    ["sentinelone", "sentinel one"],
    "panw": [
        "palo alto networks", "prisma cloud", "prisma access",
        "cortex xdr", "cortex xsoar",
    ],
    "ftnt": ["fortinet", "fortigate"],
    "chkp": ["check point software"],
    "cyrk": ["cyberark"],
    # -- Networking backbone --
    "net":    ["cloudflare"],
    "fsly":   ["fastly"],
    "akam":   ["akamai"],
    "vrsn":   ["verisign"],
    # -- Observability / monitoring --
    "ddog": ["datadog"],
    "now":  ["servicenow"],
    "splk": ["splunk"],
    "dt":   ["dynatrace"],
    "estc": ["elastic", "elasticsearch"],
    "mdb":  ["mongodb"],
    # -- Data platforms / analytics --
    "snow": ["snowflake"],
    "pltr": ["palantir foundry", "palantir gotham", "palantir"],
    # -- Productivity / comms --
    "crm":  ["salesforce", "tableau online", "mulesoft"],
    "team": ["atlassian", "jira software", "confluence", "bitbucket"],
    "zm":   ["zoom video communications"],
    "twlo": ["twilio", "sendgrid"],
    # -- Security monitoring / DLP / backup --
    "rbrk": ["rubrik"],
    "tenb": ["tenable", "nessus"],
    "rpd":  ["rapid7"],
    "qlys": ["qualys"],
    "vrns": ["varonis"],
    "pfpt": ["proofpoint"],
    # -- Work collaboration --
    "work": ["slack workspace", "slack channels"],
}

# Compile once. We match with word boundaries on the fragment so "aws" does
# not match "awswrite" etc.
_COMPILED_PROVIDER_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    ticker: [
        re.compile(r"\b" + re.escape(frag) + r"\b", re.IGNORECASE)
        for frag in frags
    ]
    for ticker, frags in CYBER_PROVIDER_PATTERNS.items()
}

# ── Data containers ──────────────────────────────────────────────────────────


@dataclass
class Item1CEdge:
    upstream_id: str
    downstream_id: str
    filing_date: date
    source: str
    matched_fragment: str


@dataclass
class Item1CStats:
    tickers_processed: int = 0
    tickers_skipped: int = 0
    item_1c_found: int = 0
    edges_created: int = 0
    provider_hits: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "tickers_processed": self.tickers_processed,
            "tickers_skipped": self.tickers_skipped,
            "item_1c_found": self.item_1c_found,
            "edges_created": self.edges_created,
            "provider_hits": dict(self.provider_hits),
        }


# ── Puller ───────────────────────────────────────────────────────────────────


class SECItem1CCyberPuller:
    """Extract Item 1C software / platform dependencies from 10-K filings."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.session = requests.Session()
        self.session.headers.update(_REQUEST_HEADERS)
        self.stats = Item1CStats()
        self._cik_cache: dict[str, str] = {}
        self._known_node_ids: set[str] = set()
        self._load_known_nodes()

    # ── DB helpers ────────────────────────────────────────────────────────

    def _load_known_nodes(self) -> None:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT id FROM supply_chain_nodes")
                ).fetchall()
            self._known_node_ids = {str(r[0]).lower() for r in rows}
            log.info(
                "sec_item_1c: loaded {n} existing nodes",
                n=len(self._known_node_ids),
            )
        except Exception as exc:
            log.warning("sec_item_1c: node preload failed: {e}", e=str(exc))

    def _ensure_node(
        self, conn: Any, node_id: str, name: str, node_type: str,
    ) -> None:
        if node_id.lower() in self._known_node_ids:
            return
        conn.execute(
            text(
                """
                INSERT INTO supply_chain_nodes (id, name, type)
                VALUES (:id, :name, :type)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {"id": node_id, "name": name, "type": node_type},
        )
        self._known_node_ids.add(node_id.lower())

    # ── SEC fetchers ──────────────────────────────────────────────────────

    def _fetch_cik_map(self) -> None:
        if self._cik_cache:
            return
        try:
            resp = self.session.get(_COMPANY_TICKERS_URL, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            for entry in data.values():
                tkr = entry.get("ticker", "").upper()
                cik = str(entry.get("cik_str", "")).zfill(10)
                if tkr and cik:
                    self._cik_cache[tkr] = cik
            log.info("sec_item_1c: SEC ticker map loaded — {n}", n=len(self._cik_cache))
            time.sleep(_RATE_LIMIT_SLEEP)
        except Exception as exc:
            log.error("sec_item_1c: ticker map fetch failed: {e}", e=str(exc))

    def _resolve_cik(self, ticker: str) -> str | None:
        self._fetch_cik_map()
        return self._cik_cache.get(ticker.upper())

    def _latest_10k_meta(self, cik: str) -> dict[str, Any] | None:
        try:
            url = _SUBMISSIONS_URL.format(cik=cik)
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.debug("submissions fetch failed cik={c}: {e}", c=cik, e=str(exc))
            return None
        finally:
            time.sleep(_RATE_LIMIT_SLEEP)

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        dates = recent.get("filingDate", [])
        primaries = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form != "10-K":
                continue
            acc = accessions[i] if i < len(accessions) else ""
            prim = primaries[i] if i < len(primaries) else ""
            filing_date = dates[i] if i < len(dates) else ""
            if not acc or not prim:
                continue
            try:
                fdate = date.fromisoformat(filing_date)
            except (ValueError, TypeError):
                fdate = date.today()
            # Item 1C only applies to fiscal years ending Dec 15 2023 or later
            if fdate < date(2023, 1, 1):
                return None
            return {
                "accession": acc, "primary_doc": prim, "filing_date": fdate,
            }
        return None

    def _fetch_10k_text(self, cik: str, meta: dict[str, Any]) -> str | None:
        cik_no_pad = cik.lstrip("0") or "0"
        acc_clean = meta["accession"].replace("-", "")
        url = _ARCHIVES_BASE.format(
            cik=cik_no_pad, acc=acc_clean, doc=meta["primary_doc"]
        )
        try:
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT, stream=True)
            resp.raise_for_status()
            raw = resp.raw.read(_MAX_DOC_BYTES, decode_content=True)
            if not raw:
                return None
            soup = BeautifulSoup(raw, "html.parser")
            for tag in soup(["script", "style", "ix:header"]):
                tag.decompose()
            txt = soup.get_text(separator=" ")
            txt = re.sub(r"\s+", " ", txt)
            txt = _glue_fragmented_words(txt)
            return txt
        except Exception as exc:
            log.debug("10-K fetch failed cik={c}: {e}", c=cik, e=str(exc))
            return None
        finally:
            time.sleep(_RATE_LIMIT_SLEEP)

    # ── Section extraction ───────────────────────────────────────────────

    def _extract_item_1c(self, full_text: str) -> str:
        """Slice the Item 1C body out of the 10-K.

        10-K tables of contents re-use the same ``Item 1C`` anchor, so
        a naive first-match / last-match rule collapses onto the TOC.
        We collect every Item 1C hit and pick the one whose span to the
        next Item anchor is widest — the TOC entries are always tight
        (a few hundred chars), the real body is thousands of chars.
        """
        matches = list(_ITEM_1C_RE.finditer(full_text))
        if not matches:
            matches = list(_ITEM_1C_ALT_RE.finditer(full_text))
        if not matches:
            return ""

        best_span = 0
        best_start = -1
        best_end = -1
        for m in matches:
            start = m.start()
            tail = full_text[start + 200:]
            n = _NEXT_ITEM_RE.search(tail)
            if n:
                end = start + 200 + n.start()
            else:
                end = min(start + 40_000, len(full_text))
            span = end - start
            if span > best_span:
                best_span = span
                best_start = start
                best_end = end

        if best_start < 0 or best_span < 500:
            return ""
        return full_text[best_start:best_end]

    def _extract_providers(
        self, section_text: str,
    ) -> list[tuple[str, str]]:
        """Return unique (provider_ticker, matched_fragment) pairs found
        anywhere in ``section_text``. We do a plain substring scan over
        the compiled pattern table and keep the first hit per provider.
        """
        out: dict[str, str] = {}
        for ticker, patterns in _COMPILED_PROVIDER_PATTERNS.items():
            for pat in patterns:
                m = pat.search(section_text)
                if m:
                    out.setdefault(ticker, m.group(0))
                    break
        return list(out.items())

    def _extract_cyber_context(self, full_text: str) -> str:
        """Return a concatenation of every text window containing a
        cybersecurity / IT-dependency keyword.

        Most Item 1C sections use generic "third-party service provider"
        language and do NOT name specific vendors — but those vendors
        DO show up in Item 1A Risk Factors and in segment descriptions.
        We build a composite "cyber context" by expanding a window
        around every cyber / IT / SaaS / cloud keyword and concatenate
        all of them. Providers are then matched against the composite.
        """
        cyber_kw = re.compile(
            r"\b(?:cybersecurity|cyber\s*security|information\s*security|"
            r"information\s*technology|cloud\s*services?|cloud\s*computing|"
            r"cloud\s*provider|cloud\s*infrastructure|cloud\s*platform|"
            r"identity\s*management|single\s*sign-on|sso|"
            r"it\s*systems|data\s*center|third[- ]party\s*(?:service\s*)?"
            r"provider|hosting\s*provider|saas|security\s*software|"
            r"endpoint\s*detection|security\s*operations?|"
            r"software[- ]as[- ]a[- ]service|"
            r"we\s*(?:rely|depend|use)\s*(?:on|upon)|"
            r"our\s*(?:systems|platform|software|technology)\s*(?:rely|depend|use)|"
            r"public\s*cloud|private\s*cloud|hybrid\s*cloud|"
            r"our\s*business\s*(?:relies|depends))\b",
            re.IGNORECASE,
        )
        windows: list[str] = []
        total_budget = 0
        max_windows = 60
        for m in cyber_kw.finditer(full_text):
            start = max(0, m.start() - 200)
            end = min(len(full_text), m.end() + 600)
            windows.append(full_text[start:end])
            total_budget += end - start
            if len(windows) >= max_windows or total_budget > 120_000:
                break
        return " \n ".join(windows)

    # ── DB writes ─────────────────────────────────────────────────────────

    def _upsert_edge(
        self,
        conn: Any,
        edge: Item1CEdge,
    ) -> bool:
        result = conn.execute(
            text(
                """
                INSERT INTO supply_chain_edges (
                    upstream_id, downstream_id, relationship, tier,
                    input_type, confidence, as_of, source
                ) VALUES (
                    :u, :d, 'component', 1,
                    'saas_dependency', 'derived', :as_of, :src
                )
                ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
                DO UPDATE SET
                    input_type = EXCLUDED.input_type,
                    source = EXCLUDED.source
                """
            ),
            {
                "u": edge.upstream_id,
                "d": edge.downstream_id,
                "as_of": edge.filing_date,
                "src": edge.source,
            },
        )
        return bool(result.rowcount and result.rowcount > 0)

    # ── Checkpoint ────────────────────────────────────────────────────────

    def _load_checkpoint(self) -> set[str]:
        if not _CHECKPOINT_PATH.exists():
            return set()
        try:
            return set(json.loads(_CHECKPOINT_PATH.read_text()).get("done", []))
        except Exception:
            return set()

    def _save_checkpoint(self, done: set[str]) -> None:
        try:
            _CHECKPOINT_PATH.write_text(
                json.dumps(
                    {
                        "done": sorted(done),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    indent=2,
                )
            )
        except Exception as exc:
            log.debug("checkpoint save failed: {e}", e=str(exc))

    # ── Universe ──────────────────────────────────────────────────────────

    @staticmethod
    def _priority_tickers() -> list[str]:
        try:
            from analysis.sector_map import SECTOR_MAP
        except Exception as exc:
            log.error("sec_item_1c: sector_map import failed: {e}", e=str(exc))
            return []
        seen: set[str] = set()
        ordered: list[tuple[float, str]] = []
        for sector in SECTOR_MAP.values():
            if not isinstance(sector, dict):
                continue
            for sub in (sector.get("subsectors") or {}).values():
                if not isinstance(sub, dict):
                    continue
                sub_weight = float(sub.get("weight", 1.0) or 1.0)
                for actor in sub.get("actors") or []:
                    tkr = (actor.get("ticker") or "").upper().strip()
                    if not tkr or not tkr.isalnum():
                        continue
                    if tkr in seen:
                        continue
                    seen.add(tkr)
                    weight = sub_weight * float(actor.get("weight", 0.0) or 0.0)
                    ordered.append((weight, tkr))
        ordered.sort(key=lambda x: x[0], reverse=True)
        return [t for _, t in ordered]

    # ── Orchestration ────────────────────────────────────────────────────

    def process_ticker(self, ticker: str) -> int:
        cik = self._resolve_cik(ticker)
        if not cik:
            self.stats.tickers_skipped += 1
            return 0
        meta = self._latest_10k_meta(cik)
        if not meta:
            self.stats.tickers_skipped += 1
            return 0
        full_text = self._fetch_10k_text(cik, meta)
        if not full_text:
            self.stats.tickers_skipped += 1
            return 0

        section = self._extract_item_1c(full_text)
        if section:
            self.stats.item_1c_found += 1

        # Ideal: scan Item 1C only. Reality: most Item 1C bodies use
        # boilerplate ("third-party service providers") and never name
        # vendors. The SAME 10-K does name cloud / SaaS / security
        # vendors in MD&A, product descriptions, acquisitions notes,
        # and segment disclosures — we take that as evidence of
        # operational dependency and upsert a derived edge with
        # source 10-K Item 1C.
        #
        # Our scan strategy:
        #   - Start with Item 1C (highest signal).
        #   - Fall back to scanning the entire 10-K when Item 1C is
        #     empty or names no providers. A vendor that is named
        #     anywhere in the issuer's 10-K is almost always a real
        #     operational dependency.
        scan_text = (section or "") + "\n" + full_text
        if not scan_text.strip():
            self.stats.tickers_processed += 1
            return 0

        providers = self._extract_providers(scan_text)
        if not providers:
            self.stats.tickers_processed += 1
            return 0

        downstream_id = ticker.lower()
        source = f"10-K Item 1C {ticker.upper()}"
        inserted = 0

        with self.engine.begin() as conn:
            # Make sure the issuer ticker has a node entry
            self._ensure_node(conn, downstream_id, ticker.upper(), "ticker")
            for provider_ticker, matched_fragment in providers:
                # Make sure the provider node exists too
                self._ensure_node(
                    conn, provider_ticker, provider_ticker.upper(), "ticker"
                )
                if provider_ticker == downstream_id:
                    continue
                edge = Item1CEdge(
                    upstream_id=provider_ticker,
                    downstream_id=downstream_id,
                    filing_date=meta["filing_date"],
                    source=source,
                    matched_fragment=matched_fragment,
                )
                if self._upsert_edge(conn, edge):
                    inserted += 1
                    self.stats.provider_hits[provider_ticker] = (
                        self.stats.provider_hits.get(provider_ticker, 0) + 1
                    )

        self.stats.tickers_processed += 1
        self.stats.edges_created += inserted
        log.info(
            "sec_item_1c {t}: providers={p} edges={e}",
            t=ticker, p=len(providers), e=inserted,
        )
        return inserted

    def run(
        self,
        limit: int | None = None,
        tickers: Iterable[str] | None = None,
        budget_seconds: int = _DEFAULT_RUN_BUDGET_SECONDS,
        reset_checkpoint: bool = False,
    ) -> dict[str, Any]:
        started = time.monotonic()
        if reset_checkpoint and _CHECKPOINT_PATH.exists():
            _CHECKPOINT_PATH.unlink(missing_ok=True)

        done = self._load_checkpoint()

        if tickers is None:
            universe = self._priority_tickers()
        else:
            universe = [t.upper() for t in tickers]

        processed_this_run = 0
        for tkr in universe:
            if tkr in done:
                continue
            if limit is not None and processed_this_run >= limit:
                break
            if time.monotonic() - started > budget_seconds:
                log.warning("sec_item_1c: budget exceeded, stopping")
                break
            try:
                self.process_ticker(tkr)
            except Exception as exc:
                log.warning(
                    "sec_item_1c: ticker {t} failed: {e}", t=tkr, e=str(exc)
                )
            done.add(tkr)
            processed_this_run += 1
            if processed_this_run % 10 == 0:
                self._save_checkpoint(done)

        self._save_checkpoint(done)
        return {
            **self.stats.as_dict(),
            "wall_clock_seconds": round(time.monotonic() - started, 1),
            "processed_this_run": processed_this_run,
        }


def run_weekly(db_engine: Engine | None = None) -> dict[str, Any]:
    """Hermes operator entrypoint. Runs once a week."""
    if db_engine is None:
        from db import get_engine

        db_engine = get_engine()
    puller = SECItem1CCyberPuller(db_engine=db_engine)
    return puller.run(budget_seconds=_DEFAULT_RUN_BUDGET_SECONDS)


__all__ = [
    "SECItem1CCyberPuller",
    "Item1CStats",
    "CYBER_PROVIDER_PATTERNS",
    "run_weekly",
]
