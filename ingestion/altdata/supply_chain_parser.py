"""
GRID 10-K Supply Chain Parser.

Pulls the most recent 10-K filing for every US-listed ticker in the
GRID sector universe, extracts Item 1 (Business) and Item 1A (Risk
Factors), and mines the text for explicit supplier + customer
relationships. Each discovered relationship is written to
``supply_chain_edges`` with ``confidence='derived'``.

The module does NOT touch the hand-curated seed. Newly discovered
suppliers/customers are added to ``supply_chain_nodes`` with
``INSERT ... ON CONFLICT DO NOTHING`` so the seed labels remain
authoritative.

Pipeline for each ticker:

  1. Resolve CIK via the SEC company_tickers.json cache.
  2. Pull ``data.sec.gov/submissions/CIK{cik}.json`` and locate the
     most recent 10-K entry (latest accession + primary document).
  3. Fetch the primary document from the SEC Archives and run it
     through BeautifulSoup to strip XBRL inline-tag noise.
  4. Extract the Item 1 and Item 1A sections via regex anchors.
  5. Apply a portfolio of regex patterns tuned to the canonical
     10-K language for supplier/customer concentration.
  6. For every (supplier, buyer, relationship) triple, resolve the
     counterparty to an existing node id or slug a new one.
  7. Upsert the edge into ``supply_chain_edges`` and any new node
     into ``supply_chain_nodes`` using parameterized SQL.

Checkpointing: the parser writes ``/tmp/grid_10k_parser_checkpoint.json``
so it can be resumed across runs. The total wall-clock budget is
capped at 1 hour per invocation.

Rate limit: the SEC Fair Access guideline caps traffic at 10 req/sec.
We sleep ~120 ms between requests and send a descriptive User-Agent.
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

_CHECKPOINT_PATH: Path = Path("/tmp/grid_10k_parser_checkpoint.json")
_DEFAULT_RUN_BUDGET_SECONDS: int = 3600  # 1 hour hard cap
_MAX_DOC_BYTES: int = 12 * 1024 * 1024   # 12 MB cap per 10-K doc

# Relationship vocabulary allowed by api/routers/supply_chain.py
_VALID_RELATIONSHIPS: frozenset[str] = frozenset(
    {
        "raw_material",
        "component",
        "contract_mfg",
        "distribution",
        "customer",
        "licensor",
        "franchisee",
    }
)

# Tokens we never want to treat as an entity name (regex noise).
_STOPWORD_NAMES: frozenset[str] = frozenset(
    {
        "the",
        "our",
        "we",
        "us",
        "their",
        "its",
        "this",
        "these",
        "those",
        "company",
        "customers",
        "customer",
        "supplier",
        "suppliers",
        "distributor",
        "distributors",
        "products",
        "services",
        "operations",
        "revenue",
        "sales",
        "cost",
        "costs",
        "united states",
        "north america",
        "international",
        "government",
        "u.s. government",
        "us government",
        "americas",
        "europe",
        "asia",
        "asia pacific",
        "emea",
        "apac",
        "latin america",
        "middle east",
        "china",
        "china mainland",
        "india",
        "japan",
        "taiwan",
        "germany",
        "france",
        "brazil",
        "canada",
        "mexico",
        "united kingdom",
        "u.k.",
        "uk",
        "alabama", "alaska", "arizona", "arkansas", "california",
        "colorado", "connecticut", "delaware", "florida", "georgia",
        "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas",
        "kentucky", "louisiana", "maine", "maryland", "massachusetts",
        "michigan", "minnesota", "mississippi", "missouri", "montana",
        "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
        "new york", "north carolina", "north dakota", "ohio", "oklahoma",
        "oregon", "pennsylvania", "rhode island", "south carolina",
        "south dakota", "tennessee", "texas", "utah", "vermont",
        "virginia", "washington", "west virginia", "wisconsin", "wyoming",
        "january", "february", "march", "april", "may", "june", "july",
        "august", "september", "october", "november", "december",
        "fiscal", "annual", "quarter", "year",
        "third parties", "third party", "third-party", "third-parties",
        "other customers", "other suppliers", "various customers",
        "certain customers", "certain suppliers", "our products",
        "our services", "our operations", "our business", "our employees",
    }
)

# ── Sections ──────────────────────────────────────────────────────────────────

# Item 1 + Item 1A anchors. 10-K formatting is wildly inconsistent; we look
# for the word "item" followed by 1/1A and a business / risk-factor heading,
# then slice to the next "item" anchor.
_ITEM_1_RE = re.compile(
    r"item\s*1\.?\s*(?:business|\(business\))",
    re.IGNORECASE,
)
_ITEM_1A_RE = re.compile(
    r"item\s*1a\.?\s*(?:risk\s*factors)",
    re.IGNORECASE,
)
# Concentration of credit/customer risk — usually lives in Notes to FS.
_CONCENTRATION_RE = re.compile(
    r"concentration\s*of\s*(?:credit|customer|customers|revenue|revenues|sales)\s*risk",
    re.IGNORECASE,
)
# Principal Customers sub-heading inside Item 1 Business
_PRINCIPAL_CUST_RE = re.compile(
    r"principal\s*customers(?:\s*and\s*marketing\s*strategy)?",
    re.IGNORECASE,
)
_NEXT_ITEM_RE = re.compile(
    r"item\s*[0-9]+[a-z]?\.?\s*[a-z]",
    re.IGNORECASE,
)

# ── Regex patterns ────────────────────────────────────────────────────────────

# Name fragment: capitalized multi-word entity. Case-sensitive even when the
# surrounding pattern is IGNORECASE (we use (?-i:...)) so we only capture
# proper nouns. Greedy within limits so we capture "McLane Company, Inc."
# instead of "McL". Optionally absorbs common legal suffixes.
_NAME_FRAG = (
    r"(?-i:"
    r"[A-Z][A-Za-z0-9&'\-\.]+"
    r"(?:\s+(?:&\s+|of\s+|and\s+|de\s+|the\s+)?"
    r"[A-Z][A-Za-z0-9&'\-\.]+)*"
    r"(?:,?\s+(?:Inc|Inc\.|LLC|L\.L\.C\.|Ltd|Ltd\.|Limited|Corp|Corp\.|"
    r"Corporation|Company|Co|Co\.|Holdings|Group|plc|PLC|AG|SA|S\.A\.|"
    r"NV|N\.V\.|GmbH|KG|Partners|Technologies|International))*"
    r")"
)

# Pattern A1: "N% of our [net] sales/revenue were derived/generated from X"
#  and variants with "made to", "to X", "attributable to X".
_PAT_CUSTOMER_PCT = re.compile(
    r"(?:approximately|about|roughly)?\s*"
    r"(?P<pct>\d{1,2}(?:\.\d+)?)\s*%\s*"
    r"of\s*(?:our|the\s*company[\'']?s|total|consolidated)?\s*"
    r"(?:net\s*|total\s*|consolidated\s*)*"
    r"(?:revenue|revenues|sales|net\s*sales|consolidated\s*net\s*sales)\s*"
    r"(?:were|was|is|are)?\s*"
    r"(?:derived|generated|attributable|made|earned|recognized)?\s*"
    r"(?:from|to)\s*"
    r"(?P<name>" + _NAME_FRAG + r")",
    re.IGNORECASE,
)

# Pattern A2: "Sales to X (and its affiliates) represent[ed] approximately N%
#              of our [total/net] sales/revenue"
#   — PG 10-K says this about Walmart. Very common. Requires "to" (not
#   "with") so we don't false-match "sales with [product]".
_PAT_SALES_TO_CUSTOMER = re.compile(
    r"(?:[Nn]et\s+)?[Ss]ales\s+to\s+"
    r"(?P<name>" + _NAME_FRAG + r")"
    r"(?:\s+(?:and\s+its\s+affiliates|and\s+its\s+subsidiaries))?\s+"
    r"(?:represent|represented|accounted\s+for|account\s+for|were)\s+"
    r"(?:approximately|about|roughly)?\s*"
    r"(?P<pct>\d{1,2}(?:\.\d+)?)\s*%",
)

# Pattern A3: "X accounted for N% of our [net] sales/revenue"
#   — HSY 10-K: "McLane Company accounted for approximately 27%..."
_PAT_NAME_ACCOUNTED = re.compile(
    r"(?P<name>" + _NAME_FRAG + r")"
    r"\s*(?:and\s*its\s*(?:affiliates|subsidiaries)\s*)?"
    r"(?:accounted\s*for|represented|comprised|made\s*up)\s*"
    r"(?:approximately|about|roughly)?\s*"
    r"(?P<pct>\d{1,2}(?:\.\d+)?)\s*%\s*"
    r"of\s*(?:our|the\s*company[\'']?s|total|consolidated)?\s*"
    r"(?:net\s*|total\s*|consolidated\s*)*"
    r"(?:revenue|revenues|sales|net\s*sales)",
    re.IGNORECASE,
)

# Pattern A4: "N% of our [net] sales were made to X"
_PAT_SALES_MADE_TO = re.compile(
    r"(?P<pct>\d{1,2}(?:\.\d+)?)\s*%\s*"
    r"of\s*(?:our|the\s*company[\'']?s|total|consolidated)?\s*"
    r"(?:net\s*|total\s*|consolidated\s*)*"
    r"(?:net\s*)?(?:revenue|revenues|sales|net\s*sales)\s*"
    r"(?:were|was|is|are)?\s*"
    r"(?:made|attributable|recognized|generated|earned)\s*to\s*"
    r"(?P<name>" + _NAME_FRAG + r")",
    re.IGNORECASE,
)

# Pattern B: supplier sole-source language.
#   "we source X primarily/principally/exclusively/solely from Y"
# The qualifier is required — otherwise the pattern fires on any geographic
# sourcing mention ('we purchase cocoa beans from Ghana').
_PAT_SUPPLIER_SOURCE = re.compile(
    r"(?:we|the\s*company)\s+"
    r"(?:source|sources|purchase|purchases|obtain|obtains|procure|procures)\s+"
    r"(?:substantially\s+all\s+of\s+)?"
    r"(?:our|its|their)\s+"
    r"(?P<input>[a-z][a-z0-9\- ,]{3,60}?)\s+"
    r"(?:primarily|principally|exclusively|solely)\s+from\s+"
    r"(?P<name>" + _NAME_FRAG + r")",
    re.IGNORECASE,
)

# Pattern C: "X supplies our Y" / "X manufactures our Z".
# Requires a strong anchor: the verb must be followed by a possessive
# ("our" / "the company's") to avoid matching business-segment descriptions
# ("Optum Rx provides pharmacy care").
_PAT_SUPPLIER_VERB = re.compile(
    r"(?P<name>" + _NAME_FRAG + r")\s+"
    r"(?:supplies|manufactures\s+for|assembles\s+for|co-?manufactures)\s+"
    r"(?:our|the\s*company[\'']?s|a\s*substantial\s*portion\s*of\s*our)\s+"
    r"(?P<input>[a-z][a-z0-9\- ,]{3,60})",
    re.IGNORECASE,
)

# Pattern D: "Principal customers include A, B, C and D"
# Requires a strong verb anchor (include/consist of/:) and excludes
# vague verbs like "are" that produce false positives in run-on sentences.
_PAT_PRINCIPAL_LIST = re.compile(
    r"(?P<role>Principal|Major|Key|Significant|Largest)\s+"
    r"(?P<kind>[Cc]ustomers|[Ss]uppliers|[Dd]istributors)\s+"
    r"(?:include|consist\s+of|are\s*:)\s+"
    r"(?P<names>[A-Z][A-Za-z0-9&\.,\-\' ]{5,400})",
)

# Pattern E: contract manufacturing / assembly
#   "our products are assembled by Foxconn and Pegatron"
_PAT_CONTRACT_MFG = re.compile(
    r"(?:our\s*products|our\s*devices|the\s*(?:iphone|ipad|mac|watch)s?)\s*"
    r"(?:are|is)\s*(?:assembled|manufactured|produced)\s*(?:by|at)\s*"
    r"(?P<name>" + _NAME_FRAG + r")",
    re.IGNORECASE,
)

# Pattern F: "rely on X" single-supplier language. Must be followed by a
# supply verb to avoid matching customer or financial reliance.
_PAT_RELY_ON = re.compile(
    r"(?:rely|depend|dependence|reliance)\s+(?:on|upon)\s+"
    r"(?P<name>" + _NAME_FRAG + r")\s+"
    r"(?:to\s+(?:supply|provide|manufacture|produce|fabricate)|"
    r"for\s+(?:the\s+)?(?:supply|manufacture|production|fabrication))",
    re.IGNORECASE,
)

# ── Data containers ───────────────────────────────────────────────────────────


@dataclass
class DerivedEdge:
    upstream_id: str
    downstream_id: str
    relationship: str
    confidence: str = "derived"
    source: str = ""
    as_of: date | None = None
    input_type: str | None = None
    pct_upstream_revenue: float | None = None
    pct_downstream_cogs: float | None = None
    raw_name: str = ""


@dataclass
class DerivedNode:
    id: str
    name: str
    type: str = "private_company"
    country: str | None = None
    notes: str | None = None


@dataclass
class ParserStats:
    tickers_processed: int = 0
    tickers_skipped: int = 0
    filings_fetched: int = 0
    filings_failed: int = 0
    edges_created: int = 0
    nodes_created: int = 0
    pattern_hits: dict[str, int] = field(default_factory=dict)

    def bump(self, key: str, amount: int = 1) -> None:
        self.pattern_hits[key] = self.pattern_hits.get(key, 0) + amount


# ── Helpers ───────────────────────────────────────────────────────────────────


def _slug(name: str) -> str:
    """Lowercase slug matching the seed convention."""
    s = re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")
    return s


def _clean_name(raw: str) -> str:
    name = raw.strip()
    name = re.sub(r"[\s]+", " ", name)
    # Strip leading/trailing punctuation that the regex might have grabbed
    name = name.strip(" .,;:()[]\"'")
    # Drop an inclusive trailing "and"
    name = re.sub(r"\s+and\s*$", "", name, flags=re.IGNORECASE)
    # Drop common trailing qualifiers that inflate node-id duplicates
    name = re.sub(
        r"\s+(?:and\s+(?:its|their)\s+(?:affiliates|subsidiaries|"
        r"operating\s+subsidiaries|related\s+entities)|"
        r"together\s+with\s+its\s+affiliates)\s*$",
        "",
        name,
        flags=re.IGNORECASE,
    )
    return name


def _is_noise_name(name: str) -> bool:
    n = name.lower().strip()
    if len(n) < 3 or len(n) > 80:
        return True
    if n in _STOPWORD_NAMES:
        return True
    if not re.search(r"[A-Za-z]", name):
        return True
    # Must start with a capital letter to avoid grabbing mid-sentence fragments
    if not name[:1].isupper():
        return True
    # Reject names that are entirely legal-entity boilerplate / short stopwords
    # like "Inc", "Company", etc. after cleanup.
    first_token = n.split()[0] if n.split() else ""
    if first_token in {
        "inc", "co", "corp", "ltd", "llc", "group", "holdings",
        "company", "corporation", "the", "our", "this", "these",
        "is", "was", "were", "which", "that", "who", "their", "its",
    }:
        return True
    return False


def _infer_relationship(
    context: str,
    input_hint: str | None,
    pattern_name: str,
) -> str:
    c = (context + " " + (input_hint or "")).lower()
    if pattern_name == "customer_pct" or pattern_name == "principal_customers":
        return "customer"
    if pattern_name == "contract_mfg":
        return "contract_mfg"
    if pattern_name == "principal_distributors":
        return "distribution"
    if "distributor" in c or "distribution" in c:
        return "distribution"
    if "manufactur" in c or "assembl" in c or "foundry" in c or "fab" in c:
        return "contract_mfg"
    if "raw material" in c or "commodit" in c or "metal" in c or "mineral" in c:
        return "raw_material"
    if "licens" in c:
        return "licensor"
    if "franchis" in c:
        return "franchisee"
    return "component"


def _split_list(raw: str) -> list[str]:
    """Split a 'A, B, C and D' list into individual names."""
    # Cut at sentence end if one slipped through
    raw = re.split(r"\.\s|;", raw, maxsplit=1)[0]
    parts = re.split(r",\s*|\s+and\s+|\s*&\s*", raw)
    return [p for p in (_clean_name(p) for p in parts) if p]


# ── Parser ────────────────────────────────────────────────────────────────────


class SupplyChain10KParser:
    """Parse the latest 10-K for each ticker and write derived edges.

    Attributes:
        engine: SQLAlchemy engine for writes to supply_chain_nodes/edges.
        session: requests.Session pre-configured with SEC User-Agent.
        stats: ParserStats with per-run counts.
        seed_node_ids: Set of lowercase ids from supply_chain_nodes (loaded once).
        name_to_node_id: Case-insensitive name lookup built from the DB.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.session = requests.Session()
        self.session.headers.update(_REQUEST_HEADERS)
        self.stats = ParserStats()
        self.seed_node_ids: set[str] = set()
        self.name_to_node_id: dict[str, str] = {}
        self._cik_cache: dict[str, str] = {}
        self._ticker_to_node: dict[str, str] = {}
        self._load_node_catalog()

    # ── Node catalog ──────────────────────────────────────────────────────

    def _load_node_catalog(self) -> None:
        """Load existing supply_chain_nodes to resolve counterparties."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("SELECT id, name, type FROM supply_chain_nodes")
                ).fetchall()
            for node_id, node_name, node_type in rows:
                nid = str(node_id).lower()
                self.seed_node_ids.add(nid)
                if node_name:
                    self.name_to_node_id[node_name.lower().strip()] = nid
                # Handle ticker-type nodes — id is already the lowercase ticker
                if node_type == "ticker":
                    self._ticker_to_node[nid.upper()] = nid
            log.info(
                "SupplyChain10KParser: loaded {n} existing nodes",
                n=len(self.seed_node_ids),
            )
        except Exception as exc:
            log.warning(
                "Could not preload node catalog: {e}", e=str(exc)
            )

    # ── SEC fetchers ──────────────────────────────────────────────────────

    def _fetch_cik_map(self) -> None:
        """Populate self._cik_cache with the full SEC ticker→CIK map."""
        if self._cik_cache:
            return
        try:
            resp = self.session.get(
                _COMPANY_TICKERS_URL, timeout=_REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()
            for entry in data.values():
                tkr = entry.get("ticker", "").upper()
                cik = str(entry.get("cik_str", "")).zfill(10)
                if tkr and cik:
                    self._cik_cache[tkr] = cik
            log.info(
                "SEC ticker map loaded: {n} tickers", n=len(self._cik_cache)
            )
            time.sleep(_RATE_LIMIT_SLEEP)
        except Exception as exc:
            log.error("SEC ticker map fetch failed: {e}", e=str(exc))

    def _resolve_cik(self, ticker: str) -> str | None:
        self._fetch_cik_map()
        return self._cik_cache.get(ticker.upper())

    def _latest_10k_meta(self, cik: str) -> dict[str, Any] | None:
        """Return {accession, primary_doc, filing_date} for the latest 10-K."""
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
            return {
                "accession": acc,
                "primary_doc": prim,
                "filing_date": fdate,
            }
        return None

    def _fetch_10k_text(self, cik: str, meta: dict[str, Any]) -> str | None:
        """Fetch and BeautifulSoup-strip a 10-K primary document."""
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
            # Drop XBRL inline tags, scripts, styles
            for tag in soup(["script", "style", "ix:header"]):
                tag.decompose()
            text_only = soup.get_text(separator=" ")
            # Collapse whitespace
            text_only = re.sub(r"\s+", " ", text_only)
            return text_only
        except Exception as exc:
            log.debug(
                "10-K fetch failed cik={c}: {e}", c=cik, e=str(exc)
            )
            return None
        finally:
            time.sleep(_RATE_LIMIT_SLEEP)

    # ── Section extraction ───────────────────────────────────────────────

    def _extract_concentration_notes(self, full_text: str) -> str:
        """Collect every 'concentration of credit/customer risk' blurb.

        These typically live in Notes to the Financial Statements and contain
        explicit customer-percent language ('X accounted for 12% ...'). We
        capture a 1500-char window around each hit and concatenate them.
        """
        out: list[str] = []
        for m in _CONCENTRATION_RE.finditer(full_text):
            start = max(0, m.start() - 200)
            end = min(len(full_text), m.end() + 1800)
            out.append(full_text[start:end])
            if len(out) >= 6:
                break
        return " ".join(out)

    def _extract_section(
        self, full_text: str, start_re: re.Pattern[str]
    ) -> str:
        """Slice the body of a 10-K section.

        10-Ks begin with a table of contents where the same 'Item 1. Business'
        anchor appears as a TOC entry. We pick the *last* occurrence (which is
        the actual body) and then walk forward to the next item anchor, but we
        require the next-anchor to appear at least 1000 chars past the start
        so we don't stop at a sub-heading.
        """
        matches = list(start_re.finditer(full_text))
        if not matches:
            return ""
        # Use the last occurrence — skips the TOC.
        m = matches[-1]
        start = m.start()
        tail = full_text[start + 1000 :]
        n = _NEXT_ITEM_RE.search(tail)
        end = start + 1000 + n.start() if n else min(start + 120_000, len(full_text))
        return full_text[start:end]

    # ── Entity resolution ────────────────────────────────────────────────

    def _resolve_counterparty(
        self, raw_name: str
    ) -> tuple[str, DerivedNode | None]:
        """Resolve a raw entity name to a node id. Returns (id, optional new node)."""
        cleaned = _clean_name(raw_name)
        if _is_noise_name(cleaned):
            return "", None

        lower = cleaned.lower()
        # Direct lookup by name
        if lower in self.name_to_node_id:
            return self.name_to_node_id[lower], None
        # Ticker lookup — rare but worth checking
        tkr_guess = cleaned.upper()
        if tkr_guess in self._ticker_to_node:
            return self._ticker_to_node[tkr_guess], None

        # Common short-form fuzzy rules
        aliases = {
            "taiwan semiconductor": "tsmc",
            "taiwan semiconductor manufacturing": "tsmc",
            "berkshire": "brk_b",
            "berkshire hathaway": "brk_b",
            "foxconn": "foxconn",
            "hon hai": "foxconn",
            "hon hai precision": "foxconn",
        }
        for alias, node_id in aliases.items():
            if alias in lower and node_id in self.seed_node_ids:
                return node_id, None

        # New node path
        new_id = _slug(cleaned)
        if not new_id:
            return "", None
        new_node = DerivedNode(id=new_id, name=cleaned, type="private_company")
        return new_id, new_node

    # ── Pattern application ──────────────────────────────────────────────

    def _extract_edges(
        self,
        ticker: str,
        downstream_id: str,
        section_text: str,
        section_label: str,
        filing_date: date,
    ) -> tuple[list[DerivedEdge], list[DerivedNode]]:
        edges: list[DerivedEdge] = []
        new_nodes: list[DerivedNode] = []

        def _make(
            name: str,
            relationship: str,
            pct: float | None = None,
            input_type: str | None = None,
            pattern_key: str = "",
            is_customer: bool = False,
        ) -> None:
            node_id, maybe_new = self._resolve_counterparty(name)
            if not node_id:
                return
            if maybe_new is not None:
                new_nodes.append(maybe_new)
            if is_customer:
                upstream = downstream_id  # the ticker is the seller
                downstream = node_id       # the counterparty is the buyer
                pct_upstream_rev = pct
                pct_downstream_cogs = None
            else:
                upstream = node_id
                downstream = downstream_id
                pct_upstream_rev = None
                pct_downstream_cogs = pct
            if upstream == downstream:
                return
            # Reject likely self-subsidiary edges (e.g. gm_financial -> gm).
            if is_customer:
                if downstream.startswith(upstream + "_") or upstream.startswith(downstream + "_"):
                    return
            else:
                if upstream.startswith(downstream + "_") or downstream.startswith(upstream + "_"):
                    return
            if relationship not in _VALID_RELATIONSHIPS:
                return
            edges.append(
                DerivedEdge(
                    upstream_id=upstream,
                    downstream_id=downstream,
                    relationship=relationship,
                    source=f"10-K {filing_date.year} {ticker.upper()} {section_label}",
                    as_of=filing_date,
                    input_type=input_type,
                    pct_upstream_revenue=pct_upstream_rev,
                    pct_downstream_cogs=pct_downstream_cogs,
                    raw_name=name,
                )
            )
            self.stats.bump(pattern_key)

        # Pattern A family — customer concentration with percent
        # Each match must have nearby customer-context vocabulary to avoid
        # matching product names, drug names, or segment descriptions.
        _CUST_CTX_RE = re.compile(
            r"\b(customer|customers|buyer|purchaser|wholesaler|distributor|"
            r"retailer|client|clients|account|accounts)\b",
            re.IGNORECASE,
        )

        for pattern, key, require_ctx in (
            (_PAT_CUSTOMER_PCT, "customer_pct", False),
            (_PAT_SALES_TO_CUSTOMER, "sales_to_customer", False),
            (_PAT_NAME_ACCOUNTED, "name_accounted", True),
            (_PAT_SALES_MADE_TO, "sales_made_to", False),
        ):
            for m in pattern.finditer(section_text):
                try:
                    pct = float(m.group("pct"))
                except (TypeError, ValueError):
                    pct = None
                # Reject implausible percents (single customer > 99% is noise)
                if pct is not None and (pct <= 0 or pct > 99):
                    continue
                if require_ctx:
                    window_start = max(0, m.start() - 200)
                    window_end = min(len(section_text), m.end() + 200)
                    if not _CUST_CTX_RE.search(section_text[window_start:window_end]):
                        continue
                _make(
                    m.group("name"),
                    "customer",
                    pct=pct,
                    pattern_key=key,
                    is_customer=True,
                )

        # Pattern B — source / purchase from
        for m in _PAT_SUPPLIER_SOURCE.finditer(section_text):
            input_type = _clean_name(m.group("input")) or None
            rel = _infer_relationship(
                section_text[max(0, m.start() - 40) : m.end() + 40],
                input_type,
                "supplier_source",
            )
            _make(
                m.group("name"),
                rel,
                input_type=input_type,
                pattern_key="supplier_source",
            )

        # Pattern C — X supplies our Y
        for m in _PAT_SUPPLIER_VERB.finditer(section_text):
            input_type = _clean_name(m.group("input")) or None
            rel = _infer_relationship(
                section_text[max(0, m.start() - 40) : m.end() + 40],
                input_type,
                "supplier_verb",
            )
            _make(
                m.group("name"),
                rel,
                input_type=input_type,
                pattern_key="supplier_verb",
            )

        # Pattern D — principal customers/suppliers/distributors include ...
        for m in _PAT_PRINCIPAL_LIST.finditer(section_text):
            kind = m.group("kind").lower()
            names = _split_list(m.group("names"))
            if kind == "customers":
                rel_pattern = "principal_customers"
                is_customer = True
                rel = "customer"
            elif kind == "distributors":
                rel_pattern = "principal_distributors"
                is_customer = False
                rel = "distribution"
            else:
                rel_pattern = "principal_suppliers"
                is_customer = False
                rel = "component"
            # Principal lists can be truncated; cap at 8 names
            for nm in names[:8]:
                _make(
                    nm,
                    rel,
                    pattern_key=rel_pattern,
                    is_customer=is_customer,
                )

        # Pattern E — contract manufacturing
        for m in _PAT_CONTRACT_MFG.finditer(section_text):
            _make(
                m.group("name"),
                "contract_mfg",
                pattern_key="contract_mfg",
            )

        # Pattern F — single-supplier reliance
        for m in _PAT_RELY_ON.finditer(section_text):
            _make(
                m.group("name"),
                "component",
                pattern_key="rely_on",
            )

        return edges, new_nodes

    # ── DB writes ─────────────────────────────────────────────────────────

    def _upsert_node(self, conn: Any, node: DerivedNode) -> bool:
        """Insert a new node. Returns True if a row was inserted."""
        result = conn.execute(
            text(
                """
                INSERT INTO supply_chain_nodes (id, name, type, country, notes)
                VALUES (:id, :name, :type, :country, :notes)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {
                "id": node.id,
                "name": node.name,
                "type": node.type,
                "country": node.country,
                "notes": node.notes,
            },
        )
        return bool(result.rowcount and result.rowcount > 0)

    def _upsert_edge(self, conn: Any, edge: DerivedEdge) -> bool:
        """Upsert an edge. Returns True if inserted (not updated)."""
        result = conn.execute(
            text(
                """
                INSERT INTO supply_chain_edges (
                    upstream_id, downstream_id, relationship, tier,
                    input_type, pct_upstream_revenue, pct_downstream_cogs,
                    confidence, as_of, source
                ) VALUES (
                    :u, :d, :rel, 1,
                    :input_type, :pct_up, :pct_down,
                    :conf, :as_of, :src
                )
                ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
                DO UPDATE SET
                    input_type = COALESCE(
                        EXCLUDED.input_type, supply_chain_edges.input_type
                    ),
                    pct_upstream_revenue = COALESCE(
                        EXCLUDED.pct_upstream_revenue,
                        supply_chain_edges.pct_upstream_revenue
                    ),
                    pct_downstream_cogs = COALESCE(
                        EXCLUDED.pct_downstream_cogs,
                        supply_chain_edges.pct_downstream_cogs
                    ),
                    source = EXCLUDED.source
                """
            ),
            {
                "u": edge.upstream_id,
                "d": edge.downstream_id,
                "rel": edge.relationship,
                "input_type": edge.input_type,
                "pct_up": edge.pct_upstream_revenue,
                "pct_down": edge.pct_downstream_cogs,
                "conf": edge.confidence,
                "as_of": edge.as_of,
                "src": edge.source,
            },
        )
        return bool(result.rowcount and result.rowcount > 0)

    # ── Checkpoint ────────────────────────────────────────────────────────

    def _load_checkpoint(self) -> dict[str, Any]:
        if not _CHECKPOINT_PATH.exists():
            return {"done": [], "stats": {}}
        try:
            return json.loads(_CHECKPOINT_PATH.read_text())
        except Exception:
            return {"done": [], "stats": {}}

    def _save_checkpoint(self, done: list[str]) -> None:
        try:
            payload = {
                "done": done,
                "stats": {
                    "edges": self.stats.edges_created,
                    "nodes": self.stats.nodes_created,
                    "pattern_hits": self.stats.pattern_hits,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            }
            _CHECKPOINT_PATH.write_text(json.dumps(payload, indent=2))
        except Exception as exc:
            log.debug("checkpoint save failed: {e}", e=str(exc))

    # ── Ticker universe ──────────────────────────────────────────────────

    @staticmethod
    def _priority_tickers() -> list[str]:
        """Flatten SECTOR_MAP into a deduped priority-ordered ticker list."""
        try:
            from analysis.sector_map import SECTOR_MAP
        except Exception as exc:
            log.error("sector_map import failed: {e}", e=str(exc))
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
        """Process a single ticker. Returns number of edges created."""
        cik = self._resolve_cik(ticker)
        if not cik:
            self.stats.tickers_skipped += 1
            log.debug("No CIK for {t}", t=ticker)
            return 0

        meta = self._latest_10k_meta(cik)
        if not meta:
            self.stats.tickers_skipped += 1
            log.debug("No 10-K meta for {t} cik={c}", t=ticker, c=cik)
            return 0

        full_text = self._fetch_10k_text(cik, meta)
        if not full_text:
            self.stats.filings_failed += 1
            return 0
        self.stats.filings_fetched += 1

        # Slug the ticker downstream id. Parser uses lowercase to match seed.
        downstream_id = ticker.lower()

        item1 = self._extract_section(full_text, _ITEM_1_RE)
        item1a = self._extract_section(full_text, _ITEM_1A_RE)
        concentration = self._extract_concentration_notes(full_text)
        # Scan the whole document too — the principal-customer disclosure
        # sometimes lives in segment / MD&A narratives far from Item 1.
        # We rely on the customer-context filter to suppress drug/segment
        # false positives.

        all_edges: list[DerivedEdge] = []
        all_nodes: list[DerivedNode] = []

        for section_text, label in (
            (item1, "Item 1"),
            (item1a, "Item 1A"),
            (concentration, "Concentration Notes"),
            (full_text, "Full Text"),
        ):
            if not section_text:
                continue
            try:
                edges, nodes = self._extract_edges(
                    ticker=ticker,
                    downstream_id=downstream_id,
                    section_text=section_text,
                    section_label=label,
                    filing_date=meta["filing_date"],
                )
                all_edges.extend(edges)
                all_nodes.extend(nodes)
            except Exception as exc:
                log.warning(
                    "parse error {t} {lbl}: {e}",
                    t=ticker, lbl=label, e=str(exc),
                )

        if not all_edges:
            return 0

        # Dedup edges by (upstream, downstream, relationship)
        dedup: dict[tuple[str, str, str], DerivedEdge] = {}
        for e in all_edges:
            key = (e.upstream_id, e.downstream_id, e.relationship)
            if key not in dedup:
                dedup[key] = e
            else:
                # Prefer the one with a percent
                if dedup[key].pct_downstream_cogs is None and e.pct_downstream_cogs is not None:
                    dedup[key] = e
                elif dedup[key].pct_upstream_revenue is None and e.pct_upstream_revenue is not None:
                    dedup[key] = e

        node_dedup: dict[str, DerivedNode] = {n.id: n for n in all_nodes}

        inserted_edges = 0
        inserted_nodes = 0
        with self.engine.begin() as conn:
            for node in node_dedup.values():
                if self._upsert_node(conn, node):
                    inserted_nodes += 1
                    self.seed_node_ids.add(node.id)
                    self.name_to_node_id[node.name.lower().strip()] = node.id

            # Also ensure the downstream ticker has a node entry
            if downstream_id not in self.seed_node_ids:
                if self._upsert_node(
                    conn,
                    DerivedNode(
                        id=downstream_id,
                        name=ticker.upper(),
                        type="ticker",
                    ),
                ):
                    inserted_nodes += 1
                    self.seed_node_ids.add(downstream_id)

            for edge in dedup.values():
                # Only write edges whose counterparty is a known node
                if edge.upstream_id not in self.seed_node_ids:
                    continue
                if edge.downstream_id not in self.seed_node_ids:
                    continue
                if self._upsert_edge(conn, edge):
                    inserted_edges += 1

        self.stats.nodes_created += inserted_nodes
        self.stats.edges_created += inserted_edges
        self.stats.tickers_processed += 1

        log.info(
            "{t}: edges={e} nodes={n} (as_of={d})",
            t=ticker,
            e=inserted_edges,
            n=inserted_nodes,
            d=meta["filing_date"],
        )
        return inserted_edges

    def run(
        self,
        limit: int | None = None,
        tickers: Iterable[str] | None = None,
        budget_seconds: int = _DEFAULT_RUN_BUDGET_SECONDS,
        reset_checkpoint: bool = False,
    ) -> dict[str, Any]:
        """Main entry point. Runs the parser until limit/budget is reached."""
        started = time.monotonic()
        if reset_checkpoint and _CHECKPOINT_PATH.exists():
            _CHECKPOINT_PATH.unlink(missing_ok=True)

        checkpoint = self._load_checkpoint()
        done: list[str] = list(checkpoint.get("done", []))
        done_set = set(done)

        if tickers is None:
            universe = self._priority_tickers()
        else:
            universe = [t.upper() for t in tickers]

        queued = [t for t in universe if t not in done_set]
        if limit is not None:
            queued = queued[:limit]

        log.info(
            "SupplyChain10KParser start: queue={q} done={d} budget={b}s",
            q=len(queued), d=len(done_set), b=budget_seconds,
        )

        for ticker in queued:
            if time.monotonic() - started > budget_seconds:
                log.warning(
                    "Budget exceeded after {n} tickers",
                    n=self.stats.tickers_processed,
                )
                break
            try:
                self.process_ticker(ticker)
            except Exception as exc:
                log.warning(
                    "process_ticker {t} failed: {e}", t=ticker, e=str(exc)
                )
                self.stats.filings_failed += 1
            done.append(ticker)
            done_set.add(ticker)
            # Checkpoint every 10 tickers to survive interruptions
            if len(done) % 10 == 0:
                self._save_checkpoint(done)

        self._save_checkpoint(done)

        summary = {
            "tickers_processed": self.stats.tickers_processed,
            "tickers_skipped": self.stats.tickers_skipped,
            "filings_fetched": self.stats.filings_fetched,
            "filings_failed": self.stats.filings_failed,
            "edges_created": self.stats.edges_created,
            "nodes_created": self.stats.nodes_created,
            "pattern_hits": self.stats.pattern_hits,
            "elapsed_seconds": round(time.monotonic() - started, 1),
        }
        log.info("SupplyChain10KParser done: {s}", s=summary)
        return summary


# ── Hermes entrypoint (weekly) ────────────────────────────────────────────────


def run_weekly(db_engine: Engine | None = None) -> dict[str, Any]:
    """Hermes operator entrypoint. Runs once a week for 1 hour max."""
    if db_engine is None:
        from db import get_engine

        db_engine = get_engine()
    parser = SupplyChain10KParser(db_engine=db_engine)
    return parser.run(
        limit=None,
        budget_seconds=_DEFAULT_RUN_BUDGET_SECONDS,
    )


if __name__ == "__main__":
    from db import get_engine

    parser = SupplyChain10KParser(db_engine=get_engine())
    result = parser.run(limit=20)
    print(json.dumps(result, indent=2, default=str))
