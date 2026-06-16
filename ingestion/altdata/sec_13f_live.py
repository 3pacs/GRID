"""Live SEC 13F-HR ingestor for the ``institutional_holdings`` table.

This module replaces the static 2024-Q4 snapshot produced by
``scripts/populate_institutional_holdings.py`` with live quarterly data
pulled directly from SEC EDGAR. Every quarter — roughly 45 days after
quarter end — institutional investment managers with > $100M AUM must
file Form 13F-HR disclosing their long US equity positions.

Pipeline
--------
For each tracked filer CIK we:

1. Fetch ``https://data.sec.gov/submissions/CIK{padded}.json`` and find
   the most recent ``13F-HR`` or ``13F-HR/A`` filing.
2. Download the filing's ``index.json`` to locate the ``informationtable``
   XML attachment (the structured positions list).
3. Parse each ``<infoTable>`` entry into a dict with issuer, CUSIP,
   value (in USD thousands), and share count.
4. Resolve CUSIP -> ticker via an on-disk CUSIP map built from the
   FINRA FTD CSVs that GRID already ships in ``data/ftd_cnsfails*.csv``.
5. Upsert rows into ``institutional_holdings`` keyed by the unique index
   ``(holder_name, ticker, report_date)`` so amendments refresh existing
   rows instead of duplicating them.

The writer uses ``source='sec_13f_live'`` so rows produced here can be
distinguished from the hand-curated bootstrap rows
(``source='sec_13f_curated'``).

SEC rate limits
---------------
The SEC enforces a 10 req/sec ceiling. We sleep ``_EDGAR_RATE_DELAY``
(0.15s) between requests and identify ourselves via a ``User-Agent``
header — matching the pattern in
``ingestion/altdata/institutional_flows.py``.
"""

from __future__ import annotations

import csv
import glob
import os
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── SEC EDGAR HTTP config ─────────────────────────────────────────────────────

_EDGAR_SUBMISSIONS_URL: str = "https://data.sec.gov/submissions/CIK{cik}.json"
_EDGAR_ARCHIVE_BASE: str = "https://www.sec.gov/Archives/edgar/data"

# The SEC requires a UA with contact info. Matches institutional_flows.py.
_EDGAR_HEADERS: dict[str, str] = {
    "User-Agent": "GRID-Research research@grid.local",
    "Accept-Encoding": "gzip, deflate",
}

_REQUEST_TIMEOUT: int = 30
_EDGAR_RATE_DELAY: float = 0.15

# edgartools identity. The SEC requires a contact UA; edgartools enforces this
# globally via set_identity(). We mirror the UA used for the raw-HTTP fallback
# so both code paths present the same identity to EDGAR.
_EDGAR_IDENTITY: str = os.environ.get(
    "SEC_USER_AGENT", "GRID-Research research@grid.local"
)
_identity_set: bool = False


def _ensure_identity() -> None:
    """Set the edgartools global identity exactly once per process."""
    global _identity_set
    if _identity_set:
        return
    from edgar import set_identity

    set_identity(_EDGAR_IDENTITY)
    _identity_set = True

# ── Filer universe ────────────────────────────────────────────────────────────
# Curated set of ~35 high-signal 13F filers. CIKs are the canonical SEC
# Central Index Keys. We keep a human-friendly short key for CLI
# selection (``--filers berkshire_hathaway``) plus the pretty display name
# stored in ``institutional_holdings.holder_name``.
#
# CIKs for the original 20 come from
# ``scripts/populate_institutional_holdings.py`` so the new rows merge
# cleanly with the curated bootstrap rows on the
# (holder_name, ticker, report_date) unique index.


@dataclass(frozen=True)
class Filer:
    """Metadata for a tracked 13F filer.

    Attributes:
        key: Short slug used in CLI selection and logs.
        cik: SEC Central Index Key (unpadded string form).
        display_name: Human-friendly holder name stored in the DB.
    """

    key: str
    cik: str
    display_name: str


FILERS: tuple[Filer, ...] = (
    # ── From populate_institutional_holdings.py bootstrap ─────────────
    Filer("berkshire_hathaway",   "1067983", "Berkshire Hathaway"),
    Filer("pershing_square",      "1336528", "Pershing Square Capital"),
    Filer("trian",                "1345471", "Trian Fund Management"),
    Filer("3g_capital",           "1421669", "3G Capital"),
    Filer("bridgewater",          "1350694", "Bridgewater Associates"),
    Filer("elliott_management",   "1791786", "Elliott Investment Management"),
    Filer("icahn_enterprises",    "921669",  "Icahn Enterprises"),
    Filer("valueact",             "1418814", "ValueAct Capital"),
    Filer("third_point",          "1159159", "Third Point"),
    Filer("starboard_value",      "1517137", "Starboard Value"),
    Filer("jana_partners",        "1027451", "Jana Partners"),
    Filer("soros_fund",           "1029160", "Soros Fund Management"),
    # ── New additions (big hedge funds + family offices + LPs) ────────
    Filer("renaissance",          "1037389", "Renaissance Technologies"),
    Filer("two_sigma",            "1649339", "Two Sigma Investments"),
    Filer("citadel",              "1423053", "Citadel Advisors"),
    Filer("millennium",           "1273087", "Millennium Management"),
    Filer("point72",              "1603466", "Point72 Asset Management"),
    Filer("tiger_global",         "1167483", "Tiger Global Management"),
    Filer("coatue",               "1135730", "Coatue Management"),
    Filer("viking_global",        "1103804", "Viking Global Investors"),
    Filer("de_shaw",              "1009207", "D.E. Shaw"),
    Filer("baupost",              "1061165", "Baupost Group"),
    Filer("aqr",                  "1167557", "AQR Capital Management"),
    Filer("lone_pine",            "1061768", "Lone Pine Capital"),
    Filer("appaloosa",            "1656456", "Appaloosa Management"),
    # ── Index / active large cap sponsors ─────────────────────────────
    Filer("sequoia_capital",      "1607841", "Sequoia Capital (SC US TTGP)"),
    Filer("altimeter",            "1541617", "Altimeter Capital"),
    Filer("baillie_gifford",      "1088875", "Baillie Gifford"),
    Filer("t_rowe_price",         "1897612", "T. Rowe Price Investment Mgmt"),
    Filer("capital_research",     "1422848", "Capital Research Global"),
    Filer("wellington",           "902219",  "Wellington Management"),
    Filer("geode_capital",        "1214717", "Geode Capital Management"),
    Filer("blackrock",            "2012383", "BlackRock Inc"),
    Filer("vanguard",             "102909",  "Vanguard Group"),
    Filer("state_street",         "93751",   "State Street"),
)


def filer_by_key(key: str) -> Filer | None:
    """Look up a filer by its short slug."""
    for f in FILERS:
        if f.key == key:
            return f
    return None


# ── CUSIP -> ticker resolution ───────────────────────────────────────────────


class CusipTickerMap:
    """Builds a CUSIP -> ticker map from the local FTD CSV corpus.

    GRID already ships historical FINRA FTD CSVs in ``data/ftd_cnsfails*.csv``.
    Each row has ``CUSIP`` and ``SYMBOL`` columns, so the full corpus
    yields a broad CUSIP -> ticker mapping that covers virtually every
    actively traded US equity. This is cheaper than paying for a CUSIP
    feed and more complete than any hardcoded top-500 list.

    The map is built lazily and cached on the instance.
    """

    def __init__(self, data_dirs: list[str] | None = None) -> None:
        """Initialise the CUSIP map.

        Parameters:
            data_dirs: Candidate directories containing FTD CSVs. If
                ``None``, derives sensible defaults relative to this
                module so the same code path works in both the local dev
                tree and the deployed ``grid_v4`` tree on the server.
        """
        if data_dirs is None:
            here = os.path.dirname(os.path.abspath(__file__))
            repo_root = os.path.abspath(os.path.join(here, "..", ".."))
            data_dirs = [
                os.path.join(repo_root, "data"),
                "/data/grid_v4/astrogrid_dedup/data",
                "/home/grid/grid_v4/data",
            ]
        self._data_dirs = data_dirs
        self._map: dict[str, str] | None = None

    def _build(self) -> dict[str, str]:
        """Scan FTD CSVs and materialise a CUSIP -> ticker map."""
        mapping: dict[str, str] = {}
        scanned = 0

        for data_dir in self._data_dirs:
            if not os.path.isdir(data_dir):
                continue
            for path in sorted(glob.glob(os.path.join(data_dir, "ftd_cnsfails*.csv"))):
                try:
                    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
                        reader = csv.DictReader(fh)
                        for row in reader:
                            cusip = (row.get("CUSIP") or "").strip()
                            symbol = (row.get("SYMBOL") or "").strip().upper()
                            if not cusip or not symbol or "." in symbol:
                                continue
                            # Skip odd tickers that look like placeholders.
                            if not symbol.isascii() or len(symbol) > 6:
                                continue
                            # Prefer first observation — FTD files roll
                            # daily and the same CUSIP maps to the same
                            # symbol consistently. Later files may have
                            # reorgs; earlier wins keeps us stable.
                            mapping.setdefault(cusip, symbol)
                    scanned += 1
                except Exception as exc:
                    log.warning("Failed to read FTD csv {p}: {e}", p=path, e=str(exc))

        log.info(
            "CusipTickerMap: loaded {n} CUSIP->ticker pairs from {f} FTD CSVs",
            n=len(mapping),
            f=scanned,
        )
        return mapping

    def lookup(self, cusip: str) -> str | None:
        """Return the ticker for a CUSIP or ``None`` if unknown.

        The SEC 13F infotable sometimes stores a 9-char CUSIP and
        sometimes a shorter variant — we also try the 8-char prefix
        (CUSIP without the check digit) for resilience.
        """
        if self._map is None:
            self._map = self._build()
        if not cusip:
            return None
        cusip = cusip.strip().upper()
        hit = self._map.get(cusip)
        if hit:
            return hit
        if len(cusip) == 9:
            return self._map.get(cusip[:8] + cusip[-1])
        return None

    def size(self) -> int:
        """Return the number of CUSIP entries currently loaded."""
        if self._map is None:
            self._map = self._build()
        return len(self._map)


# ── 13F XML parsing ──────────────────────────────────────────────────────────


def _strip_ns(tag: str) -> str:
    """Drop ``{namespace}`` from an XML element tag."""
    return tag.split("}", 1)[1] if "}" in tag else tag


def parse_infotable_xml(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse 13F ``informationtable.xml`` into position dicts.

    The schema uses a namespace ``http://www.sec.gov/edgar/document/thirteenf/informationtable``
    but older filings used ``http://www.sec.gov/document/thirteenf``. We
    iterate namespace-agnostically via local-name matching.

    Parameters:
        xml_bytes: Raw XML document bytes.

    Returns:
        A list of dicts, one per ``<infoTable>`` entry, with keys:
        ``name_of_issuer``, ``cusip``, ``value`` (USD — converted from
        reported thousands), ``shares``, ``share_type``.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        log.warning("13F XML parse error: {e}", e=str(exc))
        return []

    positions: list[dict[str, Any]] = []
    for entry in root.iter():
        if _strip_ns(entry.tag) != "infoTable":
            continue

        row: dict[str, Any] = {}
        for child in entry.iter():
            tag = _strip_ns(child.tag)
            text_val = (child.text or "").strip() if child.text else ""
            if tag == "nameOfIssuer":
                row["name_of_issuer"] = text_val
            elif tag == "cusip":
                row["cusip"] = text_val.upper()
            elif tag == "value":
                # Starting with 13F filings effective 2023-01-03, SEC
                # reports ``value`` in actual USD, not thousands. Older
                # filings reported thousands, but since we only ever
                # ingest the most recent filing per filer the "USD"
                # interpretation is correct for all live ingestion.
                try:
                    row["value"] = int(float(text_val))
                except (ValueError, TypeError):
                    row["value"] = None
            elif tag == "sshPrnamt":
                try:
                    row["shares"] = int(float(text_val))
                except (ValueError, TypeError):
                    row["shares"] = None
            elif tag == "sshPrnamtType":
                row["share_type"] = text_val

        if row.get("cusip") and row.get("name_of_issuer"):
            positions.append(row)

    return positions


# ── Filing discovery + download ──────────────────────────────────────────────


def _get_json(url: str) -> dict[str, Any]:
    """Fetch a JSON document from EDGAR."""
    resp = requests.get(url, headers=_EDGAR_HEADERS, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _get_bytes(url: str) -> bytes:
    """Fetch raw bytes from EDGAR."""
    resp = requests.get(url, headers=_EDGAR_HEADERS, timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.content


@dataclass(frozen=True)
class LatestFiling:
    """Metadata for a filer's most recent 13F-HR filing.

    Attributes:
        accession: Raw accession number with dashes (e.g. ``0001067983-25-000019``).
        filing_date: Date the filing hit EDGAR (the ``filed_date`` we store).
        report_date: Quarter end the filing covers (the ``report_date`` we store).
        form: Form type (``13F-HR`` or ``13F-HR/A``).
    """

    accession: str
    filing_date: date
    report_date: date
    form: str


def find_latest_13f(cik: str) -> LatestFiling | None:
    """Locate the most recent 13F-HR (or amendment) for a CIK.

    We look at the ``filings.recent`` block of the submissions index and
    pick the newest row whose form is ``13F-HR`` or ``13F-HR/A`` with the
    latest ``filingDate``. Amendments supersede originals for the same
    ``reportDate``, which matches our upsert semantics (amendments will
    overwrite the base row via ON CONFLICT DO UPDATE).
    """
    url = _EDGAR_SUBMISSIONS_URL.format(cik=cik.zfill(10))
    data = _get_json(url)
    recent = data.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    report_dates: list[str] = recent.get("reportDate", [])

    best: LatestFiling | None = None
    for i, form in enumerate(forms):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        try:
            fd = date.fromisoformat(filing_dates[i])
            rd = date.fromisoformat(report_dates[i]) if report_dates[i] else fd
        except (ValueError, IndexError):
            continue
        cand = LatestFiling(accession=accessions[i], filing_date=fd, report_date=rd, form=form)
        if best is None or cand.filing_date > best.filing_date:
            best = cand

    return best


def _infotable_df_to_positions(df: Any) -> list[dict[str, Any]]:
    """Convert an edgartools 13F infotable DataFrame into position dicts.

    edgartools returns one row per ``<infoTable>`` entry. Column names
    differ across edgartools major versions (4.x emits lower-case
    ``value``/``cusip``; 5.x emits ``Value``/``Cusip``), so we resolve
    every column case-insensitively to stay version-tolerant.

    Parameters:
        df: A pandas DataFrame as produced by ``ThirteenF.infotable``.

    Returns:
        Position dicts matching :func:`parse_infotable_xml`'s output shape:
        ``name_of_issuer``, ``cusip``, ``value`` (USD), ``shares``,
        ``share_type``.
    """
    cols = {str(c).lower(): c for c in df.columns}

    def pick(*names: str) -> str | None:
        for n in names:
            hit = cols.get(n.lower())
            if hit is not None:
                return hit
        return None

    issuer_c = pick("Issuer", "nameOfIssuer", "name_of_issuer")
    cusip_c = pick("Cusip", "cusip")
    value_c = pick("Value", "value")
    shares_c = pick("SharesPrnAmount", "sshPrnamt", "shares")
    type_c = pick("Type", "SharesPrnType", "sshPrnamtType", "share_type")

    def as_int(val: Any) -> int | None:
        try:
            if val is None:
                return None
            return int(float(val))
        except (ValueError, TypeError):
            return None

    positions: list[dict[str, Any]] = []
    for record in df.to_dict("records"):
        cusip = str(record.get(cusip_c) or "").strip().upper() if cusip_c else ""
        issuer = str(record.get(issuer_c) or "").strip() if issuer_c else ""
        if not cusip or not issuer:
            continue
        positions.append(
            {
                "name_of_issuer": issuer,
                "cusip": cusip,
                "value": as_int(record.get(value_c)) if value_c else None,
                "shares": as_int(record.get(shares_c)) if shares_c else None,
                "share_type": (
                    str(record.get(type_c) or "").strip() if type_c else ""
                ),
            }
        )
    return positions


def _fetch_infotable_edgartools(filing: LatestFiling) -> list[dict[str, Any]]:
    """Parse a 13F infotable via edgartools.

    Resolves the filing by accession number and reads the already-parsed
    ``infotable`` DataFrame, replacing the manual ``index.json`` directory
    walk and namespace-agnostic XML parsing of the raw path. Raises on any
    failure so the caller can fall back to the raw path.
    """
    _ensure_identity()
    from edgar import find

    resolved = find(filing.accession)
    thirteenf = resolved.obj() if hasattr(resolved, "obj") else resolved
    table = getattr(thirteenf, "infotable", None)
    if table is None or getattr(table, "empty", False):
        return []
    return _infotable_df_to_positions(table)


def fetch_infotable(cik: str, filing: LatestFiling) -> list[dict[str, Any]]:
    """Download and parse the infotable for a specific 13F filing.

    Primary path uses edgartools, which resolves the filing and parses the
    structured positions table for us. If edgartools is unavailable or
    errors (e.g. an API change or transient resolution failure), we fall
    back to the raw-HTTP path in :func:`_fetch_infotable_raw`, so a live
    pull never loses data over a library hiccup.
    """
    try:
        positions = _fetch_infotable_edgartools(filing)
        if positions:
            return positions
        log.debug(
            "edgartools returned no positions for {a}; trying raw path",
            a=filing.accession,
        )
    except Exception as exc:
        log.warning(
            "edgartools 13F parse failed for {a}; falling back to raw XML: {e}",
            a=filing.accession,
            e=str(exc),
        )
    return _fetch_infotable_raw(cik, filing)


def _fetch_infotable_raw(cik: str, filing: LatestFiling) -> list[dict[str, Any]]:
    """Raw-HTTP fallback: walk ``index.json`` and parse the infotable XML.

    Many modern 13F filings use randomised filenames (e.g. ``50240.xml``)
    rather than the canonical ``informationtable.xml``, so we use a
    multi-step strategy:

    1. Prefer files whose names contain ``infotable`` / ``information``.
    2. Otherwise probe any non-``primary_doc`` XML in the directory and
       keep the first one whose root contains an ``<infoTable>`` child.
    """
    acc_nodash = filing.accession.replace("-", "")
    base = f"{_EDGAR_ARCHIVE_BASE}/{int(cik)}/{acc_nodash}"
    index = _get_json(f"{base}/index.json")

    xml_candidates: list[str] = []
    preferred: str | None = None
    for item in index.get("directory", {}).get("item", []):
        name = (item.get("name") or "")
        lname = name.lower()
        if not lname.endswith(".xml"):
            continue
        if lname == "primary_doc.xml":
            continue
        if "infotable" in lname or "information" in lname:
            preferred = name
            break
        xml_candidates.append(name)

    if preferred:
        ordered = [preferred]
    else:
        ordered = xml_candidates

    if not ordered:
        log.warning(
            "No candidate infotable XML for CIK={c} accession={a}",
            c=cik, a=filing.accession,
        )
        return []

    for name in ordered:
        time.sleep(_EDGAR_RATE_DELAY)
        try:
            xml_bytes = _get_bytes(f"{base}/{name}")
        except Exception as exc:
            log.debug("Failed to fetch {n}: {e}", n=name, e=str(exc))
            continue
        positions = parse_infotable_xml(xml_bytes)
        if positions:
            return positions

    log.warning(
        "No parseable infotable in {n} XML candidates for CIK={c} accession={a}",
        n=len(ordered), c=cik, a=filing.accession,
    )
    return []


# ── Writer ───────────────────────────────────────────────────────────────────


_UPSERT_SQL = text(
    """
    INSERT INTO institutional_holdings
        (cik, holder_name, ticker, cusip, shares_held, value_usd,
         report_date, filed_date, source)
    VALUES
        (:cik, :holder, :ticker, :cusip, :shares, :value_usd,
         :report_date, :filed_date, 'sec_13f_live')
    ON CONFLICT (holder_name, ticker, report_date) DO UPDATE SET
        shares_held = EXCLUDED.shares_held,
        value_usd   = EXCLUDED.value_usd,
        cusip       = EXCLUDED.cusip,
        filed_date  = EXCLUDED.filed_date,
        source      = EXCLUDED.source
    """
)


@dataclass
class FilerResult:
    """Outcome of processing a single filer.

    Attributes:
        filer: Filer metadata.
        status: ``ok``, ``no_filing``, ``no_positions``, or ``error``.
        filing: The resolved filing (if any).
        positions_total: Total positions parsed from the filing.
        positions_matched: Positions successfully resolved to a ticker.
        rows_written: Rows upserted into ``institutional_holdings``.
        error: Error string (if status == ``error``).
    """

    filer: Filer
    status: str
    filing: LatestFiling | None = None
    positions_total: int = 0
    positions_matched: int = 0
    rows_written: int = 0
    error: str | None = None


# ── Orchestrator ─────────────────────────────────────────────────────────────


class SEC13FLiveIngestor:
    """Pull live 13F-HR filings and upsert into ``institutional_holdings``."""

    def __init__(self, engine: Engine, cusip_map: CusipTickerMap | None = None) -> None:
        """Initialise the ingestor.

        Parameters:
            engine: SQLAlchemy engine bound to the GRID database.
            cusip_map: Optional pre-built CUSIP map. Created lazily if ``None``.
        """
        self._engine = engine
        self._cusip_map = cusip_map or CusipTickerMap()

    def run(
        self,
        filers: list[Filer] | None = None,
        limit: int | None = None,
        verbose: bool = False,
    ) -> list[FilerResult]:
        """Run the ingestor.

        Parameters:
            filers: Specific filers to process. Defaults to ``FILERS``.
            limit: Process at most this many filers (after ``filers`` filter).
            verbose: Log per-position detail for debugging.

        Returns:
            One ``FilerResult`` per processed filer.
        """
        targets = list(filers or FILERS)
        if limit is not None:
            targets = targets[:limit]

        results: list[FilerResult] = []

        # Warm the CUSIP map once up front so the log message is clean.
        map_size = self._cusip_map.size()
        log.info("SEC 13F live ingestor starting — {n} filers, CUSIP map size={m}",
                 n=len(targets), m=map_size)

        for filer in targets:
            try:
                result = self._process_filer(filer, verbose=verbose)
            except Exception as exc:
                log.exception("Filer {k} failed: {e}", k=filer.key, e=str(exc))
                result = FilerResult(filer=filer, status="error", error=str(exc))
            results.append(result)
            time.sleep(_EDGAR_RATE_DELAY)

        ok = sum(1 for r in results if r.status == "ok")
        rows = sum(r.rows_written for r in results)
        log.info(
            "SEC 13F live ingestor complete — {ok}/{tot} filers ok, {r} rows written",
            ok=ok, tot=len(results), r=rows,
        )
        return results

    def _process_filer(self, filer: Filer, verbose: bool = False) -> FilerResult:
        """Process a single filer end-to-end."""
        log.info("13F: {k} (CIK={c})", k=filer.key, c=filer.cik)

        filing = find_latest_13f(filer.cik)
        if filing is None:
            log.warning("No 13F-HR found for {k}", k=filer.key)
            return FilerResult(filer=filer, status="no_filing")

        log.info(
            "  -> latest {form} filed={f} report={r} accession={a}",
            form=filing.form, f=filing.filing_date,
            r=filing.report_date, a=filing.accession,
        )
        time.sleep(_EDGAR_RATE_DELAY)

        positions = fetch_infotable(filer.cik, filing)
        if not positions:
            return FilerResult(filer=filer, status="no_positions", filing=filing)

        matched: list[tuple[dict[str, Any], str]] = []
        for pos in positions:
            ticker = self._cusip_map.lookup(pos.get("cusip", ""))
            if ticker:
                matched.append((pos, ticker))

        if verbose:
            log.info(
                "  -> {n} positions, {m} resolved to ticker",
                n=len(positions), m=len(matched),
            )

        rows_written = self._upsert_positions(filer, filing, matched)
        return FilerResult(
            filer=filer,
            status="ok",
            filing=filing,
            positions_total=len(positions),
            positions_matched=len(matched),
            rows_written=rows_written,
        )

    def _upsert_positions(
        self,
        filer: Filer,
        filing: LatestFiling,
        matched: list[tuple[dict[str, Any], str]],
    ) -> int:
        """Upsert matched positions into ``institutional_holdings``.

        Within a single filing the same ticker can appear multiple times
        (e.g. one row per share class). We aggregate shares and value
        before writing so the ``(holder_name, ticker, report_date)``
        unique index is satisfied.
        """
        agg: dict[str, dict[str, Any]] = {}
        for pos, ticker in matched:
            bucket = agg.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "cusip": pos.get("cusip"),
                    "shares": 0,
                    "value_usd": 0,
                },
            )
            bucket["shares"] += int(pos.get("shares") or 0)
            bucket["value_usd"] += int(pos.get("value") or 0)

        if not agg:
            return 0

        rows_written = 0
        with self._engine.begin() as conn:
            for bucket in agg.values():
                conn.execute(
                    _UPSERT_SQL,
                    {
                        "cik": filer.cik,
                        "holder": filer.display_name,
                        "ticker": bucket["ticker"],
                        "cusip": bucket["cusip"],
                        "shares": bucket["shares"] or None,
                        "value_usd": bucket["value_usd"] or None,
                        "report_date": filing.report_date,
                        "filed_date": filing.filing_date,
                    },
                )
                rows_written += 1
        return rows_written


# ── Scheduler entry point ────────────────────────────────────────────────────


def run(engine: Engine | None = None, **kwargs: Any) -> dict[str, Any]:
    """Entry point for ``hermes_operator`` registry.

    Parameters:
        engine: SQLAlchemy engine (resolved via ``db.get_engine`` if None).
        **kwargs: Forwarded to ``SEC13FLiveIngestor.run``.

    Returns:
        Summary dict with totals per status for the operator log.
    """
    if engine is None:
        from db import get_engine
        engine = get_engine()
    ingestor = SEC13FLiveIngestor(engine=engine)
    results = ingestor.run(**kwargs)

    summary = {
        "filers_ok": sum(1 for r in results if r.status == "ok"),
        "filers_total": len(results),
        "rows_written": sum(r.rows_written for r in results),
        "positions_total": sum(r.positions_total for r in results),
        "positions_matched": sum(r.positions_matched for r in results),
        "errors": [
            {"filer": r.filer.key, "error": r.error}
            for r in results if r.status == "error"
        ],
    }
    log.info("sec_13f_live summary: {s}", s=summary)
    return summary
