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
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date
from datetime import datetime as _datetime
from typing import Any, Callable, TypeVar

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


# ── edgartools HTTP/2 deadlock guard ─────────────────────────────────────────
#
# edgartools's HTTP layer (httpxthrottlecache) auto-enables HTTP/2 whenever the
# optional `h2` package happens to be importable -- no opt-in required
# (httpxthrottlecache/httpxclientmanager.py: `HTTP2 = importlib.util.find_spec
# ("h2") is not None`, used as the default for httpx_params["http2"]). That
# HTTP/2 path deadlocked repeatedly in prod fetching 13F infotables:
# faulthandler showed the main thread permanently blocked in
# httpcore/_sync/http2.py:131 acquiring a stream-allocation lock that takes no
# timeout of its own (pyrate_limiter's asyncio rate-limit bucket thread was
# still alive, so it was not a process-wide hang -- just this one call, forever).
#
# edgartools's public `configure_http()` has no `http2` parameter, so there is
# no supported way to ask for HTTP/1.1. We flip the same private knob
# `configure_http()` itself mutates (`edgar.httpclient.HTTP_MGR.httpx_params`)
# and recreate the client the same way it does when a setting changes.
_http1_forced: bool = False


def _ensure_http1_transport() -> None:
    """Force edgartools's shared HTTP client onto HTTP/1.1, once per process.

    HTTP/1.1 has no stream-multiplexing lock, so the httpcore deadlock class
    described above cannot occur on this transport.
    """
    global _http1_forced
    if _http1_forced:
        return
    from edgar import httpclient as _edgar_httpclient

    _edgar_httpclient.HTTP_MGR.httpx_params["http2"] = False
    if _edgar_httpclient.HTTP_MGR._client is not None:
        _edgar_httpclient.HTTP_MGR._client.close()
        _edgar_httpclient.HTTP_MGR._client = None
    _http1_forced = True


_T = TypeVar("_T")

# Hard ceiling on the whole edgartools attempt (resolve + fetch + parse), not
# just one HTTP request. Belt-and-suspenders alongside _ensure_http1_transport:
# httpcore's lock.acquire() that deadlocked in prod takes no timeout of its
# own, so no request-level timeout (edgartools's or httpx's default) can ever
# catch it -- disabling HTTP/2 removes that specific lock, but nothing rules
# out edgartools hanging some other way in the future, so every attempt is
# still capped from the outside.
_EDGARTOOLS_FETCH_TIMEOUT: float = 45.0


def _call_with_timeout(fn: Callable[..., _T], *args: Any, timeout: float, **kwargs: Any) -> _T:
    """Run ``fn(*args, **kwargs)`` with a hard wall-clock timeout.

    A genuine deadlock (like the httpcore lock above) never raises, so
    ``except Exception`` around a direct call cannot catch it -- there is
    nothing to catch, the call just never returns. Running it on a daemon
    thread and bounding it with ``Thread.join(timeout=)`` is the only way to
    cap a call we cannot make well-behaved from the outside: on timeout we
    raise ``TimeoutError`` (the caller treats this exactly like any other
    edgartools failure) and abandon the thread. It is daemonic, so a thread
    that stays stuck forever never blocks process exit.
    """
    result: list[_T] = []
    error: list[BaseException] = []

    def _run() -> None:
        try:
            result.append(fn(*args, **kwargs))
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread below
            error.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        name = getattr(fn, "__name__", repr(fn))
        raise TimeoutError(f"{name} did not return within {timeout}s")
    if error:
        raise error[0]
    return result[0]


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


def filer_by_cik(cik: str | int) -> Filer | None:
    """Look up a filer by CIK (unpadded or zero-padded, str or int).

    GD0 §1.3 / §6 item 4 (owner decision, adopted 2026-09-28): this module's
    ``FILERS`` is the single verified source of truth for the 13F
    **filer**-CIK space. ``ingestion/edgar.py`` and
    ``ingestion/altdata/institutional_flows.py`` used to carry their own
    independent hardcoded CIK->name maps that disagreed with each other and
    with this one on the same CIK (e.g. ``1167483`` was claimed as three
    different funds across the three files). Both now derive their
    CIK/name lookups from ``FILERS`` via this function instead of
    maintaining a second copy.
    """
    try:
        target = int(str(cik).strip())
    except (TypeError, ValueError):
        return None
    for f in FILERS:
        if int(f.cik) == target:
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


def list_recent_13f_filings(cik: str) -> list[LatestFiling]:
    """List every 13F-HR / 13F-HR/A filing in a CIK's ``recent`` submissions.

    The SEC submissions endpoint's ``filings.recent`` block holds up to
    ~1000 of the filer's most recent filings *of any form type*. Because
    quarterly 13F filers rarely file anything else, this block in practice
    covers many years of 13F history — which is what makes it safe to use
    for catch-up: a caller that has gone stale for one or more quarters
    (see ``SEC13FLiveIngestor._process_filer``) can diff this list against
    what is already on file and backfill exactly the missing quarters,
    bounded by whatever this endpoint returns (never an unbounded crawl).

    Amendments supersede originals for the same ``reportDate`` — when two
    entries share a ``reportDate`` we keep only the one with the latest
    ``filingDate``, matching our upsert semantics (``ON CONFLICT DO
    UPDATE`` on ``(holder_name, ticker, report_date)``).

    Returns:
        Filings sorted newest ``report_date`` first. Empty list if the
        CIK has no 13F-HR filings in the recent window.
    """
    url = _EDGAR_SUBMISSIONS_URL.format(cik=cik.zfill(10))
    data = _get_json(url)
    recent = data.get("filings", {}).get("recent", {})
    forms: list[str] = recent.get("form", [])
    accessions: list[str] = recent.get("accessionNumber", [])
    filing_dates: list[str] = recent.get("filingDate", [])
    report_dates: list[str] = recent.get("reportDate", [])

    by_report_date: dict[date, LatestFiling] = {}
    for i, form in enumerate(forms):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        try:
            fd = date.fromisoformat(filing_dates[i])
            rd = date.fromisoformat(report_dates[i]) if report_dates[i] else fd
        except (ValueError, IndexError):
            continue
        cand = LatestFiling(accession=accessions[i], filing_date=fd, report_date=rd, form=form)
        existing = by_report_date.get(rd)
        if existing is None or cand.filing_date > existing.filing_date:
            by_report_date[rd] = cand

    return sorted(by_report_date.values(), key=lambda f: f.report_date, reverse=True)


def find_latest_13f(cik: str) -> LatestFiling | None:
    """Locate the most recent 13F-HR (or amendment) for a CIK.

    Thin convenience wrapper over :func:`list_recent_13f_filings` for
    callers that only care about the single newest filing.
    """
    filings = list_recent_13f_filings(cik)
    return filings[0] if filings else None


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
    _ensure_http1_transport()
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
    structured positions table for us. If edgartools is unavailable, errors
    (e.g. an API change or transient resolution failure), or does not return
    within ``_EDGARTOOLS_FETCH_TIMEOUT`` seconds (it deadlocked repeatedly in
    prod on an httpcore HTTP/2 lock -- see ``_ensure_http1_transport`` /
    ``_call_with_timeout``), we fall back to the raw-HTTP path in
    :func:`_fetch_infotable_raw`, so a live pull never loses data or hangs
    forever over a library hiccup.
    """
    try:
        positions = _call_with_timeout(
            _fetch_infotable_edgartools, filing, timeout=_EDGARTOOLS_FETCH_TIMEOUT
        )
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


_KNOWN_REPORT_DATES_SQL = text(
    """
    SELECT DISTINCT report_date FROM institutional_holdings
    WHERE holder_name = :holder AND source = 'sec_13f_live'
    """
)


@dataclass
class FilerResult:
    """Outcome of processing a single filer.

    Attributes:
        filer: Filer metadata.
        status: ``ok``, ``up_to_date``, ``no_filing``, ``no_positions``, or
            ``error``. ``up_to_date`` means 13F-HR filings exist for this
            filer but every ``report_date`` is already on file — the
            common case once the writer is running on a steady cadence.
        filing: The newest filing considered (if any).
        filings_processed: Number of *new* filings upserted this run (can
            be more than one right after a stale period — see
            ``_process_filer``'s catch-up behavior).
        positions_total: Total positions parsed across processed filings.
        positions_matched: Positions successfully resolved to a ticker.
        rows_written: Rows upserted into ``institutional_holdings``.
        error: Error string (if status == ``error``).
    """

    filer: Filer
    status: str
    filing: LatestFiling | None = None
    filings_processed: int = 0
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

    def _known_report_dates(self, filer: Filer) -> set[date]:
        """``report_date``s already on file for this filer's ``sec_13f_live`` rows.

        Scoped to ``source = 'sec_13f_live'`` so the hand-curated
        ``sec_13f_curated`` bootstrap rows (different provenance, no
        ``filed_date`` guarantee) never mask a quarter this writer hasn't
        actually ingested yet.
        """
        with self._engine.connect() as conn:
            rows = conn.execute(
                _KNOWN_REPORT_DATES_SQL, {"holder": filer.display_name}
            )
            known: set[date] = set()
            for (value,) in rows:
                # Postgres (production) returns a native ``date``. SQLite
                # (unit tests, and the module's ``if __name__`` demo) can
                # hand back an ISO string for a raw-``text()`` DATE column
                # since there is no real DATE type to coerce through —
                # normalize both to ``date`` so the ``not in known`` check
                # in ``_process_filer`` actually matches.
                if isinstance(value, _datetime):
                    known.add(value.date())
                elif isinstance(value, date):
                    known.add(value)
                elif isinstance(value, str):
                    known.add(date.fromisoformat(value[:10]))
            return known

    def _process_filer(self, filer: Filer, verbose: bool = False) -> FilerResult:
        """Process a single filer end-to-end.

        Fetches every 13F-HR/13F-HR/A filing EDGAR's ``recent`` submissions
        window has for this CIK (see :func:`list_recent_13f_filings`) and
        upserts whichever ``report_date``s are not already in
        ``institutional_holdings`` for this filer. This makes the writer
        self-healing after any gap (e.g. the 2026-04-12 -> 2026-09
        outage): the first run after a gap silently backfills every missed
        quarter still inside EDGAR's recent window instead of jumping
        straight to the newest quarter and leaving the skipped ones
        permanently missing.
        """
        log.info("13F: {k} (CIK={c})", k=filer.key, c=filer.cik)

        filings = list_recent_13f_filings(filer.cik)
        if not filings:
            log.warning("No 13F-HR found for {k}", k=filer.key)
            return FilerResult(filer=filer, status="no_filing")

        known = self._known_report_dates(filer)
        new_filings = [f for f in filings if f.report_date not in known]
        if not new_filings:
            return FilerResult(filer=filer, status="up_to_date", filing=filings[0])

        # Oldest-first so a multi-quarter catch-up ingests (and logs) in
        # chronological order — easier to audit than newest-first.
        new_filings.sort(key=lambda f: f.report_date)

        positions_total = 0
        positions_matched = 0
        rows_written = 0
        last_filing = filings[0]
        for filing in new_filings:
            log.info(
                "  -> new {form} filed={f} report={r} accession={a}",
                form=filing.form, f=filing.filing_date,
                r=filing.report_date, a=filing.accession,
            )
            # Unconditional: EDGAR rate-limits per-second across *all*
            # requests, not just infotable-to-infotable gaps. Without this,
            # the first fetch_infotable per filer fires immediately after
            # the submissions request that list_recent_13f_filings() just
            # made, with no delay between them.
            time.sleep(_EDGAR_RATE_DELAY)

            positions = fetch_infotable(filer.cik, filing)
            positions_total += len(positions)
            if not positions:
                continue

            matched: list[tuple[dict[str, Any], str]] = []
            for pos in positions:
                ticker = self._cusip_map.lookup(pos.get("cusip", ""))
                if ticker:
                    matched.append((pos, ticker))
            positions_matched += len(matched)

            if verbose:
                log.info(
                    "  -> {n} positions, {m} resolved to ticker",
                    n=len(positions), m=len(matched),
                )

            rows_written += self._upsert_positions(filer, filing, matched)
            last_filing = filing

        if rows_written == 0:
            return FilerResult(
                filer=filer,
                status="no_positions",
                filing=last_filing,
                filings_processed=len(new_filings),
                positions_total=positions_total,
            )

        return FilerResult(
            filer=filer,
            status="ok",
            filing=last_filing,
            filings_processed=len(new_filings),
            positions_total=positions_total,
            positions_matched=positions_matched,
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
        "filers_up_to_date": sum(1 for r in results if r.status == "up_to_date"),
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
