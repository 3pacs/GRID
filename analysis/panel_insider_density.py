"""VS1 panel harness: insider open-market-buy density within one sector (research only).

Pre-registration: ``docs/paper_log/vs1-insider-density-v1-preregistration.md``.
The sha256 of its body (the LF bytes between :data:`BODY_START` and
:data:`BODY_END`) is pinned in :data:`PREREG_BODY_SHA256`; every run refuses to
start when the repository copy no longer hashes to it, and the holdout refuses
unless the caller passes an explicit flag *and* that hash.

What it is
----------
The cross-sectional ("panel") mode the granular-discovery plan calls Route B
(§2.4 of ``GRID-GRANULAR-DISCOVERY-PLAN-20260927``). A trial is (feature,
horizon) inside one sector. Its statistic is the mean over horizon-spaced
decision dates of the Spearman rank correlation, across the sector's eligible
issuers, between the feature at the decision and the issuer's forward return
minus the sector ETF's forward return (the "rank IC"). The per-date rank IC is
unchanged by subtracting a return common to every issuer, so the benchmark
enters the reported magnitudes (buyer-minus-benchmark returns), not the test.

Statistics reused from the time-series loop (``analysis.offline_research_proof``
and ``analysis.ledger_steered_exploration``):

* split first, then label: every price outside the window is blanked before a
  label is computed, and in discovery mode no price on or after the split is
  even read (the reader is bounded at ``split - 1 day``);
* horizon-spaced decisions: one decision every ``h`` sessions, so outcome
  windows never overlap;
* the data-driven permutation block (:func:`autocorrelation_block`) sized on
  the discovery IC series, frozen for the holdout (``MIN_BLOCKS`` cap);
* p-values from a block null with a deterministic seed: here a block
  sign-flip of the per-date IC series (H0: the IC series is sign-symmetric in
  blocks, i.e. mean IC = 0); two sensitivity nulls are reported and never
  select (a time-alignment block permutation of the outcome cross-sections,
  the direct analogue of the time-series null, and a within-date issuer
  shuffle);
* selection by Holm's step-down at the ledger-issued run alpha
  ``alpha_k = q / (k (k + 1))`` (S11 alpha spending) over every declared trial,
  untestable ones at p = 1; BH-adjusted p-values over the same trials are
  reported;
* holdout: Bonferroni over the frozen selections, same sign required; a
  write-once output directory; ``promotion_allowed`` is always false;
* one shot, enforced through the hash-chained registry: the registry must
  start with the pinned VS1 v1 registration (:data:`REGISTERED_RECORD_SHA256`);
  no price is read without a key, and the key exists only after
  ``inputs_frozen`` (every input hash and ``as_of_ts``) and then
  ``discovery_opened`` / ``holdout_opened`` were appended to the chain *and*
  the pinned off-host anchor log (``main`` of the GitHub vault, append-only)
  already witnesses that head
  (:func:`require_witness`); a second discovery or holdout is refused.

Boundaries: no DB writes, no migrations, no timers. Prices are read only
through ``store.observations.read_window`` with an explicit ``source=``, only
for tickers in a frozen admitted-price manifest, never from a refused source
(yfinance, the Kaggle bulk load). Insider events come from off-DB files
(the SEC Form 3/4/5 structured data sets: non-derivative transactions for the
purchases, the SUBMISSION table for Section 16 activity).
Nothing here is a trading signal.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

from analysis.offline_research_proof import (
    MIN_BLOCKS,
    autocorrelation_block,
    bh_adjusted,
    block_permutations,
    corrected_p,
    digest,
    holm_adjusted,
    stamp,
    write_once,
)
from store import observations

REPO = Path(__file__).resolve().parent.parent


def rankdata(values: np.ndarray) -> np.ndarray:
    """Average ranks (1-based, ties share their mean rank), like ``scipy.stats.rankdata``.

    A plain-numpy version: scipy's per-call overhead dominates the thousands of
    small cross-sections ranked here.
    """
    x = np.asarray(values, dtype=float)
    order = np.argsort(x, kind="mergesort")
    ordered = x[order]
    starts = np.flatnonzero(np.r_[True, ordered[1:] != ordered[:-1]])
    counts = np.diff(np.r_[starts, len(x)])
    ranks = np.empty(len(x))
    ranks[order] = np.repeat(starts + 1 + (counts - 1) / 2.0, counts)
    return ranks

# --- pinned pre-registration -------------------------------------------------------

PREREG_PATH = Path("docs/paper_log/vs1-insider-density-v1-preregistration.md")
BODY_START = "<!-- PREREG-BODY-START -->"
BODY_END = "<!-- PREREG-BODY-END -->"
# sha256 of the LF bytes strictly between the two markers. Changing the body is
# a new pre-registration (v2): re-pin only before any data is read.
PREREG_BODY_SHA256 = "85078eeeb08fe292f4a01a295261c6594d865423cdfd505621949ba43dea7c5a"
# sha256 (LF bytes) of analysis/sector_map_data.yaml at origin/main 1bb2f61b:
# sector membership is computed from exactly this file.
SECTOR_MAP_SHA256 = "2d262fe1a8ab4fbfe7abbde86c947c3ff35c12c49f00f4217b24e0dd3af3cdba"
SECTOR_MAP_PATH = Path("analysis/sector_map_data.yaml")

VERSION = "vs1-v1"
ORIGIN = "form345_panel_research"

# --- declared design ----------------------------------------------------------------

#: The 11 equity sectors of the plan's generalization gate and their benchmark ETF.
EQUITY_SECTORS: dict[str, str] = {
    "Technology": "XLK",
    "Energy": "XLE",
    "Financials": "XLF",
    "Healthcare": "XLV",
    "Industrials": "XLI",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
    "Materials": "XLB",
}
VS1_SECTOR = "Technology"
#: Pre-registered order of the later sector runs (all in one ledger run, k=2).
OTHER_SECTORS: tuple[str, ...] = tuple(s for s in EQUITY_SECTORS if s != VS1_SECTOR)

DISCOVERY_START = "2012-01-01T00:00:00+00:00"
SPLIT = "2020-01-01T00:00:00+00:00"
END = "2026-07-01T00:00:00+00:00"

#: feature name -> (window W in calendar days, recency half-life tau in days)
FEATURES: dict[str, tuple[int, float]] = {"A90": (90, 45.0), "A30": (30, 15.0)}
HORIZONS: tuple[int, ...] = (5, 20)
PRIMARY_TRIAL = "A90|fwd20"
PRIMARY_DIRECTION = 1  # published prior: insider buying precedes outperformance

LEDGER_ID = "grid-granular-panel"
LEDGER_Q = 0.10
VS1_RUN_K = 1
OTHER_SECTORS_RUN_K = 2
BH_Q = 0.10
HOLDOUT_ALPHA = 0.05
MIN_N = 30  # decision dates with a finite IC
MIN_ENTITIES = 20  # issuers with a feature and a label on one decision date
PERMS = 20000
SENSITIVITY_PERMS = 2000
SEED = 20260927

# Event rules (Form 4, non-derivative table)
PURCHASE_CODE = "P"
PURCHASE_FORM = "4"
MIN_TRADE_USD = 10_000.0
MIN_SHARES = 100.0
MAX_FILING_LAG_DAYS = 365
ACTIVITY_LOOKBACK_DAYS = 730
NEW_YORK = ZoneInfo("America/New_York")
#: Reg S-T 13(a)(4): a Section 16 form submitted by 22:00 ET gets that day's
#: filing date, so the filing date alone is public no later than 22:00 ET.
KNOWN_AT_LOCAL = time(22, 0)
#: A decision is taken at the session close; entry at that close.
DECISION_LOCAL = time(16, 0)
MOMENTUM_SESSIONS = 20
#: Reported-only stratum (tracker audit): largest qualifying purchase line in the window.
LARGE_LINE_USD = 500_000.0
#: Verdict note when this share of buyer issuer-dates has no label (possible delisting).
MISSING_LABEL_WARNING = 0.05

# Stage-0 power gate (feature data only, synthetic outcomes, before any price read)
POWER_TARGET_ICS: tuple[float, ...] = (0.01, 0.02, 0.03)
POWER_GATE_IC = 0.01
POWER_GATE = 0.50
POWER_SIMS = 200
POWER_PERMS = 999

#: Price sources that are never admitted (S07/#642 basis contamination).
REFUSED_PRICE_SOURCES = frozenset({"yfinance", "yf", "kaggle_bulk", "yfinance_adj"})

TRUE_TOKENS = frozenset({"1", "TRUE", "T", "Y", "YES"})

_HOLIDAYS = (
    USFederalHolidayCalendar()
    .holidays("1990-01-01", "2040-12-31")
    .to_numpy()
    .astype("datetime64[D]")
)


def run_alpha(k: int, q: float = LEDGER_Q) -> float:
    """S11 alpha spending: run ``k`` of the ledger is issued ``q / (k (k + 1))``."""
    if k < 1:
        raise ValueError("ledger runs count from 1")
    return q / (k * (k + 1))


def trial_names() -> tuple[str, ...]:
    return tuple(f"{feature}|fwd{h}" for feature in FEATURES for h in HORIZONS)


# --- hashing ---------------------------------------------------------------------------


def lf_sha256(data: bytes) -> str:
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def prereg_body(text: str) -> str:
    """The pre-registration body: the text strictly between the two markers (LF)."""
    text = text.replace("\r\n", "\n")
    if text.count(BODY_START) != 1 or text.count(BODY_END) != 1:
        raise ValueError("pre-registration must carry exactly one body start and end marker")
    start = text.index(BODY_START) + len(BODY_START)
    end = text.index(BODY_END)
    if end <= start:
        raise ValueError("pre-registration body markers are out of order")
    return text[start:end]


def prereg_body_sha256(path: Path) -> str:
    return hashlib.sha256(prereg_body(Path(path).read_text(encoding="utf-8")).encode("utf-8")).hexdigest()


def check_prereg(repo_root: Path = REPO) -> str:
    """The repository pre-registration must still hash to the pinned body hash."""
    actual = prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != PREREG_BODY_SHA256:
        raise ValueError(
            f"pre-registration body hashes to {actual[:12]}, pinned {PREREG_BODY_SHA256[:12]}: "
            "the spec changed after registration (a change is a new version)"
        )
    return actual


def file_sha256(path: Path) -> str:
    return lf_sha256(Path(path).read_bytes())


def _finite_or_none(value: float) -> float | None:
    return float(value) if value is not None and np.isfinite(value) else None


# --- sector membership ---------------------------------------------------------------


def primary_sectors(sector_map: Mapping[str, Any]) -> dict[str, str | None]:
    """Primary sector per company ticker (``None`` = ambiguous, excluded everywhere).

    For each ticker and sector, the score is the largest actor ``weight`` of the
    ticker's ``type: company`` entries in that sector's subsectors. The primary
    sector is the unique argmax; a tie between sectors leaves the ticker
    unassigned. The map is undated, so this is today's view applied to history.
    """
    scores: dict[str, dict[str, float]] = {}
    for sector, body in sector_map.items():
        for sub in (body.get("subsectors") or {}).values():
            for actor in (sub or {}).get("actors") or ():
                ticker = actor.get("ticker")
                if not ticker or actor.get("type") != "company":
                    continue
                weight = float(actor.get("weight") or 0.0)
                current = scores.setdefault(str(ticker).strip().upper(), {})
                current[sector] = max(current.get(sector, -math.inf), weight)
    out: dict[str, str | None] = {}
    for ticker, by_sector in scores.items():
        best = max(by_sector.values())
        winners = [s for s, v in by_sector.items() if v == best]
        out[ticker] = winners[0] if len(winners) == 1 else None
    return out


def load_sector_map(repo_root: Path = REPO) -> dict:
    """The pinned sector map (refused if the file changed since registration)."""
    import yaml

    path = Path(repo_root) / SECTOR_MAP_PATH
    raw = path.read_bytes()
    if lf_sha256(raw) != SECTOR_MAP_SHA256:
        raise ValueError("analysis/sector_map_data.yaml changed since the pre-registration")
    try:
        loader = yaml.CSafeLoader
    except AttributeError:  # pragma: no cover - libyaml missing
        loader = yaml.SafeLoader
    return yaml.load(raw, Loader=loader)["SECTOR_MAP"]


def load_issuer_map(path: Path) -> pd.DataFrame:
    """ticker -> issuer CIK (SEC ``company_tickers.json`` rows, as CSV or JSON)."""
    path = Path(path)
    if path.suffix.lower() == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        rows = raw.values() if isinstance(raw, dict) else raw
        frame = pd.DataFrame(
            [{"ticker": r.get("ticker"), "cik": r.get("cik_str", r.get("cik"))} for r in rows]
        )
    else:
        frame = pd.read_csv(path, dtype=str)
        frame.columns = [c.strip().lower() for c in frame.columns]
        if "cik_str" in frame.columns and "cik" not in frame.columns:
            frame = frame.rename(columns={"cik_str": "cik"})
    if not {"ticker", "cik"} <= set(frame.columns):
        raise ValueError("issuer map needs ticker and cik columns")
    frame = frame[["ticker", "cik"]].dropna()
    frame["ticker"] = frame["ticker"].astype(str).str.strip().str.upper()
    frame["cik"] = pd.to_numeric(frame["cik"], errors="coerce")
    frame = frame.dropna().astype({"cik": "int64"})
    return frame.drop_duplicates().reset_index(drop=True)


def sector_universe(
    sector: str, sector_map: Mapping[str, Any], issuer_map: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """Sector members with a CIK: one row per issuer CIK (``ticker``, ``cik``).

    A ticker is a member iff its primary sector is ``sector``. Members without
    a CIK in the issuer map are dropped. When two member tickers share a CIK
    (share classes) the alphabetically first ticker represents the issuer.
    """
    if sector not in EQUITY_SECTORS:
        raise ValueError(f"{sector}: not one of the 11 pre-registered equity sectors")
    primaries = primary_sectors(sector_map)
    members = sorted(t for t, s in primaries.items() if s == sector)
    ambiguous = sorted(
        t
        for t, s in primaries.items()
        if s is None and _ticker_in_sector(sector_map, t, sector)
    )
    by_ticker = issuer_map.drop_duplicates("ticker").set_index("ticker")["cik"]
    rows = [(t, int(by_ticker[t])) for t in members if t in by_ticker.index]
    unmapped = [t for t in members if t not in by_ticker.index]
    frame = (
        pd.DataFrame(rows, columns=["ticker", "cik"])
        .sort_values(["cik", "ticker"])
        .drop_duplicates("cik")
        .sort_values("ticker")
        .reset_index(drop=True)
    )
    return frame, {
        "sector": sector,
        "rule": "sector-map primary sector (unique max actor weight; ties excluded)",
        "members": len(members),
        "with_cik": int(len(frame)),
        "unmapped_no_cik": unmapped,
        "ambiguous_tie_excluded": ambiguous,
        "share_class_duplicates_dropped": len(rows) - int(len(frame)),
    }


def _ticker_in_sector(sector_map: Mapping[str, Any], ticker: str, sector: str) -> bool:
    for sub in (sector_map.get(sector, {}).get("subsectors") or {}).values():
        for actor in (sub or {}).get("actors") or ():
            if str(actor.get("ticker") or "").strip().upper() == ticker and actor.get("type") == "company":
                return True
    return False


# --- Form 4 events ---------------------------------------------------------------------

REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "ACCESSION_NUMBER": ("ACCESSION_NUMBER", "ACCESSIONNUMBER", "ACCESSION_NO", "ACCESSION"),
    "FILING_DATE": ("FILING_DATE", "FILINGDATE", "FILED", "DATE_FILED"),
    "ISSUERCIK": ("ISSUERCIK", "ISSUER_CIK"),
    "DOCUMENT_TYPE": ("DOCUMENT_TYPE", "DOCUMENTTYPE", "FORM_TYPE", "FORM"),
    "TRANS_CODE": ("TRANS_CODE", "TRANSACTION_CODE", "TRANSCODE"),
}
PURCHASE_COLUMNS: dict[str, tuple[str, ...]] = {
    "TRANS_DATE": ("TRANS_DATE", "TRANSACTION_DATE", "TRANSDATE"),
    "TRANS_SHARES": ("TRANS_SHARES", "TRANSACTION_SHARES", "SHARES"),
    "TRANS_PRICEPERSHARE": ("TRANS_PRICEPERSHARE", "TRANS_PRICE_PER_SHARE", "PRICE_PER_SHARE", "PRICE"),
    "TRANS_ACQUIRED_DISP_CD": ("TRANS_ACQUIRED_DISP_CD", "ACQUIRED_DISPOSED_CODE", "ACQ_DISP_CD"),
}
OWNER_COLUMNS: dict[str, tuple[str, ...]] = {
    "RPTOWNERCIK": ("RPTOWNERCIK", "RPT_OWNER_CIK", "REPORTING_OWNER_CIK", "OWNER_CIK"),
}
OPTIONAL_COLUMNS: dict[str, tuple[str, ...]] = {
    "EQUITY_SWAP_INVOLVED": ("EQUITY_SWAP_INVOLVED", "EQUITYSWAPINVOLVED"),
    "AFF10B5ONE": ("AFF10B5ONE", "AFF_10B5_ONE", "RULE_10B5_1"),
    "AMENDED": ("AMENDED", "IS_AMENDED"),
    "NONDERIV_TRANS_SK": ("NONDERIV_TRANS_SK", "TRANS_SK"),
}
#: The SUBMISSION table (one row per accession, or per accession x owner).
SUBMISSION_COLUMNS: dict[str, tuple[str, ...]] = {
    key: REQUIRED_COLUMNS[key] for key in ("ACCESSION_NUMBER", "FILING_DATE", "ISSUERCIK")
}


def _normalise(name: str) -> str:
    return str(name).strip().upper().replace(" ", "_").replace("-", "_")


ALL_ALIASES = {
    alias
    for table in (REQUIRED_COLUMNS, PURCHASE_COLUMNS, OWNER_COLUMNS, OPTIONAL_COLUMNS)
    for names in table.values()
    for alias in names
}


def read_table(path: Path, issuers: Iterable[int] | None = None) -> pd.DataFrame:
    """A CSV/TSV (optionally .gz) or parquet file, every column as text.

    Parquet files are read column-selectively: only columns whose normalised
    name is a declared alias are loaded (the derived Form 3/4/5 file has
    about 8.5M rows).
    """
    path = Path(path)
    suffixes = [s.lower() for s in path.suffixes]
    if ".parquet" in suffixes:
        import pyarrow as pa
        import pyarrow.parquet as pq

        names = pq.ParquetFile(path).schema_arrow.names
        wanted = [n for n in names if _normalise(n) in ALL_ALIASES]
        table = pq.read_table(path, columns=wanted)
        issuer_col = next(
            (n for n in wanted if _normalise(n) in REQUIRED_COLUMNS["ISSUERCIK"]), None
        )
        if issuers is not None and issuer_col is not None:
            # Filter in Arrow before any pandas string conversion (8.5M rows).
            ciks = pd.to_numeric(
                table.column(issuer_col).to_pandas().astype("string").str.strip(), errors="coerce"
            )
            keep = ciks.isin({int(i) for i in issuers}).to_numpy()
            table = table.filter(pa.array(keep))
        frame = table.to_pandas().astype("string")
    else:
        sep = "\t" if (".tsv" in suffixes or ".txt" in suffixes) else ","
        frame = pd.read_csv(path, sep=sep, dtype=str, keep_default_na=False, na_values=[""])
    frame.columns = [_normalise(c) for c in frame.columns]
    return frame


def _pick(frame: pd.DataFrame, aliases: Mapping[str, tuple[str, ...]], required: bool) -> dict[str, str]:
    found = {}
    for canonical_name, names in aliases.items():
        name = next((n for n in names if n in frame.columns), None)
        if name is None and required:
            raise ValueError(f"Form 4 file lacks a {canonical_name} column (accepted: {names})")
        if name is not None:
            found[canonical_name] = name
    return found


def parse_dates(values: pd.Series) -> pd.Series:
    """ISO (YYYY-MM-DD[...]) or SEC data-set (DD-MON-YYYY) dates; NaT otherwise."""
    text = values.astype("string").str.strip().str.upper()
    iso = pd.to_datetime(text.str.slice(0, 10), format="%Y-%m-%d", errors="coerce")
    sec = pd.to_datetime(text, format="%d-%b-%Y", errors="coerce")
    return iso.fillna(sec)


def filing_known_at(filing_dates: pd.Series) -> pd.Series:
    """Filing date at 22:00 America/New_York, in UTC (Reg S-T 13(a)(4) cutoff)."""
    local = pd.to_datetime(filing_dates) + pd.Timedelta(
        hours=KNOWN_AT_LOCAL.hour, minutes=KNOWN_AT_LOCAL.minute
    )
    return local.dt.tz_localize(NEW_YORK).dt.tz_convert("UTC")


def _truthy(values: pd.Series) -> pd.Series:
    return values.astype("string").str.strip().str.upper().isin(TRUE_TOKENS).fillna(False).astype(bool)


def _to_int(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values.astype("string").str.strip(), errors="coerce")


@dataclass(frozen=True)
class Form4Events:
    """Open-market purchases and Section 16 activity, with the receipt of their build."""

    purchases: pd.DataFrame  # issuer_cik, actor, known_at, filing_date, trans_date, shares, price, n_reports
    activity: pd.DataFrame  # issuer_cik, known_at (one row per accession)
    receipt: dict

    @property
    def receipt_sha256(self) -> str:
        return digest(self.receipt)


def section16_accessions(submissions: pd.DataFrame, issuers: Iterable[int] | None = None) -> tuple[pd.DataFrame, dict]:
    """Every accession of the SUBMISSION table: (accession, issuer_cik, filing_date).

    Pre-registration §2.1/§2.2: Section 16 activity is *every* accession of the
    issuer -- any form type (3, 4, 5 and amendments), any code, holdings-only
    Form 3s and derivative-only Form 4s included. The non-derivative
    transaction table cannot supply that (it has rows only for accessions with a
    non-derivative transaction line), so the activity comes from the SEC
    SUBMISSION table (``derived/submissions.parquet``, built by
    ``scripts/build_form345_submissions.py``). One row per accession; an
    accession fanned out per reporting owner collapses.
    """
    frame = submissions.rename(columns=_normalise)
    cols = _pick(frame, SUBMISSION_COLUMNS, True)
    counts: dict[str, int] = {"submission_rows": int(len(frame))}
    if issuers is not None:
        wanted = {int(i) for i in issuers}
        frame = frame[_to_int(frame[cols["ISSUERCIK"]]).isin(wanted).to_numpy()]
        counts["submission_rows_in_issuer_filter"] = int(len(frame))
    out = pd.DataFrame(
        {
            "accession": frame[cols["ACCESSION_NUMBER"]].astype("string").str.strip(),
            "issuer_cik": _to_int(frame[cols["ISSUERCIK"]]),
            "filing_date": parse_dates(frame[cols["FILING_DATE"]]),
        }
    )
    valid = out["accession"].notna() & (out["accession"] != "") & out["issuer_cik"].notna() & out["filing_date"].notna()
    counts["submission_rows_excluded_missing_accession_issuer_or_filing_date"] = int((~valid).sum())
    out = out[valid.to_numpy()].drop_duplicates("accession").reset_index(drop=True)
    out["issuer_cik"] = out["issuer_cik"].astype("int64")
    counts["submission_accessions"] = int(len(out))
    return out, counts


def build_events(
    transactions: pd.DataFrame,
    owners: pd.DataFrame | None = None,
    *,
    submissions: pd.DataFrame,
    inputs: dict | None = None,
    issuers: Iterable[int] | None = None,
) -> Form4Events:
    """Apply the pre-registered event rules to the non-derivative transactions table.

    ``transactions``: one row per non-derivative transaction line (or one per
    line x reporting owner, as in the derived file, which fans joint filings
    out once per owner). ``owners``: the REPORTINGOWNER table (accession, owner
    CIK) when the transactions file does not carry the owner CIK.
    ``submissions``: the SUBMISSION table (every accession, see
    :func:`section16_accessions`); required, because Section 16 activity is
    defined over every accession, not only those with a non-derivative line.
    ``issuers``: optional issuer-CIK filter applied first (the sector universe).
    """
    if submissions is None:
        raise ValueError("the SUBMISSION table is required: Section 16 activity is every accession")
    frame = transactions.rename(columns=_normalise)
    cols = _pick(frame, REQUIRED_COLUMNS, True)
    pcols = _pick(frame, PURCHASE_COLUMNS, True)
    ocols = _pick(frame, OPTIONAL_COLUMNS, False)
    counts: dict[str, int] = {"rows": int(len(frame))}
    if issuers is not None:
        wanted = {int(i) for i in issuers}
        frame = frame[_to_int(frame[cols["ISSUERCIK"]]).isin(wanted).to_numpy()]
        counts["rows_in_issuer_filter"] = int(len(frame))

    accession = frame[cols["ACCESSION_NUMBER"]].astype("string").str.strip()
    issuer = _to_int(frame[cols["ISSUERCIK"]])
    filed = parse_dates(frame[cols["FILING_DATE"]])
    valid = accession.notna() & (accession != "") & issuer.notna() & filed.notna()
    counts["excluded_missing_accession_issuer_or_filing_date"] = int((~valid).sum())
    base = pd.DataFrame(
        {
            "accession": accession,
            "issuer_cik": issuer,
            "filing_date": filed,
            "document_type": frame[cols["DOCUMENT_TYPE"]].astype("string").str.strip().str.upper(),
            "code": frame[cols["TRANS_CODE"]].astype("string").str.strip().str.upper(),
            "trans_date": parse_dates(frame[pcols["TRANS_DATE"]]),
            "shares": pd.to_numeric(frame[pcols["TRANS_SHARES"]], errors="coerce"),
            "price": pd.to_numeric(frame[pcols["TRANS_PRICEPERSHARE"]], errors="coerce"),
            "acq_disp": frame[pcols["TRANS_ACQUIRED_DISP_CD"]].astype("string").str.strip().str.upper(),
            "swap": _truthy(frame[ocols["EQUITY_SWAP_INVOLVED"]]) if "EQUITY_SWAP_INVOLVED" in ocols else False,
            "plan_10b5_1": _truthy(frame[ocols["AFF10B5ONE"]]) if "AFF10B5ONE" in ocols else False,
            "amended": _truthy(frame[ocols["AMENDED"]]) if "AMENDED" in ocols else False,
            "line": (
                frame[ocols["NONDERIV_TRANS_SK"]].astype("string").str.strip()
                if "NONDERIV_TRANS_SK" in ocols
                else pd.Series(pd.NA, index=frame.index, dtype="string")
            ),
        }
    )[valid.to_numpy()]
    counts["flag_columns_present"] = sorted(k for k in ("AFF10B5ONE", "EQUITY_SWAP_INVOLVED", "AMENDED", "NONDERIV_TRANS_SK") if k in ocols)
    base["issuer_cik"] = base["issuer_cik"].astype("int64")

    # Owner CIK per accession (from the rows, else from the owner table).
    ocol = _pick(frame, OWNER_COLUMNS, False)
    if ocol:
        owner_rows = pd.DataFrame(
            {"accession": accession, "owner_cik": _to_int(frame[ocol["RPTOWNERCIK"]])}
        )[valid.to_numpy()]
    elif owners is not None:
        owner_frame = owners.rename(columns=_normalise)
        oc = _pick(owner_frame, {"ACCESSION_NUMBER": REQUIRED_COLUMNS["ACCESSION_NUMBER"], **OWNER_COLUMNS}, True)
        owner_rows = pd.DataFrame(
            {
                "accession": owner_frame[oc["ACCESSION_NUMBER"]].astype("string").str.strip(),
                "owner_cik": _to_int(owner_frame[oc["RPTOWNERCIK"]]),
            }
        )
    else:
        raise ValueError("no reporting-owner CIK: pass the REPORTINGOWNER table as owners")
    actors = owner_rows.dropna().groupby("accession")["owner_cik"].min().astype("int64")

    # Section 16 activity: every accession of the issuer (SUBMISSION table, any
    # form type incl. holdings-only Form 3 and derivative-only Form 4), plus
    # any transaction accession the submission table lacks (counted: it
    # should be zero). Where both carry an accession, the submission row wins.
    filed_accessions, submission_counts = section16_accessions(submissions, issuers)
    counts.update(submission_counts)
    from_transactions = base.drop_duplicates("accession")[["accession", "issuer_cik", "filing_date"]]
    missing = ~from_transactions["accession"].isin(set(filed_accessions["accession"]))
    counts["transaction_accessions_missing_from_submissions"] = int(missing.sum())
    activity = (
        pd.concat([filed_accessions, from_transactions[missing.to_numpy()]], ignore_index=True)
        .drop_duplicates("accession")
        .assign(known_at=lambda d: filing_known_at(d["filing_date"]))
        [["issuer_cik", "known_at"]]
        .sort_values(["issuer_cik", "known_at"])
        .reset_index(drop=True)
    )
    counts["activity_accessions"] = int(len(activity))

    # One row per (accession, transaction): the owner join may have repeated it.
    purchase = base[base["code"] == PURCHASE_CODE]
    counts["code_p_rows"] = int(len(purchase))
    steps = (
        ("excluded_not_form_4", purchase["document_type"] != PURCHASE_FORM),
        ("excluded_amended", purchase["amended"].astype(bool)),
        ("excluded_not_acquired", purchase["acq_disp"] != "A"),
        ("excluded_equity_swap", purchase["swap"].astype(bool)),
        ("excluded_10b5_1_flag", purchase["plan_10b5_1"].astype(bool)),
    )
    keep = pd.Series(True, index=purchase.index)
    for name, mask in steps:
        mask = mask.fillna(True).astype(bool) & keep
        counts[name] = int(mask.sum())
        keep &= ~mask
    lag = (purchase["filing_date"] - purchase["trans_date"]).dt.days
    bad_date = keep & (
        purchase["trans_date"].isna() | (lag < 0) | (lag > MAX_FILING_LAG_DAYS)
    ).fillna(True)
    counts["excluded_transaction_date"] = int(bad_date.sum())
    keep &= ~bad_date
    value = purchase["shares"] * purchase["price"]
    small = keep & (
        purchase["shares"].isna()
        | purchase["price"].isna()
        | (purchase["price"] <= 0)
        | (purchase["shares"] < MIN_SHARES)
        | (value < MIN_TRADE_USD)
    ).fillna(True)
    counts["excluded_small_or_unpriced"] = int(small.sum())
    keep &= ~small
    kept = purchase[keep].copy()
    kept["actor"] = kept["accession"].map(actors)
    no_owner = kept["actor"].isna()
    counts["excluded_no_owner_cik"] = int(no_owner.sum())
    kept = kept[~no_owner]
    kept["actor"] = kept["actor"].astype("int64")
    # Owner-join repeats of one transaction line: the line key when present.
    has_line = kept["line"].notna()
    kept = pd.concat(
        [
            kept[has_line].drop_duplicates(["accession", "line"]),
            kept[~has_line].drop_duplicates(["accession", "trans_date", "shares", "price"]),
        ]
    )
    counts["purchase_transactions"] = int(len(kept))

    # One economic purchase reported in several accessions (joint filers):
    # earliest filing, smallest owner CIK.
    kept["shares_key"] = kept["shares"].round(0)
    kept["price_key"] = kept["price"].round(2)
    grouped = kept.groupby(["issuer_cik", "trans_date", "shares_key", "price_key"], as_index=False).agg(
        filing_date=("filing_date", "min"),
        actor=("actor", "min"),
        shares=("shares", "first"),
        price=("price", "first"),
        n_reports=("accession", "nunique"),
    )
    counts["purchases_after_dedup"] = int(len(grouped))
    grouped["known_at"] = filing_known_at(grouped["filing_date"])
    grouped["value"] = grouped["shares"] * grouped["price"]
    purchases = grouped[
        ["issuer_cik", "actor", "known_at", "filing_date", "trans_date", "shares", "price", "value",
         "n_reports"]
    ].sort_values(["issuer_cik", "known_at", "actor"]).reset_index(drop=True)
    receipt = {
        "version": VERSION,
        "inputs": inputs or {},
        "rules": {
            "code": PURCHASE_CODE,
            "form": PURCHASE_FORM,
            "acquired_disposed": "A",
            "exclude": [
                "every form type other than 4 (4/A, 5, 5/A)",
                "rows flagged amended",
                "equity swaps (when flagged)",
                "10b5-1 (only when an AFF10B5ONE flag is present)",
            ],
            "min_shares": MIN_SHARES,
            "min_trade_usd": MIN_TRADE_USD,
            "max_filing_lag_days": MAX_FILING_LAG_DAYS,
            "actor": "smallest reporting-owner CIK on the accession",
            "dedup": "(issuer CIK, transaction date, round(shares), round(price, 2)): earliest filing, smallest actor",
            "known_at": "filing date 22:00 America/New_York",
            "section16_activity": "every accession of the SUBMISSION table (any form type, any code, "
                                  "amended or not), plus transaction accessions missing from it",
        },
        "counts": counts,
        "purchases_sha256": digest(_records(purchases)),
        "activity_sha256": digest(_records(activity)),
    }
    return Form4Events(purchases=purchases, activity=activity, receipt=receipt)


def _records(frame: pd.DataFrame) -> list:
    out = []
    for row in frame.itertuples(index=False):
        out.append(
            [
                (value.isoformat() if hasattr(value, "isoformat") else
                 (None if isinstance(value, float) and not math.isfinite(value) else
                  (int(value) if isinstance(value, (np.integer,)) else
                   (float(value) if isinstance(value, (np.floating,)) else value))))
                for value in row
            ]
        )
    return out


def data_sha256(path: Path) -> str:
    """sha256 of a data file's exact bytes (streamed; no line-ending normalisation)."""
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_events(
    path: Path,
    owners_path: Path | None = None,
    issuers: Iterable[int] | None = None,
    *,
    submissions_path: Path,
) -> Form4Events:
    """Events from the derived non-derivative file plus the SUBMISSION table (both required)."""
    inputs = {
        "transactions": {"name": Path(path).name, "sha256": data_sha256(path)},
        "submissions": {"name": Path(submissions_path).name, "sha256": data_sha256(submissions_path)},
    }
    owners = None
    if owners_path is not None:
        owners = read_table(owners_path)
        inputs["owners"] = {"name": Path(owners_path).name, "sha256": data_sha256(owners_path)}
    if issuers is not None:
        issuers = sorted({int(i) for i in issuers})
        inputs["issuer_filter_sha256"] = digest(issuers)
    return build_events(
        read_table(path, issuers),
        owners,
        submissions=read_table(submissions_path, issuers),
        inputs=inputs,
        issuers=issuers,
    )


# --- features --------------------------------------------------------------------------


def decision_instants(sessions: Iterable[date]) -> pd.DatetimeIndex:
    """16:00 America/New_York of each session date, in UTC."""
    days = pd.DatetimeIndex([pd.Timestamp(d) for d in sessions])
    local = days + pd.Timedelta(hours=DECISION_LOCAL.hour, minutes=DECISION_LOCAL.minute)
    return local.tz_localize(NEW_YORK).tz_convert("UTC")


def proxy_sessions(start: date, end: date) -> list[date]:
    """Weekdays that are not US federal holidays (Stage-0 power only, no price read)."""
    days = np.arange(np.datetime64(start, "D"), np.datetime64(end, "D"))
    mask = np.is_busday(days, holidays=_HOLIDAYS)
    return [pd.Timestamp(d).date() for d in days[mask]]


def _ns(stamps: pd.Series) -> np.ndarray:
    """UTC nanoseconds since the epoch (independent of the column's time unit)."""
    return stamps.dt.tz_convert("UTC").dt.as_unit("ns").astype("int64").to_numpy()


def density(
    purchases: pd.DataFrame,
    issuers: Iterable[int],
    decisions: pd.DatetimeIndex,
    window_days: int,
    tau_days: float,
) -> pd.DataFrame:
    """``A(e, t)``: sum over distinct actors of exp(-age/tau) of their latest in-window purchase.

    An event counts at decision ``t`` iff ``t - W < known_at <= t``. Each actor
    counts once per issuer (its most recent qualifying event). Returns a
    decisions x issuers frame of floats (0.0 where there is no event).
    """
    if decisions.tz is None:
        raise ValueError("decisions must be tz-aware")
    issuers = [int(i) for i in issuers]
    t_ns = decisions.tz_convert("UTC").as_unit("ns").asi8.astype(np.float64)
    day_ns = 86_400e9
    window_ns = window_days * day_ns
    out = np.zeros((len(decisions), len(issuers)))
    by_issuer = {cik: g for cik, g in purchases.groupby("issuer_cik")}
    for j, cik in enumerate(issuers):
        events = by_issuer.get(cik)
        if events is None or events.empty:
            continue
        events = events.sort_values(["actor", "known_at"])
        known = _ns(events["known_at"]).astype(np.float64)
        age = t_ns[None, :] - known[:, None]  # events x decisions
        inside = (age >= 0) & (age < window_ns)
        weight = np.where(inside, np.exp(-age / (tau_days * day_ns)), 0.0)
        actors = events["actor"].to_numpy()
        starts = np.flatnonzero(np.r_[True, actors[1:] != actors[:-1]])
        per_actor = np.maximum.reduceat(weight, starts, axis=0)  # latest event = largest weight
        out[:, j] = per_actor.sum(axis=0)
    return pd.DataFrame(out, index=decisions, columns=issuers)


def active_mask(
    activity: pd.DataFrame,
    issuers: Iterable[int],
    decisions: pd.DatetimeIndex,
    lookback_days: int = ACTIVITY_LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Issuer is a Section 16 filer at ``t``: an accession known in (t - lookback, t]."""
    issuers = [int(i) for i in issuers]
    t = decisions.tz_convert("UTC").as_unit("ns").asi8
    lo = t - np.int64(lookback_days) * np.int64(86_400_000_000_000)
    out = np.zeros((len(decisions), len(issuers)), dtype=bool)
    by_issuer = {cik: g for cik, g in activity.groupby("issuer_cik")}
    for j, cik in enumerate(issuers):
        events = by_issuer.get(cik)
        if events is None:
            continue
        known = np.sort(_ns(events["known_at"]))
        count = np.searchsorted(known, t, side="right") - np.searchsorted(known, lo, side="right")
        out[:, j] = count > 0
    return pd.DataFrame(out, index=decisions, columns=issuers)


def largest_value(
    purchases: pd.DataFrame, issuers: Iterable[int], decisions: pd.DatetimeIndex, window_days: int
) -> pd.DataFrame:
    """Largest qualifying purchase value (USD) known in (t - W, t]; 0 when none."""
    issuers = [int(i) for i in issuers]
    t_ns = decisions.tz_convert("UTC").as_unit("ns").asi8.astype(np.float64)
    window_ns = window_days * 86_400e9
    out = np.zeros((len(decisions), len(issuers)))
    by_issuer = {cik: g for cik, g in purchases.groupby("issuer_cik")}
    for j, cik in enumerate(issuers):
        events = by_issuer.get(cik)
        if events is None or events.empty:
            continue
        age = t_ns[None, :] - _ns(events["known_at"]).astype(np.float64)[:, None]
        inside = (age >= 0) & (age < window_ns)
        out[:, j] = np.where(inside, events["value"].to_numpy(dtype=float)[:, None], 0.0).max(axis=0)
    return pd.DataFrame(out, index=decisions, columns=issuers)


def entry_positions(purchases: pd.DataFrame, sessions: Iterable[date]) -> pd.DataFrame:
    """One position per (issuer, entry session): the tracker-v2 event key.

    The entry session of a purchase is the first session whose 16:00 ET close
    is strictly after its known_at (never a close printed before the filing
    was public). Purchases sharing an issuer and entry session are one
    position, carrying the distinct actors, the purchase count, the total and
    the largest line value. A purchase known after the last session gets no
    entry and is kept with status ``no_entry_session`` (reported, not dropped).
    """
    closes = decision_instants(sorted(set(sessions)))
    position = np.searchsorted(_index_ns(closes), _ns(purchases["known_at"]), side="right")
    has = position < len(closes)
    entry = pd.Series(pd.NaT, index=purchases.index, dtype="datetime64[ns, UTC]")
    entry[has] = closes.as_unit("ns")[position[has]]
    frame = purchases.assign(entry_close=entry)
    grouped = frame.groupby(["issuer_cik", "entry_close"], dropna=False).agg(
        actors=("actor", lambda a: sorted({int(x) for x in a})),
        purchases=("actor", "size"),
        total_value=("value", "sum"),
        largest_value=("value", "max"),
        first_known_at=("known_at", "min"),
        last_known_at=("known_at", "max"),
    ).reset_index()
    grouped["n_actors"] = grouped["actors"].map(len)
    grouped["status"] = np.where(grouped["entry_close"].isna(), "no_entry_session", "opened")
    return grouped


def _index_ns(index: pd.DatetimeIndex) -> np.ndarray:
    return index.tz_convert("UTC").as_unit("ns").asi8


def feature_panel(
    events: Form4Events, issuers: Iterable[int], decisions: pd.DatetimeIndex, feature: str
) -> pd.DataFrame:
    """The declared feature, NaN where the issuer is not a Section 16 filer at ``t``."""
    window, tau = FEATURES[feature]
    issuers = list(issuers)
    values = density(events.purchases, issuers, decisions, window, tau)
    return values.where(active_mask(events.activity, issuers, decisions))


# --- prices ----------------------------------------------------------------------------

HEX64 = frozenset("0123456789abcdef")


@dataclass(frozen=True)
class PriceManifest:
    """The admitted-price contract, frozen before any label is computed.

    ``source``: the ``source_catalog.name`` every read is constrained to.
    ``series_template``: e.g. ``"YF:{ticker}:close"`` (the id the source writes).
    ``basis``: the declared adjustment basis of the closes.
    ``admitted``: tickers that passed the basis probe (the benchmark included).
    ``probe_report_sha256``: the probe report this list came from.
    """

    source: str
    series_template: str
    basis: str
    benchmark: str
    admitted: tuple[str, ...]
    probe_report_sha256: str

    def validate(self) -> None:
        if not self.source or self.source.strip().lower() in REFUSED_PRICE_SOURCES:
            raise ValueError(f"price source {self.source!r} is refused (unverified basis)")
        if "{ticker}" not in self.series_template:
            raise ValueError("series_template must contain {ticker}")
        if self.benchmark not in self.admitted:
            raise ValueError("the benchmark must be an admitted ticker")
        if len(self.probe_report_sha256) != 64 or set(self.probe_report_sha256) - HEX64:
            raise ValueError("probe_report_sha256 must be a sha256 hex digest")
        if not self.basis:
            raise ValueError("declare the price basis")

    @classmethod
    def from_file(cls, path: Path) -> "PriceManifest":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        manifest = cls(**{**raw, "admitted": tuple(sorted(raw["admitted"]))})
        manifest.validate()
        return manifest

    def digest(self) -> str:
        """Content hash of the manifest (what ``inputs_frozen`` pins)."""
        return digest({**asdict(self), "admitted": list(self.admitted)})


_PRICE_LOADER = object()
_KEY_TOKEN = object()


class DiscoveryKey:
    """Proof that ``discovery_opened`` is in the pinned registry chain and witnessed off-host.

    Issued only by :func:`resume_discovery`, after the chain showed one
    ``discovery_opened`` with a matching ``inputs_frozen`` record and the
    off-host anchor log already contained its head. Carries the frozen
    inputs (price-manifest digest, probe report, as_of_ts, ...) that every
    discovery price read is checked against.
    """

    window = "discovery"

    def __init__(self, token: object, inputs_frozen_sha256: str, inputs: dict, *,
                 log_dir: Path | None = None, witness_tip: str | None = None) -> None:
        if token is not _KEY_TOKEN:
            raise TypeError("a DiscoveryKey is issued only by resume_discovery")
        if log_dir is None or witness_tip is None:
            raise TypeError("a DiscoveryKey needs its registry and off-host witness")
        self.inputs_frozen_sha256 = inputs_frozen_sha256
        self.inputs = dict(inputs)
        self.as_of_ts = stamp(inputs["as_of_ts"])
        self.log_dir = Path(log_dir)
        self.witness_tip = witness_tip


class HoldoutKey:
    """Proof that the holdout was opened with the flag, the pinned hash and a
    ``holdout_opened`` record in the pinned registry chain that the off-host
    anchor log already witnesses (issued only by :func:`resume_holdout`)."""

    window = "holdout"

    def __init__(self, token: object, frozen_sha256: str, inputs: dict, *,
                 log_dir: Path | None = None, witness_tip: str | None = None) -> None:
        if token is not _HOLDOUT_TOKEN:
            raise TypeError("a HoldoutKey is issued only by resume_holdout")
        if log_dir is None or witness_tip is None:
            raise TypeError("a HoldoutKey needs its registry and off-host witness")
        self.frozen_sha256 = frozen_sha256
        self.inputs = dict(inputs)
        self.as_of_ts = stamp(inputs["as_of_ts"])
        self.log_dir = Path(log_dir)
        self.witness_tip = witness_tip


_HOLDOUT_TOKEN = object()


def check_holdout_request(
    frozen: dict, *, allow_holdout: bool, prereg_sha256: str, repo_root: Path = REPO
) -> dict:
    """The holdout's non-registry preconditions (no side effect); returns the payload."""
    if allow_holdout is not True:
        raise PermissionError("holdout evaluation needs an explicit allow_holdout=True")
    if prereg_sha256 != PREREG_BODY_SHA256:
        raise PermissionError("the pre-registration hash given does not match the pinned one")
    check_prereg(repo_root)
    payload = frozen.get("payload") or {}
    if digest(payload) != frozen.get("sha256"):
        raise PermissionError("frozen discovery manifest changed")
    if payload.get("prereg_sha256") != PREREG_BODY_SHA256:
        raise PermissionError("the discovery was not run under this pre-registration")
    if payload.get("state") != "DISCOVERY_FROZEN":
        raise PermissionError("no frozen discovery to evaluate")
    return payload


class PricePanel:
    """Closes read through ``store.observations.read_window``, with a receipt."""

    def __init__(
        self,
        *,
        token: object,
        manifest: PriceManifest,
        start: date,
        as_of: date,
        as_of_ts: datetime,
        window: str,
        data: dict[str, tuple[observations.Observation, ...]],
    ) -> None:
        if token is not _PRICE_LOADER:
            raise TypeError("PricePanel is built only by load_price_panel")
        self.manifest = manifest
        self.start, self.as_of, self.as_of_ts, self.window = start, as_of, as_of_ts, window
        self._data = dict(data)
        self.receipt = self._receipt()
        self.receipt_sha = digest(self.receipt)

    def _receipt(self) -> dict:
        return {
            "reader": "store.observations.read_window",
            "manifest": asdict(self.manifest),
            "window": self.window,
            "start": self.start.isoformat(),
            "as_of": self.as_of.isoformat(),
            "as_of_ts": self.as_of_ts.isoformat(),
            "series": {
                ticker: {
                    "n": len(obs),
                    "first": obs[0].obs_date.isoformat() if obs else None,
                    "last": obs[-1].obs_date.isoformat() if obs else None,
                    "sha256": digest([[o.obs_date.isoformat(), o.value] for o in obs]),
                }
                for ticker, obs in sorted(self._data.items())
            },
        }

    def verify(self) -> None:
        if digest(self._receipt()) != self.receipt_sha:
            raise ValueError("price panel changed after its read")

    def closes(self) -> pd.DataFrame:
        """Session-date x ticker closes; sessions are the benchmark's dates."""
        bench = self._data[self.manifest.benchmark]
        index = pd.DatetimeIndex([pd.Timestamp(o.obs_date) for o in bench])
        frame = pd.DataFrame(index=index)
        for ticker, obs in self._data.items():
            series = pd.Series(
                [o.value for o in obs],
                index=pd.DatetimeIndex([pd.Timestamp(o.obs_date) for o in obs]),
                dtype=float,
            )
            frame[ticker] = series.reindex(index)
        return frame


def load_price_panel(
    conn,
    manifest: PriceManifest,
    tickers: Iterable[str],
    *,
    start: date,
    as_of: date,
    window: str,
    key: DiscoveryKey | HoldoutKey,
) -> PricePanel:
    """Read admitted closes, bounded so discovery never sees a price on/after the split.

    No price is read without a registry key: a :class:`DiscoveryKey` (the
    chain carries ``inputs_frozen`` and now ``discovery_opened``) for the
    discovery window, a :class:`HoldoutKey` (``holdout_opened``) for the
    holdout. The manifest must be the frozen one and the read instant is the
    frozen ``as_of_ts``.
    """
    manifest.validate()
    if window == "discovery":
        if not isinstance(key, DiscoveryKey):
            raise PermissionError("discovery prices need a DiscoveryKey from resume_discovery")
        if as_of >= stamp(SPLIT).date():
            raise PermissionError("discovery reads stop before the split date")
    elif window == "holdout":
        if not isinstance(key, HoldoutKey):
            raise PermissionError("holdout prices need a HoldoutKey from resume_holdout")
        if as_of >= stamp(END).date():
            raise PermissionError("holdout reads stop before the end of the frozen window")
    else:
        raise ValueError("window must be discovery or holdout")
    if manifest.digest() != key.inputs.get("price_manifest_sha256"):
        raise PermissionError("price manifest differs from the one in inputs_frozen")
    if manifest.probe_report_sha256 != key.inputs.get("probe_report_sha256"):
        raise PermissionError("price manifest's probe report differs from the one in inputs_frozen")
    as_of_ts = key.as_of_ts
    wanted = sorted(set(tickers) | {manifest.benchmark})
    refused = [t for t in wanted if t not in manifest.admitted]
    if refused:
        raise PermissionError(f"tickers not in the admitted-price manifest: {refused[:10]}")
    data = {}
    for ticker in wanted:
        data[ticker] = tuple(
            observations.read_window(
                conn,
                manifest.series_template.format(ticker=ticker),
                source=manifest.source,
                start=start,
                as_of=as_of,
                as_of_ts=as_of_ts,
            )
        )
    if not data[manifest.benchmark]:
        raise ValueError("no benchmark closes in the read window")
    panel = PricePanel(
        token=_PRICE_LOADER,
        manifest=manifest,
        start=start,
        as_of=as_of,
        as_of_ts=as_of_ts,
        window=window,
        data=data,
    )
    # Every read is recorded; a re-read (a resumed run) must return the same prices.
    record_prices_read(key, panel.receipt_sha)
    return panel


# --- labels (split first, then label) ------------------------------------------------


@dataclass
class TrialPanel:
    """One trial's decisions x issuers feature and label matrices for one window."""

    trial: str
    window: str
    horizon: int
    decision_at: list[str]
    label_end: list[str]
    entities: list[str]
    feature: np.ndarray  # decisions x entities (NaN = abstain)
    label: np.ndarray  # decisions x entities (NaN = no label)
    momentum: np.ndarray | None = None  # past relative return (baseline only)
    largest: np.ndarray | None = None  # largest purchase line in the window (stratum only)

    def as_record(self) -> dict:
        def matrix(values):
            return None if values is None else [[_finite_or_none(v) for v in row] for row in values]

        return {
            "trial": self.trial,
            "window": self.window,
            "horizon": self.horizon,
            "decision_at": self.decision_at,
            "label_end": self.label_end,
            "entities": self.entities,
            "feature": matrix(self.feature),
            "label": matrix(self.label),
        }


def window_bounds(window: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    if window == "discovery":
        return pd.Timestamp(stamp(DISCOVERY_START)), pd.Timestamp(stamp(SPLIT))
    if window == "holdout":
        return pd.Timestamp(stamp(SPLIT)), pd.Timestamp(stamp(END))
    raise ValueError("window must be discovery or holdout")


def relative_labels(
    closes: pd.DataFrame, benchmark: str, tickers: list[str], horizon: int, window: str
) -> tuple[list[int], np.ndarray, np.ndarray]:
    """Horizon-spaced decisions and (issuer - benchmark) forward returns in one window.

    Every close outside ``[lo, hi)`` is blanked before labelling (purge by
    construction). Decisions are the window's sessions every ``horizon``
    sessions from its first session; a decision whose label end is not a
    session inside the window is dropped. Returns (decision positions,
    labels, momentum baseline).
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    lo, hi = window_bounds(window)
    dates = closes.index
    day = pd.DatetimeIndex(dates).tz_localize("UTC")
    inside = np.asarray((day >= lo.normalize()) & (day < hi.normalize()))
    raw = closes[tickers + [benchmark]].to_numpy(dtype=float)
    visible = raw.copy()
    visible[~inside, :] = np.nan
    positions = [i for i in np.flatnonzero(inside)[::horizon] if i + horizon < len(dates) and inside[i + horizon]]
    labels, momentum = [], []
    for i in positions:
        start, end = visible[i], visible[i + horizon]
        with np.errstate(divide="ignore", invalid="ignore"):
            rel = (end[:-1] / start[:-1] - 1.0) - (end[-1] / start[-1] - 1.0)
        rel[~np.isfinite(rel)] = np.nan
        labels.append(rel)
        # Baseline: past MOMENTUM_SESSIONS relative return, from closes <= t only.
        if i >= MOMENTUM_SESSIONS:
            past, now = raw[i - MOMENTUM_SESSIONS], raw[i]
            with np.errstate(divide="ignore", invalid="ignore"):
                mom = (now[:-1] / past[:-1] - 1.0) - (now[-1] / past[-1] - 1.0)
            mom[~np.isfinite(mom)] = np.nan
        else:
            mom = np.full(len(tickers), np.nan)
        momentum.append(mom)
    shape = (len(positions), len(tickers))
    return (
        positions,
        np.array(labels).reshape(shape),
        np.array(momentum).reshape(shape),
    )


def build_trial_panels(
    events: Form4Events,
    universe: pd.DataFrame,
    prices: PricePanel,
    window: str,
) -> dict[str, TrialPanel]:
    """Every declared trial's panel for one window, from verified inputs only."""
    prices.verify()
    if prices.window != window:
        raise ValueError("price panel window differs")
    closes = prices.closes()
    benchmark = prices.manifest.benchmark
    universe = universe[universe["ticker"].isin(closes.columns)]
    tickers = list(universe["ticker"])
    ciks = list(universe["cik"].astype(int))
    panels = {}
    for h in HORIZONS:
        positions, labels, momentum = relative_labels(closes, benchmark, tickers, h, window)
        sessions = [closes.index[i].date() for i in positions]
        decided = decision_instants(sessions)
        ends = decision_instants([closes.index[i + h].date() for i in positions])
        for name in FEATURES:
            feature = feature_panel(events, ciks, decided, name).to_numpy(dtype=float)
            # abstain where the issuer has no close at t (not trading)
            listed = np.isfinite(closes[tickers].to_numpy(dtype=float)[positions]) if positions else np.zeros((0, len(tickers)), bool)
            feature = np.where(listed, feature, np.nan)
            largest = largest_value(events.purchases, ciks, decided, FEATURES[name][0])
            trial = f"{name}|fwd{h}"
            panels[trial] = TrialPanel(
                trial=trial,
                window=window,
                horizon=h,
                decision_at=[d.isoformat() for d in decided],
                label_end=[d.isoformat() for d in ends],
                entities=tickers,
                feature=feature,
                label=labels,
                momentum=momentum,
                largest=np.where(np.isfinite(feature), largest.to_numpy(dtype=float), np.nan),
            )
    return panels


def validate_panel(panel: TrialPanel) -> None:
    """Non-overlapping, ordered, in-window decisions and finite-or-NaN matrices."""
    lo, hi = window_bounds(panel.window)
    previous_end = None
    for decided, ended in zip(panel.decision_at, panel.label_end):
        d, e = stamp(decided), stamp(ended)
        if e <= d or d < lo or e >= hi:
            raise ValueError("decision or label outside its window")
        if previous_end is not None and d < previous_end:
            raise ValueError("overlapping outcome windows")
        previous_end = e
    shape = (len(panel.decision_at), len(panel.entities))
    for matrix in (panel.feature, panel.label):
        if matrix.shape != shape or np.isinf(matrix).any():
            raise ValueError("panel matrices must be decisions x entities, finite or NaN")


# --- statistics ------------------------------------------------------------------------


def _row_ranks(values: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Average ranks within each row over ``mask`` (vectorised; NaN outside the mask)."""
    values = np.asarray(values, dtype=float)
    if values.size == 0:
        return np.full(values.shape, np.nan)
    rows, cols = values.shape
    x = np.where(mask, values, np.inf)  # masked cells sort last, in their own group
    order = np.argsort(x, axis=1, kind="mergesort")
    ordered = np.take_along_axis(x, order, axis=1)
    position = np.broadcast_to(np.arange(cols), (rows, cols))
    starts = np.ones((rows, cols), dtype=bool)
    starts[:, 1:] = ordered[:, 1:] != ordered[:, :-1]
    ends = np.ones((rows, cols), dtype=bool)
    ends[:, :-1] = starts[:, 1:]
    first = np.maximum.accumulate(np.where(starts, position, 0), axis=1)
    last = np.minimum.accumulate(np.where(ends, position, cols)[:, ::-1], axis=1)[:, ::-1]
    ranks = np.empty((rows, cols))
    np.put_along_axis(ranks, order, (first + last) / 2.0 + 1.0, axis=1)
    return np.where(mask, ranks, np.nan)


def _rank_rows(values: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Per-row average ranks over ``mask``, centred to mean 0 (NaN elsewhere)."""
    ranks = _row_ranks(values, mask)
    with np.errstate(invalid="ignore"):
        centre = np.nanmean(np.where(mask, ranks, np.nan), axis=1, keepdims=True) if ranks.size else 0
    return ranks - centre


def rank_ic_series(
    feature: np.ndarray, label: np.ndarray, min_entities: int = MIN_ENTITIES
) -> tuple[np.ndarray, np.ndarray]:
    """Per-date Spearman IC over issuers with both values; NaN when it abstains.

    A date abstains with fewer than ``min_entities`` joint observations or a
    constant feature or label cross-section (e.g. no buyer anywhere).
    """
    feature = np.asarray(feature, dtype=float)
    label = np.asarray(label, dtype=float)
    mask = np.isfinite(feature) & np.isfinite(label)
    counts = mask.sum(axis=1).astype(int)
    ic = np.full(feature.shape[0], np.nan)
    use = counts >= min_entities
    if not use.any():
        return ic, counts
    m = mask[use]
    with np.errstate(invalid="ignore"):
        x = np.nan_to_num(_rank_rows(feature[use], m))
        y = np.nan_to_num(_rank_rows(label[use], m))
    scale = np.sqrt((x * x).sum(axis=1) * (y * y).sum(axis=1))
    with np.errstate(divide="ignore", invalid="ignore"):
        values = np.where(scale > 1e-12, (x * y).sum(axis=1) / scale, np.nan)
    ic[use] = values
    return ic, counts


def block_signs(n: int, block: int, perms: int, seed: int) -> np.ndarray:
    """perms x n +/-1 signs, constant within contiguous blocks (seeded by shape only)."""
    rng = np.random.default_rng([seed, n, block, perms, 2])
    n_blocks = -(-n // block)
    signs = rng.choice(np.array([-1.0, 1.0]), size=(perms, n_blocks))
    return signs[:, np.arange(n) // block]


def signflip_pvalues(ic: np.ndarray, block: int, perms: int, seed: int, direction: int) -> tuple[float, float, float]:
    """Mean IC, its two-sided p and its one-sided p (in ``direction``) under block sign-flips."""
    ic = np.asarray(ic, dtype=float)
    observed = float(ic.mean())
    null = block_signs(len(ic), block, perms, seed) @ ic / len(ic)
    two = (1 + int((np.abs(null) >= abs(observed) - 1e-12).sum())) / (perms + 1)
    one = (1 + int((direction * null >= direction * observed - 1e-12).sum())) / (perms + 1)
    return observed, two, one


def time_alignment_pvalue(
    feature: np.ndarray, label: np.ndarray, rows: np.ndarray, block: int, perms: int, seed: int,
    min_entities: int = MIN_ENTITIES,
) -> tuple[float, float]:
    """Sensitivity null: permute blocks of outcome cross-sections over dates.

    The feature cross-sections stay in place (the time-series loop's null,
    lifted to panels). ``C[t, s]`` is the rank correlation of the feature at
    ``t`` with the outcomes at ``s`` over their common issuers; the observed
    statistic is the mean of the diagonal, the null the mean of
    ``C[t, pi(t)]``. Two-sided around 0, like ``block_permutation_pvalue``.
    """
    f, y = feature[rows], label[rows]
    joint = np.isfinite(f) & np.isfinite(y)
    fr, yr = _rank_rows(f, joint), _rank_rows(y, joint)
    mf, my = np.isfinite(fr), np.isfinite(yr)
    f0, y0 = np.nan_to_num(fr), np.nan_to_num(yr)
    n = mf.astype(float) @ my.T.astype(float)
    sx, sy = f0 @ my.T.astype(float), mf.astype(float) @ y0.T
    sxy = f0 @ y0.T
    sxx, syy = (f0**2) @ my.T.astype(float), mf.astype(float) @ (y0**2).T
    with np.errstate(divide="ignore", invalid="ignore"):
        cov = sxy - sx * sy / n
        var = (sxx - sx**2 / n) * (syy - sy**2 / n)
        c = np.where((n >= min_entities) & (var > 0), cov / np.sqrt(var), np.nan)
    observed = float(np.nanmean(np.diag(c)))
    index = block_permutations(len(rows), block, perms, seed)
    null = np.nanmean(c[np.arange(len(rows))[None, :], index], axis=1)
    null = null[np.isfinite(null)]
    p = (1 + int((np.abs(null) >= abs(observed) - 1e-12).sum())) / (len(null) + 1)
    return observed, p


def entity_shuffle_pvalue(
    feature: np.ndarray, label: np.ndarray, rows: np.ndarray, perms: int, seed: int
) -> float:
    """Sensitivity null: shuffle outcomes across issuers within each date (two-sided)."""
    rng = np.random.default_rng([seed, len(rows), perms, 3])
    total, observed, used = np.zeros(perms), 0.0, 0
    for i in rows:
        m = np.isfinite(feature[i]) & np.isfinite(label[i])
        x, y = rankdata(feature[i, m]), rankdata(label[i, m])
        x, y = x - x.mean(), y - y.mean()
        scale = math.sqrt(float(x @ x) * float(y @ y))
        if scale <= 0:
            continue
        shuffled = rng.permuted(np.tile(y, (perms, 1)), axis=1)
        total += shuffled @ x / scale
        observed += float(x @ y) / scale
        used += 1
    if not used:
        return 1.0
    null, observed = total / used, observed / used
    return (1 + int((np.abs(null) >= abs(observed) - 1e-12).sum())) / (perms + 1)


def buyer_excess(panel: TrialPanel, rows: np.ndarray) -> dict:
    """Reported magnitude: mean relative return of issuers with a buyer (A > 0) vs without."""
    buyers, others, dates = [], [], 0
    for i in rows:
        m = np.isfinite(panel.feature[i]) & np.isfinite(panel.label[i])
        b = m & (panel.feature[i] > 0)
        o = m & (panel.feature[i] == 0)
        if b.any() and o.any():
            buyers.append(float(panel.label[i, b].mean()))
            others.append(float(panel.label[i, o].mean()))
            dates += 1
    if not dates:
        return {"dates": 0, "buyer_minus_benchmark": None, "nonbuyer_minus_benchmark": None,
                "buyer_minus_nonbuyer": None, "buyer_issuer_dates": 0}
    b, o = np.array(buyers), np.array(others)
    count = int(sum(((np.isfinite(panel.feature[i]) & (panel.feature[i] > 0) & np.isfinite(panel.label[i])).sum()) for i in rows))
    out = {
        "dates": dates,
        "buyer_minus_benchmark": float(b.mean()),
        "nonbuyer_minus_benchmark": float(o.mean()),
        "buyer_minus_nonbuyer": float((b - o).mean()),
        "buyer_issuer_dates": count,
    }
    if panel.largest is not None:
        # Reported-only stratum: the largest qualifying line in the window.
        for name, large in (("large_line", True), ("small_line", False)):
            flat = []
            for i in rows:
                lines = panel.largest[i]
                size = lines >= LARGE_LINE_USD if large else (lines > 0) & (lines < LARGE_LINE_USD)
                m = np.isfinite(panel.label[i]) & (np.nan_to_num(panel.feature[i]) > 0) & size
                flat.extend(panel.label[i, m].tolist())
            out[f"{name}_buyer_minus_benchmark"] = float(np.mean(flat)) if flat else None
            out[f"{name}_buyer_issuer_dates"] = len(flat)
    return out


def missing_labels(panel: TrialPanel) -> dict:
    """No silent drops: issuer-dates with a feature but no label (delisted, halted, no close)."""
    has_feature = np.isfinite(panel.feature)
    missing = has_feature & ~np.isfinite(panel.label)
    buyers = has_feature & (np.nan_to_num(panel.feature) > 0)
    buyer_missing = int((missing & buyers).sum())
    return {
        "issuer_dates_with_feature": int(has_feature.sum()),
        "missing_label": int(missing.sum()),
        "buyer_issuer_dates": int(buyers.sum()),
        "buyer_missing_label": buyer_missing,
        "buyer_missing_share": buyer_missing / int(buyers.sum()) if buyers.any() else 0.0,
    }


def momentum_baseline(panel: TrialPanel, rows: np.ndarray) -> dict:
    """Reported only (never a trial): momentum IC and the feature-momentum rank correlation."""
    if panel.momentum is None:
        return {"momentum_mean_ic": None, "feature_momentum_mean_rank_corr": None}
    mom_ic, _ = rank_ic_series(panel.momentum[rows], panel.label[rows])
    cross, _ = rank_ic_series(panel.feature[rows], panel.momentum[rows])
    return {
        "momentum_mean_ic": _finite_or_none(np.nanmean(mom_ic)) if np.isfinite(mom_ic).any() else None,
        "feature_momentum_mean_rank_corr": _finite_or_none(np.nanmean(cross)) if np.isfinite(cross).any() else None,
    }


def measure_trial(
    panel: TrialPanel,
    *,
    block: int | None = None,
    perms: int = PERMS,
    seed: int = SEED,
    sensitivity_perms: int = SENSITIVITY_PERMS,
    min_n: int = MIN_N,
    sensitivity: bool = True,
) -> dict:
    """Mean rank IC, its block sign-flip p-values and (optionally) the sensitivity nulls."""
    ic, counts = rank_ic_series(panel.feature, panel.label)
    rows = np.flatnonzero(np.isfinite(ic))
    series = ic[rows]
    base = {
        "n": int(len(rows)),
        "decisions": len(panel.decision_at),
        "median_entities": int(np.median(counts[rows])) if len(rows) else 0,
        "labels": missing_labels(panel),
    }
    if len(rows) < min_n:
        return {**base, "mean_ic": None, "p": 1.0, "p_one_sided_positive": 1.0,
                "status": "insufficient_data", "block": None, "block_basis": None}
    if block is None:
        block, basis = autocorrelation_block(series.tolist(), 0)
        basis = {**basis, "block": block}
    else:
        basis = {"rule": "frozen", "block": block}
    mean_ic, p, p_one = signflip_pvalues(series, block, perms, seed, PRIMARY_DIRECTION)
    out = {
        **base,
        "mean_ic": mean_ic,
        "ic_sd": float(series.std(ddof=1)) if len(series) > 1 else None,
        "ic_positive_share": float((series > 0).mean()),
        "p": p,
        "p_one_sided_positive": p_one,
        "status": "tested",
        "block": int(block),
        "block_basis": basis,
    }
    if sensitivity:
        ta_stat, ta_p = time_alignment_pvalue(panel.feature, panel.label, rows, block, sensitivity_perms, seed)
        out["sensitivity"] = {
            "time_alignment_mean": ta_stat,
            "time_alignment_p": ta_p,
            "entity_shuffle_p": entity_shuffle_pvalue(panel.feature, panel.label, rows, sensitivity_perms, seed),
            "note": "reported only; never selects",
        }
        out["magnitude"] = buyer_excess(panel, rows)
        out["baseline"] = momentum_baseline(panel, rows)
    return out


# --- discovery, holdout, verdict ---------------------------------------------------------


@dataclass(frozen=True)
class RunSpec:
    """The run's declared identity (frozen into the discovery manifest)."""

    run_id: str
    sector: str
    ledger_id: str = LEDGER_ID
    ledger_q: float = LEDGER_Q
    run_k: int = VS1_RUN_K
    trials: tuple[str, ...] = ()
    perms: int = PERMS
    seed: int = SEED
    min_n: int = MIN_N

    def validate(self) -> None:
        if self.sector not in EQUITY_SECTORS or not self.run_id:
            raise ValueError("invalid run spec")
        if self.sector != VS1_SECTOR:
            # The other 10 sectors are ONE ledger run (k=2): Holm over all 40
            # trials jointly. This harness runs one sector's 4 trials, so it
            # refuses them rather than apply the wrong denominator.
            raise ValueError(
                "only the VS1 sector runs here; the 10-sector run needs a joint "
                "40-trial Holm (pre-registration section 13), and that plan is superseded by "
                f"{SECTOR_PLAN_SUPERSEDED_BY['version']} (analysis.panel_insider_density_sectors_v4)"
            )
        if tuple(self.trials) != trial_names():
            raise ValueError("a run declares exactly the pre-registered trials")
        expected_k = VS1_RUN_K if self.sector == VS1_SECTOR else OTHER_SECTORS_RUN_K
        if self.run_k != expected_k or self.ledger_id != LEDGER_ID or self.ledger_q != LEDGER_Q:
            raise ValueError("ledger id, q and run index are pre-registered")
        # Pre-registration §6: 20,000 sign-flip draws, seed 20260927, 30 dates.
        # Exactly these; any other value is a different (unregistered) test.
        if (self.perms, self.seed, self.min_n) != (PERMS, SEED, MIN_N):
            raise ValueError(
                f"statistical settings are pre-registered: perms={PERMS}, seed={SEED}, min_n={MIN_N}"
            )

    @property
    def alpha(self) -> float:
        return run_alpha(self.run_k, self.ledger_q)


def discover_panel(
    spec: RunSpec,
    panels: Mapping[str, TrialPanel],
    *,
    inputs: dict,
    repo_root: Path = REPO,
    sensitivity: bool = True,
) -> dict:
    """Freeze the discovery ledger. Never receives holdout rows."""
    prereg = check_prereg(repo_root)
    spec.validate()
    if set(panels) != set(spec.trials):
        raise ValueError("panels must be exactly the declared trials")
    ledger = []
    for trial in spec.trials:
        panel = panels[trial]
        if panel.window != "discovery":
            raise ValueError("discovery received a non-discovery panel")
        validate_panel(panel)
        result = measure_trial(panel, perms=spec.perms, seed=spec.seed, min_n=spec.min_n,
                               sensitivity=sensitivity)
        ledger.append({"trial_id": digest([spec.run_id, spec.sector, trial]), "trial": trial,
                       "sector": spec.sector, **result})
    pvalues = [t["p"] for t in ledger]
    for trial, holm, bh in zip(ledger, holm_adjusted(pvalues), bh_adjusted(pvalues)):
        trial["holm_adjusted_p"] = holm
        trial["bh_adjusted_p"] = bh
        trial["selected"] = trial["status"] == "tested" and holm <= spec.alpha
    payload = {
        "version": VERSION,
        "origin": ORIGIN,
        "prereg_sha256": prereg,
        "spec": asdict(spec),
        "windows": {"discovery_start": DISCOVERY_START, "split": SPLIT, "end": END},
        "selection": f"Holm at ledger run alpha {spec.alpha:.6g} (q={spec.ledger_q}, k={spec.run_k}) "
                     "over every declared trial incl. untestable; BH-adjusted p reported only",
        "null": "block sign-flip of the per-date rank-IC series; block from discovery IC acf1 "
                f"(autocorrelation_block, >= {MIN_BLOCKS} blocks)",
        "inputs": inputs,
        "discovery_sha256": digest({t: panels[t].as_record() for t in spec.trials}),
        "ledger": ledger,
        "calibration": calibration(ledger),
        "state": "DISCOVERY_FROZEN",
        "promotion_allowed": False,
    }
    return {"payload": payload, "sha256": digest(payload)}


def calibration(ledger: list[dict]) -> dict:
    """Discovery-only check against the published direction (see the pre-registration)."""
    by = {t["trial"]: t for t in ledger}
    primary = by[PRIMARY_TRIAL]
    contrary = [
        t["trial"] for t in ledger
        if t["status"] == "tested" and t["mean_ic"] is not None and t["mean_ic"] < 0 and t["p"] <= 0.05
    ]
    consistent = (
        primary["status"] == "tested"
        and primary["mean_ic"] > 0
        and primary["p_one_sided_positive"] <= 0.10
    ) or any(t["selected"] and t["mean_ic"] > 0 for t in ledger)
    if contrary:
        state = "CONTRARY"
    elif consistent:
        state = "CONSISTENT"
    elif primary["status"] == "tested" and primary["mean_ic"] > 0:
        state = "WEAK_POSITIVE"
    else:
        state = "ABSENT"
    return {"state": state, "primary_trial": PRIMARY_TRIAL, "contrary_trials": contrary}


def evaluate_panel_holdout(
    frozen: dict,
    panels: Mapping[str, TrialPanel],
    key: HoldoutKey,
    *,
    power: dict | None = None,
) -> dict:
    """Frozen selections (and the pre-registered primary trial) on the holdout, once."""
    if not isinstance(key, HoldoutKey) or key.frozen_sha256 != frozen.get("sha256"):
        raise PermissionError("holdout needs the HoldoutKey opened for this frozen discovery")
    payload = frozen["payload"]
    spec = RunSpec(**{**payload["spec"], "trials": tuple(payload["spec"]["trials"])})
    ledger = {t["trial"]: t for t in payload["ledger"]}
    selected = [t for t in payload["ledger"] if t["selected"]]
    evaluated = sorted({t["trial"] for t in selected} | {PRIMARY_TRIAL})
    checks = []
    for trial in evaluated:
        panel = panels[trial]
        if panel.window != "holdout":
            raise ValueError("holdout received a non-holdout panel")
        validate_panel(panel)
        frozen_block = ledger[trial]["block"] or 1
        ic, _ = rank_ic_series(panel.feature, panel.label)
        n = int(np.isfinite(ic).sum())
        block = max(1, min(frozen_block, max(1, n // MIN_BLOCKS)))
        result = measure_trial(panel, block=block, perms=spec.perms, seed=spec.seed,
                               min_n=spec.min_n, sensitivity=True)
        is_selected = ledger[trial]["selected"]
        adjusted = corrected_p(result["p"], len(selected)) if is_selected else None
        survives = bool(
            is_selected
            and result["status"] == "tested"
            and adjusted <= HOLDOUT_ALPHA
            and result["mean_ic"] * ledger[trial]["mean_ic"] > 0
        )
        checks.append({
            "trial_id": ledger[trial]["trial_id"],
            "trial": trial,
            "selected_in_discovery": is_selected,
            **result,
            "bonferroni_p": adjusted,
            "retrospective_survivor": survives,
            "primary_one_sided_p": result["p_one_sided_positive"] if trial == PRIMARY_TRIAL else None,
        })
    result = {
        "discovery_manifest": frozen["sha256"],
        "prereg_sha256": payload["prereg_sha256"],
        "holdout_sha256": digest({t: panels[t].as_record() for t in evaluated}),
        "holdout_checks": checks,
        "promotion_allowed": False,
    }
    result["verdict"] = verdict(payload, checks, power)
    return result


def verdict(payload: dict, checks: list[dict], power: dict | None) -> dict:
    """NO_SURVIVOR, HOLDOUT_SURVIVOR_FORWARD_PENDING or MACHINERY_SUSPECT (pre-registered)."""
    calib = payload["calibration"]["state"]
    survivors = [c for c in checks if c["retrospective_survivor"]]
    positive = [c for c in survivors if c["mean_ic"] > 0]
    powered = bool(power and power.get("gate_passed"))
    notes = []
    if calib == "CONTRARY" or any(c["mean_ic"] < 0 for c in survivors):
        state = "MACHINERY_SUSPECT"
        notes.append("a significant negative insider-buy IC contradicts the published prior: audit the "
                     "event parse, dates and prices before reading anything else")
    elif positive:
        state = "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    elif calib == "ABSENT" and powered:
        state = "MACHINERY_SUSPECT"
        notes.append("powered for IC 0.01 yet the primary discovery IC is not positive")
    else:
        state = "NO_SURVIVOR"
        if not powered:
            notes.append("UNDERPOWERED: the Stage-0 power gate did not pass; a null here is not "
                         "evidence against the published effect")
    shares = [
        t.get("labels", {}).get("buyer_missing_share", 0.0)
        for t in [*payload.get("ledger", []), *checks]
        if t.get("trial") == PRIMARY_TRIAL
    ]
    if any(share > MISSING_LABEL_WARNING for share in shares):
        notes.append(
            f"SURVIVORSHIP_WARNING: more than {MISSING_LABEL_WARNING:.0%} of the primary trial's "
            "buyer issuer-dates have no label (possible delistings; no delisting return)"
        )
    return {"state": state, "calibration": calib, "notes": notes,
            "survivors": [c["trial"] for c in positive], "promotion_allowed": False,
            "statement": "Nothing here is a trading signal."}


# --- Stage 0: power on the real feature panel with synthetic outcomes ---------------------


def _planted_ics(usable: list, rho: float, sims: int, rng: np.random.Generator) -> np.ndarray:
    """sims x dates rank ICs of ``rho * z + noise`` against the feature ranks."""
    out = np.empty((sims, len(usable)))
    noise = math.sqrt(max(0.0, 1 - rho**2))
    for k, z in enumerate(usable):
        x = rankdata(z)
        x = x - x.mean()
        y = rho * z[None, :] + noise * rng.standard_normal((sims, len(z)))
        yr = y.argsort(axis=1).argsort(axis=1) + 1.0  # continuous: no ties
        yr = yr - yr.mean(axis=1, keepdims=True)
        out[:, k] = (yr @ x) / (math.sqrt(float(x @ x)) * np.sqrt((yr**2).sum(axis=1)))
    return out


def planted_power(
    feature: np.ndarray,
    target_ic: float,
    *,
    sims: int = POWER_SIMS,
    perms: int = POWER_PERMS,
    seed: int = SEED,
    threshold: float | None = None,
    min_n: int = MIN_N,
) -> dict:
    """P(selection) for a planted effect of mean rank IC ``target_ic`` on this feature geometry.

    Outcomes are synthetic: per usable date, ``rho * z + sqrt(1 - rho^2) * noise``
    with ``z`` the standardised feature rank among eligible issuers. ``rho`` is
    scaled by a pilot run so the expected realised mean rank IC equals
    ``target_ic`` (ties in a mostly-zero feature shrink the rank IC). Uses
    feature data only (never a price or an outcome). ``threshold`` defaults to
    the smallest Holm threshold of a 4-trial run at the VS1 run alpha.
    Idiosyncratic noise only (no common factor), so it is an optimistic power.
    """
    threshold = run_alpha(VS1_RUN_K) / len(trial_names()) if threshold is None else threshold
    rng = np.random.default_rng([seed, int(round(target_ic * 1e6)), sims, 4])
    usable = []
    for row in feature:
        m = np.isfinite(row)
        if m.sum() >= MIN_ENTITIES and np.ptp(row[m]) > 0:
            r = rankdata(row[m])
            usable.append((r - r.mean()) / r.std())
    if len(usable) < min_n:
        return {"target_ic": target_ic, "usable_dates": len(usable), "power": 0.0,
                "realized_mean_ic": None, "threshold": threshold, "sims": 0}
    pilot_rho = 0.2
    scale = float(_planted_ics(usable, pilot_rho, 50, rng).mean()) / pilot_rho
    rho = min(0.99, target_ic / scale) if scale > 0 else 0.99
    ics = _planted_ics(usable, rho, sims, rng)
    block, _ = autocorrelation_block(ics[0].tolist(), 0)
    hits = 0
    for s in range(sims):
        _, p, _ = signflip_pvalues(ics[s], block, perms, seed + s, PRIMARY_DIRECTION)
        hits += p <= threshold
    return {
        "target_ic": target_ic,
        "usable_dates": len(usable),
        "planted_rho": rho,
        "power": hits / sims,
        "realized_mean_ic": float(ics.mean()),
        "threshold": threshold,
        "sims": sims,
    }


def power_settings() -> dict:
    """The pre-registered Stage-0 settings (§10), recorded in and checked against power.json."""
    return {
        "sims": POWER_SIMS,
        "perms": POWER_PERMS,
        "seed": SEED,
        "target_ics": list(POWER_TARGET_ICS),
        "gate_ic": POWER_GATE_IC,
        "gate_power": POWER_GATE,
        "threshold": run_alpha(VS1_RUN_K) / len(trial_names()),
        "min_n": MIN_N,
    }


def stage0_power(features: Mapping[str, np.ndarray]) -> dict:
    """Power per trial feature panel and target IC, and the pre-registered gate.

    Runs only at the pre-registered settings (:func:`power_settings`); there is
    no override.
    """
    if set(features) != set(trial_names()):
        raise ValueError("Stage-0 power needs exactly the declared trials' feature panels")
    table = {
        trial: [planted_power(features[trial], ic, sims=POWER_SIMS, perms=POWER_PERMS, seed=SEED)
                for ic in POWER_TARGET_ICS]
        for trial in trial_names()
    }
    primary = next(r for r in table[PRIMARY_TRIAL] if r["target_ic"] == POWER_GATE_IC)
    return {
        "table": table,
        "settings": power_settings(),
        "gate": f"power(primary {PRIMARY_TRIAL}, IC {POWER_GATE_IC}) >= {POWER_GATE}",
        "gate_passed": primary["power"] >= POWER_GATE,
        "note": "feature data only; synthetic outcomes; idiosyncratic noise (optimistic)",
    }


def verify_power(power: Mapping[str, Any]) -> None:
    """Refuse a power file not computed at the pre-registered settings (§10)."""
    if power.get("settings") != power_settings():
        raise ValueError(
            f"power file was not computed at the pre-registered settings "
            f"(sims={POWER_SIMS}, perms={POWER_PERMS}, seed={SEED})"
        )
    table = power.get("table") or {}
    if set(table) != set(trial_names()):
        raise ValueError("power file must cover exactly the declared trials")
    for trial, rows in table.items():
        if [r.get("target_ic") for r in rows] != list(POWER_TARGET_ICS):
            raise ValueError(f"{trial}: power rows must be the pre-registered target ICs")
        for r in rows:
            if r.get("sims") not in (POWER_SIMS, 0):  # 0 = too few usable dates, power 0
                raise ValueError(f"{trial}: power row not run at {POWER_SIMS} simulations")
            if r.get("sims") == 0 and r.get("power") != 0.0:
                raise ValueError(f"{trial}: an unsimulated row must carry power 0")
    primary = next(r for r in table[PRIMARY_TRIAL] if r["target_ic"] == POWER_GATE_IC)
    if power.get("gate_passed") is not (primary["power"] >= POWER_GATE):
        raise ValueError("power file's gate_passed disagrees with its primary power")


def power_features(events: Form4Events, universe: pd.DataFrame, window: str = "discovery") -> dict[str, np.ndarray]:
    """Feature panels on proxy sessions (no price read) for the Stage-0 gate."""
    lo, hi = window_bounds(window)
    sessions = proxy_sessions(lo.date(), hi.date())
    ciks = list(universe["cik"].astype(int))
    out = {}
    for h in HORIZONS:
        decided = decision_instants(sessions[::h])
        for name in FEATURES:
            out[f"{name}|fwd{h}"] = feature_panel(events, ciks, decided, name).to_numpy(dtype=float)
    return out


# --- pre-registration registry (hash-chained, research_forward_log mechanism) -----------

REGISTRY_LOG = "granular_panel_prereg_v1.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_v1.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_v1.lock"

#: The one real VS1 v1 registration (review of #697 at 7c28f281, blocking item 1).
#: It was registered once, locally, on 2026-09-27T09:27:45Z against code
#: d1e13f6e (``register`` below, unchanged since), and anchored at 2 records with
#: chain head 5b10ff57... Every registry this harness accepts must start with
#: exactly these two records -- line sha256 of the header, then of the
#: ``preregistration`` record (= the chain head at 2 records). A registry
#: started with any other time, code or content is a fork and is refused.
#: A byte copy of this prefix is indistinguishable from the original by
#: content; the off-host witness (:func:`require_witness`) decides which copy's
#: continuation counts.
REGISTERED_AT = datetime(2026, 9, 27, 9, 27, 45, 13793, tzinfo=timezone.utc)
REGISTERED_CODE_SHA = "d1e13f6ef90b02211b84533ef2c9fa1d14fa787d"
REGISTERED_RECORD_SHA256: tuple[str, str] = (
    "dc040b6dca726ae48ecb2eff26349026647ac614db63afb1c6b430726e3c8e7a",  # header
    "5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508",  # preregistration (head at 2)
)
#: The pins are for this pre-registration body only.
REGISTERED_PREREG_SHA256 = PREREG_BODY_SHA256


def registry(log_dir: Path, prereg_sha256: str = PREREG_BODY_SHA256):
    from analysis.research_forward_log import ForwardLog

    return ForwardLog(
        log_dir,
        log_filename=REGISTRY_LOG,
        anchor_filename=REGISTRY_ANCHORS,
        lock_filename=REGISTRY_LOCK,
        prereg_sha256=prereg_sha256,
    )


def registration_records(now: datetime, code_sha: str, prereg_sha256: str = PREREG_BODY_SHA256) -> list[dict]:
    """The header and ``preregistration`` records (before chaining), as ``register`` writes them."""
    header = {
        "kind": "header",
        "version": VERSION,
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "promotion_allowed": False,
    }
    record = {
        "kind": "preregistration",
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "sector_map_sha256": SECTOR_MAP_SHA256,
        "ledger_id": LEDGER_ID,
        "runs": {
            "vs1": {"sector": VS1_SECTOR, "k": VS1_RUN_K, "alpha": run_alpha(VS1_RUN_K),
                    "trials": list(trial_names())},
            "other_sectors": {"sectors": list(OTHER_SECTORS), "k": OTHER_SECTORS_RUN_K,
                              "alpha": run_alpha(OTHER_SECTORS_RUN_K),
                              "trials": list(trial_names())},
        },
        "windows": {"discovery_start": DISCOVERY_START, "split": SPLIT, "end": END},
        "promotion_allowed": False,
    }
    return [header, record]


def chained_sha256(records: list[dict]) -> list[str]:
    """Line sha256 of each record once chained (``prev_sha256`` = the previous line's hash)."""
    from analysis.research_forward_log import canonical

    previous, out = None, []
    for record in records:
        previous = hashlib.sha256(canonical({**record, "prev_sha256": previous})).hexdigest()
        out.append(previous)
    return out


def register(log_dir: Path, now: datetime, code_sha: str, *, repo_root: Path = REPO,
             prereg_sha256: str = PREREG_BODY_SHA256) -> list[dict]:
    """Write the pinned registration into an empty registry directory.

    VS1 v1 is registered already (:data:`REGISTERED_RECORD_SHA256`). This only
    re-materialises that exact registration (``now`` = :data:`REGISTERED_AT`,
    ``code_sha`` = :data:`REGISTERED_CODE_SHA`); any other time or code would
    start a fork and is refused before anything is written. A materialised copy
    is a byte copy of the real prefix: only the off-host witness makes its
    continuation count.
    """
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    records = registration_records(now, code_sha, prereg_sha256)
    if prereg_sha256 != REGISTERED_PREREG_SHA256 or tuple(chained_sha256(records)) != REGISTERED_RECORD_SHA256:
        raise PermissionError(
            "VS1 v1 is already registered (chain head "
            f"{REGISTERED_RECORD_SHA256[1][:12]}... at 2 records); a registration with another "
            "time, code or body starts a fork and is refused"
        )
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise ValueError("this pre-registration is already registered in this directory")
        return log.append_locked(records)


# --- one-shot discovery and holdout, enforced through the registry chain ------------------
#
# Order of records a VS1 run appends to the pinned registry (after the two
# registration records):
#
#   inputs_frozen     hashes of every input (before any price read; may be re-frozen
#                     only while no discovery has been opened)
#   discovery_opened  appended by open_discovery, BEFORE any discovery price read;
#                     a second one is refused
#   (off-host)        the operator appends the registry's anchor lines (up to and
#                     including discovery_opened) to WITNESS_PATH in a vault worktree,
#                     commits and pushes to main of WITNESS_REMOTE_URL; resume_discovery
#                     fetches that branch (check_offhost: pinned remote, branch, path,
#                     append-only history) and refuses the price key until
#                     verify_chain(external_anchors=<committed file>) shows the head
#   prices_read       the price receipt of every read (a resumed run must match it)
#   discovery_frozen  the frozen discovery manifest's sha256
#   holdout_opened    appended by open_holdout, BEFORE any holdout price read; needs
#                     the frozen file to hash to the chain's discovery_frozen; a
#                     second open is refused
#   (off-host)        the same witness for holdout_opened; resume_holdout checks it
#   prices_read       the holdout read's receipt
#   holdout_result    the holdout result's sha256 and verdict state
#
# The price reader refuses to read without the key resume_discovery /
# resume_holdout issue. A registry that does not start with the pinned
# registration is refused everywhere; a byte copy of the pinned prefix (or a
# deleted-and-recreated registry) can open its own discovery locally, but its
# chain differs from the one in the off-host log, so it never gets a price key.
# A discovery or holdout whose opening record is missing from the off-host log
# is invalid.

#: Inputs every ``inputs_frozen`` record must carry (the Stage-0 power file is
#: hashed by content, :func:`digest`, so its write-once copy in the run
#: directory hashes the same).
FROZEN_INPUT_KEYS: tuple[str, ...] = (
    "sector",
    "price_manifest_sha256",
    "probe_report_sha256",
    "form4_sha256",
    "submissions_sha256",
    "issuer_map_sha256",
    "power_sha256",
    "accept_underpowered",
    "as_of_ts",
)
#: Of those, the ones discovery and holdout recompute from the files they are
#: given and must match exactly (``as_of_ts`` and ``accept_underpowered`` are
#: taken from the record, never from the command line).
OBSERVED_INPUT_KEYS: tuple[str, ...] = tuple(
    k for k in FROZEN_INPUT_KEYS if k not in ("as_of_ts", "accept_underpowered")
)


def _is_hex64(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and not set(value) - HEX64


def _record_sha256(record: dict) -> str:
    from analysis.research_forward_log import canonical

    return hashlib.sha256(canonical(record)).hexdigest()


def _line_sha256(log) -> list[str]:
    from analysis.research_forward_log import _lines

    return [hashlib.sha256(line).hexdigest() for line in _lines(log.path)]


def _chain(log, prereg_sha256: str) -> list[dict]:
    """Verified records of the pinned registry.

    Refuses a broken chain, a pre-registration other than the pinned one, and
    any registry whose first two records are not the pinned registration
    (a fresh or recreated registry is a fork).
    """
    if prereg_sha256 != REGISTERED_PREREG_SHA256:
        raise PermissionError("only the pinned VS1 v1 pre-registration has a registry")
    check = log.verify_chain()
    if not check["ok"]:
        raise RuntimeError(f"registry chain is broken: {check['detail']}")
    heads = _line_sha256(log)
    if tuple(heads[: len(REGISTERED_RECORD_SHA256)]) != REGISTERED_RECORD_SHA256:
        if not heads:
            raise PermissionError("this pre-registration is not registered here (empty registry)")
        raise PermissionError(
            "registry is not the pinned VS1 v1 registration (its first records do not hash to "
            f"{REGISTERED_RECORD_SHA256[1][:12]}... at 2 records): a fork, refused"
        )
    return log.read_all()


def _kind(records: list[dict], kind: str) -> list[dict]:
    return [r for r in records if r.get("kind") == kind]


def _position(records: list[dict], kind: str) -> int:
    """1-based record count at the single record of ``kind`` (the anchor it needs)."""
    positions = [i + 1 for i, r in enumerate(records) if r.get("kind") == kind]
    if len(positions) != 1:
        raise PermissionError(f"the registry holds {len(positions)} {kind} records, not one")
    return positions[0]


#: The off-host witness, pinned (re-review of #697 at a6875ac5): the anchor log
#: lives at exactly this path on branch ``main`` of the GitHub vault repository.
#: No other repository, branch, path or local file counts.
WITNESS_REMOTE_URL = "https://github.com/3pacs/obsidian-vault.git"
WITNESS_BRANCH = "main"
WITNESS_PATH = "05-GRID/Paper-Log/vs1/granular_panel_prereg_v1.anchors.jsonl"
#: Private ref the pinned branch is fetched into (never a local branch or ``origin/*``,
#: which a local clone can point anywhere).
WITNESS_REF = "refs/vs1-witness/main"
#: The first line every committed version of the witness file starts with: the
#: registry's anchor after the pinned 2-record registration (head 5b10ff57...).
REGISTERED_ANCHOR_LINE = (
    b'{"head_sha256":"5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-27T09:27:45.013793+00:00"}'
)
_WITNESS_TOKEN = object()

#: VS1 v1 was superseded before any price read: v2 (SIC-expanded universe) and then
#: v3 (primary trial A90|fwd5) were registered (docs/paper_log/
#: vs1-insider-density-v3-preregistration.md). v1 can never open a discovery.
SUPERSEDED_BY: dict | None = {
    "version": "vs1-v6",
    "prereg_sha256": "5a87d4a4130e184a8b9e53d7eec040a7b26b697a3ad07500aac6ef4b17d6a32d",
    "registry_head_sha256": "3dfa6ee30359205c84ba4fb3bdb0858eba13b6ed0c8aa0711d98fa6aade505e7",
}
OWN_VERSION_NUMBER = 1
#: v1 section 13's other-10-sector plan (carried into v2/v3 section 13) is superseded by the
#: "sectors v4" pre-registration (all sectors on 5 sessions, SIC-expanded universes, gated on the
#: VS1 v6 Technology run), which superseded "sectors v3" and "sectors v2" (neither ever opened).
SECTOR_PLAN_SUPERSEDED_BY: dict = {
    "version": "vs1-sectors-v4",
    "prereg_path": "docs/paper_log/vs1-sectors-v4-preregistration.md",
    "prereg_sha256": "e3f41ace1bfbfed12c82e16b3b438a62ba759bc1c54dfdf743fb8b2d27b4e712",
    "registry_head_sha256": None,
}
#: Witness files of every VS1 (Technology) registry version on the pinned vault ``main``.
VERSIONED_WITNESS = re.compile(r"^05-GRID/Paper-Log/vs1/granular_panel_prereg_v(\d+)\.anchors\.jsonl$")
#: Witness files of the other-10-sector registries (``sectors-v2`` onward; v1's plan lived in v1's registry).
SECTORS_WITNESS = re.compile(r"^05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v(\d+)\.anchors\.jsonl$")


WITNESS_DIR = "05-GRID/Paper-Log/vs1/"
#: Non-witness files tolerated in the witness directory.
WITNESS_DIR_ALLOWED = frozenset({"README.md", ".gitattributes"})


def vs1_witness_census(repo: Path, tip: str) -> dict:
    """Every VS1 registry witness on the pinned ``main`` at ``tip`` and the records each covers.

    Keyed by exact path (review round 2, R1): ``by_path`` maps every
    witness-like path to its registry id (None when unknown) and covered records.
    ``files``/``records``: registry id (``vs1-v<n>`` for the Technology versions,
    ``sectors-v<n>`` for the other-10-sector registries) -> canonical path /
    records covered by its last anchor line (None when unreadable). Only the
    canonical path of an id counts (:func:`canonical_witness_path`): a path with a
    leading-zero or otherwise non-canonical number (``..._v03...``) is unknown.
    ``unknown``: those, any other file in the witness directory (except
    README.md, .gitattributes) and any ``*.anchors.jsonl`` whose path mentions
    ``vs1`` anywhere else in the tree -- a witness under a name or path no
    version knows.
    """
    files, unknown = {}, []
    for path in _git(repo, "ls-tree", "-r", "--name-only", tip).splitlines():
        path = path.strip()
        match, sectors = VERSIONED_WITNESS.match(path), SECTORS_WITNESS.match(path)
        key = (f"vs1-v{int(match.group(1))}" if match else
               f"sectors-v{int(sectors.group(1))}" if sectors else None)
        if key is not None and canonical_witness_path(key) == path:
            files[key] = path
        elif key is not None or (path.startswith(WITNESS_DIR) and path[len(WITNESS_DIR):] not in WITNESS_DIR_ALLOWED):
            unknown.append(path)
        elif "vs1" in path.lower() and path.lower().endswith(".anchors.jsonl"):
            unknown.append(path)

    def covered(path: str) -> int | None:
        content = _git(repo, "show", f"{tip}:{path}", binary=True).replace(b"\r\n", b"\n")
        lines = [line for line in content.split(b"\n") if line]
        try:
            value = json.loads(lines[-1])["records"] if lines else None
        except (ValueError, KeyError, TypeError):
            return None
        return value if isinstance(value, int) else None

    records = {key: covered(path) for key, path in files.items()}
    by_path = {path: {"id": key, "records": records[key]} for key, path in files.items()}
    for path in unknown:
        by_path[path] = {"id": None, "records": covered(path) if path.endswith(".jsonl") else None}
    return {"tip": tip, "files": dict(sorted(files.items())), "records": dict(sorted(records.items())),
            "unknown": sorted(unknown), "by_path": dict(sorted(by_path.items()))}


def canonical_witness_path(registry_id: str) -> str:
    """The one path the witness of ``registry_id`` (``vs1-v<n>`` / ``sectors-v<n>``) may live at."""
    family, _, number = registry_id.rpartition("-v")
    if not number.isdigit() or str(int(number)) != number or family not in ("vs1", "sectors"):
        raise ValueError(f"not a VS1 registry id: {registry_id!r}")
    stem = "granular_panel_prereg_v" if family == "vs1" else "granular_panel_prereg_sectors_v"
    return f"{WITNESS_DIR}{stem}{number}.anchors.jsonl"


def witnessed_versions(repo: Path, tip: str) -> dict[int, str]:
    """Version number -> witness path of every VS1 (Technology) registry witness present at ``tip``."""
    return {int(k[len("vs1-v"):]): p for k, p in vs1_witness_census(repo, tip)["files"].items()
            if k.startswith("vs1-v")}


def refuse_superseded(own: int, superseded_by: Mapping[str, Any] | None, witness: Any = None) -> None:
    """Refuse an opening of VS1 version ``own`` once any later version is registered.

    The supersession pinned in this version's code always refuses (discovery and
    holdout). When the fetched off-host witness is given (discovery openings),
    a witness file of a later version on the pinned vault ``main``, or an
    unknown VS1 witness file, refuses too -- so a checkout that predates the pin
    is refused once it fetches the witness it needs for a price key.
    """
    if superseded_by:
        raise PermissionError(
            f"VS1 v{own} is superseded by {superseded_by.get('version')} (pinned in code): "
            "it can never open a discovery or a holdout"
        )
    if witness is not None:
        census = vs1_witness_census(witness.repo, witness.tip)
        later = sorted(int(k[len("vs1-v"):]) for k in census["files"]
                       if k.startswith("vs1-v") and int(k[len("vs1-v"):]) > own)
        if later:
            raise PermissionError(
                f"a later VS1 registry (v{later[-1]}) is witnessed on the pinned {WITNESS_BRANCH}: "
                f"v{own} can never open a discovery"
            )
        if census["unknown"]:
            raise PermissionError(
                f"unknown VS1 witness files on the pinned {WITNESS_BRANCH} ({census['unknown'][:5]}): "
                "refused until an owner accounts for them"
            )


def _git(repo: Path, *argv: str, binary: bool = False):
    import subprocess

    result = subprocess.run(["git", "-C", str(repo), *argv], capture_output=True, check=False)
    if result.returncode != 0:
        raise PermissionError(
            f"git {' '.join(argv[:2])} failed in {repo}: {result.stderr.decode('utf-8', 'replace').strip()}"
        )
    return result.stdout if binary else result.stdout.decode("utf-8")


class OffhostWitness:
    """The witness file as committed on the pinned remote's ``main`` (issued by :func:`check_offhost`)."""

    def __init__(self, token: object, repo: Path, tip: str, content: bytes, versions: list[dict],
                 remote_url: str) -> None:
        if token is not _WITNESS_TOKEN:
            raise TypeError("an OffhostWitness is issued only by check_offhost")
        self.repo, self.tip, self.content, self.versions = Path(repo), tip, content, versions
        self.remote_url = remote_url

    @property
    def lines(self) -> list[bytes]:
        return [line for line in self.content.split(b"\n") if line]

    def descends_from(self, commit: str) -> bool:
        """``commit`` is an ancestor of (or equal to) the fetched tip."""
        import subprocess

        result = subprocess.run(["git", "-C", str(self.repo), "merge-base", "--is-ancestor", commit, self.tip],
                                capture_output=True, check=False)
        return result.returncode == 0

    def witnessing_commit(self, records: int) -> str | None:
        """The first committed version whose anchors cover ``records`` records."""
        for version in self.versions:
            if version["covered_records"] >= records:
                return version["commit"]
        return None

    def receipt(self) -> dict:
        return {"remote_url": self.remote_url, "branch": WITNESS_BRANCH, "path": WITNESS_PATH,
                "tip": self.tip, "versions": self.versions}


def check_offhost(vault_repo: Path, *, remote_url: str | None = None) -> OffhostWitness:
    """Fetch the pinned vault ``main`` and return the witness file as committed there.

    ``vault_repo`` is any local git repository (a clone of the vault is
    convenient: objects are reused); its working tree, index, branches and
    remotes are never read. The pinned branch of :data:`WITNESS_REMOTE_URL` is
    fetched into :data:`WITNESS_REF`. Then, over ``git log --first-parent -m
    --follow`` of :data:`WITNESS_PATH` on that ref, every committed version must

    * live at exactly :data:`WITNESS_PATH` (no rename or copy from another
      path, no deletion);
    * start with the pinned registration anchor (:data:`REGISTERED_ANCHOR_LINE`);
    * be a strict line-prefix extension of the version before it
      (append-only: no truncation, no edit, no reorder).

    The file at the fetched tip must equal the last version walked. Any
    failure refuses. ``remote_url`` exists for tests (a local bare remote);
    the CLI always uses the pinned URL.
    """
    url = WITNESS_REMOTE_URL if remote_url is None else remote_url
    repo = Path(vault_repo)
    _git(repo, "rev-parse", "--git-dir")
    _git(repo, "fetch", "--quiet", "--no-tags", "--no-write-fetch-head", url,
         f"+refs/heads/{WITNESS_BRANCH}:{WITNESS_REF}")
    tip = _git(repo, "rev-parse", "--verify", f"{WITNESS_REF}^{{commit}}").strip()
    log = _git(repo, "log", "--first-parent", "-m", "--follow", "--name-status", "--format=%x00%H",
               WITNESS_REF, "--", WITNESS_PATH)
    entries = []
    for chunk in log.split("\x00")[1:]:
        head, *rest = chunk.strip("\n").split("\n")
        changes = [line.split("\t") for line in rest if line.strip()]
        entries.append((head.strip(), changes))
    if not entries:
        raise PermissionError(f"{WITNESS_PATH} is not on {WITNESS_BRANCH} of {url}")
    versions, previous = [], None
    for commit, changes in reversed(entries):  # oldest first
        for change in changes:
            status, paths = change[0], change[1:]
            if status.startswith(("R", "C")) or any(p != WITNESS_PATH for p in paths):
                raise PermissionError(f"{commit[:12]}: the witness file came from another path ({paths})")
            if status.startswith("D"):
                raise PermissionError(f"{commit[:12]}: the witness file was deleted (not append-only)")
        content = _git(repo, "show", f"{commit}:{WITNESS_PATH}", binary=True).replace(b"\r\n", b"\n")
        lines = [line for line in content.split(b"\n") if line]
        if not lines or lines[0] != REGISTERED_ANCHOR_LINE:
            raise PermissionError(f"{commit[:12]}: the witness file does not start with the pinned registration")
        if previous is not None and not (len(lines) > len(previous) and lines[: len(previous)] == previous):
            raise PermissionError(
                f"{commit[:12]}: the witness file is not a strict line-prefix extension of its previous "
                "version (truncated, edited or unchanged): not append-only"
            )
        covered = json.loads(lines[-1]).get("records", 0)
        versions.append({"commit": commit, "lines": len(lines), "covered_records": covered})
        previous = lines
    at_tip = _git(repo, "show", f"{tip}:{WITNESS_PATH}", binary=True).replace(b"\r\n", b"\n")
    if [line for line in at_tip.split(b"\n") if line] != previous:
        raise PermissionError("the witness file at the fetched tip is not its last walked version")
    return OffhostWitness(_WITNESS_TOKEN, repo, tip, b"\n".join(previous) + b"\n", versions, url)


def require_witness(log_dir: Path, witness: OffhostWitness | None, records_needed: int, *,
                    prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """The pinned off-host anchor log must already witness this chain up to ``records_needed``.

    The witness content (the committed file on the pinned ``main``) is passed
    to ``ForwardLog.verify_chain(external_anchors=...)``: every anchor in it
    must name a prefix this registry still has, with the same head -- so a
    forked or rewritten registry is refused -- and its last anchor must cover
    at least ``records_needed`` records.
    """
    import tempfile

    if not isinstance(witness, OffhostWitness):
        raise PermissionError("the pinned off-host anchor log (check_offhost) is required")
    log = registry(log_dir, prereg_sha256)
    with tempfile.TemporaryDirectory() as scratch:
        path = Path(scratch) / Path(WITNESS_PATH).name
        path.write_bytes(witness.content)
        with log.locked():
            records = _chain(log, prereg_sha256)
            check = log.verify_chain(external_anchors=path)
    if not check["ok"]:
        raise PermissionError(f"off-host anchor log does not witness this registry: {check['detail']}")
    covered = json.loads(witness.lines[-1])["records"]
    if covered < records_needed:
        raise PermissionError(
            f"off-host anchor log covers {covered} records; it must already contain the chain head "
            f"at {records_needed} records (commit and push the registry's anchor lines to "
            f"{WITNESS_BRANCH}:{WITNESS_PATH} first)"
        )
    for record in _kind(records, "prices_read"):
        if not witness.descends_from(record["witness_tip"]):
            raise PermissionError(
                f"the pinned {WITNESS_BRANCH} no longer contains the witness commit "
                f"{record['witness_tip'][:12]} an earlier price read saw (history rewritten)"
            )
    return {"records": len(records), "witnessed_records": covered, "tip": witness.tip,
            "witnessing_commit": witness.witnessing_commit(records_needed)}


def export_anchors(log_dir: Path, vault_worktree: Path, *, prereg_sha256: str = PREREG_BODY_SHA256) -> list[str]:
    """Append to ``<vault_worktree>/WITNESS_PATH`` the registry anchor lines it lacks.

    Refuses when the file there is not a prefix of this registry's anchor file
    (it witnesses another chain). Writes the working-tree file only; the
    operator commits it and pushes it to the pinned ``main``. Returns the
    appended lines.
    """
    from analysis.research_forward_log import _lines

    log = registry(log_dir, prereg_sha256)
    with log.locked():
        _chain(log, prereg_sha256)
        local = list(_lines(log.anchor_path))
    path = Path(vault_worktree) / WITNESS_PATH
    existing = [line.rstrip(b"\r") for line in _lines(path)] if path.exists() else []
    if existing != local[: len(existing)]:
        raise PermissionError("the off-host anchor log witnesses another registry chain; not appending")
    new = local[len(existing):]
    if new:
        path.parent.mkdir(parents=True, exist_ok=True)
        tail = path.read_bytes() if path.exists() else b""
        with open(path, "ab") as stream:
            if tail and not tail.endswith(b"\n"):
                stream.write(b"\n")
            for line in new:
                stream.write(line + b"\n")
    return [line.decode("utf-8") for line in new]


def record_prices_read(key: DiscoveryKey | HoldoutKey, price_receipt_sha256: str) -> dict:
    """Append ``prices_read`` (window, price-receipt sha256, witness tip) after a price read.

    A resumed run re-reads its window; its receipt must equal the first read's,
    otherwise it is refused (the prices changed under the same frozen inputs).
    """
    if not isinstance(key, (DiscoveryKey, HoldoutKey)):
        raise PermissionError("recording a price read needs its key")
    log = registry(key.log_dir)
    with log.locked():
        records = _chain(log, PREREG_BODY_SHA256)
        earlier = [r for r in _kind(records, "prices_read") if r["window"] == key.window]
        differ = [r for r in earlier if r["price_receipt_sha256"] != price_receipt_sha256]
        if differ:
            raise PermissionError(
                f"{key.window} prices differ from the first read under this registry "
                f"(receipt {differ[0]['price_receipt_sha256'][:12]} then {price_receipt_sha256[:12]}): refused"
            )
        return log.append_locked([{
            "kind": "prices_read",
            "run_at": datetime.now(timezone.utc).isoformat(),
            "prereg_sha256": PREREG_BODY_SHA256,
            "window": key.window,
            "price_receipt_sha256": price_receipt_sha256,
            "witness_tip": key.witness_tip,
            "promotion_allowed": False,
        }])[0]


def _check_observed(frozen_inputs: Mapping[str, Any], observed: Mapping[str, Any]) -> None:
    missing = [k for k in OBSERVED_INPUT_KEYS if k not in observed]
    if missing:
        raise ValueError(f"observed inputs lack {missing}")
    differ = sorted(k for k in set(OBSERVED_INPUT_KEYS) | set(observed) if frozen_inputs.get(k) != observed.get(k))
    if differ:
        raise PermissionError(f"inputs differ from the inputs_frozen record: {differ}")


def freeze_inputs(log_dir: Path, now: datetime, inputs: Mapping[str, Any], *,
                  prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """Append ``inputs_frozen``: the hashes of every input, before any price read.

    Refused once a discovery has been opened (the inputs of a run that has
    read prices cannot change). A re-freeze before that supersedes the earlier
    record (no price has been read under it).
    """
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    missing = [k for k in FROZEN_INPUT_KEYS if k not in inputs]
    if missing:
        raise ValueError(f"inputs_frozen needs {missing}")
    if inputs["sector"] != VS1_SECTOR:
        raise ValueError("only the VS1 sector runs under this registry")
    for k in FROZEN_INPUT_KEYS:
        if k.endswith("_sha256") and not _is_hex64(inputs[k]):
            raise ValueError(f"{k} must be a sha256 hex digest")
    if not isinstance(inputs["accept_underpowered"], bool):
        raise ValueError("accept_underpowered must be a boolean")
    as_of_ts = stamp(inputs["as_of_ts"])
    if as_of_ts > now:
        raise ValueError("as_of_ts cannot be later than the freeze")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        if _kind(records, "discovery_opened"):
            raise PermissionError("a discovery was already opened: its inputs cannot be re-frozen")
        previous = _kind(records, "inputs_frozen")
        record = {
            "kind": "inputs_frozen",
            "run_at": now.isoformat(),
            "prereg_sha256": prereg_sha256,
            "inputs": dict(inputs),
            "supersedes": _record_sha256(previous[-1]) if previous else None,
            "promotion_allowed": False,
        }
        return log.append_locked([record])[0]


def latest_frozen_inputs(log_dir: Path, *, prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """The current ``inputs_frozen`` inputs (read only; appends nothing)."""
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        frozen_inputs = _kind(_chain(log, prereg_sha256), "inputs_frozen")
    if not frozen_inputs:
        raise PermissionError("no inputs_frozen record: run freeze-inputs before any price read")
    return dict(frozen_inputs[-1]["inputs"])


def open_discovery(log_dir: Path, now: datetime, observed: Mapping[str, Any], *,
                   prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """One-shot discovery, step 1: append ``discovery_opened`` (no price is read).

    Requires an ``inputs_frozen`` record whose hashes equal ``observed`` (the
    hashes of the files this run was given). Refused when any discovery was
    already opened or frozen under this pre-registration. Returns the record
    count and chain head the off-host anchor log must then witness before
    :func:`resume_discovery` issues the price key.
    """
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    refuse_superseded(OWN_VERSION_NUMBER, SUPERSEDED_BY)
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        if _kind(records, "discovery_opened") or _kind(records, "discovery_frozen"):
            raise PermissionError("discovery already ran under this pre-registration (one shot)")
        frozen_inputs = _kind(records, "inputs_frozen")
        if not frozen_inputs:
            raise PermissionError("no inputs_frozen record: run freeze-inputs before any price read")
        current = frozen_inputs[-1]
        _check_observed(current["inputs"], observed)
        log.append_locked([{
            "kind": "discovery_opened",
            "run_at": now.isoformat(),
            "prereg_sha256": prereg_sha256,
            "inputs_frozen_sha256": _record_sha256(current),
            "promotion_allowed": False,
        }])
        heads = _line_sha256(log)
    return {"kind": "discovery_opened", "records": len(heads), "head_sha256": heads[-1]}


def resume_discovery(log_dir: Path, observed: Mapping[str, Any], witness: OffhostWitness | None, *,
                     prereg_sha256: str = PREREG_BODY_SHA256) -> DiscoveryKey:
    """One-shot discovery, step 2: the price key, only once the off-host log witnesses it.

    Requires exactly one ``discovery_opened``, no ``discovery_frozen``, inputs
    equal to that discovery's ``inputs_frozen`` record, and the off-host anchor
    log covering the ``discovery_opened`` head (:func:`require_witness`).
    """
    refuse_superseded(OWN_VERSION_NUMBER, SUPERSEDED_BY, witness)
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
    if _kind(records, "discovery_frozen"):
        raise PermissionError("a discovery is already frozen (one shot)")
    position = _position(records, "discovery_opened")
    opened = records[position - 1]
    matching = [r for r in _kind(records, "inputs_frozen") if _record_sha256(r) == opened["inputs_frozen_sha256"]]
    if len(matching) != 1:
        raise PermissionError("discovery_opened does not name an inputs_frozen record of this chain")
    _check_observed(matching[0]["inputs"], observed)
    require_witness(log_dir, witness, position, prereg_sha256=prereg_sha256)
    return DiscoveryKey(_KEY_TOKEN, opened["inputs_frozen_sha256"], matching[0]["inputs"],
                        log_dir=log_dir, witness_tip=witness.tip)


def seal_discovery(log_dir: Path, now: datetime, key: DiscoveryKey, frozen: dict, *,
                   prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """Append ``discovery_frozen`` carrying the frozen discovery manifest's sha256."""
    if not isinstance(key, DiscoveryKey):
        raise PermissionError("sealing a discovery needs its DiscoveryKey")
    payload = frozen.get("payload") or {}
    if digest(payload) != frozen.get("sha256"):
        raise ValueError("frozen discovery manifest does not hash to its sha256")
    if (payload.get("inputs") or {}).get("inputs_frozen_sha256") != key.inputs_frozen_sha256:
        raise PermissionError("the discovery manifest does not carry this key's inputs_frozen hash")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        opened = _kind(records, "discovery_opened")
        if len(opened) != 1 or opened[0]["inputs_frozen_sha256"] != key.inputs_frozen_sha256:
            raise PermissionError("no discovery_opened record for this key")
        if _kind(records, "discovery_frozen"):
            raise PermissionError("a discovery is already frozen (one shot)")
        return log.append_locked([{
            "kind": "discovery_frozen",
            "run_at": now.isoformat(),
            "prereg_sha256": prereg_sha256,
            "inputs_frozen_sha256": key.inputs_frozen_sha256,
            "discovery_sha256": frozen["sha256"],
            "calibration": payload["calibration"]["state"],
            "selected": [t["trial"] for t in payload["ledger"] if t["selected"]],
            "promotion_allowed": False,
        }])[0]


def _holdout_inputs(records: list[dict], frozen: dict, payload: dict) -> dict:
    """The discovery's frozen inputs, after checking the chain froze exactly this file."""
    sealed = _kind(records, "discovery_frozen")
    if len(sealed) != 1:
        raise PermissionError("the registry holds no single frozen discovery")
    if sealed[0]["discovery_sha256"] != frozen["sha256"]:
        raise PermissionError("the frozen discovery file is not the one the registry chain froze")
    inputs_sha = sealed[0]["inputs_frozen_sha256"]
    if (payload.get("inputs") or {}).get("inputs_frozen_sha256") != inputs_sha:
        raise PermissionError("the frozen discovery does not name the chain's inputs_frozen record")
    matching = [r for r in _kind(records, "inputs_frozen") if _record_sha256(r) == inputs_sha]
    if len(matching) != 1:
        raise PermissionError("the discovery's inputs_frozen record is not in the chain")
    return matching[0]["inputs"]


def open_holdout(
    frozen: dict,
    *,
    allow_holdout: bool,
    prereg_sha256: str,
    log_dir: Path,
    now: datetime,
    observed: Mapping[str, Any],
    repo_root: Path = REPO,
) -> dict:
    """One-shot holdout, step 1: explicit flag + pinned hash + the chain, then ``holdout_opened``.

    The chain must hold exactly one ``discovery_frozen`` whose sha256 is the
    frozen file's, the inputs must equal that discovery's ``inputs_frozen``,
    and no holdout may have been opened. No price is read here; the off-host
    log must witness the returned head before :func:`resume_holdout` issues
    the price key.
    """
    refuse_superseded(OWN_VERSION_NUMBER, SUPERSEDED_BY)
    payload = check_holdout_request(frozen, allow_holdout=allow_holdout, prereg_sha256=prereg_sha256,
                                    repo_root=repo_root)
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        inputs = _holdout_inputs(records, frozen, payload)
        if _kind(records, "holdout_opened"):
            raise PermissionError("the holdout was already opened (evaluated once)")
        _check_observed(inputs, observed)
        log.append_locked([{
            "kind": "holdout_opened",
            "run_at": now.isoformat(),
            "prereg_sha256": prereg_sha256,
            "discovery_sha256": frozen["sha256"],
            "promotion_allowed": False,
        }])
        heads = _line_sha256(log)
    return {"kind": "holdout_opened", "records": len(heads), "head_sha256": heads[-1]}


def resume_holdout(
    frozen: dict,
    *,
    allow_holdout: bool,
    prereg_sha256: str,
    log_dir: Path,
    observed: Mapping[str, Any],
    witness: OffhostWitness | None,
    repo_root: Path = REPO,
) -> HoldoutKey:
    """One-shot holdout, step 2: the price key, only once the off-host log witnesses it."""
    refuse_superseded(OWN_VERSION_NUMBER, SUPERSEDED_BY)
    payload = check_holdout_request(frozen, allow_holdout=allow_holdout, prereg_sha256=prereg_sha256,
                                    repo_root=repo_root)
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
    inputs = _holdout_inputs(records, frozen, payload)
    if _kind(records, "holdout_result"):
        raise PermissionError("a holdout result is already recorded (evaluated once)")
    position = _position(records, "holdout_opened")
    if records[position - 1]["discovery_sha256"] != frozen["sha256"]:
        raise PermissionError("holdout_opened names another discovery")
    _check_observed(inputs, observed)
    require_witness(log_dir, witness, position, prereg_sha256=prereg_sha256)
    return HoldoutKey(_HOLDOUT_TOKEN, frozen["sha256"], inputs, log_dir=log_dir, witness_tip=witness.tip)


def seal_holdout(log_dir: Path, now: datetime, key: HoldoutKey, result: dict, *,
                 prereg_sha256: str = PREREG_BODY_SHA256) -> dict:
    """Append ``holdout_result`` (the result's sha256 and verdict state)."""
    if not isinstance(key, HoldoutKey) or result.get("discovery_manifest") != key.frozen_sha256:
        raise PermissionError("the holdout result does not belong to this key")
    log = registry(log_dir, prereg_sha256)
    with log.locked():
        records = _chain(log, prereg_sha256)
        if not any(r.get("discovery_sha256") == key.frozen_sha256 for r in _kind(records, "holdout_opened")):
            raise PermissionError("no holdout_opened record for this discovery")
        if _kind(records, "holdout_result"):
            raise PermissionError("a holdout result is already recorded")
        return log.append_locked([{
            "kind": "holdout_result",
            "run_at": now.isoformat(),
            "prereg_sha256": prereg_sha256,
            "discovery_sha256": key.frozen_sha256,
            "result_sha256": digest(result),
            "verdict": result["verdict"]["state"],
            "promotion_allowed": False,
        }])[0]


# --- orchestration -------------------------------------------------------------------------


def write_frozen(output: Path, name: str, value: Any) -> None:
    Path(output).mkdir(parents=True, exist_ok=True)
    write_once(Path(output) / name, value)
