"""GD4 price-admission basis probe (VS1 v3 §2.3 / v1 §2.3 / plan §2.4).

The probe decides, per ticker, whether a price series may enter a pre-registered
panel study, and writes the admitted-price manifest the VS1 harness freezes
(``freeze-inputs --price-manifest ... --probe-report ...``).

What it is allowed to look at
-----------------------------
Provenance and basis only. It reads ``raw_series`` rows of one candidate
source to establish:

* which source(s) write the series id, and that the admitted read is a single
  source (v1 §2.3 rule "per ticker a single source");
* multi-valued observation dates among that source's SUCCESS rows, after the
  #671 quarantine (rule "zero multi-valued dates");
* split consistency: the same-date adjustment factor ``adj_close / close`` of
  the source is piecewise constant, and every step in it is either a
  distribution (a dividend) or a split with a small-integer ratio, and agrees
  with ``TWELVEDATA_SPLITS`` where that source has a split in the window
  (rule "split-consistent against TIINGO or TWELVEDATA splits within
  tolerance");
* that no row of the admitted series comes from a QUARANTINED batch or a
  refused source (rule "no April-2026 bulk-batch rows"; see
  :data:`INTERPRETATION`);
* coverage start/end, gaps against the benchmark's session calendar, pull
  batches.

What it never does
------------------
It never computes a return, a price change over time, a return distribution
or anything aligned to insider-event dates, and never joins prices to Form 4
events. Price values stay in memory; the report carries only dates, counts,
adjustment-factor step ratios (corporate-action factors, not returns) and a
sha256 of the rows it examined. It refuses to read on or after the holdout
start (2020-01-01), refuses the pre-registration's refused sources, and
writes nothing to the database.

The harness's manifest schema (``analysis.panel_insider_density.PriceManifest``
on main, extended with the optional ``listed_from`` by the v2/v3 harness) is
duplicated in :data:`MANIFEST_KEYS`; ``tests/test_price_admission_probe.py``
checks it against whichever harness classes are importable.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

PROBE_NAME = "vs1-gd4-price-admission-basis-probe"
PROBE_VERSION = 1

#: VS1 v3 pre-registration body this probe implements (§2.3 -> v1 §2.3).
VS1_V3_PREREG_BODY_SHA256 = "fa7eda1c70906720b36dd84d0bb8b65a53f7badc35cd05055e08d7a9b40c2e42"

#: The first holdout instant. Nothing on or after it is ever read (v1 §3, v3 §3).
HOLDOUT_START = date(2020, 1, 1)
#: Discovery window start (v1 §3) and the harness's warm-up (``PRICE_WARMUP_DAYS`` = 60):
#: the harness reads [2012-01-01 - 60 d, 2019-12-31], so the probe examines exactly that span.
DISCOVERY_START = date(2012, 1, 1)
HARNESS_WARMUP_DAYS = 60
DEFAULT_WINDOW = (DISCOVERY_START - timedelta(days=HARNESS_WARMUP_DAYS), HOLDOUT_START - timedelta(days=1))

#: v1 §2.3 refused sources (``analysis.panel_insider_density.REFUSED_PRICE_SOURCES``), plus any
#: yfinance-family name (e.g. ``yfinance_adjusted_extended``, the #671 re-tag target).
REFUSED_SOURCES = frozenset({"yfinance", "yf", "kaggle_bulk", "yfinance_adj"})
REFUSED_PREFIXES = ("yfinance", "yf_", "kaggle")

#: The admitted-price manifest the VS1 harness reads (``PriceManifest.from_file``). Exactly these
#: keys: ``from_file`` passes the JSON object to the dataclass constructor, so any other key fails.
MANIFEST_KEYS = ("source", "series_template", "basis", "benchmark", "admitted", "probe_report_sha256")
MANIFEST_OPTIONAL_KEYS = ("listed_from",)

CLOSE_FIELD, ADJ_FIELD = "close", "adj_close"
#: Tiingo writes ``YF:{ticker}:{field}`` under its own source id (``ingestion/tiingo_pull.py``).
SERIES_PREFIX = "YF"
TD_SPLITS_SOURCE = "TWELVEDATA_SPLITS"

#: Basis declared when every admitted series is the source's split- and dividend-adjusted close.
BASIS_ADJUSTED = "split+dividend adjusted"

# --- tolerances (declared before any row was read) -----------------------------------------
#: Two vintages of one date are the same value if they agree to this relative tolerance
#: (float round-trip only; any real revision is far larger).
SAME_VALUE_RTOL = 1e-9
#: Adjustment-factor step treated as "no corporate action".
FLAT_RTOL = 1e-6
#: A step within this relative distance of a split ratio is a split. Split ratios: k-for-1 and
#: 1-for-k (2 <= k <= MAX_WHOLE_SPLIT), and p-for-q with 2 <= p, q <= MAX_SPLIT_TERM (3-for-2, 5-for-4, ...).
SPLIT_RTOL = 0.005
MAX_SPLIT_TERM = 10
MAX_WHOLE_SPLIT = 100
#: Smallest step that can be a split (|1 - s| >= 0.05); smaller non-flat drops are distributions.
MIN_SPLIT_MOVE = 0.05
#: A distribution may lower the factor by at most this much (s >= 0.75) without being a split.
MAX_DISTRIBUTION_DROP = 0.25
#: A TWELVEDATA split matches an implied split within this many calendar days.
TD_MATCH_DAYS = 3

INTERPRETATION = (
    "Checks are exactly the four of v1 §2.3 / plan §2.4 (single source; zero multi-valued dates after "
    "the #671 quarantine; split-consistent against TIINGO or TWELVEDATA splits; no April-2026 "
    "bulk-batch rows), plus the data-presence precondition that the source has both close and "
    "adj_close rows in the window.",
    "'Split-consistent against TIINGO' when the admitted source is TIINGO itself is checked on "
    "Tiingo's own two series: the same-date factor adj_close/close must be constant except at steps "
    "that are a small-integer split ratio or a distribution. TWELVEDATA_SPLITS rows in the window, "
    "where they exist, must be matched by an implied split.",
    "'No April-2026 bulk-batch rows' is read as the #671 contamination class: the admitted series may "
    "contain no QUARANTINED row and no row of a refused source. For a TIINGO series the second part "
    "holds by construction (reads are constrained to one source id). Tiingo's own history rows were "
    "also written by whole-history pulls in 2026-04/05; the report lists each ticker's pull dates. A "
    "literal reading that refuses every row pulled in April 2026 would refuse every Tiingo series; "
    "that reading is an owner decision, not the probe's.",
    "Coverage, gaps against the benchmark calendar and pull batches are reported, never used to admit.",
)


class ProbeRefused(ValueError):
    """The probe was asked to do something the pre-registration forbids."""


def is_refused_source(name: str) -> bool:
    n = (name or "").strip().lower()
    return not n or n in REFUSED_SOURCES or n.startswith(REFUSED_PREFIXES)


def check_window(start: date, end: date) -> None:
    """Refuse any read span reaching the holdout period (v1 §3: discovery reads stop at 2019-12-31)."""
    if not isinstance(start, date) or not isinstance(end, date):
        raise ProbeRefused("window bounds must be dates")
    if end >= HOLDOUT_START:
        raise ProbeRefused(f"the probe never reads on or after {HOLDOUT_START.isoformat()} (holdout period)")
    if start > end:
        raise ProbeRefused("window start after end")


def series_id(ticker: str, fld: str) -> str:
    return f"{SERIES_PREFIX}:{ticker}:{fld}"


# --- pure checks -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Row:
    obs_date: date
    value: float
    pull_timestamp: datetime | None


def collapse_vintages(rows: Iterable[Row]) -> tuple[dict[date, float], list[str]]:
    """One value per date and the dates whose SUCCESS vintages disagree (beyond float round-trip)."""
    by_date: dict[date, list[float]] = {}
    for r in rows:
        by_date.setdefault(r.obs_date, []).append(float(r.value))
    values, multi = {}, []
    for d in sorted(by_date):
        vs = by_date[d]
        lo, hi = min(vs), max(vs)
        if hi - lo > SAME_VALUE_RTOL * max(abs(lo), abs(hi), 1e-12):
            multi.append(d.isoformat())
        values[d] = vs[0]
    return values, multi


_SPLIT_RATIOS = sorted(
    {Fraction(1, k) for k in range(2, MAX_WHOLE_SPLIT + 1)} | {Fraction(k, 1) for k in range(2, MAX_WHOLE_SPLIT + 1)}
    | {Fraction(q, p) for p in range(2, MAX_SPLIT_TERM + 1) for q in range(2, MAX_SPLIT_TERM + 1) if p != q}
)


def nice_ratio(step: float) -> Fraction | None:
    """The split ratio within ``SPLIT_RTOL`` of ``step`` (see :data:`SPLIT_RTOL`), if any."""
    if not (step > 0 and math.isfinite(step)) or abs(1.0 - step) < MIN_SPLIT_MOVE:
        return None
    best = None
    for ratio in _SPLIT_RATIOS:
        err = abs(step / float(ratio) - 1.0)
        if err <= SPLIT_RTOL and (best is None or err < best[0]):
            best = (err, ratio)
    return best[1] if best else None


def split_label(ratio: Fraction) -> str:
    """``s = q/p`` is a p-for-q split (s = 1/2 -> '2-for-1'; s = 10 -> '1-for-10')."""
    return f"{ratio.denominator}-for-{ratio.numerator}"


def factor_steps(close: Mapping[date, float], adj: Mapping[date, float]) -> dict:
    """Classify every step of the same-date adjustment factor f(t) = adj(t) / close(t).

    Between consecutive common dates ``s = f(prev) / f(t)`` is the corporate-action factor at t:
    1 with no action, 1 - D/C for a distribution, q/p for a p-for-q split. It is a ratio of two
    same-date quantities per date, never a price change over time.
    """
    common = sorted(set(close) & set(adj))
    nonpositive = [d.isoformat() for d in common
                   if not (close[d] > 0 and adj[d] > 0 and math.isfinite(close[d]) and math.isfinite(adj[d]))]
    good = [d for d in common if d.isoformat() not in set(nonpositive)]
    splits, anomalies, distributions, flat = [], [], 0, 0
    prev = None
    for d in good:
        f = adj[d] / close[d]
        if prev is not None:
            s = prev[1] / f
            if abs(s - 1.0) <= FLAT_RTOL:
                flat += 1
            else:
                ratio = nice_ratio(s)
                if ratio is not None:
                    splits.append({"date": d.isoformat(), "ratio": f"{ratio.numerator}/{ratio.denominator}",
                                   "label": split_label(ratio), "step": round(s, 6)})
                elif 1.0 - MAX_DISTRIBUTION_DROP <= s < 1.0:
                    distributions += 1
                else:
                    anomalies.append({"date": d.isoformat(), "step": round(s, 6)})
        prev = (d, f)
    return {
        "common_dates": len(common),
        "nonpositive_dates": nonpositive,
        "flat_steps": flat,
        "distribution_steps": distributions,
        "implied_splits": splits,
        "anomalous_steps": anomalies,
    }


def match_td_splits(implied: Sequence[Mapping], td: Sequence[tuple[date, float]]) -> dict:
    """Every TWELVEDATA split in the window must appear as an implied split of the same ratio."""
    unmatched = []
    for d, ratio in td:
        ok = any(
            abs((date.fromisoformat(s["date"]) - d).days) <= TD_MATCH_DAYS
            and ratio > 0 and abs(s["step"] / ratio - 1.0) <= SPLIT_RTOL
            for s in implied
        )
        if not ok:
            unmatched.append({"date": d.isoformat(), "ratio": ratio})
    return {"td_splits_in_window": len(td), "td_splits_unmatched": unmatched}


def calendar_gaps(dates: Iterable[date], calendar: Sequence[date]) -> dict:
    """Benchmark sessions between the series' first and last date that the series lacks."""
    ds = sorted(set(dates))
    if not ds or not calendar:
        return {"missing_sessions": None, "longest_gap_sessions": None, "off_calendar_dates": None}
    have = set(ds)
    cal = [c for c in calendar if ds[0] <= c <= ds[-1]]
    missing, run, longest = 0, 0, 0
    for c in cal:
        if c in have:
            run = 0
        else:
            missing += 1
            run += 1
            longest = max(longest, run)
    return {"missing_sessions": missing, "longest_gap_sessions": longest,
            "off_calendar_dates": len(have - set(calendar))}


def rows_sha256(rows: Iterable[Row]) -> str:
    """Fingerprint of the rows examined (so a later read can be compared), not their values."""
    lines = sorted(f"{r.obs_date.isoformat()}|{float(r.value)!r}|"
                   f"{r.pull_timestamp.isoformat() if r.pull_timestamp else ''}" for r in rows)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def assess_ticker(
    ticker: str,
    source: str,
    close_rows: Sequence[Row],
    adj_rows: Sequence[Row],
    *,
    statuses: Mapping[str, Mapping[str, int]],
    other_sources: Sequence[str],
    td_splits: Sequence[tuple[date, float]] = (),
    calendar: Sequence[date] = (),
) -> dict:
    """The admission record of one ticker. ``statuses``: {series_id: {pull_status: rows}} of the source."""
    reasons = []
    if is_refused_source(source):
        raise ProbeRefused(f"source {source!r} is refused by the pre-registration")
    close, close_multi = collapse_vintages(close_rows)
    adj, adj_multi = collapse_vintages(adj_rows)

    present = bool(close) and bool(adj)
    if not present:
        reasons.append("no_source_series" if not close and not adj else
                       ("no_adjusted_series" if not adj else "no_close_series"))

    # 1. single source: the admitted read is constrained to one source id; report who else writes the id.
    single_source = present and not is_refused_source(source)

    # 2. zero multi-valued dates among SUCCESS rows (QUARANTINED rows are not SUCCESS).
    zero_multi = not close_multi and not adj_multi
    if present and not zero_multi:
        reasons.append("multi_valued_dates")

    # 3. split consistency against the source itself and TWELVEDATA_SPLITS.
    steps = factor_steps(close, adj) if present else None
    td = match_td_splits(steps["implied_splits"], td_splits) if steps else {"td_splits_in_window": len(td_splits),
                                                                             "td_splits_unmatched": []}
    split_ok = bool(steps) and not steps["anomalous_steps"] and not steps["nonpositive_dates"] \
        and not td["td_splits_unmatched"]
    if present and not split_ok:
        reasons.append("split_inconsistent")

    # 4. no rows from a quarantined (#671) batch or a refused source in the admitted series.
    quarantined = sum(int(s.get("QUARANTINED", 0)) for s in statuses.values())
    no_bulk = quarantined == 0 and not is_refused_source(source)
    if present and not no_bulk:
        reasons.append("quarantined_batch_rows")

    pulls = sorted({r.pull_timestamp for r in list(close_rows) + list(adj_rows) if r.pull_timestamp})
    dates = sorted(set(close) | set(adj))
    admitted = present and single_source and zero_multi and split_ok and no_bulk
    return {
        "ticker": ticker,
        "admitted": admitted,
        "reasons": reasons,
        "source": source,
        "series": {"close": series_id(ticker, CLOSE_FIELD), "adj_close": series_id(ticker, ADJ_FIELD)},
        "checks": {
            "has_close_and_adj_close": present,
            "single_source": single_source,
            "zero_multi_valued_dates": zero_multi if present else None,
            "split_consistent": split_ok if present else None,
            "no_april_2026_bulk_batch_rows": no_bulk if present else None,
        },
        "coverage": {
            "first_date": dates[0].isoformat() if dates else None,
            "last_date": dates[-1].isoformat() if dates else None,
            "close_dates": len(close),
            "adj_close_dates": len(adj),
            "close_without_adj": len(set(close) - set(adj)),
            "adj_without_close": len(set(adj) - set(close)),
            **calendar_gaps(dates, calendar),
        },
        "multi_valued": {"close_dates": close_multi[:20], "close_count": len(close_multi),
                         "adj_close_dates": adj_multi[:20], "adj_close_count": len(adj_multi)},
        "factor_steps": steps,
        "twelvedata_splits": td,
        "pull_batches": {
            "count": len(pulls),
            "first": pulls[0].isoformat() if pulls else None,
            "last": pulls[-1].isoformat() if pulls else None,
            "in_april_2026": sum(1 for p in pulls if (p.year, p.month) == (2026, 4)),
        },
        "row_status_counts": {k: dict(sorted(v.items())) for k, v in sorted(statuses.items())},
        "other_sources_on_series_id": sorted(set(other_sources) - {source}),
        "rows_examined": {"close": len(close_rows), "adj_close": len(adj_rows),
                          "close_sha256": rows_sha256(close_rows), "adj_close_sha256": rows_sha256(adj_rows)},
    }


# --- database reads (read-only; never on or after HOLDOUT_START) --------------------------------

_SOURCE_SQL = "SELECT id, name FROM source_catalog WHERE LOWER(name) = LOWER(:name)"
_ROWS_SQL = (
    "SELECT obs_date, value, pull_timestamp FROM raw_series "
    "WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS' "
    "AND obs_date >= :lo AND obs_date <= :hi ORDER BY obs_date, pull_timestamp"
)
_STATUS_SQL = (
    "SELECT pull_status, COUNT(*) FROM raw_series "
    "WHERE series_id = :sid AND source_id = :src AND obs_date >= :lo AND obs_date <= :hi GROUP BY pull_status"
)
# Loose index scan over (series_id, source_id, ...): the source ids with a SUCCESS row in the window.
_OTHER_SOURCES_SQL = (
    "WITH RECURSIVE s(id) AS ("
    " SELECT MIN(source_id) FROM raw_series WHERE series_id = :sid AND pull_status = 'SUCCESS'"
    "   AND obs_date >= :lo AND obs_date <= :hi"
    " UNION ALL"
    " SELECT (SELECT MIN(r.source_id) FROM raw_series r WHERE r.series_id = :sid AND r.source_id > s.id"
    "         AND r.pull_status = 'SUCCESS' AND r.obs_date >= :lo AND r.obs_date <= :hi)"
    " FROM s WHERE s.id IS NOT NULL"
    ") SELECT sc.name FROM s JOIN source_catalog sc ON sc.id = s.id"
)


def _d(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def _ts(v: Any) -> datetime | None:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v))


def resolve_source(conn, name: str) -> tuple[int, str]:
    if is_refused_source(name):
        raise ProbeRefused(f"source {name!r} is refused by the pre-registration")
    found = conn.execute(text(_SOURCE_SQL), {"name": name}).fetchall()
    if len(found) != 1:
        raise ProbeRefused(f"source_catalog has {len(found)} rows named {name!r}; need exactly one")
    return int(found[0][0]), str(found[0][1])


def read_rows(conn, sid: str, source_id: int, lo: date, hi: date) -> list[Row]:
    check_window(lo, hi)
    rows = conn.execute(text(_ROWS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi}).fetchall()
    return [Row(_d(r[0]), float(r[1]), _ts(r[2])) for r in rows if r[1] is not None]


def read_statuses(conn, sid: str, source_id: int, lo: date, hi: date) -> dict[str, int]:
    check_window(lo, hi)
    return {str(s): int(n) for s, n in
            conn.execute(text(_STATUS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi}).fetchall()}


def read_other_sources(conn, sid: str, lo: date, hi: date) -> list[str]:
    check_window(lo, hi)
    return sorted({str(r[0]) for r in conn.execute(text(_OTHER_SOURCES_SQL),
                                                    {"sid": sid, "lo": lo, "hi": hi}).fetchall()})


def read_td_splits(conn, ticker: str, lo: date, hi: date) -> list[tuple[date, float]]:
    """``TWELVEDATA_SPLITS:{ticker}:ratio`` SUCCESS rows in the window (split ratios, not prices)."""
    check_window(lo, hi)
    found = conn.execute(text(_SOURCE_SQL), {"name": TD_SPLITS_SOURCE}).fetchall()
    if len(found) != 1:
        return []
    rows = read_rows(conn, f"{TD_SPLITS_SOURCE}:{ticker}:ratio", int(found[0][0]), lo, hi)
    by_date, _ = collapse_vintages(rows)
    return sorted(by_date.items())


def probe_ticker(conn, ticker: str, source_id: int, source: str, lo: date, hi: date,
                 calendar: Sequence[date] = ()) -> dict:
    check_window(lo, hi)
    close_sid, adj_sid = series_id(ticker, CLOSE_FIELD), series_id(ticker, ADJ_FIELD)
    close_rows = read_rows(conn, close_sid, source_id, lo, hi)
    adj_rows = read_rows(conn, adj_sid, source_id, lo, hi)
    statuses = {close_sid: read_statuses(conn, close_sid, source_id, lo, hi),
                adj_sid: read_statuses(conn, adj_sid, source_id, lo, hi)}
    others = sorted(set(read_other_sources(conn, close_sid, lo, hi)) | set(read_other_sources(conn, adj_sid, lo, hi)))
    return assess_ticker(ticker, source, close_rows, adj_rows, statuses=statuses, other_sources=others,
                         td_splits=read_td_splits(conn, ticker, lo, hi), calendar=calendar)


def run_probe(conn, tickers: Sequence[str], *, benchmark: str, source: str, lo: date, hi: date,
              progress=None) -> dict:
    """Probe the benchmark first (its admitted dates are the session calendar), then every ticker."""
    check_window(lo, hi)
    source_id, source_name = resolve_source(conn, source)
    wanted = sorted(set(tickers) - {benchmark})
    records = {benchmark: probe_ticker(conn, benchmark, source_id, source_name, lo, hi)}
    calendar: list[date] = []
    if records[benchmark]["admitted"]:
        close, _ = collapse_vintages(read_rows(conn, series_id(benchmark, ADJ_FIELD), source_id, lo, hi))
        calendar = sorted(close)
    for i, t in enumerate(wanted):
        try:
            records[t] = probe_ticker(conn, t, source_id, source_name, lo, hi, calendar)
        except ProbeRefused:
            raise
        except Exception as exc:  # a per-ticker read failure refuses that ticker, visibly
            records[t] = {"ticker": t, "admitted": False, "reasons": ["probe_error"], "source": source_name,
                          "error": type(exc).__name__}
        if progress:
            progress(i + 1, len(wanted), t)
    return {"source": {"name": source_name, "id": source_id}, "calendar_sessions": len(calendar),
            "records": records}


# --- report and manifest -------------------------------------------------------------------------


def build_report(probe: Mapping, *, tickers: Sequence[str], benchmark: str, lo: date, hi: date,
                 code_sha: str, snapshot_as_of_ts: datetime, inputs: Mapping[str, Any]) -> dict:
    records = probe["records"]
    admitted = sorted(t for t, r in records.items() if r["admitted"])
    reasons: dict[str, int] = {}
    for r in records.values():
        for reason in r["reasons"]:
            reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "probe": PROBE_NAME,
        "probe_version": PROBE_VERSION,
        "prereg": {"study": "VS1 v3", "body_sha256": VS1_V3_PREREG_BODY_SHA256,
                   "rules": "v3 §2.3 -> v1 §2.3; plan §2.4"},
        "code_sha": code_sha,
        "snapshot_as_of_ts": snapshot_as_of_ts.astimezone(timezone.utc).isoformat(),
        "window": {"start": lo.isoformat(), "end": hi.isoformat(),
                   "why": "discovery 2012-01-01..2019-12-31 plus the harness's 60-day warm-up; "
                          "nothing on or after 2020-01-01 is read"},
        "source": dict(probe["source"]),
        "refused_sources": sorted(REFUSED_SOURCES) + [f"{p}*" for p in REFUSED_PREFIXES],
        "series_template": f"{SERIES_PREFIX}:{{ticker}}:{ADJ_FIELD}",
        "basis": BASIS_ADJUSTED,
        "benchmark": benchmark,
        "benchmark_admitted": bool(records.get(benchmark, {}).get("admitted")),
        "benchmark_calendar_sessions": probe["calendar_sessions"],
        "tolerances": {"same_value_rtol": SAME_VALUE_RTOL, "flat_rtol": FLAT_RTOL, "split_rtol": SPLIT_RTOL,
                       "max_split_term": MAX_SPLIT_TERM, "max_whole_split": MAX_WHOLE_SPLIT, "min_split_move": MIN_SPLIT_MOVE,
                       "max_distribution_drop": MAX_DISTRIBUTION_DROP, "td_match_days": TD_MATCH_DAYS},
        "interpretation": list(INTERPRETATION),
        "listed_from": "omitted: the admitted source's own listing metadata is not stored in the database",
        "inputs": dict(inputs),
        "summary": {
            "candidates": len(set(tickers) | {benchmark}),
            "admitted": len(admitted),
            "admitted_excluding_benchmark": len([t for t in admitted if t != benchmark]),
            "not_admitted_by_reason": dict(sorted(reasons.items())),
            "promotion_allowed": False,
        },
        "admitted": admitted,
        "tickers": {t: records[t] for t in sorted(records)},
    }


def build_manifest(report: Mapping, probe_report_sha256: str) -> dict:
    """The harness's admitted-price manifest (exactly :data:`MANIFEST_KEYS`)."""
    if not report["benchmark_admitted"]:
        raise ProbeRefused(f"benchmark {report['benchmark']} failed the probe: no manifest can be built")
    if is_refused_source(report["source"]["name"]):
        raise ProbeRefused("refused source")
    manifest = {
        "source": report["source"]["name"],
        "series_template": report["series_template"],
        "basis": report["basis"],
        "benchmark": report["benchmark"],
        "admitted": sorted(report["admitted"]),
        "probe_report_sha256": probe_report_sha256,
    }
    assert tuple(manifest) == MANIFEST_KEYS
    return manifest


def file_sha256(path: Path) -> str:
    """Exact-bytes sha256 (the harness's ``data_sha256`` of ``--probe-report``)."""
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json_once(path: Path, value: Any) -> str:
    data = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    with open(path, "xb") as stream:
        stream.write(data)
    return hashlib.sha256(data).hexdigest()


# --- issuer-event coverage (no prices) ------------------------------------------------------------


@dataclass
class Coverage:
    issuers: int
    issuers_price_admitted: int
    events: int
    events_price_admitted: int
    issuers_with_events: int
    issuers_with_events_price_admitted: int
    not_admitted: list[str] = field(default_factory=list)

    @property
    def event_fraction(self) -> float:
        return self.events_price_admitted / self.events if self.events else 0.0


def event_coverage(issuers: Sequence[Mapping], admitted_tickers: Iterable[str]) -> Coverage:
    """Share of filings-admitted issuer purchase events whose issuer's price ticker is price-admitted.

    Issuer-level only: an event counts when its issuer's ticker is on the manifest. No event date is
    compared with any price, price date or coverage span.
    """
    ok = set(admitted_tickers)
    events = sum(int(m["admitted_purchase_events_2012_2019"]) for m in issuers)
    hit = [m for m in issuers if m["ticker"] in ok]
    return Coverage(
        issuers=len(issuers),
        issuers_price_admitted=len(hit),
        events=events,
        events_price_admitted=sum(int(m["admitted_purchase_events_2012_2019"]) for m in hit),
        issuers_with_events=sum(1 for m in issuers if int(m["admitted_purchase_events_2012_2019"]) > 0),
        issuers_with_events_price_admitted=sum(1 for m in hit if int(m["admitted_purchase_events_2012_2019"]) > 0),
        not_admitted=sorted(m["ticker"] for m in issuers if m["ticker"] not in ok),
    )
