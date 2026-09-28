"""GD4 price-admission probe for VS1 v6 (§2.2 rules 3-4, §2.3; research only, read-only).

The probe decides, per ticker, whether a price series may enter the VS1 v6 panel,
and writes the admitted-price manifest the v6 harness freezes
(``scripts/run_vs1_v6_insider_density.py ... --price-manifest M --probe-report P
--crosscheck-report C --tiingo-meta-report T``). It implements v6 §2.3 verbatim;
where this code and ``docs/paper_log/vs1-insider-density-v6-preregistration.md``
differ, the text governs.

The rule, per ticker, over the discovery read window 2011-11-02 -> 2019-12-31
(``CROSSCHECK.discovery_window``: discovery start minus the harness's 60-day warm-up):

* **Source** TIINGO (``source_catalog`` 524), ``YF:{T}:adj_close``; ``YF:{T}:close``
  (TIINGO) only for the adjustment factor. Every read is source-filtered to TIINGO and
  bounded by the snapshot ``as_of_ts``. Rows of other sources under the same series id
  are ignored, counted per ticker and reported, never disqualifying.
* **Basis checks** (GD4, unchanged): (1) zero multi-valued dates among TIINGO SUCCESS
  vintages; (2) every step of ``adj_close / close`` a split ratio within
  :data:`SPLIT_RTOL` or a distribution of at most 25%; (3) no benchmark session inside
  the ticker's span missing a close; (4) no QUARANTINED row.
* **Pull-batch splice check** (v5): ``v6.splice_check`` on the selected rows (the rows
  the frozen read returns) with each date's pull batch, corroborated by TwelveData.
* **TwelveData cross-check** (v4): ``v6.crosscheck_statistics`` with ``v6.CROSSCHECK``
  (X = 99%, Y = 10 bp, N = 250, adjustment pairs over 1e-4 excluded, at most 10%).
* **Entity check and listing start** (v5/v6): Tiingo ``/tiingo/daily/{T}`` metadata;
  ``no_meta`` without it, ``entity_mismatch`` unless ``v6.name_match(SEC name, Tiingo
  name)``; the manifest's ``listed_from[T]`` is Tiingo ``startDate`` (C1, N = 0).
* **C1 ticker interval** (§2.2 rule 3): applied to closes by the v6 harness
  (``v6.build_trial_panels``); the probe evaluates the same interval
  (``v6.interval_close_mask`` = ``v2.ticker_mask`` at session closes) only to report how
  many window sessions it and ``startDate`` blank. It never gates admission here.

What it never does: align anything to a Form 4 event, compute a label, an event or a
forward return, read on or after 2020-01-01, or write to the database. The only
returns computed are the vendor-agreement returns inside ``crosscheck_statistics``,
and no report carries a price.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import text

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis import price_admission_fetch as fetch

PROBE_NAME = "vs1-gd4-price-admission-probe"
PROBE_VERSION = 2

#: The pre-registration this probe implements (v6 §2.3; v4/v5 rules carried into v6).
STUDY = v6.VERSION
PREREG_BODY_SHA256 = v6.PREREG_BODY_SHA256

PRICE_SOURCE = v6.PRICE_SOURCE
PRICE_SOURCE_ID = v6.PRICE_SOURCE_ID
SERIES_TEMPLATE = v6.SERIES_TEMPLATE
BASIS = v6.BASIS
BENCHMARK = v6.BENCHMARK
CROSSCHECK = v6.CROSSCHECK
SPLICE_TOL = v6.SPLICE_TOL

#: The first holdout instant. Nothing on or after it is ever read (v6 §3, §8).
HOLDOUT_START = date(2020, 1, 1)
#: The discovery read window, which is also the cross-check window (v6 §2.3).
DEFAULT_WINDOW = tuple(date.fromisoformat(d) for d in CROSSCHECK.discovery_window)

#: v1 §2.3 refused sources plus the yfinance family; the probe additionally admits only TIINGO.
REFUSED_SOURCES = frozenset({"yfinance", "yf", "kaggle_bulk", "yfinance_adj"}) | set(v1.REFUSED_PRICE_SOURCES)
REFUSED_PREFIXES = ("yfinance", "yf_", "kaggle")

CLOSE_FIELD, ADJ_FIELD = "close", "adj_close"
SERIES_PREFIX = "YF"
TD_SPLITS_SOURCE = "TWELVEDATA_SPLITS"
BASIS_ADJUSTED = BASIS

# --- GD4 tolerances (declared before any row was read; unchanged) ------------------------------------
SAME_VALUE_RTOL = 1e-9
FLAT_RTOL = 1e-6
SPLIT_RTOL = 0.005
MAX_SPLIT_TERM = 10
MAX_WHOLE_SPLIT = 100
MIN_SPLIT_MOVE = 0.05
MAX_DISTRIBUTION_DROP = 0.25
TD_MATCH_DAYS = 3
#: Run-report note 3 (review round 3): the 1e-4 / 10 bp tolerances are justified only above about $1.
LOW_PRICE_USD = 1.0

INTERPRETATION = (
    "Admission (v6 §2.3): basis checks 1-4, the pull-batch splice check, the TwelveData cross-check and, for "
    "issuer tickers, the Tiingo-metadata entity check. The benchmark has no SEC issuer name and skips the "
    "entity check; it is not given a listed_from.",
    "Source filtering (v5): every read is TIINGO SUCCESS rows with pull_timestamp <= the snapshot. Other "
    "sources' rows under the same series id are counted and reported, never read and never disqualifying.",
    "Basis check 1 uses every TIINGO SUCCESS vintage up to the snapshot; checks 2-3, the splice check and the "
    "cross-check use the selected rows (store.observations.read_window: the latest vintage per date), which "
    "are the rows the frozen harness read returns.",
    "Basis check 3: a benchmark session between a ticker's first and last TIINGO date in the window that lacks "
    "an adj_close, or lacks a close, is a gap.",
    "C1 (§2.2 rule 3, §2.3): the ticker interval and Tiingo startDate blank closes in the harness "
    "(v6.build_trial_panels). The probe reports the number of window sessions each would blank; the checks "
    "above run over the whole window, as §2.3 states.",
    "TwelveData: the registered window 2011-11-02..2019-12-31 is inclusive. TwelveData's end_date is exclusive, "
    "so the window is requested with end_date=2020-01-01 (a market holiday; coordinator decision 2026-09-28). "
    "Receipts made earlier with end_date=2019-12-31 are completed by a receipted 2019-12-20..2020-01-01 "
    "supplement whose overlapping dates must agree exactly. Any TwelveData row dated on or after 2020-01-01 fails "
    "closed (the fetch stops without saving it; a saved file carrying one drops that mode, reason "
    "twelvedata_holdout_rows).",
    "Low prices (run-report note 3): tickers with any raw TIINGO close below $1 in the window are counted, and "
    "how many of them the splice check or the cross-check exclude.",
)


class ProbeRefused(ValueError):
    """The probe was asked to do something the pre-registration forbids."""


def is_refused_source(name: str) -> bool:
    n = (name or "").strip().lower()
    return not n or n in REFUSED_SOURCES or n.startswith(REFUSED_PREFIXES)


def check_source(name: str) -> None:
    """v6 §2.3: TIINGO is the single admitted source; everything else is refused."""
    if is_refused_source(name) or name.strip().upper() != PRICE_SOURCE:
        raise ProbeRefused(f"source {name!r} is refused: VS1 v6 admits only {PRICE_SOURCE}")


def check_window(start: date, end: date) -> None:
    """Refuse any read span reaching the holdout period (discovery reads stop at 2019-12-31)."""
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
    """Classify every step of the same-date adjustment factor f(t) = adj(t) / close(t) (basis check 2)."""
    common = sorted(set(close) & set(adj))
    nonpositive = [d.isoformat() for d in common
                   if not (close[d] > 0 and adj[d] > 0 and math.isfinite(close[d]) and math.isfinite(adj[d]))]
    bad = set(nonpositive)
    good = [d for d in common if d.isoformat() not in bad]
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
    return {"common_dates": len(common), "nonpositive_dates": nonpositive, "flat_steps": flat,
            "distribution_steps": distributions, "implied_splits": splits, "anomalous_steps": anomalies}


def match_td_splits(implied: Sequence[Mapping], td: Sequence[tuple[date, float]]) -> dict:
    """Every TWELVEDATA_SPLITS split in the window must appear as an implied split of the same ratio."""
    unmatched = []
    for d, ratio in td:
        ok = any(abs((date.fromisoformat(s["date"]) - d).days) <= TD_MATCH_DAYS
                 and ratio > 0 and abs(s["step"] / ratio - 1.0) <= SPLIT_RTOL for s in implied)
        if not ok:
            unmatched.append({"date": d.isoformat(), "ratio": ratio})
    return {"td_splits_in_window": len(td), "td_splits_unmatched": unmatched}


def calendar_gaps(dates: Iterable[date], calendar: Sequence[date]) -> dict:
    """Benchmark sessions between the series' first and last date that the series lacks."""
    ds = sorted(set(dates))
    if not ds or not calendar:
        return {"missing_sessions": None, "longest_gap_sessions": None, "off_calendar_dates": None}
    have = set(ds)
    missing, run, longest = 0, 0, 0
    for c in calendar:
        if not ds[0] <= c <= ds[-1]:
            continue
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


def pull_batch(ts: datetime | None) -> str:
    """The pull batch of a selected row: the UTC calendar date of its pull_timestamp (v5 §2.3)."""
    if ts is None:
        return "none"
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc)
    return ts.date().isoformat()


def batch_labels(selected_adj: Sequence[Row], selected_close: Sequence[Row]) -> dict[str, str]:
    """date -> pull batch; a date whose adj_close and close rows come from different batches gets a joined label."""
    adj = {r.obs_date: pull_batch(r.pull_timestamp) for r in selected_adj}
    close = {r.obs_date: pull_batch(r.pull_timestamp) for r in selected_close}
    out = {}
    for d in sorted(set(adj) & set(close)):
        out[d.isoformat()] = adj[d] if adj[d] == close[d] else f"{adj[d]}|{close[d]}"
    return out


def _iso(values: Mapping[date, float]) -> dict[str, float]:
    return {d.isoformat(): float(v) for d, v in values.items()}


def interval_counts(inside: Sequence[bool] | None, calendar: Sequence[date], start_date: str | None,
                    close_dates: Iterable[date]) -> dict | None:
    """Window sessions the C1 interval and Tiingo startDate would blank (dates only; reported, never gating)."""
    if inside is None:
        return None
    have = set(close_dates)
    s_t = date.fromisoformat(start_date[:10]) if start_date else None
    in_interval = sum(bool(x) for x in inside)
    before = sum(1 for d in calendar if s_t is not None and d < s_t)
    with_close = [d for d in calendar if d in have]
    used = sum(1 for d, x in zip(calendar, inside) if d in have and x and (s_t is None or d >= s_t))
    return {"sessions": len(calendar), "inside_interval": in_interval, "outside_interval": len(calendar) - in_interval,
            "before_start_date": before if s_t is not None else None, "sessions_with_close": len(with_close),
            "closes_used": used, "closes_blanked": len(with_close) - used,
            "first_inside": next((d.isoformat() for d, x in zip(calendar, inside) if x), None)}


def assess_ticker(
    ticker: str,
    *,
    vintages_close: Sequence[Row],
    vintages_adj: Sequence[Row],
    selected_close: Sequence[Row],
    selected_adj: Sequence[Row],
    statuses: Mapping[str, Mapping[str, int]],
    other_source_rows: Mapping[str, Mapping[str, int]],
    calendar: Sequence[date],
    td: Mapping[str, Any] | None,
    meta: Mapping[str, Any] | None,
    sec_name: str | None,
    is_benchmark: bool = False,
    td_splits: Sequence[tuple[date, float]] = (),
    inside_interval: Sequence[bool] | None = None,
) -> dict:
    """One ticker's admission record (v6 §2.3). ``statuses``: {series_id: {pull_status: rows}} of TIINGO."""
    reasons: list[str] = []
    close = {r.obs_date: float(r.value) for r in selected_close}
    adj = {r.obs_date: float(r.value) for r in selected_adj}
    present = bool(close) and bool(adj)
    if not present:
        reasons.append("no_source_series" if not close and not adj else
                       ("no_adjusted_series" if not adj else "no_close_series"))

    # basis 1: zero multi-valued dates among TIINGO SUCCESS vintages
    _, close_multi = collapse_vintages(vintages_close)
    _, adj_multi = collapse_vintages(vintages_adj)
    zero_multi = not close_multi and not adj_multi
    if present and not zero_multi:
        reasons.append("multi_valued_dates")

    # basis 2: adjustment-factor steps are split ratios or distributions of at most 25%
    steps = factor_steps(close, adj) if present else None
    td_split = match_td_splits(steps["implied_splits"], td_splits) if steps else {
        "td_splits_in_window": len(td_splits), "td_splits_unmatched": []}
    split_ok = bool(steps) and not steps["anomalous_steps"] and not steps["nonpositive_dates"] \
        and not td_split["td_splits_unmatched"]
    if present and not split_ok:
        reasons.append("split_inconsistent")

    # basis 3: no benchmark session missing a close inside the ticker's span
    gaps_adj = calendar_gaps(adj, calendar)
    gaps_close = calendar_gaps(close, calendar)
    no_gaps = present and bool(calendar) and gaps_adj["missing_sessions"] == 0 and gaps_close["missing_sessions"] == 0
    if present and not no_gaps:
        reasons.append("calendar_gaps")

    # basis 4: no QUARANTINED row
    quarantined = sum(int(s.get("QUARANTINED", 0)) for s in statuses.values())
    if present and quarantined:
        reasons.append("quarantined_rows")

    # the TwelveData files (both adjust modes)
    td = td or {}
    td_all, td_none = td.get("all"), td.get("none")
    td_receipts = td.get("receipts") or {}
    td_state = td.get("state") or ("ok" if td_all and td_none else
                                   "not_fetched" if len(td_receipts) < len(fetch.TD_ADJUST_MODES) else "unavailable")
    if td_state == "ok" and not (td_all and td_none):
        td_state = "not_fetched"

    sessions = [d.isoformat() for d in calendar]
    adj_iso, close_iso = _iso(adj), _iso(close)

    # pull-batch splice check (v5): selected rows, TwelveData-corroborated
    splice = v6.splice_check(sessions, batch_labels(selected_adj, selected_close), adj_iso, close_iso,
                             td_all or None, td_none or None) if present else None
    splice_ok = bool(splice and splice["passed"])
    if present and not splice_ok:
        reasons.append("splice_failed")

    # TwelveData return cross-check (v4)
    cross = v6.crosscheck_statistics(sessions, adj_iso, close_iso, td_all or {}, td_none or {})
    if td_state != "ok":
        reasons.append(f"twelvedata_{td_state}")
    if not cross["passed"]:
        reasons.append(f"crosscheck_{cross['reason']}")

    # Tiingo metadata: listing start (C1, N = 0) and the entity check (issuers only)
    m = (meta or {}).get("meta") if meta else None
    start_date = str(m["startDate"])[:10] if m and m.get("startDate") else None
    entity: dict[str, Any] = {"sec_name": sec_name, "tiingo_name": m.get("name") if m else None}
    if is_benchmark:
        entity["check"] = "not_applicable_benchmark"
        entity_ok = True
    elif not m or not start_date:
        entity["check"] = "no_meta"
        entity_ok = False
        reasons.append("no_meta")
    else:
        entity_ok = bool(sec_name) and v6.name_match(sec_name, m.get("name") or "")
        entity["check"] = "match" if entity_ok else "entity_mismatch"
        if not entity_ok:
            reasons.append("entity_mismatch")

    low_close = sum(1 for v in close.values() if v < LOW_PRICE_USD)
    low_adj = sum(1 for v in adj.values() if v < LOW_PRICE_USD)
    pulls = sorted({r.pull_timestamp for r in list(vintages_close) + list(vintages_adj) if r.pull_timestamp})
    dates = sorted(set(close) | set(adj))
    admitted = (present and zero_multi and split_ok and no_gaps and not quarantined and splice_ok
                and cross["passed"] and td_state == "ok" and entity_ok)
    return {
        "ticker": ticker,
        "benchmark": is_benchmark,
        "admitted": bool(admitted),
        "reasons": reasons,
        "listed_from": None if is_benchmark else start_date,
        "checks": {
            "has_close_and_adj_close": present,
            "zero_multi_valued_dates": zero_multi if present else None,
            "split_consistent": split_ok if present else None,
            "no_gaps": no_gaps if present else None,
            "no_quarantined_rows": (quarantined == 0) if present else None,
            "splice": splice_ok if present else None,
            "crosscheck": cross["passed"],
            "entity": entity["check"],
        },
        "coverage": {
            "first_date": dates[0].isoformat() if dates else None,
            "last_date": dates[-1].isoformat() if dates else None,
            "close_dates": len(close), "adj_close_dates": len(adj),
            "close_without_adj": len(set(close) - set(adj)), "adj_without_close": len(set(adj) - set(close)),
            "gaps_adj_close": gaps_adj, "gaps_close": gaps_close,
        },
        "multi_valued": {"close_dates": close_multi[:20], "close_count": len(close_multi),
                         "adj_close_dates": adj_multi[:20], "adj_close_count": len(adj_multi)},
        "factor_steps": steps,
        "twelvedata_splits": td_split,
        "splice": None if splice is None else {
            "boundaries": splice["boundaries"], "failed": splice["failed"], "passed": splice["passed"],
            "detail": [{"pair": b["pair"], "step": round(b["step"], 8), "passed": b["passed"]}
                       for b in splice["detail"]]},
        "crosscheck": cross,
        "twelvedata": {"state": td_state, "receipts": td_receipts,
                       "dates": {a: len(td.get(a) or {}) for a in fetch.TD_ADJUST_MODES},
                       "has_window_end": bool(td_all and td_none and fetch.TD_WINDOW_END in td_all
                                              and fetch.TD_WINDOW_END in td_none),
                       "holdout_rows": int(td.get("holdout_rows", 0)),
                       "supplemented": td.get("supplemented") or [], "problems": td.get("problems") or []},
        "tiingo_meta": {"receipt": (meta or {}).get("receipt"), "meta": m},
        "entity": entity,
        "c1": interval_counts(inside_interval, calendar, start_date, adj),
        "low_price": {"sessions_close_below_1usd": low_close, "sessions_adj_close_below_1usd": low_adj},
        "source_filtering": {"other_source_rows": {k: dict(sorted(v.items()))
                                                   for k, v in sorted(other_source_rows.items())},
                             "other_source_rows_total": int(sum(sum(v.values()) for v in other_source_rows.values()))},
        "pull_batches": {"count": len(pulls), "first": pulls[0].isoformat() if pulls else None,
                         "last": pulls[-1].isoformat() if pulls else None,
                         "selected_batches": sorted(set(batch_labels(selected_adj, selected_close).values()))},
        "row_status_counts": {k: dict(sorted(v.items())) for k, v in sorted(statuses.items())},
        "rows_examined": {"close": len(vintages_close), "adj_close": len(vintages_adj),
                          "close_sha256": rows_sha256(vintages_close), "adj_close_sha256": rows_sha256(vintages_adj),
                          "selected_close_sha256": rows_sha256(selected_close),
                          "selected_adj_close_sha256": rows_sha256(selected_adj)},
    }


# --- database reads (read-only; never on or after HOLDOUT_START; bounded by the snapshot) ------------

_SOURCE_SQL = "SELECT id, name FROM source_catalog WHERE LOWER(name) = LOWER(:name)"
_ROWS_SQL = (
    "SELECT obs_date, value, pull_timestamp FROM raw_series "
    "WHERE series_id = :sid AND source_id = :src AND pull_status = 'SUCCESS' "
    "AND obs_date >= :lo AND obs_date <= :hi AND pull_timestamp <= :ts ORDER BY obs_date, pull_timestamp"
)
_STATUS_SQL = (
    "SELECT pull_status, COUNT(*) FROM raw_series WHERE series_id = :sid AND source_id = :src "
    "AND obs_date >= :lo AND obs_date <= :hi AND pull_timestamp <= :ts GROUP BY pull_status"
)
# Rows of every other source under the series id in the window, by status (counted, never read as prices).
_OTHER_ROWS_SQL = (
    "SELECT sc.name, r.pull_status, COUNT(*) FROM raw_series r JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.source_id <> :src AND r.obs_date >= :lo AND r.obs_date <= :hi "
    "AND r.pull_timestamp <= :ts GROUP BY sc.name, r.pull_status"
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
    check_source(name)
    found = conn.execute(text(_SOURCE_SQL), {"name": name}).fetchall()
    if len(found) != 1:
        raise ProbeRefused(f"source_catalog has {len(found)} rows named {name!r}; need exactly one")
    return int(found[0][0]), str(found[0][1])


def read_rows(conn, sid: str, source_id: int, lo: date, hi: date, as_of_ts: datetime) -> list[Row]:
    """Every SUCCESS vintage of one source (basis check 1)."""
    check_window(lo, hi)
    rows = conn.execute(text(_ROWS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi, "ts": as_of_ts}).fetchall()
    return [Row(_d(r[0]), float(r[1]), _ts(r[2])) for r in rows if r[1] is not None]


def read_selected(conn, sid: str, lo: date, hi: date, as_of_ts: datetime) -> list[Row]:
    """The frozen read's rows: ``store.observations.read_window`` with ``source="TIINGO"`` and ``as_of_ts``."""
    from store import observations

    check_window(lo, hi)
    obs = observations.read_window(conn, sid, source=PRICE_SOURCE, start=lo, as_of=hi, as_of_ts=as_of_ts)
    return [Row(o.obs_date, float(o.value), o.pull_timestamp) for o in obs]


def read_statuses(conn, sid: str, source_id: int, lo: date, hi: date, as_of_ts: datetime) -> dict[str, int]:
    check_window(lo, hi)
    return {str(s): int(n) for s, n in conn.execute(
        text(_STATUS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi, "ts": as_of_ts}).fetchall()}


def read_other_source_rows(conn, sid: str, source_id: int, lo: date, hi: date,
                           as_of_ts: datetime) -> dict[str, dict[str, int]]:
    """{source name: {pull_status: rows}} of every other source under the series id in the window."""
    check_window(lo, hi)
    out: dict[str, dict[str, int]] = {}
    for name, status, n in conn.execute(text(_OTHER_ROWS_SQL), {"sid": sid, "src": source_id, "lo": lo, "hi": hi,
                                                                "ts": as_of_ts}).fetchall():
        out.setdefault(str(name), {})[str(status)] = int(n)
    return out


def read_td_splits(conn, ticker: str, lo: date, hi: date, as_of_ts: datetime) -> list[tuple[date, float]]:
    """``TWELVEDATA_SPLITS:{ticker}:ratio`` SUCCESS rows in the window (split ratios, not prices)."""
    check_window(lo, hi)
    found = conn.execute(text(_SOURCE_SQL), {"name": TD_SPLITS_SOURCE}).fetchall()
    if len(found) != 1:
        return []
    rows = read_rows(conn, f"{TD_SPLITS_SOURCE}:{ticker}:ratio", int(found[0][0]), lo, hi, as_of_ts)
    by_date, _ = collapse_vintages(rows)
    return sorted(by_date.items())


# --- inputs outside the database ---------------------------------------------------------------------


@dataclass
class VendorFiles:
    """The TwelveData and Tiingo-metadata files written by ``price_admission_fetch`` (receipts verified once)."""

    twelvedata_dir: Path
    tiingo_meta_dir: Path
    _td_done: dict | None = field(default=None, repr=False)
    _meta_done: dict | None = field(default=None, repr=False)

    def td(self, ticker: str) -> dict:
        if self._td_done is None:
            self._td_done = fetch.FetchLog(Path(self.twelvedata_dir) / "fetch_log.jsonl").final()
        return fetch.load_td_closes(self.twelvedata_dir, ticker, self._td_done)

    def meta(self, ticker: str) -> dict | None:
        if self._meta_done is None:
            self._meta_done = fetch.FetchLog(Path(self.tiingo_meta_dir) / "fetch_log.jsonl").final()
        return fetch.load_tiingo_meta(self.tiingo_meta_dir, ticker, self._meta_done)


def sec_names_by_ticker(sic_map: Path, issuers: Sequence[Mapping[str, Any]], *, pinned: bool = True) -> dict[str, str]:
    """The issuer's SEC name (the pinned SIC map's ``name``) per price ticker (v6 §2.3 entity check)."""
    frame = v2.load_sic_map(Path(sic_map)) if pinned else v2.load_sic_map(Path(sic_map), pinned=None)
    names = {int(c): str(n) for c, n in zip(frame["cik"], frame["name"]) if n is not None and str(n).strip()}
    return {str(m["ticker"]): names[int(m["cik"])] for m in issuers
            if m.get("cik") is not None and int(m["cik"]) in names}


@dataclass
class C1Interval:
    """The §2.2 rule 3 ticker interval of each issuer, evaluated with ``v6.interval_close_mask``."""

    admission: v2.Admission
    universe: pd.DataFrame

    @classmethod
    def from_submissions(cls, submissions: Path, issuers: Sequence[Mapping[str, Any]]) -> "C1Interval":
        members = [m for m in issuers if m.get("cik") is not None]
        universe = pd.DataFrame({
            "ticker": [str(m["ticker"]) for m in members],
            "cik": [int(m["cik"]) for m in members],
            "current_tickers": [sorted({v2.canonical_symbol(t) for t in (m.get("current_tickers") or [m["ticker"]])})
                                for m in members],
        })
        subs = v2.read_submissions(Path(submissions), sorted(set(universe["cik"])))
        return cls(v2.build_admission(subs, universe), universe)

    def mask(self, calendar: Sequence[date]) -> pd.DataFrame:
        """sessions x tickers: True where the session's 16:00 New York close lies inside the interval."""
        index = pd.DatetimeIndex([pd.Timestamp(d) for d in calendar])
        closes = pd.DataFrame(index=index, columns=list(self.universe["ticker"]), dtype=float)
        return v6.interval_close_mask(self.admission)(closes, self.universe)

    @property
    def receipt_sha256(self) -> str:
        return self.admission.receipt_sha256


# --- the run -----------------------------------------------------------------------------------------


def probe_ticker(conn, ticker: str, source_id: int, lo: date, hi: date, as_of_ts: datetime, *,
                 calendar: Sequence[date], vendors: VendorFiles, sec_name: str | None, is_benchmark: bool,
                 inside_interval: Sequence[bool] | None) -> dict:
    check_window(lo, hi)
    close_sid, adj_sid = series_id(ticker, CLOSE_FIELD), series_id(ticker, ADJ_FIELD)
    others: dict[str, dict[str, int]] = {}
    for sid in (close_sid, adj_sid):
        for name, by_status in read_other_source_rows(conn, sid, source_id, lo, hi, as_of_ts).items():
            for status, n in by_status.items():
                others.setdefault(name, {})[status] = others.get(name, {}).get(status, 0) + n
    return assess_ticker(
        ticker,
        vintages_close=read_rows(conn, close_sid, source_id, lo, hi, as_of_ts),
        vintages_adj=read_rows(conn, adj_sid, source_id, lo, hi, as_of_ts),
        selected_close=read_selected(conn, close_sid, lo, hi, as_of_ts),
        selected_adj=read_selected(conn, adj_sid, lo, hi, as_of_ts),
        statuses={close_sid: read_statuses(conn, close_sid, source_id, lo, hi, as_of_ts),
                  adj_sid: read_statuses(conn, adj_sid, source_id, lo, hi, as_of_ts)},
        other_source_rows=others, calendar=calendar, td=vendors.td(ticker), meta=vendors.meta(ticker),
        sec_name=sec_name, is_benchmark=is_benchmark, td_splits=read_td_splits(conn, ticker, lo, hi, as_of_ts),
        inside_interval=inside_interval)


def run_probe(conn, tickers: Sequence[str], *, benchmark: str, source: str, lo: date, hi: date,
              as_of_ts: datetime, vendors: VendorFiles, sec_names: Mapping[str, str] | None = None,
              interval: C1Interval | None = None, progress=None) -> dict:
    """The benchmark first (its selected adj_close dates are the session calendar), then every ticker."""
    check_window(lo, hi)
    if benchmark != BENCHMARK:
        raise ProbeRefused(f"VS1 v6 declares benchmark {BENCHMARK}")
    source_id, source_name = resolve_source(conn, source)
    calendar = sorted(r.obs_date for r in read_selected(conn, series_id(benchmark, ADJ_FIELD), lo, hi, as_of_ts))
    wanted = sorted(set(tickers) - {benchmark})
    mask = interval.mask(calendar) if interval is not None and calendar else None
    names = dict(sec_names or {})
    records = {benchmark: probe_ticker(conn, benchmark, source_id, lo, hi, as_of_ts, calendar=calendar,
                                       vendors=vendors, sec_name=None, is_benchmark=True, inside_interval=None)}
    for i, t in enumerate(wanted):
        inside = list(mask[t].to_numpy(dtype=bool)) if mask is not None and t in mask.columns else None
        try:
            records[t] = probe_ticker(conn, t, source_id, lo, hi, as_of_ts, calendar=calendar, vendors=vendors,
                                      sec_name=names.get(t), is_benchmark=False, inside_interval=inside)
        except ProbeRefused:
            raise
        except Exception as exc:  # a per-ticker failure refuses that ticker, visibly
            records[t] = {"ticker": t, "benchmark": False, "admitted": False, "reasons": ["probe_error"],
                          "listed_from": None, "error": type(exc).__name__}
        if progress:
            progress(i + 1, len(wanted), t)
    return {"source": {"name": source_name, "id": source_id}, "calendar_sessions": len(calendar),
            "calendar": [calendar[0].isoformat(), calendar[-1].isoformat()] if calendar else None,
            "interval_receipt_sha256": interval.receipt_sha256 if interval is not None else None,
            "records": records}


# --- reports and manifest ----------------------------------------------------------------------------


def _count(items: Iterable[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for k in items:
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def _common(probe: Mapping, *, lo: date, hi: date, code_sha: str, snapshot_as_of_ts: datetime) -> dict:
    return {"prereg": {"study": STUDY, "body_sha256": PREREG_BODY_SHA256, "rules": "v6 §2.2 rules 3-4, §2.3"},
            "code_sha": code_sha, "snapshot_as_of_ts": snapshot_as_of_ts.astimezone(timezone.utc).isoformat(),
            "window": "discovery", "read_window": {"start": lo.isoformat(), "end": hi.isoformat()},
            "source": dict(probe["source"]), "promotion_allowed": False}


def build_crosscheck_report(probe: Mapping, *, lo: date, hi: date, code_sha: str, snapshot_as_of_ts: datetime,
                            twelvedata_fetch_log_sha256: str | None) -> dict:
    recs = probe["records"]
    tickers = {}
    for t, r in sorted(recs.items()):
        if "crosscheck" not in r:
            continue
        tickers[t] = {**r["crosscheck"], "twelvedata": r["twelvedata"]}
    # TIINGO has the window's last date but the merged TwelveData series does not
    td_end_missing = sorted(t for t, r in tickers.items()
                            if (recs[t].get("coverage") or {}).get("last_date") == hi.isoformat()
                            and r["twelvedata"]["state"] == "ok" and not r["twelvedata"].get("has_window_end"))
    holdout_rows = sum(int(r["twelvedata"].get("holdout_rows", 0)) for r in tickers.values())
    return {
        "report": "vs1-v6-twelvedata-crosscheck",
        **_common(probe, lo=lo, hi=hi, code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts),
        "rule": dataclasses.asdict(CROSSCHECK),
        "request": {"url": fetch.TD_URL, "params": {**fetch.td_params("{ticker}", "all"), "adjust": ["all", "none"]},
                    "supplement_params": {**fetch.td_params("{ticker}", "all", start=fetch.TD_SUPPLEMENT_START),
                                          "adjust": ["all", "none"]},
                    "fetch_log_sha256": twelvedata_fetch_log_sha256},
        "window_implementation": {
            "note": fetch.TD_WINDOW_NOTE,
            "registered_window": [fetch.TD_START, fetch.TD_WINDOW_END],
            "end_date_exclusive_bound": fetch.TD_END_EXCLUSIVE,
            "assertion": "no TwelveData row dated on or after 2020-01-01",
            "rows_dated_2020_or_later": holdout_rows,
            "assertion_passed": holdout_rows == 0,
            "tickers_supplemented": sorted(t for t, r in tickers.items() if r["twelvedata"].get("supplemented")),
        },
        "what": "agreement statistics only: common dates, consecutive-session pairs, within-Y share, excluded "
                "adjustment pairs, dropped pairs, first/last; no event, label or forward return",
        "summary": {
            "tickers": len(tickers),
            "passed": sum(1 for r in tickers.values() if r["passed"]),
            "by_reason": _count(r["reason"] for r in tickers.values()),
            "twelvedata_state": _count(r["twelvedata"]["state"] for r in tickers.values()),
            "twelvedata_unavailable": sorted(t for t, r in tickers.items() if r["twelvedata"]["state"] == "unavailable"),
            "twelvedata_not_fetched": sorted(t for t, r in tickers.items() if r["twelvedata"]["state"] == "not_fetched"),
            "below_n_pairs": sorted(t for t, r in tickers.items() if r["pairs"] < CROSSCHECK.min_pairs),
            "excluded_adjustment_pairs_total": sum(r["excluded_adjustment_pairs"] for r in tickers.values()),
            "dropped_nonconsecutive_total": sum(r["dropped_nonconsecutive"] for r in tickers.values()),
            "twelvedata_missing_window_end": td_end_missing,
            "twelvedata_holdout_rows_tickers": sorted(t for t, r in tickers.items()
                                                      if r["twelvedata"]["state"] == "holdout_rows"),
        },
        "tickers": tickers,
    }


def build_tiingo_meta_report(probe: Mapping, *, lo: date, hi: date, code_sha: str, snapshot_as_of_ts: datetime,
                             tiingo_meta_fetch_log_sha256: str | None) -> dict:
    tickers = {}
    for t, r in sorted(probe["records"].items()):
        if "tiingo_meta" not in r:
            continue
        m = r["tiingo_meta"]["meta"] or {}
        tickers[t] = {"startDate": m.get("startDate"), "endDate": m.get("endDate"), "name": m.get("name"),
                      "exchangeCode": m.get("exchangeCode"), "receipt": r["tiingo_meta"]["receipt"],
                      "sec_name": r["entity"]["sec_name"], "entity_check": r["entity"]["check"],
                      "listed_from": r["listed_from"]}
    return {
        "report": "vs1-v6-tiingo-metadata",
        **_common(probe, lo=lo, hi=hi, code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts),
        "request": {"url": fetch.TIINGO_META_URL, "fetch_log_sha256": tiingo_meta_fetch_log_sha256},
        "name_match": {"jaccard_min": v6.NAME_JACCARD_MIN, "stop_tokens": sorted(v6.NAME_STOP_TOKENS),
                       "or": "one joined token string prefixes the other"},
        "summary": {"tickers": len(tickers), "entity_check": _count(r["entity_check"] for r in tickers.values()),
                    "no_meta": sorted(t for t, r in tickers.items() if r["entity_check"] == "no_meta"),
                    "entity_mismatch": sorted(t for t, r in tickers.items() if r["entity_check"] == "entity_mismatch")},
        "tickers": tickers,
    }


def _summary(records: Mapping[str, Mapping], benchmark: str) -> dict:
    admitted = sorted(t for t, r in records.items() if r["admitted"])
    issuers = {t: r for t, r in records.items() if t != benchmark}
    low = {t for t, r in issuers.items() if (r.get("low_price") or {}).get("sessions_close_below_1usd")}
    others = [r for r in issuers.values() if (r.get("source_filtering") or {}).get("other_source_rows_total")]
    by_source: dict[str, int] = {}
    for r in records.values():
        for name, by_status in ((r.get("source_filtering") or {}).get("other_source_rows") or {}).items():
            by_source[name] = by_source.get(name, 0) + sum(by_status.values())
    c1 = [r["c1"] for r in issuers.values() if r.get("c1")]
    return {
        "candidates": len(records),
        "admitted": len(admitted),
        "admitted_excluding_benchmark": len([t for t in admitted if t != benchmark]),
        "not_admitted_by_reason": _count(x for r in records.values() if not r["admitted"] for x in r["reasons"]),
        "source_filtering": {"tickers_with_other_source_rows": len(others),
                             "other_source_rows_by_source": dict(sorted(by_source.items()))},
        "splice": {"tickers_with_boundaries": sum(1 for r in records.values() if (r.get("splice") or {}).get("boundaries")),
                   "boundaries_total": sum((r.get("splice") or {}).get("boundaries", 0) for r in records.values()),
                   "tickers_failed": sorted(t for t, r in records.items() if "splice_failed" in r["reasons"])},
        "low_price": {"threshold_usd": LOW_PRICE_USD, "tickers_with_close_below_threshold": len(low),
                      "excluded_by_splice": sorted(t for t in low if "splice_failed" in issuers[t]["reasons"]),
                      "excluded_by_crosscheck": sorted(t for t in low if any(x.startswith("crosscheck_")
                                                                             for x in issuers[t]["reasons"])),
                      "tickers": sorted(low)},
        "c1_interval": None if not c1 else {
            "tickers": len(c1),
            "sessions_outside_interval_total": sum(x["outside_interval"] for x in c1),
            "closes_blanked_total": sum(x["closes_blanked"] for x in c1),
            "tickers_with_closes_blanked": sum(1 for x in c1 if x["closes_blanked"]),
            "tickers_never_inside": sorted(t for t, r in issuers.items() if r.get("c1") and not r["c1"]["inside_interval"])},
        "promotion_allowed": False,
    }


def build_report(probe: Mapping, *, tickers: Sequence[str], benchmark: str, lo: date, hi: date, code_sha: str,
                 snapshot_as_of_ts: datetime, inputs: Mapping[str, Any], crosscheck_report_sha256: str,
                 tiingo_meta_report_sha256: str) -> dict:
    records = probe["records"]
    admitted = sorted(t for t, r in records.items() if r["admitted"])
    listed_from = {t: records[t]["listed_from"] for t in admitted if t != benchmark}
    slim = {t: {k: v for k, v in r.items() if k not in ("crosscheck", "tiingo_meta")}
            | ({"crosscheck": {k: r["crosscheck"][k] for k in ("passed", "reason", "pairs", "share_within")}}
               if "crosscheck" in r else {})
            for t, r in records.items()}
    return {
        "probe": PROBE_NAME,
        "probe_version": PROBE_VERSION,
        **_common(probe, lo=lo, hi=hi, code_sha=code_sha, snapshot_as_of_ts=snapshot_as_of_ts),
        "read_window": {"start": lo.isoformat(), "end": hi.isoformat(),
                        "why": "discovery 2012-01-01..2019-12-31 plus the harness's 60-day warm-up; nothing on or "
                               "after 2020-01-01 is read"},
        "refused_sources": sorted(REFUSED_SOURCES) + [f"{p}*" for p in REFUSED_PREFIXES] + ["every source but TIINGO"],
        "series_template": SERIES_TEMPLATE,
        "basis": BASIS,
        "benchmark": benchmark,
        "benchmark_admitted": bool(records.get(benchmark, {}).get("admitted")),
        "benchmark_calendar_sessions": probe["calendar_sessions"],
        "benchmark_calendar": probe.get("calendar"),
        "tolerances": {"same_value_rtol": SAME_VALUE_RTOL, "flat_rtol": FLAT_RTOL, "split_rtol": SPLIT_RTOL,
                       "max_split_term": MAX_SPLIT_TERM, "max_whole_split": MAX_WHOLE_SPLIT,
                       "min_split_move": MIN_SPLIT_MOVE, "max_distribution_drop": MAX_DISTRIBUTION_DROP,
                       "td_match_days": TD_MATCH_DAYS, "splice_tol": SPLICE_TOL,
                       "crosscheck": dataclasses.asdict(CROSSCHECK), "low_price_usd": LOW_PRICE_USD},
        "interpretation": list(INTERPRETATION),
        "crosscheck_report_sha256": crosscheck_report_sha256,
        "tiingo_meta_report_sha256": tiingo_meta_report_sha256,
        "c1_admission_receipt_sha256": probe.get("interval_receipt_sha256"),
        "inputs": dict(inputs),
        "summary": {**_summary(records, benchmark), "candidates_requested": len(set(tickers) | {benchmark})},
        "admitted": admitted,
        "listed_from": listed_from,
        "tickers": {t: slim[t] for t in sorted(slim)},
    }


def build_manifest(report: Mapping, probe_report_sha256: str) -> dict:
    """The v6 harness's admitted-price manifest, validated by ``v6.PriceManifest``."""
    if not report["benchmark_admitted"]:
        raise ProbeRefused(f"benchmark {report['benchmark']} failed the probe: no manifest can be built")
    check_source(report["source"]["name"])
    listed = dict(sorted(report["listed_from"].items()))
    manifest = v6.PriceManifest(
        source=report["source"]["name"], series_template=report["series_template"], basis=report["basis"],
        benchmark=report["benchmark"], admitted=tuple(sorted(report["admitted"])),
        probe_report_sha256=probe_report_sha256, listed_from=tuple(listed.items()),
        crosscheck_report_sha256=report["crosscheck_report_sha256"],
        tiingo_meta_report_sha256=report["tiingo_meta_report_sha256"])
    manifest.validate()
    doc = dataclasses.asdict(manifest)
    doc["admitted"] = list(manifest.admitted)
    doc["listed_from"] = listed
    return doc


def file_sha256(path: Path) -> str:
    """Exact-bytes sha256 (the harness's ``data_sha256``)."""
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


def write_outputs(out: Path, probe: Mapping, *, tickers: Sequence[str], benchmark: str, lo: date, hi: date,
                  code_sha: str, snapshot_as_of_ts: datetime, inputs: Mapping[str, Any]) -> dict:
    """The three reports, the manifest (when the benchmark passes) and ``sha256s.txt``, each written once."""
    common = {"lo": lo, "hi": hi, "code_sha": code_sha, "snapshot_as_of_ts": snapshot_as_of_ts}
    hashes = {
        "crosscheck_report.json": write_json_once(out / "crosscheck_report.json", build_crosscheck_report(
            probe, twelvedata_fetch_log_sha256=inputs.get("twelvedata_fetch_log_sha256"), **common)),
        "tiingo_meta_report.json": write_json_once(out / "tiingo_meta_report.json", build_tiingo_meta_report(
            probe, tiingo_meta_fetch_log_sha256=inputs.get("tiingo_meta_fetch_log_sha256"), **common)),
    }
    report = build_report(probe, tickers=tickers, benchmark=benchmark, inputs=inputs,
                          crosscheck_report_sha256=hashes["crosscheck_report.json"],
                          tiingo_meta_report_sha256=hashes["tiingo_meta_report.json"], **common)
    hashes["probe_report.json"] = write_json_once(out / "probe_report.json", report)
    if report["benchmark_admitted"]:
        hashes["price_manifest.json"] = write_json_once(out / "price_manifest.json",
                                                        build_manifest(report, hashes["probe_report.json"]))
        v6.PriceManifest.from_file(out / "price_manifest.json")  # the harness accepts what was written
    for name, h in hashes.items():
        assert file_sha256(out / name) == h
    with open(out / "sha256s.txt", "x", encoding="utf-8", newline="\n") as stream:
        stream.writelines(f"{h}  {name}\n" for name, h in sorted(hashes.items()))
    return {"out": str(out), "summary": report["summary"], "benchmark_admitted": report["benchmark_admitted"],
            "sha256": hashes}


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
