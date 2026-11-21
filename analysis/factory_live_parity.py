"""Research-versus-live parity for module-factory features (3pacs/GRID#1160).

A factory candidate may take a live slot only if live can compute what research
computed (design Q13). ``FEATURE_SPECS`` gives every price feature the tag
``live_source: gex-signals bars_1m_live``. A tag is a claim. This module tests
it: it runs the factory's own functions (``module_factory.Session`` and
``raw_features``) over the research bars and over the live recording, and
reports whether the two agree.

Two questions, kept apart because they fail differently:

* **Availability.** Would this session have produced a signal and a label at
  all? Research needs the prior session complete, the bars from 09:30 to T-1
  complete, the entry bar and the exit bar (``module_factory.build_rows``).
  Values that match on the days both sides have are not parity if live abstains
  on most days.
* **Values.** Where both sides can compute a feature at a decision time, do the
  numbers agree within a tolerance for that feature's type?

The two sides do not use the same availability rule (version 2). Research bars
have no gaps, so research keeps the rule above. The live recording loses a
minute when its quote source is refused, and the live recorder (gex-signals
``factory_slots.py``, ``panel-factory/2``) records a feature with up to
``GAP_MAX`` minutes missing from the prior session and from the minutes before
the decision, when the bars that feature reads are there:

* every feature reads the bar closing at the decision time and the prior
  session's last bar; without either, or with more minutes missing, the session
  is withheld as in research;
* ``F1`` and ``F1_shift30`` read nothing else (``F1_shift30`` also the bar 30
  minutes earlier): their live value is the value complete bars would give;
* ``F10`` and ``F11`` read a range. They are eligible only when every lost
  minute of their window has a backstop bar (``factory_backstop_bars``: a second
  source's minute, kept as first seen) first seen at or before the decision
  time. Only that bar's high and low are used.

This module applies that rule to the live side, so availability is judged per
feature, and the live value it compares is the value the recorder would have
recorded. The constants and reason strings below are the recorder's; a change
there must be made here too. A signals database without the backstop table
(recorded before the rule shipped) simply has no fills.

A feature is ``live_ready`` only when each of the most recent ``MIN_SESSIONS``
overlapping sessions is a matched one: at every decision time both sides were
eligible and the value was in tolerance. So in those sessions no value is out
of tolerance and the two sides never disagree about that feature's
availability. Anything else is ``research_only`` with its reasons and the
counts behind them.

The verdict is taken on the most recent sessions, not on every session in the
window (version 3). In version 2 one bad session anywhere in the window kept a
feature ``research_only`` for as long as the window included it, and a window
that began before the recorder stored the last minute could never pass. Now a
bad session costs the next ``MIN_SESSIONS`` sessions and no more. The number is
fixed here, not an argument: a verdict must not depend on a window chosen after
the fact. The whole-window counts are still reported beside the verdict, so an
older failure stays visible.

Read-only: both databases are opened ``mode=ro``. Nothing here scores a module
or registers anything. Five matching sessions are evidence about the source
and the implementation, not about any edge.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

from analysis.module_factory import (
    DEV_FIRST,
    FEATURE_SPECS,
    FEATURES,
    NY,
    Session,
    _hm,
    _minutes,
    raw_features,
)
from ingestion.market_calendar import is_market_open, market_close_time

SCHEMA = "factory-live-parity-v3"
MIN_SESSIONS = 5                  # the verdict: each of the most recent MIN_SESSIONS overlapping sessions matched
OPEN_MIN = 9 * 60 + 30
FULL_SESSION_MIN = 390

# Tolerance per feature type, on the raw (un-standardized) value.
#   return: 1 bp. gex-signals' own live-bars/1 gate allows a $0.05 median close
#           difference between sources, about 0.7 bp at SPY 730.
#   range:  0.03 of the range, i.e. three points on a 0-100 position scale.
FEATURE_TYPE = {"F1": "return", "F1_shift30": "return", "F10": "range", "F11": "range"}
TOLERANCE = {"return": 1e-4, "range": 0.03}
LABEL_TOLERANCE_DOLLARS = 0.05

PRIOR_MISSING = "prior_session_missing"
PRIOR_SHORT = "prior_session_shortened"
PRIOR_INCOMPLETE = "prior_session_incomplete"
PREFIX_INCOMPLETE = "bars_before_decision_incomplete"
AFTER_CLOSE = "decision_at_or_after_close"
ENTRY_MISSING = "entry_bar_missing"
EXIT_MISSING = "exit_bar_missing"
EXIT_AFTER_CLOSE = "horizon_ends_after_close"

# The live recorder's lost-minute rule (gex-signals factory_slots.py). Live only: research bars have no gaps.
GAP_MAX = 2                       # minutes that may be missing from the prior session, and before the decision
PRIOR_LAST_BAR = "15:59"          # read by every feature (the prior close): never one of the tolerated gaps
BACKSTOP_SOURCE = "yahoo-1m"
GAP_UNFILLED = "gap_not_backstopped"
READ_BAR_MISSING = "feature_bar_missing"
FEATURE_READS = {"F1": None, "F1_shift30": "bar_30_minutes_before", "F10": "todays_range", "F11": "prior_range"}

Bars = dict[str, dict[str, tuple]]
Backstop = dict[str, dict[str, tuple]]   # {day: {hm: ((o, h, l, c, v), first seen, aware datetime)}}


class ParityError(ValueError):
    pass


# ── reading ─────────────────────────────────────────────────────────────────


def _connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)


def load_research(db: Path, symbol: str, first: str, last: str) -> Bars:
    """{day: {hm: (o, h, l, c, v)}} from the staged research bars. Refuses pre-dev days."""
    if first < DEV_FIRST:
        raise ParityError(f"{first} is before dev ({DEV_FIRST}): holdout refused")
    con = _connect(db)
    try:
        rows = con.execute(
            "select day, hm, o, h, l, c, v from bars where sym = ? and day between ? and ? order by day, hm",
            (symbol, first, last),
        ).fetchall()
    finally:
        con.close()
    out: Bars = {}
    for day, hm, o, h, low, c, v in rows:
        out.setdefault(day, {})[hm] = (o, h, low, c, v)
    return out


def load_live(db: Path, symbol: str, first: str, last: str) -> tuple[Bars, dict[str, dict[str, int]]]:
    """Live bars in the research shape, and how many bars each source supplied per day.

    ``bars_1m_live.ts_utc`` is the minute's start in UTC; research keys bars by
    New York ``HH:MM`` of the minute's start. Only regular-session minutes are kept.
    """
    con = _connect(db)
    try:
        rows = con.execute(
            "select day, ts_utc, src, o, h, l, c, v from bars_1m_live "
            "where symbol = ? and day between ? and ? order by ts_utc",
            (symbol, first, last),
        ).fetchall()
    finally:
        con.close()
    bars: Bars = {}
    sources: dict[str, Counter] = {}
    for day, ts, src, o, h, low, c, v in rows:
        local = datetime.fromisoformat(ts).astimezone(NY)
        if local.date().isoformat() != day:
            continue
        hm = local.strftime("%H:%M")
        if hm not in expected_minutes(day):
            continue
        bars.setdefault(day, {})[hm] = (o, h, low, c, v)
        sources.setdefault(day, Counter())[src] += 1
    return bars, {day: dict(sorted(count.items())) for day, count in sources.items()}


def load_backstop(db: Path, symbol: str, first: str, last: str) -> Backstop | None:
    """The live recorder's first-seen second-source bars; None when the database has no such table."""
    con = _connect(db)
    try:
        if not con.execute("select 1 from sqlite_master where type = 'table' and name = 'factory_backstop_bars'").fetchone():
            return None
        rows = con.execute(
            "select day, ts_utc, o, h, l, c, v, first_seen_at from factory_backstop_bars "
            "where symbol = ? and source = ? and day between ? and ? order by ts_utc",
            (symbol, BACKSTOP_SOURCE, first, last),
        ).fetchall()
    finally:
        con.close()
    out: Backstop = {}
    for day, ts, o, h, low, c, v, seen in rows:
        local = datetime.fromisoformat(ts).astimezone(NY)
        hm = local.strftime("%H:%M")
        if local.date().isoformat() == day and hm in expected_minutes(day):
            out.setdefault(day, {})[hm] = ((o, h, low, c, v), datetime.fromisoformat(seen))
    return out


# ── the session calendar ────────────────────────────────────────────────────


def close_minute(day: str) -> int | None:
    """Minutes after midnight (New York) of the day's regular close; None on a closed day."""
    d = date.fromisoformat(day)
    if not is_market_open(d):
        return None
    close = market_close_time(d)
    return close.hour * 60 + close.minute


def expected_minutes(day: str) -> frozenset[str]:
    close = close_minute(day)
    return frozenset() if close is None else frozenset(_hm(i) for i in range(OPEN_MIN, close))


def previous_session(day: str) -> str:
    d = date.fromisoformat(day) - timedelta(days=1)
    while not is_market_open(d):
        d -= timedelta(days=1)
    return d.isoformat()


# ── availability under the research rules ───────────────────────────────────


def signal_reasons(bars: Bars, day: str, decision: str) -> list[str]:
    """Why research would withhold this session's signal at ``decision``; [] when eligible.

    Mirrors ``module_factory.build_rows``: prior session present, full length
    and complete; every bar from 09:30 through the one closing at T present.
    """
    reasons = []
    close = close_minute(day)
    if close is None or _minutes(decision) >= close:
        return [AFTER_CLOSE]
    prior_day = previous_session(day)
    prior = bars.get(prior_day)
    if prior is None:
        reasons.append(PRIOR_MISSING)
    elif (close_minute(prior_day) or 0) - OPEN_MIN != FULL_SESSION_MIN:
        reasons.append(PRIOR_SHORT)
    elif set(prior) != expected_minutes(prior_day):
        reasons.append(PRIOR_INCOMPLETE)
    required = {_hm(i) for i in range(OPEN_MIN, _minutes(decision))}
    if not required.issubset(bars.get(day, {})):
        reasons.append(PREFIX_INCOMPLETE)
    return reasons


# ── availability and values under the live recorder's rule ──────────────────


def live_gaps(bars: Bars, day: str, decision: str) -> tuple[list[str], list[str]]:
    """(minutes before ``decision`` missing from the day's live bars, minutes missing from the prior session's)."""
    prior_day = previous_session(day)
    required = {_hm(i) for i in range(OPEN_MIN, _minutes(decision))}
    return (sorted(required - set(bars.get(day, {}))),
            sorted(expected_minutes(prior_day) - set(bars.get(prior_day) or {})))


def live_signal_reasons(bars: Bars, day: str, decision: str) -> list[str]:
    """Why the live recorder withholds the whole session at ``decision``, for every feature; [] when it does not."""
    close = close_minute(day)
    if close is None or _minutes(decision) >= close:
        return [AFTER_CLOSE]
    reasons = []
    prior_day = previous_session(day)
    missing_today, missing_prior = live_gaps(bars, day, decision)
    if bars.get(prior_day) is None:
        reasons.append(PRIOR_MISSING)
    elif (close_minute(prior_day) or 0) - OPEN_MIN != FULL_SESSION_MIN:
        reasons.append(PRIOR_SHORT)
    elif len(missing_prior) > GAP_MAX or PRIOR_LAST_BAR in missing_prior:
        reasons.append(PRIOR_INCOMPLETE)
    if len(missing_today) > GAP_MAX or _hm(_minutes(decision) - 1) in missing_today:
        reasons.append(PREFIX_INCOMPLETE)
    return reasons


def live_features(bars: Bars, backstop: Backstop | None, day: str, decision: str) -> tuple[dict, dict, dict]:
    """(values, reasons, used) per feature: what the live recorder would have recorded at ``decision``.

    ``reasons[f]`` is [] when the feature is eligible. ``used[f]`` lists the backstop minutes its value took, as
    ("today" | "prior", "HH:MM"). A backstop bar counts only when first seen at or before the decision time, only
    for a lost minute, and only in the two range features.
    """
    session = live_signal_reasons(bars, day, decision)
    nothing = {f: [] for f in FEATURES}
    if session:
        return dict.fromkeys(FEATURES), {f: list(session) for f in FEATURES}, nothing
    prior_day = previous_session(day)
    today, prior = bars[day], bars[prior_day]
    bar = _hm(_minutes(decision) - 1)
    exact = raw_features(Session(today), Session(prior), bar)
    missing_today, missing_prior = live_gaps(bars, day, decision)
    if not missing_today and not missing_prior:
        return exact, nothing, nothing
    known_by = datetime.fromisoformat(f"{day}T{decision}").replace(tzinfo=NY)

    def fills(of_day: str, missing: list[str]) -> dict[str, tuple]:
        held = (backstop or {}).get(of_day, {})
        return {m: held[m][0] for m in missing if m in held and held[m][1] <= known_by}

    use_today, use_prior = fills(day, missing_today), fills(prior_day, missing_prior)
    filled = raw_features(Session({**today, **use_today}), Session({**prior, **use_prior}), bar)
    values, reasons, used = {}, {}, {}
    for f in FEATURES:
        reads, why, took = FEATURE_READS[f], [], []
        if reads == "bar_30_minutes_before" and _hm(_minutes(decision) - 31) in missing_today:
            why = [READ_BAR_MISSING]
        elif reads == "todays_range":
            why = [GAP_UNFILLED] if len(use_today) < len(missing_today) else []
            took = [("today", m) for m in sorted(use_today)]
        elif reads == "prior_range":
            why = [GAP_UNFILLED] if len(use_prior) < len(missing_prior) else []
            took = [("prior", m) for m in sorted(use_prior)]
        values[f] = None if why else (filled[f] if took else exact[f])
        reasons[f], used[f] = why, ([] if why else took)
    return values, reasons, used


def label_bars(decision: str, horizon: str) -> tuple[str, str, int]:
    """(entry bar, exit bar, minute the label ends) for a decision time and horizon."""
    entry = _hm(_minutes(decision) + 1)
    if horizon == "close":
        return entry, "15:59", 16 * 60
    span = int(horizon[:-1])
    return entry, _hm(_minutes(decision) - 1 + span), _minutes(decision) + span


def label_reasons(bars: Bars, day: str, decision: str, horizon: str) -> list[str]:
    """Why this session has no label; [] when the entry and exit prices exist."""
    entry, exit_bar, ends = label_bars(decision, horizon)
    close = close_minute(day)
    if close is None or ends > close:
        return [EXIT_AFTER_CLOSE]
    today = bars.get(day, {})
    reasons = []
    if entry not in today or not today[entry][0]:
        reasons.append(ENTRY_MISSING)
    if exit_bar not in today or not today[exit_bar][3]:
        reasons.append(EXIT_MISSING)
    return reasons


def availability(bars: Bars, days: list[str], decisions: list[str], horizons: list[str],
                 backstop: Backstop | None = None) -> dict:
    """The live recording's availability under the live recorder's rule, with a denominator for every count.

    ``signal_*`` counts sessions the recorder does not withhold as a whole; ``feature_*`` counts each feature, which
    may still be withheld for a lost minute it reads.
    """
    rows = []
    why_signal: Counter = Counter()
    why_label: Counter = Counter()
    why_feature = {f: Counter() for f in FEATURES}
    n_feature = dict.fromkeys(FEATURES, 0)
    backstopped = dict.fromkeys(FEATURES, 0)
    n_signal = n_label = n_both = 0
    for day in days:
        for decision in decisions:
            s = live_signal_reasons(bars, day, decision)
            _, per_feature, used = live_features(bars, backstop, day, decision)
            for f in FEATURES:
                why_feature[f].update(per_feature[f])
                n_feature[f] += not per_feature[f]
                backstopped[f] += bool(used[f])
            why_signal.update(s)
            n_signal += not s
            for horizon in horizons:
                lab = label_reasons(bars, day, decision, horizon)
                why_label.update(lab)
                n_label += not lab
                n_both += not s and not lab
                rows.append({"day": day, "decision": decision, "horizon": horizon,
                             "signal_withheld": s, "label_missing": lab,
                             "feature_withheld": {} if s else {f: r for f, r in per_feature.items() if r}})
    signals = len(days) * len(decisions)
    labels = signals * len(horizons)
    return {
        "sessions": len(days),
        "signal_rows": signals, "signal_eligible": n_signal,
        "label_rows": labels, "label_available": n_label,
        "signal_and_label": n_both,
        "signal_withheld_reasons": dict(sorted(why_signal.items())),
        "feature_eligible": n_feature,
        "feature_withheld_reasons": {f: dict(sorted(c.items())) for f, c in why_feature.items()},
        "feature_values_using_the_backstop": backstopped,
        "label_missing_reasons": dict(sorted(why_label.items())),
        "rows": rows,
    }


# ── values ──────────────────────────────────────────────────────────────────


def feature_values(bars: Bars, day: str, decision: str) -> dict[str, float | None]:
    """The factory's raw features at ``decision`` from this side's own bars; None where it cannot be computed."""
    prior = bars.get(previous_session(day))
    today = bars.get(day)
    if not prior or not today:
        return dict.fromkeys(FEATURES)
    return raw_features(Session(today), Session(prior), _hm(_minutes(decision) - 1))


def compare(research: Bars, live: Bars, decisions: list[str], horizons: list[str],
            backstop: Backstop | None = None) -> dict:
    """Availability agreement and value differences on the sessions both sides hold.

    Research is judged by the research rule, live by the live recorder's rule, feature by feature. The live value
    compared is the one the recorder would have recorded. On a session the recorder withholds as a whole, the value
    its bars alone would give is still compared, as in version 1: a wrong value there is a finding about the source.
    A feature with no value on either side by definition (``F1_shift30`` at 10:00) agrees.
    """
    days = sorted(d for d in set(research) & set(live) if close_minute(d) is not None)
    rows = []
    per_feature = {f: {"matched_sessions": set(), "bad_sessions": set(), "compared": 0,
                       "out_of_tolerance": 0, "out_of_tolerance_by_day": Counter(), "max_abs_diff": None}
                   for f in FEATURES}
    availability_mismatch = []
    label_rows = []
    for day in days:
        for decision in decisions:
            r_why = signal_reasons(research, day, decision)
            l_session = live_signal_reasons(live, day, decision)
            l_val, l_why, l_used = live_features(live, backstop, day, decision)
            if l_session:
                l_val = feature_values(live, day, decision)
            disagree = [f for f in FEATURES if (not r_why) != (not l_why[f])]
            if disagree:
                availability_mismatch.append({"day": day, "decision": decision, "features": disagree,
                                              "research": r_why, "live": {f: l_why[f] for f in disagree}})
            r_val = feature_values(research, day, decision)
            for f in FEATURES:
                a, b = r_val[f], l_val[f]
                stat = per_feature[f]
                both_eligible = not r_why and not l_why[f]
                diff = None if a is None or b is None else abs(a - b)
                within = diff is not None and diff <= TOLERANCE[FEATURE_TYPE[f]]
                undefined = a is None and b is None and both_eligible
                if diff is not None:
                    stat["compared"] += 1
                    stat["max_abs_diff"] = diff if stat["max_abs_diff"] is None else max(stat["max_abs_diff"], diff)
                    if not within:
                        stat["out_of_tolerance"] += 1
                        stat["out_of_tolerance_by_day"][day] += 1
                if both_eligible and (within or undefined):
                    stat["matched_sessions"].add(day)
                else:
                    stat["bad_sessions"].add(day)
                rows.append({"day": day, "decision": decision, "feature": f, "research": a, "live": b,
                             "abs_diff": diff, "within_tolerance": within if diff is not None else None,
                             "both_eligible": both_eligible, "live_withheld": l_why[f],
                             "backstop_minutes": [list(u) for u in l_used[f]]})
            for horizon in horizons:
                entry, exit_bar, _ = label_bars(decision, horizon)
                prices = {}
                for side, bars in (("research", research), ("live", live)):
                    today = bars.get(day, {})
                    prices[side] = {"entry": today[entry][0] if entry in today else None,
                                    "exit": today[exit_bar][3] if exit_bar in today else None}
                both = all(v is not None for side in prices.values() for v in side.values())
                change_diff = (abs((prices["research"]["exit"] - prices["research"]["entry"])
                                   - (prices["live"]["exit"] - prices["live"]["entry"])) if both else None)
                label_rows.append({"day": day, "decision": decision, "horizon": horizon, **prices,
                                   "change_abs_diff_dollars": change_diff,
                                   "within_tolerance": None if change_diff is None
                                   else change_diff <= LABEL_TOLERANCE_DOLLARS})
    features = {}
    recent = days[-MIN_SESSIONS:]            # the sessions the verdict is taken on
    for f in FEATURES:
        stat = per_feature[f]
        matched = sorted(stat["matched_sessions"] - stat["bad_sessions"])
        recent_matched = [d for d in recent if d in matched]
        recent_out = sum(stat["out_of_tolerance_by_day"][d] for d in recent)
        recent_disagreed = sorted({m["day"] for m in availability_mismatch if f in m["features"] and m["day"] in recent})
        reasons = []
        if len(recent_matched) < MIN_SESSIONS:
            reasons.append("insufficient_matched_sessions")
        if recent_out:
            reasons.append("value_out_of_tolerance")
        if recent_disagreed:
            reasons.append("availability_mismatch")
        features[f] = {
            "type": FEATURE_TYPE[f], "tolerance": TOLERANCE[FEATURE_TYPE[f]],
            "live_source_tag": FEATURE_SPECS[f]["live_source"],
            "status": "research_only" if reasons else "live_ready",
            "reasons": reasons,
            "verdict_on": {"sessions": recent, "matched": len(recent_matched), "required": MIN_SESSIONS,
                           "values_out_of_tolerance": recent_out, "sessions_with_an_availability_disagreement": recent_disagreed},
            "whole_window": {"sessions_with_an_availability_disagreement":
                             len({m["day"] for m in availability_mismatch if f in m["features"]})},
            "matched_sessions": len(matched), "required_sessions": MIN_SESSIONS,
            "overlapping_sessions": len(days),
            "values_compared": stat["compared"], "values_out_of_tolerance": stat["out_of_tolerance"],
            "max_abs_diff": stat["max_abs_diff"],
        }
    labels_compared = [r for r in label_rows if r["within_tolerance"] is not None]
    return {
        "overlapping_sessions": days,
        "features": features,
        "availability_mismatch": availability_mismatch,
        "labels": {"rows": len(label_rows), "compared": len(labels_compared),
                   "out_of_tolerance": sum(not r["within_tolerance"] for r in labels_compared),
                   "missing_on_a_side": len(label_rows) - len(labels_compared),
                   "tolerance_dollars": LABEL_TOLERANCE_DOLLARS, "detail": label_rows},
        "value_rows": rows,
    }


def report(research_db: Path, signals_db: Path, *, first: str, last: str,
           decisions: list[str], horizons: list[str], symbol: str = "SPY") -> dict:
    """The parity report: live availability on every recorded session, then the overlap comparison."""
    research = load_research(research_db, symbol, first, last)
    live, sources = load_live(signals_db, symbol, first, last)
    backstop = load_backstop(signals_db, symbol, first, last)
    live_days = sorted(d for d in live if close_minute(d) is not None)
    # The first recorded session has no recorded prior; it is counted and its reason shown.
    compared = compare(research, live, decisions, horizons, backstop)
    statuses = {f: v["status"] for f, v in compared["features"].items()}
    return {
        "schema": SCHEMA,
        "symbol": symbol, "window": {"first": first, "last": last},
        "decision_times": decisions, "horizons": horizons,
        "research_sessions": len(research), "live_sessions": len(live_days),
        "research_last_session": max(research) if research else None,
        "live_first_session": min(live_days) if live_days else None,
        "live_last_session": max(live_days) if live_days else None,
        "live_sources_per_day": sources,
        "live_missing_minutes": {d: sorted(expected_minutes(d) - set(live[d])) for d in live_days},
        "live_rule": {"gap_max": GAP_MAX, "backstop_source": BACKSTOP_SOURCE,
                      "backstop_table": backstop is not None,
                      "backstop_bars": sum(len(v) for v in (backstop or {}).values()),
                      "note": ("live features are judged by the live recorder's lost-minute rule, research by the "
                               "research rule; a range feature needs each lost minute's backstop bar first seen "
                               "at or before the decision time")},
        "live_availability": availability(live, live_days, decisions, horizons, backstop),
        "comparison": compared,
        "live_ready": sorted(f for f, s in statuses.items() if s == "live_ready"),
        "research_only": sorted(f for f, s in statuses.items() if s == "research_only"),
        "promotion_allowed": False,
        "note": ("matching sessions are evidence about the source and the implementation, "
                 "not about any edge; no feature becomes live-ready by back-filling; the verdict is taken on the "
                 f"most recent {MIN_SESSIONS} overlapping sessions and the whole-window counts are reported beside it"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m analysis.factory_live_parity")
    parser.add_argument("--research-db", type=Path, required=True)
    parser.add_argument("--signals-db", type=Path, required=True)
    parser.add_argument("--first", required=True)
    parser.add_argument("--last", required=True)
    parser.add_argument("--decision-times", nargs="+", default=["15:00"])
    parser.add_argument("--horizons", nargs="+", default=["close"])
    parser.add_argument("--out", type=Path)
    parser.add_argument("--summary", action="store_true", help="print the totals without the per-row detail")
    args = parser.parse_args(argv)
    doc = report(args.research_db, args.signals_db, first=args.first, last=args.last,
                 decisions=args.decision_times, horizons=args.horizons)
    if args.out:
        with open(args.out, "x", encoding="utf-8") as stream:
            json.dump(doc, stream, indent=1, sort_keys=True)
    if args.summary:
        doc = json.loads(json.dumps(doc))
        doc["live_availability"].pop("rows")
        doc["comparison"].pop("value_rows")
        doc["comparison"]["labels"].pop("detail")
    print(json.dumps(doc, indent=1, sort_keys=True))
    return 0 if not doc["research_only"] else 1


if __name__ == "__main__":
    sys.exit(main())
