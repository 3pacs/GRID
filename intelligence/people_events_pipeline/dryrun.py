"""Dry run: what the materializer WOULD write, as counts. Never writes anything.

Inputs are frames (so tests run on fixtures); ``scripts/people_events_dry_run.py``
loads them from the Form 3/4/5 parquet and, optionally, a read-only database
session (``readonly.read_inputs``).
"""

from __future__ import annotations

import time as _time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from intelligence.people_events_pipeline import PIPELINE_VERSION
from intelligence.people_events_pipeline import adapters as A
from intelligence.people_events_pipeline import merge as M
from intelligence.people_events_pipeline import plan as P
from intelligence.people_events_pipeline import security as S


def form345_columns() -> list[str]:
    return [
        "accession_number", "document_type", "amended", "filing_date", "issuer_cik", "issuer_ticker",
        "owner_cik", "owner_name", "is_director", "is_officer", "is_ten_pct_owner", "nonderiv_trans_sk",
        "transaction_date", "transaction_date_raw", "transaction_code", "shares", "price_per_share",
        "acquired_disposed_code",
    ]


def qq_collision_estimate(sec_events: pd.DataFrame, start: Any, end: Any) -> dict[str, Any]:
    """How many SEC acts QuiverQuant's ``signal_sources`` key cannot hold.

    ``quiverquant._store_signals`` upserts on (source_type, constant
    source_id, ticker, signal_date, signal_type) with signal_type =
    insider_buy/insider_sell, so all acts on one (ticker, transaction date,
    acquired/disposed side) share ONE row and overwrite each other. From the
    SEC data (canonical events, so an amendment repeating a line is one
    act), over the same transaction-date window: acts in such groups
    beyond the first are acts the QuiverQuant table cannot represent.
    """
    if sec_events.empty:
        return {}
    c = sec_events[(sec_events["event_date"] >= start) & (sec_events["event_date"] <= end)]
    c = c[c["entity_ticker"].notna()]
    side = c["direction"].map({"buy": "A", "award": "A", "sell": "D"}).fillna("?")
    groups = c.groupby([c["entity_ticker"], c["event_date"], side]).size()
    acts = int(groups.sum())
    return {
        "window": [str(start), str(end)],
        "sec_acts": acts,
        "qq_rows_needed": int(len(groups)),
        "acts_lost_to_key_collision": int(acts - len(groups)),
        "share_lost": round((acts - len(groups)) / acts, 6) if acts else None,
    }


def form4_overlap(events: pd.DataFrame) -> dict[str, Any]:
    """Cross-source agreement for Form 4 where the SEC data set and a live feed overlap in time."""
    f4 = events[events["channel"] == "form4"]
    if f4.empty:
        return {}
    has_sec = f4["sources"].map(lambda s: "sec_form345" in s)
    out: dict[str, Any] = {}
    sec_dates = f4.loc[has_sec, "event_date"]
    for live in ("quiverquant", "edgar_native"):
        has_live = f4["sources"].map(lambda s, live=live: live in s)
        live_dates = f4.loc[has_live, "event_date"]
        if sec_dates.empty or live_dates.empty:
            out[live] = {"overlap_window": None}
            continue
        lo, hi = max(min(sec_dates), min(live_dates)), min(max(sec_dates), max(live_dates))
        if lo > hi:
            out[live] = {"overlap_window": None}
            continue
        win = (f4["event_date"] >= lo) & (f4["event_date"] <= hi)
        live_n = int((has_live & win).sum())
        both = int((has_live & has_sec & win).sum())
        tighter = f4[has_live & has_sec & win]
        out[live] = {
            "overlap_window": [str(lo), str(hi)],
            "live_events_in_window": live_n,
            "sec_events_in_window": int((has_sec & win).sum()),
            "live_events_matched_to_sec": both,
            "live_match_rate": round(both / live_n, 6) if live_n else None,
            "matched_events_known_at_from_live": int((~tighter["known_at_basis"].eq("filing")).sum()),
        }
    return out


def run_dry_run(*, form345: pd.DataFrame | None, signal_sources: pd.DataFrame | None,
                holdings: pd.DataFrame | None, identifiers: pd.DataFrame | None,
                stored: pd.DataFrame | None, observed_at: datetime,
                db_context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build candidates from every input, merge, resolve, plan; return the report dict."""
    observed_at = observed_at.astimezone(timezone.utc)
    timings: dict[str, float] = {}
    skips: Counter = Counter()
    parts = []

    t0 = _time.perf_counter()
    sec = A.empty_candidates()
    if form345 is not None and not form345.empty:
        sec, s = A.form4_from_form345(form345)
        skips.update(s)
        parts.append(sec)
    timings["adapt_form345_s"] = round(_time.perf_counter() - t0, 3)

    t0 = _time.perf_counter()
    if signal_sources is not None and not signal_sources.empty:
        live, s = A.from_signal_sources(signal_sources, observed_at)
        skips.update(s)
        parts.append(live)
    if holdings is not None and not holdings.empty:
        th, s = A.thirteen_f_changes(holdings)
        skips.update(s)
        parts.append(th)
    timings["adapt_db_sources_s"] = round(_time.perf_counter() - t0, 3)

    parts = [p for p in parts if not p.empty]
    candidates = pd.concat(parts, ignore_index=True) if parts else A.empty_candidates()

    t0 = _time.perf_counter()
    merged = M.merge_candidates(candidates)
    timings["merge_s"] = round(_time.perf_counter() - t0, 3)

    t0 = _time.perf_counter()
    resolved = S.resolve_securities(merged.events, identifiers if identifiers is not None else pd.DataFrame())
    timings["resolve_s"] = round(_time.perf_counter() - t0, 3)

    t0 = _time.perf_counter()
    stored = stored if stored is not None else pd.DataFrame()
    plan = P.build_write_plan(resolved, stored, pd.Timestamp(observed_at))
    timings["plan_s"] = round(_time.perf_counter() - t0, 3)

    coverage = _known_at_coverage(candidates, skips)
    report: dict[str, Any] = {
        "pipeline_version": PIPELINE_VERSION,
        "observed_at": observed_at.isoformat(),
        "mode": "dry_run_no_writes",
        "inputs": {
            "form345_rows": 0 if form345 is None else int(len(form345)),
            "signal_sources_rows": 0 if signal_sources is None else int(len(signal_sources)),
            "institutional_holdings_rows": 0 if holdings is None else int(len(holdings)),
            "security_identifiers_rows": 0 if identifiers is None else int(len(identifiers)),
            "people_events_rows_stored": int(len(stored)),
        },
        "channels": merged.stats,
        "known_at_coverage": coverage,
        "security_match": S.match_summary(resolved),
        "would_write": P.plan_counts(plan),
        "pit_invariants": M.pit_violations(resolved, pd.Timestamp(observed_at)),
        "form4_cross_source": form4_overlap(resolved),
        "skips": dict(sorted((k, int(v)) for k, v in skips.items() if v)),
        "timings": timings,
    }
    if not sec.empty and signal_sources is not None and not signal_sources.empty:
        qq = signal_sources[signal_sources["source_type"] == "quiverquant:insider"]
        if not qq.empty:
            dates = pd.to_datetime(qq["signal_date"]).dt.date
            sec_acts = resolved[(resolved["channel"] == "form4")
                                & resolved["sources"].map(lambda s: "sec_form345" in s)]
            report["qq_insider_key_collision_estimate"] = qq_collision_estimate(sec_acts, min(dates), max(dates))
    if db_context:
        report["db_context"] = db_context
    return report


def _known_at_coverage(candidates: pd.DataFrame, skips: Counter) -> dict[str, Any]:
    """Share of source rows that produced a candidate with a known_at (dropped rows had none or no act)."""
    out: dict[str, Any] = {}
    no_known = Counter()
    for k, v in skips.items():
        src, reason = k.split(":", 1) if ":" in k else (k, "")
        if reason.endswith("no_known_at"):
            no_known[src] += v
    by_source = candidates.groupby(["source_type", "known_at_basis"]).size() if not candidates.empty else pd.Series(dtype=int)
    for src in sorted(set(candidates["source_type"]) if not candidates.empty else set()):
        kept = int(by_source.loc[src].sum())
        dropped = int(no_known.get(src, 0))
        out[src] = {
            "candidates_with_known_at": kept,
            "dropped_no_known_at": dropped,
            "coverage": round(kept / (kept + dropped), 6) if kept + dropped else None,
            "by_basis": {k: int(v) for k, v in by_source.loc[src].items()},
        }
    return out


def to_jsonable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if obj is pd.NaT:
        return None
    return obj
