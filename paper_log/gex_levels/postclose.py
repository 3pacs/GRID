"""The post-close run — pre-registration step 3 / task spec item 3.

Reads the matching pre-open record (must exist and be valid — a session's
validity is gated by its pre-open record per the pre-registration's
Schedule section), fetches the session's bars and OHLC, applies
`bars_missing` / `market_closed`, and computes reach/gap-through/held for
every real and placebo level plus the H3 real and placebo trades — then
writes exactly one JSONL record.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Callable

from loguru import logger as log

from paper_log.gex_levels.clock import now_utc, session_date_for
from paper_log.gex_levels.config import (
    EXCL_BARS_MISSING,
    EXCL_MARKET_CLOSED,
    H3_LEVEL_NAMES,
    LEVEL_NAMES,
    MAX_MISSING_BARS_PCT,
    TICKER,
)
from paper_log.gex_levels.definitions import compute_h3_trade, evaluate_reach, range_ln
from paper_log.gex_levels.market_data import fetch_intraday_bars, fetch_session_ohlc
from paper_log.gex_levels.records import envelope
from paper_log.gex_levels.sessions import expected_bar_count, is_known_early_close, is_market_open
from paper_log.gex_levels.storage import PaperLogStore


def _find_latest_preopen(store: PaperLogStore, session_date: date) -> dict[str, Any] | None:
    """The most recent `kind == "preopen"` record for this session_date.

    Records read back from JSONL carry `session_date` as an ISO string
    (JSON has no native date type) — compare against `.isoformat()`.
    """
    target = session_date.isoformat()
    match: dict[str, Any] | None = None
    for record in store.read_all():
        if record.get("kind") == "preopen" and record.get("session_date") == target:
            match = record
    return match


def run_postclose(
    *,
    log_dir: Path,
    code_sha: str,
    now_fn: Callable[[], Any] = now_utc,
    ticker: str = TICKER,
) -> dict[str, Any]:
    """Run one post-close cycle and append exactly one record. Returns it."""
    run_at = now_fn()
    session_date = session_date_for(run_at)
    store = PaperLogStore(Path(log_dir))

    fields: dict[str, Any] = {
        "bars": None,
        "session_ohlc": None,
        "range_ln": None,
        "reaches": None,
        "h3_trade": None,
    }

    def _finish(*, excluded: bool, reason: str | None) -> dict[str, Any]:
        record = envelope(
            kind="postclose", run_at=run_at, session_date=session_date,
            code_sha=code_sha, excluded=excluded, exclusion_reason=reason,
        )
        record.update(fields)
        written = store.append(record)
        if excluded:
            log.warning("paper_log postclose EXCLUDED session={d} reason={r}", d=session_date, r=reason)
        else:
            log.info("paper_log postclose OK session={d}", d=session_date)
        return written

    if not is_market_open(session_date):
        return _finish(excluded=True, reason=EXCL_MARKET_CLOSED)

    preopen_record = _find_latest_preopen(store, session_date)
    if preopen_record is None:
        # No taxonomy code covers "preopen never ran" (the seven exclusion
        # codes are session-level data-quality outcomes, not "the prior
        # step is missing"). Fail loudly instead of guessing; nothing is
        # written, matching the same reasoning as preopen's own P0/VIX
        # fetch-failure case.
        raise RuntimeError(
            f"postclose: no preopen record found for session_date={session_date} "
            f"in {store.log_path} — run preopen first. Not a pre-registered "
            "exclusion reason, so no record was written."
        )

    if preopen_record["excluded"]:
        # The session was never validly registered at pre-open — there are
        # no levels to score against. Reuse preopen's own (already
        # pre-registered) reason rather than inventing a new code.
        return _finish(excluded=True, reason=preopen_record["exclusion_reason"])

    ohlc = fetch_session_ohlc(ticker, session_date, now_fn=now_fn)
    if ohlc is None:
        # The calendar says this should be a trading day but yfinance has
        # no session bar for it (unscheduled closure) — market_closed is
        # the honest label; see sessions.py's early-close docstring for the
        # matching "safe direction" reasoning.
        return _finish(excluded=True, reason=EXCL_MARKET_CLOSED)
    fields["session_ohlc"] = {
        "open": ohlc.open, "high": ohlc.high, "low": ohlc.low, "close": ohlc.close,
        "fetched_at": ohlc.fetched_at,
    }

    bars_result = fetch_intraday_bars(ticker, session_date, now_fn=now_fn)
    expected = expected_bar_count(session_date)
    present = len(bars_result.bars)
    missing_pct = 1.0 - (present / expected) if expected > 0 else 1.0
    fields["bars"] = {
        "expected": expected,
        "present": present,
        "missing_pct": missing_pct,
        "early_close": is_known_early_close(session_date),
        "fetched_at": bars_result.fetched_at,
    }
    if missing_pct > MAX_MISSING_BARS_PCT:
        return _finish(excluded=True, reason=EXCL_BARS_MISSING)

    fields["range_ln"] = range_ln(ohlc.high, ohlc.low)

    p0_price = preopen_record["p0"]["price"]
    real_levels: dict[str, float] = preopen_record["levels"]["real"]
    placebo_raw: dict[str, dict[str, Any]] = preopen_record["levels"]["placebo"]
    regime = preopen_record["engine"]["regime"]
    bars_seq = bars_result.bars

    reaches_real = {
        name: asdict(evaluate_reach(real_levels[name], p0_price, ohlc.open, ohlc.close, bars_seq))
        for name in LEVEL_NAMES
    }

    reaches_placebo: dict[str, dict[str, Any] | None] = {}
    placebo_values: dict[str, float] = {}
    for name in LEVEL_NAMES:
        pb = placebo_raw.get(name)
        if pb is None or pb["dropped"]:
            reaches_placebo[name] = None
            continue
        placebo_values[name] = pb["value"]
        reaches_placebo[name] = asdict(
            evaluate_reach(pb["value"], p0_price, ohlc.open, ohlc.close, bars_seq)
        )

    fields["reaches"] = {"real": reaches_real, "placebo": reaches_placebo}

    real_walls = {name: real_levels[name] for name in H3_LEVEL_NAMES}
    placebo_walls = {name: placebo_values[name] for name in H3_LEVEL_NAMES if name in placebo_values}

    real_trade = compute_h3_trade(regime, real_walls, p0_price, ohlc.open, ohlc.close, bars_seq)
    placebo_trade = compute_h3_trade(regime, placebo_walls, p0_price, ohlc.open, ohlc.close, bars_seq)

    fields["h3_trade"] = {"real": asdict(real_trade), "placebo": asdict(placebo_trade)}

    return _finish(excluded=False, reason=None)
