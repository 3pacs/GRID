"""The pre-open run — pre-registration step 2 / task spec item 2.

Orchestrates: chain selection (PIT-safe), the dealer-gamma engine (via the
adapter), P0 + VIX (yfinance), the ref-mismatch cross-check, and placebo
construction — applying every exclusion rule along the way — then writes
exactly one JSONL record.

All I/O is injected (``db_engine``, ``adapter``, ``now_fn``) so tests can
run every branch of this function with no network and no database. See
``tests/test_paper_log_preopen.py``.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy.engine import Engine

from paper_log.gex_levels.chain import select_chain_snapshot
from paper_log.gex_levels.clock import now_utc, session_date_for, to_eastern
from paper_log.gex_levels.config import (
    EXCL_ENGINE_UNAVAILABLE,
    EXCL_MARKET_CLOSED,
    EXCL_LATE_PREOPEN,
    EXCL_NO_CHAIN,
    EXCL_REF_MISMATCH,
    EXCL_STALE_CHAIN,
    PREOPEN_DEADLINE_ET,
    REF_MISMATCH_THRESHOLD_PCT,
    TICKER,
    VIX_TICKER,
)
from paper_log.gex_levels.db import assert_read_only
from paper_log.gex_levels.engine_adapter import DealerGammaAdapter
from paper_log.gex_levels.market_data import fetch_previous_close
from paper_log.gex_levels.placebo import build_placebos
from paper_log.gex_levels.records import envelope, levels_result_to_dict, price_point_to_dict
from paper_log.gex_levels.sessions import is_market_open, last_trading_day
from paper_log.gex_levels.storage import PaperLogStore


def run_preopen(
    *,
    log_dir: Path,
    db_engine: Engine,
    code_sha: str,
    now_fn: Callable[[], Any] = now_utc,
    adapter: DealerGammaAdapter | None = None,
    ticker: str = TICKER,
) -> dict[str, Any]:
    """Run one pre-open cycle and append exactly one record. Returns it."""
    run_at = now_fn()
    session_date = session_date_for(run_at)
    store = PaperLogStore(Path(log_dir))

    fields: dict[str, Any] = {
        "chain": None,
        "engine": None,
        "p0": None,
        "vix_prev_close": None,
        "ref_mismatch_pct": None,
        "levels": None,
    }

    def _finish(*, excluded: bool, reason: str | None) -> dict[str, Any]:
        record = envelope(
            kind="preopen", run_at=run_at, session_date=session_date,
            code_sha=code_sha, excluded=excluded, exclusion_reason=reason,
        )
        record.update(fields)
        written = store.append(record)
        if excluded:
            log.warning("paper_log preopen EXCLUDED session={d} reason={r}", d=session_date, r=reason)
        else:
            log.info(
                "paper_log preopen OK session={d} regime={r} spot={s}",
                d=session_date, r=fields["engine"]["regime"] if fields["engine"] else None,
                s=fields["engine"]["spot"] if fields["engine"] else None,
            )
        return written

    # ── Gate: is there even a regular session today? (cheap, no I/O) ────
    if not is_market_open(session_date):
        return _finish(excluded=True, reason=EXCL_MARKET_CLOSED)

    # ── Gate: are we already at/after 09:30 ET? (cheap, no I/O) ─────────
    # "Refuse to write a session's pre-open record at/after 09:30
    # America/New_York (write it as excluded late_preopen)." Checked before
    # any DB/network call so a slow chain lookup can never itself be the
    # reason a run crosses 09:30 and should have been refused.
    if to_eastern(run_at).time() >= PREOPEN_DEADLINE_ET:
        return _finish(excluded=True, reason=EXCL_LATE_PREOPEN)

    # From here on we touch the database — verify the read-only guard first.
    assert_read_only(db_engine)

    chain = select_chain_snapshot(db_engine, ticker, run_at)
    if chain is None:
        return _finish(excluded=True, reason=EXCL_NO_CHAIN)
    fields["chain"] = {"snap_date": chain.snap_date, "created_at": chain.created_at}

    prior_trading_day = last_trading_day(session_date - timedelta(days=1))
    if chain.snap_date < prior_trading_day:
        return _finish(excluded=True, reason=EXCL_STALE_CHAIN)

    if adapter is None:
        adapter = DealerGammaAdapter(db_engine=db_engine)
    levels = adapter.get_levels(ticker, chain.snap_date)
    fields["engine"] = levels_result_to_dict(levels)

    if not levels.available:
        return _finish(excluded=True, reason=EXCL_ENGINE_UNAVAILABLE)

    p0 = fetch_previous_close(ticker, session_date, now_fn=now_fn)
    vix = fetch_previous_close(VIX_TICKER, session_date, now_fn=now_fn)
    if p0 is None or vix is None:
        # Not one of the pre-registration's seven exclusion codes — that
        # list covers session-level data-quality problems (a stale chain,
        # an unavailable engine, ...), not "the data vendor was briefly
        # unreachable". Treating a yfinance outage as, say, engine_unavailable
        # would misattribute the cause and pollute the exclusion-rate audit
        # in status/evaluate. Instead: fail loudly, write nothing (the log
        # stays append-only-clean — no line at all for this attempt, same
        # as any other crashed run), and let the operator re-run before
        # 09:30 ET or rely on the next scheduled attempt.
        missing = "P0 (SPY previous close)" if p0 is None else "VIX previous close"
        raise RuntimeError(
            f"preopen: yfinance returned no data for {missing} "
            f"(session_date={session_date}) — not a pre-registered exclusion "
            "reason, so no record was written. Re-run before 09:30 ET."
        )

    fields["p0"] = price_point_to_dict(p0)
    fields["vix_prev_close"] = price_point_to_dict(vix)

    ref_mismatch_pct = abs(levels.spot - p0.price) / p0.price
    fields["ref_mismatch_pct"] = ref_mismatch_pct
    if ref_mismatch_pct > REF_MISMATCH_THRESHOLD_PCT:
        return _finish(excluded=True, reason=EXCL_REF_MISMATCH)

    real_levels = {
        "gamma_flip": levels.gamma_flip,
        "put_wall": levels.put_wall,
        "call_wall": levels.call_wall,
    }
    placebos = build_placebos(real_levels, p0.price)
    fields["levels"] = {
        "real": real_levels,
        "placebo": {name: asdict(pb) for name, pb in placebos.items()},
    }

    return _finish(excluded=False, reason=None)
