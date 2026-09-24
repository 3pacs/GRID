"""The pre-open run — pre-registration step 2 / task spec item 2 (as
amended by Amendment 1, 2951e4cc).

Orchestrates: chain selection (PIT-safe), the dealer-gamma engine (via the
adapter), P0 + VIX (yfinance, with one retry each), the ref-mismatch
cross-check, tested-wall selection (Amendment 1), and placebo construction
— applying every exclusion rule along the way — then writes exactly one
JSONL record.

All I/O is injected (``db_engine``, ``adapter``, ``now_fn``,
``compute_tested_walls``) so tests can run every branch of this function
with no network and no database. See ``tests/test_paper_log_gex_levels_preopen.py``.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy.engine import Engine

from paper_log.gex_levels.chain import select_chain_snapshot
from paper_log.gex_levels.clock import now_utc, session_date_for, to_eastern
from paper_log.gex_levels.config import (
    DATA_FETCH_ATTEMPTS,
    EXCL_DATA_UNAVAILABLE,
    EXCL_ENGINE_UNAVAILABLE,
    EXCL_LATE_PREOPEN,
    EXCL_MARKET_CLOSED,
    EXCL_NO_CHAIN,
    EXCL_REF_MISMATCH,
    EXCL_STALE_CHAIN,
    PREOPEN_DEADLINE_ET,
    REF_MISMATCH_THRESHOLD_PCT,
    TESTED_WALL_MIN_DISTANCE_PCT,
    TICKER,
    VIX_TICKER,
)
from paper_log.gex_levels.db import assert_read_only
from paper_log.gex_levels.engine_adapter import DealerGammaAdapter
from paper_log.gex_levels.market_data import PricePoint, fetch_previous_close
from paper_log.gex_levels.placebo import build_placebos
from paper_log.gex_levels.records import envelope, levels_result_to_dict, price_point_to_dict
from paper_log.gex_levels.sessions import is_market_open, last_trading_day
from paper_log.gex_levels.storage import PaperLogStore
from paper_log.gex_levels.tested_walls import WallSelection, compute_tested_walls_from_db


def _fetch_with_retry(
    fetch_fn: Callable[..., PricePoint | None],
    *args: Any,
    attempts: int = DATA_FETCH_ATTEMPTS,
    label: str,
    **kwargs: Any,
) -> PricePoint | None:
    """Amendment 1: "the market-data source for P0 or VIX was unreachable
    or returned nothing at the pre-open run, after one retry." Treats a
    raised exception ("unreachable") the same as a clean ``None`` return
    ("returned nothing") — both count as an attempt that needs a retry —
    and never raises itself; the caller decides what a final ``None``
    means (``data_unavailable``)."""
    result: PricePoint | None = None
    for attempt in range(1, attempts + 1):
        try:
            result = fetch_fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 — any yfinance/network failure counts as "unreachable"
            log.warning(
                "paper_log preopen: {label} fetch attempt {a}/{n} raised: {e}",
                label=label, a=attempt, n=attempts, e=str(exc),
            )
            result = None
        if result is not None:
            return result
        if attempt < attempts:
            log.warning(
                "paper_log preopen: {label} fetch attempt {a}/{n} returned nothing, retrying",
                label=label, a=attempt, n=attempts,
            )
    return None


def run_preopen(
    *,
    log_dir: Path,
    db_engine: Engine,
    code_sha: str,
    now_fn: Callable[[], Any] = now_utc,
    adapter: DealerGammaAdapter | None = None,
    compute_tested_walls: Callable[[Engine, str, date, float, float], WallSelection] | None = None,
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
        # Amendment 1: engine_unavailable now means no measured spot or no
        # regime only — a missing flip/wall no longer lands here (handled
        # in `engine_adapter.py`).
        return _finish(excluded=True, reason=EXCL_ENGINE_UNAVAILABLE)

    p0 = _fetch_with_retry(fetch_previous_close, ticker, session_date, now_fn=now_fn, label="P0")
    if p0 is None:
        return _finish(excluded=True, reason=EXCL_DATA_UNAVAILABLE)
    fields["p0"] = price_point_to_dict(p0)

    vix = _fetch_with_retry(fetch_previous_close, VIX_TICKER, session_date, now_fn=now_fn, label="VIX")
    if vix is None:
        return _finish(excluded=True, reason=EXCL_DATA_UNAVAILABLE)
    fields["vix_prev_close"] = price_point_to_dict(vix)

    ref_mismatch_pct = abs(levels.spot - p0.price) / p0.price
    fields["ref_mismatch_pct"] = ref_mismatch_pct
    if ref_mismatch_pct > REF_MISMATCH_THRESHOLD_PCT:
        return _finish(excluded=True, reason=EXCL_REF_MISMATCH)

    # Amendment 1: tested walls, from the full per-strike chain — not the
    # engine's own (untested) put_wall/call_wall, recorded separately in
    # fields["engine"] as engine_put_wall/engine_call_wall.
    if compute_tested_walls is None:
        tested = compute_tested_walls_from_db(
            db_engine, ticker, chain.snap_date, levels.spot, p0.price,
            min_distance_pct=TESTED_WALL_MIN_DISTANCE_PCT,
        )
    else:
        tested = compute_tested_walls(db_engine, ticker, chain.snap_date, levels.spot, p0.price)

    tested_present: dict[str, float] = {}
    if levels.gamma_flip is not None:
        tested_present["gamma_flip"] = levels.gamma_flip
    if tested.put_wall is not None:
        tested_present["put_wall"] = tested.put_wall
    if tested.call_wall is not None:
        tested_present["call_wall"] = tested.call_wall

    placebos = build_placebos(tested_present, p0.price)

    fields["levels"] = {
        "real": {
            "gamma_flip": levels.gamma_flip,
            "gamma_flip_missing": levels.gamma_flip is None,
            "put_wall": tested.put_wall,
            "put_wall_missing": tested.put_wall is None,
            "call_wall": tested.call_wall,
            "call_wall_missing": tested.call_wall is None,
        },
        "placebo": {name: asdict(pb) for name, pb in placebos.items()},
    }

    return _finish(excluded=False, reason=None)
