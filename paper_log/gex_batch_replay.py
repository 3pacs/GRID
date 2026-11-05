"""Retrospective replay of the GEX-levels pre-open computation for ONE
identified options capture batch.

This module is deliberately *outside* ``paper_log.gex_levels`` -- the frozen,
pre-registered v1 paper log (prereg SHA ce9b55e2..., code pinned on
grid-svr). Nothing here is imported by that package, nothing here changes its
code, its log, or its evaluation semantics, and nothing here writes to its
JSONL store. The live pre-open job keeps reading the ``options_snapshots``
view (the latest complete batch per ticker/day, exactly what the old
delete-then-insert table held).

What this adds (review finding 1 on the GEM append-only candidate): after
``options_append_only_20260930`` every capture batch is kept, so a specific
earlier batch -- e.g. GEM's 10:05 NY SPY batch after a later same-day
scheduler capture -- can be replayed through the same steps the pre-open run
takes after chain selection: the dealer-gamma engine (via the v1 adapter's
translation), the ref-mismatch check, Amendment 1 tested walls from the full
per-strike chain, and placebos. The batch is named by an explicit
``(capture_batch_id, capture_ordinal)`` pair and every step fails closed
unless that pair is a registered, fully stored batch for the ticker.

The output is a ``kind="preopen_replay"`` dict with ``live: False`` and the
batch identity -- never confusable with a live v1 record. P0/VIX are passed
in by the caller (a replay has no "pre-open moment" to fetch at); the CLI
fetches them with the same v1 ``fetch_previous_close`` helper.

The engine used is this checkout's ``physics.dealer_gamma``; the receipt
records ``code_sha`` so a replay is attributable to the code that made it.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Date, text
from sqlalchemy.engine import Engine

from ingestion.market_calendar import is_market_open
from paper_log.gex_levels.config import (
    EXCL_ENGINE_UNAVAILABLE,
    EXCL_REF_MISMATCH,
    REF_MISMATCH_THRESHOLD_PCT,
    TESTED_WALL_MIN_DISTANCE_PCT,
    VIX_TICKER,
)
from paper_log.gex_levels.engine_adapter import DealerGammaAdapter
from paper_log.gex_levels.market_data import PricePoint
from paper_log.gex_levels.placebo import build_placebos
from paper_log.gex_levels.records import levels_result_to_dict, price_point_to_dict
from paper_log.gex_levels.tested_walls import (
    WallSelection,
    aggregate_per_strike_gex,
    select_tested_walls,
)

REPLAY_KIND = "preopen_replay"


class BatchReplayError(ValueError):
    """The requested (capture_batch_id, capture_ordinal) is not replayable."""


@dataclass(frozen=True)
class CaptureBatch:
    capture_batch_id: str
    capture_ordinal: int
    ticker: str
    snap_date: date
    capture_started_at: datetime
    capture_completed_at: datetime
    row_count: int
    capture_source: str
    backfilled: bool


_BATCH_QUERY = text(
    """
    SELECT b.capture_batch_id, b.capture_ordinal, b.ticker, b.snap_date,
           b.capture_started_at, b.capture_completed_at, b.row_count,
           b.capture_source, b.backfilled,
           (SELECT COUNT(*) FROM options_snapshots_all s
             WHERE s.capture_batch_id = b.capture_batch_id
               AND s.capture_ordinal = b.capture_ordinal
               AND s.ticker = b.ticker AND s.snap_date = b.snap_date) AS stored_rows
    FROM options_capture_batches b
    WHERE b.capture_batch_id = :batch
    """
).columns(snap_date=Date())

_BATCH_CHAIN_QUERY = text(
    """
    SELECT strike, opt_type, open_interest, implied_vol AS implied_volatility, expiry
    FROM options_snapshots_all
    WHERE capture_batch_id = :batch AND capture_ordinal = :ordinal
      AND ticker = :ticker AND snap_date = :snap_date
      AND open_interest > 0 AND implied_vol > 0
      AND expiry > :snap_date
    """
).columns(expiry=Date())


def resolve_batch(db_engine: Engine, ticker: str, capture_batch_id: str,
                  capture_ordinal: int) -> CaptureBatch:
    """Return the registered batch, or raise :class:`BatchReplayError`.

    Fail-closed pair: the id must be registered, its ordinal and ticker must
    equal the requested ones, and exactly ``row_count`` rows must be stored.
    """
    if (not isinstance(capture_batch_id, str) or not capture_batch_id
            or not isinstance(capture_ordinal, int) or isinstance(capture_ordinal, bool)
            or capture_ordinal <= 0):
        raise BatchReplayError("replay needs a non-empty capture_batch_id and a positive capture_ordinal")
    with db_engine.connect() as conn:
        row = conn.execute(_BATCH_QUERY, {"batch": capture_batch_id}).fetchone()
    if row is None:
        raise BatchReplayError(f"capture batch {capture_batch_id} is not registered")
    batch = CaptureBatch(
        capture_batch_id=row.capture_batch_id, capture_ordinal=int(row.capture_ordinal),
        ticker=row.ticker, snap_date=row.snap_date,
        capture_started_at=row.capture_started_at,
        capture_completed_at=row.capture_completed_at,
        row_count=int(row.row_count), capture_source=row.capture_source,
        backfilled=bool(row.backfilled),
    )
    if batch.capture_ordinal != capture_ordinal:
        raise BatchReplayError("capture_ordinal does not match the registered batch")
    if batch.ticker != ticker:
        raise BatchReplayError("capture batch belongs to a different ticker")
    if int(row.stored_rows) != batch.row_count:
        raise BatchReplayError("capture batch is not completely stored")
    return batch


class _BatchPinnedEngine:
    """``LevelsEngine`` that can only ever compute the one pinned batch."""

    def __init__(self, db_engine: Engine, batch: CaptureBatch, *, risk_free_rate: float = 0.05) -> None:
        from physics.dealer_gamma import DealerGammaEngine

        self._engine = DealerGammaEngine(db_engine, risk_free_rate=risk_free_rate)
        self._batch = batch

    def compute_gex_profile(self, ticker: str, snap_date: date | None = None) -> dict[str, Any]:
        if ticker != self._batch.ticker or snap_date != self._batch.snap_date:
            return {"error": "replay engine is pinned to a different batch", "ticker": ticker}
        return self._engine.compute_gex_profile(
            ticker, snap_date, capture_batch_id=self._batch.capture_batch_id,
        )


def batch_tested_walls(db_engine: Engine, batch: CaptureBatch, spot: float, p0: float, *,
                       min_distance_pct: float = TESTED_WALL_MIN_DISTANCE_PCT,
                       risk_free_rate: float = 0.05) -> WallSelection:
    """Amendment 1 tested walls from exactly the pinned batch's rows."""
    with db_engine.connect() as conn:
        rows = conn.execute(_BATCH_CHAIN_QUERY, {
            "batch": batch.capture_batch_id, "ordinal": batch.capture_ordinal,
            "ticker": batch.ticker, "snap_date": batch.snap_date,
        }).fetchall()
    per_strike = aggregate_per_strike_gex(rows, spot, batch.snap_date, risk_free_rate=risk_free_rate)
    return select_tested_walls(per_strike, p0, min_distance_pct=min_distance_pct)


def next_session_after(day: date) -> date:
    candidate = day + timedelta(days=1)
    for _ in range(14):
        if is_market_open(candidate):
            return candidate
        candidate += timedelta(days=1)
    raise BatchReplayError(f"no trading session within two weeks after {day}")


def replay_preopen_for_batch(
    *,
    db_engine: Engine,
    ticker: str,
    capture_batch_id: str,
    capture_ordinal: int,
    p0: PricePoint,
    vix: PricePoint | None,
    code_sha: str,
    adapter: DealerGammaAdapter | None = None,
    now_fn=lambda: datetime.now(timezone.utc),
) -> dict[str, Any]:
    """Replay the post-chain-selection pre-open steps for one pinned batch.

    Raises :class:`BatchReplayError` when the batch pair is not replayable;
    otherwise returns a ``preopen_replay`` record (``excluded`` when the
    engine or the ref-mismatch check would have excluded the session).
    """
    batch = resolve_batch(db_engine, ticker, capture_batch_id, capture_ordinal)
    if not isinstance(p0, PricePoint):
        raise BatchReplayError("replay needs the session's P0 as a PricePoint")

    record: dict[str, Any] = {
        "kind": REPLAY_KIND,
        "live": False,
        "replayed_at": now_fn(),
        "session_date": next_session_after(batch.snap_date),
        "code_sha": code_sha,
        "excluded": False,
        "exclusion_reason": None,
        "chain": {
            "snap_date": batch.snap_date,
            "capture_batch_id": batch.capture_batch_id,
            "capture_ordinal": batch.capture_ordinal,
            "capture_started_at": batch.capture_started_at,
            "capture_completed_at": batch.capture_completed_at,
            "row_count": batch.row_count,
            "capture_source": batch.capture_source,
            "backfilled": batch.backfilled,
        },
        "engine": None,
        "p0": price_point_to_dict(p0),
        "vix_prev_close": price_point_to_dict(vix),
        "ref_mismatch_pct": None,
        "levels": None,
    }

    def _excluded(reason: str) -> dict[str, Any]:
        record["excluded"] = True
        record["exclusion_reason"] = reason
        return record

    if adapter is None:
        adapter = DealerGammaAdapter(engine=_BatchPinnedEngine(db_engine, batch))
    levels = adapter.get_levels(ticker, batch.snap_date)
    record["engine"] = levels_result_to_dict(levels)
    if not levels.available:
        return _excluded(EXCL_ENGINE_UNAVAILABLE)
    if levels.raw.get("chain_batch_id") != batch.capture_batch_id:
        # The engine must have read the pinned batch, never the day's latest.
        raise BatchReplayError("engine result is not from the pinned capture batch")

    ref_mismatch_pct = abs(levels.spot - p0.price) / p0.price
    record["ref_mismatch_pct"] = ref_mismatch_pct
    if ref_mismatch_pct > REF_MISMATCH_THRESHOLD_PCT:
        return _excluded(EXCL_REF_MISMATCH)

    tested = batch_tested_walls(db_engine, batch, levels.spot, p0.price)
    present: dict[str, float] = {}
    if levels.gamma_flip is not None:
        present["gamma_flip"] = levels.gamma_flip
    if tested.put_wall is not None:
        present["put_wall"] = tested.put_wall
    if tested.call_wall is not None:
        present["call_wall"] = tested.call_wall
    record["levels"] = {
        "real": {
            "gamma_flip": levels.gamma_flip,
            "gamma_flip_missing": levels.gamma_flip is None,
            "put_wall": tested.put_wall,
            "put_wall_missing": tested.put_wall is None,
            "call_wall": tested.call_wall,
            "call_wall_missing": tested.call_wall is None,
        },
        "placebo": {name: asdict(pb) for name, pb in build_placebos(present, p0.price).items()},
    }
    return record


def main(argv: list[str] | None = None) -> int:
    """``python -m paper_log.gex_batch_replay --batch ID --ordinal N`` (read-only)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ticker", default="SPY")
    parser.add_argument("--batch", required=True)
    parser.add_argument("--ordinal", required=True, type=int)
    parser.add_argument("--code-sha", default="unknown")
    args = parser.parse_args(argv)

    from paper_log.gex_levels.db import build_readonly_engine
    from paper_log.gex_levels.market_data import fetch_previous_close

    engine = build_readonly_engine()
    batch = resolve_batch(engine, args.ticker, args.batch, args.ordinal)
    session = next_session_after(batch.snap_date)
    p0 = fetch_previous_close(args.ticker, session)
    if p0 is None:
        print(json.dumps({"error": "P0 unavailable"}))
        return 1
    vix = fetch_previous_close(VIX_TICKER, session)
    record = replay_preopen_for_batch(
        db_engine=engine, ticker=args.ticker, capture_batch_id=args.batch,
        capture_ordinal=args.ordinal, p0=p0, vix=vix, code_sha=args.code_sha,
    )
    print(json.dumps(record, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
