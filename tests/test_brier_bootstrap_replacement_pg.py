"""Real-PostgreSQL proof that the Brier bootstraps replace their tables atomically.

Codex HOLD 6011907767 on #1354: the per-signal and regime bootstraps skipped a
bucket whose effective weight Σw was below 5 but left the row an earlier run had
written, so a legacy (count-diluted) confidence boost outlived the replacement.
Both writers now delete the old population and insert the validated one in a
single transaction.

These tests seed legacy rows that read as a 1.5x boost, run the writer, and read
back through the production readers:
- a row the replacement rejects is gone, and so is a row it no longer produces;
- a failure part-way through rolls the whole replacement back.

Runs in a scratch schema on the shared ``pg_engine`` (``GRID_TEST_DB_URL``);
skips when no PostgreSQL is reachable.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from features import regime_conditional_brier as rcb
from features.per_signal_brier import ensure_tables, get_signal_scorecard
from scripts import bootstrap_per_signal_brier as boot

_LEGACY = datetime(2026, 5, 16, tzinfo=timezone.utc)
_STAMP = datetime(2026, 2, 1, tzinfo=timezone.utc)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"brier_replace_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        ensure_tables(engine)
        rcb.ensure_regime_brier_table(engine)
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _seed_flat(engine: Engine, source: str, horizon: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO per_signal_brier_history (signal_source, horizon_days, scored_count, "
                "running_brier, running_ece, hit_count, last_updated) "
                "VALUES (:s, :h, 40, 0.02, 0.1, 40, :t)"
            ),
            {"s": source, "h": horizon, "t": _LEGACY},
        )


def _seed_regime(engine: Engine, source: str, horizon: int, regime: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO regime_conditional_brier_history (signal_source, horizon_days, regime, "
                "scored_count, running_brier, running_ece, hit_count, last_updated) "
                "VALUES (:s, :h, :r, 40, 0.02, 0.1, 40, :t)"
            ),
            {"s": source, "h": horizon, "r": regime, "t": _LEGACY},
        )


def _acc(count: int, weight_sum: float, *, last_updated: datetime | None = _STAMP) -> dict:
    return {
        "scored_count": count,
        "weight_sum": weight_sum,
        "weighted_brier_sum": weight_sum * 0.01,
        "weighted_ece_sum": weight_sum * 0.1,
        "hit_count": min(count, 2**31 - 1),
        "last_updated": last_updated,
    }


def _flat_keys(engine: Engine) -> set[tuple[str, int]]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT signal_source, horizon_days FROM per_signal_brier_history")).fetchall()
    return {(r[0], int(r[1])) for r in rows}


def _regime_keys(engine: Engine) -> set[tuple[str, int, str]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT signal_source, horizon_days, regime FROM regime_conditional_brier_history")
        ).fetchall()
    return {(r[0], int(r[1]), r[2]) for r in rows}


def test_flat_replacement_retires_rejected_and_absent_legacy_rows(scratch):
    _seed_flat(scratch, "tiny_lucky", 7)
    _seed_flat(scratch, "retired_feed", 30)
    assert get_signal_scorecard(scratch, "tiny_lucky", 7).conviction_weight == 1.5

    stats = boot._write_bootstrapped_buckets(
        scratch,
        {("tiny_lucky", 7): _acc(25, 1.25), ("main_driver", 7): _acc(25, 23.75)},
    )

    assert stats == {"written": 1, "skipped_low_effective_weight": 1, "retired": 2, "replaced": True}
    assert _flat_keys(scratch) == {("main_driver", 7)}
    assert get_signal_scorecard(scratch, "tiny_lucky", 7) is None
    assert get_signal_scorecard(scratch, "retired_feed", 30) is None
    main = get_signal_scorecard(scratch, "main_driver", 7)
    assert main.conviction_weight == 1.5 and main.last_updated == _STAMP


def test_flat_replacement_rolls_back_when_an_insert_fails(scratch):
    _seed_flat(scratch, "tiny_lucky", 7)
    _seed_flat(scratch, "retired_feed", 30)

    # scored_count overflows INTEGER on the second insert, after the delete.
    stats = boot._write_bootstrapped_buckets(
        scratch,
        {("main_driver", 7): _acc(25, 23.75), ("overflow", 7): _acc(2**40, 23.75)},
    )

    assert stats["replaced"] is False and stats["written"] == 0
    assert _flat_keys(scratch) == {("tiny_lucky", 7), ("retired_feed", 30)}
    assert get_signal_scorecard(scratch, "tiny_lucky", 7).conviction_weight == 1.5


# The regime bootstrap reads oracle_predictions through the visibility view; the
# tests point its query at a scratch table with the same nine columns instead.
_ORACLE_FIXTURE_DDL = """
    CREATE TABLE oracle_fixture (
        id TEXT, created_at TIMESTAMPTZ, expiry DATE, confidence DOUBLE PRECISION,
        verdict TEXT, model_name TEXT, signals TEXT, signal_contributions TEXT, regime TEXT
    )
"""
_ORACLE_FIXTURE_QUERY = text(
    "SELECT id, created_at, expiry, confidence, verdict, model_name, signals, "
    "signal_contributions, regime FROM oracle_fixture WHERE :days > 0 ORDER BY created_at"
)


def _seed_lucky_predictions(engine: Engine, n: int = 25) -> None:
    """``n`` CRISIS hits at 0.9: tiny_lucky has w=0.05 (Σw = 1.25 at n=25), main_driver 0.95."""
    with engine.begin() as conn:
        conn.execute(text(_ORACLE_FIXTURE_DDL))
        for i in range(n):
            conn.execute(
                text(
                    "INSERT INTO oracle_fixture "
                    "VALUES (:id, :c, :x, 0.9, 'hit', 'm', NULL, :sc, 'CRISIS')"
                ),
                {
                    "id": f"lucky-{i}",
                    "c": _STAMP + timedelta(days=i),
                    "x": (_STAMP + timedelta(days=i + 7)).date(),
                    "sc": '{"tiny_lucky": 0.05, "main_driver": 0.95}',
                },
            )


def test_regime_replacement_retires_rejected_and_absent_legacy_rows(scratch, monkeypatch):
    _seed_regime(scratch, "tiny_lucky", 7, "CRISIS")
    _seed_regime(scratch, "retired_feed", 30, "EXPANSION")
    assert rcb.get_regime_conditional_scorecard(scratch, "tiny_lucky", 7, "CRISIS").conviction_weight == 1.5
    _seed_lucky_predictions(scratch)
    monkeypatch.setattr(rcb, "_BOOTSTRAP_QUERY", _ORACLE_FIXTURE_QUERY)

    summary = rcb.bootstrap_from_oracle_predictions(scratch, days=365)

    assert summary["replaced"] is True
    assert summary["written_count"] == 1
    assert summary["skipped_low_effective_weight"] == 1
    assert summary["retired_count"] == 2
    assert _regime_keys(scratch) == {("main_driver", 7, "CRISIS")}
    assert rcb.get_regime_conditional_scorecard(scratch, "tiny_lucky", 7, "CRISIS") is None
    # Falls back to the empty per-signal table: no history, so neutral.
    assert rcb.get_scorecard_with_regime_fallback(scratch, "tiny_lucky", 7, "CRISIS") is None
    assert rcb.get_regime_conditional_scorecard(scratch, "retired_feed", 30, "EXPANSION") is None
    main = rcb.get_regime_conditional_scorecard(scratch, "main_driver", 7, "CRISIS")
    assert main.conviction_weight == 1.5


def test_regime_replacement_rolls_back_when_an_insert_fails(scratch, monkeypatch):
    _seed_regime(scratch, "tiny_lucky", 7, "CRISIS")
    _seed_regime(scratch, "retired_feed", 30, "EXPANSION")
    _seed_lucky_predictions(scratch)
    monkeypatch.setattr(rcb, "_BOOTSTRAP_QUERY", _ORACLE_FIXTURE_QUERY)
    # Scratch-schema-only constraint: the main_driver insert fails after the delete.
    with scratch.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE regime_conditional_brier_history "
                "ADD CONSTRAINT scratch_refuses_main_driver CHECK (signal_source <> 'main_driver')"
            )
        )

    summary = rcb.bootstrap_from_oracle_predictions(scratch, days=365)

    assert summary["replaced"] is False and summary["retired_count"] == 0
    assert _regime_keys(scratch) == {("tiny_lucky", 7, "CRISIS"), ("retired_feed", 30, "EXPANSION")}
    assert rcb.get_regime_conditional_scorecard(scratch, "tiny_lucky", 7, "CRISIS").conviction_weight == 1.5
