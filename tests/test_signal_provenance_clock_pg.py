"""signal_provenance's shipping fudge-alert read stops at the decision clock, on PostgreSQL.

Codex HOLD 6030789643 on #1352: evidence that moves a decision-bearing number
must be bound by a timestamp at or before the prediction's clock, not just by
its date. ``_recent_fudge_alerts`` is the one provenance layer fenced that way
(``checked_at <= :clock``), so this test runs its real statement against
``cross_reference_checks`` rows on both sides of the clock: a same-hour late
row, a row exactly at the clock, a row past the look-back window and a row
with an assessment the layer ignores.

Runs in a scratch schema on ``pg_engine`` (``GRID_TEST_DB_URL``); skips
without PostgreSQL.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from intelligence import signal_provenance as sp

# The columns schema.sql declares for cross_reference_checks.
_DDL = """
CREATE TABLE cross_reference_checks (
    id                BIGSERIAL PRIMARY KEY,
    name              TEXT NOT NULL,
    category          TEXT NOT NULL,
    official_source   TEXT,
    official_value    DOUBLE PRECISION,
    physical_source   TEXT,
    physical_value    DOUBLE PRECISION,
    divergence_zscore DOUBLE PRECISION,
    assessment        TEXT,
    implication       TEXT,
    confidence        DOUBLE PRECISION,
    checked_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CLOCK = datetime(2026, 3, 5, 15, 30, tzinfo=timezone.utc)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"provenance_clock_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    # A non-UTC session zone, so a date-level or session-zone comparison
    # would show up as a wrong row set.
    engine = create_engine(
        pg_engine.url,
        connect_args={"options": f"-csearch_path={schema} -ctimezone=America/New_York"},
    )
    try:
        with engine.begin() as conn:
            conn.execute(text(_DDL))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _insert(engine: Engine, name: str, assessment: str, checked_at: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO cross_reference_checks "
                "(name, category, assessment, divergence_zscore, confidence, checked_at) "
                "VALUES (:n, 'shipping', :a, 2.5, 0.8, :t)"
            ),
            {"n": name, "a": assessment, "t": checked_at},
        )


def test_fudge_alerts_stop_at_the_decision_clock(scratch: Engine) -> None:
    _insert(scratch, "before", "major_divergence", CLOCK - timedelta(minutes=10))
    _insert(scratch, "at_clock", "contradiction", CLOCK)
    _insert(scratch, "late_same_hour", "major_divergence", CLOCK + timedelta(minutes=10))
    _insert(scratch, "late_same_day", "contradiction", CLOCK + timedelta(hours=5))
    _insert(scratch, "past_window", "major_divergence", CLOCK - timedelta(days=8))
    _insert(scratch, "not_a_divergence", "consistent", CLOCK - timedelta(minutes=1))

    alerts = sp._recent_fudge_alerts(scratch, ticker="XOM", clock=CLOCK)

    assert alerts is not None
    assert sorted(a["name"] for a in alerts) == ["at_clock", "before"]
    assert all(a["checked_at"] <= CLOCK for a in alerts)


def test_fudge_alerts_window_is_measured_back_from_the_clock(scratch: Engine) -> None:
    _insert(scratch, "inside", "major_divergence", CLOCK - timedelta(days=6, hours=23))
    _insert(scratch, "outside", "major_divergence", CLOCK - timedelta(days=7, minutes=1))

    alerts = sp._recent_fudge_alerts(scratch, ticker="XOM", clock=CLOCK)

    assert [a["name"] for a in (alerts or [])] == ["inside"]
