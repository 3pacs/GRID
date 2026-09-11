"""Regression tests for contracts.handlers.trust."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text

from contracts.handlers import trust
from contracts.schemas import EdgeValidated, SignalFired


@pytest.fixture
def trust_engine():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _register_greatest(dbapi_conn, _connection_record):
        dbapi_conn.create_function("GREATEST", 2, max)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE signal_sources (
                    id INTEGER PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    trust_score REAL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE supply_shock_attributions (
                    signal_source_id INTEGER NOT NULL,
                    edge_id INTEGER NOT NULL
                )
                """
            )
        )
    return engine


def _weak_edge(edge_id: int) -> EdgeValidated:
    return EdgeValidated(
        producer_module="intelligence.supply_chain_edge_validator",
        correlation_id=uuid4(),
        edge_id=edge_id,
        upstream_id="brent_crude",
        downstream_id="XOM",
        relationship="raw_material",
        validation_correlation=0.04,
        weak_since=datetime(2026, 3, 1, tzinfo=timezone.utc),
        relationship_weak=True,
        implied_pct_cogs=0.10,
    )


def _trust_scores(engine):
    with engine.begin() as conn:
        return dict(
            conn.execute(
                text("SELECT id, trust_score FROM signal_sources ORDER BY id")
            ).all()
        )


def _signal_fired(
    *,
    source: str = "congressional",
    signal_type: str = "BUY",
    strength: float = 0.42,
    ticker: str | None = "XOM",
) -> SignalFired:
    return SignalFired(
        producer_module="intelligence.signal_emitter",
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source=source,
        signal_type=signal_type,
        strength=strength,
        ticker=ticker,
        raw_row_ids=[101, 102],
    )


class _RegisterSignalSpy:
    """Captures the kwargs passed to ``register_signal`` for assertions."""

    def __init__(self, return_value: int | None = 7) -> None:
        self.calls: list[dict] = []
        self.return_value = return_value
        self.raise_on_call: Exception | None = None

    def __call__(self, _engine, **kwargs) -> int | None:
        self.calls.append(kwargs)
        if self.raise_on_call is not None:
            raise self.raise_on_call
        return self.return_value


@pytest.fixture
def register_signal_spy(monkeypatch):
    spy = _RegisterSignalSpy()
    monkeypatch.setattr(
        "intelligence.trust_scorer.register_signal", spy
    )
    return spy


@pytest.mark.parametrize(
    "signal_type, expected_direction",
    [
        ("BUY", "BUY"),
        ("buy", "BUY"),
        ("LONG", "BUY"),
        ("long", "BUY"),
        ("SELL", "SELL"),
        ("sell", "SELL"),
        ("SHORT", "SELL"),
        ("short", "SELL"),
    ],
)
def test_on_signal_fired_maps_known_signal_type_to_direction(
    signal_type, expected_direction, register_signal_spy
):
    evt = _signal_fired(signal_type=signal_type, strength=0.5)

    trust.on_signal_fired(evt, engine=object())

    assert len(register_signal_spy.calls) == 1
    call = register_signal_spy.calls[0]
    assert call["signal_type"] == expected_direction
    assert call["source_type"] == "congressional"
    assert call["source_id"] == signal_type
    assert call["ticker"] == "XOM"
    # signal_value is the price at signal time, fetched by register_signal;
    # the strength only ever travels in metadata.
    assert call["signal_value"] is None
    assert call["metadata"]["strength"] == pytest.approx(0.5)


def test_on_signal_fired_falls_back_to_positive_strength_for_unknown_type(
    register_signal_spy,
):
    evt = _signal_fired(signal_type="REGISTRY_HINT", strength=0.30)

    trust.on_signal_fired(evt, engine=object())

    assert len(register_signal_spy.calls) == 1
    assert register_signal_spy.calls[0]["signal_type"] == "BUY"
    assert register_signal_spy.calls[0]["signal_value"] is None
    assert register_signal_spy.calls[0]["metadata"]["strength"] == pytest.approx(0.30)


def test_on_signal_fired_falls_back_to_negative_strength_for_unknown_type(
    register_signal_spy,
):
    evt = _signal_fired(signal_type="REGISTRY_HINT", strength=-0.30)

    trust.on_signal_fired(evt, engine=object())

    assert len(register_signal_spy.calls) == 1
    assert register_signal_spy.calls[0]["signal_type"] == "SELL"
    assert register_signal_spy.calls[0]["signal_value"] is None
    # metadata carries the raw signed strength, not |strength|.
    assert register_signal_spy.calls[0]["metadata"]["strength"] == pytest.approx(-0.30)


def test_on_signal_fired_never_stores_strength_as_signal_value(
    register_signal_spy,
):
    """Regression: ``register_signal(signal_value=...)`` is the price at
    signal time and ``score_pending_signals`` reads it back as the entry
    price of the outcome return. The handler used to pass ``abs(strength)``
    there, so every BUY row it registered scored CORRECT (and every SELL row
    WRONG) against a 0-1 "close" whatever the market did. The stored
    signal_value must never be the strength: leave it ``None`` so the PIT
    close is auto-fetched, and carry the strength in metadata only."""
    evt = _signal_fired(signal_type="BUY", strength=0.42)

    trust.on_signal_fired(evt, engine=object())

    (call,) = register_signal_spy.calls
    assert call["signal_value"] is None
    assert call["metadata"]["strength"] == pytest.approx(0.42)


class _CaptureConn:
    """Capture-only DB connection for the real ``register_signal``.

    Records every ``execute(stmt, params)`` as ``(sql_text, params_dict)``
    and serves a row id for the ``INSERT ... RETURNING id``; the table
    setup statements are no-ops.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None):
        sql = getattr(stmt, "text", None) or str(stmt)
        self.calls.append((sql, dict(params or {})))
        result = MagicMock()
        result.fetchone.return_value = (
            (7,) if "INSERT INTO signal_sources" in sql else None
        )
        result.fetchall.return_value = []
        return result


def _capture_engine(conn: _CaptureConn) -> MagicMock:
    engine = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    engine.begin.return_value = cm
    engine.connect.return_value = cm
    return engine


def test_registered_row_binds_pit_close_not_strength_as_signal_value(
    monkeypatch,
):
    """End to end through the real ``register_signal``: the INSERT binds the
    point-in-time close as ``signal_value`` and the strength only inside the
    metadata JSON, so the scorer's entry price is a real close."""
    from intelligence import trust_scorer

    lookups: list[tuple[str, date]] = []

    def fake_close(_engine, ticker, target_date, *_args, **_kwargs):
        lookups.append((ticker, target_date))
        return 105.25

    monkeypatch.setattr(trust_scorer, "_get_price_near_date", fake_close)
    conn = _CaptureConn()
    evt = _signal_fired(source="holder_overlap", signal_type="BUY", strength=0.42)

    trust.on_signal_fired(evt, engine=_capture_engine(conn))

    inserts = [
        (sql, p) for sql, p in conn.calls if "INSERT INTO signal_sources" in sql
    ]
    assert len(inserts) == 1
    _sql, params = inserts[0]
    assert lookups and lookups[0][0] == "XOM"
    assert isinstance(lookups[0][1], date)
    assert params["p"] == pytest.approx(105.25)
    assert params["st"] == "holder_overlap"
    assert params["t"] == "XOM"
    assert params["d"] == "BUY"
    assert json.loads(params["m"])["strength"] == pytest.approx(0.42)


def test_on_signal_fired_skips_when_strength_is_zero_and_type_unknown(
    register_signal_spy,
):
    evt = _signal_fired(signal_type="REGISTRY_HINT", strength=0.0)

    trust.on_signal_fired(evt, engine=object())

    assert register_signal_spy.calls == []


def test_on_signal_fired_skips_when_ticker_missing(register_signal_spy):
    evt = _signal_fired(ticker=None)

    trust.on_signal_fired(evt, engine=object())

    assert register_signal_spy.calls == []


def test_on_signal_fired_skips_when_source_blank(register_signal_spy):
    evt = _signal_fired(source="")

    trust.on_signal_fired(evt, engine=object())

    assert register_signal_spy.calls == []


def test_on_signal_fired_swallows_register_signal_exceptions(
    register_signal_spy,
):
    register_signal_spy.raise_on_call = RuntimeError("db kaput")
    evt = _signal_fired()

    # Must not propagate; handler is best-effort.
    trust.on_signal_fired(evt, engine=object())

    assert len(register_signal_spy.calls) == 1


def test_on_edge_validated_skips_when_not_weak(trust_engine):
    # Insert a row that would otherwise be eligible.
    with trust_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO signal_sources (id, source_type, trust_score)
                VALUES (1, 'cross_lens_supply_shock', 0.80)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO supply_shock_attributions
                    (signal_source_id, edge_id)
                VALUES (1, 17)
                """
            )
        )

    strong_edge = EdgeValidated(
        producer_module="intelligence.supply_chain_edge_validator",
        correlation_id=uuid4(),
        edge_id=17,
        upstream_id="brent_crude",
        downstream_id="XOM",
        relationship="raw_material",
        validation_correlation=0.62,
        weak_since=None,
        relationship_weak=False,
        implied_pct_cogs=0.10,
    )

    trust.on_edge_validated(strong_edge, engine=trust_engine)

    assert _trust_scores(trust_engine) == {1: pytest.approx(0.80)}


def test_on_edge_validated_respects_min_trust_floor(trust_engine):
    # Trust starts below the no-op floor; multiplying by 0.75 would drop
    # below 0.05, so the GREATEST(:floor, ...) clamp must engage.
    with trust_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO signal_sources (id, source_type, trust_score)
                VALUES (1, 'cross_lens_supply_shock', 0.06)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO supply_shock_attributions
                    (signal_source_id, edge_id)
                VALUES (1, 17)
                """
            )
        )

    trust.on_edge_validated(_weak_edge(17), engine=trust_engine)

    # 0.06 * 0.75 = 0.045 → clamped to 0.05.
    assert _trust_scores(trust_engine) == {1: pytest.approx(0.05)}


def test_on_edge_validated_decays_only_production_cross_lens_supply_shock_sources(
    trust_engine,
):
    with trust_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO signal_sources (id, source_type, trust_score)
                VALUES
                    (1, 'cross_lens_supply_shock', 0.80),
                    (2, 'cross_lens', 0.80),
                    (3, 'cross_lens_supply_shock', 0.80),
                    (4, 'cross_lens_supply_shock', 0.80)
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO supply_shock_attributions (signal_source_id, edge_id)
                VALUES
                    (1, 17),
                    (2, 17),
                    (3, 99)
                """
            )
        )

    trust.on_edge_validated(_weak_edge(17), engine=trust_engine)

    assert _trust_scores(trust_engine) == {
        1: pytest.approx(0.60),
        2: pytest.approx(0.80),
        3: pytest.approx(0.80),
        4: pytest.approx(0.80),
    }
