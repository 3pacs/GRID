"""Regression tests for contracts.handlers.trust."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text

from contracts.handlers import trust
from contracts.schemas import EdgeValidated


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
