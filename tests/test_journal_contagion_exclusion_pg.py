"""PostgreSQL proof that a contagion trade ticket is never served as a regime state (#959, Codex hold on #1495).

write_ticket_to_journal puts a ticket in decision_journal with inferred_state CONTAGION_<shock> and
contradiction_flags.flow_thesis = 'contagion_derived'. The regime readers selected "the latest row" and
served it as the regime. The tag, not the CONTAGION_ state prefix, is what is excluded: regime subtypes
with the same name are genuine states.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest
from sqlalchemy import create_engine, text

from store.journal_contagion_exclusion import (
    NOT_CONTAGION_TICKET_JOURNAL_PREDICATE,
    excluding_contagion_tickets,
)
from store.journal_qualification import DECISION_JOURNAL, non_provisional_journal_select


@pytest.fixture
def journal_pg():
    db_url = os.environ.get("GRID_TEST_DB_URL")
    if not db_url:
        if os.environ.get("REQUIRE_JOURNAL_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the journal contagion-exclusion PostgreSQL proof")
        pytest.skip("GRID_TEST_DB_URL is not configured")
    admin = create_engine(db_url, pool_pre_ping=True)
    schema = f"jrnl_{uuid.uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(admin.url, pool_size=1, max_overflow=0,
                           connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE decision_journal (id serial PRIMARY KEY, decision_timestamp timestamptz, "
                "model_version_id integer, inferred_state text, state_confidence double precision, "
                "transition_probability double precision, contradiction_flags jsonb, "
                "grid_recommendation text, baseline_recommendation text, counterfactual text, "
                "operator_confidence text, verdict text, action_taken text)"
            ))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def _row(engine, hours_ago, state, flags, action="HOLD"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO decision_journal (decision_timestamp, model_version_id, inferred_state, "
            "state_confidence, transition_probability, contradiction_flags, action_taken) "
            "VALUES (now() - make_interval(hours => :h), 1, :s, 0.6, 0.1, CAST(:f AS jsonb), :a)"
        ), {"h": hours_ago, "s": state, "f": flags if isinstance(flags, str) else json.dumps(flags), "a": action})


def _latest_state(engine):
    with engine.connect() as conn:
        row = conn.execute(
            excluding_contagion_tickets(non_provisional_journal_select("inferred_state", "state_confidence"))
            .order_by(DECISION_JOURNAL.c.decision_timestamp.desc()).limit(1)
        ).fetchone()
    return row[0] if row else None


def _history_states(engine):
    with engine.connect() as conn:
        rows = conn.execute(excluding_contagion_tickets(text(
            "SELECT inferred_state FROM decision_journal WHERE "
            + NOT_CONTAGION_TICKET_JOURNAL_PREDICATE + " ORDER BY decision_timestamp"
        ))).fetchall()
    return [r[0] for r in rows]


def test_a_newer_contagion_ticket_row_is_not_the_current_regime(journal_pg):
    _row(journal_pg, 5, "FRAGILE", {"source": "regime"})
    _row(journal_pg, 1, "CONTAGION_SUPPLY_DISRUPTION", {"flow_thesis": "contagion_derived", "prediction_id": 3})
    assert _latest_state(journal_pg) == "FRAGILE"


@pytest.mark.parametrize("tag", ["contagion_derived", "CONTAGION_DERIVED", "  Contagion_Derived\t"])
def test_the_tag_is_normalized_like_the_other_structured_markers(journal_pg, tag):
    _row(journal_pg, 5, "NEUTRAL", {})
    _row(journal_pg, 1, "CONTAGION_PRICE_INCREASE", {"flow_thesis": tag})
    assert _latest_state(journal_pg) == "NEUTRAL"


def test_a_regime_subtype_with_the_same_name_but_no_tag_is_still_a_regime_state(journal_pg):
    _row(journal_pg, 5, "GROWTH", {})
    _row(journal_pg, 1, "CONTAGION_SUPPLY_DISRUPTION", {"cluster": 4})    # a genuine clustered regime subtype
    assert _latest_state(journal_pg) == "CONTAGION_SUPPLY_DISRUPTION"


@pytest.mark.parametrize("flags", ["null", "[]", '"a string"', "42", "true", '{"flow_thesis": null}', '{"flow_thesis": 7}'])
def test_odd_json_shapes_are_neither_errors_nor_matches(journal_pg, flags):
    _row(journal_pg, 5, "GROWTH", {})
    _row(journal_pg, 1, "FRAGILE", flags)
    assert _latest_state(journal_pg) == "FRAGILE"


def test_history_drops_only_the_tickets_and_keeps_provisional_evidence(journal_pg):
    _row(journal_pg, 6, "GROWTH", {})
    _row(journal_pg, 5, "CONTAGION_SUPPLY_DISRUPTION", {"flow_thesis": "contagion_derived"})
    _row(journal_pg, 4, "PROVISIONAL_FUNDAMENTAL_DIVERGENCE", {"signal_type": "fundamental_divergence"})
    _row(journal_pg, 3, "CONTAGION_SUPPLY_DISRUPTION", {"cluster": 4})
    _row(journal_pg, 2, "NEUTRAL", {})
    # History keeps the provisional evidence (it is raw by design) but never a trade ticket.
    assert _history_states(journal_pg) == [
        "GROWTH", "PROVISIONAL_FUNDAMENTAL_DIVERGENCE", "CONTAGION_SUPPLY_DISRUPTION", "NEUTRAL",
    ]
    # The current guard still skips the provisional row too.
    _row(journal_pg, 1, "PROVISIONAL_FUNDAMENTAL_DIVERGENCE", {"signal_type": "fundamental_divergence"})
    assert _latest_state(journal_pg) == "NEUTRAL"
