"""Tests for ``intelligence/security_master.py`` (GD1).

Two layers:
  * pure-function tests (entity id construction, the sector-weight tie-break
    proposal) — no DB, no network;
  * resolver tests against an in-memory SQLite DB with a hand-adapted schema
    (the ``test_fundamental_divergence_sec_priority.py`` pattern: the SELECT
    logic under test is byte-for-byte the production SQL from
    ``resolve_entity``/``resolve_primary_sector``; the only portability tweak
    is dropping Postgres-only types (``JSONB``, ``BIGSERIAL``,
    ``TIMESTAMPTZ``) and the ``REFERENCES`` FK, since SQLite doesn't have
    them and this layer only exercises the SELECT, not the DDL).
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text

from intelligence import security_master as sm


# ── entity id construction ─────────────────────────────────────────────────


def test_entity_id_for_cik_pads_to_ten_digits():
    assert sm.entity_id_for_cik(1364742) == "sm_0001364742"
    assert sm.entity_id_for_cik("0001364742") == "sm_0001364742"
    assert sm.entity_id_for_cik(" 320193 ") == "sm_0000320193"


def test_entity_id_for_cik_rejects_non_numeric():
    with pytest.raises(ValueError):
        sm.entity_id_for_cik("not-a-cik")


def test_entity_id_for_ticker_upcases_and_strips():
    assert sm.entity_id_for_ticker(" aapl ") == "sm_tkr_AAPL"


# ── compute_sector_weights / propose_primary_sector ────────────────────────


_SYNTHETIC_MAP = {
    "Technology": {
        "subsectors": {
            "Semiconductors": {
                "weight": 0.05,
                "actors": [
                    {"name": "NVIDIA", "ticker": "NVDA", "weight": 0.18, "type": "company"},
                ],
            },
        },
    },
    "Communication Services": {
        "subsectors": {
            "Internet": {
                "weight": 0.10,
                "actors": [
                    {"name": "Alphabet", "ticker": "GOOGL", "weight": 0.20, "type": "company"},
                ],
            },
        },
    },
    "Consumer Discretionary": {
        "subsectors": {
            "Ecommerce": {
                "weight": 0.10,
                "actors": [
                    {"name": "Alphabet", "ticker": "GOOGL", "weight": 0.20, "type": "company"},
                ],
            },
        },
    },
}


def test_compute_sector_weights_single_sector():
    weights = sm.compute_sector_weights(_SYNTHETIC_MAP, "NVDA")
    assert weights == {"Technology": pytest.approx(0.05 * 0.18)}


def test_compute_sector_weights_ignores_non_company_actors():
    sector_map = {
        "Technology": {
            "subsectors": {
                "X": {"weight": 1.0, "actors": [{"ticker": "NVDA", "weight": 1.0, "type": "person"}]},
            },
        },
    }
    assert sm.compute_sector_weights(sector_map, "NVDA") == {}


def test_compute_sector_weights_multi_sector_equal_weight_is_a_tie():
    # GOOGL is tagged under two sectors in the synthetic map with identical
    # subsector_weight * actor_weight products (0.10 * 0.20 each).
    weights = sm.compute_sector_weights(_SYNTHETIC_MAP, "GOOGL")
    assert set(weights) == {"Communication Services", "Consumer Discretionary"}
    assert weights["Communication Services"] == pytest.approx(weights["Consumer Discretionary"])


def test_propose_primary_sector_single_sector_is_never_flagged_conflict():
    proposed, is_multi = sm.propose_primary_sector({"Technology": 0.009})
    assert proposed == "Technology"
    assert is_multi is False


def test_propose_primary_sector_picks_max_weight():
    proposed, is_multi = sm.propose_primary_sector(
        {"Technology": 0.05, "Communication Services": 0.09}
    )
    assert proposed == "Communication Services"
    assert is_multi is True


def test_propose_primary_sector_tie_breaks_alphabetically_and_flags_conflict():
    proposed, is_multi = sm.propose_primary_sector(
        {"Consumer Discretionary": 0.02, "Communication Services": 0.02}
    )
    assert proposed == "Communication Services"  # alphabetically first of the tie
    assert is_multi is True


def test_propose_primary_sector_empty_weights():
    assert sm.propose_primary_sector({}) == (None, False)


# ── resolver against an in-memory SQLite DB ────────────────────────────────


_SQLITE_IDENTIFIERS_DDL = """
CREATE TABLE security_identifiers (
    id               INTEGER PRIMARY KEY,
    entity_id        TEXT NOT NULL,
    id_scheme        TEXT NOT NULL,
    id_value         TEXT NOT NULL,
    valid_from       DATE NOT NULL,
    valid_to         DATE,
    is_primary       BOOLEAN NOT NULL DEFAULT 1,
    source           TEXT NOT NULL,
    conflict_flag    BOOLEAN NOT NULL DEFAULT 0
)
"""

_SQLITE_SECTOR_MEMBERSHIP_DDL = """
CREATE TABLE security_sector_membership (
    id                INTEGER PRIMARY KEY,
    entity_id         TEXT NOT NULL,
    taxonomy          TEXT NOT NULL DEFAULT 'sector_map_v1',
    sector            TEXT NOT NULL,
    is_primary        BOOLEAN NOT NULL DEFAULT 1,
    valid_from        DATE NOT NULL,
    valid_to          DATE
)
"""


@pytest.fixture
def sqlite_engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(_SQLITE_IDENTIFIERS_DDL))
        conn.execute(text(_SQLITE_SECTOR_MEMBERSHIP_DDL))
    return eng


def _insert_identifier(conn, **row):
    defaults = {"is_primary": True, "conflict_flag": False, "valid_to": None}
    defaults.update(row)
    conn.execute(
        text(
            "INSERT INTO security_identifiers "
            "(entity_id, id_scheme, id_value, valid_from, valid_to, is_primary, source, conflict_flag) "
            "VALUES (:entity_id, :id_scheme, :id_value, :valid_from, :valid_to, :is_primary, :source, :conflict_flag)"
        ),
        defaults,
    )


def _insert_sector(conn, **row):
    defaults = {"is_primary": True, "valid_to": None, "taxonomy": sm.DEFAULT_TAXONOMY}
    defaults.update(row)
    conn.execute(
        text(
            "INSERT INTO security_sector_membership "
            "(entity_id, taxonomy, sector, is_primary, valid_from, valid_to) "
            "VALUES (:entity_id, :taxonomy, :sector, :is_primary, :valid_from, :valid_to)"
        ),
        defaults,
    )


def test_resolve_entity_finds_current_ticker(sqlite_engine):
    with sqlite_engine.begin() as conn:
        _insert_identifier(
            conn, entity_id="sm_0000320193", id_scheme="ticker", id_value="AAPL",
            valid_from="2020-01-01", source="sector_map",
        )
    entity_id = sm.resolve_entity(sqlite_engine, "ticker", "AAPL", as_of=date(2026, 9, 27))
    assert entity_id == "sm_0000320193"


def test_resolve_entity_respects_point_in_time_valid_to():
    """A ticker re-used by a different entity after the first one's window
    closed must resolve to whichever entity's window actually covers as_of —
    this is the ticker-change/reuse case GD0 §2 flagged as unhandled today.
    """
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(_SQLITE_IDENTIFIERS_DDL))
        _insert_identifier(
            conn, entity_id="sm_old_company", id_scheme="ticker", id_value="ABCD",
            valid_from="2010-01-01", valid_to="2015-12-31", source="sector_map",
        )
        _insert_identifier(
            conn, entity_id="sm_new_company", id_scheme="ticker", id_value="ABCD",
            valid_from="2016-01-01", source="sector_map",
        )
    assert sm.resolve_entity(eng, "ticker", "ABCD", as_of=date(2012, 6, 1)) == "sm_old_company"
    assert sm.resolve_entity(eng, "ticker", "ABCD", as_of=date(2026, 9, 27)) == "sm_new_company"


def test_resolve_entity_never_returns_a_row_from_the_future(sqlite_engine):
    """Anti-look-ahead: a row whose valid_from is after as_of must never be
    returned, mirroring the PIT guard the plan requires everywhere else
    (``known_at`` in §2.1, the GD5 anti-look-ahead test)."""
    with sqlite_engine.begin() as conn:
        _insert_identifier(
            conn, entity_id="sm_future_company", id_scheme="ticker", id_value="ZZZZ",
            valid_from="2030-01-01", source="sector_map",
        )
    assert sm.resolve_entity(sqlite_engine, "ticker", "ZZZZ", as_of=date(2026, 9, 27)) is None


def test_resolve_entity_conflict_prefers_primary_row(sqlite_engine):
    """Two entities claiming the same (scheme, value) as of the same date is
    a genuine identifier conflict (GD0 §1.2's actor_connections.actor_b
    collision is the same shape). The resolver must not error or guess — it
    deterministically prefers the row marked primary."""
    with sqlite_engine.begin() as conn:
        _insert_identifier(
            conn, entity_id="sm_secondary_claim", id_scheme="ticker", id_value="DUP",
            valid_from="2020-01-01", source="sector_map", is_primary=False, conflict_flag=True,
        )
        _insert_identifier(
            conn, entity_id="sm_primary_claim", id_scheme="ticker", id_value="DUP",
            valid_from="2020-01-01", source="sec_company_tickers", is_primary=True, conflict_flag=True,
        )
    assert sm.resolve_entity(sqlite_engine, "ticker", "DUP", as_of=date(2026, 9, 27)) == "sm_primary_claim"


def test_resolve_entity_unknown_scheme_returns_none(sqlite_engine):
    assert sm.resolve_entity(sqlite_engine, "ticker", "NOPE", as_of=date(2026, 9, 27)) is None


def test_resolve_primary_sector_returns_the_flagged_primary(sqlite_engine):
    with sqlite_engine.begin() as conn:
        _insert_sector(conn, entity_id="sm_googl", sector="Communication Services", is_primary=True, valid_from="2026-01-01")
        _insert_sector(conn, entity_id="sm_googl", sector="Technology", is_primary=False, valid_from="2026-01-01")
    assert sm.resolve_primary_sector(sqlite_engine, "sm_googl", as_of=date(2026, 9, 27)) == "Communication Services"


def test_resolve_primary_sector_none_when_no_membership(sqlite_engine):
    assert sm.resolve_primary_sector(sqlite_engine, "sm_nobody", as_of=date(2026, 9, 27)) is None


# ── ensure_tables — DDL is idempotent and importable (no live Postgres here) ─


def test_ddl_constants_are_nonempty_and_mention_all_three_tables():
    assert "CREATE TABLE IF NOT EXISTS security_master" in sm._CREATE_SECURITY_MASTER_SQL
    assert "CREATE TABLE IF NOT EXISTS security_identifiers" in sm._CREATE_SECURITY_IDENTIFIERS_SQL
    assert "REFERENCES security_master(entity_id)" in sm._CREATE_SECURITY_IDENTIFIERS_SQL
    assert "CREATE TABLE IF NOT EXISTS security_sector_membership" in sm._CREATE_SECURITY_SECTOR_MEMBERSHIP_SQL
    assert "REFERENCES security_master(entity_id)" in sm._CREATE_SECURITY_SECTOR_MEMBERSHIP_SQL
