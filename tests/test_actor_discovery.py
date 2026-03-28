"""
Tests for GRID automated actor discovery and enrichment system.

Tests the actor ID generation, normalization helpers, discovery logic
for each source type, connection discovery, enrichment, and the
orchestration functions with mocked database results.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from intelligence.actor_discovery import (
    _normalize_name,
    _actor_id_insider,
    _actor_id_congress,
    _actor_id_13f,
    _actor_id_lobbyist,
    _actor_id_gov_official,
    _upsert_actor,
    _upsert_connection,
    _SEED_13F_FILERS,
    get_actor_stats,
)


# ══════════════════════════════════════════════════════════════════════════
# Name normalization tests
# ══════════════════════════════════════════════════════════════════════════


class TestNormalizeName:
    """Test the name normalization helper."""

    def test_basic_name(self):
        assert _normalize_name("John Smith") == "john_smith"

    def test_strips_whitespace(self):
        assert _normalize_name("  Jane Doe  ") == "jane_doe"

    def test_removes_suffixes(self):
        assert _normalize_name("James Brown Jr.") == "james_brown"
        assert _normalize_name("Robert Johnson III") == "robert_johnson"

    def test_removes_special_chars(self):
        assert _normalize_name("O'Brien, Mary-Jane") == "obrien_maryjane"

    def test_empty_string(self):
        assert _normalize_name("") == ""

    def test_all_special_chars(self):
        assert _normalize_name("!!!") == ""


# ══════════════════════════════════════════════════════════════════════════
# Actor ID generation tests
# ══════════════════════════════════════════════════════════════════════════


class TestActorIdGeneration:
    """Test the actor ID prefix functions."""

    def test_insider_id(self):
        assert _actor_id_insider("Tim Cook") == "insider_tim_cook"

    def test_congress_id(self):
        assert _actor_id_congress("Nancy Pelosi") == "congress_nancy_pelosi"

    def test_13f_id_strips_leading_zeros(self):
        assert _actor_id_13f("0001067983") == "inst_13f_1067983"

    def test_13f_id_all_zeros(self):
        assert _actor_id_13f("0000000000") == "inst_13f_0"

    def test_lobbyist_id(self):
        assert _actor_id_lobbyist("K Street Partners") == "lobbyist_k_street_partners"

    def test_gov_official_id(self):
        assert _actor_id_gov_official("Department of Defense") == "gov_department_of_defense"


# ══════════════════════════════════════════════════════════════════════════
# Seed data integrity
# ══════════════════════════════════════════════════════════════════════════


class TestSeedData:
    """Verify the seed 13F filer list is well-formed."""

    def test_seed_filers_non_empty(self):
        assert len(_SEED_13F_FILERS) >= 20

    def test_seed_filers_have_required_fields(self):
        for cik, info in _SEED_13F_FILERS.items():
            assert "name" in info, f"CIK {cik} missing name"
            assert "aum_est" in info, f"CIK {cik} missing aum_est"
            assert info["aum_est"] > 0, f"CIK {cik} has non-positive AUM"

    def test_seed_filer_ciks_are_valid_format(self):
        for cik in _SEED_13F_FILERS:
            assert cik.isdigit(), f"CIK {cik} is not all digits"
            assert len(cik) == 10, f"CIK {cik} is not 10 chars"


# ══════════════════════════════════════════════════════════════════════════
# Upsert tests with mocked DB
# ══════════════════════════════════════════════════════════════════════════


class TestUpsertActor:
    """Test the _upsert_actor helper with a mocked connection."""

    def test_upsert_returns_true_for_insert(self):
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (True,)
        mock_conn.execute.return_value = mock_result

        result = _upsert_actor(
            conn=mock_conn,
            actor_id="insider_test",
            name="Test Person",
            tier="individual",
            category="insider",
            title="CEO",
        )
        assert result is True
        mock_conn.execute.assert_called_once()

    def test_upsert_returns_false_for_update(self):
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (False,)
        mock_conn.execute.return_value = mock_result

        result = _upsert_actor(
            conn=mock_conn,
            actor_id="insider_test",
            name="Test Person",
            tier="individual",
            category="insider",
        )
        assert result is False

    def test_upsert_passes_correct_params(self):
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (True,)
        mock_conn.execute.return_value = mock_result

        _upsert_actor(
            conn=mock_conn,
            actor_id="inst_13f_123",
            name="Big Fund",
            tier="institutional",
            category="fund",
            title="Hedge Fund",
            influence_score=0.75,
            aum=1_000_000_000,
            data_sources=["sec_13f"],
            credibility="hard_data",
            motivation_model="alpha_seeking",
            metadata={"cik": "123"},
        )

        args = mock_conn.execute.call_args
        params = args[0][1] if len(args[0]) > 1 else args[1]
        assert params["id"] == "inst_13f_123"
        assert params["name"] == "Big Fund"
        assert params["tier"] == "institutional"
        assert params["inf"] == 0.75
        assert params["aum"] == 1_000_000_000


class TestUpsertConnection:
    """Test the _upsert_connection helper."""

    def test_canonical_ordering(self):
        """Connections should be stored with actor_a < actor_b."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (True,)
        mock_conn.execute.return_value = mock_result

        _upsert_connection(
            conn=mock_conn,
            actor_a="zzz_actor",
            actor_b="aaa_actor",
            relationship="co_traded",
            strength=0.6,
        )

        args = mock_conn.execute.call_args
        params = args[0][1] if len(args[0]) > 1 else args[1]
        # Should be canonically ordered
        assert params["a"] == "aaa_actor"
        assert params["b"] == "zzz_actor"

    def test_returns_true_for_new_connection(self):
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (True,)
        mock_conn.execute.return_value = mock_result

        result = _upsert_connection(
            conn=mock_conn,
            actor_a="actor_a",
            actor_b="actor_b",
            relationship="board_member",
        )
        assert result is True


# ══════════════════════════════════════════════════════════════════════════
# Stats tests
# ══════════════════════════════════════════════════════════════════════════


class TestGetActorStats:
    """Test get_actor_stats with mocked database."""

    def test_returns_phase_pre1_for_small_count(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Mock sequential execute calls
        mock_conn.execute.side_effect = [
            # COUNT(*) FROM actors
            MagicMock(fetchone=MagicMock(return_value=(50,))),
            # tier breakdown
            MagicMock(fetchall=MagicMock(return_value=[
                ("individual", 30), ("institutional", 15), ("sovereign", 5),
            ])),
            # category breakdown
            MagicMock(fetchall=MagicMock(return_value=[
                ("insider", 25), ("fund", 15), ("central_bank", 5), ("politician", 5),
            ])),
            # connections count
            MagicMock(fetchone=MagicMock(return_value=(10,))),
            # connection types
            MagicMock(fetchall=MagicMock(return_value=[("co_traded", 8), ("board_member", 2)])),
            # enriched count
            MagicMock(fetchone=MagicMock(return_value=(20,))),
            # avg influence/trust
            MagicMock(fetchone=MagicMock(return_value=(0.55, 0.48))),
        ]

        stats = get_actor_stats(mock_engine)

        assert stats["total_actors"] == 50
        assert "Pre-Phase 1" in stats["phase"]
        assert stats["by_tier"]["individual"] == 30
        assert stats["total_connections"] == 10
        assert stats["enrichment_pct"] == 40.0

    def test_handles_db_error_gracefully(self):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("DB down")

        stats = get_actor_stats(mock_engine)
        assert "error" in stats
