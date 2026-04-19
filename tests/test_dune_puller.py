"""Unit tests for ingestion/altdata/dune_puller.py."""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.dune_puller import (
    DunePuller,
    _NARRATIVE_TOP_N,
    _SMART_MONEY_TOP_N,
    _safe_name,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_engine(source_id: int = 7) -> tuple[MagicMock, MagicMock]:
    """Build a mock engine that returns source_id from source_catalog."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    return engine, conn


@pytest.fixture
def mock_engine():
    return _mock_engine(source_id=7)


@pytest.fixture
def puller(mock_engine):
    engine, conn = mock_engine
    p = DunePuller(
        db_engine=engine,
        api_key="test-dune-key",
        query_ids={
            "smart_money": 111,
            "cex_flow": 222,
            "narrative_heat": 333,
        },
    )
    return p, engine, conn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestSafeName:
    def test_lower_and_underscore(self) -> None:
        assert _safe_name("PEPE") == "pepe"
        assert _safe_name("$PEPE") == "pepe"
        assert _safe_name("Wrapped-BTC") == "wrapped_btc"
        assert _safe_name("stETH.v1") == "steth_v1"

    def test_empty_returns_unknown(self) -> None:
        assert _safe_name("") == "unknown"
        assert _safe_name("###") == "unknown"


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestInit:
    def test_source_name(self) -> None:
        assert DunePuller.SOURCE_NAME == "Dune_Analytics"

    def test_inherits_base_puller(self) -> None:
        from ingestion.base import BasePuller

        assert issubclass(DunePuller, BasePuller)

    def test_init_sets_source_id(self, puller) -> None:
        p, _, _ = puller
        assert p.source_id == 7

    def test_init_reads_config_when_no_override(self, mock_engine, monkeypatch) -> None:
        engine, _ = mock_engine
        import config as cfg

        monkeypatch.setattr(cfg.settings, "DUNE_API_KEY", "env-key", raising=False)
        monkeypatch.setattr(cfg.settings, "DUNE_QUERY_SMART_MONEY", 42, raising=False)
        monkeypatch.setattr(cfg.settings, "DUNE_QUERY_CEX_FLOW", 0, raising=False)
        monkeypatch.setattr(
            cfg.settings, "DUNE_QUERY_NARRATIVE_HEAT", 9, raising=False
        )

        p = DunePuller(db_engine=engine)
        assert p.api_key == "env-key"
        assert p.query_ids["smart_money"] == 42
        assert p.query_ids["narrative_heat"] == 9

    def test_series_id_with_and_without_name(self, puller) -> None:
        p, _, _ = puller
        assert p._series_id("smart_money", "PEPE") == "dune.smart_money.pepe"
        assert p._series_id("narrative_heat") == "dune.narrative_heat"


# ---------------------------------------------------------------------------
# Smart money leaderboard
# ---------------------------------------------------------------------------


class TestPullSmartMoney:
    def _rows(self, n: int = 3, token: str = "PEPE") -> list[dict[str, Any]]:
        return [
            {
                "wallet": f"0x{i:040x}",
                "token": token,
                "realized_pnl_usd": float((n - i) * 1000),
                "still_holding": bool(i % 2 == 0),
                "balance_usd": float((n - i) * 500),
            }
            for i in range(n)
        ]

    def test_skipped_when_no_api_key(self, mock_engine) -> None:
        engine, _ = mock_engine
        p = DunePuller(db_engine=engine, api_key="", query_ids={"smart_money": 1})
        result = p.pull_smart_money()
        assert result["status"] == "SKIPPED"
        assert result["rows_inserted"] == 0

    def test_skipped_when_query_id_missing(self, mock_engine) -> None:
        engine, _ = mock_engine
        p = DunePuller(
            db_engine=engine, api_key="k", query_ids={"smart_money": 0}
        )
        result = p.pull_smart_money()
        assert result["status"] == "SKIPPED"

    def test_success_insert_one_leaderboard_per_token(self, puller) -> None:
        p, _, conn = puller
        conn.execute.return_value.fetchone.return_value = None  # _row_exists False

        rows = self._rows(n=3, token="PEPE")
        with patch.object(p, "_fetch_query_rows", return_value=rows):
            result = p.pull_smart_money()

        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 1  # one series for token "PEPE"
        assert result["leaderboard_size"] == 3

    def test_respects_top_n_cap(self, puller) -> None:
        p, _, conn = puller
        conn.execute.return_value.fetchone.return_value = None

        rows = self._rows(n=_SMART_MONEY_TOP_N + 10, token="PEPE")
        with patch.object(p, "_fetch_query_rows", return_value=rows):
            result = p.pull_smart_money()

        assert result["leaderboard_size"] == _SMART_MONEY_TOP_N

    def test_empty_rows_returns_success_zero(self, puller) -> None:
        p, _, _ = puller
        with patch.object(p, "_fetch_query_rows", return_value=[]):
            result = p.pull_smart_money()
        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 0

    def test_fetch_failure_returns_failed(self, puller) -> None:
        p, _, _ = puller
        with patch.object(
            p, "_fetch_query_rows", side_effect=RuntimeError("boom")
        ):
            result = p.pull_smart_money()
        assert result["status"] == "FAILED"
        assert "boom" in result["error"]

    def test_token_argument_forces_fresh_execution(self, puller) -> None:
        p, _, conn = puller
        conn.execute.return_value.fetchone.return_value = None
        fake = MagicMock(return_value=self._rows(n=2, token="ABC"))
        with patch.object(p, "_fetch_query_rows", fake):
            p.pull_smart_money(token="ABC", lookback_days=42)
        # When a token is passed, we expect a fresh execution (use_cache=False)
        _, kwargs = fake.call_args
        assert kwargs["use_cache"] is False
        assert kwargs["parameters"] == {"token": "ABC", "days": 42}


# ---------------------------------------------------------------------------
# CEX flow
# ---------------------------------------------------------------------------


class TestPullCEXFlows:
    def _rows(self) -> list[dict[str, Any]]:
        return [
            {
                "token": "PEPE",
                "inflow_usd": 100.0,
                "outflow_usd": 250.0,
                "net_usd": 150.0,
                "exchange_count": 5,
            },
            {
                "token": "WIF",
                "inflow_usd": 900.0,
                "outflow_usd": 400.0,
                "net_usd": -500.0,
                "exchange_count": 8,
            },
        ]

    def test_skipped_when_not_configured(self, mock_engine) -> None:
        engine, _ = mock_engine
        p = DunePuller(db_engine=engine, api_key="", query_ids={"cex_flow": 0})
        assert p.pull_cex_flows()["status"] == "SKIPPED"

    def test_success_inserts_per_token(self, puller) -> None:
        p, _, conn = puller
        conn.execute.return_value.fetchone.return_value = None
        with patch.object(p, "_fetch_query_rows", return_value=self._rows()):
            result = p.pull_cex_flows()
        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 2
        assert set(result["tokens_seen"]) == {"PEPE", "WIF"}

    def test_skips_nonnumeric_net(self, puller) -> None:
        p, _, conn = puller
        conn.execute.return_value.fetchone.return_value = None
        bad = [{"token": "FOO", "net_usd": "not-a-number"}]
        with patch.object(p, "_fetch_query_rows", return_value=bad):
            result = p.pull_cex_flows()
        assert result["rows_inserted"] == 0


# ---------------------------------------------------------------------------
# Narrative heat
# ---------------------------------------------------------------------------


class TestPullNarrativeHeat:
    def _rows(self, n: int = 12) -> list[dict[str, Any]]:
        return [
            {
                "token": f"T{i}",
                "new_holders": 100 + i,
                "prior_holders": 1000,
                "pct_change": float(n - i) / 10.0,
            }
            for i in range(n)
        ]

    def test_skipped_when_not_configured(self, mock_engine) -> None:
        engine, _ = mock_engine
        p = DunePuller(
            db_engine=engine, api_key="", query_ids={"narrative_heat": 0}
        )
        assert p.pull_narrative_heat()["status"] == "SKIPPED"

    def test_inserts_per_token_plus_snapshot(self, puller) -> None:
        p, _, conn = puller
        conn.execute.return_value.fetchone.return_value = None

        rows = self._rows(n=12)
        with patch.object(p, "_fetch_query_rows", return_value=rows):
            result = p.pull_narrative_heat()

        assert result["status"] == "SUCCESS"
        # 12 per-token rows + 1 aggregate snapshot
        assert result["rows_inserted"] == 13
        assert len(result["top_tokens"]) == _NARRATIVE_TOP_N
        # Top list should be sorted descending by pct_change
        assert result["top_tokens"][0] == "T0"

    def test_empty_rows_returns_success_zero(self, puller) -> None:
        p, _, _ = puller
        with patch.object(p, "_fetch_query_rows", return_value=[]):
            result = p.pull_narrative_heat()
        assert result["status"] == "SUCCESS"
        assert result["rows_inserted"] == 0


# ---------------------------------------------------------------------------
# pull_all
# ---------------------------------------------------------------------------


class TestPullAll:
    def test_runs_all_three(self, puller) -> None:
        p, _, _ = puller
        with (
            patch.object(p, "pull_smart_money", return_value={"status": "SUCCESS", "rows_inserted": 1}) as sm,
            patch.object(p, "pull_cex_flows", return_value={"status": "SUCCESS", "rows_inserted": 2}) as cf,
            patch.object(p, "pull_narrative_heat", return_value={"status": "SUCCESS", "rows_inserted": 3}) as nh,
        ):
            results = p.pull_all()

        assert sm.called
        assert cf.called
        assert nh.called
        assert [r["source"] for r in results] == [
            "smart_money",
            "cex_flow",
            "narrative_heat",
        ]
        assert sum(r["rows_inserted"] for r in results) == 6
