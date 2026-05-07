"""Unit tests for intelligence/dune_smart_money.py."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock


from intelligence.dune_smart_money import (
    cex_flow_balance,
    narrative_heat,
    smart_money_leaderboard,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_engine(fetchone_return=None, fetchall_return=None) -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = fetchone_return
    conn.execute.return_value.fetchall.return_value = fetchall_return or []
    return engine, conn


# ---------------------------------------------------------------------------
# smart_money_leaderboard
# ---------------------------------------------------------------------------


class TestSmartMoneyLeaderboard:
    def test_no_snapshot_returns_empty(self) -> None:
        engine, _ = _mock_engine(fetchone_return=None)
        result = smart_money_leaderboard(engine, "PEPE")
        assert result["token"] == "PEPE"
        assert result["leaderboard"] == []
        assert result["wallet_count"] == 0

    def test_strips_dollar_sign_and_uppercases(self) -> None:
        engine, _ = _mock_engine(fetchone_return=None)
        result = smart_money_leaderboard(engine, "$pepe")
        assert result["token"] == "PEPE"

    def test_sorts_by_pnl_descending(self) -> None:
        payload = {
            "lookback_days": 30,
            "wallets": [
                {"wallet": "0xA", "realized_pnl_usd": 100.0, "still_holding": True, "balance_usd": 50.0},
                {"wallet": "0xB", "realized_pnl_usd": 500.0, "still_holding": False, "balance_usd": 0.0},
                {"wallet": "0xC", "realized_pnl_usd": 300.0, "still_holding": True, "balance_usd": 10.0},
            ],
        }
        engine, _ = _mock_engine(fetchone_return=(date(2026, 4, 19), payload))
        result = smart_money_leaderboard(engine, "PEPE")
        wallets = [w["wallet"] for w in result["leaderboard"]]
        assert wallets == ["0xB", "0xC", "0xA"]
        assert result["still_holding"] == 2
        assert result["wallet_count"] == 3
        assert result["as_of"] == "2026-04-19"

    def test_handles_json_string_payload(self) -> None:
        payload = {"wallets": [{"wallet": "0xA", "realized_pnl_usd": 1.0}]}
        engine, _ = _mock_engine(
            fetchone_return=(date(2026, 4, 19), json.dumps(payload))
        )
        result = smart_money_leaderboard(engine, "PEPE")
        assert result["wallet_count"] == 1


# ---------------------------------------------------------------------------
# cex_flow_balance
# ---------------------------------------------------------------------------


class TestCEXFlowBalance:
    def test_missing_returns_unknown_direction(self) -> None:
        engine, _ = _mock_engine(fetchone_return=None)
        result = cex_flow_balance(engine, "PEPE")
        assert result["direction"] == "unknown"
        assert result["net_usd"] == 0.0

    def test_positive_net_is_accumulation(self) -> None:
        payload = {
            "inflow_usd": 100.0,
            "outflow_usd": 250.0,
            "net_usd": 150.0,
            "exchange_count": 5,
            "direction": "accumulation",
            "lookback_days": 14,
        }
        engine, _ = _mock_engine(fetchone_return=(date(2026, 4, 19), payload))
        result = cex_flow_balance(engine, "PEPE")
        assert result["direction"] == "accumulation"
        assert result["net_usd"] == 150.0
        assert result["lookback_days"] == 14

    def test_infers_direction_when_missing_field(self) -> None:
        payload = {"net_usd": -300.0}
        engine, _ = _mock_engine(fetchone_return=(date(2026, 4, 19), payload))
        result = cex_flow_balance(engine, "WIF")
        assert result["direction"] == "distribution"


# ---------------------------------------------------------------------------
# narrative_heat
# ---------------------------------------------------------------------------


class TestNarrativeHeat:
    def test_reads_aggregate_snapshot(self) -> None:
        payload = {
            "top": [
                {"token": "PEPE", "new_holders": 100, "pct_change": 0.8},
                {"token": "WIF", "new_holders": 50, "pct_change": 0.4},
                {"token": "BONK", "new_holders": 25, "pct_change": 0.9},
            ]
        }
        engine, _ = _mock_engine(fetchone_return=(date(2026, 4, 19), payload))
        result = narrative_heat(engine, limit=10)
        assert result["as_of"] == "2026-04-19"
        # Sorted descending by pct_change
        assert [t["token"] for t in result["tokens"]] == ["BONK", "PEPE", "WIF"]

    def test_limit_trims_list(self) -> None:
        payload = {
            "top": [
                {"token": f"T{i}", "pct_change": float(i)} for i in range(20)
            ]
        }
        engine, _ = _mock_engine(fetchone_return=(date(2026, 4, 19), payload))
        result = narrative_heat(engine, limit=5)
        assert len(result["tokens"]) == 5
        assert result["tokens"][0]["token"] == "T19"

    def test_fallback_uses_per_token_rows(self) -> None:
        # aggregate missing -> fall back to per-token scan
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # First call: aggregate fetchone returns None
        # Second call: fetchall returns per-token rows
        conn.execute.return_value.fetchone.return_value = None
        conn.execute.return_value.fetchall.return_value = [
            ("dune.holder_growth.pepe", date(2026, 4, 19), 0.5, {"token": "PEPE", "new_holders": 10}),
            ("dune.holder_growth.wif", date(2026, 4, 19), 0.9, {"token": "WIF", "new_holders": 30}),
        ]

        result = narrative_heat(engine, limit=5)
        assert [t["token"] for t in result["tokens"]] == ["WIF", "PEPE"]
