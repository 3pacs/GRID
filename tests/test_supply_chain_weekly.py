"""3pacs/GRID#1179: graph node symbols, the 10-K parser's weekly pass and the stale-graph alert."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from alerts import supply_chain_alerts as sca
from ingestion.altdata import supply_chain_parser as scp
from intelligence.contagion_backtest import _iter_impact_tickers
from intelligence.supply_chain_symbols import (
    DELISTED,
    NODE_SYMBOLS,
    load_node_types,
    symbol_for,
)

# --- node -> symbol ---------------------------------------------------------------


@pytest.mark.parametrize("node, typ, symbol", [
    ("tsmc", "ticker", "TSM"),
    ("intel", "ticker", "INTC"),
    ("micron", "ticker", "MU"),
    ("SQ", "ticker", "XYZ"),
    ("bf_b", "ticker", "BF-B"),
    ("aapl", "ticker", "AAPL"),
    ("HSY", None, "HSY"),
])
def test_listed_nodes_map_to_their_symbol(node, typ, symbol):
    assert symbol_for(node, typ).symbol == symbol


@pytest.mark.parametrize("node, typ, reason", [
    ("openai", "private_company", "type:private_company"),
    ("cargill", "private_company", "type:private_company"),
    ("corn", "commodity", "type:commodity"),
    ("cowos_packaging", "commodity", "type:commodity"),
    ("005930.ks", "ticker", "foreign listing"),
    ("2317.tw", "ticker", "foreign listing"),
    ("ifx.de", "ticker", "foreign listing"),
    ("pxd", "ticker", "delisted: Pioneer, acquired by ExxonMobil (2024)"),
    ("foxconn", "ticker", "not a US symbol"),
    ("air_liquide", "ticker", "not a US symbol"),
    ("OPENAI", None, "not a US symbol"),
    ("lindt", "ticker", "foreign listing"),
    ("trina", "ticker", "foreign listing"),
    ("cyrk", "ticker", "no listing found"),
])
def test_untradable_nodes_carry_a_reason(node, typ, reason):
    t = symbol_for(node, typ)
    assert t.symbol is None and t.reason == reason


def test_without_types_a_bare_commodity_id_looks_like_a_symbol():
    # documents why callers load node types: "corn" alone is also the CORN fund
    assert symbol_for("corn").symbol == "CORN"
    assert symbol_for("corn", "commodity").symbol is None


def test_aliases_and_delisted_never_overlap_and_are_us_symbols():
    from intelligence.supply_chain_symbols import FOREIGN_SLUGS, UNKNOWN_LISTING

    assert not set(NODE_SYMBOLS) & set(DELISTED)
    assert not (set(NODE_SYMBOLS) | set(DELISTED)) & (set(FOREIGN_SLUGS) | UNKNOWN_LISTING)
    assert all(symbol_for(k, "ticker").symbol == v for k, v in NODE_SYMBOLS.items())


def test_load_node_types_lowercases_and_survives_errors():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [("SQ", "ticker"), ("corn", "commodity")]
    assert load_node_types(conn, ["SQ", "corn", None]) == {"sq": "ticker", "corn": "commodity"}
    assert conn.execute.call_args.args[1] == {"ids": ["sq", "corn"]}
    conn.execute.side_effect = RuntimeError("no table")
    assert load_node_types(conn, ["x"]) == {}
    assert load_node_types(MagicMock(), []) == {}


def test_backtest_scores_symbols_not_node_ids():
    ranked = [
        {"id": "tsmc", "margin_impact_pct": -0.05},
        {"id": "openai", "margin_impact_pct": -0.04},
        {"id": "corn", "margin_impact_pct": -0.03},
        {"id": "005930.ks", "margin_impact_pct": -0.02},
        {"id": "aapl", "margin_impact_pct": -0.01},
    ]
    types = {"tsmc": "ticker", "openai": "private_company", "corn": "commodity",
             "005930.ks": "ticker", "aapl": "ticker"}
    assert [t[0] for t in _iter_impact_tickers(ranked, node_types=types)] == ["TSM", "AAPL"]


# --- 10-K parser: persistent checkpoint and rolling pass ----------------------------


@pytest.fixture
def parser(tmp_path, monkeypatch):
    monkeypatch.setattr(scp, "_CHECKPOINT_PATH", tmp_path / "state" / "ckpt.json")
    p = scp.SupplyChain10KParser(db_engine=MagicMock())
    monkeypatch.setattr(p, "_priority_tickers", lambda: ["AAA", "BBB", "CCC"])
    seen = []
    monkeypatch.setattr(p, "process_ticker", lambda t: seen.append(t) or 0)
    p.seen = seen
    return p


def test_checkpoint_persists_and_the_pass_resumes(parser):
    first = parser.run(limit=2)
    assert parser.seen == ["AAA", "BBB"] and first["remaining_in_pass"] == 1
    ckpt = json.loads(scp._CHECKPOINT_PATH.read_text())
    assert ckpt["done"] == ["AAA", "BBB"] and ckpt["cycle"] == 1
    second = parser.run()
    assert parser.seen == ["AAA", "BBB", "CCC"] and second["cycle"] == 1 and second["remaining_in_pass"] == 0


def test_a_finished_pass_starts_the_next_one(parser):
    parser.run()
    again = parser.run()
    assert again["cycle"] == 2
    assert parser.seen == ["AAA", "BBB", "CCC", "AAA", "BBB", "CCC"]
    assert json.loads(scp._CHECKPOINT_PATH.read_text())["cycle"] == 2


def test_explicit_tickers_never_start_a_new_pass(parser):
    parser.run(tickers=["AAA"])
    assert parser.run(tickers=["AAA"])["cycle"] == 1


def test_default_checkpoint_is_not_in_tmp():
    assert not str(scp._CHECKPOINT_PATH).startswith("/tmp")


def test_run_weekly_uses_the_short_budget(monkeypatch):
    captured = {}

    def fake_run(self, limit=None, budget_seconds=None, **kw):
        captured["budget"] = budget_seconds
        return {}

    monkeypatch.setattr(scp.SupplyChain10KParser, "run", fake_run)
    scp.run_weekly(MagicMock())
    assert captured["budget"] == scp.WEEKLY_BUDGET_SECONDS == 900


# --- stale graph alert ---------------------------------------------------------------


def _engine(fresh_row):
    conn = MagicMock()
    conn.execute.return_value.mappings.return_value.fetchone.return_value = fresh_row
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    return engine


def test_stale_graph_alerts_once_a_week():
    old = datetime.now(timezone.utc) - timedelta(days=122)
    engine = _engine({"edges": 857, "newest_created": old, "added_7d": 0, "added_30d": 0,
                      "validated_7d": 0, "last_validation": None})
    with patch.object(sca, "_already_alerted", return_value=False):
        out = sca.detect_graph_stale(engine)
    assert len(out) == 1 and out[0]["alert_type"] == "graph_stale"
    assert out[0]["key"].startswith("graph_stale:") and "W" in out[0]["key"]
    assert "no new edge in 122 days" in out[0]["headline"]
    assert out[0]["payload"]["added_30d"] == 0
    with patch.object(sca, "_already_alerted", return_value=True):
        assert sca.detect_graph_stale(engine) == []


def test_fresh_graph_is_quiet():
    recent = datetime.now(timezone.utc) - timedelta(days=3)
    engine = _engine({"edges": 900, "newest_created": recent, "added_7d": 12, "added_30d": 40,
                      "validated_7d": 900, "last_validation": recent})
    with patch.object(sca, "_already_alerted", return_value=False):
        assert sca.detect_graph_stale(engine) == []


def test_stale_graph_is_in_the_digest_order():
    assert "graph_stale" in sca.DETECTOR_ORDER
    assert sca._SECTION_TITLES["graph_stale"][0] == "Stale Supply-Chain Graph"
