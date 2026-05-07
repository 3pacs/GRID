"""Tests for ``intelligence.news_contagion_listener``.

Covers the mandatory five scenarios plus a few extras:
  1. Pattern detection — every pattern family fires exactly once
  2. Entity resolution — sector_map hit and supply_chain_nodes hit
  3. Shock-type mapping — each pattern maps to the right shock + magnitude
  4. Dry-run mode — no persistence, no simulator calls
  5. Idempotent dedup — pre-existing (news_id, shock_node, shock_type) skip
  6. Skip list suppresses M&A / CEO departure
  7. Commodity spike layered detection
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intelligence import news_contagion_listener as ncl
from intelligence.news_contagion_listener import (
    Candidate,
    detect_patterns,
    resolve_entity,
    run_once,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


class _CM:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False


def _fake_engine(conn):
    engine = MagicMock()
    engine.connect.side_effect = lambda: _CM(conn)
    engine.begin.side_effect = lambda: _CM(conn)
    return engine


@pytest.fixture(autouse=True)
def _reset_sector_map_cache():
    """Reset the module-level sector_map cache between tests."""
    ncl._SECTOR_MAP_CACHE = None
    yield
    ncl._SECTOR_MAP_CACHE = None


# ─────────────────────────────────────────────────────────────────────────────
# 1. Pattern detection
# ─────────────────────────────────────────────────────────────────────────────


def test_detect_bankruptcy_fires_supply_disruption_07():
    hits = detect_patterns("Acme Corp files for bankruptcy after cashflow crisis")
    patterns = [h[0] for h in hits]
    assert "bankruptcy" in patterns
    bank = next(h for h in hits if h[0] == "bankruptcy")
    assert bank[2] == "supply_disruption"
    assert bank[3] == 0.70
    assert "Acme" in bank[1]


def test_detect_halt_production():
    hits = detect_patterns("Boeing halts production at Everett factory")
    assert any(h[0] == "halt_production" for h in hits)
    h = next(h for h in hits if h[0] == "halt_production")
    assert h[2] == "supply_disruption"
    assert h[3] == 0.40


def test_detect_recall():
    hits = detect_patterns("Tesla recalls 50000 vehicles over brake issue")
    assert any(h[0] == "recall" for h in hits)
    h = next(h for h in hits if h[0] == "recall")
    assert h[3] == 0.20


def test_detect_sanctions():
    hits = detect_patterns("US imposes sanctions on Rosneft over Ukraine war")
    assert any(h[0] == "sanctions" for h in hits)
    h = next(h for h in hits if h[0] == "sanctions")
    assert h[2] == "supply_disruption"
    assert h[3] == 0.50


def test_detect_commodity_spike():
    hits = detect_patterns("Cocoa surges to record high on West Africa shortage")
    spike = [h for h in hits if h[0] == "commodity_spike"]
    assert len(spike) == 1
    assert spike[0][1] == "cocoa_beans"
    assert spike[0][2] == "price_increase"
    assert spike[0][3] == 0.20


def test_detect_oil_alias():
    hits = detect_patterns("Oil plunges 8% after OPEC surprise")
    spike = next(h for h in hits if h[0] == "commodity_spike")
    assert spike[1] == "oil_crude"


def test_skip_list_suppresses_m_and_a():
    # Even though "acquires" has a halt-like verb nearby, M&A is out of scope.
    hits = detect_patterns("Microsoft agrees to acquire Activision for $69B")
    assert hits == []


def test_skip_list_suppresses_ceo_departure():
    hits = detect_patterns("Disney CEO Bob Chapek resigns after board vote")
    assert hits == []


def test_empty_title_returns_empty():
    assert detect_patterns("") == []
    assert detect_patterns(None) == []  # type: ignore[arg-type]


# ─────────────────────────────────────────────────────────────────────────────
# 2. Entity resolution
# ─────────────────────────────────────────────────────────────────────────────


def test_resolve_sector_map_hit(monkeypatch):
    fake_map = {
        "Technology": {
            "subsectors": {
                "Semiconductors": {
                    "actors": [
                        {"name": "NVIDIA", "ticker": "NVDA"},
                        {"name": "TSMC", "ticker": "TSM"},
                    ]
                }
            }
        }
    }
    # Patch the import inside _load_sector_map_index.
    import sys

    fake_mod = SimpleNamespace(SECTOR_MAP=fake_map)
    monkeypatch.setitem(sys.modules, "analysis.sector_map", fake_mod)

    conn = MagicMock()
    assert resolve_entity(conn, "NVIDIA") == "nvda"
    assert resolve_entity(conn, "TSMC") == "tsm"
    # Normalization strips "Inc".
    assert resolve_entity(conn, "NVIDIA Inc.") == "nvda"


def test_resolve_supply_chain_nodes_hit(monkeypatch):
    # No sector_map module.
    import sys

    monkeypatch.setitem(
        sys.modules, "analysis.sector_map", SimpleNamespace(SECTOR_MAP={})
    )
    conn = MagicMock()
    # First query returns a row.
    conn.execute.return_value.fetchone.return_value = ("cocoa_beans",)
    node = resolve_entity(conn, "Cocoa Beans")
    assert node == "cocoa_beans"


def test_resolve_returns_none_when_unknown(monkeypatch):
    import sys

    monkeypatch.setitem(
        sys.modules, "analysis.sector_map", SimpleNamespace(SECTOR_MAP={})
    )
    conn = MagicMock()
    # Both DB lookups miss.
    conn.execute.return_value.fetchone.return_value = None
    assert resolve_entity(conn, "WidgetCo") is None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Shock-type mapping (sweep via fixture pattern)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "title, expected_pattern, expected_type, expected_mag",
    [
        ("Evergrande insolvency deepens as bondholders panic",
         "bankruptcy", "supply_disruption", 0.70),
        ("fire at Samsung plant disrupts memory output",
         "fire_at", "supply_disruption", 0.40),
        ("strike at Ford plant halts pickup assembly",
         "strike_at", "supply_disruption", 0.40),
        ("FDA warns Moderna over mislabeled lot",
         "fda_warn", "supply_disruption", 0.20),
        ("White House imposes export controls on Huawei chips",
         "export_controls", "supply_disruption", 0.50),
    ],
)
def test_shock_type_mapping(title, expected_pattern, expected_type, expected_mag):
    hits = detect_patterns(title)
    match = [h for h in hits if h[0] == expected_pattern]
    assert match, f"expected pattern {expected_pattern} in {title!r}, got {hits}"
    assert match[0][2] == expected_type
    assert match[0][3] == expected_mag


# ─────────────────────────────────────────────────────────────────────────────
# 4. Dry-run mode
# ─────────────────────────────────────────────────────────────────────────────


def test_run_once_dry_run_does_not_simulate_or_persist(monkeypatch):
    conn = MagicMock()
    # Return one actionable article.
    conn.execute.return_value.fetchall.return_value = [
        (101, "Acme halts production after fire", "http://n/1", None),
    ]
    # sector_map returns "acme" → "acme" mapping so resolve_entity succeeds.
    import sys

    fake_map = {
        "Industrials": {
            "subsectors": {
                "Machinery": {
                    "actors": [{"name": "Acme", "ticker": "ACME"}]
                }
            }
        }
    }
    monkeypatch.setitem(
        sys.modules, "analysis.sector_map", SimpleNamespace(SECTOR_MAP=fake_map)
    )

    engine = _fake_engine(conn)

    simulate_mock = MagicMock()
    monkeypatch.setattr(ncl, "simulate_contagion", simulate_mock)
    persist_mock = MagicMock()
    monkeypatch.setattr(ncl, "_persist", persist_mock)

    report = run_once(engine, since_hours=24, dry_run=True, limit=100)

    simulate_mock.assert_not_called()
    persist_mock.assert_not_called()
    assert report["dry_run"] is True
    assert report["fired"] >= 1
    assert all(p["dry_run"] for p in report["predictions"])
    assert all(p["prediction_id"] is None for p in report["predictions"])


# ─────────────────────────────────────────────────────────────────────────────
# 5. Idempotent dedup
# ─────────────────────────────────────────────────────────────────────────────


def test_run_once_skips_previously_triggered(monkeypatch):
    conn = MagicMock()
    # One article.
    conn.execute.return_value.fetchall.return_value = [
        (202, "Acme files for bankruptcy", "http://n/2", None),
    ]
    # _already_triggered returns True → dedup.
    conn.execute.return_value.fetchone.return_value = (1,)

    import sys

    fake_map = {
        "Industrials": {
            "subsectors": {
                "Machinery": {
                    "actors": [{"name": "Acme", "ticker": "ACME"}]
                }
            }
        }
    }
    monkeypatch.setitem(
        sys.modules, "analysis.sector_map", SimpleNamespace(SECTOR_MAP=fake_map)
    )
    engine = _fake_engine(conn)

    simulate_mock = MagicMock()
    monkeypatch.setattr(ncl, "simulate_contagion", simulate_mock)
    persist_mock = MagicMock(return_value=999)
    monkeypatch.setattr(ncl, "_persist", persist_mock)
    monkeypatch.setattr(ncl, "_already_triggered", lambda *a, **kw: True)

    report = run_once(engine, since_hours=24, dry_run=False, limit=100)

    simulate_mock.assert_not_called()
    persist_mock.assert_not_called()
    assert report["skipped_duplicate"] >= 1
    assert report["fired"] == 0


def test_run_once_fires_and_persists_when_not_duplicate(monkeypatch):
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (303, "Acme halts production at Ohio plant", "http://n/3", None),
    ]
    import sys

    fake_map = {
        "Industrials": {
            "subsectors": {
                "Machinery": {
                    "actors": [{"name": "Acme", "ticker": "ACME"}]
                }
            }
        }
    }
    monkeypatch.setitem(
        sys.modules, "analysis.sector_map", SimpleNamespace(SECTOR_MAP=fake_map)
    )
    engine = _fake_engine(conn)

    fake_result = {
        "summary": {"total_actors_affected": 5},
        "ranked_impact": [{"id": "foo", "margin_impact_pct": -0.01}],
    }
    simulate_mock = MagicMock(return_value=fake_result)
    monkeypatch.setattr(ncl, "simulate_contagion", simulate_mock)
    persist_mock = MagicMock(return_value=4242)
    monkeypatch.setattr(ncl, "_persist", persist_mock)
    monkeypatch.setattr(ncl, "_already_triggered", lambda *a, **kw: False)

    report = run_once(engine, since_hours=24, dry_run=False, limit=100)

    simulate_mock.assert_called_once()
    persist_mock.assert_called_once()
    assert report["fired"] == 1
    assert report["predictions"][0]["prediction_id"] == 4242
    assert report["predictions"][0]["shock_node"] == "acme"
    assert report["predictions"][0]["pattern"] == "halt_production"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Candidate struct immutability
# ─────────────────────────────────────────────────────────────────────────────


def test_candidate_is_frozen_and_serializable():
    c = Candidate(
        news_id=1,
        url="http://x",
        title="t",
        pattern="bankruptcy",
        shock_type="supply_disruption",
        magnitude=0.7,
        raw_entity="Acme",
        resolved_node="acme",
    )
    with pytest.raises(Exception):
        c.news_id = 2  # type: ignore[misc]
    d = c.as_dict()
    assert d["news_id"] == 1
    assert d["resolved_node"] == "acme"
