"""Tests for ``intelligence.edge_signals``.

Loader + lookup + multiplier API, pinned against synthetic edge_table
CSVs written into tmp paths.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from intelligence import edge_signals


# ── Helpers ────────────────────────────────────────────────────────────────


_CSV_HEADER = (
    "source_type,ticker,signal_direction,n_events,n_absent,"
    "hit_rate_present,hit_rate_absent,avg_return_present,avg_return_absent,"
    "information_coefficient,p_value,verdict\n"
)


def _write_csv(path: Path, rows: list[tuple[str, ...]]) -> None:
    with path.open("w") as f:
        f.write(_CSV_HEADER)
        for row in rows:
            f.write(",".join(str(c) for c in row) + "\n")


@pytest.fixture(autouse=True)
def _isolate_cache(monkeypatch, tmp_path):
    """Point edge_signals at a fresh tmp CSV per test and reset its cache.

    Also force the master switch ON for these tests — production
    default is OFF, but tests need to exercise the multiplier path.
    """
    csv_path = tmp_path / "edge_table.csv"
    monkeypatch.setenv("GRID_EDGE_TABLE_PATH", str(csv_path))
    monkeypatch.setattr(edge_signals, "_CACHE", None)
    monkeypatch.setattr(edge_signals, "EDGE_SIGNALS_ENABLED", True)
    yield csv_path
    monkeypatch.setattr(edge_signals, "_CACHE", None)


# ── Lookup ────────────────────────────────────────────────────────────────


def test_lookup_returns_none_when_csv_missing(_isolate_cache):
    # Don't write a file. Loader should fall through quietly.
    out = edge_signals.lookup_edge("insider", "AMZN", "SELL")
    assert out is None


def test_lookup_finds_edge_row(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 183, 276, 0.803, 0.174, 8.45, 12.16, 0.6234, 0.0, "EDGE"),
    ])
    out = edge_signals.lookup_edge("insider", "AMZN", "SELL")
    assert out is not None
    assert out.source_type == "insider"
    assert out.ticker == "AMZN"
    assert out.signal_direction == "SELL"
    assert out.information_coefficient == pytest.approx(0.6234)
    assert out.n_events == 183


def test_lookup_is_case_insensitive_on_ticker(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.7, 0.3, 1.0, 0.0, 0.4, 0.0, "EDGE"),
    ])
    # ticker passed in lowercase — should still hit
    assert edge_signals.lookup_edge("insider", "amzn", "SELL") is not None


def test_non_edge_verdicts_are_ignored(_isolate_cache):
    # NOISE / INCONCLUSIVE rows must not appear in the multiplier path
    # regardless of their IC magnitude.
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 50, 50, 0.6, 0.4, 0.0, 0.0, 0.5, 0.5, "NOISE"),
        ("insider", "MSFT", "BUY", 50, 50, 0.6, 0.4, 0.0, 0.0, 0.5, 0.5, "INCONCLUSIVE"),
    ])
    assert edge_signals.lookup_edge("insider", "AMZN", "SELL") is None
    assert edge_signals.lookup_edge("insider", "MSFT", "BUY") is None


# ── Multiplier semantics ──────────────────────────────────────────────────


def test_no_edge_returns_neutral_multiplier(_isolate_cache):
    assert edge_signals.edge_multiplier("insider", "AMZN", "SELL") == 1.0


def test_positive_ic_produces_boost(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0.0, 0.0, 0.6234, 0.0, "EDGE"),
    ])
    mult = edge_signals.edge_multiplier("insider", "AMZN", "SELL")
    # 1 + 0.8 * 0.6234 = 1.499
    assert mult == pytest.approx(1.0 + 0.8 * 0.6234, abs=1e-6)
    assert mult > 1.0


def test_negative_ic_produces_penalty(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("quiverquant:offexchange", "META", "off_exchange", 181, 532,
         0.19, 0.78, 0.0, 0.0, -0.5292, 0.0, "EDGE"),
    ])
    mult = edge_signals.edge_multiplier(
        "quiverquant:offexchange", "META", "off_exchange"
    )
    # 1 - 0.8 * 0.5292 = 0.577
    assert mult == pytest.approx(1.0 - 0.8 * 0.5292, abs=1e-6)
    assert mult < 1.0


def test_multiplier_is_clipped_to_bounds(_isolate_cache, monkeypatch):
    # Synthetic extreme IC values to verify the clip kicks in.
    _write_csv(_isolate_cache, [
        ("synthetic", "X", "y", 0, 0, 0, 0, 0, 0, 5.0, 0.0, "EDGE"),
        ("synthetic", "Z", "y", 0, 0, 0, 0, 0, 0, -5.0, 0.0, "EDGE"),
    ])
    assert edge_signals.edge_multiplier("synthetic", "X", "y") == edge_signals.EDGE_MULTIPLIER_MAX
    assert edge_signals.edge_multiplier("synthetic", "Z", "y") == edge_signals.EDGE_MULTIPLIER_MIN


def test_apply_multiplier_scales_base_conviction(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0.0, 0.0, 0.5, 0.0, "EDGE"),
    ])
    out = edge_signals.apply_multiplier(0.6, "insider", "AMZN", "SELL")
    expected = 0.6 * (1.0 + 0.8 * 0.5)  # 0.84
    assert out == pytest.approx(expected)


def test_apply_multiplier_passes_through_when_no_edge(_isolate_cache):
    out = edge_signals.apply_multiplier(0.6, "insider", "AMZN", "SELL")
    assert out == pytest.approx(0.6)


# ── Reload + cache ────────────────────────────────────────────────────────


def test_reload_picks_up_new_csv_contents(_isolate_cache):
    # First load — empty file
    _write_csv(_isolate_cache, [])
    assert edge_signals.lookup_edge("insider", "AMZN", "SELL") is None

    # Write a new row + reload
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0.0, 0.0, 0.6, 0.0, "EDGE"),
    ])
    n_loaded = edge_signals.reload()
    assert n_loaded == 1
    assert edge_signals.lookup_edge("insider", "AMZN", "SELL") is not None


def test_malformed_rows_are_skipped_silently(_isolate_cache):
    # Mix of valid + malformed lines (bad IC, missing fields, junk verdict).
    csv_path = _isolate_cache
    with csv_path.open("w") as f:
        f.write(_CSV_HEADER)
        # Valid
        f.write("insider,AMZN,SELL,100,100,0.8,0.2,0.0,0.0,0.6,0.0,EDGE\n")
        # IC is junk
        f.write("insider,MSFT,BUY,100,100,0.8,0.2,0.0,0.0,not-a-float,0.0,EDGE\n")
        # Missing source_type
        f.write(",NVDA,BUY,100,100,0.8,0.2,0.0,0.0,0.5,0.0,EDGE\n")
        # Wrong verdict
        f.write("insider,GOOGL,BUY,100,100,0.8,0.2,0.0,0.0,0.5,0.0,NOISE\n")

    edge_signals.reload()
    assert edge_signals.lookup_edge("insider", "AMZN", "SELL") is not None
    assert edge_signals.lookup_edge("insider", "MSFT", "BUY") is None
    assert edge_signals.lookup_edge("insider", "GOOGL", "BUY") is None


# ── Live data sanity: parses the real CSV without errors ──────────────────


def test_real_edge_table_loads_with_124_rows(monkeypatch, tmp_path):
    # Re-enable the production path for this one test.
    monkeypatch.delenv("GRID_EDGE_TABLE_PATH", raising=False)
    monkeypatch.setattr(edge_signals, "_CACHE", None)
    real = Path(__file__).resolve().parent.parent / "outputs" / "backtest" / "edge_table.csv"
    if not real.exists():
        pytest.skip("real edge_table.csv not present in this checkout")
    n = edge_signals.reload()
    assert n == 124, f"expected 124 EDGE rows, got {n}"
    # AMZN insider SELL was the headline edge in the 2026-05-11 backtest.
    edge = edge_signals.lookup_edge("insider", "AMZN", "SELL")
    assert edge is not None
    assert edge.information_coefficient > 0.6


# ── Master kill-switch ────────────────────────────────────────────────────


def test_disabled_master_switch_short_circuits_every_lookup(_isolate_cache, monkeypatch):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0.0, 0.0, 0.6234, 0.0, "EDGE"),
    ])
    # Toggle off — every call should now be a no-op (1.0 multiplier).
    monkeypatch.setattr(edge_signals, "EDGE_SIGNALS_ENABLED", False)
    assert edge_signals.edge_multiplier("insider", "AMZN", "SELL") == 1.0
    assert edge_signals.multiplier_for_source_ticker("insider", "AMZN") == 1.0
    # ``lookup_edge`` still works — it's a data access, not a multiplier.
    # Operator using lookup_edge directly knows what they're doing.
    assert edge_signals.lookup_edge("insider", "AMZN", "SELL") is not None


def test_set_enabled_toggles_at_runtime(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0.0, 0.0, 0.6234, 0.0, "EDGE"),
    ])
    edge_signals.set_enabled(False)
    assert edge_signals.edge_multiplier("insider", "AMZN", "SELL") == 1.0
    edge_signals.set_enabled(True)
    assert edge_signals.edge_multiplier("insider", "AMZN", "SELL") > 1.0


# ── multiplier_for_source_ticker (direction-agnostic) ─────────────────────


def test_multiplier_for_source_ticker_finds_match_without_direction(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0.0, 0.0, 0.6, 0.0, "EDGE"),
    ])
    # Should hit even though we didn't pass signal_direction.
    mult = edge_signals.multiplier_for_source_ticker("insider", "AMZN")
    assert mult == pytest.approx(1.0 + 0.8 * 0.6, abs=1e-6)


def test_multiplier_for_source_ticker_picks_strongest_ic_when_multiple(_isolate_cache):
    # Two EDGE rows on the same (source, ticker) pair but different
    # directions — the lookup should return the one with the strongest
    # |IC| since direction isn't known here.
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.7, 0.3, 0, 0, 0.30, 0, "EDGE"),
        ("insider", "AMZN", "BUY", 100, 100, 0.7, 0.3, 0, 0, -0.50, 0, "EDGE"),
    ])
    mult = edge_signals.multiplier_for_source_ticker("insider", "AMZN")
    # Stronger |IC| is -0.50 → penalty multiplier ≈ 1 - 0.8 * 0.5 = 0.6
    assert mult == pytest.approx(1.0 - 0.8 * 0.5, abs=1e-6)


def test_multiplier_for_source_ticker_neutral_on_no_match(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0, 0, 0.5, 0, "EDGE"),
    ])
    assert edge_signals.multiplier_for_source_ticker("xyz_source", "ZZZZ") == 1.0


# ── compute_aggregate_edge_multiplier ─────────────────────────────────────


class _Evidence:
    """Duck-typed SignalEvidence stand-in for these tests."""

    def __init__(self, signal_source: str) -> None:
        self.signal_source = signal_source


def test_aggregate_geomean_combines_two_edges(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0, 0, 0.5, 0, "EDGE"),
        ("quiverquant:house", "AMZN", "house_trading", 100, 100, 0.8, 0.2, 0, 0, 0.5, 0, "EDGE"),
    ])
    evidence = [_Evidence("insider"), _Evidence("quiverquant:house")]
    out = edge_signals.compute_aggregate_edge_multiplier(evidence, "AMZN")
    # Both contribute 1.4× → geomean = 1.4.
    assert out == pytest.approx(1.4, abs=1e-6)


def test_aggregate_ignores_evidence_without_known_edge(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0, 0, 0.5, 0, "EDGE"),
    ])
    evidence = [
        _Evidence("insider"),         # has edge → 1.4×
        _Evidence("unrelated_source"),  # no edge → ignored
    ]
    out = edge_signals.compute_aggregate_edge_multiplier(evidence, "AMZN")
    assert out == pytest.approx(1.4, abs=1e-6)


def test_aggregate_neutral_when_no_evidence_has_edge(_isolate_cache):
    evidence = [_Evidence("x"), _Evidence("y")]
    assert edge_signals.compute_aggregate_edge_multiplier(evidence, "AMZN") == 1.0


def test_aggregate_neutral_when_master_switch_off(_isolate_cache, monkeypatch):
    _write_csv(_isolate_cache, [
        ("insider", "AMZN", "SELL", 100, 100, 0.8, 0.2, 0, 0, 0.5, 0, "EDGE"),
    ])
    monkeypatch.setattr(edge_signals, "EDGE_SIGNALS_ENABLED", False)
    evidence = [_Evidence("insider")]
    assert edge_signals.compute_aggregate_edge_multiplier(evidence, "AMZN") == 1.0


def test_aggregate_mixed_boost_and_penalty(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("a", "X", "_", 0, 0, 0, 0, 0, 0, 0.5, 0, "EDGE"),   # → 1.4
        ("b", "X", "_", 0, 0, 0, 0, 0, 0, -0.5, 0, "EDGE"),  # → 0.6
    ])
    evidence = [_Evidence("a"), _Evidence("b")]
    out = edge_signals.compute_aggregate_edge_multiplier(evidence, "X")
    # geomean(1.4, 0.6) = sqrt(0.84) ≈ 0.917
    import math
    assert out == pytest.approx(math.sqrt(1.4 * 0.6), abs=1e-6)


def test_aggregate_clipped_when_many_strong_boosts(_isolate_cache):
    _write_csv(_isolate_cache, [
        ("a", "X", "_", 0, 0, 0, 0, 0, 0, 5.0, 0, "EDGE"),  # mult clipped to MAX
        ("b", "X", "_", 0, 0, 0, 0, 0, 0, 5.0, 0, "EDGE"),
        ("c", "X", "_", 0, 0, 0, 0, 0, 0, 5.0, 0, "EDGE"),
    ])
    evidence = [_Evidence(s) for s in "abc"]
    out = edge_signals.compute_aggregate_edge_multiplier(evidence, "X")
    # All three multipliers are at MAX (1.8) → geomean is also 1.8.
    assert out == pytest.approx(edge_signals.EDGE_MULTIPLIER_MAX)
