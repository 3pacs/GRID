"""``intelligence/long_plays.py`` — the multi-year Long Plays board.

No live DB: every loader is monkeypatched or driven by a ``MagicMock``
engine (the ``tests/test_universe_ranker_readback.py`` pattern). The pure
rules (``classify_stance``, ``asymmetry_score``, ``multiple_math``) are
pinned by truth tables; the board builder is pinned on shape, ordering,
``top_k``, stand-down and per-source degradation; persistence is pinned on
parameterised SQL and a JSON round-trip; the scheduler / digest wiring is
pinned at source level (``tests/test_sprint2_wiring.py`` style).
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from intelligence import long_plays as lp

ROOT = Path(__file__).resolve().parents[1]
AS_OF = date(2026, 9, 6)


# ── helpers ───────────────────────────────────────────────────────────────


def _series(years: float = 10.0, cagr: float = 0.25, start: float = 10.0, wobble: float = 0.05) -> list[tuple[date, float]]:
    """Deterministic daily adjusted-close path with a mild cycle."""
    n_days = int(years * 365.25)
    first = AS_OF - timedelta(days=n_days)
    out: list[tuple[date, float]] = []
    for i in range(0, n_days, 1):
        t = i / 365.25
        value = start * math.exp(math.log1p(cagr) * t) * (1.0 + wobble * math.sin(t * 4.0))
        out.append((first + timedelta(days=i), value))
    return out


def _engine(first: Any = None, rows: list[Any] | None = None) -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def execute(stmt: Any, params: Any = None) -> MagicMock:
        result = MagicMock()
        result.first.return_value = first
        result.fetchall.return_value = rows or []
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine, conn


def _patch_loaders(monkeypatch: pytest.MonkeyPatch, **overrides: Any) -> None:
    """Install a full set of fake loaders; ``overrides`` replace individual ones."""
    defaults: dict[str, Any] = {
        "_playbook_index": lambda: {
            "CCJ": [
                {
                    "id": "uranium-fuel",
                    "title": "Uranium policy",
                    "thesis_stub": "Fuel suppliers keep getting selected.",
                    "horizon": "3-6 months",
                    "category": "Energy",
                    "sector_focus": "Nuclear fuel",
                    "base_edge": 15.0,
                }
            ]
        },
        "_load_trial_tickers": lambda engine, as_of: {
            "BIOX": {"company_name": "Biox Therapeutics", "primary_indication": "oncology", "market_cap_mm": 450.0}
        },
        "_load_catalysts": lambda engine, as_of: {
            "BIOX": [
                {
                    "event_type": "phase3_readout",
                    "expected_date": (as_of + timedelta(days=200)).isoformat(),
                    "days_out": 200,
                    "confidence_window_days": 30,
                    "trial_strength_score": 0.8,
                    "signal_type": "trial",
                    "regime_at_signal": "trending",
                    "market_cap_mm": 450.0,
                    "primary_indication": "oncology",
                }
            ]
        },
        "_load_options_asymmetry": lambda engine, as_of: {
            "BIOX": {
                "max_payoff_multiple": 40.0,
                "score": 0.7,
                "direction": "call",
                "thesis": "cheap tail",
                "is_100x": False,
                "scan_date": as_of.isoformat(),
            }
        },
        "_load_adj_close": lambda engine, tickers, years, as_of: {
            "CCJ": _series(10.0, 0.30),
            "NVDA": _series(10.0, 0.45),
            "UEC": _series(10.0, -0.45, wobble=0.0),
            "FCX": _series(0.5, 0.10),
        },
        "_load_market_caps": lambda engine, tickers, as_of: {
            "CCJ": {"market_cap_usd": 25e9, "as_of": as_of.isoformat()},
            "NVDA": {"market_cap_usd": 3e12, "as_of": as_of.isoformat()},
        },
        "_load_sweep": lambda engine: {
            "CCJ": {"verdict": "high", "composite_score": 1.2, "horizon_days": 90, "generated_at": "2026-09-06T05:00:00+00:00"},
            "NVDA": {"verdict": "low", "composite_score": 0.1, "horizon_days": 90, "generated_at": "2026-09-06T05:00:00+00:00"},
        },
        "_load_realized_alpha": lambda engine: {
            "oracle_predictions_60d_mean_alpha": 0.012,
            "paper_trades_60d_mean_alpha": -0.004,
            "as_of": None,
        },
        "_company_name": lambda ticker: {"CCJ": "Cameco", "NVDA": "NVIDIA"}.get(ticker),
        "_load_company_profiles": lambda engine, as_of: {
            # enriched small cap (joins the universe; cap from company_profiles)
            "SMLX": {
                "market_cap": 900e6, "cash": 150e6, "cash_runway_months": 14.0, "revenue_ttm": 12e6,
                "net_income_ttm": -40e6, "shares_outstanding": 50e6, "sector": "Healthcare",
                "industry": "Biotechnology", "description": "Small biotech.", "name": "Smallex Bio",
                "enriched_at": (as_of - timedelta(days=3)).isoformat(),
            },
            # fundamentals but no cap -> cap chain falls through to trial_signals
            "BIOX": {
                "market_cap": None, "cash": 80e6, "cash_runway_months": 9.0, "revenue_ttm": None,
                "net_income_ttm": -25e6, "shares_outstanding": None, "sector": "Healthcare",
                "industry": "Biotechnology", "description": None, "name": None,
                "enriched_at": (as_of - timedelta(days=1)).isoformat(),
            },
        },
    }
    defaults.update(overrides)
    for name, fn in defaults.items():
        monkeypatch.setattr(lp, name, fn)


# ── classify_stance truth table ───────────────────────────────────────────


def _stance(**kw: Any) -> str:
    base: dict[str, Any] = dict(
        p50_3y_multiple=2.0,
        p10_3y_multiple=0.8,
        max_drawdown=-0.4,
        sweep_verdict="high",
        has_sweep_coverage=True,
        catalyst_strength_12m=None,
        options_payoff_multiple=None,
        has_catalyst=False,
    )
    base.update(kw)
    return lp.classify_stance(**base)


@pytest.mark.parametrize(
    "kw,expected",
    [
        ({}, "entry_candidate"),
        ({"sweep_verdict": "moderate"}, "entry_candidate"),
        ({"sweep_verdict": "low"}, "watch"),
        ({"sweep_verdict": "no_trade"}, "watch"),
        ({"p50_3y_multiple": 1.5}, "watch"),  # strictly greater
        ({"p50_3y_multiple": 1.2, "catalyst_strength_12m": 0.6, "has_catalyst": True}, "entry_candidate"),
        ({"p50_3y_multiple": 1.2, "catalyst_strength_12m": 0.59, "has_catalyst": True}, "watch"),
        ({"max_drawdown": -0.75}, "watch"),
        ({"max_drawdown": -0.8}, "watch"),
        ({"max_drawdown": None}, "watch"),
        ({"has_sweep_coverage": False, "sweep_verdict": None, "options_payoff_multiple": 20.0}, "entry_candidate"),
        ({"has_sweep_coverage": False, "sweep_verdict": None, "options_payoff_multiple": 19.9}, "watch"),
        ({"has_sweep_coverage": False, "sweep_verdict": None, "options_payoff_multiple": None}, "watch"),
        # options payoff cannot override a covered-but-low sweep verdict
        ({"sweep_verdict": "low", "options_payoff_multiple": 80.0}, "watch"),
        ({"p50_3y_multiple": 0.9, "p10_3y_multiple": 0.2}, "avoid"),
        ({"p50_3y_multiple": 0.9, "p10_3y_multiple": 0.2, "has_catalyst": True}, "watch"),
        ({"p50_3y_multiple": 0.9, "p10_3y_multiple": 0.25}, "watch"),
        ({"p50_3y_multiple": None, "p10_3y_multiple": None}, "watch"),
        ({"p50_3y_multiple": float("nan"), "p10_3y_multiple": float("nan")}, "watch"),
    ],
)
def test_classify_stance_truth_table(kw: dict[str, Any], expected: str) -> None:
    assert _stance(**kw) == expected


# ── asymmetry_score ───────────────────────────────────────────────────────


def test_asymmetry_score_monotonic_in_p90_and_bounded() -> None:
    scores = [lp.asymmetry_score(p90_3y_multiple=m) for m in (0.2, 0.5, 1.0, 2.0, 5.0, 20.0, 1e6)]
    assert scores == sorted(scores)
    assert all(0.0 <= s <= 1.0 for s in scores)
    assert lp.asymmetry_score(p90_3y_multiple=1.0) == 0.0
    assert lp.asymmetry_score(p90_3y_multiple=0.2) == 0.0  # negative term clamps to 0
    full = lp.asymmetry_score(p90_3y_multiple=1e9, catalyst_strength=5.0, payoff_multiple=1000.0, playbook_edge=25.0)
    assert full == 1.0


def test_asymmetry_score_missing_components_contribute_zero() -> None:
    base = lp.asymmetry_score(p90_3y_multiple=4.0)
    assert base == pytest.approx(0.5 * math.tanh(math.log(4.0)), abs=1e-4)
    assert lp.asymmetry_score(p90_3y_multiple=None) == 0.0
    assert lp.asymmetry_score(p90_3y_multiple=None, catalyst_strength=0.5) == pytest.approx(0.1, abs=1e-4)
    assert lp.asymmetry_score(p90_3y_multiple=None, payoff_multiple=50.0) == pytest.approx(0.1, abs=1e-4)
    assert lp.asymmetry_score(p90_3y_multiple=None, playbook_edge=12.5) == pytest.approx(0.05, abs=1e-4)
    assert lp.asymmetry_score(p90_3y_multiple=float("nan"), catalyst_strength=float("inf")) == 0.0


# ── multiple_math / buckets ───────────────────────────────────────────────


def test_multiple_math_is_explicit_arithmetic_and_labelled_proxy() -> None:
    out = lp.multiple_math(p50_3y_multiple=2.0, p90_3y_multiple=8.0, market_cap_usd=2e9)
    cagr50 = 2.0 ** (1 / 3) - 1
    assert out["p50_cagr_proxy"] == pytest.approx(cagr50, abs=1e-4)
    assert out["years_to_10x_at_p50_cagr"] == pytest.approx(math.log(10) / math.log1p(cagr50), abs=0.1)
    assert out["years_to_10x_at_p90"] == pytest.approx(math.log(10) / math.log(2.0), abs=0.1)
    assert out["mcap_at_10x_usd"] == 2e10 and out["mcap_at_100x_usd"] == 2e11
    assert out["note"] == "proxy from historical CAGR/vol; not a forecast"
    flat = lp.multiple_math(p50_3y_multiple=0.9, p90_3y_multiple=None, market_cap_usd=None)
    assert flat["years_to_10x_at_p50_cagr"] is None and flat["years_to_10x_at_p90"] is None
    assert flat["mcap_at_10x_usd"] is None
    assert "no finite path" in lp._what_must_be_true(flat)
    assert "$20.0 B" in lp._what_must_be_true(out)


def test_market_cap_bucket() -> None:
    assert lp.market_cap_bucket(None) is None
    assert lp.market_cap_bucket(0) is None
    assert lp.market_cap_bucket(1e8) == "micro"
    assert lp.market_cap_bucket(1e9) == "small"
    assert lp.market_cap_bucket(5e9) == "mid"
    assert lp.market_cap_bucket(50e9) == "large"
    assert lp.market_cap_bucket(1e12) == "mega"


# ── build_long_plays_board ────────────────────────────────────────────────


_CANDIDATE_KEYS = {
    "ticker", "name", "themes", "thesis_sources", "market_cap_usd", "market_cap_bucket",
    "price", "price_as_of", "chart", "projection", "multiple_math", "catalysts",
    "options_asymmetry", "sweep", "realized_alpha_context", "asymmetry_score", "stance",
    "why", "what_must_be_true_for_10x",
}


def test_board_shape_sort_and_composition(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=25)

    assert set(board) >= {"as_of", "generated_at", "universe_size", "candidates", "stand_down_reason", "method_notes"}
    assert board["as_of"] == AS_OF.isoformat()
    # union of frontier universe + playbook + trial + catalyst + options tickers, stocks only
    assert board["universe_size"] >= len(lp.FRONTIER_THEMATIC_UNIVERSE) + 1
    by_ticker = {c["ticker"]: c for c in board["candidates"]}
    assert "BIOX" in by_ticker and "CCJ" in by_ticker

    scores = [c["asymmetry_score"] for c in board["candidates"]]
    assert scores == sorted(scores, reverse=True)
    for cand in board["candidates"]:
        assert _CANDIDATE_KEYS <= set(cand)
        json.dumps(cand)  # JSON-safe
        assert cand["stance"] in {"entry_candidate", "watch", "avoid"}
        assert 0.0 <= cand["asymmetry_score"] <= 1.0
        assert cand["realized_alpha_context"]["oracle_predictions_60d_mean_alpha"] == 0.012

    ccj = by_ticker["CCJ"]
    assert ccj["name"] == "Cameco"
    assert "uranium" in ccj["themes"] and "Energy" in ccj["themes"]
    assert {s["kind"] for s in ccj["thesis_sources"]} == {"frontier_theme", "playbook"}
    assert ccj["market_cap_bucket"] == "large" and ccj["market_cap_usd"] == 25e9
    assert ccj["chart"]["max_drawdown"] <= 0.0
    assert ccj["projection"]["method"] == lp.PROJECTION_METHOD
    assert set(ccj["projection"]) == {"1y", "3y", "5y", "method"}
    assert ccj["projection"]["3y"]["p90_multiple"] >= ccj["projection"]["3y"]["p50_multiple"] >= ccj["projection"]["3y"]["p10_multiple"]
    assert ccj["sweep"]["verdict"] == "high"
    assert ccj["stance"] == "entry_candidate"
    assert ccj["multiple_math"]["mcap_at_10x_usd"] == 250e9
    assert "not a forecast" in ccj["multiple_math"]["note"]
    assert "$250.0 B" in ccj["what_must_be_true_for_10x"]
    assert "sweep verdict high" in ccj["why"]

    biox = by_ticker["BIOX"]
    assert biox["name"] == "Biox Therapeutics"
    assert biox["chart"] is None and biox["projection"] is None
    assert biox["market_cap_usd"] == 450e6 and biox["market_cap_bucket"] == "small"
    assert biox["catalysts"][0]["event_type"] == "phase3_readout"
    assert biox["options_asymmetry"]["max_payoff_multiple"] == 40.0
    assert biox["sweep"] is None
    # no chart → drawdown gate fails → cannot be an entry candidate, even with catalyst + payoff
    assert biox["stance"] == "watch"
    assert {s["kind"] for s in biox["thesis_sources"]} == {"trial_catalyst", "options_asymmetry"}

    # NVDA: covered by the sweep with a low verdict → watch despite a strong chart
    assert by_ticker["NVDA"]["stance"] == "watch"
    # UEC: 10 y of -45 % CAGR → p10 3y < 0.25 with no catalyst → avoid
    assert by_ticker["UEC"]["projection"]["3y"]["p10_multiple"] < 0.25
    assert by_ticker["UEC"]["stance"] == "avoid"
    # FCX: half a year of history → chart degraded with a note
    assert by_ticker["FCX"]["chart"] is None
    assert any(n.startswith("FCX:") for n in board["method_notes"])
    assert board["stand_down_reason"] is None
    assert board["entry_candidates"] >= 1


def test_board_respects_top_k_and_universe_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(
        monkeypatch,
        _load_options_asymmetry=lambda engine, as_of: {
            "SPY": {"max_payoff_multiple": 90.0, "score": 0.9, "direction": "call", "thesis": "x", "is_100x": True, "scan_date": as_of.isoformat()},
            "BTC-USD": {"max_payoff_multiple": 90.0, "score": 0.9, "direction": "call", "thesis": "x", "is_100x": True, "scan_date": as_of.isoformat()},
        },
    )
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=3)
    assert len(board["candidates"]) == 3
    assert board["universe_size"] > 3
    tickers = {c["ticker"] for c in board["candidates"]}
    assert "SPY" not in tickers and "BTC-USD" not in tickers
    assert any("skipped" in n and "ETF" in n for n in board["method_notes"])
    # top_k < 1 is clamped, and the 3y horizon is always present for the stance rule
    board_1y = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, horizons_years=(1,), top_k=0)
    assert len(board_1y["candidates"]) == 1
    assert board_1y["horizons_years"] == [1, 3]


def test_board_sets_stand_down_reason_when_nothing_is_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(
        monkeypatch,
        _load_sweep=lambda engine: {"CCJ": {"verdict": "low", "composite_score": 0.0, "horizon_days": 90, "generated_at": None}},
        _load_options_asymmetry=lambda engine, as_of: {},
        _load_catalysts=lambda engine, as_of: {},
    )
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF)
    assert all(c["stance"] != "entry_candidate" for c in board["candidates"])
    assert board["entry_candidates"] == 0
    assert board["stand_down_reason"] and "no entry candidates" in board["stand_down_reason"]


def test_board_degrades_failing_sources_to_none_with_notes(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("relation does not exist")

    _patch_loaders(
        monkeypatch,
        _load_sweep=boom,
        _load_realized_alpha=boom,
        _load_market_caps=boom,
        _load_options_asymmetry=boom,
        _load_catalysts=boom,
        _load_trial_tickers=boom,
    )
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF)
    notes = "\n".join(board["method_notes"])
    for source in (
        "universe_ranking_history", "realized_alpha_daily", "ticker_metrics_daily",
        "options_mispricing_scans", "upcoming_catalysts", "trial_signals",
    ):
        assert f"{source}: unavailable" in notes, source
    assert board["candidates"], "price-history-only board still produces candidates"
    for cand in board["candidates"]:
        assert cand["sweep"] is None
        assert cand["realized_alpha_context"] is None
        assert cand["options_asymmetry"] is None
        assert cand["catalysts"] == []
    ccj = next(c for c in board["candidates"] if c["ticker"] == "CCJ")
    assert ccj["market_cap_usd"] is None and ccj["market_cap_bucket"] is None
    assert ccj["multiple_math"]["mcap_at_10x_usd"] is None
    assert "Market cap unknown" in ccj["what_must_be_true_for_10x"]
    # no sweep + no options payoff → nothing can pass the coverage gate
    assert board["stand_down_reason"] is not None


def test_board_survives_total_price_outage(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("db down")

    _patch_loaders(monkeypatch, _load_adj_close=boom)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF)
    assert any(n.startswith("price_history: unavailable") for n in board["method_notes"])
    assert all(c["chart"] is None and c["projection"] is None for c in board["candidates"])
    assert board["stand_down_reason"] is not None


# ── fundamentals / cap fallback chain / sweep note (task #28) ─────────────


def test_candidates_carry_fundamentals_and_cap_fallback_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=100)
    by_ticker = {c["ticker"]: c for c in board["candidates"]}

    # enriched small cap joined the universe with a note
    assert "SMLX" in by_ticker
    assert any("company_profiles small caps" in n for n in board["method_notes"])
    smlx = by_ticker["SMLX"]
    assert set(smlx["fundamentals"]) == set(lp.FUNDAMENTALS_KEYS)
    assert smlx["fundamentals"]["cash_runway_months"] == 14.0 and smlx["fundamentals"]["sector"] == "Healthcare"
    assert smlx["market_cap_usd"] == 900e6 and smlx["market_cap_source"] == "company_profiles.market_cap"
    assert smlx["market_cap_bucket"] == "small" and smlx["name"] == "Smallex Bio"
    assert "runway 14 mo" in smlx["why"]

    # profile without cap: fundamentals attach, cap falls back to trial_signals.market_cap_mm
    biox = by_ticker["BIOX"]
    assert biox["fundamentals"]["cash"] == 80e6 and biox["fundamentals"]["revenue_ttm"] is None
    assert biox["market_cap_usd"] == 450e6 and biox["market_cap_source"] == "trial_signals.market_cap_mm"
    assert "oncology" in biox["themes"]  # trial primary_indication is a theme
    assert "runway 9 mo" in biox["why"]

    # ticker_metrics_daily still wins over everything; no profile -> None fundamentals, no runway text
    ccj = by_ticker["CCJ"]
    assert ccj["market_cap_source"] == "ticker_metrics_daily"
    assert all(v is None for v in ccj["fundamentals"].values())
    assert "runway" not in ccj["why"]
    for cand in board["candidates"]:
        json.dumps(cand["fundamentals"])


def test_small_cap_universe_requires_recent_enrichment_and_small_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    profiles = {
        "STALE": {"market_cap": 500e6, "enriched_at": (AS_OF - timedelta(days=45)).isoformat()},
        "BIGCO": {"market_cap": 5e9, "enriched_at": (AS_OF - timedelta(days=1)).isoformat()},
        "FUTUR": {"market_cap": 500e6, "enriched_at": (AS_OF + timedelta(days=2)).isoformat()},  # after as_of
        "NOCAP": {"market_cap": None, "enriched_at": (AS_OF - timedelta(days=1)).isoformat()},
        "GOOD": {"market_cap": 500e6, "enriched_at": (AS_OF - timedelta(days=29)).isoformat()},
        "BADTS": {"market_cap": 500e6, "enriched_at": "not-a-date"},
    }
    assert lp._recently_enriched_small_caps(profiles, AS_OF) == {"GOOD"}
    _patch_loaders(monkeypatch, _load_company_profiles=lambda engine, as_of: profiles)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=100)
    tickers = {c["ticker"] for c in board["candidates"]}
    assert "GOOD" in tickers and not ({"STALE", "BIGCO", "FUTUR", "NOCAP", "BADTS"} & tickers)


def test_company_profiles_failure_degrades_with_note(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("relation does not exist")

    _patch_loaders(monkeypatch, _load_company_profiles=boom)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF)
    assert any(n.startswith("company_profiles: unavailable") for n in board["method_notes"])
    assert all(all(v is None for v in c["fundamentals"].values()) for c in board["candidates"])
    biox = next(c for c in board["candidates"] if c["ticker"] == "BIOX")
    assert biox["market_cap_usd"] == 450e6  # trial_signals fallback still works


def test_sweep_note_distinguishes_missing_from_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch, _load_sweep=lambda engine: None)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=3)
    assert f"universe_ranking_history: {lp.SWEEP_NOTE_MISSING}" in board["method_notes"]
    assert all(c["sweep_note"] == lp.SWEEP_NOTE_MISSING for c in board["candidates"])

    _patch_loaders(monkeypatch, _load_sweep=lambda engine: {})
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=3)
    assert f"universe_ranking_history: {lp.SWEEP_NOTE_EMPTY}" in board["method_notes"]
    assert "no rankable verdicts" in lp.SWEEP_NOTE_EMPTY
    assert all(c["sweep_note"] == lp.SWEEP_NOTE_EMPTY for c in board["candidates"])
    assert not any(lp.SWEEP_NOTE_MISSING in n for n in board["method_notes"])

    # a covered board has no per-candidate note
    _patch_loaders(monkeypatch)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=3)
    assert all(c["sweep_note"] is None for c in board["candidates"])


def test_load_sweep_returns_none_for_no_row_and_empty_dict_for_empty_top_k(monkeypatch: pytest.MonkeyPatch) -> None:
    import intelligence.universe_ranker as ur

    monkeypatch.setattr(ur, "load_latest_ranking", lambda engine, horizon_days: None)
    assert lp._load_sweep(MagicMock()) is None
    monkeypatch.setattr(ur, "load_latest_ranking", lambda engine, horizon_days: {"top_k": [], "horizon_days": 90})
    assert lp._load_sweep(MagicMock()) == {}
    monkeypatch.setattr(
        ur, "load_latest_ranking",
        lambda engine, horizon_days: {"top_k": [{"ticker": "ccj", "verdict": "high", "composite_score": "1.1"}], "horizon_days": 90, "generated_at": "g"},
    )
    assert lp._load_sweep(MagicMock()) == {"CCJ": {"verdict": "high", "composite_score": 1.1, "horizon_days": 90, "generated_at": "g"}}


def test_load_company_profiles_parses_jsonb_and_is_pit_bounded() -> None:
    rows = [
        ("smlx", "Smallex Bio", "Healthcare", json.dumps({"market_cap": 9e8, "cash": "1.5e8", "enriched_at": "2026-09-01T00:00:00+00:00"})),
        ("BIOX", None, None, {"cash_runway_months": 9, "industry": "Biotechnology"}),
        (None, "x", None, {}),
        ("BAD", None, None, "not json"),
    ]
    engine, conn = _engine(rows=rows)
    out = lp._load_company_profiles(engine, AS_OF)
    assert set(out) == {"SMLX", "BIOX", "BAD"}
    assert out["SMLX"]["market_cap"] == 9e8 and out["SMLX"]["cash"] == 1.5e8 and out["SMLX"]["name"] == "Smallex Bio"
    assert out["SMLX"]["sector"] == "Healthcare" and out["SMLX"]["enriched_at"] == "2026-09-01T00:00:00+00:00"
    assert out["BIOX"]["cash_runway_months"] == 9.0 and out["BIOX"]["industry"] == "Biotechnology" and out["BIOX"]["market_cap"] is None
    assert all(out["BAD"][k] is None for k in lp.FUNDAMENTALS_KEYS)
    stmt, params = conn.execute.call_args.args
    assert "FROM company_profiles" in str(stmt) and "last_analyzed <= :as_of_ts" in str(stmt)
    assert params["as_of_ts"].date() == AS_OF and params["as_of_ts"].tzinfo is not None


# ── price loader: PIT cut-off on both paths ───────────────────────────────


def test_load_adj_close_uses_pit_store_then_raw_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    def execute(stmt: Any, params: Any = None) -> MagicMock:
        sql = str(stmt)
        calls.append((sql, params or {}))
        result = MagicMock()
        if "feature_registry" in sql:
            result.fetchall.return_value = [(101, "nvda_full")]
        else:
            result.fetchall.return_value = [
                ("YF:CCJ:adj_close", AS_OF - timedelta(days=2), 41.0),
                ("YF:CCJ:adj_close", AS_OF - timedelta(days=1), 42.0),
            ]
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value = conn

    seen: dict[str, Any] = {}

    class FakePIT:
        def __init__(self, eng: Any) -> None:
            seen["engine"] = eng

        def get_feature_matrix(self, feature_ids: list[int], start_date: date, end_date: date, as_of_date: date, vintage_policy: str = "FIRST_RELEASE") -> pd.DataFrame:
            seen.update(feature_ids=feature_ids, start=start_date, end=end_date, as_of=as_of_date, policy=vintage_policy)
            idx = pd.DatetimeIndex([datetime(2026, 9, 3), datetime(2026, 9, 4)], name="obs_date")
            return pd.DataFrame({101: [100.0, 101.0]}, index=idx)

    import store.pit as pit_module

    monkeypatch.setattr(pit_module, "PITStore", FakePIT)
    history = lp._load_adj_close(engine, ["NVDA", "CCJ", "nvda"], 10, AS_OF)

    assert seen["feature_ids"] == [101] and seen["as_of"] == AS_OF and seen["end"] == AS_OF
    assert seen["policy"] == "LATEST_AS_OF"
    assert history["NVDA"] == [(date(2026, 9, 3), 100.0), (date(2026, 9, 4), 101.0)]
    assert history["CCJ"] == [(AS_OF - timedelta(days=2), 41.0), (AS_OF - timedelta(days=1), 42.0)]

    reg_sql, reg_params = calls[0]
    assert ":names" in reg_sql and reg_params == {"names": ["ccj_full", "nvda_full"]}
    raw_sql, raw_params = calls[1]
    assert "DISTINCT ON (series_id, obs_date)" in raw_sql
    assert "pull_timestamp <= :as_of_ts" in raw_sql and "obs_date <= :as_of" in raw_sql
    assert raw_params["series_ids"] == ["YF:CCJ:adj_close"]
    assert raw_params["as_of"] == AS_OF
    assert raw_params["as_of_ts"].date() == AS_OF and raw_params["as_of_ts"].tzinfo is not None


# ── persistence ───────────────────────────────────────────────────────────


def test_persist_board_is_parameterised_and_json_encodes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_loaders(monkeypatch)
    board = lp.build_long_plays_board(MagicMock(), as_of=AS_OF, top_k=2)
    engine, conn = _engine(first=(42,))
    assert lp.persist_board(engine, board) == 42
    insert_calls = [c for c in conn.execute.call_args_list if "INSERT INTO long_plays_board" in str(c.args[0])]
    assert len(insert_calls) == 1
    stmt, params = insert_calls[0].args
    for bind in (":as_of", ":generated_at", ":universe_size", ":candidates", ":stand_down_reason", ":method_notes"):
        assert bind in str(stmt)
    assert params["as_of"] == AS_OF
    assert params["generated_at"].tzinfo is not None
    assert params["universe_size"] == board["universe_size"]
    assert json.loads(params["candidates"]) == board["candidates"]
    assert json.loads(params["method_notes"]) == board["method_notes"]
    # ensure_long_plays_table ran first (CREATE TABLE IF NOT EXISTS via engine.begin)
    assert any("CREATE TABLE IF NOT EXISTS long_plays_board" in str(c.args[0]) for c in conn.execute.call_args_list)


def test_persist_board_returns_minus_one_on_failure() -> None:
    broken = MagicMock()
    broken.begin.side_effect = RuntimeError("db down")
    assert lp.persist_board(broken, {"as_of": AS_OF.isoformat(), "candidates": []}) == -1


def test_load_latest_board_round_trips_json_and_handles_empty() -> None:
    candidates = [{"ticker": "CCJ", "stance": "entry_candidate", "asymmetry_score": 0.7}, {"ticker": "UEC", "stance": "avoid"}]
    row = (
        9,
        AS_OF,
        datetime(2026, 9, 6, 5, 30, tzinfo=timezone.utc),
        31,
        json.dumps(candidates),
        None,
        ["note a"],  # JSONB may arrive parsed
    )
    engine, conn = _engine(first=row)
    out = lp.load_latest_board(engine)
    assert out is not None
    assert out["id"] == 9 and out["as_of"] == "2026-09-06"
    assert out["generated_at"].startswith("2026-09-06T05:30:00")
    assert out["candidates"] == candidates and out["entry_candidates"] == 1
    assert out["method_notes"] == ["note a"] and out["stand_down_reason"] is None
    stmt = conn.execute.call_args.args[0]
    assert "ORDER BY generated_at DESC" in str(stmt) and "LIMIT 1" in str(stmt)

    empty, _ = _engine(first=None)
    assert lp.load_latest_board(empty) is None
    broken = MagicMock()
    broken.connect.side_effect = RuntimeError("relation does not exist")
    assert lp.load_latest_board(broken) is None


def test_module_sql_has_no_fstrings_or_format() -> None:
    src = Path(lp.__file__).read_text(encoding="utf-8")
    assert 'f"""' not in src and "f'''" not in src
    assert ".format(" not in src
    # every SQL statement carries at least one bind or is a fixed DDL/read
    for name in ("_FEATURE_IDS_SQL", "_RAW_ADJ_CLOSE_SQL", "_MARKET_CAP_SQL", "_CATALYSTS_SQL", "_TRIAL_TICKERS_SQL", "_OPTIONS_SQL", "_INSERT_BOARD_SQL"):
        block = src[src.index(name):]
        block = block[: block.index('"""\n)') + 3]
        assert ":" in block.split("text(")[1], name


# ── wiring pins (scheduler / digest / migration) ──────────────────────────


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_long_plays_weekly_is_registered_after_the_sweep() -> None:
    src = _read("intelligence/scheduler.py")
    assert '_sched.every().sunday.at("05:30").do(_long_plays_weekly)' in src
    assert "from intelligence.long_plays import build_long_plays_board, persist_board" in src
    i_sweep = src.index('_sched.every().sunday.at("05:00").do(_long_horizon_sweep)')
    i_plays = src.index('_sched.every().sunday.at("05:30").do(_long_plays_weekly)')
    assert i_sweep < i_plays
    assert "05:00" < "05:30"


def test_daily_digest_has_a_guarded_long_plays_section() -> None:
    src = _read("alerts/email.py")
    i_def = src.index("def daily_digest")
    block = src[i_def : src.index("def _section_code_block")]
    assert "from intelligence.long_plays import entry_first, load_latest_board" in block
    assert '"Long plays"' in block
    assert "not forecasts" in block


def test_migration_0059_matches_the_module_ddl() -> None:
    sql = _read("migrations/0059_long_plays_board.sql")
    assert "CREATE TABLE IF NOT EXISTS long_plays_board" in sql
    for col in ("as_of", "generated_at", "universe_size", "candidates", "stand_down_reason", "method_notes"):
        assert col in sql
    assert "GRANT ALL ON long_plays_board TO grid;" in sql
    assert "GRANT USAGE, SELECT ON SEQUENCE long_plays_board_id_seq TO grid;" in sql
    ddl = str(lp._ENSURE_TABLE_SQL)
    for col in ("as_of", "generated_at", "universe_size", "candidates", "stand_down_reason", "method_notes"):
        assert col in ddl
