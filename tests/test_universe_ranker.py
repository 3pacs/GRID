"""
Tests for ``intelligence.universe_ranker``.

Covers:
- Pure helpers (composite_score, classify_regime_signature,
  detect_sector_concentration, rank_tickers, build_narrative).
- Dataclass ``to_dict()`` roundtrips.
- Universe constants are non-empty tuples.
- ``_run_one_ticker`` happy path + error path (mocked should_i_trade).
- ``_get_ticker_sector`` known / unknown handling (mocked loader).
- ``rank_universe`` with custom list, named universe, partial failure,
  empty universe, and parallel mode (verified by patching
  ``ThreadPoolExecutor``).
- ``persist_ranking`` try/except — DB error never raises.

All external modules (``decision_gateway``, ``sector_networks.loader``)
are patched via ``unittest.mock.patch.dict(sys.modules, ...)`` so the
tests run without the live GRID stack.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from intelligence import universe_ranker as ur
from intelligence.universe_ranker import (
    SectorDistribution,
    TickerRanking,
    UNIVERSE_NASDAQ100,
    UNIVERSE_SP500,
    UniverseRankingReport,
    build_narrative,
    classify_regime_signature,
    composite_score,
    detect_sector_concentration,
    persist_ranking,
    rank_tickers,
    rank_universe,
)


# ── Helpers ──────────────────────────────────────────────────────────────


def _make_ranking(
    ticker: str = "AAPL",
    sector: str | None = "tech",
    verdict: str = "high",
    aggregate_conviction: float = 1.2,
    robustness_score: float = 1.0,
    robustness_label: str | None = "robust",
    composite: float | None = None,
    has_ticket: bool = True,
    error: str | None = None,
) -> TickerRanking:
    if composite is None:
        composite = composite_score(aggregate_conviction, robustness_score)
    return TickerRanking(
        ticker=ticker,
        sector=sector,
        verdict=verdict,
        aggregate_conviction=aggregate_conviction,
        robustness_score=robustness_score,
        robustness_label=robustness_label,
        composite_score=composite,
        has_ticket=has_ticket,
        error=error,
    )


def _make_should_i_trade_response(
    ticker: str = "AAPL",
    verdict: str = "high",
    conviction: float = 1.2,
    robustness: float = 0.95,
    robustness_label: str = "robust",
    has_ticket: bool = True,
) -> MagicMock:
    prov = MagicMock()
    prov.aggregate_conviction = conviction
    stress = MagicMock()
    stress.robustness_score = robustness
    stress.robustness_label = robustness_label
    ticket = MagicMock() if has_ticket else None
    resp = MagicMock()
    resp.ticker = ticker
    resp.unified_verdict = verdict
    resp.provenance_report = prov
    resp.stress_report = stress
    resp.trade_ticket = ticket
    return resp


# ── Pure helpers ─────────────────────────────────────────────────────────


class TestCompositeScore:
    def test_happy_path(self) -> None:
        # 1.2 × (0.5 + 0.5 × 1.0) = 1.2
        assert composite_score(1.2, 1.0) == pytest.approx(1.2)

    def test_zero_robustness_halves_score(self) -> None:
        # 1.2 × 0.5 = 0.6
        assert composite_score(1.2, 0.0) == pytest.approx(0.6)

    def test_upper_clamp(self) -> None:
        # 3.0 × 1.0 = 3.0 → clamped to 1.5
        assert composite_score(3.0, 1.0) == pytest.approx(1.5)

    def test_lower_clamp_and_non_numeric(self) -> None:
        assert composite_score(-5.0, 0.5) == 0.0
        assert composite_score("bad", "worse") == 0.0  # type: ignore[arg-type]

    def test_robustness_above_one_is_clamped(self) -> None:
        # 1.0 × (0.5 + 0.5 × 1) since robustness clamps to 1
        assert composite_score(1.0, 5.0) == pytest.approx(1.0)


class TestClassifyRegimeSignature:
    def test_trending_when_above_20pct(self) -> None:
        assert (
            classify_regime_signature({"total_count": 100, "high_count": 25})
            == "trending"
        )

    def test_divergent_when_below_5pct(self) -> None:
        assert (
            classify_regime_signature({"total_count": 100, "high_count": 2})
            == "divergent"
        )

    def test_mixed_when_middle(self) -> None:
        assert (
            classify_regime_signature({"total_count": 100, "high_count": 10})
            == "mixed"
        )

    def test_empty_total(self) -> None:
        assert classify_regime_signature({"total_count": 0, "high_count": 0}) == "mixed"


class TestDetectSectorConcentration:
    def test_one_sector_with_40pct_of_high(self) -> None:
        dists = [
            SectorDistribution("tech", 4, 2, 1, 3, 0.4, False),
            SectorDistribution("finance", 2, 1, 1, 1, 0.4, False),
            SectorDistribution("energy", 2, 0, 1, 1, 0.5, False),
            SectorDistribution("health", 2, 0, 0, 0, 1.0, False),
        ]
        alerts = detect_sector_concentration(dists, total_high=10)
        assert len(alerts) == 1
        assert "tech" in alerts[0]

    def test_no_alerts_when_spread_even(self) -> None:
        dists = [
            SectorDistribution("a", 2, 0, 0, 0, 1.0, False),
            SectorDistribution("b", 2, 0, 0, 0, 1.0, False),
            SectorDistribution("c", 2, 0, 0, 0, 1.0, False),
            SectorDistribution("d", 2, 0, 0, 0, 1.0, False),
            SectorDistribution("e", 2, 0, 0, 0, 1.0, False),
        ]
        assert detect_sector_concentration(dists, total_high=10) == []

    def test_no_alerts_when_total_high_is_zero(self) -> None:
        dists = [SectorDistribution("tech", 0, 5, 2, 3, 0.0, False)]
        assert detect_sector_concentration(dists, total_high=0) == []


class TestRankTickers:
    def test_filters_no_trade_and_low(self) -> None:
        rankings = [
            _make_ranking("A", verdict="high", composite=1.2),
            _make_ranking("B", verdict="no_trade", composite=0.0),
            _make_ranking("C", verdict="low", composite=0.3),
            _make_ranking("D", verdict="medium", composite=0.8),
        ]
        top = rank_tickers(rankings, k=10)
        assert [r.ticker for r in top] == ["A", "D"]

    def test_sort_desc_by_composite(self) -> None:
        rankings = [
            _make_ranking("C", verdict="high", composite=0.9),
            _make_ranking("A", verdict="high", composite=1.3),
            _make_ranking("B", verdict="medium", composite=1.1),
        ]
        top = rank_tickers(rankings, k=5)
        assert [r.ticker for r in top] == ["A", "B", "C"]

    def test_top_k_truncation(self) -> None:
        rankings = [
            _make_ranking(f"T{i}", verdict="high", composite=1.5 - i * 0.01)
            for i in range(30)
        ]
        top = rank_tickers(rankings, k=5)
        assert len(top) == 5
        assert top[0].ticker == "T0"

    def test_k_zero_returns_empty(self) -> None:
        rankings = [_make_ranking("A", verdict="high", composite=1.2)]
        assert rank_tickers(rankings, k=0) == []


# ── Dataclass roundtrips ─────────────────────────────────────────────────


class TestDataclassRoundtrips:
    def test_ticker_ranking_to_dict(self) -> None:
        r = _make_ranking("MSFT", sector="tech", verdict="medium")
        d = r.to_dict()
        assert d["ticker"] == "MSFT"
        assert d["sector"] == "tech"
        assert d["verdict"] == "medium"
        assert "composite_score" in d

    def test_sector_distribution_to_dict(self) -> None:
        sd = SectorDistribution("tech", 5, 3, 2, 1, 0.45, True)
        d = sd.to_dict()
        assert d["sector"] == "tech"
        assert d["high_count"] == 5
        assert d["concentrated"] is True

    def test_universe_report_to_dict(self) -> None:
        report = UniverseRankingReport(
            universe_name="TEST",
            tickers_attempted=3,
            tickers_succeeded=3,
            top_k=[_make_ranking("AAPL")],
            all_rankings=[_make_ranking("AAPL")],
            sector_distributions=[
                SectorDistribution("tech", 1, 0, 0, 0, 1.0, False)
            ],
            concentration_alerts=["alert"],
            regime_signature="trending",
            narrative="hi",
        )
        d = report.to_dict()
        assert d["universe_name"] == "TEST"
        assert d["regime_signature"] == "trending"
        assert len(d["top_k"]) == 1
        assert d["concentration_alerts"] == ["alert"]


# ── Narrative ────────────────────────────────────────────────────────────


class TestBuildNarrative:
    def test_mentions_top_three_tickers(self) -> None:
        top = [
            _make_ranking("NVDA", verdict="high", composite=1.4),
            _make_ranking("AMD", verdict="high", composite=1.3),
            _make_ranking("INTC", verdict="medium", composite=1.1),
            _make_ranking("AAPL", verdict="medium", composite=1.0),
        ]
        report = UniverseRankingReport(
            universe_name="SP500",
            tickers_attempted=10,
            tickers_succeeded=10,
            top_k=top,
            all_rankings=top,
            sector_distributions=[],
            concentration_alerts=[],
            regime_signature="trending",
            narrative="",
        )
        narrative = build_narrative(report)
        assert narrative
        assert "NVDA" in narrative
        assert "AMD" in narrative
        assert "INTC" in narrative
        assert "trending" in narrative

    def test_empty_universe_narrative(self) -> None:
        report = UniverseRankingReport(
            universe_name="EMPTY",
            tickers_attempted=0,
            tickers_succeeded=0,
            top_k=[],
            all_rankings=[],
            sector_distributions=[],
            concentration_alerts=[],
            regime_signature="mixed",
            narrative="",
        )
        assert "no tickers" in build_narrative(report).lower()

    def test_no_actionable_verdicts_stand_down(self) -> None:
        report = UniverseRankingReport(
            universe_name="SP500",
            tickers_attempted=5,
            tickers_succeeded=5,
            top_k=[],
            all_rankings=[_make_ranking("A", verdict="no_trade")],
            sector_distributions=[],
            concentration_alerts=[],
            regime_signature="divergent",
            narrative="",
        )
        assert "stand down" in build_narrative(report).lower()


# ── Universe constants ──────────────────────────────────────────────────


def test_universe_sp500_non_empty_tuple() -> None:
    assert isinstance(UNIVERSE_SP500, tuple)
    assert len(UNIVERSE_SP500) > 0
    assert all(isinstance(t, str) and t for t in UNIVERSE_SP500)


def test_universe_nasdaq100_non_empty_tuple() -> None:
    assert isinstance(UNIVERSE_NASDAQ100, tuple)
    assert len(UNIVERSE_NASDAQ100) > 0


# ── _run_one_ticker ──────────────────────────────────────────────────────


def _install_fake_decision_gateway(monkeypatch, side_effect):
    """Install a fake ``intelligence.decision_gateway`` module with a
    ``should_i_trade`` attribute that is either callable or raises.
    """
    mod = types.ModuleType("intelligence.decision_gateway")
    mod.should_i_trade = side_effect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "intelligence.decision_gateway", mod)


class TestRunOneTicker:
    def test_happy_path(self, monkeypatch) -> None:
        resp = _make_should_i_trade_response(
            "AAPL", verdict="high", conviction=1.25, robustness=0.95
        )
        _install_fake_decision_gateway(
            monkeypatch, lambda engine, ticker, **kw: resp
        )
        # sector loader unavailable → sector None, no crash
        monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: "tech")

        result = ur._run_one_ticker(
            MagicMock(), "AAPL", account_size_usd=100_000.0
        )
        assert result.ticker == "AAPL"
        assert result.sector == "tech"
        assert result.verdict == "high"
        assert result.aggregate_conviction == pytest.approx(1.25)
        assert result.robustness_score == pytest.approx(0.95)
        assert result.has_ticket is True
        assert result.error is None
        assert result.composite_score > 0

    def test_should_i_trade_raises(self, monkeypatch) -> None:
        def boom(*args, **kwargs):
            raise RuntimeError("oracle exploded")

        _install_fake_decision_gateway(monkeypatch, boom)
        monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: None)

        result = ur._run_one_ticker(
            MagicMock(), "TSLA", account_size_usd=100_000.0
        )
        assert result.ticker == "TSLA"
        assert result.verdict == "no_trade"
        assert result.composite_score == 0.0
        assert result.error is not None
        assert "oracle exploded" in result.error

    def test_decision_gateway_import_fails(self, monkeypatch) -> None:
        # Sabotage the import by installing a module that raises on attr
        types.ModuleType("intelligence.decision_gateway")

        def raise_on_access(name):
            raise ImportError("cannot import should_i_trade")

        class BadMod:
            def __getattr__(self, item):
                raise ImportError(f"cannot get {item}")

        monkeypatch.setitem(
            sys.modules, "intelligence.decision_gateway", BadMod()
        )
        monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: None)
        result = ur._run_one_ticker(
            MagicMock(), "BAD", account_size_usd=100_000.0
        )
        assert result.verdict == "no_trade"
        assert result.error is not None


# ── _get_ticker_sector ───────────────────────────────────────────────────


class TestGetTickerSector:
    def test_known_ticker(self, monkeypatch) -> None:
        fake_loader = types.ModuleType("intelligence.sector_networks.loader")
        fake_loader.list_sectors = lambda: ["tech", "energy"]  # type: ignore[attr-defined]

        def fake_get_actors(sector: str):
            if sector == "tech":
                return [{"ticker": "AAPL", "name": "Apple"}]
            return [{"ticker": "XOM", "name": "Exxon"}]

        fake_loader.get_actors = fake_get_actors  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "intelligence.sector_networks.loader", fake_loader
        )
        assert ur._get_ticker_sector("AAPL") == "tech"
        assert ur._get_ticker_sector("XOM") == "energy"

    def test_unknown_ticker_returns_none(self, monkeypatch) -> None:
        fake_loader = types.ModuleType("intelligence.sector_networks.loader")
        fake_loader.list_sectors = lambda: ["tech"]  # type: ignore[attr-defined]
        fake_loader.get_actors = lambda s: [{"ticker": "AAPL"}]  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "intelligence.sector_networks.loader", fake_loader
        )
        assert ur._get_ticker_sector("UNKNOWN") is None

    def test_empty_ticker_returns_none(self) -> None:
        assert ur._get_ticker_sector("") is None


# ── rank_universe ────────────────────────────────────────────────────────


@pytest.fixture
def patched_decision_gateway(monkeypatch):
    """Default should_i_trade mock: returns a HIGH verdict for every call."""
    def fake_should_i_trade(engine, ticker, **kwargs):
        return _make_should_i_trade_response(
            ticker=ticker, verdict="high", conviction=1.2, robustness=1.0
        )

    _install_fake_decision_gateway(monkeypatch, fake_should_i_trade)
    monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: "tech")
    return fake_should_i_trade


class TestRankUniverse:
    def test_custom_ticker_list(self, patched_decision_gateway) -> None:
        report = rank_universe(
            MagicMock(),
            ["A", "B", "C"],
            account_size_usd=100_000.0,
            top_k=5,
        )
        assert report.universe_name == "custom"
        assert report.tickers_attempted == 3
        assert report.tickers_succeeded == 3
        assert len(report.top_k) == 3
        assert report.narrative

    def test_named_universe_string(self, patched_decision_gateway) -> None:
        report = rank_universe(
            MagicMock(), "SP500", account_size_usd=100_000.0, top_k=5
        )
        assert report.universe_name == "SP500"
        assert report.tickers_attempted == len(UNIVERSE_SP500)
        assert report.tickers_succeeded == len(UNIVERSE_SP500)
        # All high → trending
        assert report.regime_signature == "trending"

    def test_partial_failure(self, monkeypatch) -> None:
        call_count = {"n": 0}

        def flaky(engine, ticker, **kwargs):
            call_count["n"] += 1
            if call_count["n"] % 2 == 0:
                raise RuntimeError("transient")
            return _make_should_i_trade_response(
                ticker=ticker, verdict="high"
            )

        _install_fake_decision_gateway(monkeypatch, flaky)
        monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: None)

        report = rank_universe(
            MagicMock(), ["A", "B", "C", "D"], top_k=10
        )
        assert report.tickers_attempted == 4
        assert report.tickers_succeeded == 2
        # Half failed but report is still valid
        assert len(report.all_rankings) == 4
        assert any(r.error is not None for r in report.all_rankings)

    def test_empty_universe(self, patched_decision_gateway) -> None:
        report = rank_universe(MagicMock(), [], top_k=5)
        assert report.tickers_attempted == 0
        assert report.tickers_succeeded == 0
        assert report.top_k == []
        assert "no tickers" in report.narrative.lower()

    def test_parallel_true_uses_thread_pool(
        self, patched_decision_gateway, monkeypatch
    ) -> None:
        import intelligence.universe_ranker as ur_mod
        sentinel = {"used": False, "max_workers": None}

        real_executor = ur_mod.ThreadPoolExecutor

        class SpyExecutor(real_executor):  # type: ignore[misc, valid-type]
            def __init__(self, *args, **kwargs):
                sentinel["used"] = True
                sentinel["max_workers"] = kwargs.get("max_workers")
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(ur_mod, "ThreadPoolExecutor", SpyExecutor)
        report = rank_universe(
            MagicMock(),
            ["A", "B", "C", "D", "E"],
            top_k=10,
            parallel=True,
        )
        assert sentinel["used"] is True
        assert sentinel["max_workers"] is not None
        assert sentinel["max_workers"] <= ur_mod.MAX_PARALLEL_WORKERS
        assert report.tickers_succeeded == 5

    def test_regime_signature_divergent(self, monkeypatch) -> None:
        # 1 high out of 50 → 2% < 5% → divergent
        def mostly_low(engine, ticker, **kwargs):
            if ticker == "A":
                return _make_should_i_trade_response(
                    ticker=ticker, verdict="high"
                )
            return _make_should_i_trade_response(
                ticker=ticker, verdict="low", conviction=0.4, robustness=0.6
            )

        _install_fake_decision_gateway(monkeypatch, mostly_low)
        monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: None)
        tickers = ["A"] + [f"T{i}" for i in range(49)]
        report = rank_universe(MagicMock(), tickers, top_k=5)
        assert report.regime_signature == "divergent"


# ── persist_ranking ──────────────────────────────────────────────────────


class TestPersistRanking:
    def test_db_error_does_not_raise(self) -> None:
        engine = MagicMock()
        engine.begin.side_effect = RuntimeError("db down")
        report = UniverseRankingReport(
            universe_name="X",
            tickers_attempted=1,
            tickers_succeeded=1,
            top_k=[_make_ranking("AAPL")],
            all_rankings=[_make_ranking("AAPL")],
            sector_distributions=[],
            concentration_alerts=[],
            regime_signature="mixed",
            narrative="ok",
        )
        result = persist_ranking(engine, report)
        assert result == -1


# ── main() CLI entrypoint ────────────────────────────────────────────────


class TestMainCli:
    def test_engine_bootstrap_failure_logs_and_writes_stderr(
        self, monkeypatch, capsys
    ) -> None:
        """Engine bootstrap failure must surface as log.error AND stderr
        (not silent stdout) so it lands in errors.jsonl and the CLI user
        sees the failure on the proper stream."""
        fake_db = types.ModuleType("db")

        def _boom() -> object:
            raise RuntimeError("engine offline")

        fake_db.get_engine = _boom  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "db", fake_db)

        rc = ur.main(["--universe", "AAPL"])

        assert rc == 1
        captured = capsys.readouterr()
        assert "engine offline" in captured.err
        assert "engine bootstrap failed" in captured.err
        # The error message must not leak to stdout — stdout is reserved
        # for the narrative/ranking report.
        assert "engine bootstrap failed" not in captured.out
