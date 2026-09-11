"""Why an empty sweep is empty (``verdict_counts``, 2026-09-10).

The first 90 d sweep persisted "33/33 tickers scored, no high/medium
verdicts found — stand down" and nothing else. That row cannot answer the
next question — were the verdicts computed and weak, or did the decision
stack never produce a prediction? — so the Long Plays coverage gate sat on
an unexplained ``SWEEP_NOTE_EMPTY`` and diagnosing it needed a live probe
on the box. These tests pin the histogram, the degraded-stage tally, the
narrative that reports them, and the column they persist to.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

from intelligence import universe_ranker as ur


def _ranking(
    ticker: str,
    verdict: str,
    *,
    error: str | None = None,
    stage_errors: tuple[str, ...] = (),
    score: float = 0.0,
) -> ur.TickerRanking:
    return ur.TickerRanking(
        ticker=ticker,
        sector="tech",
        verdict=verdict,
        aggregate_conviction=0.0,
        robustness_score=0.0,
        robustness_label=None,
        composite_score=score,
        has_ticket=False,
        error=error,
        stage_errors=stage_errors,
    )


# ── verdict_counts ────────────────────────────────────────────────────────


def test_verdict_counts_histograms_verdicts_errors_and_stages() -> None:
    rankings = [
        _ranking("A", "no_trade", stage_errors=("prediction", "trade_ticket")),
        _ranking("B", "no_trade", stage_errors=("prediction",)),
        _ranking("C", "low", stage_errors=("trade_ticket",)),
        _ranking("D", "medium", score=0.9),
        _ranking("E", "high", score=1.2),
        _ranking("F", "no_trade", error="should_i_trade raised: boom"),
    ]
    counts = ur.verdict_counts(rankings)
    assert counts["no_trade"] == 3 and counts["low"] == 1
    assert counts["medium"] == 1 and counts["high"] == 1
    assert counts["errors"] == 1
    assert counts["rankable"] == 2
    assert counts["stage_errors"] == {"prediction": 2, "trade_ticket": 2}
    json.dumps(counts)


def test_verdict_counts_of_an_empty_sweep_is_all_zero() -> None:
    counts = ur.verdict_counts([])
    assert counts["rankable"] == 0 and counts["errors"] == 0
    assert counts["stage_errors"] == {}
    assert all(counts[v] == 0 for v in ur._VERDICT_ORDER)


def test_ticker_ranking_serializes_stage_errors() -> None:
    row = _ranking("A", "no_trade", stage_errors=("prediction",)).to_dict()
    assert row["stage_errors"] == ["prediction"]
    json.dumps(row)
    # the field is optional — legacy construction still works
    assert ur.TickerRanking(
        ticker="B", sector=None, verdict="low", aggregate_conviction=0.0,
        robustness_score=0.0, robustness_label=None, composite_score=0.0, has_ticket=False,
    ).stage_errors == ()


# ── the narrative says what the verdicts were ─────────────────────────────


def _report(rankings: list[ur.TickerRanking]) -> ur.UniverseRankingReport:
    return ur.UniverseRankingReport(
        universe_name="custom",
        tickers_attempted=len(rankings),
        tickers_succeeded=len(rankings),
        top_k=ur.rank_tickers(rankings, k=25),
        all_rankings=rankings,
        sector_distributions=[],
        concentration_alerts=[],
        regime_signature="divergent",
        narrative="",
        horizon_days=90,
        verdict_counts=ur.verdict_counts(rankings),
    )


def test_empty_sweep_narrative_reports_the_histogram_and_worst_stages() -> None:
    rankings = [
        _ranking(f"T{i}", "no_trade", stage_errors=("prediction", "trade_ticket"))
        for i in range(20)
    ] + [_ranking("U1", "low", stage_errors=("trade_ticket",))]
    text = ur.build_narrative(_report(rankings))
    assert "no high/medium verdicts found — stand down." in text
    assert "verdicts: no_trade=20, low=1" in text
    assert "Degraded stages:" in text
    assert "trade_ticket on 21" in text and "prediction on 20" in text


def test_empty_sweep_narrative_without_counts_keeps_the_old_sentence() -> None:
    report = ur.UniverseRankingReport(
        universe_name="custom", tickers_attempted=3, tickers_succeeded=3,
        top_k=[], all_rankings=[], sector_distributions=[], concentration_alerts=[],
        regime_signature="divergent", narrative="",
    )
    text = ur.build_narrative(report)
    assert text.endswith("no high/medium verdicts found — stand down.")


def test_a_ranked_sweep_narrative_is_unchanged() -> None:
    rankings = [_ranking("NVDA", "high", score=1.3), _ranking("AMD", "medium", score=0.8)]
    text = ur.build_narrative(_report(rankings))
    assert "top: NVDA(high:1.30), AMD(medium:0.80)." in text
    assert "Degraded stages" not in text


# ── rank_universe carries the counts through ──────────────────────────────


def test_rank_universe_attaches_verdict_counts(monkeypatch: Any) -> None:
    def fake_run(engine: Any, ticker: str, **kwargs: Any) -> ur.TickerRanking:
        return _ranking(ticker, "no_trade", stage_errors=("prediction",))

    monkeypatch.setattr(ur, "_run_one_ticker", fake_run)
    report = ur.rank_universe(MagicMock(), ["AAA", "BBB"], horizon_days=90)
    assert report.top_k == []
    assert report.verdict_counts["no_trade"] == 2
    assert report.verdict_counts["stage_errors"] == {"prediction": 2}
    # _with_narrative rebuilds the frozen dataclass — the counts must survive
    assert "verdicts: no_trade=2" in report.narrative
    json.dumps(report.to_dict())


def test_run_one_ticker_captures_the_stage_errors(monkeypatch: Any) -> None:
    class FakeResponse:
        unified_verdict = "low"
        provenance_report = None
        stress_report = None
        trade_ticket = None
        stage_errors = {"trade_ticket": "verdict=low below ticket threshold", "prediction": "oracle.predict failed"}

    import intelligence.decision_gateway as dg

    monkeypatch.setattr(dg, "should_i_trade", lambda *a, **k: FakeResponse())
    monkeypatch.setattr(ur, "_get_ticker_sector", lambda t: "tech")
    out = ur._run_one_ticker(MagicMock(), "NVDA", account_size_usd=1000.0, horizon_days=90)
    assert out.verdict == "low" and out.error is None
    assert out.stage_errors == ("prediction", "trade_ticket")  # sorted


# ── persistence ───────────────────────────────────────────────────────────


def _engine(first: Any = None) -> tuple[MagicMock, MagicMock]:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.first.return_value = first
    engine = MagicMock()
    engine.begin.return_value = conn
    engine.connect.return_value = conn
    return engine, conn


def test_persist_ranking_binds_verdict_counts_and_migrates_the_column() -> None:
    rankings = [_ranking("A", "no_trade", stage_errors=("prediction",))]
    engine, conn = _engine(first=(11,))
    assert ur.persist_ranking(engine, _report(rankings)) == 11

    ddl = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any("ADD COLUMN IF NOT EXISTS verdict_counts JSONB" in d for d in ddl)

    insert = [c for c in conn.execute.call_args_list if "INSERT INTO universe_ranking_history" in str(c.args[0])][0]
    stmt, params = insert.args
    assert ":verdict_counts" in str(stmt)
    assert json.loads(params["verdict_counts"])["no_trade"] == 1
    assert json.loads(params["verdict_counts"])["stage_errors"] == {"prediction": 1}


def test_read_back_exposes_verdict_counts_and_tolerates_pre_migration_rows() -> None:
    base = (
        7, datetime(2026, 9, 6, 5, 0, tzinfo=timezone.utc), "custom", 90, 33, 33,
        "divergent", json.dumps([]), json.dumps([]), json.dumps([]), "stand down.",
    )
    counts = {"no_trade": 33, "rankable": 0, "stage_errors": {"prediction": 33}}
    out = ur._ranking_row_to_dict(base + (json.dumps(counts),))
    assert out["verdict_counts"] == counts
    # JSONB may arrive parsed
    assert ur._ranking_row_to_dict(base + (counts,))["verdict_counts"] == counts
    # NULL column, and a short pre-migration row, both degrade to {}
    assert ur._ranking_row_to_dict(base + (None,))["verdict_counts"] == {}
    assert ur._ranking_row_to_dict(base)["verdict_counts"] == {}
    # the other JSON columns still default to []
    assert ur._ranking_row_to_dict(base)["top_k"] == []


def test_read_back_sql_selects_the_column() -> None:
    for sql in (str(ur._LATEST_RANKING_SQL), str(ur._PAGE_RANKINGS_SQL)):
        assert "verdict_counts" in sql
