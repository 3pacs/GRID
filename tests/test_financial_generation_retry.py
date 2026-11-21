"""Offline retry-status regressions for source-generation publication races."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from intelligence import fundamental_divergence as fd
from intelligence import sector_health as sh


A = "a" * 32
B = "b" * 32


def candidate(generation=A, ticker="NVDA"):
    return {"ticker": ticker, "as_of": date(2026, 11, 8), "sector": "Technology",
            "fundamental_score": 70, "price_score": 50, "divergence": 20,
            "classification": "aligned", "narrative": "Fixture", "sec_generation": generation}


@pytest.fixture
def divergence_job(monkeypatch):
    engine = MagicMock()
    writer = MagicMock(return_value=1)
    generation = MagicMock(return_value=A)
    monkeypatch.setattr(fd, "compute_divergence", lambda *args, **kwargs: [candidate()])
    monkeypatch.setattr(fd, "_table_exists", lambda *args: True)
    monkeypatch.setattr(fd, "get_financial_generation", generation)
    monkeypatch.setattr(fd, "_upsert_divergence_chunk", writer)
    return engine, writer, generation


@pytest.mark.parametrize("written", [0, -1])
def test_divergence_filtered_write_raises_instead_of_completing_day(divergence_job, written):
    engine, writer, _generation = divergence_job
    writer.return_value = written
    with pytest.raises(fd.FinancialGenerationRetry, match="rejected"):
        fd.snapshot_all(engine)
    assert engine.begin.return_value.__exit__.call_args.args[0] is fd.FinancialGenerationRetry


def test_divergence_partial_generation_filter_is_retryable(divergence_job, monkeypatch):
    engine, writer, _generation = divergence_job
    monkeypatch.setattr(fd, "compute_divergence", lambda *args, **kwargs: [candidate(), candidate(ticker="AAPL")])
    writer.return_value = 1
    with pytest.raises(fd.FinancialGenerationRetry):
        fd.snapshot_all(engine)


def test_divergence_changed_before_write_never_writes(divergence_job):
    engine, writer, generation = divergence_job
    generation.return_value = B
    with pytest.raises(fd.FinancialGenerationRetry):
        fd.snapshot_all(engine)
    writer.assert_not_called()


@pytest.mark.parametrize("last", [B, RuntimeError("generation read failed")])
def test_divergence_postcommit_change_or_error_remains_retryable(divergence_job, last):
    engine, writer, generation = divergence_job
    generation.side_effect = [A, A, last]
    with pytest.raises(fd.FinancialGenerationRetry):
        fd.snapshot_all(engine)
    writer.assert_called_once()
    # The insert committed, but is not reported as successful current output.
    assert engine.begin.return_value.__exit__.call_args.args[0] is None


def test_divergence_mixed_candidates_never_write(divergence_job, monkeypatch):
    engine, writer, _generation = divergence_job
    monkeypatch.setattr(fd, "compute_divergence", lambda *args, **kwargs: [candidate(), candidate(B, "AAPL")])
    with pytest.raises(fd.FinancialGenerationRetry, match="mixed"):
        fd.snapshot_all(engine)
    writer.assert_not_called()


def test_divergence_legitimate_empty_universe_stays_successful(monkeypatch):
    monkeypatch.setattr(fd, "_load_universe", lambda: [])
    result = fd.snapshot_all(MagicMock())
    assert result["rows"] == result["written"] == 0


def sector_result(generation=A):
    return {"score": 70.0, "trend_30d": None, "components": {"margin": 0.7},
            "status": "ok", "sec_generation": generation}


@pytest.fixture
def sector_job(monkeypatch):
    import analysis.sector_map as sector_map
    monkeypatch.setattr(sector_map, "SECTOR_MAP", {"Technology": {}})
    engine = MagicMock()
    conn = engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.rowcount = 1
    conn.execute.return_value.scalar_one_or_none.return_value = A
    generation = MagicMock(return_value=A)
    compute = MagicMock(return_value=sector_result())
    monkeypatch.setattr(sh, "get_financial_generation", generation)
    monkeypatch.setattr(sh, "compute_sector_health", compute)
    return engine, conn, generation, compute


def test_sector_source_failure_propagates_before_any_write(sector_job):
    engine, conn, generation, compute = sector_job
    generation.side_effect = RuntimeError("source unavailable")
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.snapshot_all_sectors(engine)
    compute.assert_not_called()
    conn.execute.assert_not_called()


def test_sector_compute_generation_failure_is_not_no_eligible(sector_job):
    engine, conn, _generation, compute = sector_job
    compute.side_effect = sh.FinancialGenerationRetry("changed during computation")
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.snapshot_all_sectors(engine)
    conn.execute.assert_not_called()


@pytest.mark.parametrize("replacement", [None, B])
def test_sector_zero_write_without_current_replacement_is_retryable(sector_job, replacement):
    engine, conn, _generation, _compute = sector_job
    conn.execute.return_value.rowcount = 0
    conn.execute.return_value.scalar_one_or_none.return_value = replacement
    with pytest.raises(sh.FinancialGenerationRetry, match="replacement"):
        sh.snapshot_all_sectors(engine)


def test_sector_same_generation_replacement_is_genuinely_superseded(sector_job):
    engine, conn, _generation, _compute = sector_job
    conn.execute.return_value.rowcount = 0
    result = sh.snapshot_all_sectors(engine)
    assert result["snapshots_stale_skipped"] == 1
    assert result["snapshots_written"] == 0
    assert result["upsert_failed"] == 0


def test_sector_gate_rejection_from_head_change_is_not_superseded(sector_job):
    engine, conn, generation, _compute = sector_job
    conn.execute.return_value.rowcount = 0
    generation.side_effect = [A, A, B]
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.snapshot_all_sectors(engine)


@pytest.mark.parametrize("last", [B, RuntimeError("source read failed")])
def test_sector_postcommit_change_or_error_remains_retryable(sector_job, last):
    engine, _conn, generation, _compute = sector_job
    generation.side_effect = [A, A, A, last]
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.snapshot_all_sectors(engine)
    assert engine.begin.return_value.__exit__.call_args.args[0] is None


def test_sector_attempt_end_change_does_not_report_success(sector_job):
    engine, _conn, generation, _compute = sector_job
    generation.side_effect = [A, A, A, A, B]
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.snapshot_all_sectors(engine)


def test_sector_mixed_generation_run_is_retryable(sector_job, monkeypatch):
    import analysis.sector_map as sector_map
    engine, _conn, _generation, compute = sector_job
    monkeypatch.setattr(sector_map, "SECTOR_MAP", {"Technology": {}, "Energy": {}})
    compute.side_effect = [sector_result(), sector_result(B)]
    with pytest.raises(sh.FinancialGenerationRetry, match="spans"):
        sh.snapshot_all_sectors(engine)


def test_sector_legitimate_no_data_is_distinct_from_source_failure(sector_job):
    engine, conn, _generation, compute = sector_job
    compute.return_value = sh._unavailable("Technology", "no underlying data")
    result = sh.snapshot_all_sectors(engine)
    assert result["snapshots_skipped_unavailable"] == 1
    assert result["snapshots_stale_skipped"] == 0
    assert result["upsert_failed"] == 0
    conn.execute.assert_not_called()


def test_no_data_computation_still_checks_source_stability(monkeypatch):
    monkeypatch.setattr(sh, "get_financial_generation", MagicMock(side_effect=[A, B]))
    monkeypatch.setattr(sh, "_SCORERS", {key: lambda *args: None for key in sh.WEIGHTS})
    with pytest.raises(sh.FinancialGenerationRetry):
        sh.compute_sector_health(MagicMock(), "Technology")


def test_scheduler_keeps_sector_day_due_after_real_generation_rejection(sector_job):
    from scripts import hermes_operator as ho
    from scripts.hermes_health import OperatorState
    engine, conn, _generation, _compute = sector_job
    conn.execute.return_value.rowcount = 0
    conn.execute.return_value.scalar_one_or_none.return_value = None
    state = OperatorState()
    now = datetime(2026, 11, 8, 10, tzinfo=timezone.utc)
    prior = now - timedelta(days=1)
    state.last_sector_health = prior
    results = {}
    ho._maybe_run_sector_health_snapshot(engine, state, now, results)
    assert results["sector_health_snapshot"]["outcome"] == "failure"
    assert state.last_sector_health == prior
    assert ho.daily_task_due(state.last_sector_health, now, boundary_hour=3)


def test_daily_intel_divergence_generation_rejection_propagates(divergence_job):
    from scripts import hermes_operator as ho
    from scripts.hermes_health import OperatorState
    engine, writer, _generation = divergence_job
    writer.return_value = 0
    results = {}
    with pytest.raises(fd.FinancialGenerationRetry):
        ho._daily_intel_fundamental_divergence(engine, OperatorState(), datetime.now(timezone.utc), results)
    assert "fundamental_divergence" not in results
