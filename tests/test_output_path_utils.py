from __future__ import annotations

import json
from pathlib import Path

from backtest.engine import PitchBacktester
from ollama.market_briefing import MarketBriefingEngine
from outputs.path_utils import ensure_output_dir


def test_ensure_output_dir_falls_back_from_dangling_symlink(tmp_path: Path) -> None:
    broken = tmp_path / "market_briefings"
    broken.symlink_to(tmp_path / "missing-target")

    resolved = ensure_output_dir(broken)

    assert resolved == tmp_path / "_market_briefings_local"
    assert resolved.is_dir()


def test_market_briefing_engine_uses_local_fallback_for_dangling_symlink(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broken = tmp_path / "market_briefings"
    broken.symlink_to(tmp_path / "missing-target")
    monkeypatch.setattr("ollama.market_briefing._BRIEFING_DIR", broken)

    engine = MarketBriefingEngine(ollama_client=type("Client", (), {})(), db_engine=None)

    assert engine.output_dir == tmp_path / "_market_briefings_local"
    assert engine.get_latest_briefing("hourly") is None


def test_pitch_backtester_reads_results_from_local_fallback_for_dangling_symlink(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broken = tmp_path / "backtest"
    broken.symlink_to(tmp_path / "missing-target")
    fallback = tmp_path / "_backtest_local"
    fallback.mkdir()
    (fallback / "backtest_results.json").write_text(json.dumps({"status": "ok"}), encoding="utf-8")
    monkeypatch.setattr("backtest.engine._OUTPUT_DIR", broken)

    backtester = PitchBacktester()

    assert backtester.output_dir == fallback
    assert backtester.get_latest_results() == {"status": "ok"}
