"""earnings_events puller: transcript sentiment must not depend on a missing module.

``ingestion.ml.earnings_sentiment_llm`` was referenced by the puller since #207
but never existed in the repository, so every transcript logged an import
warning (291 times from 2026-05-18) while the job exited 0.
"""
from __future__ import annotations

import ast
import importlib.util
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

from loguru import logger

from ingestion import earnings_events_puller as mod
from ingestion.av_transcript_plan import TranscriptPeriod
from ingestion.earnings_events_puller import (
    SENTIMENT_NOTICE,
    SENTIMENT_SOURCE,
    EarningsEventsPuller,
)

SRC = Path(mod.__file__).read_text()


def test_no_import_of_nonexistent_llm_module():
    assert importlib.util.find_spec("ingestion.ml.earnings_sentiment_llm") is None
    imported = [
        n.module for n in ast.walk(ast.parse(SRC))
        if isinstance(n, ast.ImportFrom) and n.module
    ]
    assert "ingestion.ml.earnings_sentiment_llm" not in imported
    assert not hasattr(EarningsEventsPuller, "_classify_transcript_sentiment")


def _run_pull(monkeypatch, tmp_path):
    monkeypatch.setattr("ingestion.base.BasePuller._resolve_source_id", lambda self: 1)
    monkeypatch.setattr("ingestion.earnings_events_puller.time.sleep", lambda _: None)
    p = EarningsEventsPuller(MagicMock(), av_api_key="x", av_max_calls=5,
                             av_state_path=tmp_path / "state.db")
    monkeypatch.setattr(p, "_load_universe", lambda *a, **k: ["AAPL", "MSFT"])
    monkeypatch.setattr(p, "load_cik_map", lambda: {})
    monkeypatch.setattr(
        p, "_load_verified_periods",
        lambda t, f: [TranscriptPeriod("2026Q3", date(2026, 6, 27), date(2026, 7, 31),
                                       "0000000001-26-000001")],
    )
    monkeypatch.setattr(p, "_stored_transcript_periods", lambda t, periods: (set(), set()))
    monkeypatch.setattr(
        p, "fetch_av_transcript",
        lambda t, q: {"symbol": t, "quarter": q, "transcript": [
            {"speaker": "CEO", "content": "great quarter", "sentiment": "0.5"}]},
    )
    upserts = []
    monkeypatch.setattr(p, "_upsert_event", lambda **kw: upserts.append(kw) or True)
    msgs = []
    sink = logger.add(lambda m: msgs.append(m.record), level="WARNING")
    try:
        result = p.pull(include_transcripts=True, mode="incremental")
    finally:
        logger.remove(sink)
    return result, upserts, msgs


def test_pull_stores_av_sentiment_and_counts_and_warns_once(monkeypatch, tmp_path):
    result, upserts, msgs = _run_pull(monkeypatch, tmp_path)
    assert upserts, "transcripts should still be stored"
    for u in upserts:
        assert u["sentiment"] == "positive"
        assert u["confidence"] == 0.8
        assert u["raw_payload"]["sentiment_source"] == SENTIMENT_SOURCE
    assert result["transcripts_av_sentiment_only"] == len(upserts)
    text = [m["message"] for m in msgs]
    assert not any("import failed" in t for t in text)
    notices = [t for t in text if "LLM transcript sentiment is not available" in t]
    assert len(notices) == 1  # once per run, not once per transcript
    assert SENTIMENT_NOTICE.split(" (")[0] in notices[0]
