"""earnings_events puller: transcript sentiment must not depend on a missing module.

``ingestion.ml.earnings_sentiment_llm`` was referenced by the puller since #207
but never existed in the repository, so every transcript logged an import
warning (291 times from 2026-05-18) while the job exited 0.
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

from loguru import logger

from ingestion import earnings_events_puller as mod
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


def _run_pull(monkeypatch):
    p = EarningsEventsPuller.__new__(EarningsEventsPuller)
    p.engine = MagicMock()
    p.av_api_key = "x"
    p.av_max_calls = 5
    monkeypatch.setattr(p, "_load_universe", lambda *a, **k: ["AAPL", "MSFT"])
    monkeypatch.setattr(p, "load_cik_map", lambda: {})
    monkeypatch.setattr(p, "_load_last_transcript_dates", lambda u: {})
    monkeypatch.setattr(
        p, "fetch_av_transcript",
        lambda t, q: {"symbol": t, "transcript": [
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


def test_pull_stores_av_sentiment_and_counts_and_warns_once(monkeypatch):
    result, upserts, msgs = _run_pull(monkeypatch)
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
