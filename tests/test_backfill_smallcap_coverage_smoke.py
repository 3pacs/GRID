"""Smoke/import test for scripts/backfill_smallcap_coverage.py (recovery).

This script ran on grid-svr's grid-gem-watchlist-coverage.timer for weeks
without ever being committed to origin/main — untracked, invisible to
review. Recovered 2026-09-27.

Pure-logic smoke coverage only — no live DB, no network. The module already
reads credentials exclusively through `config.settings` / `os.getenv`, so
no credential fix was needed here (unlike its sibling scripts).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("DB_PASSWORD", "testpass")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.backfill_smallcap_coverage import (  # noqa: E402
    _aggregate_insight_sentiment,
    _filter_tickers,
    _load_watchlist,
)


def test_module_imports_without_live_db():
    import scripts.backfill_smallcap_coverage as mod

    assert mod._POLYGON_NEWS_URL.startswith("https://")
    assert mod._TWELVEDATA_TS_URL.startswith("https://")


def test_filter_tickers_keeps_valid_symbols_only():
    # Validation runs on the pre-dot base (share-class suffixes like BRK.B
    # are allowed), but the original (uppercased) token is what's kept.
    out = _filter_tickers(["FLUT", "gehc", "", 123, "TOOLONGNAME", "BHRB"])
    assert "FLUT" in out
    assert "GEHC" in out
    assert "BHRB" in out
    assert "" not in out
    assert "TOOLONGNAME" not in out


def test_aggregate_insight_sentiment_majority_positive():
    label, confidence = _aggregate_insight_sentiment(
        [{"sentiment": "positive"}, {"sentiment": "positive"}, {"sentiment": "negative"}]
    )
    assert label == "BULLISH"
    assert 0.5 < confidence <= 0.95


def test_aggregate_insight_sentiment_empty_is_neutral():
    label, confidence = _aggregate_insight_sentiment([])
    assert label == "NEUTRAL"
    assert confidence == 0.5


def test_load_watchlist_missing_file_returns_empty(tmp_path):
    missing = tmp_path / "no-such-watchlist.txt"
    assert _load_watchlist(str(missing)) == []


def test_load_watchlist_parses_tickers_and_comments(tmp_path):
    p = tmp_path / "watchlist.txt"
    p.write_text("flut\n# comment\n\ngehc\n")
    assert _load_watchlist(str(p)) == ["FLUT", "GEHC"]
