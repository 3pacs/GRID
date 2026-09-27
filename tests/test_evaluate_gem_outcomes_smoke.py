"""Smoke/import test for scripts/evaluate_gem_outcomes.py (task #119 recovery).

This script ran on grid-svr's grid-gem-outcomes.timer for weeks without ever
being committed to origin/main — untracked, invisible to review. Recovered
2026-09-27. `scripts/auto_disable_underperforming_rules.py` already
documents this script as an upstream dependency ("daily, 03:00 UTC, after
`evaluate_gem_outcomes` has run"), confirming the gap.

Keeps this test to pure-logic smoke coverage — no live DB, no network.
DB_PASSWORD is set before import because the module builds its connection
kwargs (and fails fast if the var is absent) at import time.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("DB_PASSWORD", "testpass")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evaluate_gem_outcomes import (  # noqa: E402
    DIR_BEAR,
    DIR_BULL,
    DIR_NEUTRAL,
    TickerHint,
    _candidate_tickers,
    _window_days,
    classify,
    parse_ticker_direction,
)


def test_module_imports_without_live_db():
    """Import alone must not require a live Postgres connection."""
    import scripts.evaluate_gem_outcomes as mod

    assert mod.DB_CONNECT_PARAMS["password"] == "testpass"
    assert "password" not in repr(mod.DB_CONNECT_PARAMS) or mod.DB_CONNECT_PARAMS["password"]


def test_window_days_parses_units():
    assert _window_days("1d") == 1
    assert _window_days("3w") == 21
    assert _window_days("1m") == 30


def test_classify_hit_and_wrong_direction():
    assert classify(DIR_BULL, 0.05, 0.02) == "HIT"
    assert classify(DIR_BULL, -0.05, 0.02) == "WRONG_DIRECTION"
    assert classify(DIR_BEAR, -0.05, 0.02) == "HIT"
    assert classify(DIR_NEUTRAL, 0.01, 0.02) == "HIT"
    assert classify(DIR_NEUTRAL, 0.05, 0.02) == "MISS"
    assert classify(DIR_BULL, None, 0.02) == "INCONCLUSIVE"


def test_parse_ticker_direction_heading_pattern():
    gem = {
        "source": "hermes",
        "subject_id": "",
        "related_ids": [],
        "evidence": {"note": "AAPL is heading CALL on strong guidance"},
        "score": 0.9,
    }
    hint = parse_ticker_direction(gem)
    assert isinstance(hint, TickerHint)
    assert hint.ticker == "AAPL"
    assert hint.direction == DIR_BULL


def test_parse_ticker_direction_subject_id_fallback():
    gem = {
        "source": "gem_hunter",
        "subject_id": "hypothesis||GEHC",
        "related_ids": [],
        "evidence": {"kind": "new_low"},
        "score": -0.4,
    }
    hint = parse_ticker_direction(gem)
    assert hint is not None
    assert hint.ticker == "GEHC"
    assert hint.direction == DIR_BEAR


def test_candidate_tickers_excludes_non_tickers():
    out = _candidate_tickers("FLUT reported CPI data with the SEC and FED")
    assert "FLUT" in out
    assert "CPI" not in out
    assert "SEC" not in out
    assert "FED" not in out
