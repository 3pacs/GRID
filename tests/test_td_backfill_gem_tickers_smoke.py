"""Smoke/import test for scripts/td_backfill_gem_tickers.py (task #134 recovery).

This script ran on grid-svr's grid-td-backfill.timer for weeks without ever
being committed to origin/main — untracked via `.git/info/exclude`,
invisible to review. Recovered 2026-09-27, with its hardcoded plaintext DB
password and host-specific `.env` path read replaced by env-var reads,
mirroring the credential-exposure fix already applied to its sibling
scripts/td_backfill_universe.py (commit b978ee91).

Pure-logic smoke coverage only — no live DB, no network call to Twelve Data.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("DB_PASSWORD", "testpass")
os.environ.setdefault("TWELVEDATA_API_KEY", "test-td-key")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.td_backfill_gem_tickers as mod  # noqa: E402


def test_module_imports_without_live_db_or_network():
    assert mod.CONNECT_PARAMS["password"] == "testpass"
    assert mod.CONNECT_PARAMS["dbname"] == "griddb"
    assert mod.API_KEY == "test-td-key"
    assert mod.TD_URL.startswith("https://")


def test_redact_api_key_strips_the_live_key_from_error_text():
    msg = f"GET failed for https://api.twelvedata.com/time_series?apikey={mod.API_KEY}&symbol=FLUT"
    redacted = mod._redact_api_key(msg)
    assert mod.API_KEY not in redacted
    assert "***REDACTED***" in redacted


def test_redact_api_key_is_a_noop_when_key_absent(monkeypatch):
    monkeypatch.setattr(mod, "API_KEY", "")
    assert mod._redact_api_key("no key here") == "no key here"
