"""Permanent config states warn once per process and still return SKIPPED.

grid-hermes, 24 h to 2026-11-04, identical WARNING every cycle:
pmxt ``SDK not installed`` x23, Cloudflare Radar ``no CF_RADAR_TOKEN`` x21,
Telegram / Discord ``no users configured`` x19 each, QuiverQuant
``transition marker ... not found`` x14. The SKIPPED result (and its reason,
which freshness logic reads) is unchanged; only the repeat warnings go to DEBUG.
No network, no database.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from loguru import logger as log

# Import before any sink is added: importing config reconfigures loguru.
from ingestion.altdata import cloudflare_radar_puller  # noqa: F401
from ingestion.altdata import discord_scanner  # noqa: F401
from ingestion.altdata import prediction_pmxt  # noqa: F401
from ingestion.altdata import quiverquant  # noqa: F401
from ingestion.altdata import telegram_scanner  # noqa: F401


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink = log.add(lambda m: records.append(m.record["message"]), level="WARNING")
    yield records
    log.remove(sink)


def _bare(cls):
    """An instance without BasePuller.__init__'s source_catalog lookup."""
    return cls.__new__(cls)


@pytest.mark.unit
def test_pmxt_missing_sdk(monkeypatch, warnings_captured):
    from ingestion.altdata import prediction_pmxt as mod

    monkeypatch.setattr(mod, "_PMXT_AVAILABLE", False)
    monkeypatch.setattr(mod, "_WARNED_PMXT_MISSING", False)
    outs = [_bare(mod.PmxtPredictionPuller).pull() for _ in range(5)]
    assert all(o["status"] == "SKIPPED" and o["reason"] == "pmxt not installed" for o in outs)
    assert sum("pmxt SDK not installed" in w for w in warnings_captured) == 1


@pytest.mark.unit
def test_cloudflare_radar_without_token(monkeypatch, warnings_captured):
    from ingestion.altdata import cloudflare_radar_puller as mod

    monkeypatch.setattr(mod, "_resolve_cf_token", lambda: None)
    monkeypatch.setattr(mod, "_WARNED_NO_CF_TOKEN", False)
    outs = [_bare(mod.CloudflareRadarPuller).pull() for _ in range(5)]
    assert all(o["status"] == "SKIPPED" and o["errors"] == ["no CF Radar token configured"]
               for o in outs)
    assert sum("CF_RADAR_TOKEN" in w for w in warnings_captured) == 1


@pytest.mark.unit
@pytest.mark.parametrize("modname, cls, env", [
    ("ingestion.altdata.telegram_scanner", "TelegramScanner", "TELEGRAM_USERS"),
    ("ingestion.altdata.discord_scanner", "DiscordScanner", "DISCORD_USERS"),
])
def test_scanners_without_users(monkeypatch, warnings_captured, modname, cls, env):
    import importlib

    mod = importlib.import_module(modname)
    monkeypatch.setattr(mod, "_WARNED_NO_USERS", False)
    for _ in range(4):  # a new scanner per run, as the scheduler does
        scanner = _bare(getattr(mod, cls))
        scanner.users = []
        out = scanner.pull_recent()
        assert out["status"] == "SKIPPED" and env in out["error"]
    assert sum("no users configured" in w for w in warnings_captured) == 1


@pytest.mark.unit
def test_quiverquant_transition_hold(monkeypatch, warnings_captured):
    from ingestion.altdata import quiverquant as mod

    monkeypatch.setattr(mod, "_WARNED_HOLD_REASONS", set())
    monkeypatch.setattr(mod, "transition_guard_blocks", lambda key: key == "gov_contracts")
    fetch = MagicMock(side_effect=AssertionError("held job must not call the paid API"))
    monkeypatch.setattr(mod, "_fetch_endpoint", fetch)
    for _ in range(5):
        results = mod.pull_all(MagicMock())
        assert {r["status"] for r in results} == {"SKIPPED"}
        assert all("transition marker" in r["skipped_reason"] for r in results)
    held = [w for w in warnings_captured if "QuiverQuant pull SKIPPED" in w]
    assert len(held) == 1


@pytest.mark.unit
def test_quiverquant_new_reason_still_warns(monkeypatch, warnings_captured):
    from ingestion.altdata import quiverquant as mod

    monkeypatch.setattr(mod, "_WARNED_HOLD_REASONS", set())
    mod._warn_hold_once("reason A")
    mod._warn_hold_once("reason A")
    mod._warn_hold_once("reason B")
    assert len([w for w in warnings_captured if "QuiverQuant pull SKIPPED" in w]) == 2
