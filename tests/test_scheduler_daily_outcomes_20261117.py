"""grid-scheduler's 20:00Z "daily" group: retired pullers stay out, 0-row runs stay honest.

The 2026-11-17 audit read pull_log.status alone and saw SUCCESS / 0 rows for
about ten pullers in this group. pull_log's CHECK constraint allows only
RUNNING/SUCCESS/PARTIAL/FAILED, so the outcome lives in error_message as a
``SKIPPED:`` / ``NO_NEW_DATA:`` marker -- SmartScheduler's convention, read by
ingestion/source_freshness.run_outcome, pipeline-health and the health
alerter. These tests pin that for the real return shapes those pullers
produce, and pin Dune_Analytics out of the group (registry hold, retired
2026-11-04 in commit 17889eec).
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _stub_registration_imports(monkeypatch, users: dict[str, list] | None = None) -> list[str]:
    """Replace every module _get_pullers_for_group imports with a fixture.

    ingestion.smart_scheduler stays real so its registry holds apply.
    ``users`` maps a scanner class name to what its
    ``_load_users_from_config()`` returns (default: no users).
    Returns the class names that were constructed, in order.
    """
    constructed: list[str] = []
    users = users or {}

    def fixture_class(name):
        class FixturePuller:
            def __init__(self, *args, **kwargs):
                constructed.append(name)
                self.kwargs = kwargs

            @staticmethod
            def _load_users_from_config():
                return list(users.get(name, []))
        FixturePuller.__name__ = name
        return FixturePuller

    source = (ROOT / "ingestion/scheduler.py").read_text(encoding="utf-8")
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.FunctionDef) and n.name == "_get_pullers_for_group")
    for imp in (n for n in ast.walk(node) if isinstance(n, ast.ImportFrom)):
        if imp.module == "ingestion.smart_scheduler":
            continue
        monkeypatch.setitem(sys.modules, imp.module, SimpleNamespace(**{
            alias.name: fixture_class(alias.name) for alias in imp.names
        }))
    return constructed


def test_daily_group_does_not_run_dune(monkeypatch):
    import ingestion.scheduler as sched
    from ingestion.smart_scheduler import hold_reason_for

    assert "Retired 2026-11-04" in (hold_reason_for("dune") or "")
    constructed = _stub_registration_imports(monkeypatch)

    names = [entry[0] for entry in sched._get_pullers_for_group("daily", None, {})]

    assert names, "registration produced no pullers; the stubbing is broken"
    assert "Dune_Analytics" not in names
    assert "DunePuller" not in constructed


def test_dune_hold_is_the_one_switch(monkeypatch):
    """Lifting the SmartScheduler hold is all it takes to run Dune here again."""
    import ingestion.scheduler as sched
    import ingestion.smart_scheduler as smart

    real = smart.hold_reason_for
    monkeypatch.setattr(smart, "hold_reason_for",
                        lambda name: None if name == "dune" else real(name))
    constructed = _stub_registration_imports(monkeypatch)

    names = [entry[0] for entry in sched._get_pullers_for_group("daily", None, {})]

    assert names.count("Dune_Analytics") == 1
    assert constructed.count("DunePuller") == 1


# Return shapes as the 2026-11-16 20:00Z daily run produced them.
_DISCORD_NO_USERS = [{
    "source": "discord", "status": "SKIPPED",
    "error": "No Discord users configured. Set DISCORD_USERS env var.",
    "signals_found": 0, "rows_inserted": 0,
}]
_CF_RADAR_NO_TOKEN = {
    "status": "SKIPPED", "total_inserted": 0, "per_series": {},
    "detected_anomalies": [], "errors": ["no CF Radar token configured"],
}


@pytest.mark.parametrize(
    ("puller_name", "result", "outcome", "marker"),
    [
        ("Discord_Solana_Scanner", _DISCORD_NO_USERS, "SKIPPED",
         "SKIPPED: all 1 items skipped: No Discord users configured"),
        ("Cloudflare_Radar", _CF_RADAR_NO_TOKEN, "SKIPPED",
         "SKIPPED: ['no CF Radar token configured']"),
        ("USPTO_Patents", {"rows_inserted": 0}, "NO_NEW_DATA",
         "NO_NEW_DATA: run completed, 0 rows written"),
        ("nowcast", 0, "NO_NEW_DATA", "NO_NEW_DATA: run completed, 0 rows written"),
    ],
)
def test_zero_row_daily_runs_record_smart_scheduler_markers(
    monkeypatch, puller_name, result, outcome, marker,
):
    import ingestion.scheduler as sched
    from tests.test_scheduler_pull_logging import _FakeEngine, _SuccessfulPuller

    engine = _FakeEngine({puller_name: 900})
    monkeypatch.setattr(
        sched, "_get_pullers_for_group",
        lambda group, db_engine, config: [(puller_name, _SuccessfulPuller(result), "pull", {})],
    )

    summary = sched.run_pull_group("daily", engine, config={})

    assert summary["success_count"] == 0
    assert summary["results"][0]["status"] == outcome
    row = engine.pull_logs[1]
    # SUCCESS is the only zero-row status pull_log's CHECK constraint allows;
    # the outcome is the error_message marker.
    assert (row["status"], row["rows_inserted"]) == ("SUCCESS", 0)
    assert row["error_message"].startswith(marker)
    assert engine.touched_source_ids == []  # not marked fresh
    assert json.loads(engine.events[0]["payload"])["status"] == outcome

    from ingestion.source_freshness import RUN_NO_NEW_DATA, RUN_SKIPPED, run_outcome
    expected = RUN_SKIPPED if outcome == "SKIPPED" else RUN_NO_NEW_DATA
    assert run_outcome(row["status"], row["rows_inserted"], row["error_message"]) == expected


def test_daily_group_does_not_run_crucix(monkeypatch):
    """grid-intelligence (every 15 min) and SmartScheduler (hourly) run the Crucix
    bridge; grid-scheduler's copies only re-inserted deduplicated news."""
    import ingestion.scheduler as sched

    constructed = _stub_registration_imports(monkeypatch)
    names = [entry[0] for entry in sched._get_pullers_for_group("daily", None, {})]

    assert names, "registration produced no pullers; the stubbing is broken"
    assert "Crucix" not in names
    assert "CrucixBridgePuller" not in constructed


def test_run_daily_pulls_does_not_call_crucix():
    source = (ROOT / "ingestion/scheduler.py").read_text(encoding="utf-8")
    node = next(n for n in ast.parse(source).body
                if isinstance(n, ast.FunctionDef) and n.name == "run_daily_pulls")
    imported = {alias.name for n in ast.walk(node) if isinstance(n, ast.ImportFrom)
                for alias in n.names}
    assert "CrucixBridgePuller" not in imported


def test_crucix_still_runs_elsewhere():
    intel = (ROOT / "intelligence/scheduler.py").read_text(encoding="utf-8")
    smart = (ROOT / "ingestion/smart_scheduler.py").read_text(encoding="utf-8")
    assert "_sched.every(15).minutes.do(_crucix_ingest)" in intel
    assert '"crucix_bridge"' in smart and '"ingestion.crucix_bridge"' in smart


# ── 2026-11-17 retirement and de-duplication of the daily group ──────────────
#
# Evidence (griddb, read-only, 2026-11-17): no raw_series row ever under the
# catalog rows of Cloudflare_Radar (718), USPTO_PV (628), polygon (722),
# cryptoquant (720), JQuants (213), Telegram/Discord (510/511), and no
# PREDICTION:* row or prediction_market signal from Prediction_Odds.

_RETIRED = {
    # pull_log name -> class that must not be constructed
    "Cloudflare_Radar": "CloudflareRadarPuller",     # registry hold
    "Prediction_Odds": "PredictionOddsPuller",       # registry hold
    "USPTO_Patents": "USPTOPuller",                  # not in registry: removed
    "Polygon": "PolygonPuller",                      # not in registry: removed
    "CryptoQuant": "CryptoQuantPuller",              # not in registry: removed
    "JQuants": "JQuantsPuller",                      # not in registry: removed
    "Telegram_Solana_Scanner": "TelegramScanner",    # no users configured
    "Discord_Solana_Scanner": "DiscordScanner",      # no users configured
}

# Each wrote raw_series rows in the 30 days to 2026-11-17 (pull_log), or is
# fed by another runner (CBOE_Indices); none of them may be dropped.
_PRODUCING = [
    "ECB_SDW", "BCB_BR", "MAS_SG", "AKShare", "OppInsights", "GDELT", "WorldNews",
    "OFR", "EDINET", "alphavantage_news_sentiment", "NYFed", "Congress_Trading",
    "Congress_Legislation", "SEC_Insider", "Unusual_Whales", "Smart_Money",
    "Fed_Liquidity", "SEC_XBRL_Financials", "ETF_Flows", "News_Scraper_RSS",
    "Analyst_Ratings", "DeFi_Llama", "Etherscan", "FMP_Earnings",
    "Wikipedia_Pageviews", "Wikipedia_Topic_Pageviews", "yfinance_Earnings",
    "NASA_FIRMS", "Tiingo_News", "CBOE_Indices", "StockTwits",
    "SEC_EDGAR_Fundamentals", "MarketWatch_News", "Google_Trends_Daily",
    "EIA_Energy", "NOAA_Space_Weather", "NASA_Fire", "Open_Meteo", "nowcast",
    "USASpending", "FINRA_Margin", "LittleSis", "Wikidata_Relations",
    "AAII_Sentiment", "Kalshi_Markets", "FedSpeeches",
]


def _daily(monkeypatch, users=None):
    import ingestion.scheduler as sched

    constructed = _stub_registration_imports(monkeypatch, users)
    entries = sched._get_pullers_for_group("daily", None, {})
    assert entries, "registration produced no pullers; the stubbing is broken"
    return entries, constructed


def _lift_holds(monkeypatch, *lifted):
    import ingestion.smart_scheduler as smart

    real = smart.hold_reason_for
    monkeypatch.setattr(smart, "hold_reason_for",
                        lambda name: None if name in lifted else real(name))


def test_retired_pullers_are_not_registered_or_constructed(monkeypatch):
    entries, constructed = _daily(monkeypatch)
    names = [e[0] for e in entries]
    for name, cls in _RETIRED.items():
        assert name not in names, name
        assert cls not in constructed, cls


def test_new_holds_are_dated_registry_holds():
    from ingestion.smart_scheduler import hold_reason_for

    for registry_name in ("cloudflare_radar", "prediction_odds"):
        assert "Retired 2026-11-17" in (hold_reason_for(registry_name) or ""), registry_name


def test_removed_pullers_have_no_registry_entry_to_hold():
    """USPTO, Polygon, CryptoQuant and JQuants were removed outright because
    SmartScheduler has no entry for them, so a hold could not gate them."""
    from ingestion.smart_scheduler import PULLER_REGISTRY

    mods = {e["mod"] for e in PULLER_REGISTRY}
    for mod in ("ingestion.altdata.uspto_puller", "ingestion.altdata.polygon_puller",
                "ingestion.altdata.cryptoquant_puller", "ingestion.international.jquants"):
        assert mod not in mods, mod


@pytest.mark.parametrize(
    ("registry_name", "name", "cls"),
    [("cloudflare_radar", "Cloudflare_Radar", "CloudflareRadarPuller"),
     ("prediction_odds", "Prediction_Odds", "PredictionOddsPuller")],
)
def test_lifting_a_hold_registers_the_puller_exactly_once(monkeypatch, registry_name, name, cls):
    _lift_holds(monkeypatch, registry_name)
    entries, constructed = _daily(monkeypatch)
    assert [e[0] for e in entries].count(name) == 1
    assert constructed.count(cls) == 1


@pytest.mark.parametrize(
    ("name", "cls"),
    [("Telegram_Solana_Scanner", "TelegramScanner"),
     ("Discord_Solana_Scanner", "DiscordScanner")],
)
def test_scanners_come_back_when_users_are_configured(monkeypatch, name, cls):
    user = object()
    entries, constructed = _daily(monkeypatch, users={cls: [user]})
    mine = [e for e in entries if e[0] == name]
    assert len(mine) == 1
    assert constructed.count(cls) == 1
    assert mine[0][1].kwargs["users"] == [user]  # not re-read in the constructor


def test_scanner_scaffolding_is_kept():
    import importlib.util

    from ingestion.smart_scheduler import PULLER_REGISTRY

    for mod in ("ingestion.altdata.telegram_scanner", "ingestion.altdata.discord_scanner"):
        assert importlib.util.find_spec(mod) is not None, mod
    names = {e["name"] for e in PULLER_REGISTRY}
    assert {"telegram_scanner", "discord_scanner"} <= names


def test_polygon_module_is_kept():
    """Only the legacy daily registration is retired; gex/research use Polygon."""
    import importlib.util

    assert importlib.util.find_spec("ingestion.altdata.polygon_puller") is not None


def test_each_daily_pull_log_name_is_registered_once(monkeypatch):
    _lift_holds(monkeypatch, "cloudflare_radar", "prediction_odds")
    entries, constructed = _daily(monkeypatch, users={"TelegramScanner": [1], "DiscordScanner": [1]})
    names = [e[0] for e in entries]
    dupes = sorted({n for n in names if names.count(n) > 1})
    assert dupes == []
    for name in ("Cloudflare_Radar", "Wikipedia_Pageviews", "MarketWatch_News", "nowcast"):
        assert names.count(name) == 1, name
    assert "Nowcast_GDPNow" not in names
    assert constructed.count("NowcastPuller") == 1
    assert constructed.count("MarketWatchNewsPuller") == 1
    assert constructed.count("CloudflareRadarPuller") == 1


def test_kept_duplicates_use_the_honest_entry_points(monkeypatch):
    entries, _ = _daily(monkeypatch)
    by_name = {e[0]: e for e in entries}
    # pull_all returns one result per feed, so a failed feed is PARTIAL;
    # pull() reported SUCCESS whatever the feeds did.
    assert by_name["MarketWatch_News"][2] == "pull_all"
    assert by_name["nowcast"][2] == "pull"
    # Two different Wikipedia pullers, both writing rows: both stay, named apart.
    assert type(by_name["Wikipedia_Pageviews"][1]).__name__ == "WikipediaPuller"
    assert type(by_name["Wikipedia_Topic_Pageviews"][1]).__name__ == "WikipediaPageviewsPuller"


def test_producing_pullers_are_still_registered(monkeypatch):
    entries, _ = _daily(monkeypatch)
    names = [e[0] for e in entries]
    missing = [n for n in _PRODUCING if n not in names]
    assert missing == []
