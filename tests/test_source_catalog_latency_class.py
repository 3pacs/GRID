"""Regression tests for source_catalog.latency_class auto-registration.

Production (grid-hermes journal, 2026-10-20 03:24:42Z) hit::

    new row for relation "source_catalog" violates check constraint
    "source_catalog_latency_class_check"
    DETAIL: (Asset_Registries, https://registry.faa.gov, FREE, DAILY, ...)

because ``AssetRegistryPuller.SOURCE_CONFIG`` declared ``"DAILY"`` and
``BasePuller._resolve_source_id`` bound it straight into the auto-create
INSERT. ``DAILY`` is not in the CHECK constraint; the closest allowed
class is ``EOD``.

Two layers are pinned here:

1. Every puller that declared a non-allowed value at the time of the fix
   (DAILY / BATCH / HISTORICAL) now declares an allowed one at the source,
   and the sweep asserts the DECLARED value without patching the logger,
   so a future "STATIC"-style typo fails loudly in CI
   (``test_offending_pullers_declare_allowed_latency_class``).
2. ``_resolve_source_id`` normalises whatever a puller declares before
   binding it, so a runtime typo degrades to a WARNING + safe default
   instead of a CheckViolation that fails the whole pull. This is a safety
   net, not the fix.
"""
from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from ingestion.base import (
    VALID_LATENCY_CLASSES,
    BasePuller,
    _normalize_latency_class,
)

# Declared seven-value constraint shared by the composed source.
# Historical griddb comments are not fresh installed-schema evidence:
#   CHECK (latency_class = ANY (ARRAY['REALTIME','INTRADAY','EOD','WEEKLY',
#                                     'MONTHLY','QUARTERLY','ANNUAL']))
# Merged PR #881 aligns schema.sql; installed database state is separate.
_DECLARED_CONSTRAINT_SET = frozenset(
    {"REALTIME", "INTRADAY", "EOD", "WEEKLY", "MONTHLY", "QUARTERLY", "ANNUAL"}
)


def test_valid_latency_classes_match_declared_constraint():
    assert VALID_LATENCY_CLASSES == _DECLARED_CONSTRAINT_SET


# ── Pullers that declared "DAILY" (the journal failure + identical siblings) ─

_DAILY_PULLERS = [
    ("ingestion.altdata.asset_registries", "AssetRegistryPuller"),
    ("ingestion.altdata.pmxt_archive", "PmxtArchivePuller"),
    ("ingestion.altdata.social_port_activity", "SocialPortActivityPuller"),
    ("ingestion.altdata.warn_layoffs", "WARNLayoffsPuller"),
    ("ingestion.physical.nasa_fire", "NASAFirePuller"),
    ("ingestion.physical.weather_puller", "WeatherPuller"),
]

# Every BasePuller subclass whose SOURCE_CONFIG["latency_class"] was outside
# the legacy schema.sql four-value set when this fix landed (main 6fc9434a).
# INTRADAY/QUARTERLY are now declared in the composed schema.sql;
# DAILY/BATCH/HISTORICAL were allowed nowhere and are now fixed at source.
_OFFENDING_PULLERS = _DAILY_PULLERS + [
    ("ingestion.altdata.findkg_puller", "FinDKGPuller"),            # was BATCH -> MONTHLY
    ("ingestion.altdata.icij_puller", "ICIJPuller"),                # was BATCH -> MONTHLY
    ("ingestion.altdata.prediction_market_history", "PredictionMarketHistoryPuller"),  # was HISTORICAL -> MONTHLY
    ("ingestion.altdata.pushshift_reddit", "PushshiftRedditPuller"),  # was HISTORICAL -> ANNUAL
    ("ingestion.altdata.buyback_execution", "BuybackExecutionPuller"),  # QUARTERLY
    ("ingestion.altdata.ais_ground_truth", "AISGroundTruthPuller"),  # INTRADAY
    ("ingestion.altdata.kalshi", "KalshiPuller"),                    # INTRADAY
    ("ingestion.altdata.news_scraper", "NewsScraperPuller"),         # INTRADAY
    ("ingestion.altdata.prediction_odds", "PredictionOddsPuller"),   # INTRADAY
    ("ingestion.altdata.prediction_pmxt", "PmxtPredictionPuller"),   # INTRADAY
    ("ingestion.altdata.smart_money", "SmartMoneyPuller"),           # INTRADAY
    ("ingestion.altdata.trending_news", "TrendingNewsPuller"),       # INTRADAY
    ("ingestion.crucix_bridge", "CrucixBridgePuller"),               # INTRADAY
    ("ingestion.physical.noaa_space_weather", "NOAASpaceWeatherPuller"),  # INTRADAY
]


# Only this exact direct dependency is optional for this puller in the
# sweep. Internal GRID imports, the requested puller, and other missing
# dependencies must fail rather than silently disappear from coverage.
_OPTIONAL_IMPORTS = {
    "ingestion.altdata.pushshift_reddit": frozenset({"zstandard"}),
}


def _load(modname: str, clsname: str) -> type[BasePuller]:
    try:
        module = importlib.import_module(modname)
    except ModuleNotFoundError as exc:
        if exc.name not in _OPTIONAL_IMPORTS.get(modname, frozenset()):
            raise
        pytest.skip(f"{modname} needs explicitly optional module {exc.name!r}")
    return getattr(module, clsname)


@pytest.mark.parametrize("missing", [
    "ingestion.internal_typo", "ingestion.altdata.pushshift_reddit",
    "zstandard.internal_typo", "orjson",
])
def test_load_does_not_hide_unexpected_missing_modules(monkeypatch, missing):
    failure = ModuleNotFoundError("synthetic missing module", name=missing)

    def unavailable(_):
        raise failure

    monkeypatch.setattr(importlib, "import_module", unavailable)
    with pytest.raises(ModuleNotFoundError) as caught:
        _load("ingestion.altdata.pushshift_reddit", "PushshiftRedditPuller")
    assert caught.value is failure


def test_load_skips_only_explicit_optional_dependency(monkeypatch):
    def unavailable(_):
        raise ModuleNotFoundError("synthetic optional dependency", name="zstandard")

    monkeypatch.setattr(importlib, "import_module", unavailable)
    with pytest.raises(pytest.skip.Exception, match="explicitly optional module 'zstandard'"):
        _load("ingestion.altdata.pushshift_reddit", "PushshiftRedditPuller")


def _ids(entries):
    return [f"{m.rsplit('.', 1)[-1]}.{c}" for m, c in entries]


@pytest.mark.parametrize(("modname", "clsname"), _DAILY_PULLERS, ids=_ids(_DAILY_PULLERS))
def test_daily_pullers_declare_allowed_latency_class(modname, clsname):
    """Source-level fix: the declared value itself must be allowed."""
    cls = _load(modname, clsname)
    declared = cls.SOURCE_CONFIG["latency_class"]
    assert declared in VALID_LATENCY_CLASSES, (
        f"{clsname}.SOURCE_CONFIG['latency_class']={declared!r} not in constraint"
    )


def test_asset_registries_declares_weekly():
    """GRID polls the FAA registry weekly (smart_scheduler freq_h 168) and
    alerts/cadence.py reads latency_class for staleness: EOD would map to
    the 30 h DAILY grace and flag it stale ~5 of 7 days."""
    cls = _load("ingestion.altdata.asset_registries", "AssetRegistryPuller")
    assert cls.SOURCE_NAME == "Asset_Registries"
    assert cls.SOURCE_CONFIG["latency_class"] == "WEEKLY"


@pytest.mark.parametrize(
    ("modname", "clsname"), _OFFENDING_PULLERS, ids=_ids(_OFFENDING_PULLERS)
)
def test_offending_pullers_declare_allowed_latency_class(modname, clsname):
    """Source-level sweep: the DECLARED value must be allowed. No logger
    patching here on purpose -- a future "STATIC"/"BATCH" must fail CI,
    not slide through the runtime fallback."""
    cls = _load(modname, clsname)
    declared = cls.SOURCE_CONFIG["latency_class"]
    assert declared in VALID_LATENCY_CLASSES, (
        f"{clsname}.SOURCE_CONFIG['latency_class']={declared!r} not in constraint"
    )


@pytest.mark.parametrize(
    ("modname", "clsname"),
    [("ingestion.altdata.findkg_puller", "FinDKGPuller"),
     ("ingestion.altdata.icij_puller", "ICIJPuller"),
     ("ingestion.altdata.prediction_market_history", "PredictionMarketHistoryPuller")],
)
def test_batch_archive_pullers_declare_monthly(modname, clsname):
    assert _load(modname, clsname).SOURCE_CONFIG["latency_class"] == "MONTHLY"


def test_pushshift_declares_annual():
    assert _load("ingestion.altdata.pushshift_reddit", "PushshiftRedditPuller").SOURCE_CONFIG["latency_class"] == "ANNUAL"


# ── Defensive normaliser in BasePuller._resolve_source_id ───────────────────


def _auto_create_engine(new_id: int = 99) -> tuple[MagicMock, MagicMock]:
    """Mock engine: first SELECT misses, INSERT ... RETURNING yields new_id."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.side_effect = [None, (new_id,)]
    return engine, conn


def _bound_insert_params(conn: MagicMock) -> dict:
    for call in conn.execute.call_args_list:
        if "INSERT INTO source_catalog" in str(call.args[0]):
            return call.args[1]
    raise AssertionError("INSERT INTO source_catalog was never executed")


class TestNormalizeLatencyClass:
    @pytest.mark.parametrize("value", sorted(_DECLARED_CONSTRAINT_SET))
    def test_allowed_values_pass_through(self, value):
        assert _normalize_latency_class(value) == value

    @pytest.mark.parametrize(
        ("alias", "expected"),
        [("DAILY", "EOD"), ("daily", "EOD"), (" Daily ", "EOD"),
         ("HOURLY", "INTRADAY"), ("YEARLY", "ANNUAL"),
         ("ANNUALLY", "ANNUAL"), (" annually ", "ANNUAL")],
    )
    def test_aliases_map_without_warning(self, alias, expected):
        with patch("ingestion.base.log") as mock_log:
            assert _normalize_latency_class(alias) == expected
        mock_log.warning.assert_not_called()

    @pytest.mark.parametrize("bad", ["BATCH", "HISTORICAL", "STATIC", "", None, 7])
    def test_unknown_falls_back_to_eod_with_warning(self, bad):
        with patch("ingestion.base.log") as mock_log:
            assert _normalize_latency_class(bad) == "EOD"
        mock_log.warning.assert_called_once()
        assert "latency_class" in mock_log.warning.call_args.args[0]


class TestResolveSourceIdBindsAllowedLatencyClass:
    def test_auto_create_with_daily_does_not_raise_and_binds_eod(self):
        """The exact journal failure: DAILY must bind as EOD, not raise."""
        engine, conn = _auto_create_engine(new_id=55)

        class Puller(BasePuller):
            SOURCE_NAME = "Asset_Registries_Test"
            SOURCE_CONFIG = {
                "base_url": "https://registry.faa.gov",
                "cost_tier": "FREE",
                "latency_class": "DAILY",
                "pit_available": False,
                "revision_behavior": "RARE",
                "trust_score": "HIGH",
                "priority_rank": 55,
            }

        puller = Puller(engine)
        assert puller.source_id == 55
        assert _bound_insert_params(conn)["latency"] == "EOD"

    def test_auto_create_with_unknown_value_warns_and_binds_default(self):
        engine, conn = _auto_create_engine(new_id=56)

        class Puller(BasePuller):
            SOURCE_NAME = "Unknown_Latency_Test"
            SOURCE_CONFIG = {"base_url": "x", "latency_class": "STATIC"}

        with patch("ingestion.base.log") as mock_log:
            puller = Puller(engine)
        assert puller.source_id == 56
        assert _bound_insert_params(conn)["latency"] in VALID_LATENCY_CLASSES
        mock_log.warning.assert_called_once()

    def test_auto_create_without_latency_key_binds_default(self):
        engine, conn = _auto_create_engine(new_id=57)

        class Puller(BasePuller):
            SOURCE_NAME = "No_Latency_Key_Test"
            SOURCE_CONFIG = {"base_url": "x"}

        Puller(engine)
        assert _bound_insert_params(conn)["latency"] == "EOD"

    @pytest.mark.parametrize(
        ("modname", "clsname"), _OFFENDING_PULLERS, ids=_ids(_OFFENDING_PULLERS)
    )
    def test_every_offending_puller_binds_declared_value_without_warning(self, modname, clsname):
        """The INSERT must carry the declared value unchanged and never warn."""
        cls = _load(modname, clsname)
        engine, conn = _auto_create_engine()
        # Call the resolver directly on a bare instance so subclass
        # __init__ side effects (HTTP sessions, extra tables) stay out.
        inst = object.__new__(cls)
        inst.engine = engine
        inst.SOURCE_NAME = cls.SOURCE_NAME
        inst.SOURCE_CONFIG = cls.SOURCE_CONFIG
        with patch("ingestion.base.log") as mock_log:
            BasePuller._resolve_source_id(inst)
        bound = _bound_insert_params(conn)["latency"]
        assert bound == cls.SOURCE_CONFIG["latency_class"]
        assert bound in VALID_LATENCY_CLASSES
        mock_log.warning.assert_not_called()
