"""Real Tiingo retry construction, with catalog/provider boundaries mocked."""

import os
from unittest.mock import MagicMock

import pytest

from config import settings
from ingestion import smart_scheduler as ss, tiingo_pull as tp
from ingestion.international.kosis import KOSISPuller
from scripts import hermes_fixers as hf, hermes_operator as ho


@pytest.fixture
def catalog_engine(monkeypatch):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = (524,)
    monkeypatch.setattr(tp.requests, "get", lambda *_a, **_k: pytest.fail("provider reached"))
    monkeypatch.setenv("GRID_ALLOW_PAID_LLM", "false")
    monkeypatch.setattr(settings, "GRID_ALLOW_PAID_LLM", False)
    return engine


def _module_environment_key(monkeypatch, value):
    # Tiingo captures the environment once at module import. Set that same
    # boundary explicitly so these tests do not depend on collection order.
    monkeypatch.setenv("TIINGO_API_KEY", value)
    monkeypatch.setattr(tp, "_TIINGO_API_KEY", os.getenv("TIINGO_API_KEY", ""))


def test_tiingo_derived_registry_preserves_scheduler_key_gate():
    scheduler_entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "tiingo")
    assert scheduler_entry["api_key"] == "TIINGO_API_KEY"
    assert scheduler_entry["api_key_mode"] == "env"
    assert scheduler_entry["method"] == "pull_incremental"
    assert "api_key" not in ho._SOURCE_REGISTRY["tiingo"]
    assert ho._build_source_registry()["tiingo"] == ho._SOURCE_REGISTRY["tiingo"]


@pytest.mark.parametrize("source", ["tiingo", "TIINGO"])
def test_retry_resolves_actual_tiingo_constructor(monkeypatch, catalog_engine, source):
    _module_environment_key(monkeypatch, "reserved-offline-tiingo")
    # Construction must use Tiingo's module key, not the settings keyword.
    monkeypatch.setattr(settings, "TIINGO_API_KEY", "")
    puller, method, kwargs = hf._resolve_puller(source, catalog_engine)
    assert type(puller) is tp.TiingoPuller
    assert puller.engine is catalog_engine and puller.source_id == 524
    assert method == "pull_incremental" and kwargs == {}
    connection = catalog_engine.connect.return_value.__enter__.return_value
    query, parameters = connection.execute.call_args.args
    assert "SELECT id FROM source_catalog" in str(query)
    assert parameters == {"name": "TIINGO"}
    catalog_engine.begin.assert_not_called()


@pytest.mark.parametrize("out,expected,fresh", [
    ({"status": "SUCCESS", "rows_inserted": 6}, "SUCCESS", True),
    ({"status": "SUCCESS", "rows_inserted": 6, "failed": 1}, "PARTIAL", False),
    ({"status": "SUCCESS", "rows_inserted": None}, "FAILED", False),
    ({"status": "SUCCESS", "rows_inserted": 0}, "NO_NEW_DATA", False),
])
def test_real_constructor_through_retry_keeps_outcome_honesty(
    monkeypatch, catalog_engine, out, expected, fresh,
):
    _module_environment_key(monkeypatch, "reserved-offline-tiingo")
    observed = []

    def offline_incremental(self, *, should_continue=None):
        assert type(self) is tp.TiingoPuller
        assert self.engine is catalog_engine and self.source_id == 524
        assert should_continue is not None and should_continue()
        observed.append(self)
        return out

    # Replace the provider/ingest call, while retaining the actual constructor,
    # BasePuller catalog lookup, registry resolution and retry publication.
    monkeypatch.setattr(tp.TiingoPuller, "pull_incremental", offline_incremental)
    result = hf._retry_source("TIINGO", catalog_engine)
    assert len(observed) == 1
    assert (result["outcome"], result["rows_inserted"]) == (expected, out["rows_inserted"])
    connection = catalog_engine.begin.return_value.__enter__.return_value
    updates = [call for call in connection.execute.call_args_list
               if "last_pull_at" in str(call.args[0])]
    assert bool(updates) == fresh
    assert (hf.retry_not_fresh_reason(result) is None) == fresh
    assert "tiingo" not in hf._REPAIRS_IN_FLIGHT
    assert not settings.GRID_ALLOW_PAID_LLM


def test_missing_module_key_fails_before_catalog_or_freshness(monkeypatch, catalog_engine):
    _module_environment_key(monkeypatch, "")
    monkeypatch.setattr(settings, "TIINGO_API_KEY", "reserved-stale-settings-key")
    with pytest.raises(ValueError, match="TIINGO_API_KEY not set"):
        hf._retry_source("TIINGO", catalog_engine)
    catalog_engine.connect.assert_not_called()
    catalog_engine.begin.assert_not_called()
    assert "tiingo" not in hf._REPAIRS_IN_FLIGHT


def test_scheduler_still_refuses_missing_tiingo_key(catalog_engine):
    scheduler = ss.SmartScheduler(catalog_engine)
    # State restoration is part of real scheduler initialization; only calls
    # made while attempting this constructor belong to the missing-key gate.
    catalog_engine.reset_mock()
    entry = next(p for p in ss.PULLER_REGISTRY if p["name"] == "tiingo")
    with pytest.raises(ss.MissingPullerApiKey):
        scheduler._build_puller_instance(entry, tp.TiingoPuller, {})
    catalog_engine.connect.assert_not_called()


def test_actual_keyword_key_provider_still_receives_key(monkeypatch, catalog_engine):
    monkeypatch.setattr(settings, "KOSIS_API_KEY", "reserved-offline-kosis")
    puller, _method, _kwargs = hf._resolve_puller("kosis", catalog_engine)
    assert type(puller) is KOSISPuller
    assert puller.api_key == settings.KOSIS_API_KEY
    assert puller.engine is catalog_engine and puller.source_id == 524
    assert ho._SOURCE_REGISTRY["kosis"]["api_key"] == "KOSIS_API_KEY"
