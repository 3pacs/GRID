from __future__ import annotations

import types

import pytest

from ingestion.smart_scheduler import MissingPullerApiKey, SmartScheduler


class _EnvPuller:
    def __init__(self, db_engine):
        self.db_engine = db_engine


class _FirstArgPuller:
    def __init__(self, api_key, db_engine):
        self.api_key = api_key
        self.db_engine = db_engine


class _KeywordPuller:
    def __init__(self, db_engine, api_key=""):
        self.api_key = api_key
        self.db_engine = db_engine


def _scheduler(engine=object()) -> SmartScheduler:
    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = engine
    return sched


def test_build_puller_instance_supports_env_api_key_mode() -> None:
    instance = _scheduler("engine")._build_puller_instance(
        {"name": "env_source", "api_key": "API_KEY", "api_key_mode": "env"},
        _EnvPuller,
        {"API_KEY": "secret"},
    )

    assert isinstance(instance, _EnvPuller)
    assert instance.db_engine == "engine"


def test_build_puller_instance_supports_first_arg_api_key_mode() -> None:
    instance = _scheduler("engine")._build_puller_instance(
        {"name": "first_source", "api_key": "API_KEY"},
        _FirstArgPuller,
        {"API_KEY": "secret"},
    )

    assert instance.api_key == "secret"
    assert instance.db_engine == "engine"


def test_build_puller_instance_supports_keyword_api_key_mode() -> None:
    instance = _scheduler("engine")._build_puller_instance(
        {"name": "keyword_source", "api_key": "API_KEY", "api_key_mode": "keyword"},
        _KeywordPuller,
        {"API_KEY": "secret"},
    )

    assert instance.api_key == "secret"
    assert instance.db_engine == "engine"


def test_build_puller_instance_raises_on_missing_required_api_key() -> None:
    with pytest.raises(MissingPullerApiKey):
        _scheduler("engine")._build_puller_instance(
            {"name": "source", "api_key": "API_KEY"},
            _FirstArgPuller,
            {},
        )


def test_gdelt_bounded_recent_skips_heavy_sections(monkeypatch) -> None:
    from ingestion.altdata import gdelt

    puller = gdelt.GDELTPuller.__new__(gdelt.GDELTPuller)
    puller.engine = types.SimpleNamespace(
        begin=lambda: _NullContext(types.SimpleNamespace())
    )
    puller.source_id = "gdelt-source"

    calls = {"themes": 0, "actors": 0, "tensions": 0, "signals": 0}

    def fake_fetch(query, mode, timespan):
        calls["themes"] += 1
        assert timespan == "1d"
        return {"timeline": []}

    monkeypatch.setattr(puller, "_fetch_gdelt_api", fake_fetch)
    monkeypatch.setattr(puller, "_pull_actor_tones", lambda: calls.__setitem__("actors", 1) or 0)
    monkeypatch.setattr(puller, "_pull_tension_scores", lambda: calls.__setitem__("tensions", 1) or 0)
    monkeypatch.setattr(puller, "_emit_tension_signals", lambda: calls.__setitem__("signals", 1) or 0)
    monkeypatch.setattr(gdelt, "time", types.SimpleNamespace(sleep=lambda _seconds: None))

    result = puller.pull_recent(
        days_back=1,
        max_theme_queries=2,
        include_actor_tones=False,
        include_tensions=False,
        include_signals=False,
    )

    assert result["status"] == "SUCCESS"
    assert calls == {"themes": 2, "actors": 0, "tensions": 0, "signals": 0}


def test_gdelt_rate_limit_is_clean_skip(monkeypatch) -> None:
    from ingestion.altdata import gdelt

    response = types.SimpleNamespace(status_code=429, raise_for_status=lambda: None)
    monkeypatch.setattr(gdelt.requests, "get", lambda *args, **kwargs: response)

    puller = gdelt.GDELTPuller.__new__(gdelt.GDELTPuller)
    result = puller._fetch_gdelt_api("economy recession", "timelineTone", "1d")

    assert result == {"timeline": [], "status": "SKIPPED", "http_status": 429}


class _NullContext:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, exc_type, exc, tb):
        return False
