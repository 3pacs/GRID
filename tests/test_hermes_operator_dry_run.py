from __future__ import annotations

import sys
from types import SimpleNamespace


class _FakeConn:
    def execute(self, *_args, **_kwargs):
        return SimpleNamespace(rowcount=0)


class _FakeBegin:
    def __init__(self, calls: list[str]):
        self._calls = calls

    def __enter__(self):
        self._calls.append("resolution_begin")
        return _FakeConn()

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeEngine:
    def __init__(self, calls: list[str]):
        self._calls = calls

    def begin(self):
        return _FakeBegin(self._calls)


class _FakeCooldowns:
    def can_retry(self, _source: str) -> bool:
        return True

    def record_attempt(self, *_args, **_kwargs) -> None:
        return None

    def blacklist_for_timeout(self, *_args, **_kwargs) -> None:
        return None

    def skipped_sources(self) -> list[str]:
        return []

    def blacklisted_sources(self) -> list[dict]:
        return []


class _FakeState:
    def __init__(self):
        self.cycle_count = 0
        self.current_step = None
        self.consecutive_failures = 0
        self.pulls_retried = 0
        self.fixes_applied = 0
        self.errors_diagnosed = 0
        self.cooldowns = _FakeCooldowns()

    def to_dict(self) -> dict:
        return {"cycle_count": self.cycle_count}


def test_hermes_dry_run_skips_mutating_cycle_steps(monkeypatch):
    import scripts.hermes_operator as hermes

    calls: list[str] = []
    fake_engine = _FakeEngine(calls)

    monkeypatch.setitem(
        sys.modules,
        "db",
        SimpleNamespace(get_engine=lambda: fake_engine),
    )
    monkeypatch.setattr(
        hermes,
        "check_system_health",
        lambda _engine: {
            "db": {"healthy": True, "stale_sources": []},
            "hermes": {"healthy": True},
            "overall_healthy": True,
        },
    )
    monkeypatch.setattr(
        hermes,
        "git_pull",
        lambda: calls.append("git_pull") or {"status": "ok"},
    )
    monkeypatch.setattr(
        hermes,
        "_ensure_issues_table",
        lambda _engine: calls.append("ensure_issues"),
    )
    monkeypatch.setattr(
        hermes,
        "diagnose_and_fix_pulls",
        lambda *_args, **_kwargs: {
            "retried": 0,
            "fixed": 0,
            "diagnosed": 0,
            "skipped_cooldown": 0,
            "skipped_no_handler": 0,
        },
    )
    monkeypatch.setattr(
        hermes,
        "_run_with_timeout",
        lambda _name, fn, _timeout, _state: (fn(), True),
    )

    class FakeSmartScheduler:
        def __init__(self, _engine):
            calls.append("smart_scheduler_init")

        def tick(self):
            calls.append("smart_scheduler_tick")
            return {"ran": 1, "succeeded": 1, "still_due": []}

    monkeypatch.setitem(
        sys.modules,
        "ingestion.smart_scheduler",
        SimpleNamespace(SmartScheduler=FakeSmartScheduler),
    )
    monkeypatch.setitem(
        sys.modules,
        "scripts.daily_digest",
        SimpleNamespace(maybe_send_daily_digest=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.heartbeat",
        SimpleNamespace(
            run_heartbeat=lambda _engine: [],
            format_alerts=lambda _alerts: "",
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.adapters.signal_adapter",
        SimpleNamespace(
            publish_all_alpha_signals=lambda _engine: calls.append("publish_alpha"),
        ),
    )
    monkeypatch.setattr(
        hermes,
        "git_push_outputs",
        lambda: calls.append("git_push") or {"status": "ok"},
    )
    monkeypatch.setattr(
        hermes,
        "save_cycle_snapshot",
        lambda *_args, **_kwargs: calls.append("snapshot"),
    )
    monkeypatch.setattr(
        hermes,
        "_emit_obsidian_cycle_report",
        lambda *_args, **_kwargs: calls.append("obsidian_report"),
    )

    result = hermes.run_cycle(_FakeState(), dry_run=True)

    assert result["dry_run"] is True
    assert result["git_pull"] == {"skipped": "dry_run"}
    assert result["ingestion"] == {"skipped": "dry_run"}
    assert result["resolution"] == {"skipped": "dry_run"}
    assert result["git_push"] == {"skipped": "dry_run"}
    assert result["snapshot"] == {"skipped": "dry_run"}
    assert result["obsidian_report"] == {"skipped": "dry_run"}

    assert "git_pull" not in calls
    assert "ensure_issues" not in calls
    assert "smart_scheduler_init" not in calls
    assert "smart_scheduler_tick" not in calls
    assert "resolution_begin" not in calls
    assert "git_push" not in calls
    assert "snapshot" not in calls
    assert "obsidian_report" not in calls
    assert "publish_alpha" not in calls


def test_hermes_dry_run_does_not_send_health_alerts(monkeypatch):
    import scripts.hermes_operator as hermes

    calls: list[str] = []
    fake_engine = _FakeEngine(calls)

    monkeypatch.setitem(
        sys.modules,
        "db",
        SimpleNamespace(get_engine=lambda: fake_engine),
    )
    monkeypatch.setattr(
        hermes,
        "check_system_health",
        lambda _engine: {
            "db": {
                "healthy": True,
                "stale_sources": [{"source": f"s{i}", "last_pull": None} for i in range(30)],
                "failed_pulls_24h": 500,
            },
            "hermes": {"healthy": True},
            "overall_healthy": False,
        },
    )
    monkeypatch.setattr(
        hermes,
        "diagnose_and_fix_pulls",
        lambda *_args, **_kwargs: {"retried": 0, "fixed": 0, "diagnosed": 0},
    )
    monkeypatch.setattr(
        hermes,
        "_run_with_timeout",
        lambda _name, fn, _timeout, _state: (fn(), True),
    )
    monkeypatch.setitem(
        sys.modules,
        "alerts.health_alerter",
        SimpleNamespace(check_and_alert=lambda _health: calls.append("health_alert")),
    )
    monkeypatch.setitem(
        sys.modules,
        "scripts.daily_digest",
        SimpleNamespace(maybe_send_daily_digest=lambda *_args, **_kwargs: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.heartbeat",
        SimpleNamespace(run_heartbeat=lambda _engine: []),
    )
    monkeypatch.setitem(
        sys.modules,
        "alpha_research.adapters.signal_adapter",
        SimpleNamespace(publish_all_alpha_signals=lambda _engine: None),
    )

    hermes.run_cycle(_FakeState(), dry_run=True)

    assert "health_alert" not in calls
