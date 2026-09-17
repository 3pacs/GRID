"""Coverage for alerts/email.py::daily_digest() — the digest that sat
unreachable for ~4 months (see GRID-wt-alerts-digest-wiring PR) because
nothing called schedule_alerts(). These tests exist so a revival like that
one is checked for content correctness, not just wiring: a collector
failure must never look like "All systems operational", and a dry run
must never touch the real send path.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import alerts.email as email_mod


class _FakeResult:
    def __init__(self, one=None, many=None):
        self._one = one
        self._many = many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _FakeConn:
    """Routes queries to canned rows by a substring in the SQL text."""

    def __init__(self, routes: dict[str, _FakeResult]):
        self._routes = routes

    def execute(self, clause, *_args, **_kwargs):
        sql = str(clause)
        for needle, result in self._routes.items():
            if needle in sql:
                return result
        return _FakeResult(one=None, many=[])

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _FakeEngine:
    def __init__(self, routes: dict[str, _FakeResult]):
        self._routes = routes

    def connect(self):
        return _FakeConn(self._routes)


def _patch_engine(monkeypatch, routes: dict[str, _FakeResult]):
    monkeypatch.setattr("db.get_engine", lambda: _FakeEngine(routes), raising=False)
    monkeypatch.setattr(
        "intelligence.long_plays.load_latest_board", lambda engine: None, raising=False
    )


def _patch_send(monkeypatch):
    calls = []
    monkeypatch.setattr(email_mod, "_send", lambda *a, **k: calls.append((a, k)))
    return calls


def test_dry_run_never_calls_send(monkeypatch) -> None:
    _patch_engine(monkeypatch, routes={})
    calls = _patch_send(monkeypatch)

    result = email_mod.daily_digest(dry_run=True)

    assert result["dry_run"] is True
    assert calls == []


def test_collector_failure_is_reported_not_hidden_as_all_systems_operational(monkeypatch) -> None:
    """This is the exact bug the reviewer flagged: every collector wraps its
    query in try/except, so an outage that makes every query raise ends up
    indistinguishable from "nothing happened" — and the old code then
    claimed 'All systems operational' regardless.
    """

    class _AlwaysRaisingConn(_FakeConn):
        def execute(self, *_a, **_k):
            raise RuntimeError("simulated DB outage")

    monkeypatch.setattr(
        "db.get_engine",
        lambda: type("E", (), {"connect": lambda self: _AlwaysRaisingConn({})})(),
        raising=False,
    )
    monkeypatch.setattr(
        "intelligence.long_plays.load_latest_board",
        lambda engine: (_ for _ in ()).throw(RuntimeError("also down")),
        raising=False,
    )

    result = email_mod.daily_digest(dry_run=True)

    assert set(result["degraded"]) == {
        "regime", "journal_count", "100x_opportunities", "data_freshness", "long_plays",
    }
    assert "Data collection issues" in result["section_titles"]
    assert "Status" not in result["section_titles"]


def test_genuinely_quiet_day_is_not_marked_degraded(monkeypatch) -> None:
    """The journal-count KPI tile is unconditional on a successful query, so
    a quiet day (zero rows everywhere) still yields one section — it must
    not be flagged as a collector failure.
    """
    _patch_engine(monkeypatch, routes={
        "inferred_state": _FakeResult(one=None),
        "COUNT(*)": _FakeResult(one=(0,)),
        "options_mispricing_scans": _FakeResult(many=[]),
        "raw_series": _FakeResult(one=(0, None)),
    })

    result = email_mod.daily_digest(dry_run=True)

    assert result["degraded"] == []
    assert "Data collection issues" not in result["section_titles"]
    assert result["section_titles"] == ["Decisions (24h)"]


def test_stale_regime_is_labelled_stale_not_shown_as_current(monkeypatch) -> None:
    stale_ts = datetime.now(timezone.utc) - timedelta(hours=email_mod._STALE_REGIME_HOURS + 1)
    _patch_engine(monkeypatch, routes={
        "inferred_state": _FakeResult(one=("RISK_ON", 0.8, "BUY", stale_ts)),
        "COUNT(*)": _FakeResult(one=(0,)),
        "options_mispricing_scans": _FakeResult(many=[]),
        "raw_series": _FakeResult(one=(0, None)),
    })

    result = email_mod.daily_digest(dry_run=True)

    assert "Regime State" in result["section_titles"]
    assert "regime" not in result["degraded"]


def test_fresh_regime_uses_the_normal_regime_section(monkeypatch) -> None:
    fresh_ts = datetime.now(timezone.utc) - timedelta(hours=1)
    _patch_engine(monkeypatch, routes={
        "inferred_state": _FakeResult(one=("RISK_ON", 0.8, "BUY", fresh_ts)),
        "COUNT(*)": _FakeResult(one=(0,)),
        "options_mispricing_scans": _FakeResult(many=[]),
        "raw_series": _FakeResult(one=(0, None)),
    })

    result = email_mod.daily_digest(dry_run=True)

    assert "Regime State" in result["section_titles"]
    assert result["degraded"] == []
