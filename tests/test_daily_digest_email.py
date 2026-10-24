"""Coverage for alerts/email.py::daily_digest() — the digest that sat
unreachable for ~4 months (see GRID-wt-alerts-digest-wiring PR) because
nothing called schedule_alerts(). These tests exist so a revival like that
one is checked for content correctness, not just wiring: a collector
failure must never look like "All systems operational", a dry run must
never touch the real send path, "sent" must mean a confirmed SMTP outcome
rather than a dispatched-to-a-thread guess, and a missing/future/stale
regime timestamp must never render as a current regime.
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


def _patch_send_sync(monkeypatch, returns: str = "sent"):
    calls = []

    def _fake(*a, **k):
        calls.append((a, k))
        return returns

    monkeypatch.setattr(email_mod, "_send_sync", _fake)
    return calls


_QUIET_ROUTES = {
    "inferred_state": _FakeResult(one=None),
    "COUNT(*)": _FakeResult(one=(0,)),
    "options_mispricing_scans": _FakeResult(many=[]),
    "raw_series": _FakeResult(one=(0, None)),
}


def test_dry_run_never_calls_send(monkeypatch) -> None:
    _patch_engine(monkeypatch, routes={})
    calls = _patch_send_sync(monkeypatch)

    result = email_mod.daily_digest(dry_run=True)

    assert result["dry_run"] is True
    assert calls == []


def test_live_send_confirmed_sets_sent_true(monkeypatch) -> None:
    _patch_engine(monkeypatch, routes=_QUIET_ROUTES)
    calls = _patch_send_sync(monkeypatch, returns="sent")

    result = email_mod.daily_digest(dry_run=False)

    assert calls  # _send_sync was actually invoked
    assert result["send_status"] == "sent"
    assert result["sent"] is True
    assert "error" not in result


def test_live_send_failed_sets_sent_false(monkeypatch) -> None:
    """This is the exact bug the reviewer flagged: the previous version
    called the fire-and-forget _send() and then set sent=True
    unconditionally, before SMTP had even attempted anything. daily_digest
    must only report sent=True once _send_sync has actually confirmed it.
    """
    _patch_engine(monkeypatch, routes=_QUIET_ROUTES)
    _patch_send_sync(monkeypatch, returns="failed")

    result = email_mod.daily_digest(dry_run=False)

    assert result["send_status"] == "failed"
    assert result["sent"] is False
    assert "error" in result


def test_live_send_uncertain_is_not_reported_as_sent(monkeypatch) -> None:
    """An ambiguous SMTP outcome (connection lost mid-transmission) must
    never be collapsed into either "sent" or a plain "failed" — the
    scheduler treats "uncertain" differently from a clean failure (it
    does not auto-retry), and that distinction has to survive here.
    """
    _patch_engine(monkeypatch, routes=_QUIET_ROUTES)
    _patch_send_sync(monkeypatch, returns="uncertain")

    result = email_mod.daily_digest(dry_run=False)

    assert result["send_status"] == "uncertain"
    assert result["sent"] is False
    assert "error" in result


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
    _patch_engine(monkeypatch, routes=_QUIET_ROUTES)

    result = email_mod.daily_digest(dry_run=True)

    assert result["degraded"] == []
    assert "Data collection issues" not in result["section_titles"]
    assert result["section_titles"] == ["Decisions (24h)"]


def _regime_result(monkeypatch, row_tuple):
    routes = dict(_QUIET_ROUTES)
    routes["inferred_state"] = _FakeResult(one=row_tuple)
    _patch_engine(monkeypatch, routes=routes)
    return email_mod.daily_digest(dry_run=True)


def test_fresh_regime_uses_the_plain_regime_title(monkeypatch) -> None:
    fresh_ts = datetime.now(timezone.utc) - timedelta(hours=1)
    result = _regime_result(monkeypatch, ("RISK_ON", 0.8, "BUY", fresh_ts))

    assert result["degraded"] == []
    # Exact title match, not "in" — the stale/unverified branches share
    # the "Regime State" prefix, so a substring check alone would still
    # pass if the fresh branch accidentally took one of those instead.
    assert "Regime State" in result["section_titles"]
    assert "Regime State — stale" not in result["section_titles"]
    assert "Regime State — unverified" not in result["section_titles"]


def test_stale_regime_gets_a_distinct_title_and_states_its_age(monkeypatch) -> None:
    stale_ts = datetime.now(timezone.utc) - timedelta(hours=email_mod._STALE_REGIME_HOURS + 1)
    result = _regime_result(monkeypatch, ("RISK_ON", 0.8, "BUY", stale_ts))

    assert "regime" not in result["degraded"]
    assert "Regime State — stale" in result["section_titles"]
    assert "Regime State" not in [
        t for t in result["section_titles"] if t != "Regime State — stale"
    ]
    body = next(s["body"] for s in result["section_bodies"] if s["title"] == "Regime State — stale")
    assert "h ago" in body


def test_missing_regime_timestamp_is_unverified_not_current(monkeypatch) -> None:
    """Defensive handling for a NULL decision_timestamp (schema says NOT
    NULL, but code that treats "unknown age" as "current" is exactly
    backwards if that constraint is ever violated or relaxed) must not
    silently fall into the normal current-regime rendering branch.
    """
    result = _regime_result(monkeypatch, ("RISK_ON", 0.8, "BUY", None))

    assert "Regime State — unverified" in result["section_titles"]


def test_future_regime_timestamp_is_rejected_not_treated_as_extra_fresh(monkeypatch) -> None:
    future_ts = datetime.now(timezone.utc) + timedelta(hours=5)
    result = _regime_result(monkeypatch, ("RISK_ON", 0.8, "BUY", future_ts))

    assert "Regime State — unverified" in result["section_titles"]
    body = next(
        s["body"] for s in result["section_bodies"] if s["title"] == "Regime State — unverified"
    )
    assert "future" in body.lower()


def test_dry_run_preview_includes_rendered_body_not_just_titles(monkeypatch) -> None:
    fresh_ts = datetime.now(timezone.utc) - timedelta(hours=1)
    result = _regime_result(monkeypatch, ("RISK_ON", 0.8, "BUY", fresh_ts))

    assert "section_bodies" in result
    assert all("body" in s for s in result["section_bodies"])
