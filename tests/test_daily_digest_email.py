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
from types import SimpleNamespace

import pytest

import alerts.email as email_mod


@pytest.fixture(autouse=True)
def _synthetic_research_audience(monkeypatch):
    """Content tests require an explicit research audience, never live config."""
    monkeypatch.setattr(email_mod, "_get_settings", lambda: SimpleNamespace(
        RESEARCH_DIGEST_EMAIL_TO=("research.one@example.com", "research.two@example.com")))


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
    """Scoped to the transport paths this module actually has: the
    synchronous send path daily_digest uses live (_send_sync), the
    fire-and-forget one other alert functions in this module use (_send),
    and smtplib.SMTP itself — proving this at the client-construction
    level, not just by checking that the module's own wrapper functions
    weren't invoked, so a future change that builds an SMTP client
    directly instead of going through _do_send would still be caught
    here. This does not prove no transport of any kind anywhere in the
    process fires — only these three paths, which are the ones reachable
    from alerts/email.py. daily_digest never imports alerts.push_notify
    (grep-verified), so push transport isn't a separate call path to
    assert against for this function; it is not exercised or ruled out
    by this test.
    """
    import smtplib

    smtp_calls: list = []
    monkeypatch.setattr(smtplib, "SMTP", lambda *a, **k: smtp_calls.append((a, k)))

    _patch_engine(monkeypatch, routes={})
    sync_calls = _patch_send_sync(monkeypatch)
    fire_and_forget_calls = []
    monkeypatch.setattr(
        email_mod, "_send",
        lambda *a, **k: fire_and_forget_calls.append((a, k)),
    )

    result = email_mod.daily_digest(dry_run=True)

    assert result["dry_run"] is True
    assert sync_calls == []
    assert fire_and_forget_calls == []
    assert smtp_calls == []


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
    assert result["section_titles"] == ["At a glance", "Decisions (24h)"]


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


# ---------------------------------------------------------------------------
# 100x confidence must survive into the rendered alert (not just is_100x)
# ---------------------------------------------------------------------------

def _hundredx_result(monkeypatch, row_tuple):
    routes = dict(_QUIET_ROUTES)
    routes["options_mispricing_scans"] = _FakeResult(many=[row_tuple])
    _patch_engine(monkeypatch, routes=routes)
    return email_mod.daily_digest(dry_run=True)


def test_low_confidence_100x_row_is_visually_flagged(monkeypatch) -> None:
    """is_100x=TRUE alone doesn't mean the row is trustworthy — the scanner
    stores its own LOW/MEDIUM/HIGH confidence alongside the score, and a
    LOW-confidence flagged row must not render with the same purple,
    unqualified "100x Opportunity" treatment as a HIGH-confidence one. The
    label says "heuristic confidence", never the bare word — see
    docs/reference/CONFIDENCE_POLICY.md — since the scanner computes it
    from a composite-score threshold, not a scored track record.
    """
    result = _hundredx_result(
        monkeypatch, ("XYZ", "CALL", 9.1, 150.0, "thin-chain dislocation", "LOW"),
    )

    body = next(s for s in result["section_bodies"] if s["title"] == "100x Opportunity — XYZ")
    assert body["accent"] == "amber"
    assert "HEURISTIC CONFIDENCE: LOW" in body["body"]
    assert "scanner heuristic" in body["body"]


def test_high_confidence_100x_row_is_labelled_heuristic_not_bare_confidence(monkeypatch) -> None:
    """A HIGH bucket is still just a threshold on the same composite score
    as LOW/MEDIUM — not a calibrated high-confidence signal. It must not
    render as an unqualified "confidence", or a reader could mistake the
    scanner's own bucket for a measured one.
    """
    result = _hundredx_result(
        monkeypatch, ("XYZ", "CALL", 9.1, 150.0, "thin-chain dislocation", "HIGH"),
    )

    body = next(s for s in result["section_bodies"] if s["title"] == "100x Opportunity — XYZ")
    assert body["accent"] == "purple"
    assert "heuristic confidence: HIGH" in body["body"]
    assert "HEURISTIC CONFIDENCE: LOW" not in body["body"]


def test_medium_confidence_100x_row_is_labelled_heuristic_not_bare_confidence(monkeypatch) -> None:
    result = _hundredx_result(
        monkeypatch, ("XYZ", "CALL", 9.1, 150.0, "thin-chain dislocation", "MEDIUM"),
    )

    body = next(s for s in result["section_bodies"] if s["title"] == "100x Opportunity — XYZ")
    assert body["accent"] == "purple"
    assert "heuristic confidence: MEDIUM" in body["body"]
    assert "HEURISTIC CONFIDENCE: LOW" not in body["body"]


def test_missing_confidence_100x_row_is_labelled_unknown_not_low(monkeypatch) -> None:
    """A row with no recognizable confidence label (NULL, empty, or a value
    this code doesn't know about) is not known to be LOW — that would
    assert something the data doesn't say. It must still fail toward the
    cautious (amber) rendering, but with wording that doesn't invent a
    confidence category, same fail-closed-but-honest posture as the
    regime section's handling of a missing/unverifiable timestamp.
    """
    result = _hundredx_result(
        monkeypatch, ("XYZ", "CALL", 9.1, 150.0, "thin-chain dislocation", None),
    )

    body = next(s for s in result["section_bodies"] if s["title"] == "100x Opportunity — XYZ")
    assert body["accent"] == "amber"
    assert "HEURISTIC CONFIDENCE: UNKNOWN" in body["body"]
    assert "HEURISTIC CONFIDENCE: LOW" not in body["body"]


def test_malformed_confidence_100x_row_is_labelled_unknown_not_low(monkeypatch) -> None:
    """A value that isn't LOW/MEDIUM/HIGH at all (garbage, a future label
    this code hasn't been taught) must not silently become MEDIUM/HIGH's
    plain rendering, and must not be misreported as the specific "LOW"
    the scanner never actually recorded either. This is a reachable case,
    not a hypothetical one: options_mispricing_scans.confidence is
    `TEXT NOT NULL` with no CHECK constraint on its values (schema.sql
    verified directly — contrast decision_journal.operator_confidence,
    which does constrain its values), so nothing in the database stops a
    non-LOW/MEDIUM/HIGH string from being written.
    """
    result = _hundredx_result(
        monkeypatch, ("XYZ", "CALL", 9.1, 150.0, "thin-chain dislocation", "banana"),
    )

    body = next(s for s in result["section_bodies"] if s["title"] == "100x Opportunity — XYZ")
    assert body["accent"] == "amber"
    assert "HEURISTIC CONFIDENCE: UNKNOWN" in body["body"]
    assert "HEURISTIC CONFIDENCE: LOW" not in body["body"]


def test_alert_on_100x_opportunity_defaults_to_unknown_confidence_rendering(monkeypatch) -> None:
    """The standalone alert function (called directly by
    scripts/run_full_pipeline.py, not through daily_digest) must apply the
    same fail-closed-but-honest default when its caller doesn't pass a
    confidence: caution without asserting a specific "LOW" it wasn't told.
    """
    captured: dict = {}
    monkeypatch.setattr(
        email_mod, "_send",
        lambda subject, sections, footer_note="": captured.update(
            subject=subject, sections=sections,
        ),
    )

    email_mod.alert_on_100x_opportunity("XYZ", 9.1, "CALL", "thesis text")

    assert captured["sections"][0]["accent"] == "amber"
    assert "HEURISTIC CONFIDENCE: UNKNOWN" in captured["sections"][0]["body"]
    assert "HEURISTIC CONFIDENCE: LOW" not in captured["sections"][0]["body"]

    captured.clear()
    email_mod.alert_on_100x_opportunity("XYZ", 9.1, "CALL", "thesis text", confidence="LOW")

    assert captured["sections"][0]["accent"] == "amber"
    assert "HEURISTIC CONFIDENCE: LOW" in captured["sections"][0]["body"]

    captured.clear()
    email_mod.alert_on_100x_opportunity("XYZ", 9.1, "CALL", "thesis text", confidence="HIGH")

    assert captured["sections"][0]["accent"] == "purple"
    assert "heuristic confidence: HIGH" in captured["sections"][0]["body"]
    assert "HEURISTIC CONFIDENCE: LOW" not in captured["sections"][0]["body"]
    assert "HEURISTIC CONFIDENCE: UNKNOWN" not in captured["sections"][0]["body"]


# ---------------------------------------------------------------------------
# Data freshness must not count a source as "active" off a FAILED-only pull
# ---------------------------------------------------------------------------

def test_data_freshness_query_filters_to_successful_pulls(monkeypatch) -> None:
    """raw_series gets a row on a FAILED pull too (value=0, a real
    pull_timestamp) — counting every row regardless of pull_status would
    report a source as "active" off pulls that never produced an actual
    observation. Assert the real SQL text sent to the database, not just
    the returned count, so a future edit that quietly drops the filter
    (while keeping some other row shape that happens to satisfy the fake)
    is still caught.
    """
    captured_sql: list[str] = []

    class _CapturingConn(_FakeConn):
        def execute(self, clause, *args, **kwargs):
            captured_sql.append(str(clause))
            return super().execute(clause, *args, **kwargs)

    routes = dict(_QUIET_ROUTES)
    monkeypatch.setattr(
        "db.get_engine",
        lambda: type("E", (), {"connect": lambda self: _CapturingConn(routes)})(),
        raising=False,
    )
    monkeypatch.setattr(
        "intelligence.long_plays.load_latest_board", lambda engine: None, raising=False,
    )

    email_mod.daily_digest(dry_run=True)

    freshness_sql = [s for s in captured_sql if "raw_series" in s and "COUNT(DISTINCT source_id)" in s]
    assert freshness_sql, "expected the data-freshness query to run"
    assert "pull_status" in freshness_sql[0]
    assert "SUCCESS" in freshness_sql[0]


def test_data_freshness_ignores_a_failed_only_source(monkeypatch) -> None:
    """Behavioral counterpart to the SQL-text check above: a fake DB that
    actually honors the pull_status filter (unlike the substring-routed
    _FakeConn, which can't) should report zero active sources when the
    only row in the window is a FAILED one — proving the fix changes real
    query results, not just query text.
    """
    class _StatusAwareConn:
        """Minimal stand-in that evaluates the one predicate this test cares
        about instead of ignoring pull_status like the shared _FakeConn.
        """
        def __init__(self, only_row_status: str):
            self._status = only_row_status

        def execute(self, clause, *_args, **_kwargs):
            sql = str(clause)
            if "raw_series" in sql and "COUNT(DISTINCT source_id)" in sql:
                if "pull_status = 'SUCCESS'" in sql and self._status != "SUCCESS":
                    return _FakeResult(one=(0, None))
                return _FakeResult(one=(1, datetime.now(timezone.utc)))
            if "inferred_state" in sql:
                return _FakeResult(one=None)
            if "COUNT(*)" in sql:
                return _FakeResult(one=(0,))
            if "options_mispricing_scans" in sql:
                return _FakeResult(many=[])
            return _FakeResult(one=None, many=[])

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(
        "db.get_engine",
        lambda: type("E", (), {"connect": lambda self: _StatusAwareConn("FAILED")})(),
        raising=False,
    )
    monkeypatch.setattr(
        "intelligence.long_plays.load_latest_board", lambda engine: None, raising=False,
    )

    result = email_mod.daily_digest(dry_run=True)

    assert "Active Sources (24h)" not in result["section_titles"]


# Current-main consolidation controls. These use only synthetic rows/transports.
def test_digest_raw_read_guard_has_no_permissive_allowance() -> None:
    import runpy
    from pathlib import Path

    guard = runpy.run_path(str(Path(__file__).with_name("test_raw_series_read_guard.py")))
    assert "alerts/email.py" not in guard["LEGACY_UNFILTERED_READS"]
    assert guard["_unfiltered_reads"]().get("alerts/email.py", 0) == 0


def test_push_wrapper_forwards_confidence_and_preserves_legacy_call(monkeypatch) -> None:
    import alerts.push_notify as push

    sent, pushed = [], []
    monkeypatch.setattr(email_mod, "_send", lambda subject, sections: sent.append(sections))
    outcomes = iter((0, 3, 0))
    def fake_push(*args):
        pushed.append(args)
        return next(outcomes)
    monkeypatch.setattr(push, "notify_trade_recommendation", fake_push)
    # Register these assignments with monkeypatch before the integration hook
    # replaces them, so the real functions are restored after this test.
    for name in ("alert_on_regime_change", "alert_on_100x_opportunity", "alert_on_failure"):
        monkeypatch.setattr(email_mod, name, getattr(email_mod, name))
    push.integrate_with_email_alerts()

    assert email_mod.alert_on_100x_opportunity("SYN", 8.0, "CALL", "synthetic") is None
    assert "HEURISTIC CONFIDENCE: UNKNOWN" in sent[-1][0]["body"]
    assert email_mod.alert_on_100x_opportunity(
        "SYN", 8.0, "CALL", "synthetic", confidence="HIGH",
    ) is None
    assert "heuristic confidence: HIGH" in sent[-1][0]["body"]
    assert email_mod.alert_on_100x_opportunity(
        "SYN", 8.0, "CALL", "synthetic", confidence="LOW",
    ) is None
    assert "HEURISTIC CONFIDENCE: LOW" in sent[-1][0]["body"]
    assert pushed == [("SYN", "CALL")] * 3


def test_push_wrapper_preserves_failure_without_sending_push(monkeypatch) -> None:
    import alerts.push_notify as push

    pushed = []
    def fail(*a, **kw):
        raise RuntimeError("synthetic email failure")
    for name in ("alert_on_regime_change", "alert_on_100x_opportunity", "alert_on_failure"):
        monkeypatch.setattr(email_mod, name, getattr(email_mod, name))
    monkeypatch.setattr(email_mod, "alert_on_100x_opportunity", fail)
    monkeypatch.setattr(push, "notify_trade_recommendation", lambda *a: pushed.append(a))
    push.integrate_with_email_alerts()
    try:
        email_mod.alert_on_100x_opportunity("SYN", 8.0, "CALL", "synthetic", confidence="HIGH")
    except RuntimeError as exc:
        assert str(exc) == "synthetic email failure"
    else:
        raise AssertionError("existing email failure must still propagate")
    assert pushed == []


def test_push_wrapper_keeps_email_before_push_failure(monkeypatch) -> None:
    import alerts.push_notify as push

    sent = []
    monkeypatch.setattr(email_mod, "_send", lambda subject, sections: sent.append(sections))
    for name in ("alert_on_regime_change", "alert_on_100x_opportunity", "alert_on_failure"):
        monkeypatch.setattr(email_mod, name, getattr(email_mod, name))
    def fail_push(*args):
        raise RuntimeError("synthetic push failure")
    monkeypatch.setattr(push, "notify_trade_recommendation", fail_push)
    push.integrate_with_email_alerts()
    try:
        email_mod.alert_on_100x_opportunity("SYN", 8.0, "CALL", "synthetic", confidence="HIGH")
    except RuntimeError as exc:
        assert str(exc) == "synthetic push failure"
    else:
        raise AssertionError("existing push failure must still propagate")
    assert len(sent) == 1
    assert "heuristic confidence: HIGH" in sent[0][0]["body"]


_DIGEST_CUTOFF = datetime(2026, 10, 26, 0, 0, 0, tzinfo=timezone.utc)


def _bounded_digest(monkeypatch, *, journals=(), options=(), pulls=()):
    """Predicate-aware fake for the four direct digest queries.

    Assert real SQL clauses and bound parameters before applying those
    clauses to synthetic records. This is not a PostgreSQL engine proof.
    A second wall-clock read deliberately crosses into the next day.
    """
    trace = []
    class Clock(datetime):
        calls = 0
        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            return _DIGEST_CUTOFF if cls.calls == 1 else _DIGEST_CUTOFF + timedelta(days=1)
    monkeypatch.setattr(email_mod, "datetime", Clock)

    class Conn:
        def execute(self, clause, params):
            sql = str(clause)
            trace.append((sql, params))
            assert "NOW()" not in sql and "CURRENT_DATE" not in sql
            if "inferred_state" in sql:
                assert "decision_timestamp <= :as_of OR decision_timestamp IS NULL" in sql
                assert params == {"as_of": _DIGEST_CUTOFF}
                eligible = [row for row in journals if row[3] is None or row[3] <= params["as_of"]]
                eligible.sort(key=lambda row: (row[3] is None, row[3] or _DIGEST_CUTOFF), reverse=True)
                return _FakeResult(one=eligible[0] if eligible else None)
            if "COUNT(*)" in sql:
                assert "decision_timestamp >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'" in sql
                assert "AND decision_timestamp <= :as_of" in sql
                assert params == {"as_of": _DIGEST_CUTOFF}
                lower = params["as_of"] - timedelta(hours=24)
                return _FakeResult(one=(sum(row[3] is not None and lower <= row[3] <= params["as_of"] for row in journals),))
            if "options_mispricing_scans" in sql:
                assert "scan_date >= :as_of_date - 3" in sql
                assert "AND scan_date <= :as_of_date" in sql
                assert params == {"as_of_date": _DIGEST_CUTOFF.date()}
                lower = params["as_of_date"] - timedelta(days=3)
                eligible = [row for at, row in options if lower <= at <= params["as_of_date"]]
                return _FakeResult(many=sorted(eligible, key=lambda row: row[2], reverse=True)[:5])
            if "raw_series" in sql:
                assert "pull_timestamp >= CAST(:as_of AS TIMESTAMPTZ) - INTERVAL '24 hours'" in sql
                assert "AND pull_timestamp <= :as_of" in sql
                assert "AND pull_status = 'SUCCESS'" in sql
                assert params == {"as_of": _DIGEST_CUTOFF}
                lower = params["as_of"] - timedelta(hours=24)
                eligible = [(name, at) for name, at, status in pulls if status == "SUCCESS" and lower <= at <= params["as_of"]]
                return _FakeResult(one=(len({name for name, _at in eligible}), max((at for _name, at in eligible), default=None)))
            raise AssertionError("unexpected digest SQL")
        def __enter__(self):
            return self
        def __exit__(self, *_exc):
            return False
    monkeypatch.setattr("db.get_engine", lambda: type("Engine", (), {"connect": lambda self: Conn()})(), raising=False)
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: None, raising=False)
    result = email_mod.daily_digest(dry_run=True)
    assert result["degraded"] == []
    assert result["timestamp"] == _DIGEST_CUTOFF.isoformat()
    assert len(trace) == 4
    assert Clock.calls == 1, "all windows and age checks must share one clock capture"
    return {section["title"]: section["body"] for section in result["section_bodies"]}


def test_digest_timestamp_windows_include_before_and_at_but_exclude_after(monkeypatch) -> None:
    delta = timedelta(microseconds=1)
    for at, included in (
        (_DIGEST_CUTOFF - timedelta(hours=24) - delta, False),
        (_DIGEST_CUTOFF - timedelta(hours=24), True),
        (_DIGEST_CUTOFF - delta, True),
        (_DIGEST_CUTOFF, True),
        (_DIGEST_CUTOFF + delta, False),
    ):
        sections = _bounded_digest(monkeypatch,
            journals=[("SYNTHETIC", 0.8, "HOLD", at)],
            pulls=[("synthetic-source", at, "SUCCESS"), ("failed-source", _DIGEST_CUTOFF, "FAILED")])
        expected = email_mod._section_kpi("Decisions (24h)", str(int(included)))
        assert sections["Decisions (24h)"] == expected["body"]
        assert ("Active Sources (24h)" in sections) is included


def test_digest_options_keep_declared_calendar_window_and_exclude_future_day(monkeypatch) -> None:
    for offset, included in ((-4, False), (-3, True), (-1, True), (0, True), (1, False)):
        at = _DIGEST_CUTOFF.date() + timedelta(days=offset)
        sections = _bounded_digest(monkeypatch,
            options=[(at, ("SYN", "CALL", 8.0, 120.0, "synthetic", "HIGH"))])
        assert ("100x Opportunity — SYN" in sections) is included


def test_digest_latest_regime_ignores_future_poison_and_keeps_boundary_row(monkeypatch) -> None:
    sections = _bounded_digest(monkeypatch, journals=[
        ("BEFORE", 0.8, "HOLD", _DIGEST_CUTOFF - timedelta(microseconds=1)),
        ("BOUNDARY", 0.8, "HOLD", _DIGEST_CUTOFF),
        ("FUTURE_POISON", 0.8, "HOLD", _DIGEST_CUTOFF + timedelta(microseconds=1)),
    ])
    body = sections["Regime State"]
    assert "BOUNDARY" in body
    assert "FUTURE_POISON" not in body and "BEFORE" not in body


def test_digest_latest_regime_keeps_missing_timestamp_unverified(monkeypatch) -> None:
    sections = _bounded_digest(monkeypatch, journals=[("SYNTHETIC", 0.8, "HOLD", None)])
    assert "Regime State — unverified" in sections
    assert "no timestamp" in sections["Regime State — unverified"]


@pytest.mark.parametrize("age_hours,status", [(1, "RISK_ON"), (26, "stale"), (None, "unverified"), (-2, "unverified")])
def test_digest_summary_preserves_regime_freshness_and_scopes_metrics(monkeypatch, age_hours, status):
    ts = datetime.now(timezone.utc) - timedelta(hours=age_hours) if age_hours is not None else None
    routes = dict(_QUIET_ROUTES)
    routes.update({
        "inferred_state": _FakeResult(one=("RISK_ON", .8, "BUY", ts)),
        "COUNT(*)": _FakeResult(one=(17,)),
        "raw_series": _FakeResult(one=(4, datetime.now(timezone.utc))),
        "options_mispricing_scans": _FakeResult(many=[("SYN", "CALL", 8.2, 120, "Volatility is low.", "LOW")]),
    })
    _patch_engine(monkeypatch, routes)
    result = email_mod.daily_digest(dry_run=True)
    lead = result["section_bodies"][0]
    assert lead["title"] == "At a glance"
    assert status in lead["body"]
    assert "17" in lead["body"] and "Decisions recorded (24h)" in lead["body"]
    assert "4 sources with successful pulls (24h)" in lead["body"]
    assert "1 sampled option row (up to 5" in lead["body"]
    assert "1 sampled option rows" not in lead["body"]
    assert "not a total" in lead["body"]
    assert "8.2/10" in lead["body"] and "not probability" in lead["body"]
    assert "LOW" in lead["body"]
    regime = next(s for s in result["section_bodies"] if s["title"].startswith("Regime State"))
    if age_hours == 1:
        assert "Regime State: RISK_ON; Reported state confidence: 80%; Suggested Action: BUY" in lead["body"]
        assert regime["body"] == email_mod._section_regime("RISK_ON", .8, "BUY")["body"]
    else:
        expected_recap = email_mod._summary_text(email_mod._html_to_plain(regime["body"]), 250)
        assert expected_recap in email_mod._html_to_plain(lead["body"])


def test_digest_summary_distinguishes_absent_regime_and_zero_activity(monkeypatch):
    _patch_engine(monkeypatch, _QUIET_ROUTES)
    result = email_mod.daily_digest(dry_run=True)
    lead = result["section_bodies"][0]["body"]
    assert "Regime unavailable" in lead
    assert "0" in lead and "Decisions recorded (24h)" in lead
    assert "0 sources with successful pulls (24h)" in lead
    assert "All systems operational" not in lead


def test_digest_summary_failed_collection_stays_unknown_not_zero(monkeypatch):
    class Broken(_FakeConn):
        def execute(self, clause, *args, **kwargs):
            if "COUNT(*)" in str(clause) or "raw_series" in str(clause):
                raise RuntimeError("synthetic unavailable")
            return super().execute(clause, *args, **kwargs)
    monkeypatch.setattr("db.get_engine", lambda: type("E", (), {"connect": lambda self: Broken({})})())
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: None)
    result = email_mod.daily_digest(dry_run=True)
    lead = result["section_bodies"][0]["body"]
    assert "INCOMPLETE" in lead
    assert "journal_count" in lead and "data_freshness" in lead
    assert "Decisions (24h): unavailable" in lead
    assert "Sources with successful pulls (24h): unavailable" in lead
    assert "0 sources" not in lead


@pytest.mark.parametrize("generated,expected", [
    (None, "generation time unknown"),
    ("2026-01-01T00:00:00+00:00", "h ago"),
    ("2099-01-01T00:00:00+00:00", "future timestamp"),
    ("2026-01-01T00:00:00", "timezone unknown"),
    ("not-a-date", "generation time invalid"),
    (datetime(2026, 1, 1), "timezone unknown"),
    (datetime(2026, 1, 1, tzinfo=timezone.utc), "generated 2026-01-01T00:00:00+00:00"),
    ("2026-01-01T02:00:00+02:00", "generated 2026-01-01T00:00:00+00:00"),
])
def test_research_saved_board_age_and_past_catalysts_are_visible(monkeypatch, generated, expected):
    _patch_engine(monkeypatch, _QUIET_ROUTES)
    board = {"as_of": "2026-01-01", "generated_at": generated, "candidates": [
        {"ticker": "SYN", "stance": "entry_candidate", "projection": {"3y": {"p50_multiple": 2.1}},
         "catalysts": [{"event_type": "earnings", "expected_date": "2026-01-02"}]},
    ]}
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: board)
    result = email_mod.daily_digest(dry_run=True)
    lead = result["section_bodies"][0]["body"]
    details = next(s["body"] for s in result["section_bodies"] if s["title"] == "Long plays")
    assert "Weekly saved snapshot" in lead and "not live eligibility" in lead
    assert "data as of 2026-01-01" in lead and "saved stance:" not in lead
    assert "generated " not in lead
    assert "Weekly saved snapshot, not live eligibility" in details
    assert "2026-01-01" in details and expected in details
    assert "saved stance: entry_candidate" in details
    assert "2026-01-02 (past date; outcome unconfirmed)" in details
    assert result["degraded"] == []
    assert "2.10x (proxy)" in details
    assert expected in email_mod._render_plain(result["section_bodies"])


@pytest.mark.parametrize("summary", [
    ["State <unknown> & unverified.", "5 sampled rows, not a total."],
    ("State <unknown> & unverified.", "5 sampled rows, not a total."),
])
def test_structured_summary_keeps_escaped_html_and_separate_plain_bullets(summary):
    section = email_mod._section_summary("17 < 20", "Decisions & records", summary)
    markup = email_mod._render_html("Synthetic summary", [section])
    plain = email_mod._render_plain([section])
    assert section["body"].count("<li ") == 2
    assert "<ul " in markup and "</ul>" in markup
    assert "17 &lt; 20" in markup and "Decisions &amp; records" in markup
    assert "State &lt;unknown&gt; &amp; unverified." in markup
    assert "<unknown>" not in markup
    for item in summary:
        assert f"- {item}" in plain.splitlines()
        assert f"- {item}" in email_mod._html_to_plain(markup).splitlines()


def test_string_summary_retains_existing_paragraph_and_plain_contract():
    summary = "First <sentence>.\nSecond & sentence."
    section = email_mod._section_summary("7", "Records", summary, "amber")
    assert section == {
        "title": "At a glance",
        "body": ('<div style="font-size:32px;font-weight:800;color:#E8F0F8;">7</div>'
                 '<div style="font-size:13px;color:#9AB2C8;">Records</div>'
                 '<p>First &lt;sentence&gt;.\nSecond &amp; sentence.</p>'),
        "plain": f"7 \u2014 Records\n{summary}",
        "accent": "amber",
    }


def test_daily_recap_renders_bullets_with_scoped_samples_and_full_details(monkeypatch):
    now = datetime.now(timezone.utc)
    thesis = "Synthetic thesis <not HTML> & evidence. " * 30 + "FULL THESIS END."
    routes = dict(_QUIET_ROUTES)
    routes.update({
        "inferred_state": _FakeResult(one=("RISK_ON", .8, "HOLD", now)),
        "COUNT(*)": _FakeResult(one=(17,)),
        "raw_series": _FakeResult(one=(4, now)),
        "options_mispricing_scans": _FakeResult(many=[
            (f"OPT{i}", "CALL", 8.2, 120, thesis, "LOW") for i in range(5)
        ]),
    })
    _patch_engine(monkeypatch, routes)
    board = {
        "as_of": "2026-01-01", "generated_at": "2026-01-01T06:10:00+00:00",
        "candidates": [
            {"ticker": "SKIP", "stance": "avoid"},
            *[{"ticker": f"SYN{i}<tag>", "stance": "watch" if i == 0 else "entry_candidate",
               "coverage_route": "route & detail", "projection": {"3y": {"p50_multiple": 2.1}},
               "catalysts": [{"event_type": "earnings", "expected_date": "2026-01-02"}]}
              for i in range(7)],
        ],
    }
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: board)
    sends = _patch_send_sync(monkeypatch)
    result = email_mod.daily_digest()
    assert result["degraded"] == [] and len(sends) == 1
    sections = sends[0][0][1]
    lead = sections[0]
    recap = email_mod._html_to_plain(lead["body"])
    bullets = [line for line in recap.splitlines() if line.startswith("- ")]
    assert len(bullets) == 5
    assert "- Regime State: RISK_ON; Reported state confidence: 80%; Suggested Action: HOLD" in bullets
    assert "Decisions (24h):" not in recap
    assert "17 \u2014 Decisions recorded (24h)" in lead["plain"]
    assert "5 sampled option rows (up to 5, not a total; scan dates D-3 through D inclusive)" in recap
    assert "Top sampled row: OPT0 CALL, 8.2/10 composite score (not probability); heuristic confidence: LOW" in recap
    assert "Long plays: Weekly saved snapshot" in recap
    assert "5 candidates shown (up to 5)" in recap
    assert "not live eligibility" in recap
    assert "data as of 2026-01-01" in recap
    assert "2026-01-01T06:10" not in recap
    assert "First saved row" not in recap and "saved stance:" not in recap
    detail = next(s for s in sections if s["title"] == "Long plays")
    assert detail["accent"] == "purple"
    assert "Weekly saved snapshot, not live eligibility" in detail["body"]
    assert "Data as of 2026-01-01; generated 2026-01-01T06:10:00+00:00" in detail["body"]
    assert detail["body"].count("saved stance:") == 5
    assert "SYN1&lt;tag&gt;" in detail["body"] and "SYN5&lt;tag&gt;" in detail["body"]
    assert "SYN0" not in detail["body"] and "SYN6" not in detail["body"] and "SKIP" not in detail["body"]
    assert "route &amp; detail" in detail["body"]
    assert "2.10x (proxy)" in detail["body"]
    assert "past date; outcome unconfirmed" in detail["body"]
    markup = email_mod._render_html("Synthetic daily digest", sections)
    plain = email_mod._render_plain(sections)
    assert 'class="card card-accent-purple"' in markup
    assert ".card-accent-purple { border-left-color:#A855F7; }" in markup
    assert "card-accent-#" not in markup
    assert "<tag>" not in markup and "<not HTML>" not in markup
    assert markup.index("At a glance") < markup.index('class="card-title">Long plays')
    assert plain.index("[At a glance]") < plain.index("[Long plays]")
    assert plain.count(thesis) == 5
    for line in bullets:
        assert line in plain.splitlines()
    assert "SYN1<tag> \u2014 saved stance: entry_candidate \u2014 via route & detail" in plain


def test_daily_recap_keeps_stand_down_reason_below_weekly_snapshot_summary(monkeypatch):
    _patch_engine(monkeypatch, _QUIET_ROUTES)
    reason = "No entries <qualified> & evidence missing. " * 20 + "FULL REASON END."
    board = {"as_of": "2026-01-01", "stand_down_reason": reason, "candidates": []}
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: board)
    result = email_mod.daily_digest(dry_run=True)
    lead = result["section_bodies"][0]["body"]
    detail = next(s["body"] for s in result["section_bodies"] if s["title"] == "Long plays")
    assert "Weekly saved snapshot" in lead and "stand-down reason below" in lead
    assert "not live eligibility" in lead and "FULL REASON END" not in lead
    assert reason in email_mod._html_to_plain(detail)
    assert "generation time unknown; age unverified" in detail
    assert "<qualified>" not in detail


def test_degraded_recap_retains_known_decision_count_when_headline_is_incomplete(monkeypatch):
    _patch_engine(monkeypatch, {**_QUIET_ROUTES, "COUNT(*)": _FakeResult(one=(17,))})
    def broken_board(engine):
        raise RuntimeError("Synthetic board unavailable")
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", broken_board)
    result = email_mod.daily_digest(dry_run=True)
    lead = email_mod._html_to_plain(result["section_bodies"][0]["body"])
    assert "INCOMPLETE" in lead
    assert "- Incomplete collection: long_plays." in lead.splitlines()
    assert "- Decisions (24h): 17." in lead.splitlines()
    assert "Weekly saved snapshot" not in lead


@pytest.mark.parametrize("data_date,expected", [
    (None, "unknown"), ("<unverified> & date", "<unverified> & date"),
])
def test_saved_snapshot_recap_keeps_missing_or_untrusted_data_date_honest(monkeypatch, data_date, expected):
    _patch_engine(monkeypatch, _QUIET_ROUTES)
    board = {"as_of": data_date, "stand_down_reason": "Synthetic stand-down."}
    monkeypatch.setattr("intelligence.long_plays.load_latest_board", lambda engine: board)
    result = email_mod.daily_digest(dry_run=True)
    lead = result["section_bodies"][0]["body"]
    assert f"data as of {expected}" in email_mod._html_to_plain(lead)
    assert "<unverified>" not in lead
    assert "not live eligibility" in lead


def test_summary_accepts_single_use_iterable_without_losing_plain_bullets():
    section = email_mod._section_summary("1", "Record", iter(["First.", "Second."]))
    assert section["plain"].endswith("- First.\n- Second.")
    assert email_mod._html_to_plain(section["body"]).endswith("- First.\n- Second.")



def test_absent_decision_count_remains_unknown_in_bullet_recap(monkeypatch):
    _patch_engine(monkeypatch, {**_QUIET_ROUTES, "COUNT(*)": _FakeResult(one=None)})
    result = email_mod.daily_digest(dry_run=True)
    lead = email_mod._html_to_plain(result["section_bodies"][0]["body"])
    assert "RESEARCH UPDATE" in lead
    assert "- Decisions (24h): unavailable." in lead.splitlines()
    assert "Decisions recorded (24h)" not in lead
    assert "Weekly saved snapshot" not in lead


def test_failed_option_sample_stays_unavailable_not_zero_in_bullet_recap(monkeypatch):
    class BrokenOptions(_FakeConn):
        def execute(self, clause, *args, **kwargs):
            if "options_mispricing_scans" in str(clause):
                raise RuntimeError("Synthetic sample unavailable")
            return super().execute(clause, *args, **kwargs)
    _patch_engine(monkeypatch, _QUIET_ROUTES)
    monkeypatch.setattr("db.get_engine", lambda: type("E", (), {
        "connect": lambda self: BrokenOptions(_QUIET_ROUTES),
    })())
    result = email_mod.daily_digest(dry_run=True)
    lead = email_mod._html_to_plain(result["section_bodies"][0]["body"])
    assert result["degraded"] == ["100x_opportunities"]
    assert "INCOMPLETE" in lead
    assert "- Option scan sample unavailable." in lead.splitlines()
    assert "0 sampled option rows" not in lead
    assert "Top sampled row" not in lead
