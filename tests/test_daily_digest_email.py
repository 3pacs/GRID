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
