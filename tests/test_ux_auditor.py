"""Tests for scripts.ux_auditor — UX audit engine."""


from scripts.ux_auditor import (
    _test_journey,
    _truncate_sample,
    _parse_ux_analysis,
    _ENDPOINT_REGISTRY,
    _USER_JOURNEYS,
)

import pytest


class TestTruncateSample:
    def test_short_passthrough(self):
        assert _truncate_sample({"a": 1}) == '{"a": 1}'

    def test_long_truncated(self):
        data = {"key": "x" * 1000}
        result = _truncate_sample(data, max_chars=50)
        assert len(result) < 100
        assert "truncated" in result


class TestParseUxAnalysis:
    def test_parses_score(self):
        response = "SCORE: 7\n\nWORKING_WELL:\n- fast endpoints\n\nFRICTION_POINTS:\n- slow synthesis\n\nIMPROVEMENTS:\n- add cache\n\nDATA_GAPS:\n- none\n\nPRIORITY_FIX: speed up synthesis"
        result = _parse_ux_analysis(response)
        assert result["score"] == 7
        assert result["priority_fix"] == "speed up synthesis"
        assert len(result["WORKING_WELL"]) == 1
        assert len(result["FRICTION_POINTS"]) == 1
        assert len(result["IMPROVEMENTS"]) == 1

    def test_missing_score(self):
        result = _parse_ux_analysis("Some random text without structure")
        assert "score" not in result
        assert result["raw"] == "Some random text without structure"


class TestEndpointRegistry:
    def test_registry_not_empty(self):
        assert len(_ENDPOINT_REGISTRY) > 10

    def test_all_have_required_fields(self):
        for ep in _ENDPOINT_REGISTRY:
            assert "method" in ep
            assert "path" in ep
            assert "desc" in ep
            assert ep["path"].startswith("/api/")


class TestUserJourneys:
    def test_journeys_not_empty(self):
        assert len(_USER_JOURNEYS) >= 3

    def test_journey_steps_are_valid_paths(self):
        valid_paths = {ep["path"] for ep in _ENDPOINT_REGISTRY}
        for journey in _USER_JOURNEYS:
            for step in journey["steps"]:
                assert step in valid_paths, f"Journey '{journey['name']}' references unknown path: {step}"


class TestTestJourney:
    def test_all_pass(self):
        endpoint_results = {
            "/a": {"ok": True, "latency_ms": 100},
            "/b": {"ok": True, "latency_ms": 200},
        }
        journey = {"name": "test", "desc": "test", "steps": ["/a", "/b"]}
        result = _test_journey(journey, None, endpoint_results)
        assert result["grade"] == "PASS"
        assert result["completion_rate"] == 1.0

    def test_partial_fail(self):
        endpoint_results = {
            "/a": {"ok": True, "latency_ms": 100},
            "/b": {"ok": True, "latency_ms": 200},
            "/c": {"ok": False, "latency_ms": 500, "error": "500"},
        }
        journey = {"name": "test", "desc": "test", "steps": ["/a", "/b", "/c"]}
        result = _test_journey(journey, None, endpoint_results)
        assert result["grade"] == "DEGRADED"  # 2/3 = 0.67 > 0.5
        assert len(result["blockers"]) == 1


def test_skipped_and_missing_steps_are_incomplete():
    journey = {"name": "test", "desc": "test", "steps": ["/ok", "/auth", "/missing"]}
    result = _test_journey(journey, None, {
        "/ok": {"ok": True, "latency_ms": 0},
        "/auth": {"ok": False, "data_quality": "SKIPPED", "error": "NO_AUTH"},
    })
    assert result["grade"] == "INCOMPLETE"
    assert result["completion_rate"] is None
    assert result["total_latency_ms"] is None
    assert result["steps_failed"] == 0
    assert result["steps_skipped"] == 1
    assert result["steps_untested"] == 1
    assert any("NO_AUTH" in b for b in result["blockers"])


def test_no_auth_audit_counts_and_persistence(monkeypatch):
    import json
    from unittest.mock import MagicMock
    from scripts import ux_auditor as audit

    monkeypatch.setattr(audit, "_get_auth_token", lambda _: None)
    tested = []
    def test_endpoint(ep, *_):
        tested.append(ep["path"])
        return {"path": ep["path"], "ok": True, "latency_ms": 0}
    monkeypatch.setattr(audit, "_test_endpoint", test_endpoint)
    monkeypatch.setattr(audit, "_run_llm_analysis", lambda *_: None)
    engine = MagicMock()
    report = audit.run_ux_audit(engine)
    summary = report["summary"]
    assert tested == [ep["path"] for ep in audit._ENDPOINT_REGISTRY if not ep.get("auth")]
    assert summary["endpoints_failed"] == 0
    assert summary["endpoints_tested"] == len(tested)
    assert summary["endpoints_skipped"] == len(audit._ENDPOINT_REGISTRY) - len(tested)
    assert summary["journeys_incomplete"] == len(audit._USER_JOURNEYS)
    assert summary["avg_latency_ms"] == 0
    assert all(e["ok"] is None for e in report["endpoints"] if e.get("error") == "NO_AUTH")
    insert = engine.begin.return_value.__enter__.return_value.execute.call_args.args[0]
    params = insert.compile().params
    assert json.loads(params["full_report"])["summary"] == summary


def test_measured_failures_stay_failures():
    from scripts.ux_auditor import _summarize_checks
    summary = _summarize_checks([
        {"ok": True, "latency_ms": 0},
        {"ok": False, "error": "TIMEOUT", "latency_ms": 15000},
        {"ok": False, "status_code": 401, "latency_ms": 10},
        {"ok": False, "data_quality": "SKIPPED", "error": "NO_AUTH"},
        {"ok": None},
    ], [])
    assert summary["endpoints_tested"] == 3
    assert summary["endpoints_failed"] == 2
    assert summary["endpoints_skipped"] == 1
    assert summary["endpoints_untested"] == 1

def test_missing_endpoint():
    endpoint_results = {"/a": {"ok": True, "latency_ms": 100}}
    journey = {"name": "test", "desc": "test", "steps": ["/a", "/missing"]}
    result = _test_journey(journey, None, endpoint_results)
    assert result["steps_ok"] == 1
    assert len(result["blockers"]) == 1


@pytest.mark.parametrize("status", [200, 401, 500, "timeout", "connection"])
def test_authenticated_audit_preserves_measured_results(monkeypatch, status):
    from unittest.mock import Mock
    from scripts import ux_auditor as audit

    monkeypatch.setattr(audit, "_get_auth_token", lambda _: "synthetic-token")
    request = Mock()
    if status == "timeout":
        request.side_effect = audit.requests.Timeout()
    elif status == "connection":
        request.side_effect = audit.requests.ConnectionError()
    else:
        request.return_value.status_code = status
        request.return_value.json.return_value = {"synthetic": True}
    monkeypatch.setattr(audit.requests, "request", request)
    monkeypatch.setattr(audit, "_run_llm_analysis", lambda *_: None)
    report = audit.run_ux_audit(verbose=True)
    total = len(audit._ENDPOINT_REGISTRY)
    assert request.call_count == total
    assert report["summary"]["endpoints_tested"] == total
    assert report["summary"]["endpoints_skipped"] == 0
    assert report["summary"]["endpoints_untested"] == 0
    assert report["summary"]["endpoints_failed"] == (0 if status == 200 else total)
    assert report["summary"]["journeys_incomplete"] == 0
    assert report["summary"]["journeys_pass"] == (len(audit._USER_JOURNEYS) if status == 200 else 0)


def test_partial_coverage_does_not_hide_real_journey_failure():
    journey = {"name": "test", "desc": "test", "steps": ["/failed", "/skipped"]}
    result = _test_journey(journey, None, {
        "/failed": {"ok": False, "status_code": 500},
        "/skipped": {"ok": None, "data_quality": "SKIPPED", "error": "NO_AUTH"},
    })
    assert result["grade"] == "FAIL"
    assert result["steps_failed"] == 1
    assert result["steps_skipped"] == 1
    assert result["completion_rate"] is None


def test_unmeasured_latency_is_unknown():
    from scripts.ux_auditor import _summarize_checks
    summary = _summarize_checks([{"ok": None, "data_quality": "SKIPPED"}], [])
    assert summary["avg_latency_ms"] is None
    assert summary["endpoints_failed"] == 0


def test_llm_receives_coverage_even_with_missing_http_measurements(monkeypatch):
    import sys
    import types
    from unittest.mock import Mock
    from scripts import ux_auditor as audit

    client = Mock(is_available=True)
    client.chat.return_value = "SCORE: 3\nPRIORITY_FIX: restore audit coverage"
    router = types.ModuleType("llm.router")
    router.get_llm = lambda **_: client
    monkeypatch.setitem(sys.modules, "llm.router", router)
    journey = audit._test_journey({"name": "incomplete", "desc": "test", "steps": ["/missing"]}, None, {})
    result = audit._run_llm_analysis([
        {"desc": "test", "path": "/timeout", "ok": False, "status_code": None, "latency_ms": None},
        {"desc": "skipped", "path": "/auth", "ok": None, "data_quality": "SKIPPED", "error": "NO_AUTH"},
    ], [journey])
    assert result["score"] == 3
    prompt = client.chat.call_args.kwargs["messages"][1]["content"]
    assert "NO_AUTH" in prompt and "SKIPPED and untested" in prompt
    assert "total latency unknown" in prompt
