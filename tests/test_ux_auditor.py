"""Tests for scripts.ux_auditor — UX audit engine."""

import pytest

from scripts.ux_auditor import (
    _test_endpoint,
    _test_journey,
    _truncate_sample,
    _parse_ux_analysis,
    _ENDPOINT_REGISTRY,
    _USER_JOURNEYS,
)


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

    def test_missing_endpoint(self):
        endpoint_results = {"/a": {"ok": True, "latency_ms": 100}}
        journey = {"name": "test", "desc": "test", "steps": ["/a", "/missing"]}
        result = _test_journey(journey, None, endpoint_results)
        assert result["steps_ok"] == 1
        assert len(result["blockers"]) == 1
