"""Tests for the live HTTP fleet-health probe (scripts/live_health_probe.py).

The probe's only side effect is network I/O via ``_fetch``; every test patches
that boundary so nothing hits a real endpoint (per testing.md: never hit live
endpoints in tests).
"""

from __future__ import annotations

import json

import pytest

from scripts import live_health_probe as lhp


def _patch_fetch(monkeypatch, responses: dict[str, tuple[int, object]]) -> None:
    """Patch ``_fetch`` to return canned (status, body) keyed by path suffix."""

    def fake_fetch(base_url, path, token, timeout):
        for suffix, resp in responses.items():
            if path.endswith(suffix):
                return resp
        raise AssertionError(f"unexpected path probed: {path}")

    monkeypatch.setattr(lhp, "_fetch", fake_fetch)


def test_healthy_returns_exit_ok(monkeypatch):
    _patch_fetch(monkeypatch, {
        "/health": (200, {"status": "ok", "checks": {"database": True}, "degraded_reasons": []}),
    })
    code, result = lhp.probe("https://example.test", token=None, timeout=1.0)
    assert code == lhp.EXIT_OK
    assert result["overall"] == "ok"


def test_degraded_returns_exit_degraded(monkeypatch):
    _patch_fetch(monkeypatch, {
        "/health": (200, {
            "status": "degraded",
            "checks": {"database": True, "recent_data": False},
            "degraded_reasons": ["no data pulled in 7 days"],
        }),
    })
    code, result = lhp.probe("https://example.test", token=None, timeout=1.0)
    assert code == lhp.EXIT_DEGRADED
    assert result["overall"] == "degraded"


def test_network_failure_returns_unreachable(monkeypatch):
    _patch_fetch(monkeypatch, {"/health": (0, "Connection refused")})
    code, result = lhp.probe("https://example.test", token=None, timeout=1.0)
    assert code == lhp.EXIT_UNREACHABLE
    assert result["overall"] == "unreachable"
    assert "Connection refused" in result["reason"]


def test_non_200_health_is_unreachable(monkeypatch):
    _patch_fetch(monkeypatch, {"/health": (503, {"detail": "starting up"})})
    code, result = lhp.probe("https://example.test", token=None, timeout=1.0)
    assert code == lhp.EXIT_UNREACHABLE


def test_token_triggers_authenticated_endpoints(monkeypatch):
    seen: list[str] = []

    def fake_fetch(base_url, path, token, timeout):
        seen.append(path)
        if path.endswith("/health"):
            return 200, {"status": "ok", "checks": {}, "degraded_reasons": []}
        return 200, {"families": []}

    monkeypatch.setattr(lhp, "_fetch", fake_fetch)
    code, result = lhp.probe("https://example.test", token="jwt", timeout=1.0)
    assert code == lhp.EXIT_OK
    assert any(p.endswith("/freshness") for p in seen)
    assert any(p.endswith("/services") for p in seen)
    assert any(p.endswith("/hermes-status") for p in seen)


def test_no_token_skips_authenticated_endpoints(monkeypatch):
    seen: list[str] = []

    def fake_fetch(base_url, path, token, timeout):
        seen.append(path)
        return 200, {"status": "ok", "checks": {}, "degraded_reasons": []}

    monkeypatch.setattr(lhp, "_fetch", fake_fetch)
    lhp.probe("https://example.test", token=None, timeout=1.0)
    assert seen == [f"{lhp.API_PREFIX}/health"]


def test_main_json_output(monkeypatch, capsys):
    _patch_fetch(monkeypatch, {
        "/health": (200, {"status": "ok", "checks": {"database": True}, "degraded_reasons": []}),
    })
    rc = lhp.main(["--base-url", "https://example.test", "--json"])
    assert rc == lhp.EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["overall"] == "ok"


def test_main_dashboard_renders_degraded_reasons(monkeypatch, capsys):
    _patch_fetch(monkeypatch, {
        "/health": (200, {
            "status": "degraded",
            "checks": {"database": True},
            "degraded_reasons": ["connection pool exhausted"],
        }),
    })
    rc = lhp.main(["--base-url", "https://example.test"])
    assert rc == lhp.EXIT_DEGRADED
    out = capsys.readouterr().out
    assert "DEGRADED" in out
    assert "connection pool exhausted" in out


@pytest.mark.parametrize("value,expected", [(True, "✓"), (False, "✗"), (5, "5"), ("x", "x")])
def test_icon(value, expected):
    assert lhp._icon(value) == expected
