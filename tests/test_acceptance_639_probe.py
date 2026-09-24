"""No-network fixture checks for the bounded #639 acceptance probe."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

import pytest


SPEC = importlib.util.spec_from_file_location(
    "acceptance_639_probe", Path(__file__).resolve().parents[1] / "scripts" / "acceptance_639_probe.py"
)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class FakeResponse:
    def __init__(self, body, status=200):
        self.body = json.dumps(body).encode()
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self, limit):
        assert limit == 256 * 1024
        return self.body


def test_allowlist_excludes_refresh_and_provider_fallbacks():
    paths = [item[1] for item in probe.PROBES]
    assert paths == [
        "/api/v1/system/health",
        "/api/v1/options/recommendations?ticker=AAPL",
        "/api/v1/options/recommendations?ticker=AAPL",
        "/api/v1/watchlist/AAPL/edge",
    ]
    assert not any("refresh" in path or "gold" in path or "quote" in path for path in paths)
    with pytest.raises(ValueError, match="outside"):
        probe.fetch_shape("http://127.0.0.1:8000", "gold", "/api/v1/dad/ticker/AAPL/gold?refresh_finviz=true", "secret")


@pytest.mark.parametrize("origin", ["http://grid.stepdad.finance", "https://other.example", "https://grid.stepdad.finance/x", "https://user:pass@grid.stepdad.finance", "http://127.0.0.1:5432"])
def test_unapproved_origins_fail_closed(origin):
    with pytest.raises(ValueError):
        probe.validate_origin(origin)


def test_redirect_is_not_followed_to_a_provider():
    assert probe.NoRedirect().redirect_request(None, None, 302, "Found", {}, "https://example.com") is None


def test_fixture_requests_are_get_only_and_output_omits_body_and_token():
    seen = []

    def fake_open(req, timeout):
        seen.append((req.full_url, req.get_method(), req.get_header("Authorization"), timeout))
        return FakeResponse({"status": "ok", "checks": {"database": True}, "token": "secret-value"})

    with patch.object(probe, "open_request", side_effect=fake_open):
        health = probe.fetch_shape("http://127.0.0.1:8000", "health", probe.PROBES[0][1], None)
        options = probe.fetch_shape("http://127.0.0.1:8000", "saved_options", probe.PROBES[2][1], "secret-value")
    assert [item[1] for item in seen] == ["GET", "GET"]
    assert seen[0][2] is None and seen[1][2] == "Bearer secret-value"
    assert health["database_check"] is True
    assert "secret-value" not in json.dumps([health, options])


def test_release_identity_mismatch_refuses_before_network(tmp_path):
    with patch.object(probe.subprocess, "check_output", side_effect=["a" * 40, "b" * 40]):
        with pytest.raises(ValueError, match="does not match"):
            probe.verify_release(tmp_path, "c" * 40, "b" * 40)


def test_wrong_api_process_cwd_refuses_before_network(tmp_path):
    with patch.object(probe.subprocess, "check_output", return_value="123"), \
            patch.object(probe.Path, "resolve", return_value=tmp_path / "other"):
        with pytest.raises(ValueError, match="cwd does not match"):
            probe.verify_api_process(str(tmp_path))


def test_missing_token_refuses_without_request(tmp_path, monkeypatch):
    monkeypatch.delenv("GRID_ACCEPTANCE_BEARER", raising=False)
    with patch.object(probe, "verify_release", return_value={"resolved_path": str(tmp_path), "head": "a" * 40, "tree": "b" * 40}), \
            patch.object(probe, "verify_api_process", return_value=123), patch.object(probe, "open_request") as opener:
        assert probe.main(["--origin", "http://127.0.0.1:8000", "--release-path", str(tmp_path),
                           "--expected-sha", "a" * 40, "--expected-tree", "b" * 40]) == 2
        opener.assert_not_called()


def test_full_fixture_run_requires_auth_boundary_and_db_health(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GRID_ACCEPTANCE_BEARER", "secret-value")
    statuses = iter([200, 401, 200, 200])
    seen = []

    def fake_open(req, timeout):
        seen.append((req.full_url, req.get_method(), req.get_header("Authorization")))
        status = next(statuses)
        return FakeResponse({"status": "ok", "checks": {"database": True}, "token": "secret-value"}, status)

    with patch.object(probe, "verify_release", return_value={"resolved_path": str(tmp_path), "head": "a" * 40, "tree": "b" * 40}), \
            patch.object(probe, "verify_api_process", return_value=123), \
            patch.object(probe, "open_request", side_effect=fake_open), patch.object(probe.time, "sleep"):
        result = probe.main(["--origin", "http://127.0.0.1:8000", "--release-path", str(tmp_path),
                             "--expected-sha", "a" * 40, "--expected-tree", "b" * 40])
    assert result == 0
    assert [item[1] for item in seen] == ["GET"] * 4
    assert seen[1][2] is None and all(item[2] == "Bearer secret-value" for item in seen[2:])
    assert "secret-value" not in capsys.readouterr().out
