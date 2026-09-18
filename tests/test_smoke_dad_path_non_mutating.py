"""Regression tests: routine deploy smoke (scripts/smoke_dad_path.py) must be
non-mutating.

Context (2026-09-18): the post-deploy "Dad Path Smoke (report-only)" job
(.github/workflows/deploy.yml -> .github/workflows/dad-smoke.yml ->
scripts/smoke_dad_path.py) minted a contributor token and POSTed
"tell me when NVDA drops 5 percent" to /api/v1/chat/compose. The composer's
fast deterministic alert path (api/routers/chat.py:2144-2178) classified it
as `branch=alert_created` and persisted a real price alert via
api/routers/price_alerts.create_alert_record (INSERT INTO sd_price_alerts,
price_alerts.py:148-154). It also POSTed /api/v1/chat/ask/stream, a real LLM
call. Routine, unattended deploy smoke must never be able to do that again.

This file proves, with a fake HTTP client (no network, no real server):
  (i)   the default (--mode=non-mutating) plan issues no request that is not
        GET or on the proven-safe allow-list;
  (ii)  a fake compose response carrying alert_created: true fails the run
        with a clear reason;
  (iii) a GET /api/v1/alerts count change across the run fails the run;
  (iv)  --mode=mutating without both required gates refuses to start (no
        network call is attempted at all);
  (v)   no notification/email/SMTP path is reachable from the default plan.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.smoke_dad_path as smoke
from scripts.smoke_dad_path import (
    MODE_MUTATING,
    MODE_NON_MUTATING,
    Client,
    MutationBlocked,
    NON_MUTATING_EXCLUDED_ENDPOINTS,
    NON_MUTATING_POST_ALLOWLIST,
    StepResult,
    check_alerts_invariant,
    compose_mutation_markers,
    evaluate_mutation_guard,
    validate_mode,
)

SMOKE_SOURCE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "smoke_dad_path.py"


# ── (i) the non-mutating guard itself ────────────────────────────────────


class TestNonMutatingAllowlist:
    def test_allowlist_is_empty(self):
        """Nothing has been proven safe yet — every POST examined for this
        fix turned out to write. Widening this set requires reading the new
        handler end to end and citing it, the same way chat.py's endpoints
        are cited below."""
        assert NON_MUTATING_POST_ALLOWLIST == frozenset()

    def test_compose_and_ask_stream_are_excluded_with_citations(self):
        assert "/api/v1/chat/compose" in NON_MUTATING_EXCLUDED_ENDPOINTS
        assert "/api/v1/chat/ask/stream" in NON_MUTATING_EXCLUDED_ENDPOINTS
        for reason in NON_MUTATING_EXCLUDED_ENDPOINTS.values():
            assert "chat.py:" in reason  # every exclusion cites a file:line


class TestClientGuard:
    def test_get_is_always_allowed(self, monkeypatch):
        calls = []

        def fake_request(method, url, **kwargs):
            calls.append((method, url))
            return SimpleNamespace(status_code=200, text="", json=lambda: {})

        import requests

        monkeypatch.setattr(requests, "request", fake_request)
        client = Client("http://x", token="t", mode=MODE_NON_MUTATING)
        resp, _ms = client.request("GET", "/api/v1/alerts", timeout_s=1)
        assert resp.status_code == 200
        assert calls == [("GET", "http://x/api/v1/alerts")]

    def test_post_to_compose_is_blocked(self):
        client = Client("http://x", token="t", mode=MODE_NON_MUTATING)
        with pytest.raises(MutationBlocked) as exc_info:
            client.request(
                "POST", "/api/v1/chat/compose", timeout_s=1,
                json_body={"question": "tell me when NVDA drops 5 percent", "history": []},
            )
        assert "chat.py:2144-2178" in str(exc_info.value)

    def test_post_to_ask_stream_is_blocked(self):
        client = Client("http://x", token="t", mode=MODE_NON_MUTATING)
        with pytest.raises(MutationBlocked):
            client.request(
                "POST", "/api/v1/chat/ask/stream", timeout_s=1,
                json_body={"question": "hi", "history": []},
            )

    def test_mutating_mode_does_not_guard(self, monkeypatch):
        calls = []

        def fake_request(method, url, **kwargs):
            calls.append((method, url))
            return SimpleNamespace(status_code=200, text="", json=lambda: {"alert_created": True})

        import requests

        monkeypatch.setattr(requests, "request", fake_request)
        client = Client("http://x", token="t", mode=MODE_MUTATING)
        resp, _ms = client.request("POST", "/api/v1/chat/compose", timeout_s=1, json_body={})
        assert resp.status_code == 200
        assert calls == [("POST", "http://x/api/v1/chat/compose")]


class TestDefaultPlanEndToEnd:
    """Drive run() top to bottom with a fake `requests.request` — no real
    network, no real DB. Proves the default plan (--mode=non-mutating,
    which is what dad-smoke.yml/deploy.yml always pass) never sends a
    non-GET request."""

    def _install_fake_requests(self, monkeypatch):
        calls: list[tuple[str, str, dict | None]] = []

        def fake_request(method, url, *, headers=None, json=None, timeout=None, stream=False):
            calls.append((method, url, json))
            if url.endswith("/api/v1/system/health"):
                body = {"status": "ok", "checks": {}}
            elif url.endswith("/api/v1/alerts"):
                body = {"alerts": []}
            elif url.endswith("/"):
                return SimpleNamespace(status_code=200, text="<html><title>x</title></html>")
            else:
                body = {}
            return SimpleNamespace(status_code=200, text="", json=lambda b=body: b)

        import requests

        monkeypatch.setattr(requests, "request", fake_request)
        return calls

    def _args(self, tmp_path, **overrides):
        base = dict(
            base_url="http://x", release_dir=str(tmp_path / "no-such-release"),
            budget_ms=5000, strict=False, mode=MODE_NON_MUTATING,
            i_accept_production_writes=False,
        )
        base.update(overrides)
        return argparse.Namespace(**base)

    def test_no_non_get_request_is_ever_sent(self, monkeypatch, tmp_path):
        calls = self._install_fake_requests(monkeypatch)
        # No real release dir -> mint_contributor_token fails -> client.token
        # stays None -> composer/widget_data/alerts_count all short-circuit
        # to "blocked" without a request. That is itself part of the
        # invariant this test protects: prove it holds with a token too.
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))

        exit_code, report, result = smoke.run(self._args(tmp_path))

        non_get = [(m, u) for (m, u, _b) in calls if m != "GET"]
        assert non_get == [], f"non-mutating default plan sent a non-GET request: {non_get}"
        # And specifically: compose/ask-stream were never reached at all.
        assert not any("/chat/compose" in u for (_m, u, _b) in calls)
        assert not any("/chat/ask/stream" in u for (_m, u, _b) in calls)
        statuses = {s["name"]: s["status"] for s in result["steps"]}
        assert statuses["composer"] == "blocked"
        assert "non-mutating" in next(
            s["note"] for s in result["steps"] if s["name"] == "composer"
        )

    def test_no_alert_prompt_anywhere_in_requests(self, monkeypatch, tmp_path):
        calls = self._install_fake_requests(monkeypatch)
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))

        smoke.run(self._args(tmp_path))

        for _method, _url, body in calls:
            text = str(body or "")
            assert "NVDA" not in text
            assert "drops 5 percent" not in text


# ── (ii) alert_created (or any mutation marker) fails the run ───────────


class TestComposeMutationMarkers:
    def test_alert_created_is_a_marker(self):
        assert compose_mutation_markers({"alert_created": True, "spoken_reply": None}) == ["alert_created"]

    def test_cannot_fulfill_is_a_marker(self):
        assert compose_mutation_markers({"cannot_fulfill": True}) == ["cannot_fulfill"]

    def test_normal_reply_has_no_marker(self):
        assert compose_mutation_markers({"spoken_reply": "hi", "widgets": []}) == []

    def test_non_dict_has_no_marker(self):
        assert compose_mutation_markers(None) == []
        assert compose_mutation_markers([1, 2, 3]) == []


class TestEvaluateMutationGuard:
    def test_fake_alert_created_response_breaks_the_run(self):
        result = evaluate_mutation_guard(
            MODE_NON_MUTATING, before_count=3, after_count=3,
            compose_payloads=[{"alert_created": True, "alert": {"ticker": "NVDA"}}],
        )
        assert result.status == "broken"
        assert "alert_created" in result.note

    def test_clean_compose_response_does_not_break_the_run(self):
        result = evaluate_mutation_guard(
            MODE_NON_MUTATING, before_count=3, after_count=3,
            compose_payloads=[{"spoken_reply": "here", "widgets": []}],
        )
        assert result.status == "ok"

    def test_not_enforced_in_mutating_mode(self):
        result = evaluate_mutation_guard(
            MODE_MUTATING, before_count=3, after_count=9,
            compose_payloads=[{"alert_created": True}],
        )
        assert result.status == "ok"

    def test_end_to_end_run_fails_on_alert_created(self, monkeypatch, tmp_path):
        """A real (albeit gated-off) composer response carrying
        alert_created must fail run() with exit code 1, --strict or not."""
        monkeypatch.setattr(smoke, "step_health", lambda client, budget_ms: StepResult("health", "ok"))
        monkeypatch.setattr(
            smoke, "step_static", lambda client, budget_ms, release_dir: StepResult("static", "ok")
        )
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))
        monkeypatch.setattr(
            smoke, "step_alerts_count",
            lambda client, budget_ms, label: StepResult(f"alerts_count:{label}", "ok", None, "count=1", {"count": 1}),
        )

        def fake_composer(client, budget_ms, mode):
            # Simulate a regression: compose ran anyway (e.g. someone
            # widened NON_MUTATING_POST_ALLOWLIST) and the server did create
            # an alert.
            return StepResult(
                "composer", "ok", None, "compose:tell me when NVDA=ok",
                {"substeps": [{
                    "name": "compose:tell me when NVDA",
                    "status": "ok",
                    "data": {"branch": "alert_created", "alert_created": True, "cannot_fulfill": False},
                }]},
            )

        monkeypatch.setattr(smoke, "step_composer", fake_composer)
        monkeypatch.setattr(smoke, "step_widget_data", lambda client, budget_ms: StepResult("widget_data", "ok"))
        monkeypatch.setattr(smoke, "step_freshness", lambda release_dir: StepResult("freshness", "ok"))
        monkeypatch.setattr(smoke, "step_logs", lambda: StepResult("logs", "ok"))
        monkeypatch.setattr(smoke, "step_deploy_tree", lambda release_dir: StepResult("deploy_tree", "ok"))

        args = argparse.Namespace(
            base_url="http://x", release_dir=str(tmp_path), budget_ms=5000, strict=False,
            mode=MODE_NON_MUTATING, i_accept_production_writes=False,
        )
        exit_code, report, result = smoke.run(args)

        assert exit_code == 1  # non-zero even though --strict was not passed
        statuses = {s["name"]: s["status"] for s in result["steps"]}
        assert statuses["mutation_guard"] == "broken"
        assert "alert_created" in report


# ── (iii) alerts-count invariant ─────────────────────────────────────────


class TestCheckAlertsInvariant:
    def test_unchanged_count_is_ok(self):
        ok, reason = check_alerts_invariant(MODE_NON_MUTATING, 4, 4)
        assert ok is True

    def test_changed_count_fails(self):
        ok, reason = check_alerts_invariant(MODE_NON_MUTATING, 4, 5)
        assert ok is False
        assert "4 -> 5" in reason

    def test_missing_counts_are_not_a_violation(self):
        ok, _reason = check_alerts_invariant(MODE_NON_MUTATING, None, 5)
        assert ok is True

    def test_not_enforced_in_mutating_mode(self):
        ok, _reason = check_alerts_invariant(MODE_MUTATING, 4, 99)
        assert ok is True

    def test_end_to_end_run_fails_on_count_change(self, monkeypatch, tmp_path):
        monkeypatch.setattr(smoke, "step_health", lambda client, budget_ms: StepResult("health", "ok"))
        monkeypatch.setattr(
            smoke, "step_static", lambda client, budget_ms, release_dir: StepResult("static", "ok")
        )
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))
        monkeypatch.setattr(smoke, "step_composer", lambda client, budget_ms, mode: StepResult("composer", "blocked", None, "skipped"))
        monkeypatch.setattr(smoke, "step_widget_data", lambda client, budget_ms: StepResult("widget_data", "ok"))
        monkeypatch.setattr(smoke, "step_freshness", lambda release_dir: StepResult("freshness", "ok"))
        monkeypatch.setattr(smoke, "step_logs", lambda: StepResult("logs", "ok"))
        monkeypatch.setattr(smoke, "step_deploy_tree", lambda release_dir: StepResult("deploy_tree", "ok"))

        counts = iter([2, 5])  # before=2, after=5 -> something mutated

        def fake_alerts_count(client, budget_ms, label):
            n = next(counts)
            return StepResult(f"alerts_count:{label}", "ok", None, f"count={n}", {"count": n})

        monkeypatch.setattr(smoke, "step_alerts_count", fake_alerts_count)

        args = argparse.Namespace(
            base_url="http://x", release_dir=str(tmp_path), budget_ms=5000, strict=False,
            mode=MODE_NON_MUTATING, i_accept_production_writes=False,
        )
        exit_code, report, result = smoke.run(args)

        assert exit_code == 1
        statuses = {s["name"]: s["status"] for s in result["steps"]}
        assert statuses["mutation_guard"] == "broken"
        assert "2 -> 5" in report


# ── (iv) --mode=mutating requires both gates ─────────────────────────────


class TestValidateMode:
    def test_non_mutating_never_needs_a_gate(self):
        assert validate_mode(MODE_NON_MUTATING, env={}, accept_flag=False) is None

    def test_mutating_refused_with_neither_gate(self):
        msg = validate_mode(MODE_MUTATING, env={}, accept_flag=False)
        assert msg is not None
        assert "SMOKE_ALLOW_MUTATIONS" in msg
        assert "--i-accept-production-writes" in msg

    def test_mutating_refused_with_only_env(self):
        msg = validate_mode(MODE_MUTATING, env={"SMOKE_ALLOW_MUTATIONS": "1"}, accept_flag=False)
        assert msg is not None
        assert "--i-accept-production-writes" in msg

    def test_mutating_refused_with_only_flag(self):
        msg = validate_mode(MODE_MUTATING, env={}, accept_flag=True)
        assert msg is not None
        assert "SMOKE_ALLOW_MUTATIONS" in msg

    def test_mutating_allowed_with_both_gates(self):
        assert validate_mode(MODE_MUTATING, env={"SMOKE_ALLOW_MUTATIONS": "1"}, accept_flag=True) is None

    def test_wrong_env_value_still_refuses(self):
        msg = validate_mode(MODE_MUTATING, env={"SMOKE_ALLOW_MUTATIONS": "yes"}, accept_flag=True)
        assert msg is not None


class TestMutatingModeRefusesToStartEndToEnd:
    def test_no_network_call_is_attempted(self, monkeypatch, tmp_path):
        monkeypatch.delenv("SMOKE_ALLOW_MUTATIONS", raising=False)

        def fail_if_called(*args, **kwargs):
            raise AssertionError("mutating mode without both gates must never make a network call")

        import requests

        monkeypatch.setattr(requests, "request", fail_if_called)
        monkeypatch.setattr(
            smoke, "mint_contributor_token",
            lambda release_dir: (_ for _ in ()).throw(AssertionError("must not mint a token either")),
        )

        args = argparse.Namespace(
            base_url="http://x", release_dir=str(tmp_path), budget_ms=5000, strict=False,
            mode=MODE_MUTATING, i_accept_production_writes=False,
        )
        exit_code, report, result = smoke.run(args)

        assert exit_code == 2
        assert result["steps"] == []
        assert "refused to start" in report

    def test_with_both_gates_it_proceeds(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SMOKE_ALLOW_MUTATIONS", "1")
        monkeypatch.setattr(smoke, "step_health", lambda client, budget_ms: StepResult("health", "ok"))
        monkeypatch.setattr(
            smoke, "step_static", lambda client, budget_ms, release_dir: StepResult("static", "ok")
        )
        monkeypatch.setattr(
            smoke, "mint_contributor_token",
            lambda release_dir: (None, "release dir not found: n/a"),
        )
        monkeypatch.setattr(smoke, "step_freshness", lambda release_dir: StepResult("freshness", "ok"))
        monkeypatch.setattr(smoke, "step_logs", lambda: StepResult("logs", "ok"))
        monkeypatch.setattr(smoke, "step_deploy_tree", lambda release_dir: StepResult("deploy_tree", "ok"))

        args = argparse.Namespace(
            base_url="http://x", release_dir=str(tmp_path), budget_ms=5000, strict=False,
            mode=MODE_MUTATING, i_accept_production_writes=True,
        )
        exit_code, report, result = smoke.run(args)

        # No gate violation this time — it actually ran the plan (and then
        # got "blocked" for the ordinary reason of no token, same as any
        # other run against a release dir that doesn't exist).
        assert result["steps"] != []
        statuses = {s["name"]: s["status"] for s in result["steps"]}
        assert statuses["auth"] == "blocked"


# ── (v) no notification/email/SMTP path reachable from the default plan ──


class TestNoNotificationPathInSmokeScript:
    """scripts/smoke_dad_path.py must never import or call anything that
    could notify a human or persist a request — that machinery only exists
    on the server side of /chat/compose (api/routers/chat.py:2307-2342,
    scripts/notify.py's smtplib send), which this script does not reach in
    the default plan (see TestDefaultPlanEndToEnd above)."""

    def test_smoke_script_never_imports_or_calls_notify_machinery(self):
        """AST-level check (not a naive substring grep): the script's own
        module-level comments cite `smtplib`/`send_insight_email` etc. by
        name as *proof of why /chat/compose is excluded* — that prose must
        not trip this check. What must never appear is an actual import or
        call node referencing that machinery."""
        import ast

        tree = ast.parse(SMOKE_SOURCE_PATH.read_text(encoding="utf-8"))
        forbidden_names = {
            "smtplib",
            "send_insight_email",
            "_email_capability_gap",
            "_imessage_operator",
        }
        forbidden_modules = {"smtplib", "scripts.notify"}

        found: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in forbidden_modules:
                        found.append(f"import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                if mod in forbidden_modules or mod.endswith(".notify"):
                    found.append(f"from {mod} import ...")
                for alias in node.names:
                    if alias.name in forbidden_names:
                        found.append(f"from {mod} import {alias.name}")
            elif isinstance(node, ast.Name) and node.id in forbidden_names:
                found.append(f"name reference: {node.id}")
            elif isinstance(node, ast.Attribute) and node.attr in forbidden_names:
                found.append(f"attribute reference: .{node.attr}")

        assert found == [], (
            f"scripts/smoke_dad_path.py must not import or call {found} — "
            "no notification/email/SMTP path may be reachable from the "
            "default (non-mutating) plan"
        )

    def test_smoke_module_does_not_import_smtplib_at_load_time(self):
        assert "smtplib" not in sys.modules or not any(
            mod is sys.modules.get("smtplib") for mod in (smoke,)
        )
        assert not hasattr(smoke, "smtplib")
