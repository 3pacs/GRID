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

Follow-up (same day): GET /api/v1/alerts turned out to be unsafe too — its
handler (api/routers/price_alerts.py:186-201) calls ensure_alerts_table()
at line 194, which runs `CREATE TABLE IF NOT EXISTS sd_price_alerts`
(price_alerts.py:55-57) on *every* call, DDL executed by a GET. The
before/after alert-count invariant and step_widget_data's old "alerts"
substep both called it. Fixed by: (a) removing every call to
/api/v1/alerts from the non-mutating plan; (b) replacing the invariant
with a direct, read-only `SELECT count(*) FROM sd_price_alerts WHERE
active` (step_alerts_db_count — same connection style as step_freshness,
never HTTP); (c) adding NON_MUTATING_GET_BLOCKLIST so the request guard
refuses /api/v1/alerts outright, for any verb, even if some future step
tries to call it again.

This file proves, with a fake HTTP client (no network, no real server) and,
where noted, a fake DB engine (no real database):
  (i)   the default (--mode=non-mutating) plan issues no request that is not
        GET or on the proven-safe allow-list, and specifically no request
        whose path starts with /api/v1/alerts;
  (ii)  a fake compose response carrying alert_created: true fails the run
        with a clear reason; a direct attempt to call /api/v1/alerts
        through the client in non-mutating mode raises MutationBlocked
        before any HTTP call is made;
  (iii) an active-alerts count change across the run (read via the direct
        SQL path, never GET /api/v1/alerts) fails the run; the SQL path
        itself is proven, with a fake DB engine, to execute only a SELECT;
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
    NON_MUTATING_GET_BLOCKLIST,
    NON_MUTATING_GET_BLOCKLIST_REASONS,
    NON_MUTATING_POST_ALLOWLIST,
    StepResult,
    check_alerts_invariant,
    compose_mutation_markers,
    evaluate_mutation_guard,
    step_alerts_db_count,
    validate_mode,
)

SMOKE_SOURCE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "smoke_dad_path.py"

# step_alerts_db_count (like the pre-existing step_freshness/
# mint_contributor_token it copies its connection style from) deletes
# "config"/"db"/"api.*" from sys.modules UNCONDITIONALLY, before it even
# checks whether release_dir exists (see scripts/smoke_dad_path.py:728-730)
# -- a real, if latent, production behavior that every test in this file
# calling the real step_alerts_db_count/smoke.run() can trigger. Backend
# Tests runs the *whole* suite in one pytest process, so leaking that
# deletion breaks unrelated files (tests/contracts/test_api_contracts.py
# started failing DB auth once a stale-looking sys.modules["db"]/["config"]
# forced a fresh, differently-configured reimport mid-session). Autoused so
# every test in this file is protected, not just the ones that call the
# real function directly.
@pytest.fixture(autouse=True)
def _protect_shared_module_cache():
    tracked = lambda: {  # noqa: E731
        k: v for k, v in sys.modules.items() if k in ("config", "db") or k.startswith("api.")
    }
    snapshot = tracked()
    yield
    for k in tracked().keys() - snapshot.keys():
        del sys.modules[k]
    for k, v in snapshot.items():
        sys.modules[k] = v


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
        resp, _ms = client.request("GET", "/api/v1/system/health", timeout_s=1)
        assert resp.status_code == 200
        assert calls == [("GET", "http://x/api/v1/system/health")]

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

    def test_alerts_get_is_blocklisted_with_citation(self):
        """api/routers/price_alerts.py:186-201 list_alerts() calls
        ensure_alerts_table() (line 194), which runs `CREATE TABLE IF NOT
        EXISTS sd_price_alerts` (price_alerts.py:55-57) on every call — DDL
        executed by a GET. NON_MUTATING_GET_BLOCKLIST must refuse it
        outright, independent of the POST allow-list logic."""
        assert "/api/v1/alerts" in NON_MUTATING_GET_BLOCKLIST
        assert "price_alerts.py:55-57" in NON_MUTATING_GET_BLOCKLIST_REASONS["/api/v1/alerts"]

    def test_get_to_alerts_is_blocked_before_any_http_call(self, monkeypatch):
        """(ii) A direct attempt via the client in non-mutating mode must
        raise MutationBlocked BEFORE any HTTP call — not translate the
        blocked request into a graceful "broken" result after the fact."""
        def fail_if_called(*args, **kwargs):
            raise AssertionError("GET /api/v1/alerts must never reach requests.request in non-mutating mode")

        import requests

        monkeypatch.setattr(requests, "request", fail_if_called)
        client = Client("http://x", token="t", mode=MODE_NON_MUTATING)

        with pytest.raises(MutationBlocked) as exc_info:
            client.request("GET", "/api/v1/alerts", timeout_s=1)

        assert "price_alerts.py:186-201" in str(exc_info.value) or "price_alerts.py:55-57" in str(exc_info.value)

    def test_delete_to_alerts_is_also_blocked_by_the_blocklist(self):
        """The blocklist check runs before the method check, so it covers
        every verb on this path, not just GET."""
        client = Client("http://x", token="t", mode=MODE_NON_MUTATING)
        with pytest.raises(MutationBlocked):
            client.request("DELETE", "/api/v1/alerts", timeout_s=1)

    def test_alerts_get_passes_through_in_mutating_mode(self, monkeypatch):
        calls = []

        def fake_request(method, url, **kwargs):
            calls.append((method, url))
            return SimpleNamespace(status_code=200, text="", json=lambda: {"alerts": []})

        import requests

        monkeypatch.setattr(requests, "request", fake_request)
        client = Client("http://x", token="t", mode=MODE_MUTATING)
        resp, _ms = client.request("GET", "/api/v1/alerts", timeout_s=1)
        assert resp.status_code == 200
        assert calls == [("GET", "http://x/api/v1/alerts")]


class TestDefaultPlanEndToEnd:
    """Drive run() top to bottom with a fake `requests.request` — no real
    network, no real DB. Proves the default plan (--mode=non-mutating,
    which is what dad-smoke.yml/deploy.yml always pass) never sends a
    non-GET request."""

    def _install_fake_requests(self, monkeypatch):
        calls: list[tuple[str, str, dict | None]] = []

        def fake_request(method, url, *, headers=None, json=None, timeout=None, stream=False):
            calls.append((method, url, json))
            # No /api/v1/alerts branch here on purpose: it must never be
            # requested in non-mutating mode (NON_MUTATING_GET_BLOCKLIST) —
            # if a regression ever reintroduces that call, Client.request()
            # raises MutationBlocked before this fake is even reached, which
            # fails these tests loudly rather than quietly answering it.
            if url.endswith("/api/v1/system/health"):
                body = {"status": "ok", "checks": {}}
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
        # step_alerts_db_count is DB-only (never touches the HTTP client at
        # all — see TestStepAlertsDbCount below); mint_contributor_token is
        # faked here so composer/widget_data get a token and actually
        # attempt their (GET-only) requests, the strongest form of this
        # proof.
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))

        exit_code, report, result = smoke.run(self._args(tmp_path))

        non_get = [(m, u) for (m, u, _b) in calls if m != "GET"]
        assert non_get == [], f"non-mutating default plan sent a non-GET request: {non_get}"
        # And specifically: compose/ask-stream were never reached at all.
        assert not any("/chat/compose" in u for (_m, u, _b) in calls)
        assert not any("/chat/ask/stream" in u for (_m, u, _b) in calls)
        statuses = {s["name"]: s["status"] for s in result["steps"]}
        # Intentionally not run because of --mode=non-mutating — this is a
        # "skipped" grading, not a "blocked" one: it must never contribute
        # to exit_code (see TestFullyOkNonMutatingRun below for the isolated
        # proof), unlike a genuine environment blockage (no token, missing
        # release dir, ...) which this test's fake nonexistent release_dir
        # does still trip on other steps (alerts_count, freshness, ...).
        assert statuses["composer"] == "skipped"
        assert "non-mutating" in next(
            s["note"] for s in result["steps"] if s["name"] == "composer"
        )

    def test_no_request_path_starts_with_api_v1_alerts(self, monkeypatch, tmp_path):
        """(i), specifically: the default plan issues NO request — GET or
        otherwise — whose path starts with /api/v1/alerts. This is stronger
        than "no non-GET request": GET /api/v1/alerts is itself forbidden
        (its handler runs DDL), and the before/after invariant now reads
        the count directly from the DB instead."""
        calls = self._install_fake_requests(monkeypatch)
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))

        smoke.run(self._args(tmp_path))

        alerts_calls = [(m, u) for (m, u, _b) in calls if u.split("http://x", 1)[-1].startswith("/api/v1/alerts")]
        assert alerts_calls == [], f"non-mutating default plan reached /api/v1/alerts: {alerts_calls}"

    def test_no_alert_prompt_anywhere_in_requests(self, monkeypatch, tmp_path):
        calls = self._install_fake_requests(monkeypatch)
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))

        smoke.run(self._args(tmp_path))

        for _method, _url, body in calls:
            text = str(body or "")
            assert "NVDA" not in text
            assert "drops 5 percent" not in text


class TestFullyOkNonMutatingRun:
    """Regression test for the 2026-09-18 production run (deploy run
    35410892584, artifact exit_code 2): every read step reported "ok" —
    health, static, auth, widget_data, freshness, logs, deploy_tree — and
    the only non-"ok" step was composer, intentionally not run under
    --mode=non-mutating (the only mode dad-smoke.yml/deploy.yml ever pass).
    Because step_composer graded that mode-skip "blocked" (the same status
    used for a genuine environment blockage), run() mapped it to exit_code
    2 and the deploy's smoke job failed on every deployment even though
    nothing was actually broken or blocked.

    step_composer is deliberately NOT mocked here — this proves the real
    mode-skip branch (scripts/smoke_dad_path.py's step_composer, `mode !=
    MODE_MUTATING`) is graded "skipped", not "blocked", and that a
    "skipped" step never raises the exit code the way a "blocked" one
    does."""

    def test_all_reads_ok_composer_skipped_by_mode_exits_zero(self, monkeypatch, tmp_path):
        monkeypatch.setattr(smoke, "step_health", lambda client, budget_ms: StepResult("health", "ok"))
        monkeypatch.setattr(
            smoke, "step_static", lambda client, budget_ms, release_dir: StepResult("static", "ok")
        )
        monkeypatch.setattr(smoke, "mint_contributor_token", lambda release_dir: ("fake-token", "minted"))
        monkeypatch.setattr(smoke, "step_widget_data", lambda client, budget_ms: StepResult("widget_data", "ok"))
        monkeypatch.setattr(
            smoke, "step_alerts_db_count",
            lambda release_dir, label: StepResult(f"alerts_count:{label}", "ok", None, "count=3", {"count": 3}),
        )
        monkeypatch.setattr(smoke, "step_freshness", lambda release_dir: StepResult("freshness", "ok"))
        monkeypatch.setattr(smoke, "step_logs", lambda: StepResult("logs", "ok"))
        monkeypatch.setattr(smoke, "step_deploy_tree", lambda release_dir: StepResult("deploy_tree", "ok"))
        # step_composer is left as the real function — mode defaults to
        # MODE_NON_MUTATING, exactly what dad-smoke.yml/deploy.yml pass.

        args = argparse.Namespace(
            base_url="http://x", release_dir=str(tmp_path), budget_ms=5000, strict=False,
            mode=MODE_NON_MUTATING, i_accept_production_writes=False,
        )
        exit_code, report, result = smoke.run(args)

        statuses = {s["name"]: s["status"] for s in result["steps"]}
        assert statuses["health"] == "ok"
        assert statuses["static"] == "ok"
        assert statuses["auth"] == "ok"
        assert statuses["widget_data"] == "ok"
        assert statuses["freshness"] == "ok"
        assert statuses["logs"] == "ok"
        assert statuses["deploy_tree"] == "ok"
        assert statuses["composer"] == "skipped"
        assert statuses["mutation_guard"] == "ok"
        assert exit_code == 0, (
            "a step intentionally skipped by --mode must never raise the "
            f"exit code (production evidence: run 35410892584); got {exit_code}, steps={statuses}"
        )
        assert "## Skipped by mode" in report
        assert "composer" in report
        # And specifically: composer must NOT show up under "## Blocked
        # items" (blocked is reserved for genuine environment blockages).
        blocked_section = report.split("## Blocked items", 1)[1] if "## Blocked items" in report else ""
        assert "composer" not in blocked_section


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
            smoke, "step_alerts_db_count",
            lambda release_dir, label: StepResult(f"alerts_count:{label}", "ok", None, "count=1", {"count": 1}),
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

        def fake_alerts_count(release_dir, label):
            n = next(counts)
            return StepResult(f"alerts_count:{label}", "ok", None, f"count={n}", {"count": n})

        monkeypatch.setattr(smoke, "step_alerts_db_count", fake_alerts_count)

        args = argparse.Namespace(
            base_url="http://x", release_dir=str(tmp_path), budget_ms=5000, strict=False,
            mode=MODE_NON_MUTATING, i_accept_production_writes=False,
        )
        exit_code, report, result = smoke.run(args)

        assert exit_code == 1
        statuses = {s["name"]: s["status"] for s in result["steps"]}
        assert statuses["mutation_guard"] == "broken"
        assert "2 -> 5" in report


class TestStepAlertsDbCount:
    """(iii): step_alerts_db_count is the detector this PR kept (over
    omitting it entirely) — reusing step_freshness's direct-DB connection
    style to run a pure `SELECT count(*) FROM sd_price_alerts WHERE
    active`. Prove, with a fake `db.get_engine()` (no real database, no
    HTTP), that it executes exactly that SELECT and nothing else — no
    CREATE/INSERT/UPDATE/DELETE/DROP/ALTER."""

    _FORBIDDEN_SQL_KEYWORDS = ("CREATE", "INSERT", "UPDATE", "DELETE", "DROP", "ALTER")

    def _write_fake_db_module(self, release_dir: Path, *, row=(3,), raise_on_connect: bool = False):
        """Write a `db.py` into `release_dir` that step_alerts_db_count's
        `from db import get_engine` (after sys.path.insert(0, release_dir))
        will import instead of the real repo db.py. Its fake connection
        appends every statement passed to `.execute()` (as its compiled SQL
        text) to `executed.log` next to it — a FILE, not a module-level
        list, because step_alerts_db_count deletes "db" from sys.modules in
        its own `finally` block, so a module-level list would vanish with
        it before this test could read it back."""
        release_dir.mkdir(parents=True, exist_ok=True)
        raise_flag = "True" if raise_on_connect else "False"
        (release_dir / "db.py").write_text(
            "from pathlib import Path\n"
            "\n"
            "_LOG_PATH = Path(__file__).with_name('executed.log')\n"
            "\n"
            "class _FakeResult:\n"
            "    def __init__(self, row):\n"
            "        self._row = row\n"
            "    def fetchone(self):\n"
            "        return self._row\n"
            "\n"
            "class _FakeConn:\n"
            "    def __enter__(self):\n"
            f"        if {raise_flag}:\n"
            "            raise RuntimeError('relation \"sd_price_alerts\" does not exist')\n"
            "        return self\n"
            "    def __exit__(self, *a):\n"
            "        return False\n"
            "    def execute(self, stmt, *a, **kw):\n"
            "        with open(_LOG_PATH, 'a', encoding='utf-8') as f:\n"
            "            f.write(str(stmt) + '\\n')\n"
            f"        return _FakeResult({row!r})\n"
            "\n"
            "class _FakeEngine:\n"
            "    def connect(self):\n"
            "        return _FakeConn()\n"
            "\n"
            "def get_engine():\n"
            "    return _FakeEngine()\n",
            encoding="utf-8",
        )

    def _read_executed(self, release_dir: Path) -> list[str]:
        log_path = release_dir / "executed.log"
        if not log_path.exists():
            return []
        return [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def test_executes_exactly_one_pure_select(self, tmp_path):
        release_dir = tmp_path / "release"
        self._write_fake_db_module(release_dir, row=(3,))

        result = step_alerts_db_count(str(release_dir), "before")

        executed = self._read_executed(release_dir)
        assert executed == ["SELECT count(*) FROM sd_price_alerts WHERE active"]
        for stmt in executed:
            for kw in self._FORBIDDEN_SQL_KEYWORDS:
                assert kw not in stmt.upper(), f"{kw} found in {stmt!r} — detector must be SELECT-only"
        assert result.status == "ok"
        assert result.data["count"] == 3
        assert result.name == "alerts_count:before"

    def test_blocked_when_release_dir_missing(self, tmp_path):
        result = step_alerts_db_count(str(tmp_path / "no-such-release"), "before")
        assert result.status == "blocked"

    def test_blocked_not_broken_when_query_fails(self, tmp_path):
        release_dir = tmp_path / "release"
        self._write_fake_db_module(release_dir, raise_on_connect=True)

        result = step_alerts_db_count(str(release_dir), "after")

        assert result.status == "blocked"
        assert "alerts count query failed" in result.note

    def test_never_calls_the_http_client(self, tmp_path, monkeypatch):
        """step_alerts_db_count takes release_dir, not a Client — there is
        no HTTP client for it to call. Guard against a future signature
        change that reintroduces one by asserting requests.request is never
        touched."""
        def fail_if_called(*args, **kwargs):
            raise AssertionError("step_alerts_db_count must never touch the network")

        import requests

        monkeypatch.setattr(requests, "request", fail_if_called)
        release_dir = tmp_path / "release"
        self._write_fake_db_module(release_dir, row=(0,))

        result = step_alerts_db_count(str(release_dir), "before")
        assert result.status == "ok"


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
        # step_alerts_db_count doesn't gate on client.token the way
        # step_composer/step_widget_data do (it takes release_dir, not a
        # Client) -- with a real, existing tmp_path and no mock it would
        # actually import the real repo db.py and attempt a live DB
        # connection. Mock it explicitly; this test is about the mode gate,
        # not the alert-count invariant.
        monkeypatch.setattr(
            smoke, "step_alerts_db_count",
            lambda release_dir, label: StepResult(f"alerts_count:{label}", "blocked", None, "n/a", {"count": None}),
        )

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
