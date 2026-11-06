import pytest

from scripts import worker


class FakeFuture:
    def __init__(self, done=False, result=None):
        self._done = done
        self._result = result
        self.result_called = False

    def done(self):
        return self._done

    def result(self):
        self.result_called = True
        return self._result


def test_available_claim_slots_respects_max_concurrent():
    assert worker.available_claim_slots(max_concurrent=2, active_jobs=0) == 2
    assert worker.available_claim_slots(max_concurrent=2, active_jobs=1) == 1
    assert worker.available_claim_slots(max_concurrent=2, active_jobs=2) == 0
    assert worker.available_claim_slots(max_concurrent=2, active_jobs=3) == 0
    assert worker.available_claim_slots(max_concurrent=0, active_jobs=0) == 1


def test_drain_finished_futures_surfaces_done_jobs_and_keeps_pending():
    done = FakeFuture(done=True)
    pending = FakeFuture(done=False)

    active = worker.drain_finished_futures({done, pending})

    assert active == {pending}
    assert done.result_called is True
    assert pending.result_called is False


def test_send_heartbeat_posts_worker_active_count(monkeypatch):
    calls = []

    class Response:
        def raise_for_status(self):
            return None

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr(worker.requests, "post", fake_post)

    assert worker.send_heartbeat("http://coordinator", 9, active_jobs=2) is True
    assert calls == [
        (
            "http://coordinator/workers/9/heartbeat",
            {"params": {"active_jobs": 2}, "timeout": 5},
        )
    ]


def test_requested_feature_ids_are_deduped_ints():
    assert worker.requested_feature_ids({"feature_ids": [3, "4", 3, None, "bad"]}) == [3, 4]
    assert worker.requested_feature_ids({}) == []


class _Resp:
    def __init__(self, payload=None, status_code=200):
        self._payload = payload or {}
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("bad status")


def _tags(*names):
    return lambda *_a, **_k: _Resp({"models": [{"name": n} for n in names]})


def _no_generate(*_a, **_k):
    raise AssertionError("generate probe must be opt-in")


@pytest.fixture
def ollama_env(monkeypatch):
    for name in (
        worker.OLLAMA_PROBE_MODEL_ENV,
        worker.OLLAMA_GENERATE_PROBE_ENV,
        worker.OLLAMA_PROBE_TIMEOUT_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


# Regression for 2026-11-06: has_ollama was false on every node while Ollama
# served. The probe asked for llama3.2 (installed nowhere), fell back to the
# first model (qwen3.8:27b / qwen3-vl:32b), and gave its cold load 20 s; the
# load alone took 25 s on grid-svr.


def test_probe_reports_available_with_any_installed_chat_model(ollama_env):
    ollama_env.setattr(worker.requests, "get", _tags("qwen3.8:27b", "nomic-embed-text:latest"))
    ollama_env.setattr(worker.requests, "post", _no_generate)

    probe = worker.probe_ollama()

    assert probe["available"] is True
    assert probe["reachable"] is True
    assert probe["probe_model"] == "qwen3.8:27b"
    assert probe["models"] == ["qwen3.8:27b", "nomic-embed-text:latest"]
    assert worker.detect_ollama() is True


def test_probe_rejects_embedding_only_server(ollama_env):
    ollama_env.setattr(worker.requests, "get", _tags("nomic-embed-text:latest"))
    ollama_env.setattr(worker.requests, "post", _no_generate)

    probe = worker.probe_ollama()

    assert probe["available"] is False
    assert probe["reachable"] is True
    assert probe["models"] == ["nomic-embed-text:latest"]


def test_probe_reports_unreachable_server(ollama_env):
    def refuse(*_a, **_k):
        raise ConnectionError("refused")

    ollama_env.setattr(worker.requests, "get", refuse)

    probe = worker.probe_ollama()

    assert probe == {
        "available": False,
        "reachable": False,
        "models": [],
        "probe_model": None,
        "reason": "unreachable: ConnectionError",
    }
    assert worker.detect_ollama() is False


def test_probe_honours_configured_model_list(ollama_env):
    ollama_env.setattr(worker.requests, "get", _tags("qwen3-vl:32b-instruct", "qwen3:8b"))
    ollama_env.setattr(worker.requests, "post", _no_generate)

    ollama_env.setenv(worker.OLLAMA_PROBE_MODEL_ENV, "llama3.2")
    assert worker.probe_ollama()["available"] is False

    ollama_env.setenv(worker.OLLAMA_PROBE_MODEL_ENV, "llama3.2, qwen3")
    probe = worker.probe_ollama()
    assert probe["available"] is True
    assert probe["probe_model"] == "qwen3:8b"


def test_opt_in_generate_probe_uses_long_timeout(ollama_env):
    seen = {}

    def fake_post(url, **kwargs):
        seen["url"] = url
        seen.update(kwargs)
        return _Resp({"done": True})

    ollama_env.setattr(worker.requests, "get", _tags("qwen3.8:27b"))
    ollama_env.setattr(worker.requests, "post", fake_post)
    ollama_env.setenv(worker.OLLAMA_GENERATE_PROBE_ENV, "1")

    assert worker.probe_ollama()["available"] is True
    assert seen["url"] == "http://localhost:11434/api/generate"
    assert seen["json"]["model"] == "qwen3.8:27b"
    assert seen["timeout"] == worker.DEFAULT_OLLAMA_GENERATE_TIMEOUT_S >= 60


def test_opt_in_generate_probe_failure_marks_unavailable(ollama_env):
    def fail(*_a, **_k):
        raise TimeoutError("read timed out")

    ollama_env.setattr(worker.requests, "get", _tags("qwen3.8:27b"))
    ollama_env.setattr(worker.requests, "post", fail)
    ollama_env.setenv(worker.OLLAMA_GENERATE_PROBE_ENV, "1")
    ollama_env.setenv(worker.OLLAMA_PROBE_TIMEOUT_ENV, "5")

    probe = worker.probe_ollama()

    assert probe["available"] is False
    assert "TimeoutError" in probe["reason"]


def test_resolve_model_falls_back_past_embedding_models(monkeypatch):
    monkeypatch.setattr(worker.requests, "get", _tags("nomic-embed-text:latest", "qwen3:8b"))

    assert worker._resolve_ollama_model("llama3.2") == "qwen3:8b"
    assert worker._resolve_ollama_model("qwen3") == "qwen3:8b"


# Regression for 2026-11-06: forge registered as 127.0.1.1 (Debian maps the
# hostname to that address) because `tailscale` was not on its PATH.


def _no_tailscale(*_a, **_k):
    raise FileNotFoundError("tailscale")


def test_tailscale_ip_never_falls_back_to_loopback(monkeypatch):
    monkeypatch.delenv(worker.TAILSCALE_IP_ENV, raising=False)
    monkeypatch.setattr(worker.subprocess, "run", _no_tailscale)
    monkeypatch.setattr(worker.socket, "gethostname", lambda: "forge")
    monkeypatch.setattr(worker.socket, "gethostbyname", lambda _h: "127.0.1.1")
    monkeypatch.setattr(worker, "_route_source_ip", lambda _url: "100.70.1.2")

    assert worker.get_tailscale_ip("http://100.75.185.36:8100") == "100.70.1.2"

    monkeypatch.setattr(worker, "_route_source_ip", lambda _url: None)
    assert worker.get_tailscale_ip("http://localhost:8100") == "forge"


def test_tailscale_ip_prefers_override_then_cli(monkeypatch):
    class Done:
        returncode = 0
        stdout = "100.68.9.27\n"

    monkeypatch.setattr(worker.subprocess, "run", lambda *_a, **_k: Done())
    monkeypatch.setenv(worker.TAILSCALE_IP_ENV, "100.99.0.1")
    assert worker.get_tailscale_ip() == "100.99.0.1"

    monkeypatch.delenv(worker.TAILSCALE_IP_ENV)
    assert worker.get_tailscale_ip() == "100.68.9.27"


def test_route_source_ip_skips_loopback_coordinator():
    assert worker._route_source_ip("http://localhost:8100") is None
    assert worker._route_source_ip("http://127.0.0.1:8100") is None
    assert worker._route_source_ip(None) is None


# ── Rotation jobs from a PIT snapshot (W2-7) ────────────────────────────


def test_rotation_snapshot_is_off_without_the_env(monkeypatch):
    monkeypatch.delenv(worker.PIT_SNAPSHOT_DIR_ENV, raising=False)
    from datetime import date

    assert worker.rotation_snapshot_for({}, date(2024, 1, 1), date(2026, 3, 25)) is None


def test_rotation_snapshot_passes_dir_and_pinned_sha(monkeypatch, tmp_path):
    from datetime import date

    from alpha_research.strategies import rotation_variant_backtest as rvb

    seen = {}
    sentinel = object()

    def fake_find(directory, ws, we, *, sha256=None):
        seen.update(directory=directory, ws=ws, we=we, sha256=sha256)
        return sentinel

    monkeypatch.setenv(worker.PIT_SNAPSHOT_DIR_ENV, str(tmp_path))
    monkeypatch.setattr(rvb, "find_rotation_snapshot", fake_find)
    got = worker.rotation_snapshot_for(
        {"pit_snapshot_sha256": "ab" * 32}, date(2024, 1, 1), date(2026, 3, 25)
    )
    assert got is sentinel
    assert seen == {"directory": str(tmp_path), "ws": date(2024, 1, 1),
                    "we": date(2026, 3, 25), "sha256": "ab" * 32}


def test_rotation_job_hands_the_snapshot_to_the_backtest(monkeypatch):
    from alpha_research.strategies import rotation_variant_backtest as rvb

    calls = {}
    snapshot = object()
    monkeypatch.setattr(worker, "rotation_snapshot_for", lambda params, ws, we: snapshot)
    monkeypatch.setattr("db.get_engine", lambda: "engine")

    def fake_backtest(engine, cfg, ws, we, *, pit=None):
        calls.update(engine=engine, pit=pit)
        return {"total_return": 0.1, "data_source": {"mode": "snapshot"}}

    monkeypatch.setattr(rvb, "backtest_rotation_variant", fake_backtest)
    out = worker.run_backtest({"kind": "rotation_variant", "TREND_WEEKS": 20})
    assert calls == {"engine": "engine", "pit": snapshot}
    assert out["metrics"]["data_source"] == "snapshot"
