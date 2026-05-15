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


def test_detect_ollama_requires_generate_probe(monkeypatch):
    class Response:
        def __init__(self, payload=None, status_code=200):
            self._payload = payload or {}
            self.status_code = status_code

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError("bad status")

    def fake_get(*_args, **_kwargs):
        return Response({"models": [{"name": "llama3.2:latest"}]})

    def fake_post(url, **kwargs):
        assert url == "http://localhost:11434/api/generate"
        assert kwargs["json"]["model"] == "llama3.2:latest"
        return Response({"done": True})

    monkeypatch.setattr(worker.requests, "get", fake_get)
    monkeypatch.setattr(worker.requests, "post", fake_post)

    assert worker.detect_ollama()


def test_detect_ollama_rejects_tags_only_server(monkeypatch):
    class Response:
        status_code = 200

        def json(self):
            return {"models": [{"name": "llama3.2:latest"}]}

    monkeypatch.setattr(worker.requests, "get", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(worker.requests, "post", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("generate failed")))

    assert not worker.detect_ollama()
