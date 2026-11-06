from scripts import compute_job_producer as producer


def test_recommended_queue_target_scales_with_open_worker_capacity():
    workers = [
        {"state": "IDLE", "max_concurrent": 2, "active_jobs": 0},
        {"state": "BUSY", "max_concurrent": 4, "active_jobs": 3},
        {"state": "OFFLINE", "max_concurrent": 8, "active_jobs": 0},
    ]

    assert producer.recommended_queue_target(workers, multiplier=4, floor=8, ceiling=64) == 12


def test_build_job_plan_uses_valid_models_and_ollama_capability(monkeypatch):
    monkeypatch.setenv(producer.LLM_JOBS_ENV, "1")
    workers = [
        {"state": "IDLE", "max_concurrent": 2, "active_jobs": 0, "has_ollama": False},
        {"state": "IDLE", "max_concurrent": 1, "active_jobs": 0, "has_ollama": True},
    ]

    plan = producer.build_job_plan(
        workers=workers,
        existing_queued=0,
        target_queued=8,
        max_create=8,
        valid_model_ids=[3, 4],
        feature_ids=[101, 102, 103],
        stamp="test",
    )

    assert len(plan) == 8
    assert any(job["job_type"] == "LLM_INFERENCE" and job["requires_ollama"] for job in plan)
    regime_jobs = [job for job in plan if job["job_type"] == "REGIME_DETECT"]
    assert regime_jobs
    assert {tuple(job["params"]["feature_ids"]) for job in regime_jobs} == {(101, 102, 103)}
    backtests = [job for job in plan if job["job_type"] == "BACKTEST"]
    assert backtests
    assert {job["params"]["model_id"] for job in backtests} <= {3, 4}


def test_build_job_plan_skips_db_dependent_jobs_without_valid_ids():
    workers = [
        {"state": "IDLE", "max_concurrent": 2, "active_jobs": 0, "has_ollama": False},
    ]

    plan = producer.build_job_plan(
        workers=workers,
        existing_queued=0,
        target_queued=6,
        max_create=6,
        valid_model_ids=[],
        feature_ids=[],
        stamp="test",
    )

    assert plan == []


def test_parse_compute_inputs_deduplicates_live_metadata():
    payload = {
        "model_ids": [3, 4, 3],
        "feature_ids": [1, "2", 1, None],
    }

    model_ids, feature_ids = producer.parse_compute_inputs(payload)

    assert model_ids == [3, 4]
    assert feature_ids == [1, 2]


def test_llm_filler_jobs_are_opt_in(monkeypatch):
    """Fixing has_ollama must not restart the canned-prompt LLM stream."""
    workers = [{"state": "IDLE", "max_concurrent": 2, "active_jobs": 0, "has_ollama": True}]
    kwargs = {
        "workers": workers, "existing_queued": 0, "target_queued": 8, "max_create": 8,
        "valid_model_ids": [3], "feature_ids": [101], "stamp": "test",
    }

    monkeypatch.delenv(producer.LLM_JOBS_ENV, raising=False)
    assert not any(j["job_type"] == "LLM_INFERENCE" for j in producer.build_job_plan(**kwargs))

    monkeypatch.setenv(producer.LLM_JOBS_ENV, "1")
    assert any(j["job_type"] == "LLM_INFERENCE" for j in producer.build_job_plan(**kwargs))


def test_offline_workers_add_no_capacity_or_ollama():
    workers = [
        {"state": "OFFLINE", "max_concurrent": 4, "active_jobs": 0, "has_ollama": True},
    ]

    assert producer.recommended_queue_target(workers) == 0
    assert producer._has_ollama_worker(workers) is False
