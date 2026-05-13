from scripts import local_llm_autotune as autotune


def test_parse_ollama_generate_metrics_uses_eval_duration_for_tokens_per_second() -> None:
    metrics = autotune.ollama_metrics(
        {
            "model": "gemma3:12b",
            "eval_count": 64,
            "eval_duration": 4_000_000_000,
            "total_duration": 5_000_000_000,
        }
    )

    assert metrics.model == "gemma3:12b"
    assert metrics.completion_tokens == 64
    assert metrics.tokens_per_second == 16.0
    assert metrics.latency_seconds == 5.0


def test_llamacpp_metrics_falls_back_to_output_token_estimate() -> None:
    metrics = autotune.llamacpp_metrics(
        model="qwen3-32b",
        payload={"choices": [{"text": "alpha beta gamma delta"}]},
        elapsed_seconds=2.0,
    )

    assert metrics.model == "qwen3-32b"
    assert metrics.completion_tokens == 4
    assert metrics.tokens_per_second == 2.0


def test_recommend_worker_profile_keeps_cpu_only_hosts_conservative() -> None:
    profile = autotune.recommend_worker_profile(
        hostname="ANIK-PC",
        cpu_cores=8,
        ram_gb=16.0,
        gpu_vram_gb=None,
        has_ollama=False,
    )

    assert profile["max_concurrent"] == 1
    assert "CPU-only" in profile["notes"]
    assert "HUMAN_LLM_QUERY" in profile["exclude_types"]


def test_choose_ollama_model_prefers_light_non_embedding_model() -> None:
    assert autotune._choose_ollama_model([
        "nomic-embed-text:latest",
        "llama3.3:70b-instruct-q4_K_M",
        "qwen2.5:7b",
    ]) == "qwen2.5:7b"


def test_inventory_ssh_host_adds_worker_recommendation(monkeypatch) -> None:
    calls = []

    class Result:
        returncode = 0
        stdout = (
            '{"hostname":"koala","cpu_cores":4,"ram_gb":15.0,'
            '"gpu_model":"GTX TITAN X","gpu_vram_gb":24.0,'
            '"ollama_available":true,"ollama_models":["gemma3:12b"]}'
        )
        stderr = ""

    def fake_run(*args, **_kwargs):
        calls.append(args[0])
        return Result()

    monkeypatch.setattr(autotune.subprocess, "run", fake_run)

    inventory = autotune.inventory_ssh_host("koala", timeout=1)

    assert inventory["ok"] is True
    assert inventory["ssh_host"] == "koala"
    assert inventory["recommendation"]["max_concurrent"] == 1
    assert inventory["recommendation"]["exclude_types"] == []
    assert calls[0][0:2] == ["ssh", "koala"]
    assert calls[0][2].startswith("python3 -c ")


def test_inventory_ssh_host_can_request_remote_benchmark(monkeypatch) -> None:
    calls = []

    class Result:
        returncode = 0
        stdout = (
            '{"hostname":"panda","cpu_cores":12,"ram_gb":62.7,'
            '"gpu_model":"Tesla P100","gpu_vram_gb":48.0,'
            '"ollama_available":true,"ollama_models":["qwen"],'
            '"benchmarks":[{"provider":"ollama","model":"qwen",'
            '"ok":true,"tokens_per_second":22.5,"quality_sanity_ok":true}]}'
        )
        stderr = ""

    def fake_run(*args, **_kwargs):
        calls.append(args[0])
        return Result()

    monkeypatch.setattr(autotune.subprocess, "run", fake_run)

    inventory = autotune.inventory_ssh_host("panda", timeout=1, benchmark=True)

    assert calls[0][2].startswith("GRID_LLM_BENCHMARK=1 python3 -c ")
    assert inventory["benchmarks"][0]["tokens_per_second"] == 22.5
    assert inventory["benchmarks"][0]["quality_sanity_ok"] is True
