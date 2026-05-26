"""Tests for the LLM autoresearch loop (quality + tok/sec)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from llm.autoresearch.bench import (
    QualityResult,
    ThroughputResult,
    _content,
    _grade,
    _post_chat,
    _strip_think,
    load_eval_cases,
)
from llm.autoresearch.hosts import (
    HostProfile,
    ModelSpec,
    _aggregate_profile,
    _snapshot_cards,
    arch_caps,
    arch_from_name,
    fits_on,
    load_host_profiles,
    profiles_from_snapshot,
    recommend_for_host,
)
from llm.autoresearch.loop import (
    AutoResearchLoop,
    RunningEndpointApplier,
    TrialConfig,
    compute_fitness,
)
from llm.autoresearch.registry import assess_model


# --------------------------------------------------------------------------
# Quality bar / model assessment
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "model,expected",
    [
        ("qwen3.6:27b-q4_K_M", True),
        ("Qwen3.6-32B", True),
        ("Qwen3-32B-Q4_K_M", False),   # plain Qwen3 == 3.0 < 3.6
        ("qwen3:8b", False),
        ("qwen2.5:7b-instruct-q4_K_M", False),
        ("deepseek-r1:32b", True),     # allow-listed equivalent
        ("gemma3:12b-it-q4_K_M", False),  # non-Qwen, not allow-listed
    ],
)
def test_assess_model(model, expected):
    meets, note = assess_model(model)
    assert meets is expected
    assert note  # always explains itself


# --------------------------------------------------------------------------
# Fitness — hard quality gate
# --------------------------------------------------------------------------
def test_fitness_rejects_below_floor():
    # Blazing fast but below the floor -> rejected.
    assert compute_fitness(quality=0.3, tok_per_sec=999.0, quality_floor=0.6) == float("-inf")


def test_fitness_rewards_throughput_above_floor():
    slow_good = compute_fitness(quality=0.9, tok_per_sec=20.0, quality_floor=0.6)
    fast_good = compute_fitness(quality=0.9, tok_per_sec=40.0, quality_floor=0.6)
    assert fast_good > slow_good


def test_fitness_quality_breaks_ties():
    # Same throughput, higher quality wins.
    lo = compute_fitness(quality=0.7, tok_per_sec=30.0, quality_floor=0.6)
    hi = compute_fitness(quality=0.9, tok_per_sec=30.0, quality_floor=0.6)
    assert hi > lo


# --------------------------------------------------------------------------
# Architecture detection (capability flags from GPU name)
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name,arch,fa,fp8",
    [
        ("NVIDIA GeForce GTX TITAN X", "maxwell", False, False),
        ("NVIDIA TITAN Xp", "pascal", False, False),       # Pascal before Maxwell
        ("NVIDIA GeForce RTX 3090", "ampere", True, False),
        ("NVIDIA RTX PRO 6000 Blackwell", "blackwell", True, True),
        ("NVIDIA H100 80GB", "hopper", True, True),
        ("Some Future Card", "unknown", True, False),
    ],
)
def test_arch_from_name(name, arch, fa, fp8):
    a, f, q = arch_from_name(name)
    assert (a, f, q) == (arch, fa, fp8)


# --------------------------------------------------------------------------
# Host fit estimation (explicit profiles — independent of any stale table)
# --------------------------------------------------------------------------
def _profile(vram, gpus=1, arch="pascal", fa=False):
    return HostProfile("test", vram_gb=vram, gpus=gpus, arch=arch, flash_attn=fa)


def test_small_model_fits_12gb():
    spec = ModelSpec("qwen3.6-7b", params_b=7.0, quant="q4_k_m")
    ok, note = fits_on(spec, _profile(12.0))
    assert ok
    assert "fits" in note


def test_large_model_rejected_on_12gb_single_card():
    spec = ModelSpec("qwen3.6-32b", params_b=32.0, quant="q4_k_m")
    ok, _ = fits_on(spec, _profile(12.0), allow_multi_gpu=False)
    assert not ok


def test_maxwell_flags_no_flash_attn():
    spec = ModelSpec("qwen3.6-7b", params_b=7.0, quant="q4_k_m")
    ok, note = fits_on(spec, _profile(12.0, gpus=2, arch="maxwell", fa=False))
    assert ok
    assert "flash-attn" in note


# --------------------------------------------------------------------------
# VRAM-tier recommendation tree
# --------------------------------------------------------------------------
def test_recommend_high_vram_picks_dense_high_quant():
    rec = recommend_for_host(_profile(48.0, arch="blackwell", fa=True))
    assert "27B" in rec["model"]
    assert rec["quant"] == "Q6_K"
    assert "draft-mtp" in rec["flags"]


def test_recommend_16gb_picks_moe_with_offload():
    rec = recommend_for_host(_profile(16.0, arch="ampere", fa=True))
    assert "A3B" in rec["model"]
    assert "exps.=CPU" in rec["flags"]


def test_recommend_tiny_vram_says_repurpose():
    rec = recommend_for_host(_profile(8.0, arch="pascal", fa=False))
    assert rec["model"] is None
    assert "repurpose" in rec["rationale"]


def test_recommend_no_flash_attn_arch_omits_fa():
    rec = recommend_for_host(_profile(48.0, arch="maxwell", fa=False))
    assert "no -fa" in rec["flags"]


# --------------------------------------------------------------------------
# Grading
# --------------------------------------------------------------------------
def test_grade_contains_and_regex_and_json():
    assert _grade({"check": "contains", "expect": ["25"]}, "It rose 25 bp")
    assert not _grade({"check": "contains", "expect": ["25"]}, "It rose 30 bp")
    assert _grade({"check": "regex", "expect": "^ACK$"}, "ACK")
    assert _grade({"check": "json_keys", "expect": ["a", "b"]}, 'noise {"a":1,"b":2} tail')
    assert not _grade({"check": "json_keys", "expect": ["a", "b"]}, "not json")
    assert _grade({"check": "contains_any", "expect": ["recession", "slowdown"]}, "signals a recession")


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("<think>weighing options</think>The answer is 25 bp", "The answer is 25 bp"),
        ("<THINK>x\ny</THINK>\nACK", "ACK"),                       # case-insensitive, multiline
        ("reasoning blah</think>final answer", "final answer"),    # dangling close tag
        ("no thinking here", "no thinking here"),                  # untouched
        ("a<think>1</think>b<think>2</think>c", "abc"),            # multiple blocks
    ],
)
def test_strip_think(raw, expected):
    assert _strip_think(raw).strip() == expected


def test_content_strips_think_from_chat_payload():
    data = {"choices": [{"message": {"content": "<think>hmm</think>  42  "}}]}
    assert _content(data) == "42"


def test_post_chat_disables_thinking(monkeypatch):
    captured = {}

    class _Resp:
        status_code = 200

        def json(self):
            return {"choices": [{"message": {"content": "ok"}}]}

    def _fake_post(url, json, timeout):
        captured["url"] = url
        captured["payload"] = json
        return _Resp()

    monkeypatch.setattr("requests.post", _fake_post)
    _post_chat("http://x", "qwen3.6:27b", [{"role": "user", "content": "hi"}],
               max_tokens=8, temperature=0.0, timeout=5.0)
    assert captured["payload"]["chat_template_kwargs"] == {"enable_thinking": False}
    assert captured["url"].endswith("/v1/chat/completions")


def test_eval_set_loads_and_is_wellformed():
    cases = load_eval_cases()
    assert len(cases) >= 8
    for c in cases:
        assert "id" in c and "prompt" in c and "check" in c


# --------------------------------------------------------------------------
# Loop integration with injected measurement (no network)
# --------------------------------------------------------------------------
def _fake_quality(score):
    return lambda base_url, model, **kw: QualityResult(score=score, passed=int(score * 10), total=10, reachable=True)


def _fake_throughput(tps):
    return lambda base_url, model, **kw: ThroughputResult(tok_per_sec=tps, samples=[tps], reachable=True, source="timings")


def test_loop_picks_fastest_passing_config(tmp_path: Path):
    journal = tmp_path / "j.jsonl"
    # Two configs both pass quality; second is faster -> champion.
    qmap = {"slow": 0.9, "fast": 0.9}
    tmap = {"slow": 20.0, "fast": 45.0}

    def quality_fn(base_url, model, **kw):
        return QualityResult(score=qmap[model], passed=9, total=10, reachable=True)

    def throughput_fn(base_url, model, **kw):
        return ThroughputResult(tok_per_sec=tmap[model], samples=[tmap[model]], reachable=True, source="timings")

    loop = AutoResearchLoop(
        quality_floor=0.6,
        quality_fn=quality_fn,
        throughput_fn=throughput_fn,
        journal_path=journal,
    )
    configs = [
        TrialConfig("e1", "http://x", "slow"),
        TrialConfig("e2", "http://x", "fast"),
    ]
    champ = loop.run(configs)
    assert champ is not None
    assert champ.config.model == "fast"
    # Journal written for both trials.
    lines = journal.read_text().strip().splitlines()
    assert len(lines) == 2
    assert all(json.loads(ln)["reachable"] for ln in lines)


def test_loop_rejects_fast_but_low_quality(tmp_path: Path):
    journal = tmp_path / "j.jsonl"
    loop = AutoResearchLoop(
        quality_floor=0.6,
        quality_fn=_fake_quality(0.2),       # below floor
        throughput_fn=_fake_throughput(500.0),  # very fast
        journal_path=journal,
    )
    champ = loop.run([TrialConfig("e1", "http://x", "fast-but-dumb")])
    assert champ is None  # nothing cleared the floor
    assert loop.history[0].accepted is False
    assert "more harm than good" in loop.history[0].note


def test_running_endpoint_applier_is_noop():
    applier = RunningEndpointApplier()
    cfg = TrialConfig("e1", "http://host:8081", "qwen3.6")
    assert applier.apply(cfg) == ("http://host:8081", "qwen3.6")


# --------------------------------------------------------------------------
# Architecture capability map (label -> flash_attn / fp8)
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "arch,fa,fp8",
    [
        ("Blackwell", True, True),
        ("Hopper", True, True),
        ("Ampere", True, False),
        ("Turing", True, False),
        ("Pascal", False, False),
        ("Maxwell", False, False),
        ("SomethingNew", True, False),  # unknown -> modern assumption
    ],
)
def test_arch_caps(arch, fa, fp8):
    assert arch_caps(arch) == (fa, fp8)


# --------------------------------------------------------------------------
# Heterogeneous GPU aggregation (the fleet is all mixed-card boxes)
# --------------------------------------------------------------------------
def test_aggregate_sums_vram_across_mixed_cards():
    # grid-svr: A2000 12GB (Ampere) + RTX PRO 2000 Blackwell 16GB.
    prof = _aggregate_profile(
        "grid-svr",
        [("NVIDIA RTX A2000 12GB", 12.0, "Ampere"),
         ("NVIDIA RTX PRO 2000 Blackwell", 16.0, "Blackwell")],
        "snapshot",
    )
    assert prof.total_vram_gb == 28.0   # summed, not first*count
    assert prof.gpus == 2
    assert prof.flash_attn is True      # both support FA
    assert prof.fp8 is False            # Ampere lacks fp8 -> conservative AND
    assert prof.arch == "mixed:ampere+blackwell"


def test_aggregate_pascal_disables_flash_attn():
    # panda: 3x P100 Pascal -> no flash-attn, 48GB total.
    prof = _aggregate_profile(
        "panda",
        [("Tesla P100-PCIE-16GB", 16.0, "Pascal")] * 3,
        "snapshot",
    )
    assert prof.total_vram_gb == 48.0
    assert prof.gpus == 3
    assert prof.flash_attn is False
    assert prof.arch == "pascal"
    assert "3x" in prof.gpu_name


def test_aggregate_mixed_arch_uses_least_capable():
    # koala is Turing (fa yes); a hypothetical Pascal sibling would gate FA off.
    prof = _aggregate_profile(
        "mixed",
        [("NVIDIA GeForce RTX 2070 SUPER", 8.0, "Turing"),
         ("NVIDIA GeForce GTX 1070", 8.0, "Pascal")],
        "detected",
    )
    assert prof.flash_attn is False  # one Pascal card forces it off


def test_aggregate_infers_arch_from_name_when_label_blank():
    prof = _aggregate_profile(
        "k", [("NVIDIA GeForce RTX 2070 SUPER", 8.0, "")], "detected"
    )
    assert prof.arch == "turing"
    assert prof.flash_attn is True


# --------------------------------------------------------------------------
# Snapshot parsing — filter display adapters, gate on status
# --------------------------------------------------------------------------
def test_snapshot_cards_filters_display_adapters():
    host = {"gpus": [
        {"name": "NVIDIA RTX A2000 12GB", "uuid": "GPU-abc",
         "memoryTotalMiB": 12282, "architecture": "Ampere"},
        {"name": "Matrox MGA G200eW", "uuid": "",
         "memoryTotalMiB": None, "architecture": ""},   # display adapter -> drop
        {"name": "NVIDIA RTX PRO 2000 Blackwell", "uuid": "GPU-def",
         "memoryTotalMiB": 16311, "architecture": "Blackwell"},
    ]}
    cards = _snapshot_cards(host)
    assert len(cards) == 2
    assert {c[0] for c in cards} == {"NVIDIA RTX A2000 12GB", "NVIDIA RTX PRO 2000 Blackwell"}


def _fake_snapshot_response(payload):
    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    return _Resp()


def test_profiles_from_snapshot_skips_non_ok_and_builds_totals(monkeypatch):
    payload = {"snapshot": {"hosts": [
        {"id": "grid-svr", "status": "ok", "gpus": [
            {"name": "NVIDIA RTX A2000 12GB", "uuid": "g1",
             "memoryTotalMiB": 12282, "architecture": "Ampere"},
            {"name": "NVIDIA RTX PRO 2000 Blackwell", "uuid": "g2",
             "memoryTotalMiB": 16311, "architecture": "Blackwell"},
        ]},
        {"id": "z400", "status": "inactive", "gpus": []},     # skipped
        {"id": "ocr-node", "status": "error", "gpus": []},    # skipped
        {"id": "panda", "status": "ok", "gpus": [
            {"name": "Tesla P100-PCIE-16GB", "uuid": "p1",
             "memoryTotalMiB": 16384, "architecture": "Pascal"},
            {"name": "Tesla P100-PCIE-16GB", "uuid": "p2",
             "memoryTotalMiB": 16384, "architecture": "Pascal"},
        ]},
    ]}}
    monkeypatch.setattr("requests.get", lambda *a, **k: _fake_snapshot_response(payload))

    profiles = profiles_from_snapshot("http://fake/api/snapshot")
    assert set(profiles) == {"grid-svr", "panda"}            # non-ok dropped
    assert round(profiles["grid-svr"].total_vram_gb) == 28   # 12282+16311 MiB
    assert profiles["grid-svr"].source == "snapshot"
    assert profiles["panda"].flash_attn is False             # Pascal


# --------------------------------------------------------------------------
# Layered resolution: fallback < override < snapshot; import stays offline
# --------------------------------------------------------------------------
def test_load_host_profiles_offline_skips_network(monkeypatch, tmp_path):
    def _boom(*a, **k):
        raise AssertionError("network must not be touched when use_snapshot=False")

    monkeypatch.setattr("requests.get", _boom)
    profiles = load_host_profiles(override_path=tmp_path / "missing.json", use_snapshot=False)
    assert profiles  # falls back to the baked-in table
    assert all(p.source == "fallback" for p in profiles.values())


def test_load_host_profiles_snapshot_overrides_override_file(monkeypatch, tmp_path):
    override = tmp_path / "host_profiles.json"
    override.write_text(json.dumps({
        "grid-svr": {"vram_gb": 99.0, "gpus": 1, "gpu_name": "stale", "arch": "ampere"},
        "koala": {"vram_gb": 10.0, "gpus": 2, "gpu_name": "from-file", "arch": "turing"},
    }))
    payload = {"snapshot": {"hosts": [
        {"id": "grid-svr", "status": "ok", "gpus": [
            {"name": "NVIDIA RTX A2000 12GB", "uuid": "g1",
             "memoryTotalMiB": 12282, "architecture": "Ampere"},
        ]},
    ]}}
    monkeypatch.setattr("requests.get", lambda *a, **k: _fake_snapshot_response(payload))

    profiles = load_host_profiles(override_path=override, use_snapshot=True)
    # grid-svr comes from the live snapshot (wins over the stale override file)...
    assert profiles["grid-svr"].source == "snapshot"
    assert profiles["grid-svr"].total_vram_gb == 12.0
    # ...koala isn't in the snapshot, so the override-file value survives.
    assert profiles["koala"].source == "override"
    assert profiles["koala"].gpu_name == "from-file"
