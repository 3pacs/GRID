"""Tests for the LLM autoresearch loop (quality + tok/sec)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from llm.autoresearch.bench import QualityResult, ThroughputResult, _grade, load_eval_cases
from llm.autoresearch.hosts import (
    HostProfile,
    ModelSpec,
    arch_from_name,
    fits_on,
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
