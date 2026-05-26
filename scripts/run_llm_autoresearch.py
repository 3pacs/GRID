#!/usr/bin/env python3
"""Run the LLM autoresearch loop across the GRID fleet.

Examples
--------
  # Show every endpoint and whether it clears the Qwen-3.6 quality bar:
  python -m scripts.run_llm_autoresearch --audit

  # Baseline: measure quality + tok/sec for every eligible endpoint:
  python -m scripts.run_llm_autoresearch --baseline

  # Include below-bar models too (for upgrade planning):
  python -m scripts.run_llm_autoresearch --baseline --include-below-bar

  # Time-boxed search with a custom quality floor:
  python -m scripts.run_llm_autoresearch --baseline --quality-floor 0.7 --budget 1800

This script only *measures* running endpoints. Realizing new serving configs
(swapping model/quant, enabling speculative decoding) requires a fleet-specific
ConfigApplier — see llm/autoresearch/loop.py.
"""

from __future__ import annotations

import argparse
import json
import sys

import json as _json

from llm.autoresearch import (
    AutoResearchLoop,
    TrialConfig,
    detect_local_profile,
    discover_endpoints,
    eligible_endpoints,
    recommend_for_host,
)
from llm.autoresearch.hosts import PROFILE_OVERRIDE_PATH, load_host_profiles, profiles_from_snapshot


def _audit(include_below_bar: bool, qwen_bar: float) -> None:
    profiles = load_host_profiles(use_snapshot=True)
    eps = discover_endpoints(qwen_bar)
    if not eps:
        print("No endpoints configured (check config.settings URLs).")
        return
    print(f"{'ENDPOINT':<28} {'HOST':<10} {'MODEL':<28} {'BAR':<5} NOTE")
    print("-" * 100)
    for e in eps:
        flag = "PASS" if e.meets_bar else "FAIL"
        print(f"{e.name:<28} {e.host:<10} {e.model:<28} {flag:<5} {e.bar_note}")
    below = [e for e in eps if not e.meets_bar]
    if below:
        print()
        print(f"{len(below)} endpoint(s) below the quality bar — recommend upgrading to Qwen 3.6+:")
        for e in below:
            prof = profiles.get(e.host)
            vram = f"{prof.total_vram_gb:.0f}GB {prof.arch}" if prof else "unknown VRAM"
            print(f"  - {e.host} ({vram}): {e.model}")


def _detect(host_name: str) -> None:
    """Print this host's detected GPU profile as a host_profiles.json snippet."""
    prof = detect_local_profile(host_name)
    if prof is None:
        print("No GPU detected here (nvidia-smi unavailable or CPU-only node).")
        return
    print(f"Detected on '{host_name}': {prof.gpu_name} "
          f"@ {prof.total_vram_gb:.0f}GB total ({prof.arch}, flash_attn={prof.flash_attn}, fp8={prof.fp8})")
    print("\nAdd to llm/autoresearch/host_profiles.json:")
    # Emit the resolved arch/caps too — for mixed-card hosts these can't be
    # re-derived from a single gpu_name, so they must be pinned explicitly.
    snippet = {host_name: {
        "vram_gb": prof.vram_gb, "gpus": prof.gpus, "gpu_name": prof.gpu_name,
        "arch": prof.arch, "flash_attn": prof.flash_attn, "fp8": prof.fp8,
    }}
    print(_json.dumps(snippet, indent=2))


def _refresh_profiles(snapshot_url: str | None) -> int:
    """Write a cached host_profiles.json from the live fleet dashboard."""
    try:
        profiles = profiles_from_snapshot(snapshot_url) if snapshot_url else profiles_from_snapshot()
    except Exception as exc:  # network/parse failure — report, don't overwrite
        print(f"Snapshot fetch failed ({exc}); host_profiles.json left unchanged.")
        return 1
    if not profiles:
        print("Snapshot returned no usable GPU hosts; host_profiles.json left unchanged.")
        return 1
    out = {
        host: {
            "vram_gb": p.vram_gb, "gpus": p.gpus, "gpu_name": p.gpu_name,
            "arch": p.arch, "flash_attn": p.flash_attn, "fp8": p.fp8, "notes": p.notes,
        }
        for host, p in sorted(profiles.items())
    }
    PROFILE_OVERRIDE_PATH.write_text(_json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(out)} host profile(s) to {PROFILE_OVERRIDE_PATH}:")
    for host, p in sorted(profiles.items()):
        print(f"  {host:<10} {p.total_vram_gb:.0f}GB {p.arch:<22} {p.gpu_name}")
    return 0


def _plan(qwen_bar: float) -> None:
    """Print the VRAM-tier Qwen 3.6+ recommendation for each known host."""
    profiles = load_host_profiles(use_snapshot=True)
    print(f"{'HOST':<10} {'SRC':<9} {'VRAM':<7} {'MODEL':<26} {'QUANT':<8} RATIONALE")
    print("-" * 110)
    for host, prof in profiles.items():
        rec = recommend_for_host(prof)
        model = rec.get("model") or "(none — repurpose)"
        quant = rec.get("quant") or "-"
        vram = f"{prof.total_vram_gb:.0f}GB"
        print(f"{host:<10} {prof.source:<9} {vram:<7} {model:<26} {quant:<8} {rec['rationale']}")
        if rec.get("flags"):
            print(f"{'':<10} flags: {rec['flags']}")
    if any(p.source == "fallback" for p in profiles.values()):
        print("\nWARNING: some profiles are STALE fallbacks (live snapshot didn't cover them). "
              "Run --detect on those hosts, or --refresh-profiles to re-pull the dashboard.")


def _baseline(args: argparse.Namespace) -> int:
    eps = eligible_endpoints(include_below_bar=args.include_below_bar, qwen_bar=args.qwen_bar)
    if not eps:
        print("No eligible endpoints. Use --include-below-bar to measure everything, "
              "or --audit to see why each was excluded.")
        return 1

    configs = [
        TrialConfig(endpoint=e.name, base_url=e.base_url, model=e.model, host=e.host)
        for e in eps
    ]
    loop = AutoResearchLoop(quality_floor=args.quality_floor)
    print(f"Measuring {len(configs)} endpoint(s); quality floor = {args.quality_floor} ...")
    print(f"Journal: {loop.journal_path}")
    loop.run(configs, budget_seconds=args.budget, max_trials=args.max_trials)

    print()
    print(f"{'ENDPOINT':<28} {'QUALITY':<9} {'TOK/S':<9} {'ACCEPTED':<9} NOTE")
    print("-" * 100)
    for r in loop.history:
        acc = "yes" if r.accepted else "NO"
        star = " *CHAMPION*" if r.is_champion else ""
        print(f"{r.config.endpoint:<28} {r.quality:<9.3f} {r.tok_per_sec:<9.1f} {acc:<9} {r.note}{star}")

    best = loop.best_config()
    print()
    if best:
        print("Champion config:")
        print(json.dumps(best, indent=2))
    else:
        print("No config cleared the quality floor.")
    if loop.pareto:
        print(f"\nPareto front ({len(loop.pareto)} non-dominated configs):")
        for r in sorted(loop.pareto, key=lambda x: x.tok_per_sec, reverse=True):
            print(f"  {r.config.endpoint:<28} quality={r.quality:.3f} tok/s={r.tok_per_sec:.1f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="GRID LLM autoresearch (quality + tok/sec)")
    p.add_argument("--audit", action="store_true", help="List endpoints and quality-bar status, then exit")
    p.add_argument("--detect", metavar="HOST", help="Detect this machine's GPU profile (run on each host)")
    p.add_argument("--refresh-profiles", action="store_true",
                   help="Re-pull the live fleet dashboard and cache host_profiles.json")
    p.add_argument("--snapshot-url", default=None,
                   help="Override the fleet inventory snapshot URL (else env / default)")
    p.add_argument("--plan", action="store_true", help="Print VRAM-tier Qwen 3.6+ recommendation per host")
    p.add_argument("--baseline", action="store_true", help="Measure quality + tok/sec for eligible endpoints")
    p.add_argument("--include-below-bar", action="store_true", help="Include models below the Qwen-3.6 bar")
    p.add_argument("--quality-floor", type=float, default=0.6, help="Hard quality gate (eval pass fraction)")
    p.add_argument("--qwen-bar", type=float, default=3.6, help="Minimum acceptable Qwen version")
    p.add_argument("--budget", type=float, default=None, help="Wall-clock budget in seconds")
    p.add_argument("--max-trials", type=int, default=None, help="Maximum number of trials")
    args = p.parse_args(argv)

    if args.detect:
        _detect(args.detect)
        return 0
    if args.refresh_profiles:
        return _refresh_profiles(args.snapshot_url)
    if args.plan:
        _plan(args.qwen_bar)
        return 0
    if args.audit:
        _audit(args.include_below_bar, args.qwen_bar)
        return 0
    if args.baseline:
        return _baseline(args)
    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
