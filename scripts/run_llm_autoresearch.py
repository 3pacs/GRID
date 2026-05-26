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

from llm.autoresearch import (
    AutoResearchLoop,
    TrialConfig,
    discover_endpoints,
    eligible_endpoints,
)
from llm.autoresearch.hosts import HOST_PROFILES


def _audit(include_below_bar: bool, qwen_bar: float) -> None:
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
            prof = HOST_PROFILES.get(e.host)
            vram = f"{prof.total_vram_gb:.0f}GB {prof.arch}" if prof else "unknown VRAM"
            print(f"  - {e.host} ({vram}): {e.model}")


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
    p.add_argument("--baseline", action="store_true", help="Measure quality + tok/sec for eligible endpoints")
    p.add_argument("--include-below-bar", action="store_true", help="Include models below the Qwen-3.6 bar")
    p.add_argument("--quality-floor", type=float, default=0.6, help="Hard quality gate (eval pass fraction)")
    p.add_argument("--qwen-bar", type=float, default=3.6, help="Minimum acceptable Qwen version")
    p.add_argument("--budget", type=float, default=None, help="Wall-clock budget in seconds")
    p.add_argument("--max-trials", type=int, default=None, help="Maximum number of trials")
    args = p.parse_args(argv)

    if args.audit:
        _audit(args.include_below_bar, args.qwen_bar)
        return 0
    if args.baseline:
        return _baseline(args)
    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
