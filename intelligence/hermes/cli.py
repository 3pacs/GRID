"""Hermes CLI — ``python -m intelligence.hermes.cli {ping,ask}``.

(On the server the working tree sits under ``grid/`` so the equivalent
invocation is ``python -m grid.intelligence.hermes.cli ...``.)

``ping`` reports config + reachability and, when an API key is set, makes one
tiny real call so you can confirm the model, latency, token accounting
(including reasoning tokens) and today's spend. It exits 0 when *any* analyst
path is usable (Hermes or the local fallback) and 1 only when nothing is.
"""

from __future__ import annotations

import argparse
import shutil
import sys

from .agent import HermesAgent
from .codex_provider import CodexProvider
from .config import load_hermes_config
from .prompts import SYSTEM_VERSION, build_messages
from .provider import HermesProvider


def _cmd_ping(args: argparse.Namespace) -> int:
    cfg = load_hermes_config()
    print("Hermes analyst bridge")
    print(f"  enabled:        {cfg.enabled}")
    print(f"  backend:        {cfg.backend}")
    print(f"  fallback tier:  {cfg.fallback_tier}")
    print(f"  prompt version: {SYSTEM_VERSION}")

    if cfg.backend == "codex":
        primary_ok = _ping_codex(cfg, args.no_call)
    else:
        primary_ok = _ping_openai(cfg, args.no_call)

    fallback_ok = _fallback_available(cfg.fallback_tier)
    print(f"  local fallback: {'available' if fallback_ok else 'unavailable'}")

    usable = primary_ok or fallback_ok
    print(f"\nstatus: {'OK' if usable else 'UNAVAILABLE'} "
          f"(primary={'up' if primary_ok else 'down'}, "
          f"fallback={'up' if fallback_ok else 'down'})")
    return 0 if usable else 1


def _ping_openai(cfg, no_call: bool) -> bool:
    print(f"  api key:        {'set' if cfg.configured else '(not set)'}")
    print(f"  model:          {cfg.model}")
    print(f"  base url:       {cfg.base_url}")
    print(f"  reasoning:      {cfg.reasoning_effort or '(default)'}")
    print(f"  daily cap:      {f'${cfg.daily_spend_cap_usd:.2f}' if cfg.daily_spend_cap_usd else '(none)'}")
    provider = HermesProvider(cfg)
    print(f"  spend today:    ${provider.ledger.spend_today():.4f}")
    if not cfg.configured:
        return False
    if no_call:
        print("  -> call skipped (--no-call)")
        return True
    print("  -> pinging model ...")
    # Budget headroom matters for reasoning models — a tiny cap can be fully
    # consumed by reasoning tokens, leaving empty content.
    resp = provider.complete(
        build_messages("Reply with the single word: pong"),
        max_completion_tokens=256,
    )
    if resp is None:
        print("     primary call failed/capped — see logs")
        return False
    u = resp.usage
    print(f"     OK: {resp.text!r} model={resp.model} {resp.latency_ms:.0f}ms")
    print(f"     tokens: in={u.prompt_tokens} out={u.completion_tokens} "
          f"reasoning={u.reasoning_tokens} | cost=${resp.cost_usd:.4f}")
    return True


def _ping_codex(cfg, no_call: bool) -> bool:
    found = shutil.which(cfg.codex_bin) is not None
    print(f"  codex bin:      {cfg.codex_bin} ({'found' if found else 'NOT FOUND'})")
    print(f"  codex model:    {cfg.codex_model or '(CLI default / GPT-5.5)'}")
    print(f"  timeout:        {cfg.codex_timeout_seconds}s")
    print("  auth:           ChatGPT subscription via `codex login` (no key in .env)")
    if not found:
        print("     install: `npm install -g @openai/codex`, then `codex login`")
        return False
    if no_call:
        print("  -> call skipped (--no-call)")
        return True
    print("  -> pinging codex (subscription) ...")
    resp = CodexProvider(cfg).complete(build_messages("Reply with the single word: pong"))
    if resp is None:
        print("     codex call failed — not signed in? run `codex login`. See logs.")
        return False
    print(f"     OK: {resp.text!r} model={resp.model} {resp.latency_ms:.0f}ms")
    return True


def _cmd_ask(args: argparse.Namespace) -> int:
    agent = HermesAgent()
    result = agent.analyze(args.prompt, context=args.context)
    if not result.ok:
        print("No analyst available (primary and fallback both down).", file=sys.stderr)
        return 1
    print(f"[source={result.source} model={result.model} "
          f"cost=${result.cost_usd:.4f} reasoning={result.reasoning_tokens}]")
    print(result.text)
    return 0


def _fallback_available(tier_name: str) -> bool:
    try:
        from llm.router import Tier, get_llm

        from .agent import _tier_from_str

        client = get_llm(_tier_from_str(tier_name, Tier))
        return bool(client is not None and getattr(client, "is_available", False))
    except Exception:
        return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hermes", description="Hermes analyst bridge CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ping = sub.add_parser("ping", help="report config + reachability")
    p_ping.add_argument("--no-call", action="store_true",
                        help="do not make a real API call, just report config")
    p_ping.set_defaults(func=_cmd_ping)

    p_ask = sub.add_parser("ask", help="run an analyst prompt")
    p_ask.add_argument("prompt", help="the analyst question")
    p_ask.add_argument("--context", default=None, help="optional point-in-time context")
    p_ask.set_defaults(func=_cmd_ask)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
