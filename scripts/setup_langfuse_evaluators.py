"""Configure server-side Langfuse evaluators that auto-score every chat-ask trace.

Creates 4 LLM-as-judge evaluators + matching evaluation rules in the project:
  1. banned_phrases  — detects hedging cliches GRID forbids
  2. plain_english   — child-simple language, jargon translated
  3. multi_source    — cites >=2 independent sources or admits gap
  4. action_call     — ends with concrete tickers/levels/triggers/timeframes

Each rule targets observations whose trace name == "chat-ask" so every real user
question gets scored as soon as it arrives.

Docs consulted:
  - https://langfuse.com/docs/evaluation/evaluation-methods/llm-as-a-judge.md
  - https://langfuse.com/docs/administration/llm-connection.md
  - REST endpoints discovered via `npx langfuse-cli api unstable-evaluators --help`
    and `npx langfuse-cli api unstable-evaluation-rules --help`

REST endpoints used:
  PUT   /api/public/llm-connections                 — register OpenAI key
  POST  /api/public/unstable/evaluators             — create evaluator (rubric)
  POST  /api/public/unstable/evaluation-rules       — create live rule
  GET   /api/public/unstable/evaluation-rules       — list rules (idempotency)
  PATCH /api/public/unstable/evaluation-rules/{id}  — update existing rule

Auth:
  HTTP Basic with (LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY) from .env.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

# ── env loading ─────────────────────────────────────────────────────────────

ENV_PATH = Path("/data/grid_v4/grid_repo/.env")


def _load_env() -> dict[str, str]:
    """Read .env file into a dict; do not mutate process env."""
    if not ENV_PATH.exists():
        raise SystemExit(f"missing env file: {ENV_PATH}")
    out: dict[str, str] = {}
    for raw in ENV_PATH.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


_ENV = _load_env()
LANGFUSE_HOST = _ENV.get("LANGFUSE_HOST") or os.environ.get("LANGFUSE_HOST", "")
PUBLIC_KEY = _ENV.get("LANGFUSE_PUBLIC_KEY") or os.environ.get("LANGFUSE_PUBLIC_KEY", "")
SECRET_KEY = _ENV.get("LANGFUSE_SECRET_KEY") or os.environ.get("LANGFUSE_SECRET_KEY", "")
OPENAI_KEY = _ENV.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY", "")
ANTHROPIC_KEY = _ENV.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")

if not (LANGFUSE_HOST and PUBLIC_KEY and SECRET_KEY):
    raise SystemExit("missing LANGFUSE_HOST / LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY")

AUTH = (PUBLIC_KEY, SECRET_KEY)
TIMEOUT = 30


# ── http helpers ────────────────────────────────────────────────────────────


def _req(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{LANGFUSE_HOST}{path}"
    resp = requests.request(method, url, auth=AUTH, json=body, timeout=TIMEOUT)
    if resp.status_code >= 400:
        raise SystemExit(
            f"{method} {path} → {resp.status_code}\n"
            f"body={json.dumps(body, indent=2) if body else '<none>'}\n"
            f"resp={resp.text}"
        )
    if resp.text:
        return resp.json()
    return {}


# ── llm connection ──────────────────────────────────────────────────────────


def ensure_llm_connection() -> tuple[str, str]:
    """Upsert OpenAI LLM connection. Returns (provider_key, model)."""
    if not OPENAI_KEY:
        raise SystemExit("OPENAI_API_KEY missing — cannot register LLM connection")
    body = {
        "provider": "openai",
        "adapter": "openai",
        "secretKey": OPENAI_KEY,
        "withDefaultModels": True,
    }
    out = _req("PUT", "/api/public/llm-connections", body)
    print(f"[ok] llm-connection: provider={out.get('provider')} id={out.get('id')}")
    # gpt-4o-mini is default-shipped with the openai adapter.
    return ("openai", "gpt-4o-mini")


# ── evaluator definitions ───────────────────────────────────────────────────

# All evaluators target the trace's final answer (observation.output → trace
# root span output). The chatbot wraps responses in {answer:..., latency_s:...},
# so we instruct the judge to read the `answer` field.

EVALUATOR_SPECS: list[dict[str, Any]] = [
    {
        "name": "grid_banned_phrases",
        "prompt": (
            "You are auditing a GRID Intelligence chatbot answer for forbidden hedging "
            "and disclaimer phrases. The product spec FORBIDS these substrings "
            "(case-insensitive):\n"
            "  - 'this is not financial advice'\n"
            "  - 'do your own research'\n"
            "  - 'consider monitoring'\n"
            "  - 'past performance'\n"
            "  - \"while i can't predict\"\n"
            "  - 'it is important to note' / \"it's important to note\"\n"
            "  - 'as always'\n"
            "  - 'let me know if'\n\n"
            "Return score = 1 if NONE of those substrings appear anywhere in the "
            "answer. Return score = 0 if ANY of them appear.\n\n"
            "Answer to audit:\n{{output}}"
        ),
        "score_desc": (
            "1 if the answer contains zero banned phrases, 0 if any banned phrase appears."
        ),
    },
    {
        "name": "grid_plain_english",
        "prompt": (
            "You are evaluating whether a GRID Intelligence chatbot answer is written in "
            "plain English that a smart 12-year-old could follow. The product rule is: "
            "the answer must be CHILD-SIMPLE and TRANSLATE jargon, not just use it.\n\n"
            "Score 1.0  — every technical term (e.g. gamma, basis, contango, Sharpe, vol "
            "             surface) is followed by a short plain-English gloss; sentences "
            "             are short and direct; verdict is unmistakable.\n"
            "Score 0.7  — mostly plain, one or two unexplained jargon terms.\n"
            "Score 0.4  — jargon-heavy with little explanation.\n"
            "Score 0.0  — incomprehensible jargon dump, no translation, no clear verdict.\n\n"
            "Return a single number in [0,1] and one sentence of reasoning.\n\n"
            "Answer:\n{{output}}"
        ),
        "score_desc": "Score in [0,1]: how child-simple and jargon-translated the answer is.",
    },
    {
        "name": "grid_multi_source",
        "prompt": (
            "You are evaluating whether a GRID Intelligence chatbot answer is grounded in "
            "MULTIPLE INDEPENDENT signal sources. The product rule (data integrity): every "
            "claim should be backed by >=2 independent sources, OR the answer must "
            "explicitly state where data is missing.\n\n"
            "Independent sources include things like: price action, options flow, "
            "insider trades, news headlines, sentiment, macro prints, on-chain flows, "
            "shipping/AIS, satellite, fundamentals, technicals — count each as one "
            "source if explicitly cited.\n\n"
            "Score 1.0  — answer cites >=2 named, distinct signal sources OR explicitly "
            "             flags 'insufficient data' on points it cannot support.\n"
            "Score 0.6  — cites 1 source and reasons reasonably from it.\n"
            "Score 0.3  — vague references, no clear sources named.\n"
            "Score 0.0  — pure speculation with no sourcing and no honesty about gaps.\n\n"
            "Return a single number in [0,1] and one sentence of reasoning.\n\n"
            "Answer:\n{{output}}"
        ),
        "score_desc": (
            "Score in [0,1]: does the answer cite >=2 independent sources or honestly "
            "flag missing data?"
        ),
    },
    {
        "name": "grid_action_call",
        "prompt": (
            "You are evaluating whether a GRID Intelligence chatbot answer ends with a "
            "CONCRETE, ACTIONABLE call. The product rule: never end with mush like "
            "'monitor the situation' — the answer must give the reader real handles.\n\n"
            "An actionable ending names at least one of: specific tickers, price levels, "
            "triggers (breakouts, breakdowns, threshold crossings), or explicit time "
            "horizons (intraday / 1-week / 1-month / event date).\n\n"
            "Score 1.0  — closes with named tickers AND specific levels OR triggers AND a "
            "             time horizon.\n"
            "Score 0.7  — closes with tickers and levels OR triggers, but no time horizon.\n"
            "Score 0.4  — gestures at action but stays generic ('watch energy names').\n"
            "Score 0.0  — closes with 'monitor', 'wait and see', 'stay tuned' or nothing.\n\n"
            "Return a single number in [0,1] and one sentence of reasoning.\n\n"
            "Answer:\n{{output}}"
        ),
        "score_desc": (
            "Score in [0,1]: does the answer end with concrete tickers/levels/triggers/"
            "timeframes (not 'monitor the situation')?"
        ),
    },
]


def create_evaluator(spec: dict[str, Any], provider: str, model: str) -> dict[str, Any]:
    body = {
        "name": spec["name"],
        "prompt": spec["prompt"],
        "outputDefinition": {
            "dataType": "NUMERIC",
            "reasoning": {"description": "One sentence of reasoning for the score."},
            "score": {"description": spec["score_desc"]},
        },
        "modelConfig": {"provider": provider, "model": model},
    }
    out = _req("POST", "/api/public/unstable/evaluators", body)
    print(
        f"[ok] evaluator: name={out.get('name')} id={out.get('id')} "
        f"version={out.get('version')}"
    )
    return out


# ── evaluation rules ────────────────────────────────────────────────────────


CHAT_TRACE_NAME = "chat-ask"


def _list_rules() -> list[dict[str, Any]]:
    out = _req("GET", "/api/public/unstable/evaluation-rules?limit=100")
    return out.get("data", []) or []


def upsert_rule(evaluator_name: str, rule_name: str) -> dict[str, Any]:
    """Create rule if absent; otherwise PATCH it to current desired config."""
    existing = next(
        (r for r in _list_rules() if r.get("name") == rule_name),
        None,
    )
    desired = {
        "evaluator": {"name": evaluator_name, "scope": "project"},
        "target": "observation",
        "enabled": True,
        "sampling": 1,
        # Filter by observation `name` (not `traceName`). The chat-ask root
        # span is itself named "chat-ask", and Langfuse only auto-stamps trace
        # attributes on observations when `propagate_attributes()` is called.
        # Filtering on observation `name` works without any propagation.
        "filter": [
            {
                "column": "name",
                "operator": "any of",
                "value": [CHAT_TRACE_NAME],
                "type": "stringOptions",
            }
        ],
        "mapping": [{"variable": "output", "source": "output"}],
    }
    if existing:
        rid = existing["id"]
        # PATCH requires `target` discriminator even though it is effectively
        # immutable. Pass through the full desired body.
        out = _req("PATCH", f"/api/public/unstable/evaluation-rules/{rid}", desired)
        print(f"[ok] rule: name={rule_name} id={rid} (updated)")
        return out
    body = {"name": rule_name, **desired}
    out = _req("POST", "/api/public/unstable/evaluation-rules", body)
    print(f"[ok] rule: name={rule_name} id={out.get('id')} (created)")
    return out


# ── main ────────────────────────────────────────────────────────────────────


def main() -> int:
    print(f"[langfuse] host={LANGFUSE_HOST}")
    if ANTHROPIC_KEY:
        print("[note] ANTHROPIC_API_KEY present, but using OpenAI gpt-4o-mini "
              "for parity with the rest of the chatbot pipeline.")
    else:
        print("[note] no ANTHROPIC_API_KEY — using OpenAI gpt-4o-mini as judge.")

    provider, model = ensure_llm_connection()

    created: list[dict[str, str]] = []
    for spec in EVALUATOR_SPECS:
        ev = create_evaluator(spec, provider, model)
        rule = upsert_rule(ev["name"], f"{ev['name']}__chat-ask")
        created.append(
            {
                "evaluator_name": ev["name"],
                "evaluator_id": ev["id"],
                "evaluator_version": str(ev.get("version", "")),
                "rule_name": rule["name"],
                "rule_id": rule["id"],
            }
        )

    print("\n=== summary ===")
    print(json.dumps(created, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
