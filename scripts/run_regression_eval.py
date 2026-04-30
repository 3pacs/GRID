"""GRID chatbot regression eval runner.

Pulls the `grid-chatbot-regressions` dataset from Langfuse, runs each item's
question through GRID's chat LLM (using the same system prompt as
api/routers/chat.py), then asks an LLM-as-judge whether the answer satisfies
the item's `behavior_contract`. Scores are posted back to Langfuse via the
v4 `dataset.run_experiment(...)` API so each run shows up in the UI with
per-item `contract_satisfaction` (numeric) and `passed` (boolean) scores.

Usage:
    python scripts/run_regression_eval.py
    python scripts/run_regression_eval.py --provider openai --judge-model gpt-4o
    python scripts/run_regression_eval.py --dataset grid-chatbot-regressions \\
        --run-name my-test-run --pass-threshold 0.7

Exit code: 0 if every item passes, 1 otherwise. 2 on configuration errors.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# --- env loading ------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"

try:
    from dotenv import load_dotenv
except ImportError as exc:  # pragma: no cover
    print(f"FATAL: python-dotenv not installed: {exc}", file=sys.stderr)
    sys.exit(2)

if not ENV_PATH.exists():
    print(f"FATAL: .env not found at {ENV_PATH}", file=sys.stderr)
    sys.exit(2)
load_dotenv(ENV_PATH)

# Make repo importable for `llm.router` and `api.routers.chat`.
sys.path.insert(0, str(REPO_ROOT))

# --- third-party ------------------------------------------------------------
try:
    from langfuse import Evaluation, Langfuse
except ImportError as exc:
    print(f"FATAL: langfuse not installed: {exc}", file=sys.stderr)
    sys.exit(2)

# --- GRID imports -----------------------------------------------------------
try:
    from llm.router import get_llm
except Exception as exc:
    print(f"FATAL: cannot import llm.router: {exc}", file=sys.stderr)
    sys.exit(2)

try:
    # Reuse the exact static system prompt the chat router uses. We
    # intentionally do NOT call _build_system_prompt() because that pulls
    # live codebase context which is non-deterministic across runs.
    from api.routers.chat import GRID_SYSTEM_CONTEXT
except Exception as exc:
    print(f"FATAL: cannot import GRID_SYSTEM_CONTEXT from api.routers.chat: {exc}",
          file=sys.stderr)
    sys.exit(2)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_DATASET = "grid-chatbot-regressions"
DEFAULT_PROVIDER = "openai"
DEFAULT_JUDGE_MODEL = "gpt-4o-mini"
DEFAULT_PASS_THRESHOLD = 0.7
CHAT_TEMPERATURE = 0.2
CHAT_MAX_TOKENS = 800
JUDGE_TEMPERATURE = 0.0
JUDGE_MAX_TOKENS = 400
DEFAULT_MAX_CONCURRENCY = 3  # OpenAI rate limits + judge calls — keep modest

JUDGE_SYSTEM = (
    "You are a strict regression-test judge for an AI financial-intelligence "
    "chatbot. You will be given a behavior contract describing what the "
    "chatbot MUST do, and the chatbot's actual answer. Score how well the "
    "answer satisfies the contract on a 0.0-1.0 scale. Be strict: missing a "
    "required behavior is a fail. Reply ONLY with a JSON object of the form "
    '{"score": <float 0..1>, "passed": <bool>, "reason": <one-sentence string>}. '
    "Do not wrap it in markdown fences. Do not add commentary outside the JSON."
)


# ---------------------------------------------------------------------------
# Data classes (immutable)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class JudgeVerdict:
    score: float
    passed: bool
    reason: str


# ---------------------------------------------------------------------------
# Chatbot mimicry
# ---------------------------------------------------------------------------

def build_user_prompt(item_input: Any) -> str:
    """Assemble the user-side prompt from a dataset item input.

    The input has shape {question, ticker?, timeframe?}. We inject ticker /
    timeframe as a small preamble so the model has the same hints the real
    chat endpoint would receive via the request body.
    """
    if not isinstance(item_input, dict):
        return str(item_input)

    question = str(item_input.get("question", "")).strip()
    ticker = item_input.get("ticker")
    timeframe = item_input.get("timeframe")

    hints: list[str] = []
    if ticker:
        hints.append(f"Ticker: {ticker}")
    if timeframe:
        hints.append(f"Timeframe: {timeframe}")

    if hints:
        return "\n".join(hints) + "\n\n" + question
    return question


def run_chatbot(question_prompt: str, provider: str) -> str:
    """Invoke the GRID LLM the same way chat.py would.

    Raises if the client is unavailable or returns nothing — we never
    silently swallow.
    """
    client = get_llm(provider=provider)
    if not getattr(client, "is_available", False):
        raise RuntimeError(f"LLM provider '{provider}' reports is_available=False")

    messages = [
        {"role": "system", "content": GRID_SYSTEM_CONTEXT},
        {"role": "user", "content": question_prompt},
    ]
    answer = client.chat(
        messages,
        temperature=CHAT_TEMPERATURE,
        num_predict=CHAT_MAX_TOKENS,
    )
    if not answer or not str(answer).strip():
        raise RuntimeError(f"Empty response from LLM provider '{provider}'")
    return str(answer).strip()


# ---------------------------------------------------------------------------
# Judge
# ---------------------------------------------------------------------------

_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(text: str) -> dict[str, Any]:
    """Best-effort extraction of a JSON object from judge output.

    Some models return code fences or extra prose despite instructions;
    falling back to a regex grab beats throwing away the verdict.
    """
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = _JSON_OBJ_RE.search(text)
    if not match:
        raise ValueError(f"Judge output contained no JSON object: {text!r}")
    return json.loads(match.group(0))


def judge_answer(
    *,
    question: str,
    behavior_contract: str,
    failure_mode: str,
    answer: str,
    judge_provider: str,
    judge_model: str,
    pass_threshold: float,
) -> JudgeVerdict:
    """Ask a separate LLM whether `answer` satisfies the contract.

    Note: ANTHROPIC_API_KEY isn't currently in .env, so the default judge
    routes through OpenAI (gpt-4o-mini). Once an Anthropic key lands, pass
    `--judge-provider anthropic` to swap in Claude.
    """
    judge_client = get_llm(provider=judge_provider)
    if not getattr(judge_client, "is_available", False):
        raise RuntimeError(f"Judge provider '{judge_provider}' is unavailable")

    user_msg = (
        "## Question to chatbot\n"
        f"{question}\n\n"
        "## Behavior contract (what the chatbot MUST do)\n"
        f"{behavior_contract}\n\n"
        f"## Failure mode being tested\n{failure_mode}\n\n"
        "## Chatbot's actual answer\n"
        f"{answer}\n\n"
        "Score 0.0 (total fail) to 1.0 (full satisfaction). "
        "Pass requires substantial compliance with every clause of the contract. "
        "Return JSON only."
    )
    messages = [
        {"role": "system", "content": JUDGE_SYSTEM},
        {"role": "user", "content": user_msg},
    ]
    raw = judge_client.chat(
        messages,
        model=judge_model,
        temperature=JUDGE_TEMPERATURE,
        num_predict=JUDGE_MAX_TOKENS,
    )
    if not raw:
        raise RuntimeError("Empty response from judge LLM")

    parsed = _extract_json(str(raw))
    score_raw = parsed.get("score")
    if not isinstance(score_raw, (int, float)):
        raise ValueError(f"Judge returned non-numeric score: {parsed!r}")
    score = max(0.0, min(1.0, float(score_raw)))

    # Trust the judge's `passed` if it's a bool; combine with threshold so
    # a single rule governs final verdicts.
    passed_raw = parsed.get("passed")
    if isinstance(passed_raw, bool):
        passed = passed_raw and score >= pass_threshold
    else:
        passed = score >= pass_threshold

    reason = str(parsed.get("reason", "")).strip() or "(no reason given)"
    return JudgeVerdict(score=score, passed=passed, reason=reason)


# ---------------------------------------------------------------------------
# Helpers for evaluator/task callbacks
# ---------------------------------------------------------------------------

def _safe_get(d: Any, key: str, default: str = "") -> str:
    if isinstance(d, dict):
        v = d.get(key, default)
        return str(v) if v is not None else default
    return default


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run_regression(args: argparse.Namespace) -> int:
    lf = Langfuse()
    if not lf.auth_check():
        print("FATAL: Langfuse auth_check() returned False — check "
              "LANGFUSE_HOST / LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY",
              file=sys.stderr)
        return 2

    print(f"[regression] dataset={args.dataset} provider={args.provider} "
          f"judge={args.judge_provider}/{args.judge_model} "
          f"threshold={args.pass_threshold}")

    dataset = lf.get_dataset(args.dataset)
    if not dataset.items:
        print(f"FATAL: dataset '{args.dataset}' has 0 items", file=sys.stderr)
        return 2

    run_name = args.run_name or (
        f"regression-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    run_metadata = {
        "provider": args.provider,
        "judge_model": args.judge_model,
        "judge_provider": args.judge_provider,
        "pass_threshold": str(args.pass_threshold),
    }

    # Track per-item verdicts so we can render a summary after run_experiment
    # returns. dict keyed by item_id; populated inside the evaluator closure.
    verdict_log: dict[str, dict[str, Any]] = {}

    def task(*, item, **_kwargs) -> dict[str, Any]:
        """Run the chatbot on a single dataset item.

        Returns a dict so the trace's `output` field is structured.
        Errors are caught and surfaced as `{error: ..., answer: ""}`
        so the evaluator can score them as fails rather than aborting
        the whole experiment.
        """
        item_input = item.input or {}
        prompt = build_user_prompt(item_input)
        t0 = time.time()
        try:
            answer = run_chatbot(prompt, args.provider)
            return {
                "answer": answer,
                "latency_s": round(time.time() - t0, 3),
                "error": None,
            }
        except Exception as exc:
            print(f"[task] item={getattr(item, 'id', '?')} chatbot failed: {exc}",
                  file=sys.stderr)
            return {
                "answer": "",
                "latency_s": round(time.time() - t0, 3),
                "error": f"chatbot call failed: {exc}",
            }

    def evaluator(*, input, output, expected_output, metadata=None, **_kwargs):
        """Judge a single item's output against its behavior contract.

        Returns a list of two Evaluation objects: a numeric
        `contract_satisfaction` score and a boolean `passed`. Langfuse
        will create matching scores on the dataset run automatically.

        The SDK doesn't pass item_id to evaluators (only input / output /
        expected_output / item.metadata), so we key the verdict log on
        the question text — unique for our regression seeds.
        """
        question = _safe_get(input, "question", "")
        contract = _safe_get(expected_output, "behavior_contract", "")
        failure_mode = _safe_get(expected_output, "failure_mode", "(unspecified)")
        answer = (output or {}).get("answer", "") if isinstance(output, dict) else ""
        task_error = (output or {}).get("error") if isinstance(output, dict) else None

        if task_error or not answer:
            verdict = JudgeVerdict(
                score=0.0, passed=False,
                reason=task_error or "empty answer",
            )
        else:
            try:
                verdict = judge_answer(
                    question=question,
                    behavior_contract=contract,
                    failure_mode=failure_mode,
                    answer=answer,
                    judge_provider=args.judge_provider,
                    judge_model=args.judge_model,
                    pass_threshold=args.pass_threshold,
                )
            except Exception as exc:
                print(f"[evaluator] judge failed for question={question[:60]!r}: {exc}",
                      file=sys.stderr)
                verdict = JudgeVerdict(
                    score=0.0, passed=False,
                    reason=f"judge call failed: {exc}",
                )

        # Stash for summary rendering — key on question (unique per seed).
        verdict_log[question] = {
            "question": question,
            "failure_mode": failure_mode,
            "answer": answer,
            "verdict": verdict,
            "task_error": task_error,
        }

        # Trim reason for storage; keep full reason in metadata.
        short_reason = verdict.reason[:240]
        return [
            Evaluation(
                name="contract_satisfaction",
                value=verdict.score,
                comment=short_reason,
                data_type="NUMERIC",
                metadata={
                    "failure_mode": failure_mode,
                    "judge_model": args.judge_model,
                    "passed": verdict.passed,
                },
            ),
            Evaluation(
                name="passed",
                value=bool(verdict.passed),
                comment=short_reason,
                data_type="BOOLEAN",
                metadata={"score": verdict.score},
            ),
        ]

    # ---- run -------------------------------------------------------------
    print(f"[regression] running {len(dataset.items)} items as run='{run_name}' "
          f"(max_concurrency={args.max_concurrency})")
    result = dataset.run_experiment(
        name="grid-chatbot-regression",
        run_name=run_name,
        description=f"GRID chatbot regression — provider={args.provider}",
        task=task,
        evaluators=[evaluator],
        max_concurrency=args.max_concurrency,
        metadata=run_metadata,
    )
    lf.flush()

    # ----- summary --------------------------------------------------------
    print("\n" + "=" * 110)
    print(f"REGRESSION SUMMARY  dataset={args.dataset}  run={run_name}")
    if getattr(result, "dataset_run_url", None):
        print(f"Langfuse URL: {result.dataset_run_url}")
    print("=" * 110)
    header = (f"{'#':>2}  {'item_id':<10}  {'failure_mode':<28}  "
              f"{'score':>5}  {'pass':<5}  reason")
    print(header)
    print("-" * len(header))

    n_pass = 0
    n_total = 0
    for i, item_result in enumerate(result.item_results, start=1):
        n_total += 1
        item = item_result.item
        item_id = getattr(item, "id", None) or "?"
        # Lookup by question text (the key we used inside the evaluator).
        item_input = getattr(item, "input", None) or {}
        question = _safe_get(item_input, "question", "")
        log_entry = verdict_log.get(question, {})
        verdict: JudgeVerdict = log_entry.get(
            "verdict",
            JudgeVerdict(score=0.0, passed=False, reason="no verdict recorded"),
        )
        failure_mode = log_entry.get("failure_mode") or _safe_get(
            getattr(item, "expected_output", None) or {},
            "failure_mode", "?",
        )
        if verdict.passed:
            n_pass += 1
        marker = "PASS" if verdict.passed else "FAIL"
        reason = verdict.reason[:200].replace("\n", " ")
        print(f"{i:>2}  {item_id[:8]:<10}  {failure_mode[:28]:<28}  "
              f"{verdict.score:>5.2f}  {marker:<5}  {reason}")

    rate = (n_pass / n_total * 100.0) if n_total else 0.0
    print("-" * len(header))
    print(f"Pass rate: {n_pass}/{n_total} ({rate:.0f}%)")
    print(f"Run name : {run_name}")
    return 0 if (n_total > 0 and n_pass == n_total) else 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the GRID chatbot regression eval against a Langfuse dataset.",
    )
    p.add_argument("--dataset", default=DEFAULT_DATASET,
                   help=f"Langfuse dataset name (default: {DEFAULT_DATASET})")
    p.add_argument("--provider", default=DEFAULT_PROVIDER,
                   help=f"LLM provider for the chatbot (default: {DEFAULT_PROVIDER}). "
                        "Same value passed to llm.router.get_llm(provider=...).")
    p.add_argument("--judge-provider", default=DEFAULT_PROVIDER,
                   help="LLM provider for the judge. Default mirrors --provider; "
                        "swap to 'anthropic' once ANTHROPIC_API_KEY is in .env.")
    p.add_argument("--judge-model", default=DEFAULT_JUDGE_MODEL,
                   help=f"Model ID passed to the judge client (default: {DEFAULT_JUDGE_MODEL})")
    p.add_argument("--run-name", default=None,
                   help="Override Langfuse dataset run name "
                        "(default: regression-<UTC ISO>)")
    p.add_argument("--pass-threshold", type=float, default=DEFAULT_PASS_THRESHOLD,
                   help=f"Min judge score to count as pass (default: {DEFAULT_PASS_THRESHOLD})")
    p.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY,
                   help=f"Concurrent items processed (default: {DEFAULT_MAX_CONCURRENCY})")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not (0.0 <= args.pass_threshold <= 1.0):
        print(f"FATAL: --pass-threshold must be in [0, 1], got {args.pass_threshold}",
              file=sys.stderr)
        return 2
    if args.max_concurrency < 1:
        print(f"FATAL: --max-concurrency must be >= 1, got {args.max_concurrency}",
              file=sys.stderr)
        return 2
    try:
        return run_regression(args)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
