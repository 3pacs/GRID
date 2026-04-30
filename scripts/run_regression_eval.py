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

try:
    # Direct OpenAI-compatible client for arm B (OpenRouter -> Opus).
    from llm.router import OpenAIClient as _OpenAIClient
except Exception as exc:
    print(f"FATAL: cannot import OpenAIClient from llm.router: {exc}",
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

# A/B arms: arm A is the primary (provider-driven, defaults to OpenAI). Arm
# B routes through OpenRouter to Claude Opus, mirroring the live chat A/B.
VALID_ARMS = ("A", "B", "both")
ARM_B_MODEL = "anthropic/claude-opus-4"
ARM_B_BASE_URL = "https://openrouter.ai/api/v1"

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


def _make_arm_b_client() -> Any:
    """Build the OpenRouter Opus client used for arm B.

    Reads OPENROUTER_API_KEY from env (falls back to settings if available).
    Raises RuntimeError when the key is missing — the caller decides whether
    that fails the whole run or downgrades to arm-A-only.
    """
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        try:
            from config import settings
            api_key = getattr(settings, "OPENROUTER_API_KEY", "") or ""
        except Exception:
            pass
    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY missing — arm B (OpenRouter -> Opus) cannot run"
        )
    return _OpenAIClient(
        api_key=api_key,
        base_url=ARM_B_BASE_URL,
        model=ARM_B_MODEL,
        timeout=120,
    )


def run_chatbot(question_prompt: str, provider: str, *, arm: str = "A") -> str:
    """Invoke the GRID LLM the same way chat.py would, for the given arm.

    Arm A: routes via `get_llm(provider=...)`, mirroring the chat router's
        primary path.
    Arm B: routes directly through OpenRouter to Claude Opus, mirroring the
        background A/B comparison call in chat.py.

    Raises if the client is unavailable or returns nothing — we never
    silently swallow.
    """
    if arm == "B":
        client = _make_arm_b_client()
        client_label = f"openrouter/{ARM_B_MODEL}"
    else:
        client = get_llm(provider=provider)
        client_label = f"provider={provider}"

    if not getattr(client, "is_available", False):
        raise RuntimeError(f"Arm {arm} client ({client_label}) reports is_available=False")

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
        raise RuntimeError(f"Empty response from arm {arm} client ({client_label})")
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

def _run_one_arm(
    *,
    arm: str,
    args: argparse.Namespace,
    dataset: Any,
    base_run_name: str,
) -> dict[str, Any]:
    """Run the regression set for a single arm and return a result bundle.

    Returns:
        {
            "arm": "A" | "B",
            "run_name": str,
            "url": str | None,
            "verdicts": dict[item_id, {question, failure_mode, verdict, error}],
            "skipped": bool,
            "skip_reason": str | None,
        }
    """
    # Probe arm B early — bail gracefully if no key, no rate limit issue yet.
    if arm == "B":
        try:
            _make_arm_b_client()  # raises if no key
        except Exception as exc:
            print(f"[arm B] SKIPPED — {exc}", file=sys.stderr)
            return {
                "arm": "B",
                "run_name": "",
                "url": None,
                "verdicts": {},
                "skipped": True,
                "skip_reason": str(exc),
            }

    run_name = f"{base_run_name}-arm{arm}"
    arm_label = "A" if arm == "A" else "B"
    arm_provider_desc = (
        f"provider={args.provider}" if arm == "A" else f"openrouter/{ARM_B_MODEL}"
    )
    run_metadata = {
        "provider": args.provider if arm == "A" else f"openrouter/{ARM_B_MODEL}",
        "judge_model": args.judge_model,
        "judge_provider": args.judge_provider,
        "pass_threshold": str(args.pass_threshold),
        "ab_arm": arm_label,
    }

    # Track per-item verdicts so we can render a summary after run_experiment
    # returns. dict keyed by question text (unique per seed).
    verdict_log: dict[str, dict[str, Any]] = {}

    def task(*, item, **_kwargs) -> dict[str, Any]:
        """Run the chatbot on a single dataset item for this arm."""
        item_input = item.input or {}
        prompt = build_user_prompt(item_input)
        t0 = time.time()
        try:
            answer = run_chatbot(prompt, args.provider, arm=arm)
            return {
                "answer": answer,
                "latency_s": round(time.time() - t0, 3),
                "error": None,
                "ab_arm": arm_label,
            }
        except Exception as exc:
            print(f"[task arm={arm}] item={getattr(item, 'id', '?')} chatbot failed: {exc}",
                  file=sys.stderr)
            return {
                "answer": "",
                "latency_s": round(time.time() - t0, 3),
                "error": f"chatbot call failed: {exc}",
                "ab_arm": arm_label,
            }

    def evaluator(*, input, output, expected_output, metadata=None, **_kwargs):
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
                print(f"[evaluator arm={arm}] judge failed for question={question[:60]!r}: {exc}",
                      file=sys.stderr)
                verdict = JudgeVerdict(
                    score=0.0, passed=False,
                    reason=f"judge call failed: {exc}",
                )

        verdict_log[question] = {
            "question": question,
            "failure_mode": failure_mode,
            "answer": answer,
            "verdict": verdict,
            "task_error": task_error,
        }

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
                    "ab_arm": arm_label,
                },
            ),
            Evaluation(
                name="passed",
                value=bool(verdict.passed),
                comment=short_reason,
                data_type="BOOLEAN",
                metadata={"score": verdict.score, "ab_arm": arm_label},
            ),
        ]

    print(f"[arm {arm}] running {len(dataset.items)} items as run='{run_name}' "
          f"({arm_provider_desc}, max_concurrency={args.max_concurrency})")
    result = dataset.run_experiment(
        name=f"grid-chatbot-regression-arm{arm}",
        run_name=run_name,
        description=f"GRID chatbot regression — arm {arm} ({arm_provider_desc})",
        task=task,
        evaluators=[evaluator],
        max_concurrency=args.max_concurrency,
        metadata=run_metadata,
    )

    return {
        "arm": arm_label,
        "run_name": run_name,
        "url": getattr(result, "dataset_run_url", None),
        "verdicts": verdict_log,
        "item_results": list(result.item_results),
        "skipped": False,
        "skip_reason": None,
    }


def _print_arm_summary(bundle: dict[str, Any]) -> tuple[int, int]:
    """Print a per-item summary table for one arm. Returns (n_pass, n_total)."""
    arm = bundle["arm"]
    run_name = bundle["run_name"]
    print("\n" + "=" * 110)
    print(f"REGRESSION SUMMARY  arm={arm}  run={run_name}")
    if bundle.get("url"):
        print(f"Langfuse URL: {bundle['url']}")
    print("=" * 110)
    header = (f"{'#':>2}  {'item_id':<10}  {'failure_mode':<28}  "
              f"{'score':>5}  {'pass':<5}  reason")
    print(header)
    print("-" * len(header))

    n_pass = 0
    n_total = 0
    for i, item_result in enumerate(bundle.get("item_results", []), start=1):
        n_total += 1
        item = item_result.item
        item_id = getattr(item, "id", None) or "?"
        item_input = getattr(item, "input", None) or {}
        question = _safe_get(item_input, "question", "")
        log_entry = bundle["verdicts"].get(question, {})
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
    print(f"Arm {arm} pass rate: {n_pass}/{n_total} ({rate:.0f}%)  "
          f"items={n_total}  run='{run_name}'")
    return n_pass, n_total


def _print_ab_comparison(
    bundle_a: dict[str, Any] | None,
    bundle_b: dict[str, Any] | None,
) -> None:
    """Print side-by-side A vs B per-failure_mode pass rates and winners."""
    if not bundle_a or not bundle_b:
        return
    if bundle_a.get("skipped") or bundle_b.get("skipped"):
        return

    # Group by failure_mode using question key from each verdict log.
    def _by_mode(bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for q, entry in bundle["verdicts"].items():
            mode = entry.get("failure_mode") or "(unspecified)"
            slot = out.setdefault(mode, {"pass": 0, "total": 0, "questions": []})
            slot["total"] += 1
            slot["questions"].append(q)
            if entry.get("verdict") and entry["verdict"].passed:
                slot["pass"] += 1
        return out

    a_modes = _by_mode(bundle_a)
    b_modes = _by_mode(bundle_b)
    all_modes = sorted(set(a_modes.keys()) | set(b_modes.keys()))

    print("\n" + "=" * 110)
    print("A vs B  side-by-side  (pass rate per failure_mode)")
    print("=" * 110)
    header = (f"{'failure_mode':<32}  {'A pass':>8}  {'B pass':>8}  "
              f"{'A %':>5}  {'B %':>5}  winner")
    print(header)
    print("-" * len(header))

    a_wins = 0
    b_wins = 0
    ties = 0
    for mode in all_modes:
        a = a_modes.get(mode, {"pass": 0, "total": 0})
        b = b_modes.get(mode, {"pass": 0, "total": 0})
        a_pct = (a["pass"] / a["total"] * 100.0) if a["total"] else 0.0
        b_pct = (b["pass"] / b["total"] * 100.0) if b["total"] else 0.0
        if a["pass"] > b["pass"]:
            winner = "A"; a_wins += 1
        elif b["pass"] > a["pass"]:
            winner = "B"; b_wins += 1
        else:
            winner = "tie"; ties += 1
        print(f"{mode[:32]:<32}  {a['pass']:>3}/{a['total']:<3}  "
              f"{b['pass']:>3}/{b['total']:<3}  "
              f"{a_pct:>5.0f}  {b_pct:>5.0f}  {winner}")
    print("-" * len(header))
    print(f"Failure modes won — A: {a_wins}  B: {b_wins}  ties: {ties}")


def run_regression(args: argparse.Namespace) -> int:
    lf = Langfuse()
    if not lf.auth_check():
        print("FATAL: Langfuse auth_check() returned False — check "
              "LANGFUSE_HOST / LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY",
              file=sys.stderr)
        return 2

    arm = args.arm
    if arm not in VALID_ARMS:
        print(f"FATAL: --arm must be one of {VALID_ARMS}, got {arm!r}",
              file=sys.stderr)
        return 2

    print(f"[regression] dataset={args.dataset} provider={args.provider} "
          f"arm={arm} judge={args.judge_provider}/{args.judge_model} "
          f"threshold={args.pass_threshold}")

    dataset = lf.get_dataset(args.dataset)
    if not dataset.items:
        print(f"FATAL: dataset '{args.dataset}' has 0 items", file=sys.stderr)
        return 2

    base_run_name = args.run_name or (
        f"regression-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )

    arms_to_run: list[str] = ["A", "B"] if arm == "both" else [arm]
    bundles: dict[str, dict[str, Any]] = {}

    for one_arm in arms_to_run:
        bundle = _run_one_arm(
            arm=one_arm,
            args=args,
            dataset=dataset,
            base_run_name=base_run_name,
        )
        bundles[one_arm] = bundle
        lf.flush()

    # ----- per-arm summaries ---------------------------------------------
    overall_pass = 0
    overall_total = 0
    any_full_fail = False

    for one_arm in arms_to_run:
        bundle = bundles[one_arm]
        if bundle.get("skipped"):
            print(f"\n[arm {one_arm}] skipped — {bundle.get('skip_reason')}",
                  file=sys.stderr)
            continue
        n_pass, n_total = _print_arm_summary(bundle)
        overall_pass += n_pass
        overall_total += n_total
        if n_total == 0 or n_pass < n_total:
            any_full_fail = True

    # ----- A vs B side-by-side -------------------------------------------
    if arm == "both":
        _print_ab_comparison(bundles.get("A"), bundles.get("B"))

    print("\n" + "=" * 110)
    print(f"OVERALL: {overall_pass}/{overall_total} items passed across {len(arms_to_run)} arm(s)")
    print(f"Base run name: {base_run_name}")
    return 0 if (overall_total > 0 and not any_full_fail) else 1


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
    p.add_argument("--arm", default="A", choices=list(VALID_ARMS),
                   help="A/B arm to run. 'A' = primary provider (--provider). "
                        "'B' = OpenRouter -> anthropic/claude-opus-4. "
                        "'both' = run each item against both arms and report "
                        "side-by-side pass rates. (default: A)")
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
