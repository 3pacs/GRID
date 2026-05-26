"""Throughput (tok/sec) and quality measurement for a single endpoint.

Both measurements hit the same OpenAI-compatible ``/v1/chat/completions``
endpoint so they work uniformly across llama.cpp and Ollama servers.

- ``measure_throughput`` runs a small set of generation prompts and reports
  tokens/second. It prefers llama.cpp's own ``timings.predicted_per_second``
  when present (most accurate, excludes prompt processing), else falls back
  to ``completion_tokens / wall_time``.
- ``measure_quality`` runs a fixed eval set of GRID tasks with checkable
  answers and returns a score in [0, 1].

Network calls are wrapped so a flaky endpoint yields a zero/empty result
rather than raising — the loop treats unreachable configs as worst-case.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import requests
from loguru import logger as log

EVAL_PATH = Path(__file__).parent / "evals" / "grid_eval.jsonl"

# (connect, read) timeouts. Cold model loads on slow nodes (e.g. a 17 GB GGUF
# off a spinning disk on Pascal) can take minutes, so the warm-up read timeout
# is generous; per-request reads on an already-warm model are bounded tighter.
WARMUP_TIMEOUT: tuple[float, float] = (15.0, 600.0)
DEFAULT_TIMEOUT: tuple[float, float] = (15.0, 180.0)

# Default throughput probes — short, deterministic, generation-heavy.
DEFAULT_THROUGHPUT_PROMPTS: tuple[str, ...] = (
    "List the 12 months of the year, one per line.",
    "Count from 1 to 40 separated by commas.",
    "Write three sentences explaining what an interest rate is.",
)


@dataclass(frozen=True)
class ThroughputResult:
    """Outcome of a throughput probe.

    Attributes:
        tok_per_sec: Median tokens/second across probes (0.0 on failure).
        samples: Per-probe tok/sec values.
        reachable: Whether the endpoint responded at all.
        source: ``timings`` (server-reported) or ``wallclock``.
    """

    tok_per_sec: float
    samples: list[float] = field(default_factory=list)
    reachable: bool = False
    source: str = "wallclock"


@dataclass(frozen=True)
class QualityResult:
    """Outcome of a quality eval.

    Attributes:
        score: Fraction of *answered* cases passed, in [0, 1] (passed/answered)
            — timeouts are excluded so a slow endpoint isn't scored as wrong.
        passed: Number of cases passed.
        total: Number of cases in the eval set.
        reachable: Whether the endpoint answered at least one case.
        answered: Number of cases that returned a gradeable response
            (``total - answered`` = unmeasured timeouts/errors). Coverage =
            ``answered / total``; a low value means the score is unreliable.
    """

    score: float
    passed: int
    total: int
    reachable: bool = False
    answered: int = 0


def _post_chat(
    base_url: str,
    model: str,
    messages: list[dict[str, str]],
    *,
    max_tokens: int,
    temperature: float,
    timeout: float | tuple[float, float],
) -> tuple[dict[str, Any] | None, str | None]:
    """POST a chat completion.

    Returns ``(data, error)``: ``error`` is None on success, else one of
    ``"timeout"`` (slow/cold endpoint — *not* a quality signal), ``"http_<n>"``
    (server rejected the request), or ``"unreachable"`` (connection failed).
    """
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "stream": False,
        # Disable chain-of-thought for eval/throughput. Qwen3.6's chat template
        # emits <think>…</think> unless told otherwise, which blows the token
        # budget and breaks exact-match grading (the answer never arrives or is
        # buried in reasoning). Non-thinking models ignore this field.
        "chat_template_kwargs": {"enable_thinking": False},
    }
    try:
        resp = requests.post(
            f"{base_url}/v1/chat/completions",
            json=payload,
            timeout=timeout,
        )
        if resp.status_code >= 400:
            log.debug("bench chat {s} @ {u}", s=resp.status_code, u=base_url)
            return None, f"http_{resp.status_code}"
        return resp.json(), None
    except requests.exceptions.Timeout:
        log.debug("bench chat timeout @ {u}", u=base_url)
        return None, "timeout"
    except Exception as exc:
        log.debug("bench chat failed @ {u}: {e}", u=base_url, e=str(exc))
        return None, "unreachable"


def warm_up(
    base_url: str,
    model: str,
    *,
    timeout: float | tuple[float, float] = WARMUP_TIMEOUT,
) -> bool:
    """Load the model into VRAM with one tiny request before measuring.

    Slow/cold endpoints otherwise time out the first real eval case, which
    corrupts the score. Returns True if the endpoint answered (warm). A long
    read timeout absorbs multi-minute cold loads off slow storage.
    """
    _data, err = _post_chat(
        base_url, model, [{"role": "user", "content": "ready?"}],
        max_tokens=1, temperature=0.0, timeout=timeout,
    )
    return err is None


def _completion_tokens(data: dict[str, Any]) -> int:
    usage = data.get("usage") or {}
    return int(usage.get("completion_tokens") or 0)


_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_think(text: str) -> str:
    """Remove reasoning so grading sees only the final answer.

    Belt-and-suspenders for endpoints that don't honor ``enable_thinking``:
    drops paired ``<think>…</think>`` blocks, and if a dangling ``</think>``
    remains (truncated/unclosed reasoning) keeps only what follows it.
    """
    text = _THINK_RE.sub("", text)
    if "</think>" in text:
        text = text.rsplit("</think>", 1)[-1]
    return text


def _content(data: dict[str, Any]) -> str:
    try:
        msg = data["choices"][0]["message"]
        return _strip_think(msg.get("content") or "").strip()
    except Exception:
        return ""


def measure_throughput(
    base_url: str,
    model: str,
    *,
    prompts: tuple[str, ...] = DEFAULT_THROUGHPUT_PROMPTS,
    max_tokens: int = 160,
    timeout: float | tuple[float, float] = DEFAULT_TIMEOUT,
    warm: bool = True,
) -> ThroughputResult:
    """Measure generation throughput (tok/sec) for an endpoint."""
    if warm:
        warm_up(base_url, model)
    samples: list[float] = []
    source = "wallclock"
    reachable = False
    for prompt in prompts:
        start = time.monotonic()
        data, _err = _post_chat(
            base_url, model,
            [{"role": "user", "content": prompt}],
            max_tokens=max_tokens, temperature=0.0, timeout=timeout,
        )
        elapsed = time.monotonic() - start
        if data is None:
            continue
        reachable = True
        # llama.cpp returns a `timings` block with predicted_per_second.
        timings = data.get("timings") or {}
        pps = timings.get("predicted_per_second")
        if isinstance(pps, (int, float)) and pps > 0:
            samples.append(float(pps))
            source = "timings"
            continue
        gen = _completion_tokens(data)
        if gen > 0 and elapsed > 0:
            samples.append(gen / elapsed)

    if not samples:
        return ThroughputResult(0.0, [], reachable, source)
    ordered = sorted(samples)
    median = ordered[len(ordered) // 2]
    return ThroughputResult(round(median, 2), [round(s, 2) for s in samples], reachable, source)


def load_eval_cases(path: Path = EVAL_PATH) -> list[dict[str, Any]]:
    """Load the fixed GRID quality eval set (JSONL)."""
    cases: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("//"):
                cases.append(json.loads(line))
    except Exception as exc:
        log.warning("Could not load eval cases from {p}: {e}", p=path, e=str(exc))
    return cases


def _grade(case: dict[str, Any], answer: str) -> bool:
    """Grade one answer against a case spec.

    Supported check types:
        contains      — every string in ``expect`` appears (case-insensitive)
        contains_any  — at least one string in ``expect`` appears
        regex         — ``expect`` regex matches
        json_keys     — answer parses as JSON and contains all ``expect`` keys
    """
    check = case.get("check", "contains")
    expect = case.get("expect", [])
    low = answer.lower()
    if check == "contains":
        return all(str(e).lower() in low for e in expect)
    if check == "contains_any":
        return any(str(e).lower() in low for e in expect)
    if check == "regex":
        return re.search(str(expect), answer, re.IGNORECASE | re.DOTALL) is not None
    if check == "json_keys":
        try:
            start, end = answer.find("{"), answer.rfind("}")
            obj = json.loads(answer[start : end + 1])
        except Exception:
            return False
        return all(k in obj for k in expect)
    return False


def measure_quality(
    base_url: str,
    model: str,
    *,
    cases: list[dict[str, Any]] | None = None,
    max_tokens: int = 512,
    timeout: float | tuple[float, float] = DEFAULT_TIMEOUT,
    grader: Callable[[dict[str, Any], str], bool] = _grade,
    warm: bool = True,
) -> QualityResult:
    """Run the quality eval set against an endpoint.

    Score is ``passed / answered`` — cases that time out (or otherwise return
    no content) are *unmeasured*, not counted as failures, so a slow endpoint
    isn't mistaken for a low-quality one. ``answered`` exposes coverage.
    """
    cases = cases if cases is not None else load_eval_cases()
    if not cases:
        return QualityResult(0.0, 0, 0, False, 0)

    if warm:
        warm_up(base_url, model)

    passed = 0
    answered = 0
    for case in cases:
        messages: list[dict[str, str]] = []
        if case.get("system"):
            messages.append({"role": "system", "content": case["system"]})
        messages.append({"role": "user", "content": case["prompt"]})
        data, _err = _post_chat(
            base_url, model, messages,
            max_tokens=max_tokens,
            temperature=0.0,
            timeout=timeout,
        )
        if data is None:
            continue  # timeout / unreachable / http error -> unmeasured
        answered += 1
        if grader(case, _content(data)):
            passed += 1

    total = len(cases)
    score = passed / answered if answered else 0.0
    return QualityResult(round(score, 4), passed, total, answered > 0, answered)
