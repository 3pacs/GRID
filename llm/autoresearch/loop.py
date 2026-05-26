"""Autoresearch keep-or-discard loop with a hard quality gate.

Mirrors the Karpathy *autoresearch* pattern (propose a change, run a
time-boxed trial, measure a fitness signal, keep or discard, repeat) but
with two objectives instead of one:

    1. quality  — fraction of the GRID eval set passed  (HARD GATE)
    2. tok/sec  — generation throughput                  (maximize)

Operator rule baked in: *a low-quality LLM does more harm than good*. Any
trial whose quality falls below ``quality_floor`` is rejected outright, no
matter how fast it is. Among trials that clear the floor, throughput is the
objective, with quality as the tie-break. A Pareto front of non-dominated
(quality, tok/sec) configs is also tracked so nothing good is lost.

Every trial is appended to a JSONL journal (immutable, append-only) so an
overnight run leaves a full audit trail. The applier that actually changes
the served config is pluggable: the default just measures the currently
running endpoint (safe baseline); a fleet-specific applier restarts
``llama-server`` / re-pulls an Ollama model to realize a candidate config.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from loguru import logger as log

from llm.autoresearch.bench import (
    QualityResult,
    ThroughputResult,
    measure_quality,
    measure_throughput,
)

RUNS_DIR = Path(__file__).parent / "runs"

# Minimum fraction of eval cases that must return a gradeable answer before a
# quality score is treated as a real verdict. Below this, the endpoint was too
# slow/cold to measure — we report "unmeasured" rather than rejecting it as
# low-quality (which is what corrupted the early panda baselines).
MIN_COVERAGE = 0.5


@dataclass(frozen=True)
class TrialConfig:
    """A candidate serving configuration to evaluate.

    ``flags`` holds runtime knobs an applier may realize (draft model for
    speculative decoding, kv-cache type, n_gpu_layers, flash-attn, etc.).
    """

    endpoint: str
    base_url: str
    model: str
    host: str = ""
    flags: dict[str, Any] = field(default_factory=dict)

    def key(self) -> str:
        return f"{self.endpoint}|{self.model}|{json.dumps(self.flags, sort_keys=True)}"


@dataclass(frozen=True)
class TrialResult:
    """Measured outcome of one trial."""

    config: TrialConfig
    quality: float
    tok_per_sec: float
    quality_passed: int
    quality_total: int
    reachable: bool
    accepted: bool
    is_champion: bool
    fitness: float
    note: str
    ts: str
    quality_answered: int = 0

    def to_journal(self) -> dict[str, Any]:
        d = asdict(self)
        return d


class ConfigApplier(Protocol):
    """Realizes a TrialConfig on the fleet, returning the live endpoint.

    Implementations restart llama-server with new flags, pull/load a new
    model, etc. Must return ``(base_url, model)`` to benchmark, or raise to
    signal the config could not be applied (treated as a failed trial).
    """

    def apply(self, config: TrialConfig) -> tuple[str, str]: ...


class RunningEndpointApplier:
    """Default applier: measure the endpoint exactly as it runs now.

    Changes nothing on the fleet — used for baseline measurement and for
    CI/tests. Safe everywhere.
    """

    def apply(self, config: TrialConfig) -> tuple[str, str]:
        return config.base_url, config.model


def _dominates(a: TrialResult, b: TrialResult) -> bool:
    """True if ``a`` Pareto-dominates ``b`` on (quality, tok/sec)."""
    return (
        a.quality >= b.quality
        and a.tok_per_sec >= b.tok_per_sec
        and (a.quality > b.quality or a.tok_per_sec > b.tok_per_sec)
    )


def compute_fitness(quality: float, tok_per_sec: float, quality_floor: float) -> float:
    """Scalar fitness with a hard quality gate.

    Below the floor -> -inf (rejected). Otherwise throughput is the score
    with a small bonus for quality above the floor so a faster-but-equal
    config wins and quality breaks ties.
    """
    if quality < quality_floor:
        return float("-inf")
    quality_bonus = (quality - quality_floor) * 1000.0
    return tok_per_sec + quality_bonus


@dataclass
class AutoResearchLoop:
    """Drives the keep-or-discard search over serving configs.

    Parameters:
        quality_floor: Minimum eval score to accept a config (hard gate).
        applier: Realizes a config on the fleet (default: measure as-is).
        throughput_fn / quality_fn: Measurement callables (injectable for tests).
        journal_path: Where to append trial records (JSONL).
        champion_margin: tok/sec must beat the champion by this fraction to
            take the crown (avoids churn on noise).
    """

    quality_floor: float = 0.6
    applier: ConfigApplier = field(default_factory=RunningEndpointApplier)
    throughput_fn: Callable[..., ThroughputResult] = measure_throughput
    quality_fn: Callable[..., QualityResult] = measure_quality
    journal_path: Path | None = None
    champion_margin: float = 0.02

    champion: TrialResult | None = field(default=None, init=False)
    pareto: list[TrialResult] = field(default_factory=list, init=False)
    history: list[TrialResult] = field(default_factory=list, init=False)

    def __post_init__(self) -> None:
        if self.journal_path is None:
            RUNS_DIR.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self.journal_path = RUNS_DIR / f"autoresearch-{stamp}.jsonl"

    def _journal(self, result: TrialResult) -> None:
        try:
            with open(self.journal_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(result.to_journal()) + "\n")
        except Exception as exc:
            log.warning("Could not append autoresearch journal: {e}", e=str(exc))

    def _update_pareto(self, result: TrialResult) -> None:
        if not result.accepted:
            return
        if any(_dominates(p, result) for p in self.pareto):
            return
        self.pareto = [p for p in self.pareto if not _dominates(result, p)]
        self.pareto.append(result)

    def evaluate(self, config: TrialConfig) -> TrialResult:
        """Apply, benchmark, grade, journal, and update champion/Pareto."""
        ts = datetime.now(timezone.utc).isoformat()
        try:
            base_url, model = self.applier.apply(config)
        except Exception as exc:
            log.warning("apply failed for {k}: {e}", k=config.key(), e=str(exc))
            result = TrialResult(
                config=config, quality=0.0, tok_per_sec=0.0,
                quality_passed=0, quality_total=0, reachable=False,
                accepted=False, is_champion=False, fitness=float("-inf"),
                note=f"apply failed: {exc}", ts=ts,
            )
            self.history.append(result)
            self._journal(result)
            return result

        q = self.quality_fn(base_url, model)
        t = self.throughput_fn(base_url, model)
        reachable = q.reachable or t.reachable

        # Coverage gate: how much of the eval actually returned an answer.
        # `answered == 0` (older/injected QualityResults) means "unknown" — treat
        # as fully covered so existing callers/tests behave as before.
        answered = q.answered if q.answered else q.total
        coverage = (answered / q.total) if q.total else 1.0
        measured = reachable and coverage >= MIN_COVERAGE

        fitness = compute_fitness(q.score, t.tok_per_sec, self.quality_floor) if measured else float("-inf")
        accepted = measured and q.score >= self.quality_floor

        if not reachable:
            note = "endpoint unreachable"
        elif not measured:
            note = (f"unmeasured — only {answered}/{q.total} eval cases answered "
                    f"(coverage {coverage:.0%} < {MIN_COVERAGE:.0%}); endpoint too slow/cold, not a quality verdict")
        elif not accepted:
            note = f"quality {q.score:.2f} < floor {self.quality_floor:.2f} — rejected (a weak LLM does more harm than good)"
        else:
            note = f"ok — quality {q.score:.2f}, {t.tok_per_sec:.1f} tok/s ({t.source})"

        result = TrialResult(
            config=config, quality=q.score, tok_per_sec=t.tok_per_sec,
            quality_passed=q.passed, quality_total=q.total, reachable=reachable,
            accepted=accepted, is_champion=False, fitness=fitness, note=note, ts=ts,
            quality_answered=answered,
        )

        is_champion = False
        if accepted and (
            self.champion is None
            or result.tok_per_sec > self.champion.tok_per_sec * (1.0 + self.champion_margin)
            or (
                abs(result.tok_per_sec - self.champion.tok_per_sec)
                <= self.champion.tok_per_sec * self.champion_margin
                and result.quality > self.champion.quality
            )
        ):
            is_champion = True

        # Re-stamp champion flag immutably.
        result = replace(result, is_champion=is_champion)
        if is_champion:
            self.champion = result

        self.history.append(result)
        self._update_pareto(result)
        self._journal(result)
        log.info("trial {k}: {note}", k=config.key(), note=note)
        return result

    def run(
        self,
        configs: list[TrialConfig],
        *,
        budget_seconds: float | None = None,
        max_trials: int | None = None,
    ) -> TrialResult | None:
        """Evaluate configs until the list, time budget, or trial cap is hit.

        Returns the champion (best accepted config) or None if nothing
        cleared the quality floor.
        """
        start = time.monotonic()
        for i, config in enumerate(configs):
            if max_trials is not None and i >= max_trials:
                break
            if budget_seconds is not None and (time.monotonic() - start) >= budget_seconds:
                log.info("autoresearch budget exhausted after {n} trials", n=i)
                break
            self.evaluate(config)
        return self.champion

    def best_config(self) -> dict[str, Any] | None:
        """Return the champion config as a serializable dict, or None."""
        if self.champion is None:
            return None
        return {
            "endpoint": self.champion.config.endpoint,
            "model": self.champion.config.model,
            "host": self.champion.config.host,
            "flags": self.champion.config.flags,
            "quality": self.champion.quality,
            "tok_per_sec": self.champion.tok_per_sec,
        }
