"""LLM autoresearch — multi-objective (quality + tok/sec) tuning loop.

Karpathy-style keep-or-discard search applied to GRID's LLM fleet. Measures
generation throughput and quality (against a fixed GRID eval set) for each
endpoint, and searches for the fastest serving config that still clears a
hard quality floor. A low-quality LLM is rejected outright — speed never
buys its way past the quality gate.

See ``scripts/run_llm_autoresearch.py`` for the CLI entry point.
"""

from __future__ import annotations

from llm.autoresearch.bench import (
    QualityResult,
    ThroughputResult,
    measure_quality,
    measure_throughput,
)
from llm.autoresearch.hosts import HOST_PROFILES, HostProfile, ModelSpec, fits_on
from llm.autoresearch.loop import (
    AutoResearchLoop,
    ConfigApplier,
    RunningEndpointApplier,
    TrialConfig,
    TrialResult,
    compute_fitness,
)
from llm.autoresearch.registry import (
    Endpoint,
    assess_model,
    discover_endpoints,
    eligible_endpoints,
)

__all__ = [
    "AutoResearchLoop",
    "ConfigApplier",
    "Endpoint",
    "HOST_PROFILES",
    "HostProfile",
    "ModelSpec",
    "QualityResult",
    "RunningEndpointApplier",
    "ThroughputResult",
    "TrialConfig",
    "TrialResult",
    "assess_model",
    "compute_fitness",
    "discover_endpoints",
    "eligible_endpoints",
    "fits_on",
    "measure_quality",
    "measure_throughput",
]
