"""Proper scoring rules, cost model and uncertainty -- pure functions, no I/O.

Everything here is deterministic: no wall clock, no global RNG. The
bootstrap uses a counter-based splitmix64 stream seeded from a sha256 of the
group it summarises, implemented with numpy uint64 arithmetic so the draws
do not depend on numpy's Generator stream-compatibility policy.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Sequence

import numpy as np

PACKAGE = Path(__file__).resolve().parent
Z95 = 1.959963984540054


def load_json(name: str) -> dict:
    return json.loads((PACKAGE / name).read_text(encoding="utf-8"))


# --- per-prediction rules ------------------------------------------------------------


def direction_hit(side: int, ret: float) -> int | None:
    """1 if ``side * ret > 0``, 0 if ``< 0``, ``None`` for a push (ret == 0)."""
    if side not in (1, -1):
        raise ValueError(f"side must be +1 or -1, got {side!r}")
    signed = side * ret
    if signed > 0:
        return 1
    if signed < 0:
        return 0
    return None


def brier(p: float, y: int) -> float:
    _check_probability(p, y)
    return (p - y) ** 2


def log_loss(p: float, y: int, eps: float) -> float:
    _check_probability(p, y)
    q = min(max(p, eps), 1.0 - eps)
    return -(y * math.log(q) + (1 - y) * math.log(1.0 - q))


def _check_probability(p: float, y: int) -> None:
    if not (0.0 <= p <= 1.0) or not math.isfinite(p):
        raise ValueError(f"probability out of [0, 1]: {p!r}")
    if y not in (0, 1):
        raise ValueError(f"binary outcome must be 0 or 1: {y!r}")


def cost_per_side_bps(instrument_class: str, cost_model: dict) -> float | None:
    """Total per-side cost in bp for a class; ``None`` if the class is not tradable."""
    classes = cost_model["classes"]
    if instrument_class not in classes:
        raise KeyError(f"instrument class {instrument_class!r} is not in the cost model")
    spec = classes[instrument_class]
    if spec is None:
        return None
    return float(spec["half_spread_bps"]) + float(spec["commission_bps"]) + float(spec["slippage_bps"])


def net_return(side: int, entry: float, exit_: float, instrument_class: str, cost_model: dict) -> float | None:
    """``side * (exit/entry - 1) - 2 * per_side_bps / 1e4``; ``None`` if not tradable."""
    if side not in (1, -1):
        raise ValueError(f"side must be +1 or -1, got {side!r}")
    if not (entry > 0 and exit_ > 0 and math.isfinite(entry) and math.isfinite(exit_)):
        raise ValueError("entry and exit prices must be positive and finite")
    per_side = cost_per_side_bps(instrument_class, cost_model)
    if per_side is None:
        return None
    return side * (exit_ / entry - 1.0) - 2.0 * per_side / 1e4


def _average_ranks(values: Sequence[float]) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    order = np.argsort(arr, kind="mergesort")
    ranks = np.empty(len(arr), dtype=float)
    sorted_vals = arr[order]
    i = 0
    while i < len(arr):
        j = i
        while j + 1 < len(arr) and sorted_vals[j + 1] == sorted_vals[i]:
            j += 1
        ranks[order[i:j + 1]] = (i + j) / 2.0 + 1.0
        i = j + 1
    return ranks


def pearson(x: Sequence[float], y: Sequence[float]) -> float | None:
    xa, ya = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    if len(xa) != len(ya) or len(xa) < 2:
        return None
    xc, yc = xa - xa.mean(), ya - ya.mean()
    denom = math.sqrt(float(xc @ xc) * float(yc @ yc))
    if denom == 0.0:
        return None
    return float(xc @ yc) / denom


def spearman(x: Sequence[float], y: Sequence[float]) -> float | None:
    """Spearman correlation with average ranks for ties; ``None`` if undefined."""
    if len(x) != len(y) or len(x) < 2:
        return None
    return pearson(_average_ranks(x), _average_ranks(y))


# --- uncertainty -----------------------------------------------------------------------


def wilson_interval(hits: int, n: int, z: float = Z95) -> tuple[float, float] | None:
    if n <= 0:
        return None
    p = hits / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


_GOLDEN = np.uint64(0x9E3779B97F4A7C15)
_M1 = np.uint64(0xBF58476D1CE4E5B9)
_M2 = np.uint64(0x94D049BB133111EB)


def splitmix64(seed: int, count: int, offset: int = 0) -> np.ndarray:
    """Draws ``offset .. offset+count-1`` of the splitmix64 sequence started at ``seed``."""
    with np.errstate(over="ignore"):
        steps = np.arange(offset + 1, offset + count + 1, dtype=np.uint64)
        state = np.uint64(seed & 0xFFFFFFFFFFFFFFFF) + _GOLDEN * steps
        z = state
        z = (z ^ (z >> np.uint64(30))) * _M1
        z = (z ^ (z >> np.uint64(27))) * _M2
        return z ^ (z >> np.uint64(31))


def seed_for(*parts: str) -> int:
    return int.from_bytes(hashlib.sha256("|".join(parts).encode("utf-8")).digest()[:8], "big")


def bootstrap_mean_ci(values: Sequence[float], *, seed: int, n_boot: int, ci: float) -> tuple[float, float] | None:
    """Percentile bootstrap CI of the mean (resampling units with replacement)."""
    arr = np.asarray(values, dtype=float)
    n = len(arr)
    if n < 2:
        return None
    chunk = max(1, min(n_boot, 2_000_000 // n))  # bound memory for large groups
    parts = []
    for start in range(0, n_boot, chunk):
        rows = min(chunk, n_boot - start)
        draws = splitmix64(seed, rows * n, offset=start * n)
        idx = (draws % np.uint64(n)).astype(np.int64).reshape(rows, n)
        parts.append(arr[idx].mean(axis=1))
    means = np.sort(np.concatenate(parts))
    lo_q, hi_q = (1.0 - ci) / 2.0, 1.0 - (1.0 - ci) / 2.0
    # deterministic order statistics (no interpolation mode differences across numpy)
    lo = means[int(math.floor(lo_q * (n_boot - 1)))]
    hi = means[int(math.ceil(hi_q * (n_boot - 1)))]
    return (float(lo), float(hi))


def summarize(values: Sequence[float], *, kind: str, seed_parts: tuple[str, ...], aggregation: dict) -> dict:
    """n, mean and CI for one metric over one group/window.

    ``kind == "binary"`` adds hits and a Wilson interval (the binomial CI);
    every kind gets a bootstrap CI of the mean once n >= min_n_for_ci.
    """
    vals = [float(v) for v in values]
    n = len(vals)
    boot = aggregation["bootstrap"]
    out: dict = {"n": n, "mean": (sum(vals) / n) if n else None}
    if kind == "binary":
        hits = int(round(sum(vals)))
        out["hits"] = hits
        wilson = wilson_interval(hits, n)
        out["wilson_ci"] = list(wilson) if wilson else None
    if kind == "sum":
        out["sum"] = sum(vals) if n else None
    if n >= aggregation["min_n_for_ci"]:
        ci = bootstrap_mean_ci(vals, seed=seed_for(*seed_parts), n_boot=int(boot["n_boot"]), ci=float(boot["ci"]))
        out["bootstrap_ci"] = list(ci) if ci else None
    else:
        out["bootstrap_ci"] = None
    return out
