"""The pass test every look is judged by (``policy.policy_payload()['pass_test']``).

Expectancy after costs, never win rate: mean net > 0 overall and on each side,
a session-resampled percentile bootstrap lower bound at ``1 - alpha_trial`` above
zero, mean net above always-long on the same trades, and positive in at least two
of three equal time thirds. Trades on the same session are resampled together
(pooled instruments share a session's market).
"""

from __future__ import annotations

import math

import numpy as np

from . import policy as P


def bootstrap_b(alpha_trial: float) -> int:
    b = max(P.BOOT_B_MIN, math.ceil(P.BOOT_TAIL_RESAMPLES / alpha_trial))
    if b > P.BOOT_B_MAX:
        raise ValueError(f"alpha {alpha_trial:g} needs B {b} > {P.BOOT_B_MAX}: not resolvable")
    return b


def session_bootstrap_lower(sessions: list[str], values: np.ndarray, alpha: float,
                            seed: int = P.BOOT_SEED, chunk: int = 5_000) -> tuple[float, int]:
    """Percentile lower bound of the pooled mean at level ``alpha``, resampling sessions."""
    keys, inverse = np.unique(np.asarray(sessions), return_inverse=True)
    sums = np.bincount(inverse, weights=values, minlength=len(keys))
    counts = np.bincount(inverse, minlength=len(keys)).astype(float)
    b = bootstrap_b(alpha)
    rng = np.random.default_rng(seed)
    means = np.empty(b)
    n_s = len(keys)
    for lo in range(0, b, chunk):
        hi = min(b, lo + chunk)
        idx = rng.integers(0, n_s, size=(hi - lo, n_s))
        means[lo:hi] = sums[idx].sum(axis=1) / counts[idx].sum(axis=1)
    return float(np.quantile(means, alpha, method="lower")), b


def session_t_lower(sessions: list[str], values: np.ndarray, alpha: float) -> float:
    """Session-clustered t lower bound of the pooled mean (ratio estimator, S - 1 df).

    A floor beside the percentile bootstrap, whose far tail (1.25e-3) is too narrow when
    sessions are few; a trial passes only if both bounds are above zero.
    """
    from scipy.stats import t as student_t

    keys, inverse = np.unique(np.asarray(sessions), return_inverse=True)
    sums = np.bincount(inverse, weights=values, minlength=len(keys))
    counts = np.bincount(inverse, minlength=len(keys)).astype(float)
    n_s = len(keys)
    if n_s < 2:
        return float("-inf")
    mean = sums.sum() / counts.sum()
    resid = sums - mean * counts
    se = math.sqrt(n_s / (n_s - 1) * float((resid ** 2).sum())) / counts.sum()
    return float(mean - student_t.ppf(1 - alpha, n_s - 1) * se)


def _f(x) -> float | None:
    x = float(x)
    return None if not math.isfinite(x) else round(x, 10)


def judge(trades: list[dict], alpha_trial: float, directional: bool = True) -> dict:
    """Verdict of one trial. ``trades`` rows: session, side (+1/-1), net, gross_long_net."""
    n = len(trades)
    longs = [t for t in trades if t["side"] > 0]
    shorts = [t for t in trades if t["side"] < 0]
    out: dict = {
        "n": n, "n_long": len(longs), "n_short": len(shorts),
        "sessions": len({t["session"] for t in trades}),
        "alpha_trial": alpha_trial,
    }
    if n == 0:
        return {**out, "verdict": "UNDERPOWERED", "reasons": ["no trades"]}
    net = np.array([t["net"] for t in trades], dtype=float)
    long_only = np.array([t["always_long_net"] for t in trades], dtype=float)
    order = np.argsort([t["session"] for t in trades], kind="stable")
    thirds = [float(part.mean()) for part in np.array_split(net[order], 3) if len(part)]
    out.update({
        "mean_net": _f(net.mean()),
        "median_net": _f(np.median(net)),
        "sd_net": _f(net.std(ddof=1)) if n > 1 else None,
        "hit_rate": _f((net > 0).mean()),
        "mean_long": _f(np.mean([t["net"] for t in longs])) if longs else None,
        "mean_short": _f(np.mean([t["net"] for t in shorts])) if shorts else None,
        "always_long_mean_net": _f(long_only.mean()),
        "thirds_mean_net": [_f(x) for x in thirds],
        "first_session": min(t["session"] for t in trades),
        "last_session": max(t["session"] for t in trades),
    })
    under = []
    if n < P.N_MIN:
        under.append(f"n {n} < {P.N_MIN}")
    if directional and (len(longs) < P.N_SIDE_MIN or len(shorts) < P.N_SIDE_MIN):
        under.append(f"sides {len(longs)}/{len(shorts)} < {P.N_SIDE_MIN} each")
    if under:
        return {**out, "verdict": "UNDERPOWERED", "reasons": under}
    sess = [t["session"] for t in trades]
    lower, b = session_bootstrap_lower(sess, net, alpha_trial)
    t_lower = session_t_lower(sess, net, alpha_trial)
    out.update({"bootstrap_lower": _f(lower), "bootstrap_b": b, "bootstrap_seed": P.BOOT_SEED,
                "t_lower": _f(t_lower)})
    fails = []
    if not net.mean() > 0:
        fails.append("mean net <= 0")
    if directional and not (out["mean_long"] > 0 and out["mean_short"] > 0):
        fails.append("a side's mean net <= 0")
    if not lower > 0:
        fails.append(f"bootstrap lower bound at 1-{alpha_trial:g} <= 0")
    if not t_lower > 0:
        fails.append(f"session-clustered t lower bound at 1-{alpha_trial:g} <= 0")
    if not net.mean() > long_only.mean():
        fails.append("does not beat always-long on the same trades")
    if sum(1 for x in thirds if x > 0) < P.THIRDS_REQUIRED:
        fails.append(f"positive in fewer than {P.THIRDS_REQUIRED} of 3 thirds")
    return {**out, "verdict": "FAIL" if fails else "PASS", "reasons": fails}
