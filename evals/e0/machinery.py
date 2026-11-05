"""The GRID machinery under test, named in one place.

E0 does not re-implement the statistics it grades. Every p-value, rank IC,
block choice and multiple-testing adjustment below is the production VS1
discovery path:

* ``analysis.panel_insider_density.measure_trial`` -- per-date Spearman rank IC
  over issuers (``rank_ic_series``), the data-driven permutation block
  (``analysis.offline_research_proof.autocorrelation_block``) and the block
  sign-flip null (``signflip_pvalues``) at the machinery's own seed;
* ``analysis.offline_research_proof.holm_adjusted`` / ``bh_adjusted`` over every
  declared trial (untestable trials enter at p = 1);
* ``analysis.panel_insider_density.run_alpha`` -- the S11 ledger alpha.

A change to any of those files changes what E0 measures, which is the point:
the scorecard records their hashes (:func:`machinery_fingerprint`) so every
scorecard is attributable to one machinery version. This adapter itself is
pinned in the E0 manifest; the machinery files are not (E0 grades them).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Sequence

import numpy as np

from analysis import offline_research_proof as orp
from analysis import panel_insider_density as panel

REPO = Path(__file__).resolve().parents[2]
MACHINERY_FILES: tuple[str, ...] = (
    "analysis/panel_insider_density.py",
    "analysis/offline_research_proof.py",
)

SEED = panel.SEED
MIN_N = panel.MIN_N
MIN_ENTITIES = panel.MIN_ENTITIES


def machinery_fingerprint(repo: Path = REPO) -> dict[str, str]:
    """sha256 (LF-normalised) of each machinery source file E0 grades."""
    out = {}
    for rel in MACHINERY_FILES:
        data = (Path(repo) / rel).read_bytes().replace(b"\r\n", b"\n")
        out[rel] = hashlib.sha256(data).hexdigest()
    return out


def run_alpha(k: int, q: float) -> float:
    return panel.run_alpha(k, q)


def measure(feature: np.ndarray, label: np.ndarray, horizon: int, trial: str, *, perms: int) -> dict:
    """One trial through the VS1 discovery statistic (sensitivity nulls off).

    Returns the machinery's own record: ``mean_ic``, two-sided ``p``,
    ``p_one_sided_positive``, ``status`` ("tested" or "insufficient_data"),
    ``n`` and ``block``.
    """
    n = feature.shape[0]
    tp = panel.TrialPanel(
        trial=trial,
        window="discovery",
        horizon=horizon,
        decision_at=[str(i) for i in range(n)],
        label_end=[str(i) for i in range(n)],
        entities=[str(j) for j in range(feature.shape[1])],
        feature=feature,
        label=label,
    )
    return panel.measure_trial(tp, perms=perms, seed=SEED, min_n=MIN_N, sensitivity=False)


def measure_ic_series(ic: np.ndarray, *, perms: int, direction: int) -> dict:
    """The machinery's block sign-flip on a precomputed per-date IC series."""
    series = np.asarray(ic, dtype=float)
    series = series[np.isfinite(series)]
    if len(series) < MIN_N:
        return {"n": int(len(series)), "mean_ic": None, "p": 1.0, "p_one_sided": 1.0,
                "status": "insufficient_data", "block": None}
    block, _ = orp.autocorrelation_block(series.tolist(), 0)
    mean_ic, p_two, p_one = panel.signflip_pvalues(series, block, perms, SEED, direction)
    return {"n": int(len(series)), "mean_ic": float(mean_ic), "p": float(p_two),
            "p_one_sided": float(p_one), "status": "tested", "block": int(block)}


def rank_ic_series(feature: np.ndarray, label: np.ndarray) -> np.ndarray:
    ic, _ = panel.rank_ic_series(feature, label)
    return ic


def holm(pvalues: Sequence[float]) -> list[float]:
    return list(orp.holm_adjusted(list(pvalues)))


def bh(pvalues: Sequence[float]) -> list[float]:
    return list(orp.bh_adjusted(list(pvalues)))
