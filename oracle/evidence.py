"""Evidence behind each oracle model, on corrected outcomes.

The scoreboard counts predictions, but predictions are not independent:
models issue hundreds of calls that expire on the same date, so one market
move scores them all together (options_flow's 351 scored calls fall on 2
expiry dates). A hit rate or a per-prediction bootstrap over them overstates
the evidence by orders of magnitude (read-only audit, 2026-11-08).

This module measures each model the way a rule is judged elsewhere in GRID
(mean net expectancy, bootstrap lower bound > 0 at alpha/k):

* Outcomes: the corrected outcome per ``dedup_keep`` prediction -- a
  split-adjusted rescore, else a hindsight rescore, else the official
  verdict. Rescores are exploratory hindsight (``oracle_prediction_rescores``),
  so the payload says which basis each count came from.
* Excess: ``pnl_pct`` minus the mean of every prediction with the same
  direction and expiry, so a model that only bets one way is not credited
  with the market's drift.
* Independent windows: distinct expiry dates. The lower bound is a cluster
  bootstrap over expiry dates, and a model needs ``MIN_WINDOWS`` of them
  before it gets a verdict at all.
* Implausible outcomes (|pnl_pct| > ``MAX_PLAUSIBLE_PNL_PCT``, i.e. price or
  split errors) are excluded and counted.
"""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

MIN_WINDOWS = 30
MIN_OUTCOMES_SHOWN = 5
ALPHA = 0.05
N_BOOT = 2000
MAX_PLAUSIBLE_PNL_PCT = 100.0

VERDICT_EDGE = "edge"
VERDICT_NO_EDGE = "no_edge"
VERDICT_INSUFFICIENT = "insufficient_windows"

_SPLIT_ADJUSTED = "oracle_rescore_split_adjusted_v1"

_OUTCOMES_SQL = """
WITH r AS (
  SELECT DISTINCT ON (prediction_id) prediction_id, pnl_pct, verdict, scoring_version
  FROM oracle_prediction_rescores
  WHERE pnl_pct IS NOT NULL
  ORDER BY prediction_id, (scoring_version = :split_adjusted) DESC, rescored_at DESC
)
SELECT p.model_name,
       COALESCE(r.pnl_pct, p.pnl_pct)::float AS pnl,
       COALESCE(r.verdict, p.verdict) AS verdict,
       p.direction,
       p.expiry,
       p.created_at,
       COALESCE(r.scoring_version, 'official') AS basis
FROM oracle_predictions p
LEFT JOIN r ON r.prediction_id = p.id
WHERE p.dedup_keep = TRUE
  AND p.expiry IS NOT NULL
  AND (r.prediction_id IS NOT NULL
       OR (p.verdict IN ('hit', 'miss', 'partial') AND p.pnl_pct IS NOT NULL))
"""


@dataclass(frozen=True)
class Outcome:
    model: str
    pnl: float
    verdict: str
    direction: str
    expiry: date
    created_at: datetime
    basis: str


def load_outcomes(engine: Engine) -> list[Outcome]:
    """One corrected outcome per scored ``dedup_keep`` prediction."""
    with engine.connect() as conn:
        rows = conn.execute(text(_OUTCOMES_SQL), {"split_adjusted": _SPLIT_ADJUSTED}).fetchall()
    return [Outcome(str(r[0]), float(r[1]), str(r[2]), str(r[3]), r[4], r[5], str(r[6])) for r in rows]


def _cluster_lower_bound(groups: Sequence[Sequence[float]], q: float, rng: random.Random) -> float:
    """Lower ``q`` quantile of the mean, resampling whole clusters with replacement."""
    means: list[float] = []
    for _ in range(N_BOOT):
        total = 0.0
        count = 0
        for _ in groups:
            g = groups[rng.randrange(len(groups))]
            total += sum(g)
            count += len(g)
        means.append(total / count)
    means.sort()
    return means[min(int(q * N_BOOT), N_BOOT - 1)]


def _iso(value: date | datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def build_evidence(
    outcomes: Iterable[Outcome],
    *,
    min_windows: int = MIN_WINDOWS,
    seed: int = 0,
) -> dict[str, Any]:
    """Per-model evidence and the summary the Predictions view leads with."""
    rows = list(outcomes)
    plausible = [o for o in rows if abs(o.pnl) <= MAX_PLAUSIBLE_PNL_PCT]
    excluded = len(rows) - len(plausible)

    cohort: dict[tuple[str, date], list[float]] = defaultdict(list)
    for o in plausible:
        cohort[(o.direction, o.expiry)].append(o.pnl)
    cohort_mean = {k: sum(v) / len(v) for k, v in cohort.items()}

    by_model: dict[str, list[Outcome]] = defaultdict(list)
    for o in plausible:
        by_model[o.model].append(o)

    tested = [m for m, v in by_model.items() if len({o.expiry for o in v}) >= min_windows]
    k = max(len(tested), 1)
    rng = random.Random(seed)

    models: list[dict[str, Any]] = []
    hidden = 0
    for name, v in by_model.items():
        if len(v) < MIN_OUTCOMES_SHOWN:
            hidden += 1
            continue
        windows: dict[date, list[float]] = defaultdict(list)
        for o in v:
            windows[o.expiry].append(o.pnl - cohort_mean[(o.direction, o.expiry)])
        n = len(v)
        raw_mean = sum(o.pnl for o in v) / n
        excess_mean = sum(sum(g) for g in windows.values()) / n
        hits = sum(1 for o in v if o.verdict == "hit")
        partials = sum(1 for o in v if o.verdict == "partial")
        row: dict[str, Any] = {
            "name": name,
            "outcomes": n,
            "windows": len(windows),
            "official": sum(1 for o in v if o.basis == "official"),
            "hit_rate": round((hits + 0.5 * partials) / n, 4),
            "raw_mean_pct": round(raw_mean, 3),
            "excess_mean_pct": round(excess_mean, 3),
            "lower_bound_pct": None,
            "first_created": _iso(min(o.created_at for o in v)),
            "last_created": _iso(max(o.created_at for o in v)),
        }
        if len(windows) >= min_windows:
            lb = _cluster_lower_bound(list(windows.values()), ALPHA / k, rng)
            row["lower_bound_pct"] = round(lb, 3)
            row["verdict"] = VERDICT_EDGE if lb > 0 else VERDICT_NO_EDGE
        else:
            row["verdict"] = VERDICT_INSUFFICIENT
        models.append(row)

    order = {VERDICT_EDGE: 0, VERDICT_NO_EDGE: 1, VERDICT_INSUFFICIENT: 2}
    models.sort(key=lambda r: (order[r["verdict"]], -r["windows"], -r["outcomes"]))

    official = [o for o in plausible if o.basis == "official"]
    corrected = [o for o in plausible if o.basis != "official"]
    return {
        "method": {
            "outcome_basis": "split-adjusted rescore > hindsight rescore > official verdict",
            "excess": "pnl_pct minus the mean of same-direction, same-expiry predictions (all models)",
            "independent_window": "expiry date",
            "lower_bound": f"expiry-cluster bootstrap, one-sided alpha {ALPHA}/k, k = models with >= {min_windows} windows",
            "min_windows": min_windows,
            "min_outcomes_shown": MIN_OUTCOMES_SHOWN,
            "max_plausible_pnl_pct": MAX_PLAUSIBLE_PNL_PCT,
        },
        "summary": {
            "outcomes": len(plausible),
            "official_outcomes": len(official),
            "corrected_outcomes": len(corrected),
            "official_last_expiry": _iso(max((o.expiry for o in official), default=None)),
            "corrected_last_expiry": _iso(max((o.expiry for o in corrected), default=None)),
            "excluded_implausible": excluded,
            "windows": len({o.expiry for o in plausible}),
            "models_shown": len(models),
            "models_hidden_small": hidden,
            "models_tested": len(tested),
            "models_with_edge": sum(1 for r in models if r["verdict"] == VERDICT_EDGE),
            "mean_pnl_pct": round(sum(o.pnl for o in plausible) / len(plausible), 3) if plausible else None,
        },
        "models": models,
    }
