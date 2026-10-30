"""The real panel STRUCTURE E0 plants synthetic outcomes into.

A structure is the feature side of a declared VS1 run with no outcome: the
session calendar, each trial's horizon-spaced decision positions and its
decisions x entities feature matrix (NaN = the issuer abstains at that
decision). ``vs1_v7_technology_structure`` is the VS1 v7 Technology panel
(202 price-admitted issuers, 2011-10-01 .. 2019-12-31 proxy sessions, four
declared trials) rebuilt from SEC Form 4 data by :mod:`evals.e0.extract_structure`.
It holds no price, return or IC and no ticker/CIK.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np

PACKAGE = Path(__file__).resolve().parent


@dataclass(frozen=True)
class TrialStructure:
    trial: str
    horizon: int
    positions: np.ndarray  # decision positions into the session calendar
    feature: np.ndarray  # decisions x entities, NaN = abstain

    def standardized_ranks(self) -> np.ndarray:
        """Per decision: average feature ranks among finite entries, centred and scaled to sd 1.

        Rows with fewer than two finite entries or a constant cross-section get 0
        (the machinery abstains on those dates anyway). NaN stays NaN.
        """
        out = np.full(self.feature.shape, np.nan)
        for i, row in enumerate(self.feature):
            m = np.isfinite(row)
            if m.sum() < 2:
                continue
            values = row[m]
            if np.ptp(values) <= 0:
                out[i, m] = 0.0
                continue
            ranks = _average_ranks(values)
            ranks = ranks - ranks.mean()
            out[i, m] = ranks / ranks.std()
        return out


@dataclass(frozen=True)
class Structure:
    name: str
    n_sessions: int
    trials: Mapping[str, TrialStructure]
    primary_trial: str
    propensity_trial: str

    @property
    def n_entities(self) -> int:
        return next(iter(self.trials.values())).feature.shape[1]

    @property
    def max_horizon(self) -> int:
        return max(t.horizon for t in self.trials.values())

    def propensity(self) -> np.ndarray:
        """Per entity: standardized rank of its share of decisions with a positive feature.

        Used only to correlate synthetic factor loadings with the feature (the
        ``exposure`` stress); entities never admitted get the median.
        """
        f = self.trials[self.propensity_trial].feature
        finite = np.isfinite(f)
        with np.errstate(invalid="ignore", divide="ignore"):
            share = np.where(finite.any(axis=0), (np.nan_to_num(f) > 0).sum(axis=0) / finite.sum(axis=0), np.nan)
        share = np.where(np.isfinite(share), share, np.nanmedian(share))
        ranks = _average_ranks(share)
        ranks = ranks - ranks.mean()
        sd = ranks.std()
        return ranks / sd if sd > 0 else ranks


def _average_ranks(values: np.ndarray) -> np.ndarray:
    x = np.asarray(values, dtype=float)
    order = np.argsort(x, kind="mergesort")
    ordered = x[order]
    starts = np.flatnonzero(np.r_[True, ordered[1:] != ordered[:-1]])
    counts = np.diff(np.r_[starts, len(x)])
    ranks = np.empty(len(x))
    ranks[order] = np.repeat(starts + 1 + (counts - 1) / 2.0, counts)
    return ranks


def load_structure(config: Mapping) -> Structure:
    spec = config["structure"]
    data = np.load(PACKAGE / spec["file"], allow_pickle=False)
    n_sessions = int(len(data["sessions"]))
    trials = {}
    for trial in spec["trials"]:
        key = trial.replace("|", "_")
        horizon = int(trial.split("|fwd")[1])
        positions = np.asarray(data[f"{key}__positions"], dtype=np.int64)
        feature = np.asarray(data[f"{key}__feature"], dtype=float)
        if feature.shape[0] != len(positions):
            raise ValueError(f"{trial}: positions and feature rows differ")
        trials[trial] = TrialStructure(trial=trial, horizon=horizon, positions=positions, feature=feature)
    return Structure(
        name=spec["name"],
        n_sessions=n_sessions,
        trials=trials,
        primary_trial=spec["primary_trial"],
        propensity_trial=spec["propensity_trial"],
    )
