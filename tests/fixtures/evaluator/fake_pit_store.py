"""Deterministic, pure-Python stand-in for ``store.pit.PITStore``.

Used only by ``tests/test_evaluator_contracts.py`` so the walk-forward
evaluator contract tests never need a real database. It reproduces the
same PIT semantics as the real store (see ``store/pit.py``):

    HARD CONSTRAINT 1: every returned row has release_date <= as_of_date.
    HARD CONSTRAINT 2: every returned row has obs_date <= as_of_date.
    HARD CONSTRAINT 3:
        FIRST_RELEASE — earliest vintage_date per (feature_id, obs_date).
        LATEST_AS_OF  — latest vintage_date per (feature_id, obs_date)
                        where release_date <= as_of_date.

so that a bug in the *caller's* use of ``as_of_date`` (e.g. passing a
single window-end cutoff instead of each row's own date) shows up as a
real, reproducible lookahead in the tests, exactly as it would against
the production PostgreSQL-backed store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd


@dataclass(frozen=True)
class Row:
    feature_id: int
    obs_date: date
    value: float
    release_date: date
    vintage_date: date


class FakePITStore:
    """In-memory PIT store built from a list of ``Row`` records.

    ``rows`` can be mutated after construction (``store.rows.append(...)``)
    to simulate a future revision or a brand-new observation landing —
    that is exactly how the "appending future rows must not change earlier
    results" contract test drives the fixture.
    """

    def __init__(self, rows: list[Row] | None = None) -> None:
        self.rows: list[Row] = list(rows or [])

    def get_feature_matrix(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        as_of_date: date,
        vintage_policy: str = "FIRST_RELEASE",
    ) -> pd.DataFrame:
        if vintage_policy not in ("FIRST_RELEASE", "LATEST_AS_OF"):
            raise ValueError(f"Invalid vintage_policy '{vintage_policy}'.")

        candidates = [
            r
            for r in self.rows
            if r.feature_id in feature_ids
            and r.obs_date <= as_of_date
            and r.release_date <= as_of_date
            and start_date <= r.obs_date <= end_date
        ]

        # DISTINCT ON (feature_id, obs_date) ORDER BY vintage_date ASC/DESC
        best: dict[tuple[int, date], Row] = {}
        for r in candidates:
            key = (r.feature_id, r.obs_date)
            current = best.get(key)
            if current is None:
                best[key] = r
                continue
            if vintage_policy == "FIRST_RELEASE":
                if r.vintage_date < current.vintage_date:
                    best[key] = r
            else:  # LATEST_AS_OF
                if r.vintage_date > current.vintage_date:
                    best[key] = r

        if not best:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="obs_date"))

        frame = pd.DataFrame(
            [
                {"obs_date": r.obs_date, "feature_id": r.feature_id, "value": r.value}
                for r in best.values()
            ]
        )
        matrix = frame.pivot_table(
            index="obs_date", columns="feature_id", values="value", aggfunc="first"
        )
        matrix.index = pd.DatetimeIndex(matrix.index, name="obs_date")
        return matrix.sort_index()
