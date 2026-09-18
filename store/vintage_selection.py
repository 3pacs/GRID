"""Pure-Python per-decision vintage selection over an all-vintages frame.

Companion to ``store.pit.PITStore.get_feature_vintages``. That method
returns every vintage on record for a (feature_id, obs_date) range,
undeduplicated; ``select_vintage_per_decision`` below is the piece that
turns that raw frame into exactly what a correct per-day PIT fetch would
have returned -- one row per (feature_id, obs_date), selected using ONLY
vintages whose ``release_date`` is on or before that row's own
``obs_date``.

This is the crux of the whole exercise: ``store.pit.PITStore.get_pit``
takes a single ``as_of_date`` for an entire query and filters
``release_date <= as_of_date`` globally, then picks one vintage per
(feature_id, obs_date) from what's left. That is only correct when
``as_of_date`` equals each row's own ``obs_date`` -- which is exactly what
the existing per-day loop (``_fetch_pit_correct_matrix_per_day`` in
``validation/backtest.py`` and ``backtest/engine.py``) achieves by calling
``get_feature_matrix`` once per calendar day with ``as_of_date`` pinned to
that same day. Doing the equivalent in one round trip means fetching every
candidate vintage up front and then applying that same per-row cutoff
(``release_date <= obs_date``, not a single window-wide ``as_of_date``) in
Python before the standard vintage-policy tiebreak.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger as log

_RESULT_COLUMNS = ["feature_id", "obs_date", "value", "release_date", "vintage_date"]

_VALID_POLICIES = ("FIRST_RELEASE", "LATEST_AS_OF")


def select_vintage_per_decision(vintages_df: pd.DataFrame, policy: str) -> pd.DataFrame:
    """Collapse an all-vintages frame to one row per (feature_id, obs_date).

    For every (feature_id, obs_date) pair present in ``vintages_df``:

    1. Keep only candidate vintages with ``release_date <= obs_date`` --
       this is the per-decision cutoff: a vintage released after the
       observation's own date could never have been known on that date,
       regardless of what ``as_of_date`` a caller might otherwise pass.
       This is intentionally NOT "release_date <= some window-wide
       as_of_date" -- that is precisely the batching mistake this module
       exists to avoid (see ``store/vintage_selection.py`` module
       docstring and ``tests/test_evaluator_contracts.py``'s
       ``test_single_asof_batch_then_mask_is_not_equivalent_to_per_day_fetch``).
    2. Among the surviving candidates, apply the vintage policy:
         - FIRST_RELEASE — the row with the MINIMUM ``vintage_date``.
         - LATEST_AS_OF  — the row with the MAXIMUM ``vintage_date``.
       This mirrors ``store.pit.PITStore.get_pit``'s
       ``DISTINCT ON (feature_id, obs_date) ORDER BY vintage_date ASC/DESC``
       exactly, with ``as_of_date`` implicitly equal to that row's own
       ``obs_date`` -- which is what the reference per-day loop actually
       computes.

    A (feature_id, obs_date) pair with no surviving candidate (every
    vintage for it was released after its own obs_date -- e.g. a data
    error, or simply no data yet) is dropped from the result entirely,
    the same way the per-day loop's empty daily fetch drops it.

    Parameters:
        vintages_df: Output of ``PITStore.get_feature_vintages`` (or an
            equivalent all-vintages frame) with columns [feature_id,
            obs_date, value, release_date, vintage_date]. Undeduplicated
            -- may contain multiple vintages per (feature_id, obs_date).
        policy: 'FIRST_RELEASE' or 'LATEST_AS_OF'.

    Returns:
        pd.DataFrame: Columns [feature_id, obs_date, value, release_date,
                      vintage_date], exactly one row per (feature_id,
                      obs_date) that had at least one valid candidate.
                      Row order is not significant to callers -- both
                      ``get_pit`` and the per-day loop's concatenation are
                      re-sorted by the caller anyway.

    Raises:
        ValueError: If ``policy`` is not 'FIRST_RELEASE' or 'LATEST_AS_OF'.
    """
    if policy not in _VALID_POLICIES:
        raise ValueError(
            f"Invalid policy '{policy}'. Must be 'FIRST_RELEASE' or 'LATEST_AS_OF'."
        )

    if vintages_df.empty:
        return pd.DataFrame(columns=_RESULT_COLUMNS)

    # Per-decision cutoff: a vintage is even eligible only if it was
    # released on or before the observation date it describes. This is
    # the one line that makes batching safe -- it is evaluated per row,
    # never against a single shared as_of_date.
    eligible = vintages_df[vintages_df["release_date"] <= vintages_df["obs_date"]]

    if eligible.empty:
        log.debug("select_vintage_per_decision: no eligible vintages after cutoff")
        return pd.DataFrame(columns=_RESULT_COLUMNS)

    ascending = policy == "FIRST_RELEASE"  # min vintage_date for FIRST_RELEASE, max for LATEST_AS_OF
    ordered = eligible.sort_values(
        ["feature_id", "obs_date", "vintage_date"],
        ascending=[True, True, ascending],
    )
    selected = ordered.drop_duplicates(subset=["feature_id", "obs_date"], keep="first")

    return selected.loc[:, _RESULT_COLUMNS].reset_index(drop=True)
