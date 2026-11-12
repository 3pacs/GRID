"""The physics API estimates Hurst on increments, not price levels (2026-11-11).

R/S on a random-walk *level* returns H near 1, so every series was labelled
"trending". On the walk's increments H is near 0.5.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from api.routers.physics import _increments
from physics.transforms import hurst_exponent


@pytest.mark.unit
def test_random_walk_level_reads_trending_but_increments_read_random() -> None:
    rng = np.random.default_rng(7)
    level = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.01, 1500))))
    assert hurst_exponent(level, max_lag=100) > 0.8
    assert 0.35 < hurst_exponent(_increments(level), max_lag=100) < 0.65


@pytest.mark.unit
def test_series_crossing_zero_uses_first_differences() -> None:
    s = pd.Series([1.0, -0.5, 0.25, -2.0])
    assert _increments(s).tolist() == [-1.5, 0.75, -2.25]
