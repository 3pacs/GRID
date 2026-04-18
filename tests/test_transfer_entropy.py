"""CAT-111 — Transfer entropy discovery engine tests."""
from __future__ import annotations

import numpy as np
import pytest

from analysis.transfer_entropy import (
    _DEFAULT_BINS,
    LeadLagScan,
    TransferEntropyResult,
    discover_leaders,
    pair_transfer_entropy,
    quantile_discretize,
    scan_lead_lag,
    transfer_entropy,
)


def _seeded_random(seed=42):
    return np.random.default_rng(seed)


class TestQuantileDiscretize:
    def test_uniform_bins(self):
        data = np.linspace(0, 100, 100)
        labels = quantile_discretize(data, bins=4)
        for i in range(4):
            assert 20 <= (labels == i).sum() <= 30

    def test_empty_input(self):
        labels = quantile_discretize([])
        assert len(labels) == 0

    def test_single_bin(self):
        data = [1, 2, 3, 4, 5]
        labels = quantile_discretize(data, bins=1)
        assert (labels == 0).all()

    def test_handles_duplicates(self):
        data = [1, 1, 1, 2, 2, 2, 3, 3, 3]
        labels = quantile_discretize(data, bins=3)
        assert len(labels) == 9


class TestTransferEntropy:
    def test_too_short_zero(self):
        te = transfer_entropy([1, 2, 3], [4, 5, 6])
        assert te == 0.0

    def test_identical_series_low_te(self):
        rng = _seeded_random()
        data = rng.standard_normal(200).cumsum().tolist()
        te = transfer_entropy(data, data)
        assert te < 0.5

    def test_independent_series_low_te(self):
        rng = _seeded_random()
        x = rng.standard_normal(200).tolist()
        y = rng.standard_normal(200).tolist()
        te = transfer_entropy(x, y)
        assert te < 0.5

    def test_causal_relationship_non_negative(self):
        rng = _seeded_random()
        x = rng.standard_normal(300).cumsum()
        noise = rng.standard_normal(300) * 0.1
        y = np.roll(x, 1) + noise
        y[0] = 0
        te_forward = transfer_entropy(x.tolist(), y.tolist(), lag=1)
        assert te_forward >= 0


class TestPairTransferEntropy:
    def test_result_shape(self):
        rng = _seeded_random()
        x = rng.standard_normal(200).tolist()
        y = rng.standard_normal(200).tolist()
        r = pair_transfer_entropy("X", x, "Y", y)
        assert isinstance(r, TransferEntropyResult)
        assert r.source_name == "X"
        assert r.target_name == "Y"
        assert r.n_observations == 200
        assert r.bins == _DEFAULT_BINS

    def test_is_directional_flag(self):
        r = TransferEntropyResult(
            source_name="X", target_name="Y", lag=1,
            te_bits=0.5, symmetric_te=0.1, n_observations=100, bins=4,
        )
        assert r.is_directional is True

        r2 = TransferEntropyResult(
            source_name="X", target_name="Y", lag=1,
            te_bits=0.3, symmetric_te=0.28, n_observations=100, bins=4,
        )
        assert r2.is_directional is False


class TestScanLeadLag:
    def test_scan_length(self):
        rng = _seeded_random()
        x = rng.standard_normal(200).tolist()
        y = rng.standard_normal(200).tolist()
        scan = scan_lead_lag("X", x, "Y", y, max_lag=5)
        assert len(scan.results) == 5

    def test_empty_scan_no_best_lag(self):
        x = [1.0, 2.0, 3.0]
        y = [4.0, 5.0, 6.0]
        scan = scan_lead_lag("X", x, "Y", y, max_lag=3)
        assert scan.best_lag is None
        assert scan.best_te == 0.0

    def test_to_dict(self):
        rng = _seeded_random()
        x = rng.standard_normal(100).tolist()
        y = rng.standard_normal(100).tolist()
        scan = scan_lead_lag("X", x, "Y", y, max_lag=3)
        d = scan.to_dict()
        assert "source_name" in d
        assert "best_lag" in d
        assert "results" in d


class TestDiscoverLeaders:
    def test_empty_map(self):
        findings = discover_leaders({})
        assert findings == []

    def test_single_series_no_pairs(self):
        findings = discover_leaders({"A": [1.0] * 200})
        assert findings == []

    def test_random_pairs_mostly_filtered(self):
        rng = _seeded_random()
        series = {
            "A": rng.standard_normal(200).tolist(),
            "B": rng.standard_normal(200).tolist(),
            "C": rng.standard_normal(200).tolist(),
        }
        findings = discover_leaders(series, max_lag=3, min_directional_bits=0.1)
        assert len(findings) <= 6


class TestDataclassRoundtrip:
    def test_result_to_dict(self):
        r = TransferEntropyResult(
            source_name="X", target_name="Y", lag=1,
            te_bits=0.3, symmetric_te=0.1, n_observations=100, bins=4,
        )
        d = r.to_dict()
        for k in ("source_name", "target_name", "lag", "te_bits",
                  "symmetric_te", "n_observations", "bins", "is_directional"):
            assert k in d
