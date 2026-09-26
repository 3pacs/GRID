"""S08 statistical core of analysis.offline_research_proof.

(a) split-then-label purge and a holdout-tamper leak test modelled on the vault
    #101 test_grid_vein_scan_boundary.py (obsidian-vault 532883a68),
(b) horizon-spaced sampling vs fixed-step overlap accounting,
(c) the block-permutation null,
(d) BH-FDR over the whole run including untestable trials.
Synthetic data only; no DB.
"""

from dataclasses import replace
from itertools import pairwise

import numpy as np
import pandas as pd
import pytest
from scipy.stats import pearsonr

from analysis.offline_research_proof import (
    Protocol,
    bh_adjusted,
    block_permutation_pvalue,
    block_permutations,
    build_family_rows,
    discover,
    permutation_block,
    validate_rows,
)

HORIZONS = (1, 5, 10, 20)


def synthetic_panel(seed=7, n_days=520, start="2025-01-02"):
    """Business-daily closes for two targets and three backward-looking features."""
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range(start, periods=n_days, tz="UTC")
    prices = pd.DataFrame(
        {
            t: 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n_days)))
            for t in ("SPY", "TLT")
        },
        index=idx,
    )
    levels = pd.DataFrame(
        {
            f: np.cumsum(rng.normal(0, 0.1, n_days)) + 10
            for f in ("VIX", "T10YIE", "HY")
        },
        index=idx,
    )
    features = pd.concat(
        [levels.diff(5).add_suffix("|chg5"), levels.diff(20).add_suffix("|chg20")],
        axis=1,
    )
    return features, prices


def protocol_for(features, sampling="horizon_spaced", **overrides):
    idx = features.index
    split = idx[int(len(idx) * 0.6)]
    return Protocol(
        run_id=f"s08-{sampling}",
        features=tuple(features.columns),
        split=split.isoformat(),
        end=(idx[-1] + pd.Timedelta(days=1)).isoformat(),
        families=tuple(f"{t}|fwd{h}" for t in ("SPY", "TLT") for h in HORIZONS),
        sampling=sampling,
        step=5,
        perms=299,
        statistic="spearman",
        start=idx[60].isoformat(),
        **overrides,
    )


def family_rows(protocol, features, prices, window):
    out = {}
    for family in protocol.families:
        ticker, h = family.split("|fwd")
        out[family] = build_family_rows(
            protocol, features, prices[ticker], int(h), window
        )
    return out


# --- (a) split first, then label ---------------------------------------------------


@pytest.mark.parametrize("sampling", ["horizon_spaced", "fixed_step_block_null"])
def test_labels_are_built_after_the_split_and_purged_exactly(sampling):
    features, prices = synthetic_panel()
    protocol = protocol_for(features, sampling)
    idx, split = features.index, pd.Timestamp(protocol.split)
    cutpos, startpos = idx.get_loc(split), idx.get_loc(pd.Timestamp(protocol.start))
    purged = 0
    for family, rows in family_rows(protocol, features, prices, "discovery").items():
        h = int(family.split("|fwd")[1])
        step = max(5, h) if sampling == "horizon_spaced" else 5
        sampled = range(startpos, cutpos, step)
        expected = [i for i in sampled if i + h < cutpos]
        assert [idx.get_loc(pd.Timestamp(r["decision_at"])) for r in rows] == expected
        # every label that would read a price on/after the holdout start is gone
        assert all(pd.Timestamp(r["label_end"]) < split for r in rows)
        purged += len(sampled) - len(expected)
        validate_rows(rows, protocol, "discovery")
    assert purged > 0  # otherwise this test proves nothing
    for rows in family_rows(protocol, features, prices, "holdout").values():
        assert all(
            pd.Timestamp(r["decision_at"]) >= split
            and pd.Timestamp(r["label_end"]) < pd.Timestamp(protocol.end)
            for r in rows
        )
        validate_rows(rows, protocol, "holdout")


@pytest.mark.parametrize("sampling", ["horizon_spaced", "fixed_step_block_null"])
def test_discovery_is_invariant_to_holdout_prices_only_with_split_first(sampling):
    features, prices = synthetic_panel()
    protocol = protocol_for(features, sampling)
    split = pd.Timestamp(protocol.split)
    tampered = prices.copy()
    after = tampered.index >= split
    rng = np.random.default_rng(99)
    for c in tampered.columns:
        tampered.loc[after, c] = 100 * np.exp(
            np.cumsum(rng.normal(0, 0.02, after.sum()))
        )

    rows_a = family_rows(protocol, features, prices, "discovery")
    rows_b = family_rows(protocol, features, tampered, "discovery")
    assert rows_a == rows_b
    assert discover(protocol, rows_a) == discover(protocol, rows_b)

    # Counterfactual (the v1 defect): labels computed on the full series before
    # the split DO read the tampered prices, and the contract refuses them.
    for family in protocol.families:
        ticker, h = family.split("|fwd")
        h = int(h)
        naive_a = prices[ticker].shift(-h) / prices[ticker] - 1
        naive_b = tampered[ticker].shift(-h) / tampered[ticker] - 1
        last = features.index[features.index < split][-1]
        assert naive_a[last] != naive_b[last]
    leaky = {f: [dict(r) for r in rows] for f, rows in rows_a.items()}
    family = protocol.families[-1]
    last_row = leaky[family][-1]
    last_row["label_end"] = last_row["target_known_at"] = protocol.split
    with pytest.raises(ValueError, match="crosses holdout"):
        discover(protocol, leaky)


def test_builder_refuses_misaligned_or_naive_indexes():
    features, prices = synthetic_panel()
    protocol = protocol_for(features)
    with pytest.raises(ValueError, match="session index"):
        build_family_rows(protocol, features, prices["SPY"].iloc[1:], 5, "discovery")
    naive = features.tz_localize(None)
    with pytest.raises(ValueError, match="session index"):
        build_family_rows(
            protocol, naive, prices["SPY"].tz_localize(None), 5, "discovery"
        )


def test_missing_feature_is_explicit_abstention_not_silent_nan():
    features, prices = synthetic_panel()
    protocol = protocol_for(features)
    rows = family_rows(protocol, features, prices, "discovery")["SPY|fwd5"]
    assert all(
        v["value"] is None or np.isfinite(v["value"])
        for r in rows
        for v in r["features"].values()
    )
    rows[0]["features"][protocol.features[0]]["value"] = float("nan")
    with pytest.raises(ValueError, match="nonfinite feature"):
        validate_rows(rows, protocol, "discovery")


# --- (b) horizon-spaced sampling ---------------------------------------------------


def test_horizon_spaced_rows_never_overlap_and_overlap_is_refused():
    features, prices = synthetic_panel()
    spaced = protocol_for(features)
    rows = family_rows(spaced, features, prices, "discovery")["SPY|fwd20"]
    assert all(
        pd.Timestamp(b["decision_at"]) >= pd.Timestamp(a["label_end"])
        for a, b in pairwise(rows)
    )
    assert validate_rows(rows, spaced, "discovery") == 0

    fixed = protocol_for(features, "fixed_step_block_null")
    overlapping = family_rows(fixed, features, prices, "discovery")["SPY|fwd20"]
    with pytest.raises(ValueError, match="overlapping"):
        validate_rows(overlapping, spaced, "discovery")


@pytest.mark.parametrize("h, block", [(1, 1), (5, 1), (10, 2), (20, 4)])
def test_fixed_step_block_covers_measured_overlap(h, block):
    features, prices = synthetic_panel()
    fixed = protocol_for(features, "fixed_step_block_null")
    rows = family_rows(fixed, features, prices, "discovery")[f"SPY|fwd{h}"]
    depth = validate_rows(rows, fixed, "discovery")
    assert permutation_block(fixed, depth) == block  # v2: ceil(h / 5)
    if block > 1:
        with pytest.raises(ValueError, match="block shorter"):
            permutation_block(replace(fixed, block=block - 1), depth)
    assert permutation_block(replace(fixed, block=block + 3), depth) == block + 3


# --- (c) block-permutation null ----------------------------------------------------


@pytest.mark.parametrize("n, block", [(48, 1), (48, 4), (47, 4), (45, 2)])
def test_block_permutations_are_exact_and_keep_blocks_contiguous(n, block):
    index = block_permutations(n, block, 200, 11)
    assert index.shape == (200, n) and not index.flags.writeable
    assert (np.sort(index, axis=1) == np.arange(n)).all()
    for row in index[:25]:
        starts = [k for k in range(n) if row[k] % block == 0]
        for k in starts:
            length = min(block, n - row[k])
            assert list(row[k : k + length]) == list(range(row[k], row[k] + length))
    assert (block_permutations(n, block, 200, 11) == index).all()


def test_block_null_is_deterministic_with_bounded_resolution():
    rng = np.random.default_rng(3)
    x = rng.normal(size=60)
    y = 0.25 * x + rng.normal(size=60)
    r1, p1 = block_permutation_pvalue(x, y, 2, 999, 1)
    r2, p2 = block_permutation_pvalue(x, y, 2, 999, 1)
    assert (r1, p1) == (r2, p2)
    assert r1 == pytest.approx(np.corrcoef(x, y)[0, 1])
    assert block_permutation_pvalue(x, y, 2, 999, 2)[1] != p1
    assert block_permutation_pvalue(x, x, 1, 999, 1)[1] == 1 / 1000


def test_block_null_corrects_iid_overconfidence_on_overlapping_targets():
    """Independent persistent feature vs 4x-overlapping target sums (the fwd20 at
    weekly-step shape): IID Pearson rejects far too often, the block null does not."""
    rng = np.random.default_rng(7)
    n, h, sims = 60, 4, 300
    iid = block = 0
    for s in range(sims):
        e = rng.normal(size=n + 200)
        x = np.zeros(n + 200)
        for i in range(1, n + 200):
            x[i] = 0.95 * x[i - 1] + e[i]
        x = x[200:]
        y = np.convolve(rng.normal(size=n + h), np.ones(h), "valid")[:n]
        iid += pearsonr(x, y).pvalue < 0.05
        block += block_permutation_pvalue(x, y, h, 999, s)[1] < 0.05
    assert iid / sims > 0.2
    assert block / sims < 0.12 and block < iid / 2


# --- (d) BH-FDR over the whole run -------------------------------------------------


def v2_bh(p, q):
    """Reference copy of the v2 prototype's bh() rejection mask."""
    p = np.asarray(p, float)
    m = len(p)
    order = np.argsort(p)
    ok = p[order] <= q * (np.arange(1, m + 1) / m)
    k = np.max(np.where(ok)[0]) + 1 if ok.any() else 0
    mask = np.zeros(m, bool)
    mask[order[:k]] = True
    return mask


@pytest.mark.parametrize("seed", range(5))
def test_bh_adjusted_matches_step_up_rejections(seed):
    rng = np.random.default_rng(seed)
    p = np.concatenate([rng.uniform(size=180), rng.uniform(0, 0.002, size=20)])
    adjusted = np.array(bh_adjusted(p))
    for q in (0.05, 0.10):
        assert ((adjusted <= q) == v2_bh(p, q)).all()
    assert (adjusted >= p * (1 - 1e-12)).all() and (adjusted <= 1).all()


def test_untestable_trials_stay_in_the_denominator():
    testable = [0.02, 0.03]
    assert all(a <= 0.10 for a in bh_adjusted(testable))  # testable-only: 2 selected
    full = bh_adjusted(testable + [1.0] * 8)  # whole run: none selected
    assert full[:2] == pytest.approx([0.15, 0.15]) and max(full[:2]) > 0.10
    with pytest.raises(ValueError):
        bh_adjusted([0.5, float("nan")])


def test_discover_applies_one_bh_family_across_every_trial():
    features, prices = synthetic_panel()
    protocol = protocol_for(features)
    rows = family_rows(protocol, features, prices, "discovery")
    rows["TLT|fwd20"] = []  # a family with no rows is still declared trials
    payload = discover(protocol, rows)["payload"]
    ledger = payload["ledger"]
    assert payload["trial_count"] == len(ledger) == 6 * 8
    assert payload["untestable_count"] >= 6 + 6  # fwd20 families: n < min_n / empty
    assert [t["adjusted_p"] for t in ledger] == bh_adjusted([t["p"] for t in ledger])
    assert all(t["p"] == 1.0 for t in ledger if t["status"] != "tested")
    assert all(
        t["selected"] == (t["status"] == "tested" and t["adjusted_p"] <= 0.10)
        for t in ledger
    )
    assert payload["min_attainable_p"] == 1 / 300
    with pytest.raises(ValueError, match="family universe"):
        discover(protocol, {k: v for k, v in rows.items() if k != "SPY|fwd1"})
