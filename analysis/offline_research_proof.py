"""Offline research contract proof. No registry, DB, provider, route or timer wiring.

Reuses GRID's pure correlation primitive, never its DB fetcher or state updater.

Statistical core (S08, 2026-09-26), integrated from the corrected vault #101
vein-scan prototype v2 (obsidian-vault 532883a68) into this contract:

(a) Split first, then label. ``build_family_rows`` blanks every price outside
    the evaluation window *before* computing any forward label, so a discovery
    label that would need a price on/after the holdout start is never produced
    (purged), and holdout prices cannot reach discovery. ``validate_rows`` still
    refuses any crossing label that reaches it by another route.
(b) Horizon-spaced sampling. In ``horizon_spaced`` mode decisions are drawn
    every ``max(step, horizon)`` sessions and overlapping outcome windows are
    refused. ``fixed_step_block_null`` keeps a fixed step and allows overlap
    only when the permutation block covers the measured overlap depth.
(c) Block-permutation null. p-values come from permuting contiguous blocks of
    the target sequence (the feature is never permuted, so its autocorrelation
    is kept), with a deterministic seed and configurable perms/block length.
    IID Pearson p-values are no longer used anywhere.
(d) Benjamini-Hochberg FDR over the whole run: every declared trial in every
    target family, including excluded, insufficient, constant and NaN trials,
    is in the denominator (they carry p = 1.0).

Origins: ``synthetic_fixture`` (deterministic proof data),
``exploratory_replay`` (a hindsight replay of an offline CSV that has no
per-source known-at/vintage contract) and ``latest_vintage_read`` (S09, renamed
from ``pit_vintage_read`` in S09b). The read origin is not a self-declared
label: its protocol must carry the receipt hash of a panel that
``analysis.research_real_panel`` read through ``store.observations.read_window``,
and ``discover``/``evaluate_holdout`` re-derive every row from that verified
panel and refuse anything else. Any other origin is refused. It is **not**
point-in-time: every value is the latest vintage (hindsight), so its state is
``LATEST_VINTAGE_READ_EXPLORATORY``.

S09 fixes carried from the #658 review:

* ``fixed_step_block_null`` is anti-conservative at its default block (overlap
  depth + 1): the reviewer measured 8.5% at n=60 and 10.75% at n=240 for a
  nominal 5%. Fixed-step runs are therefore diagnostic only: they still report
  every p-value and BH-adjusted p, but no trial is ever ``selected``, so no
  holdout check and no candidate can come from them. Candidates come only from
  ``horizon_spaced`` runs. The caveat is written into the manifest
  (``caveats``, ``candidate_eligible``) and the payload ``method`` string.
* Targets may be labelled as a ``return`` (end/start - 1, prices) or a
  ``change`` (end - start, for rates, spreads and indexes that can cross 0).

S09b fixes carried from the #660 review:

* ``self_lag``: declared (family, feature) pairs whose feature is the target's
  own series or a declared near-copy (``research_real_panel.PROXY_GROUPS``).
  They are measured for the record but carry status ``self_lag`` and p = 1.0
  in the BH denominator, so they can never be selected or become candidates.
* The default permutation block (``block=0``) is data-driven. The block-1 null
  is anti-conservative when the sampled target is autocorrelated (target AR(1)
  phi=0.35 against a persistent feature: 15.9% / 14.6% rejections at n=60 / 240
  for a nominal 5%). :func:`autocorrelation_block` sizes the block from the
  lag-1 autocorrelation of the sampled target on discovery rows only, and the
  holdout reuses the frozen discovery block. Where the block may still be
  anti-conservative (the MIN_BLOCKS cap binds, or |acf1| is inside a noise band
  wider than 0.2 at small n) the family is named in the manifest ``caveats``
  and the ``method`` string carries a CAVEAT.
* Feature ``known_at`` may be supplied per value (the adapter stamps declared
  publication times). ``validate_rows`` refuses any feature known after its
  decision.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import rankdata

from analysis.hypothesis_tester import compute_lagged_correlation

# origin -> state carried by every holdout result of that origin
ORIGINS = {
    "synthetic_fixture": "SYNTHETIC_PROOF_ONLY",
    "exploratory_replay": "EXPLORATORY_REPLAY_ONLY",
    "latest_vintage_read": "LATEST_VINTAGE_READ_EXPLORATORY",
}
LATEST_VINTAGE_ORIGIN = "latest_vintage_read"
SAMPLING = ("horizon_spaced", "fixed_step_block_null")
CANDIDATE_SAMPLING = ("horizon_spaced",)
LABELS = ("return", "change")
FIXED_STEP_CAVEAT = (
    "fixed_step_block_null is anti-conservative at block = overlap depth + 1 "
    "(#658 review: 8.5% at n=60, 10.75% at n=240 for nominal 5%); diagnostic "
    "only, never selects, no candidates"
)
STATISTICS = ("pearson", "spearman")
# Data-driven block (S09b): the tolerated Bartlett-weight bias of the block null,
# sum_k min(k/L, 1) |phi|^k ~ |phi| / ((1 - |phi|)^2 L) <= BLOCK_TOLERANCE, and
# the fewest blocks a permutation may have.
BLOCK_TOLERANCE = 0.05
MIN_BLOCKS = 8
# Below n = 100 the 2/sqrt(n) band exceeds 0.2: dependence that size goes unseen.
UNDETECTABLE_ACF = 0.2
CAPPED_BLOCK_CALIBRATION = (
    "target AR(1) 0.35 vs persistent feature at n=60, block capped at 7: "
    "7.0% rejections at nominal 5%"
)
SELF_LAG = "self_lag"


FORWARD_PENDING = "FORWARD_EVIDENCE_PENDING"


def candidate_state(origin: str) -> str:
    """A frozen candidate's state, tagged by origin (S10, from the #658 review).

    Only a ``latest_vintage_read`` candidate may wait for forward evidence. A
    synthetic-fixture or exploratory-replay "candidate" is a proof artifact:
    it carries its origin's state, so no forward log can mistake it for one.
    """
    return FORWARD_PENDING if origin == LATEST_VINTAGE_ORIGIN else ORIGINS[origin]


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, allow_nan=False).encode()
    ).hexdigest()


def stamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("timestamps must carry a timezone")
    return result


@dataclass(frozen=True)
class Protocol:
    run_id: str
    features: tuple[str, ...]
    split: str
    end: str
    min_n: int = 30
    alpha: float = 0.05  # holdout Bonferroni level over frozen selections
    origin: str = "synthetic_fixture"
    families: tuple[str, ...] = ("target",)  # one family per target/horizon
    fdr_q: float = 0.10  # discovery BH-FDR level over the whole run
    sampling: str = "horizon_spaced"
    step: int = 1  # base decision spacing, in panel sessions
    block: int = 0  # permutation block length in samples; 0 = data-driven (acf1)
    perms: int = 10000
    seed: int = 20260926
    statistic: str = "pearson"
    start: str = ""  # optional discovery start (earlier rows are warm-up only)
    read_receipt: str = ""  # sha256 of the latest-vintage read receipt (read origin only)
    # (family, feature) trials whose feature proxies the family's own target
    self_lag: tuple[tuple[str, str], ...] = ()

    def validate(self):
        if self.origin not in ORIGINS:
            raise ValueError("real PIT/source/session contracts are not implemented")
        if (self.origin == LATEST_VINTAGE_ORIGIN) != bool(self.read_receipt) or (
            self.read_receipt
            and (
                len(self.read_receipt) != 64
                or set(self.read_receipt) - set("0123456789abcdef")
            )
        ):
            raise ValueError(
                "latest_vintage_read needs, and only it may carry, a read receipt"
            )
        if len(set(self.self_lag)) != len(self.self_lag) or any(
            len(pair) != 2
            or pair[0] not in self.families
            or pair[1] not in self.features
            for pair in self.self_lag
        ):
            raise ValueError("self_lag pairs must name declared families and features")
        if (
            not self.run_id
            or not self.features
            or len(set(self.features)) != len(self.features)
            or not self.families
            or len(set(self.families)) != len(self.families)
        ):
            raise ValueError("unique complete trial universe required")
        if (
            self.min_n < 30
            or not 0 < self.alpha <= 0.05
            or not 0 < self.fdr_q <= 0.10
            or stamp(self.split) >= stamp(self.end)
            or (self.start and stamp(self.start) >= stamp(self.split))
        ):
            raise ValueError("invalid frozen protocol")
        if (
            self.sampling not in SAMPLING
            or self.statistic not in STATISTICS
            or self.step < 1
            or self.block < 0
            or self.perms < 99
        ):
            raise ValueError("invalid frozen statistical protocol")


def protocol_from_payload(value):
    return Protocol(
        **{
            **value,
            "features": tuple(value["features"]),
            "families": tuple(value["families"]),
            "self_lag": tuple(tuple(pair) for pair in value.get("self_lag", ())),
        }
    )


def excluded(feature):
    return any(
        token in feature.lower()
        for token in ("snap:", "llm", "telemetry", "pipeline", "astro")
    )


def outcome_horizon(row):
    """Declared horizon label (e.g. "5 sessions") or the wall-clock seconds.

    A ``change`` label is part of the horizon identity, so a family can never
    mix return and change labels, and the holdout must use discovery's label.
    """
    if "horizon" in row:
        kind = row.get("label", "return")
        return row["horizon"] if kind == "return" else f"{row['horizon']} {kind}"
    return (stamp(row["label_end"]) - stamp(row["decision_at"])).total_seconds()


def validate_rows(rows, protocol, window):
    """Reject future features, crossing labels, overlap, duplicates and silent NaNs.

    A feature value of ``None`` is an explicit abstention (not observed at the
    decision); a NaN/inf float is a silent defect and is refused. Returns the
    overlap depth: the most later decisions that start inside one outcome window.
    """
    protocol.validate()
    previous_decision = previous_end = horizon = None
    open_ends, depth = [], 0
    for row in rows:
        decision, end = stamp(row["decision_at"]), stamp(row["label_end"])
        if row.get("origin") != protocol.origin or set(row["features"]) != set(
            protocol.features
        ):
            raise ValueError("origin/universe mismatch")
        if end <= decision or (
            previous_decision is not None and decision <= previous_decision
        ):
            raise ValueError("overlapping or unordered outcomes")
        if (
            protocol.sampling == "horizon_spaced"
            and previous_end is not None
            and decision < previous_end
        ):
            raise ValueError("overlapping or unordered outcomes")
        open_ends = [e for e in open_ends if e > decision]
        depth = max(depth, len(open_ends))
        open_ends.append(end)
        if horizon is not None and outcome_horizon(row) != horizon:
            raise ValueError("mixed outcome horizons")
        horizon, previous_decision, previous_end = outcome_horizon(row), decision, end
        if window == "discovery" and end >= stamp(protocol.split):
            raise ValueError("discovery label crosses holdout boundary")
        if (
            window == "discovery"
            and protocol.start
            and decision < stamp(protocol.start)
        ):
            raise ValueError("discovery decision before declared start")
        if window == "holdout" and (
            decision < stamp(protocol.split) or end >= stamp(protocol.end)
        ):
            raise ValueError("holdout outside frozen window")
        if (
            not end
            <= stamp(row["target_known_at"])
            < stamp(protocol.split if window == "discovery" else protocol.end)
        ):
            raise ValueError("outcome unavailable in evaluation window")
        if not math.isfinite(row["target"]):
            raise ValueError("nonfinite outcome")
        for feature in row["features"].values():
            value = feature["value"]
            if stamp(feature["known_at"]) > decision or (
                value is not None and not math.isfinite(value)
            ):
                raise ValueError("future or nonfinite feature")
    return depth


def lag1_autocorrelation(values) -> float | None:
    """Sample lag-1 autocorrelation (``None`` when undefined)."""
    y = np.asarray(values, dtype=float)
    if len(y) < 3:
        return None
    y = y - y.mean()
    denominator = float(y @ y)
    return float(y[1:] @ y[:-1]) / denominator if denominator > 0 else None


def autocorrelation_block(target, depth: int = 0) -> tuple[int, dict]:
    """Data-driven permutation block for a sampled target sequence (S09b).

    ``target`` must be the sampled *discovery* labels only. If the lag-1
    autocorrelation phi lies inside the 2/sqrt(n) band, the block is the overlap
    floor (``depth + 1``). Otherwise it is sized so the block null's
    Bartlett-weight bias for an AR(1) of that |phi| stays below
    ``BLOCK_TOLERANCE``, i.e. ``ceil(|phi| / ((1 - |phi|)^2 * tol))``, capped so
    at least ``MIN_BLOCKS`` blocks remain and never below the overlap floor.

    Negative phi (mean-reverting change labels) lengthens the block too: block
    1 is then conservative against a persistent feature, and the longer block
    is the accurate null (see the S09b calibration tests).

    Returns ``(block, basis)``; ``basis`` is written into the manifest. It
    carries a ``caveat`` when the block is known to be possibly
    anti-conservative: the ``MIN_BLOCKS`` cap binds, or |phi| is inside a noise
    band wider than ``UNDETECTABLE_ACF`` (small n), where real dependence of
    that size would go undetected and the floor block would be used.
    """
    floor = depth + 1
    n = len(target)
    phi = lag1_autocorrelation(target)
    band = 2 / math.sqrt(n) if n else None
    basis = {"n": n, "acf1": phi, "band_2se": band, "rule": "overlap floor"}
    if phi is None or abs(phi) <= band:
        if phi is not None and band > UNDETECTABLE_ACF:
            basis["caveat"] = (
                f"|acf1|={abs(phi):.3f} inside the 2/sqrt(n) band {band:.3f} at n={n}: "
                f"serial dependence up to that size is undetectable, so block {floor} "
                "may be anti-conservative"
            )
        return floor, basis
    a = min(abs(phi), 0.95)
    wanted = math.ceil(a / ((1 - a) ** 2 * BLOCK_TOLERANCE))
    block = max(floor, min(wanted, n // MIN_BLOCKS))
    basis["rule"] = (
        f"ceil(|acf1|/((1-|acf1|)^2*{BLOCK_TOLERANCE}))={wanted}, "
        f"capped at n//{MIN_BLOCKS}={n // MIN_BLOCKS}, floor {floor}"
    )
    if block < wanted:
        basis["caveat"] = (
            f"block capped at {block} < {wanted} (n={n}, >= {MIN_BLOCKS} blocks): "
            f"residual anti-conservatism (calibration: {CAPPED_BLOCK_CALIBRATION})"
        )
    return block, basis


def permutation_block(protocol, depth, target=None) -> int:
    """Block length covering the outcome overlap; a shorter declared block is refused.

    A declared ``protocol.block`` is used as is. The default (``block=0``) is
    data-driven from ``target`` (the sampled discovery labels) when given, and
    the overlap floor ``depth + 1`` otherwise.
    """
    needed = depth + 1
    if protocol.block and protocol.block < needed:
        raise ValueError("permutation block shorter than outcome overlap")
    if protocol.block:
        return protocol.block
    if target is None:
        return needed
    return autocorrelation_block(target, depth)[0]


def holdout_block(protocol, frozen_block: int, depth: int, n: int) -> int:
    """The frozen discovery block: holdout labels never re-estimate it.

    Capped so at least ``MIN_BLOCKS`` blocks remain, never below the holdout
    overlap floor. A declared block is used as is.
    """
    if protocol.block:
        return permutation_block(protocol, depth)
    return max(depth + 1, min(frozen_block, max(1, n // MIN_BLOCKS)))


# --- (a) split first, then label -------------------------------------------------


def build_family_rows(
    protocol, features, price, horizon, window, label="return", known_at=None
):
    """Build one target family's rows for one window, labelling only after the split.

    ``features``: DataFrame on a sorted, unique, tz-aware session index; the
    value at t must be computable from data up to t. ``price``: the target
    level on the same index. ``horizon``: forward label length in sessions.
    ``label``: ``return`` (end/start - 1, for prices) or ``change`` (end -
    start, for rates/spreads/indexes whose level can be 0 or negative).

    Every price outside the window ([start or first row, split) for discovery,
    [split, end) for holdout) is blanked before any label is computed, so a
    label that would need such a price does not exist. Decisions are sampled
    from the window's first session every ``max(step, horizon)`` sessions
    (``horizon_spaced``) or every ``step`` sessions (``fixed_step_block_null``).
    NaN feature values become explicit ``None`` abstentions.
    ``known_at``: optional DataFrame shaped like ``features`` holding when each
    value became known (tz-aware timestamps). Without it a value is stamped as
    known at its decision, which is only honest for synthetic fixtures.
    """
    protocol.validate()
    if window not in ("discovery", "holdout") or horizon < 1 or label not in LABELS:
        raise ValueError("unknown window, horizon or label")
    index = features.index
    if (
        not isinstance(index, pd.DatetimeIndex)
        or index.tz is None
        or not index.is_monotonic_increasing
        or not index.is_unique
        or not price.index.equals(index)
        or list(features.columns) != list(protocol.features)
    ):
        raise ValueError("features/price must share a unique tz-aware session index")
    if known_at is not None and (
        not known_at.index.equals(index) or list(known_at.columns) != list(features.columns)
    ):
        raise ValueError("known_at must be shaped like the features")
    split, end = stamp(protocol.split), stamp(protocol.end)
    if window == "discovery":
        lo = stamp(protocol.start) if protocol.start else index[0]
        in_window = (index >= lo) & (index < split)
    else:
        in_window = (index >= split) & (index < end)
    # Blank out-of-window prices BEFORE labelling: purge by construction.
    visible = price.to_numpy(dtype=float, copy=True)
    visible[~in_window] = np.nan
    values = features.to_numpy(dtype=float)
    stamps = known_at
    step = (
        max(protocol.step, horizon)
        if protocol.sampling == "horizon_spaced"
        else protocol.step
    )
    rows = []
    for i in np.flatnonzero(in_window)[::step]:
        j = i + horizon
        if j >= len(index):
            continue
        start_price, end_price = visible[i], visible[j]
        if not (np.isfinite(start_price) and np.isfinite(end_price)) or (
            label == "return" and not start_price
        ):
            continue  # label needs a price outside the window (or missing): purged
        decided, labelled = index[i].isoformat(), index[j].isoformat()
        target = (
            end_price / start_price - 1 if label == "return" else end_price - start_price
        )
        rows.append(
            {
                "origin": protocol.origin,
                "decision_at": decided,
                "label_end": labelled,
                "target_known_at": labelled,
                "horizon": f"{horizon} sessions",
                **({} if label == "return" else {"label": label}),
                "target": float(target),
                "features": {
                    name: {
                        "value": float(v) if np.isfinite(v) else None,
                        "known_at": (
                            stamps.iat[i, k].isoformat()
                            if stamps is not None
                            and np.isfinite(v)
                            and not pd.isna(stamps.iat[i, k])
                            else decided
                        ),
                    }
                    for k, (name, v) in enumerate(zip(protocol.features, values[i]))
                },
            }
        )
    return rows


# --- (c) block-permutation null --------------------------------------------------


@lru_cache(maxsize=64)
def block_permutations(n, block, perms, seed):
    """perms x n exact permutations of 0..n-1 that reorder contiguous blocks.

    Blocks keep their internal order; a short final block moves as one unit.
    Seeded only by (seed, n, block, perms), so results do not depend on trial
    order. Cached and returned read-only.
    """
    rng = np.random.default_rng([seed, n, block, perms])
    positions = np.arange(n)
    n_blocks = -(-n // block)
    rank = rng.random((perms, n_blocks)).argsort(axis=1).argsort(axis=1)
    keys = rank[:, positions // block] * block + positions % block
    index = keys.argsort(axis=1, kind="stable").astype(np.int32)
    index.setflags(write=False)
    return index


def block_permutation_pvalue(x, y, block, perms, seed):
    """Two-sided p for corr(x, y) against block permutations of y.

    Returns (observed correlation, p). Resolution is 1/(perms+1).
    """
    xc = np.asarray(x, dtype=float) - np.mean(x)
    yc = np.asarray(y, dtype=float) - np.mean(y)
    scale = math.sqrt(float(xc @ xc) * float(yc @ yc))
    observed = float(xc @ yc) / scale
    null = (yc[block_permutations(len(yc), block, perms, seed)] @ xc) / scale
    extreme = int((np.abs(null) >= abs(observed) - 1e-12).sum())
    return observed, (1 + extreme) / (perms + 1)


# --- (d) BH-FDR over the whole run -----------------------------------------------


def bh_adjusted(pvalues):
    """Benjamini-Hochberg step-up adjusted p-values over ALL given trials.

    ``adjusted <= q`` is exactly the BH rejection set at FDR q. The caller must
    pass every declared trial (untestable ones as 1.0): the length is the
    denominator.
    """
    p = np.asarray(pvalues, dtype=float)
    if not len(p):
        return []
    if not np.all((p >= 0) & (p <= 1)):
        raise ValueError("p-values must lie in [0, 1]")
    order = np.argsort(p, kind="stable")
    scaled = p[order] * len(p) / np.arange(1, len(p) + 1)
    scaled = np.minimum.accumulate(scaled[::-1])[::-1]
    adjusted = np.empty(len(p))
    adjusted[order] = np.minimum(scaled, 1.0)
    return adjusted.tolist()


def corrected_p(p, total):
    """Bonferroni over a whole family; used for the holdout confirmation family."""
    return min(1.0, p * total)


# --- measurement -----------------------------------------------------------------


def measure(rows, feature, protocol, block):
    pairs = [
        (row["features"][feature]["value"], row["target"])
        for row in rows
        if row["features"][feature]["value"] is not None
    ]
    if len(pairs) < protocol.min_n:
        return {"n": len(pairs), "r": None, "p": 1.0, "status": "insufficient_data"}
    x, y = (np.array(column, dtype=float) for column in zip(*pairs))
    if protocol.statistic == "spearman":
        x, y = rankdata(x), rankdata(y)
    # max_lag=0 avoids a hidden search over lags. Every declared feature is a trial.
    result = compute_lagged_correlation(pd.Series(x), pd.Series(y), max_lag=0)
    if result.get("error"):
        return {"n": len(pairs), "r": None, "p": 1.0, "status": result["error"]}
    _, p = block_permutation_pvalue(x, y, block, protocol.perms, protocol.seed)
    return {
        "n": len(pairs),
        "r": result["optimal_correlation"],
        "p": p,
        "status": "tested",
    }


def as_families(protocol, rows):
    """Normalise rows to {family: rows}; a bare list is the single declared family."""
    if isinstance(rows, dict):
        if set(rows) != set(protocol.families):
            raise ValueError("family universe mismatch")
        return {family: rows[family] for family in protocol.families}
    if len(protocol.families) != 1:
        raise ValueError("family universe mismatch")
    return {protocol.families[0]: rows}


def check_read_rows(protocol, families, panel, window):
    """The read origin is proven by re-deriving rows from a verified panel, not by its label."""
    if protocol.origin != LATEST_VINTAGE_ORIGIN:
        if panel is not None:
            raise ValueError(
                "a latest-vintage panel is only accepted with origin latest_vintage_read"
            )
        return
    # Lazy import: the adapter imports this module.
    from analysis.research_real_panel import verify_latest_vintage_rows

    verify_latest_vintage_rows(panel, protocol, families, window)


def self_lag_result(rows, feature, protocol, block):
    """A proxy trial: measured for the record, never testable, p = 1.0 in BH."""
    measured = measure(rows, feature, protocol, block)
    return {
        "n": measured["n"],
        "r": None,
        "p": 1.0,
        "status": SELF_LAG,
        "self_lag_r": measured["r"],
        "self_lag_p": measured["p"] if measured["status"] == "tested" else None,
    }


def discover(protocol, discovery_rows, panel=None):
    """This API never receives holdout rows. Freeze its result before evaluation.

    ``panel`` is required for (and only accepted with) ``latest_vintage_read``.
    """
    families = as_families(protocol, discovery_rows)
    check_read_rows(protocol, families, panel, "discovery")
    self_lag = set(protocol.self_lag)
    ledger, horizons, blocks, block_basis = [], {}, {}, {}
    for family, rows in families.items():
        depth = validate_rows(rows, protocol, "discovery")
        targets = [row["target"] for row in rows]  # discovery labels only
        if protocol.block:
            blocks[family] = permutation_block(protocol, depth)
            block_basis[family] = {"rule": "declared", "block": protocol.block}
        else:
            blocks[family], basis = autocorrelation_block(targets, depth)
            block_basis[family] = {**basis, "block": blocks[family]}
        horizons[family] = outcome_horizon(rows[0]) if rows else None
        for feature in protocol.features:
            if excluded(feature):
                result = {"n": 0, "r": None, "p": 1.0, "status": "excluded_telemetry"}
            elif (family, feature) in self_lag:
                result = self_lag_result(rows, feature, protocol, blocks[family])
            else:
                result = measure(rows, feature, protocol, blocks[family])
            ledger.append(
                {
                    "trial_id": digest([protocol.run_id, family, feature]),
                    "family": family,
                    "feature": feature,
                    "block": blocks[family],
                    **result,
                }
            )
    # (d) one BH family: the whole run, untestable trials included at p = 1.0.
    # Only horizon-spaced runs may select (the fixed-step null is anti-conservative).
    eligible = protocol.sampling in CANDIDATE_SAMPLING
    for trial, adjusted in zip(ledger, bh_adjusted([t["p"] for t in ledger])):
        trial["adjusted_p"] = adjusted
        trial["selected"] = (
            eligible and trial["status"] == "tested" and adjusted <= protocol.fdr_q
        )
    tested = sum(t["status"] == "tested" for t in ledger)
    # Block caveats only where trials could be tested (n >= min_n).
    block_caveats = [
        f"{family}: {basis['caveat']}"
        for family, basis in block_basis.items()
        if "caveat" in basis and basis["n"] >= protocol.min_n
    ]
    caveats = ([] if eligible else [FIXED_STEP_CAVEAT]) + block_caveats
    payload = {
        "protocol": asdict(protocol),
        "discovery_sha256": digest(families),
        "horizons": horizons,
        "blocks": blocks,
        "block_basis": block_basis,
        "trial_count": len(ledger),
        "self_lag_count": sum(t["status"] == SELF_LAG for t in ledger),
        "tested_count": tested,
        "untestable_count": len(ledger) - tested,
        "min_attainable_p": 1 / (protocol.perms + 1),
        "ledger": ledger,
        "candidate_eligible": eligible,
        "caveats": caveats,
        "method": (
            f"fixed lag0 {protocol.statistic}; {protocol.sampling} sampling; "
            f"block-permutation null ({protocol.perms} perms, seed {protocol.seed}, "
            f"block {'declared' if protocol.block else 'from discovery target acf1'}); "
            "self_lag proxy trials never select; "
            f"BH-FDR q={protocol.fdr_q} over the full declared universe incl. "
            "untestable; holdout Bonferroni over frozen selections"
            + ("" if eligible else f"; CAVEAT: {FIXED_STEP_CAVEAT}")
            + (
                f"; CAVEAT: data-driven block may be anti-conservative in "
                f"{len(block_caveats)} of {len(block_basis)} families (block capped "
                "or acf1 undetectable at small n; see caveats)"
                if block_caveats
                else ""
            )
        ),
        "state": "DISCOVERY_FROZEN",
        "promotion_allowed": False,
    }
    return {"payload": payload, "sha256": digest(payload)}


def evaluate_holdout(frozen, holdout_rows, panel=None):
    payload = frozen["payload"]
    if digest(payload) != frozen["sha256"]:
        raise ValueError("frozen discovery manifest changed")
    protocol = protocol_from_payload(payload["protocol"])
    families = as_families(protocol, holdout_rows)
    check_read_rows(protocol, families, panel, "holdout")
    blocks = {}
    for family, rows in families.items():
        blocks[family] = holdout_block(
            protocol,
            payload["blocks"][family],
            validate_rows(rows, protocol, "holdout"),
            len(rows),
        )
        if rows and payload["horizons"][family] != outcome_horizon(rows[0]):
            raise ValueError("holdout horizon differs from frozen discovery")
    selected = [trial for trial in payload["ledger"] if trial["selected"]]
    if selected and protocol.sampling not in CANDIDATE_SAMPLING:
        raise ValueError("fixed-step manifests are diagnostic only")
    self_lag = set(protocol.self_lag)
    if any((t["family"], t["feature"]) in self_lag for t in selected):
        raise ValueError("self_lag proxy trials can never become candidates")
    checks, candidates = [], []
    for trial in selected:
        family = trial["family"]
        result = measure(families[family], trial["feature"], protocol, blocks[family])
        adjusted = corrected_p(result["p"], len(selected))
        survives = (
            result["status"] == "tested"
            and adjusted <= protocol.alpha
            and result["r"] * trial["r"] > 0
        )
        checks.append(
            {
                "trial_id": trial["trial_id"],
                **result,
                "adjusted_p": adjusted,
                "retrospective_survivor": survives,
            }
        )
        if survives:
            specification = {
                "family": family,
                "feature": trial["feature"],
                "direction": 1 if trial["r"] > 0 else -1,
                "discovery_manifest": frozen["sha256"],
                "horizon": payload["horizons"][family],
                "forward_start_not_before": protocol.end,
                "forward_rule": "not implemented: prospective timestamped predictions required",
                "origin": protocol.origin,
            }
            candidates.append(
                {
                    "specification": specification,
                    "sha256": digest(specification),
                    "state": candidate_state(protocol.origin),
                    "promotion_allowed": False,
                }
            )
    return {
        "discovery_manifest": frozen["sha256"],
        "holdout_sha256": digest(families),
        "holdout_blocks": blocks,
        "holdout_checks": checks,
        "candidates": candidates,
        "state": ORIGINS[protocol.origin],
        "promotion_allowed": False,
        "forward_evidence_count": 0,
    }


def write_once(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)


def run_proof(protocol, discovery_rows, holdout_rows, output, panel=None):
    """Local receipt directory must be new: reruns cannot overwrite consumed evidence."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    frozen = discover(protocol, discovery_rows, panel)
    write_once(output / "discovery-frozen.json", frozen)
    # Read back the persisted freeze; holdout cannot alter search or trial universe.
    result = evaluate_holdout(
        json.loads((output / "discovery-frozen.json").read_text()),
        holdout_rows,
        panel,
    )
    write_once(output / "holdout-result.json", result)
    return result
