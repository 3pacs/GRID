"""Module factory statistics (3pacs/GRID#1155): net when fires, graded by max-T.

Kept apart from :mod:`analysis.offline_research_proof` on purpose: that file is
part of the machinery the E0 v1 scorecard fingerprints
(``evals.e0.machinery.MACHINERY_FILES``), so it stays byte-identical and the
factory builds on it from here instead.

* ``statistic="net_when_fires"``: a feature is a module's firing signal: +1
  (long) or -1 (short) when it fires, ``None`` when it abstains. The measure is
  the mean of ``side * target - cost`` over the fires (``target`` a ``change``
  label in price units, ``cost`` per share), with its t statistic; the marginal
  p is one-sided on t against block permutations of the target.
* ``selection="max_t"``: Westfall-Young step-down max-T over every tested
  trial of the run. All families share each session permutation, so the bar
  is the null distribution of the run's best t: it scales with the number of
  trials, needs no per-trial permutation budget, and is valid under any
  dependence between trials. A trial is selected when its adjusted p is <=
  ``selection_alpha`` and its mean net is > 0. Ranking runs only: the factory
  never claims a discovery from them (forward live recording is the test).
* Origin ``intraday_dev_ranking``: rows built by ``analysis.module_factory``
  from staged dev bars; its state marks every result as dev ranking only.

Row validation, block choice and permutations are the offline proof's own
(:func:`~analysis.offline_research_proof.validate_rows`,
:func:`~analysis.offline_research_proof.autocorrelation_block`,
:func:`~analysis.offline_research_proof.block_permutations`).
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, fields

import numpy as np

from analysis import offline_research_proof as orp

NET_WHEN_FIRES = "net_when_fires"
MAX_T_SELECTION = "max_t"
FACTORY_ORIGIN = "intraday_dev_ranking"
ORIGINS = {**orp.ORIGINS, FACTORY_ORIGIN: "INTRADAY_DEV_RANKING_ONLY"}


@dataclass(frozen=True)
class FactoryProtocol(orp.Protocol):
    """An offline-proof protocol for a factory run: net_when_fires graded by max-T."""

    statistic: str = NET_WHEN_FIRES
    selection: str = MAX_T_SELECTION
    cost: float = 0.0  # per-share cost subtracted from every fire without its own row cost

    def validate(self):
        if self.origin not in ORIGINS:
            raise ValueError("real PIT/source/session contracts are not implemented")
        if self.statistic != NET_WHEN_FIRES or self.selection != MAX_T_SELECTION:
            raise ValueError("max_t selects net_when_fires runs, and only them")
        if not 0 < self.selection_alpha <= 0.10 or self.allocation_sha256:
            raise ValueError("max_t needs a run alpha in (0, 0.10] and no ledger allocation")
        if not (isinstance(self.cost, (int, float)) and math.isfinite(self.cost) and self.cost >= 0):
            raise ValueError("invalid frozen statistical protocol")
        if self.self_lag:
            raise ValueError("a factory run declares no self_lag trial")
        # Every other check is the offline proof's own, on the same values.
        base = {f.name: getattr(self, f.name) for f in fields(orp.Protocol)}
        orp.Protocol(
            **{
                **base,
                "origin": "exploratory_replay" if self.origin == FACTORY_ORIGIN else self.origin,
                "statistic": "pearson",
                "selection": "bh_run",
                "selection_alpha": 0.0,
            }
        ).validate()


def net_stats(sides, targets, cost):
    """(n, mean net, t) of ``side * target - cost`` over the fires; t is None when undefined.

    ``cost``: one value, or one per fire (a row's own ``cost``, in target units).
    """
    net = np.asarray(sides, dtype=float) * np.asarray(targets, dtype=float) - np.asarray(cost, dtype=float)
    n = len(net)
    mean = float(net.mean()) if n else None
    if n < 2:
        return n, mean, None
    sd = float(net.std(ddof=1))
    return n, mean, (mean / (sd / math.sqrt(n)) if sd > 0 else None)


def row_cost(row, protocol):
    """A row's cost in target units: its own ``cost`` when given, else the protocol's."""
    cost = row.get("cost", protocol.cost)
    if not (isinstance(cost, (int, float)) and math.isfinite(cost) and cost >= 0):
        raise ValueError("a row cost is a finite number >= 0")
    return float(cost)


def measure_net(rows, feature, protocol):
    """n, mean net and t of the module's fires.

    Its p-values (marginal and max-T adjusted, both one-sided) come from the
    run's shared permutations in :func:`max_t_adjusted`; ``p`` is a 1.0
    placeholder until then.
    """
    triples = [
        (row["features"][feature]["value"], row["target"], row_cost(row, protocol))
        for row in rows
        if row["features"][feature]["value"] is not None
    ]
    if any(side not in (1.0, -1.0) for side, _, _ in triples):
        raise ValueError("a net_when_fires signal is +1, -1 or None")
    if len(triples) < protocol.min_n:
        return {"n": len(triples), "r": None, "t": None, "p": 1.0, "status": "insufficient_data"}
    n, mean, t = net_stats(*zip(*triples))
    if t is None:
        return {"n": n, "r": mean, "t": None, "p": 1.0, "status": "zero_variance"}
    return {"n": n, "r": mean, "t": t, "p": 1.0, "status": "tested"}


def net_t_matrix(targets, sides, cost):
    """t of ``side * target - cost`` over each column's fires, for every row of ``targets``.

    ``targets``: perms x n (NaN = no label for that session); ``sides``: n x m
    with +1 / -1 at fires and 0 elsewhere; ``cost``: one value or one per
    session (it stays with the session's decision when the targets are
    permuted). Returns perms x m float32, -inf where a column has < 2
    labelled fires or zero variance. Matrix products only, using
    (s y - c)^2 = y^2 - 2 s y c + c^2 for s = +/-1.
    """
    labelled = np.isfinite(targets)
    mask = labelled.astype(float)
    y = np.where(labelled, targets, 0.0)
    fires = np.abs(sides)
    c = np.broadcast_to(np.asarray(cost, dtype=float), (sides.shape[0],))[:, None]
    n = mask @ fires
    total = y @ sides - mask @ (c * fires)
    squares = (y * y) @ fires - 2 * (y @ (sides * c)) + mask @ (c * c * fires)
    with np.errstate(divide="ignore", invalid="ignore"):
        mean = total / n
        var = (squares - n * mean**2) / (n - 1)
        t = mean / np.sqrt(var / n)
    t[~((n >= 2) & (var > 1e-18))] = -np.inf
    return t.astype(np.float32)


def max_t_adjusted(families, ledger, protocol, block):
    """Westfall-Young step-down max-T adjusted p over every tested trial (one-sided).

    One block permutation of the run's session sequence is applied to every
    family's targets, so each permutation yields a t for every trial at once and
    the run's best null t is compared with each observed t. Untestable trials
    get p = 1. Returns (marginal p, adjusted p, critical t at
    ``selection_alpha`` = the run's first-step bar, sessions), the p lists
    aligned with ``ledger``.
    """
    sessions = sorted({row["decision_at"][:10] for rows in families.values() for row in rows})
    at = {d: i for i, d in enumerate(sessions)}
    tested = [i for i, t in enumerate(ledger) if t["status"] == "tested"]
    adjusted, marginal = [1.0] * len(ledger), [1.0] * len(ledger)
    if not tested:
        return marginal, adjusted, None, len(sessions)
    perm = orp.block_permutations(len(sessions), block, protocol.perms, protocol.seed)
    null = np.empty((protocol.perms, len(tested)), dtype=np.float32)
    by_family = {}
    for col, i in enumerate(tested):
        by_family.setdefault(ledger[i]["family"], []).append((col, ledger[i]["feature"]))
    for family, members in by_family.items():
        rows = families[family]
        y = np.full(len(sessions), np.nan)
        cost = np.zeros(len(sessions))
        sides = np.zeros((len(sessions), len(members)))
        for row in rows:
            k = at[row["decision_at"][:10]]
            y[k] = row["target"]
            cost[k] = row_cost(row, protocol)
            for j, (_, feature) in enumerate(members):
                sides[k, j] = row["features"][feature]["value"] or 0.0
        null[:, [col for col, _ in members]] = net_t_matrix(y[perm], sides, cost)
    observed = np.array([ledger[i]["t"] for i in tested], dtype=np.float32)
    for col, i in enumerate(tested):
        marginal[i] = float((1 + int((null[:, col] >= observed[col] - 1e-6).sum())) / (protocol.perms + 1))
    order = np.argsort(-observed, kind="stable")
    # successive maxima from the bottom of the observed ranking (step-down)
    tail_max = np.maximum.accumulate(null[:, order[::-1]], axis=1)[:, ::-1]
    exceed = (tail_max >= observed[order][None, :] - 1e-6).sum(axis=0)
    step = np.maximum.accumulate((1 + exceed) / (protocol.perms + 1))
    for rank, col in enumerate(order):
        adjusted[tested[col]] = float(min(1.0, step[rank]))
    critical = float(np.quantile(null.max(axis=1), 1 - protocol.selection_alpha))
    return marginal, adjusted, critical, len(sessions)


def discover(protocol, discovery_rows, panel=None):
    """The offline proof's discovery, for a factory run. Never receives holdout rows.

    Same rows, validation, blocks and frozen payload as
    :func:`analysis.offline_research_proof.discover`; each trial is measured
    by :func:`measure_net` and the run is graded by :func:`max_t_adjusted`.
    """
    if not isinstance(protocol, FactoryProtocol):
        raise ValueError("factory discovery needs a FactoryProtocol")
    families = orp.as_families(protocol, discovery_rows)
    orp.check_read_rows(protocol, families, panel, "discovery")
    declared = set(protocol.trials) if protocol.trials else None
    ledger, horizons, blocks, block_basis = [], {}, {}, {}
    for family, rows in families.items():
        depth = orp.validate_rows(rows, protocol, "discovery")
        targets = [row["target"] for row in rows]  # discovery labels only
        if protocol.block:
            blocks[family] = orp.permutation_block(protocol, depth)
            block_basis[family] = {"rule": "declared", "block": protocol.block}
        else:
            blocks[family], basis = orp.autocorrelation_block(targets, depth)
            block_basis[family] = {**basis, "block": blocks[family]}
        horizons[family] = orp.outcome_horizon(rows[0]) if rows else None
        for feature in protocol.features:
            if declared is not None and (family, feature) not in declared:
                continue
            if orp.excluded(feature):
                result = {"n": 0, "r": None, "p": 1.0, "status": "excluded_telemetry"}
            else:
                result = measure_net(rows, feature, protocol)
            ledger.append(
                {
                    "trial_id": orp.digest([protocol.run_id, family, feature]),
                    "family": family,
                    "feature": feature,
                    "block": blocks[family],
                    **result,
                }
            )
    # Only horizon-spaced runs may select (the fixed-step null is anti-conservative).
    eligible = protocol.sampling in orp.CANDIDATE_SAMPLING
    joint_block = protocol.block or max(blocks.values(), default=1)
    marginal, adjusted_all, critical, n_sessions = max_t_adjusted(families, ledger, protocol, joint_block)
    for trial, p_marginal, adjusted in zip(ledger, marginal, adjusted_all):
        trial["p"] = p_marginal
        trial["adjusted_p"] = adjusted
        trial["selected"] = (
            eligible
            and trial["status"] == "tested"
            and adjusted <= protocol.selection_alpha
            and trial["r"] > 0
        )
    tested = sum(t["status"] == "tested" for t in ledger)
    max_t = {"block": joint_block, "critical_t": critical, "sessions": n_sessions, "tested": tested}
    block_caveats = [
        f"{family}: {basis['caveat']}"
        for family, basis in block_basis.items()
        if "caveat" in basis and basis["n"] >= protocol.min_n
    ]
    caveats = ([] if eligible else [orp.FIXED_STEP_CAVEAT]) + block_caveats
    payload = {
        "protocol": asdict(protocol),
        "discovery_sha256": orp.digest(families),
        "horizons": horizons,
        "blocks": blocks,
        "block_basis": block_basis,
        "trial_count": len(ledger),
        "self_lag_count": 0,
        "tested_count": tested,
        "untestable_count": len(ledger) - tested,
        "min_attainable_p": 1 / (protocol.perms + 1),
        "ledger": ledger,
        "candidate_eligible": eligible,
        "caveats": caveats,
        "max_t": max_t,
        "method": (
            "net when fires (side x target - cost; a row cost where given, else "
            f"{format(protocol.cost, 'g')}); {protocol.sampling} sampling; "
            f"block-permutation null ({protocol.perms} perms, seed {protocol.seed}, "
            f"block {'declared' if protocol.block else 'from discovery target acf1'}); "
            "self_lag proxy trials never select; "
            f"Westfall-Young step-down max-T at {protocol.selection_alpha:.6g} over "
            "every tested trial, one session permutation shared by all families "
            f"(block {joint_block}), selected only with mean net > 0"
            "; holdout Bonferroni over frozen selections"
            + ("" if eligible else f"; CAVEAT: {orp.FIXED_STEP_CAVEAT}")
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
    return {"payload": payload, "sha256": orp.digest(payload)}


__all__ = [
    "FACTORY_ORIGIN",
    "FactoryProtocol",
    "MAX_T_SELECTION",
    "NET_WHEN_FIRES",
    "discover",
    "max_t_adjusted",
    "measure_net",
    "net_stats",
    "net_t_matrix",
    "row_cost",
]
