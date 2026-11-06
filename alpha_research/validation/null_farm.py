"""Null-calibration farm: seeded synthetic worlds through GRID's real tests (S7, 2026-11-06).

Measures the empirical size (false-positive rate at nominal 5%) and power of
GRID's statistical tests by pushing worlds with a *known* answer through the
real code paths, unchanged:

* ``gauntlet_perm``: :func:`alpha_research.validation.gauntlet.permutation_test`
  (block permutation of signal dates, long-short Sharpe, #979), plus the
  one-trial deflated Sharpe (PSR against 0) computed exactly as
  :func:`~alpha_research.validation.gauntlet.run_gauntlet` does.
* ``gauntlet_full``: :func:`~alpha_research.validation.gauntlet.run_gauntlet`
  end to end (verdict, permutation p, DSR, CV, subsample, decay).
* ``dsr_best_of``: the best of ``n_trials`` strategy return series, deflated
  with :func:`~alpha_research.validation.gauntlet.deflated_sharpe_ratio` and the
  cross-trial Sharpe variance (Bailey & Lopez de Prado 2014), as
  ``run_gauntlet`` does with ``trial_sharpes``.
* ``s09_discover``: S09/S09b/S11 discovery,
  :func:`analysis.offline_research_proof.build_family_rows` then
  :func:`~analysis.offline_research_proof.discover` (data-driven or declared
  block, block-permutation p, BH run selection or ledger Holm). Read-only use;
  that module is hash-pinned by E0/VS1 and is never modified here.

Worlds (all independent of the tested feature unless ``effect`` is set):
AR(1) targets, persistent (AR(1)) features, Markov regime switches with
regime-dependent drifts and volatility, jump-diffusion returns (Student-t plus
common and idiosyncratic jumps) and overlapping horizons (``horizon`` > 1:
h-day forward-return sums in the panel, fixed-step sampling in S09). Signal
worlds plant a small, specified effect: a cross-sectional IC for the panel, a
per-step correlation for S09, a per-period Sharpe for DSR.

Everything is a pure function of a JSON cell spec and a seed: no DB, no
network, no clock. A *shard* (cell + replicate range) is the unit of parallel
work; ``scripts/null_farm.py`` plans, runs (multi-process, resumable, per box)
and summarizes, and ``scripts/worker.py`` runs a shard as a ``SIMULATION`` job
with ``kind="null_calibration"``.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from alpha_research.validation import gauntlet
from alpha_research.validation.metrics import long_short_returns, sharpe_ratio
from analysis import offline_research_proof as orp

FARM_VERSION = "null-farm-v1"
ALPHA = 0.05
SIZE_BAND = (0.04, 0.06)
TESTS = ("gauntlet_perm", "gauntlet_full", "dsr_best_of", "s09_discover")
PERIODS_PER_YEAR = 252
DAILY_SCALE = 0.01  # panel and strategy returns are ~1% daily

# World = target (returns / label increments) generator + feature generator.
# Cell ``params["target"]`` / ``params["feature"]`` override these per cell.
WORLDS: dict[str, dict[str, dict[str, Any]]] = {
    "iid": {"target": {"kind": "iid"}, "feature": {"kind": "iid"}},
    "persistent_feature": {
        "target": {"kind": "iid"},
        "feature": {"kind": "ar1", "phi": 0.95},
    },
    "ar1_target": {
        "target": {"kind": "ar1", "phi": 0.35},
        "feature": {"kind": "ar1", "phi": 0.95},
    },
    "regime_switch": {
        "target": {"kind": "regime", "stay": 0.98, "vol": [1.0, 3.0], "mu_sd": 0.25},
        "feature": {"kind": "regime", "stay": 0.98, "phi": 0.9, "level_sd": 1.0},
    },
    "jump_diffusion": {
        "target": {"kind": "jump", "df": 4, "p_jump": 0.03, "jump_sd": 4.0},
        "feature": {"kind": "ar1", "phi": 0.95},
    },
}


# --- cells, shards and seeds --------------------------------------------------------


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def cell_id(cell: Mapping[str, Any]) -> str:
    """Stable 12-hex id of a cell spec (everything except ``reps`` and ``id``)."""
    body = {k: v for k, v in cell.items() if k not in ("reps", "id")}
    return hashlib.sha256(canonical(body).encode()).hexdigest()[:12]


def make_cell(
    test: str,
    world: str,
    n: int,
    *,
    block: int = 0,
    reps: int = 1000,
    **params: Any,
) -> dict[str, Any]:
    """A validated cell spec with its id. ``params`` are test/world options."""
    if test not in TESTS:
        raise ValueError(f"unknown test {test!r}")
    if world not in WORLDS:
        raise ValueError(f"unknown world {world!r}")
    if int(n) != n or n < 30:
        raise ValueError("n must be an integer >= 30")
    if int(block) != block or block < 0 or int(reps) != reps or reps < 1:
        raise ValueError("block must be >= 0 and reps >= 1")
    cell = {"test": test, "world": world, "n": int(n), "block": int(block), "params": params}
    cell["id"] = cell_id(cell)
    cell["reps"] = int(reps)
    return cell


def make_shards(
    cells: Iterable[Mapping[str, Any]], shard_reps: int, base_seed: int
) -> list[dict[str, Any]]:
    """Split every cell's replicates into shards of at most ``shard_reps``."""
    if shard_reps < 1:
        raise ValueError("shard_reps must be >= 1")
    shards = []
    for cell in cells:
        for start in range(0, cell["reps"], shard_reps):
            stop = min(cell["reps"], start + shard_reps)
            shards.append(
                {
                    "cell": dict(cell),
                    "rep_start": start,
                    "rep_stop": stop,
                    "base_seed": int(base_seed),
                    "shard_id": f"{cell['id']}-{start:05d}-{stop:05d}",
                }
            )
    return shards


def replicate_seed(base_seed: int, cid: str, rep: int) -> np.random.SeedSequence:
    """Independent stream per (base seed, cell, replicate); shard layout never matters."""
    return np.random.SeedSequence([int(base_seed), int(cid, 16) % (2**63), int(rep)])


# --- generators ----------------------------------------------------------------------


def _ar1(innovations: np.ndarray, phi: float) -> np.ndarray:
    """Stationary AR(1) driven by ``innovations`` (rows = time), unit innovation sd."""
    if not -1 < phi < 1:
        raise ValueError("AR(1) phi must lie in (-1, 1)")
    out = np.empty_like(innovations, dtype=float)
    out[0] = innovations[0] / math.sqrt(1 - phi**2)
    for t in range(1, len(innovations)):
        out[t] = phi * out[t - 1] + innovations[t]
    return out


def _regime_path(rng: np.random.Generator, n: int, stay: float) -> np.ndarray:
    """Two-state Markov chain with probability ``stay`` of keeping the state."""
    flips = rng.random(n) >= stay
    flips[0] = bool(rng.integers(2))
    return np.cumsum(flips) % 2


def generate(spec: Mapping[str, Any], rng: np.random.Generator, shape: tuple[int, int]) -> np.ndarray:
    """One target or feature block of ``shape`` (time x columns), roughly unit scale."""
    kind = spec.get("kind", "iid")
    n, k = shape
    if kind == "iid":
        return rng.standard_normal(shape)
    if kind == "ar1":
        return _ar1(rng.standard_normal(shape), float(spec.get("phi", 0.0)))
    if kind == "regime":
        state = _regime_path(rng, n, float(spec.get("stay", 0.98)))
        if "level_sd" in spec:  # feature: AR(1) around a regime-dependent level
            levels = rng.normal(0.0, float(spec["level_sd"]), size=(2, k))
            base = _ar1(rng.standard_normal(shape), float(spec.get("phi", 0.9)))
            return base + levels[state]
        vol = np.asarray(spec.get("vol", [1.0, 3.0]), dtype=float)
        drift = rng.normal(0.0, float(spec.get("mu_sd", 0.25)), size=(2, k))
        return drift[state] + vol[state][:, None] * rng.standard_normal(shape)
    if kind == "jump":
        df = float(spec.get("df", 4))
        t_scale = math.sqrt((df - 2) / df) if df > 2 else 1.0
        base = rng.standard_t(df, size=shape) * t_scale
        p_jump = float(spec.get("p_jump", 0.03))
        jump_sd = float(spec.get("jump_sd", 4.0))
        common = (rng.random((n, 1)) < p_jump) * rng.normal(0.0, jump_sd, size=(n, 1))
        betas = rng.uniform(0.5, 1.5, size=(1, k))
        idio = (rng.random(shape) < p_jump / 2) * rng.normal(0.0, jump_sd, size=shape)
        return base + common * betas + idio
    raise ValueError(f"unknown generator kind {kind!r}")


def world_specs(cell: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    world = WORLDS[cell["world"]]
    params = cell.get("params", {})
    target = {**world["target"], **params.get("target", {})}
    feature = {**world["feature"], **params.get("feature", {})}
    return target, feature


def _zscore_rows(values: np.ndarray) -> np.ndarray:
    centred = values - values.mean(axis=1, keepdims=True)
    sd = centred.std(axis=1, keepdims=True)
    return np.divide(centred, sd, out=np.zeros_like(centred), where=sd > 0)


def panel_world(
    cell: Mapping[str, Any], rng: np.random.Generator
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """(signal, forward_returns, overlap_depth) on ``n`` dates for the gauntlet.

    The gauntlet pairs ``signal[t]`` with ``forward_returns[t + 1]``. With
    ``horizon`` h > 1, ``forward_returns[t]`` is the overlapping h-day sum of
    daily returns ``t .. t + h - 1``; ``overlap_depth`` is ``h - 1`` when the
    cell declares it (``declare_overlap``), else 0. ``effect`` plants a
    cross-sectional IC: ``daily[t + 1] = effect * z(signal[t]) + sqrt(1 - effect^2) * noise``.
    """
    params = cell.get("params", {})
    n, n_assets = cell["n"], int(params.get("n_assets", 20))
    horizon = int(params.get("horizon", 1))
    effect = float(params.get("effect", 0.0))
    target_spec, feature_spec = world_specs(cell)
    total = n + horizon + 1
    noise = generate(target_spec, rng, (total, n_assets))
    noise = noise / noise.std()
    feature = generate(feature_spec, rng, (total, n_assets))
    daily = noise.copy()
    if effect:
        z = _zscore_rows(feature)
        daily[1:] = effect * z[:-1] + math.sqrt(1 - effect**2) * noise[1:]
    daily *= DAILY_SCALE
    if horizon > 1:
        csum = np.vstack([np.zeros((1, n_assets)), np.cumsum(daily, axis=0)])
        forward = csum[horizon:] - csum[:-horizon]  # forward[t] = sum daily[t .. t+h-1]
    else:
        forward = daily
    index = pd.bdate_range("2001-01-02", periods=n)
    columns = [f"A{i:02d}" for i in range(n_assets)]
    signal = pd.DataFrame(feature[:n], index=index, columns=columns)
    fwd = pd.DataFrame(forward[:n], index=index, columns=columns)
    depth = horizon - 1 if params.get("declare_overlap", True) else 0
    return signal, fwd, depth


def strategy_returns(cell: Mapping[str, Any], rng: np.random.Generator) -> np.ndarray:
    """``n`` x ``n_trials`` strategy return series for the DSR test.

    Trials share a common factor with correlation ``trial_rho``; ``horizon`` h
    > 1 makes each series an overlapping h-period sum (autocorrelated, as
    overlapping holding periods are). ``effect_sr`` (annualized) is the true
    Sharpe of trial 0; the others have a true Sharpe of 0.
    """
    params = cell.get("params", {})
    n, k = cell["n"], int(params.get("n_trials", 1))
    horizon = int(params.get("horizon", 1))
    rho = float(params.get("trial_rho", 0.0))
    target_spec, _ = world_specs(cell)
    raw = generate(target_spec, rng, (n + horizon - 1, k))
    if rho:
        common = generate(target_spec, rng, (n + horizon - 1, 1))
        raw = math.sqrt(rho) * common + math.sqrt(1 - rho) * raw
    if horizon > 1:
        csum = np.vstack([np.zeros((1, k)), np.cumsum(raw, axis=0)])
        raw = csum[horizon:] - csum[:-horizon]
    out = raw / raw.std() * DAILY_SCALE
    effect_sr = float(params.get("effect_sr", 0.0))
    if effect_sr:
        out[:, 0] += effect_sr / math.sqrt(PERIODS_PER_YEAR) * out[:, 0].std()
    return out


def s09_world(
    cell: Mapping[str, Any], rng: np.random.Generator, run_id: str, perm_seed: int
) -> tuple[orp.Protocol, pd.DataFrame, pd.Series, int]:
    """(protocol, features, level, horizon) shaped for ``build_family_rows``.

    Label increments ``d`` come from the target generator, the ``n_features``
    features from the feature generator; the label of a decision at session t
    is the ``change`` of ``level`` over (t, t + h]. ``effect`` plants
    ``corr(d[t], x0[t]) ~ effect`` (``d[t]`` is realized after ``x0[t]`` is
    known). Sessions are sized so discovery yields exactly ``n`` rows.
    """
    params = cell.get("params", {})
    n, horizon = cell["n"], int(params.get("horizon", 1))
    sampling = params.get("sampling", "horizon_spaced")
    m = int(params.get("n_features", 3))
    effect = float(params.get("effect", 0.0))
    spacing = horizon if sampling == "horizon_spaced" else 1
    split_pos = (n - 1) * spacing + horizon + 1
    sessions = split_pos + 3
    target_spec, feature_spec = world_specs(cell)
    d = generate(target_spec, rng, (sessions, 1))[:, 0]
    d = (d - d.mean()) / d.std()
    x = generate(feature_spec, rng, (sessions, m))
    if effect:
        zx = (x[:, 0] - x[:, 0].mean()) / x[:, 0].std()
        d = effect * zx + math.sqrt(1 - effect**2) * d
    level = np.concatenate([[0.0], np.cumsum(d)[:-1]])  # level[t+1] - level[t] = d[t]
    index = pd.bdate_range("2001-01-02", periods=sessions, tz="UTC")
    names = tuple(f"x{i}" for i in range(m))
    selection = params.get("selection", "bh_run")
    extra: dict[str, Any] = {}
    if selection == orp.LEDGER_SELECTION:
        extra = {
            "selection": selection,
            "selection_alpha": float(params.get("selection_alpha", ALPHA)),
            "allocation_sha256": hashlib.sha256(run_id.encode()).hexdigest(),
            "trials": tuple(("target", name) for name in names),
            "start": index[0].isoformat(),
        }
    protocol = orp.Protocol(
        run_id=run_id,
        features=names,
        split=index[split_pos].isoformat(),
        end=index[sessions - 1].isoformat(),
        min_n=30,
        sampling=sampling,
        step=1,
        block=int(cell["block"]),
        perms=int(params.get("perms", 1999)),
        seed=int(perm_seed),
        statistic=params.get("statistic", "pearson"),
        **extra,
    )
    features = pd.DataFrame(x, index=index, columns=list(names))
    return protocol, features, pd.Series(level, index=index), horizon


# --- one replicate per test ------------------------------------------------------------


def _gauntlet_psr(signal: pd.DataFrame, fwd: pd.DataFrame, top_n: int, cost_bps: float) -> dict:
    """One-trial DSR (PSR against SR0 = 0) exactly as ``run_gauntlet`` computes it."""
    ls_ret = long_short_returns(signal, fwd, top_n, cost_bps)
    arr = ls_ret.dropna().values
    if len(arr) < 2:
        return {"dsr": None, "dsr_passed": False}
    skew = float(stats.skew(arr)) if len(arr) > 10 else 0.0
    kurt = float(stats.kurtosis(arr, fisher=False)) if len(arr) > 10 else 3.0
    dsr = gauntlet.deflated_sharpe_ratio(
        sharpe_ratio(ls_ret, annualize=1), 1, len(arr), 0.0, skew, kurt
    )
    return {"dsr": dsr.dsr, "dsr_passed": dsr.passed}


def _perm_seed(seq: np.random.SeedSequence) -> int:
    return int(seq.spawn(1)[0].generate_state(1)[0])


def replicate(cell: Mapping[str, Any], rep: int, base_seed: int) -> dict[str, Any]:
    """Run one replicate of ``cell``; returns a flat, JSON-ready record.

    ``reject`` is the test's own decision at nominal 5%: gauntlet ``perm_p <
    0.05``, DSR ``passed`` (dsr > 0.95), S09 trial-0 ``p <= 0.05``.
    """
    cid = cell["id"]
    seq = replicate_seed(base_seed, cid, rep)
    perm_seed = _perm_seed(seq)
    rng = np.random.default_rng(seq)
    params = cell.get("params", {})
    test = cell["test"]
    record: dict[str, Any] = {"cell_id": cid, "rep": int(rep)}
    if test in ("gauntlet_perm", "gauntlet_full"):
        signal, fwd, depth = panel_world(cell, rng)
        top_n = int(params.get("top_n", 5))
        cost = float(params.get("cost_bps", 10.0))
        perms = int(params.get("perms", 249))
        if test == "gauntlet_perm":
            perm = gauntlet.permutation_test(
                signal, fwd, perms, top_n, cost, seed=perm_seed,
                block=cell["block"], overlap_depth=depth,
            )
            record.update(
                p=perm.p_value,
                reject=bool(perm.p_value < ALPHA),
                block_used=perm.block,
                observed_sharpe=perm.observed_sharpe,
                **_gauntlet_psr(signal, fwd, top_n, cost),
            )
        else:
            result = gauntlet.run_gauntlet(
                signal, fwd, n_models_tested=1, top_n=top_n, cost_bps=cost,
                n_permutations=perms, seed=perm_seed, block=cell["block"],
                overlap_depth=depth,
            )
            record.update(
                p=result.permutation_p,
                reject=bool(result.permutation_passed),
                block_used=result.permutation_block,
                observed_sharpe=result.observed_sharpe,
                dsr=result.deflated_sharpe,
                dsr_passed=result.deflated_sharpe_passed,
                verdict=result.verdict,
                robust=result.verdict == "ROBUST",
                marginal_or_better=result.verdict in ("ROBUST", "MARGINAL"),
                cv_consistency=result.cv_consistency,
                subsample_stability=result.subsample_stability,
                decay_monotonic=result.decay_monotonic,
            )
    elif test == "dsr_best_of":
        returns = strategy_returns(cell, rng)
        series = [pd.Series(returns[:, j]) for j in range(returns.shape[1])]
        srs = [sharpe_ratio(s, annualize=1) for s in series]
        best = int(np.argmax(srs))
        arr = series[best].dropna().values
        skew = float(stats.skew(arr)) if len(arr) > 10 else 0.0
        kurt = float(stats.kurtosis(arr, fisher=False)) if len(arr) > 10 else 3.0
        k = len(srs)
        variance = gauntlet.trial_sharpe_variance(srs) if k > 1 else 0.0
        dsr = gauntlet.deflated_sharpe_ratio(srs[best], k, len(arr), variance, skew, kurt)
        record.update(
            dsr=dsr.dsr,
            reject=dsr.passed,
            best_sharpe=srs[best],
            sr0=dsr.sr0,
            best_is_planted=best == 0,
        )
    elif test == "s09_discover":
        protocol, features, level, horizon = s09_world(
            cell, rng, f"nullfarm-{cid}-{rep}", perm_seed
        )
        rows = orp.build_family_rows(protocol, features, level, horizon, "discovery", label="change")
        if len(rows) != cell["n"]:
            raise RuntimeError(f"expected {cell['n']} discovery rows, built {len(rows)}")
        payload = orp.discover(protocol, rows)["payload"]
        ledger = {t["feature"]: t for t in payload["ledger"]}
        first = ledger["x0"]
        basis = payload["block_basis"]["target"]
        record.update(
            p=first["p"],
            reject=bool(first["status"] == "tested" and first["p"] <= ALPHA),
            status=first["status"],
            block_used=payload["blocks"]["target"],
            block_caveat="caveat" in basis,
            acf1=basis.get("acf1"),
            any_selected=any(t["selected"] for t in payload["ledger"]),
            x0_selected=bool(first["selected"]),
            min_p=min(t["p"] for t in payload["ledger"]),
        )
    else:  # pragma: no cover - make_cell refuses unknown tests
        raise ValueError(f"unknown test {test!r}")
    return record


def run_shard(shard: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every replicate of one shard (pure: same shard, same records)."""
    cell = shard["cell"]
    if cell_id(cell) != cell["id"]:
        raise ValueError("cell id does not match its spec")
    return [
        replicate(cell, rep, shard["base_seed"])
        for rep in range(int(shard["rep_start"]), int(shard["rep_stop"]))
    ]


# --- summary ---------------------------------------------------------------------------


def exact_permutation_level(perms: int, alpha: float = ALPHA, strict: bool = True) -> float:
    """P(reject) of a perfectly calibrated ``(1 + k) / (perms + 1)`` p-value.

    ``strict``: reject on ``p < alpha`` (the gauntlet); else ``p <= alpha`` (S09).
    """
    grid = np.arange(1, perms + 2) / (perms + 1)
    hits = grid < alpha if strict else grid <= alpha + 1e-15
    return float(hits.sum() / (perms + 1))


def rate(flags: Sequence[bool]) -> dict[str, Any]:
    """Rejection rate, Monte Carlo SE and 95% Wilson interval."""
    x = np.asarray([bool(f) for f in flags], dtype=float)
    n = len(x)
    if not n:
        return {"rate": None, "se": None, "n": 0, "ci95": None}
    p = float(x.mean())
    z = 1.959963984540054
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return {
        "rate": p,
        "se": math.sqrt(p * (1 - p) / n),
        "n": n,
        "ci95": [max(0.0, centre - half), min(1.0, centre + half)],
    }


def nominal_level(cell: Mapping[str, Any]) -> float:
    params = cell.get("params", {})
    if cell["test"] in ("gauntlet_perm", "gauntlet_full"):
        return exact_permutation_level(int(params.get("perms", 249)), strict=True)
    if cell["test"] == "s09_discover":
        return exact_permutation_level(int(params.get("perms", 1999)), strict=False)
    return ALPHA


def is_null(cell: Mapping[str, Any]) -> bool:
    params = cell.get("params", {})
    return not (params.get("effect") or params.get("effect_sr"))


def size_status(stat: Mapping[str, Any], band: tuple[float, float] = SIZE_BAND) -> str:
    """``in_band`` / ``high`` / ``low`` by the point estimate; ``*_sig`` if the CI also misses the band."""
    lo, hi = band
    r, ci = stat["rate"], stat["ci95"]
    if r is None:
        return "empty"
    if lo <= r <= hi:
        return "in_band"
    if r > hi:
        return "high_sig" if ci[0] > hi else "high"
    return "low_sig" if ci[1] < lo else "low"


SECONDARY_FLAGS = (
    "dsr_passed", "robust", "marginal_or_better", "any_selected", "x0_selected",
    "block_caveat", "best_is_planted",
)


def summarize(cells: Iterable[Mapping[str, Any]], records: Iterable[Mapping[str, Any]]) -> list[dict]:
    """One row per cell: size (null) or power (signal) with MC SE, plus secondary rates."""
    by_cell: dict[str, list[Mapping[str, Any]]] = {}
    for rec in records:
        by_cell.setdefault(rec["cell_id"], []).append(rec)
    rows = []
    for cell in cells:
        recs = sorted(by_cell.get(cell["id"], []), key=lambda r: r["rep"])
        seen: dict[int, Mapping[str, Any]] = {r["rep"]: r for r in recs}  # de-duplicate reruns
        recs = list(seen.values())
        primary = rate([r["reject"] for r in recs])
        row = {
            "cell_id": cell["id"],
            "test": cell["test"],
            "world": cell["world"],
            "n": cell["n"],
            "block": cell["block"],
            "params": cell.get("params", {}),
            "kind": "size" if is_null(cell) else "power",
            "reps_planned": cell["reps"],
            "reject": primary,
            "nominal_exact": nominal_level(cell),
        }
        if row["kind"] == "size":
            row["status"] = size_status(primary)
        for flag in SECONDARY_FLAGS:
            values = [r[flag] for r in recs if flag in r and r[flag] is not None]
            if values:
                row[flag] = rate(values)
        blocks = [r["block_used"] for r in recs if r.get("block_used") is not None]
        if blocks:
            row["block_used"] = {
                "mean": float(np.mean(blocks)),
                "min": int(np.min(blocks)),
                "max": int(np.max(blocks)),
            }
        rows.append(row)
    return rows


def describe(row: Mapping[str, Any]) -> str:
    """Short, human label of a cell's non-default parameters."""
    params = row.get("params", {})
    bits = [f"{k}={v}" for k, v in sorted(params.items()) if k not in ("target", "feature")]
    for key in ("target", "feature"):
        if key in params:
            bits.append(f"{key}={canonical(params[key])}")
    return ", ".join(bits)


def markdown_table(rows: Sequence[Mapping[str, Any]]) -> str:
    """Markdown summary: one line per cell."""
    lines = [
        "| test | world | n | block rule | params | kind | rate | MC SE | 95% CI | exact nominal | status | block used (mean) | secondary |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        r = row["reject"]
        if r["rate"] is None:
            continue
        secondary = "; ".join(
            f"{flag} {row[flag]['rate']:.3f}"
            for flag in SECONDARY_FLAGS
            if flag in row and row[flag]["rate"] is not None
        )
        block = row.get("block_used", {}).get("mean")
        lines.append(
            "| {test} | {world} | {n} | {rule} | {params} | {kind} | {rate:.3f} | {se:.3f} | "
            "{lo:.3f}-{hi:.3f} | {nom:.3f} | {status} | {block} | {sec} |".format(
                test=row["test"],
                world=row["world"],
                n=row["n"],
                rule="data-driven" if row["block"] == 0 else f"declared {row['block']}",
                params=describe(row) or "-",
                kind=f"{row['kind']} ({r['n']})",
                rate=r["rate"],
                se=r["se"],
                lo=r["ci95"][0],
                hi=r["ci95"][1],
                nom=row["nominal_exact"],
                status=row.get("status", "-"),
                block="-" if block is None else f"{block:.1f}",
                sec=secondary or "-",
            )
        )
    return "\n".join(lines) + "\n"
