"""GD10b: the generalization gate (analysis.generalization_gate).

Synthetic worlds only: no DB, no prices, no labels, no real sector result.

* breadth calibration: independent null worlds reproduce the binomial chance
  P(>=4 of 11 | 0.10) = 0.0185; in cross-correlated null worlds (shared factor
  rho = 0.3, 0.6) the sector-block permutation threshold keeps the false
  breadth rate <= 0.05 while the binomial threshold does not (4.1);
* dominance: one sector driving everything fails LOSO; one entity carrying 40%
  of a sector's IC fails the top-entity rule (4.2);
* forward: one SUPPORTED sector -> FORWARD_PENDING, two -> pass (4.3);
* witness: a missing or unwitnessed terminal record -> REFUSED (4.4);
* determinism: byte-identical verdict JSON with the spec hash embedded (4.5);
* the 10-sector (v8 STOP) branch uses its pre-declared threshold (4.6).
"""

from dataclasses import replace
from datetime import date, timedelta

import numpy as np
import pytest

import analysis.generalization_gate as gg
from analysis.panel_insider_density import block_signs, signflip_pvalues

FAST = replace(gg.GATE_SPEC_V1, perms=999, bootstrap=999)
N = 80
DATES = tuple((date(2020, 1, 3) + timedelta(days=28 * i)).isoformat() for i in range(N))
PREREG = "b" * 64
TERMINAL = {s: f"{i + 1:064x}" for i, s in enumerate(gg.EQUITY_SECTORS)}
FORWARD = {s: f"f{i + 1:063x}" for i, s in enumerate(gg.EQUITY_SECTORS)}
WITNESSED = frozenset(TERMINAL.values()) | frozenset(FORWARD.values())


def witnessed(sha: str) -> bool:
    return sha in WITNESSED


def exact_series(mean: float, seed: int, sd: float = 0.05) -> np.ndarray:
    """Noise with exactly zero sample mean, scaled to ``sd``, plus ``mean``."""
    z = np.random.default_rng(seed).standard_normal(N)
    z = (z - z.mean()) / z.std()
    return mean + sd * z


def result(sector, ic, *, direction=1, forward=None, contributions=None, coverage=None, kind="holdout_result"):
    if kind != "holdout_result":
        return gg.SectorResult(sector=sector, terminal_record_sha256=TERMINAL[sector], terminal_kind=kind,
                               prereg_sha256=PREREG, direction=direction)
    ic = np.asarray(ic, dtype=float)
    _mean, _two, one = signflip_pvalues(ic, 1, 999, 7, direction)  # the per-sector harness's p
    if contributions is None:
        contributions = [ic.sum() / 20] * 20  # 20 entities, 5% each
    return gg.SectorResult(
        sector=sector,
        terminal_record_sha256=TERMINAL[sector],
        terminal_kind=kind,
        prereg_sha256=PREREG,
        direction=direction,
        holdout_p_one_sided=float(one),
        block=1,
        ic_series=tuple(zip(DATES, ic.tolist())),
        entity_contributions=tuple((f"cik:{k}", float(c)) for k, c in enumerate(contributions)),
        coverage_stable_ic_series=tuple(zip(DATES, (ic if coverage is None else coverage).tolist())),
        forward_verdict=forward,
        forward_record_sha256=FORWARD[sector] if forward else None,
    )


def world(strong=6, mean=0.03, forward=2, **overrides):
    """``strong`` sectors with a real effect, the rest exact nulls."""
    out = []
    for i, sector in enumerate(gg.EQUITY_SECTORS):
        mu = mean if i < strong else 0.0
        fwd = gg.SUPPORTED if i < min(forward, strong) else None
        kwargs = overrides.get(sector, {})
        out.append(result(sector, kwargs.pop("ic", exact_series(mu, 100 + i)), forward=fwd, **kwargs))
    return out


# --- spec ---------------------------------------------------------------------------

GATE_SPEC_V1_SHA256 = "fa2fa2bd1b1ef5cb81168f393135795824991e08829b60f66a109decdf401174"


def test_spec_v1_is_pinned_and_valid():
    gg.GATE_SPEC_V1.validate()
    assert gg.GATE_SPEC_V1_SHA256 == gg.spec_sha256(gg.GATE_SPEC_V1)
    assert gg.GATE_SPEC_V1_SHA256 == GATE_SPEC_V1_SHA256, gg.GATE_SPEC_V1_SHA256
    assert gg.GATE_SPEC_V1.promotion_allowed is False


def test_spec_constants_match_their_sources():
    import analysis.panel_insider_density as v1
    from analysis.research_forward_log import SUPPORTED

    assert gg.EQUITY_SECTORS == tuple(v1.EQUITY_SECTORS)
    assert gg.SUPPORTED == SUPPORTED


def test_declared_thresholds_and_chance_levels():
    spec = gg.GATE_SPEC_V1
    assert round(gg.binomial_chance(11, spec.min_survivors, 0.10), 4) == 0.0185
    assert round(gg.binomial_chance(10, spec.branch_10_min_survivors, 0.10), 4) == 0.0128
    # each is the smallest k with chance <= breadth_alpha
    assert gg.binomial_chance(11, spec.min_survivors - 1, 0.10) > spec.breadth_alpha
    assert gg.binomial_chance(10, spec.branch_10_min_survivors - 1, 0.10) > spec.breadth_alpha


@pytest.mark.parametrize(
    "change",
    [{"min_survivors": 3}, {"promotion_allowed": True}, {"loso_method": "ols"}, {"perms": 50},
     {"version": "generalization-gate-v2"}, {"branch_10_min_survivors": 2},
     {"breadth_null": "independent_circular_shift"}, {"forward_verdict": "FORWARD_INCONCLUSIVE_STOPPED"}],
)
def test_invalid_specs_are_refused(change):
    with pytest.raises(ValueError):
        replace(gg.GATE_SPEC_V1, **change).validate()


# --- 4.1 breadth calibration ----------------------------------------------------------

SIGNS = block_signs(N, 1, 999, 777)


def null_world(rng, rho):
    f = rng.standard_normal(N)
    e = rng.standard_normal((11, N))
    return 0.05 * (np.sqrt(rho) * f + np.sqrt(1 - rho) * e)


def harness_p(ic):
    """Vectorised per-sector one-sided sign-flip p (what each sector's holdout seals)."""
    obs = ic.mean(axis=1)
    null = SIGNS @ ic.T / N
    return (1 + (null >= obs - 1e-12).sum(axis=0)) / (len(SIGNS) + 1), obs


def null_results(ic, p):
    return [
        gg.SectorResult(sector=s, terminal_record_sha256=TERMINAL[s], terminal_kind="holdout_result",
                        prereg_sha256=PREREG, direction=1, holdout_p_one_sided=float(p[i]), block=1,
                        ic_series=tuple(zip(DATES, ic[i].tolist())))
        for i, s in enumerate(gg.EQUITY_SECTORS)
    ]


def false_breadth_rates(rho, worlds, seed):
    rng = np.random.default_rng([seed, int(rho * 10)])
    binomial = permutation = 0
    for _ in range(worlds):
        ic = null_world(rng, rho)
        p, obs = harness_p(ic)
        count = int(((p < 0.10) & (obs > 0)).sum())
        binomial += count >= 4
        counts = gg.sector_block_permutation_null(null_results(ic, p), 999, FAST.seed, alpha=0.10)
        k, _ = gg.permutation_threshold(counts, 11, 0.05)
        permutation += count >= max(4, k)
    return binomial / worlds, permutation / worlds


def test_independent_null_worlds_match_the_binomial_chance():
    rng = np.random.default_rng(20261001)
    hits, worlds = 0, 6000
    for _ in range(worlds):
        p, obs = harness_p(null_world(rng, 0.0))
        hits += int(((p < 0.10) & (obs > 0)).sum()) >= 4
    assert abs(hits / worlds - 0.0185) < 0.006, hits / worlds


@pytest.mark.parametrize("rho", [0.3, 0.6])
def test_correlated_null_worlds_permutation_controls_binomial_does_not(rho):
    binomial, permutation = false_breadth_rates(rho, 700, 2026)
    assert permutation <= 0.05, (rho, permutation)
    assert binomial > 0.05, (rho, binomial)
    assert binomial > permutation


def test_independent_circular_shifts_cannot_calibrate_the_count():
    """Why the plan's circular-offset sketch is not the null: it leaves every mean IC unchanged."""
    ic = null_world(np.random.default_rng(1), 0.6)
    rng = np.random.default_rng(2)
    shifted = np.stack([np.roll(row, rng.integers(N)) for row in ic])
    assert np.allclose(shifted.mean(axis=1), ic.mean(axis=1))


def test_untestable_sectors_never_survive_in_the_null():
    ic = null_world(np.random.default_rng(3), 0.0)
    p, _ = harness_p(ic)
    res = null_results(ic, p)
    res[1] = gg.SectorResult(sector=res[1].sector, terminal_record_sha256=TERMINAL[res[1].sector],
                             terminal_kind="stage0_untestable", prereg_sha256=PREREG, direction=1)
    counts = gg.sector_block_permutation_null(res, 999, 5, alpha=0.10)
    assert counts.max() <= 10


# --- the verdicts --------------------------------------------------------------------


def test_general_review_required_when_everything_passes():
    v = gg.evaluate_gate(world(), FAST, witness_check=witnessed)
    assert v.verdict == "GENERAL_REVIEW_REQUIRED", v.reasons
    assert v.payload["promotion_allowed"] is False
    assert v.payload["breadth"]["count"] == 6
    assert v.payload["breadth"]["required"] == 4
    assert v.payload["dominance"]["passed"] and v.payload["coverage"]["passed"]


def test_one_sector_driving_everything_fails_loso():
    over = {"Technology": {"ic": exact_series(0.30, 1)}}
    res = world(strong=6, mean=0.02, **over)
    # the other five non-survivors are exactly as negative as the weak ones are positive
    res = [r if i < 6 else result(r.sector, exact_series(-0.02, 200 + i)) for i, r in enumerate(res)]
    v = gg.evaluate_gate(res, FAST, witness_check=witnessed)
    assert v.payload["breadth"]["passed"], v.payload["breadth"]
    assert v.verdict == "SECTOR_SPECIFIC", v.reasons
    assert v.payload["dominance"]["loso"]["failed_when_leaving_out"] == ["Technology"]
    boot = gg.loso_pooled(res, "sector_bootstrap", alpha=0.05, draws=999, seed=1)
    assert not boot["passed"] and "Technology" in boot["failed_when_leaving_out"]


def test_one_entity_with_40pct_of_a_sector_fails_top_entity():
    ic = exact_series(0.03, 100)
    total = ic.sum()
    contributions = [0.40 * total] + [0.60 * total / 19] * 19
    res = world(Technology={"ic": ic, "contributions": contributions})
    v = gg.evaluate_gate(res, FAST, witness_check=witnessed)
    assert v.verdict == "SECTOR_SPECIFIC", v.reasons
    assert v.payload["dominance"]["loso"]["passed"]
    assert v.payload["dominance"]["top_entity_share"]["Technology"] == pytest.approx(0.40)
    assert gg.top_entity_share(res[0]) == pytest.approx(0.40)


def test_forward_one_is_pending_two_pass():
    assert gg.evaluate_gate(world(forward=1), FAST, witness_check=witnessed).verdict == "FORWARD_PENDING"
    assert gg.evaluate_gate(world(forward=0), FAST, witness_check=witnessed).verdict == "FORWARD_PENDING"
    assert gg.evaluate_gate(world(forward=2), FAST, witness_check=witnessed).verdict == "GENERAL_REVIEW_REQUIRED"
    # a SUPPORTED verdict on a non-survivor does not count
    res = world(forward=1)
    res[8] = result(res[8].sector, exact_series(0.0, 108), forward=gg.SUPPORTED)
    v = gg.evaluate_gate(res, FAST, witness_check=witnessed)
    assert v.verdict == "FORWARD_PENDING" and v.payload["forward"]["supported"] == [gg.EQUITY_SECTORS[0]]


def test_too_few_survivors_is_insufficient():
    v = gg.evaluate_gate(world(strong=3), FAST, witness_check=witnessed)
    assert v.verdict == "INSUFFICIENT_SECTORS"
    assert v.payload["breadth"]["count"] == 3


def test_coverage_artifact_is_insufficient():
    over = {s: {"ic": exact_series(0.03, 100 + i), "coverage": exact_series(0.0, 300 + i)}
            for i, s in enumerate(gg.EQUITY_SECTORS[:6])}
    v = gg.evaluate_gate(world(**over), FAST, witness_check=witnessed)
    assert v.verdict == "INSUFFICIENT_SECTORS", v.reasons
    assert v.payload["breadth"]["passed"] and not v.payload["coverage"]["passed"]


def test_ten_sector_branch_uses_the_predeclared_values():
    res = world(strong=6, forward=3)
    res[0] = result("Technology", None, kind="stop")  # v8 ended in STOP: 10 counted sectors
    v = gg.evaluate_gate(res, FAST, witness_check=witnessed)
    b = v.payload["breadth"]
    assert b["branch"] == "10-sector (v8 STOP)" and b["n_sectors"] == 10
    assert b["required_binomial"] == gg.GATE_SPEC_V1.branch_10_min_survivors == 4
    assert round(b["binomial_chance"], 4) == 0.0128
    assert b["count"] == 5
    assert v.verdict == "GENERAL_REVIEW_REQUIRED", v.reasons


def test_stage0_untestable_sector_is_a_non_survivor():
    res = world(strong=4)
    res[2] = result(res[2].sector, None, kind="stage0_untestable")
    v = gg.evaluate_gate(res, FAST, witness_check=witnessed)
    assert v.payload["breadth"]["untestable"] == [gg.EQUITY_SECTORS[2]]
    assert v.payload["breadth"]["n_sectors"] == 11 and v.payload["breadth"]["count"] == 3
    assert v.verdict == "INSUFFICIENT_SECTORS"


# --- 4.4 witness and other refusals ---------------------------------------------------


def _refused(res, check=witnessed, spec=FAST):
    v = gg.evaluate_gate(res, spec, witness_check=check)
    assert v.verdict == "REFUSED"
    assert v.payload["promotion_allowed"] is False
    return " | ".join(v.reasons)


def test_unwitnessed_inputs_are_refused():
    res = world()
    assert "not witnessed" in _refused(res, check=lambda sha: sha != TERMINAL["Energy"])
    assert "not witnessed" in _refused(res, check=lambda sha: False)

    def boom(sha):
        raise OSError("vault unreachable")

    assert "witness_check raised OSError" in _refused(res, check=boom)
    assert "not witnessed" in _refused(res, check=lambda sha: "yes")  # only True passes
    bad = list(res)
    bad[3] = replace(bad[3], terminal_record_sha256="")
    assert "terminal record sha256 missing" in _refused(bad)
    bad[3] = replace(res[3], terminal_record_sha256=None)
    assert "terminal record sha256 missing" in _refused(bad)
    fwd = list(res)
    fwd[0] = replace(res[0], forward_record_sha256="e" * 64)
    assert "forward record eeeeeeeeeeee is not witnessed" in _refused(fwd)
    fwd[0] = replace(res[0], forward_record_sha256=None)
    assert "forward verdict without a forward record" in _refused(fwd)


def test_incomplete_or_inconsistent_inputs_are_refused():
    res = world()
    assert "missing terminal results" in _refused(res[:-1])
    assert "duplicate sector" in _refused(res + [res[0]])
    assert "only Technology may end in STOP" in _refused([result(r.sector, None, kind="stop") if i == 4
                                                         else r for i, r in enumerate(res)])
    mixed = list(res)
    mixed[5] = replace(res[5], prereg_sha256="c" * 64)
    assert "different preregistrations" in _refused(mixed)
    mixed = list(res)
    mixed[5] = replace(res[5], direction=-1)
    assert "one sign" in _refused(mixed)
    nan = list(res)
    nan[1] = replace(res[1], ic_series=(("2020-01-03", float("nan")),) + res[1].ic_series[1:])
    assert "non-finite" in _refused(nan)
    fake = list(res)
    fake[1] = replace(res[1], entity_contributions=(("cik:1", 1.0),))
    assert "do not add up" in _refused(fake)
    held = list(res)
    held[7] = replace(result(res[7].sector, None, kind="stage0_untestable"), holdout_p_one_sided=0.01)
    assert "cannot carry holdout statistics" in _refused(held)
    shifted = list(res)
    for i in range(6, 11):
        shifted[i] = replace(res[i], ic_series=tuple((f"{int(d[:4]) + 20}{d[4:]}", v) for d, v in res[i].ic_series))
    assert "common grid" in _refused(shifted)


# --- 4.5 determinism ------------------------------------------------------------------


def test_verdict_json_is_deterministic_and_carries_the_spec_hash():
    a = gg.evaluate_gate(world(), FAST, witness_check=witnessed)
    b = gg.evaluate_gate(list(reversed(world())), FAST, witness_check=witnessed)
    assert a.to_json() == b.to_json()
    assert a.sha256() == b.sha256()
    assert a.payload["spec_sha256"] == gg.spec_sha256(FAST)
    assert b'"spec_sha256":"' + gg.spec_sha256(FAST).encode() + b'"' in a.to_json()
    assert b"NaN" not in a.to_json()


def test_v1_spec_runs_end_to_end():
    v = gg.evaluate_gate(world(), witness_check=witnessed)
    assert v.payload["spec_sha256"] == gg.GATE_SPEC_V1_SHA256
    assert v.verdict == "GENERAL_REVIEW_REQUIRED", v.reasons
