"""``evaluate`` — H1, H2, H3, run once at (or after) 60 valid sessions.

Pre-registration, "Hypotheses and tests": "Three tests, Bonferroni-corrected:
a hypothesis passes only with one-sided p < 0.0167."

  H1 (primary): OLS, ln(range) = a + b*SHORT + c*ln(VIX previous close) + e,
  Newey-West (HAC) standard errors, 5 lags. Pass if b > 0 and one-sided
  p < 0.0167. NEUTRAL sessions reported, not tested. <10 sessions in either
  group -> inconclusive.

  H2: permutation test on (real held-rate - placebo held-rate), shuffling
  real/placebo labels within each session, 10,000 draws, fixed seed. Pass
  if one-sided p < 0.0167. <20 real-level reaches -> inconclusive.

  H3: one-sided t-test that the mean real-level trade return > 0, with the
  placebo trades reported alongside (not gating). <20 real trades ->
  inconclusive.

"It refuses to run with fewer than 60 valid sessions unless `--interim` is
passed, and then labels every line INTERIM."

This module only ever *reads* records written by preopen/postclose; it
never recomputes reach/held/trade outcomes itself (those are computed
exactly once, in postclose.py, at the time each record is written).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import statsmodels.api as sm
from loguru import logger as log
from scipy import stats

from paper_log.gex_levels.config import (
    BONFERRONI_ALPHA,
    H1_MIN_GROUP_SESSIONS,
    H2_MIN_REAL_REACHES,
    H2_PERMUTATION_DRAWS,
    H2_PERMUTATION_SEED,
    H3_MIN_TRADES,
    LEVEL_NAMES,
    NEWEY_WEST_LAGS,
    REGIME_LONG_GAMMA,
    REGIME_NEUTRAL,
    REGIME_SHORT_GAMMA,
    VALID_REGIMES,
    VALID_SESSIONS_REQUIRED,
)
from paper_log.gex_levels.reading import SessionPair, valid_session_pairs


@dataclass(frozen=True)
class HypothesisResult:
    name: str
    outcome: str  # "PASS" | "FAIL" | "INCONCLUSIVE"
    detail: str
    p_value: float | None
    n: int


@dataclass(frozen=True)
class EvaluateReport:
    interim: bool
    valid_sessions: int
    refused: bool
    refusal_reason: str | None
    h1: HypothesisResult | None
    h2: HypothesisResult | None
    h3: HypothesisResult | None


# ── H1 ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class H1Row:
    session_date: str
    regime: str
    ln_range: float
    ln_vix: float


def extract_h1_rows(pairs: list[SessionPair]) -> list[H1Row]:
    rows = []
    for p in pairs:
        regime = p.preopen["engine"]["regime"]
        ln_range = p.postclose["range_ln"]
        vix_price = p.preopen["vix_prev_close"]["price"]
        rows.append(H1Row(p.session_date, regime, ln_range, math.log(vix_price)))
    unexpected = [r for r in rows if r.regime not in VALID_REGIMES]
    if unexpected:
        log.warning(
            "paper_log evaluate: {n} session(s) have an unrecognized regime "
            "label (not LONG_GAMMA/SHORT_GAMMA/NEUTRAL) — excluded from H1",
            n=len(unexpected),
        )
    return rows


def compute_h1(rows: list[H1Row]) -> HypothesisResult:
    short_rows = [r for r in rows if r.regime == REGIME_SHORT_GAMMA]
    long_rows = [r for r in rows if r.regime == REGIME_LONG_GAMMA]
    neutral_n = sum(1 for r in rows if r.regime == REGIME_NEUTRAL)

    if len(short_rows) < H1_MIN_GROUP_SESSIONS or len(long_rows) < H1_MIN_GROUP_SESSIONS:
        return HypothesisResult(
            "H1", "INCONCLUSIVE",
            f"SHORT_GAMMA n={len(short_rows)}, LONG_GAMMA n={len(long_rows)} "
            f"(need >= {H1_MIN_GROUP_SESSIONS} each); NEUTRAL n={neutral_n} reported, not tested",
            None, len(short_rows) + len(long_rows),
        )

    sample = short_rows + long_rows
    y = np.array([r.ln_range for r in sample])
    short_dummy = np.array([1.0 if r.regime == REGIME_SHORT_GAMMA else 0.0 for r in sample])
    ln_vix = np.array([r.ln_vix for r in sample])
    X = sm.add_constant(np.column_stack([short_dummy, ln_vix]), prepend=True)

    model = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": NEWEY_WEST_LAGS})
    b = float(model.params[1])
    t_stat = float(model.tvalues[1])
    p_one_sided = float(stats.t.sf(t_stat, model.df_resid))  # P(T > t_stat): tests b > 0

    passed = b > 0 and p_one_sided < BONFERRONI_ALPHA
    detail = (
        f"b(SHORT)={b:.5f}, one-sided p={p_one_sided:.4f} "
        f"(Newey-West, {NEWEY_WEST_LAGS} lags), n_short={len(short_rows)}, "
        f"n_long={len(long_rows)}, NEUTRAL n={neutral_n} reported, not tested"
    )
    return HypothesisResult("H1", "PASS" if passed else "FAIL", detail, p_one_sided, len(sample))


# ── H2 ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class H2Observation:
    session_date: str
    arm: str  # "real" | "placebo"
    held: bool


def extract_h2_observations(pairs: list[SessionPair]) -> list[H2Observation]:
    obs: list[H2Observation] = []
    for p in pairs:
        reaches = p.postclose["reaches"]
        real_arm = reaches.get("real") or {}
        placebo_arm = reaches.get("placebo") or {}
        for name in LEVEL_NAMES:
            real = real_arm.get(name)
            if real and real.get("status") == "reached":
                obs.append(H2Observation(p.session_date, "real", bool(real["held"])))
            placebo = placebo_arm.get(name)
            if placebo and placebo.get("status") == "reached":
                obs.append(H2Observation(p.session_date, "placebo", bool(placebo["held"])))
    return obs


def _held_rate_diff(observations: list[H2Observation]) -> float:
    real_held = [o.held for o in observations if o.arm == "real"]
    placebo_held = [o.held for o in observations if o.arm == "placebo"]
    if not real_held or not placebo_held:
        return 0.0
    return (sum(real_held) / len(real_held)) - (sum(placebo_held) / len(placebo_held))


def compute_h2(observations: list[H2Observation]) -> HypothesisResult:
    n_real = sum(1 for o in observations if o.arm == "real")
    n_placebo = len(observations) - n_real

    if n_real < H2_MIN_REAL_REACHES:
        return HypothesisResult(
            "H2", "INCONCLUSIVE",
            f"real-level reaches n={n_real} (need >= {H2_MIN_REAL_REACHES}); "
            f"placebo reaches n={n_placebo}",
            None, n_real,
        )

    observed = _held_rate_diff(observations)

    by_session: dict[str, list[H2Observation]] = {}
    for o in observations:
        by_session.setdefault(o.session_date, []).append(o)

    # Vectorized reformulation of "shuffle real/placebo labels within each
    # session": mathematically equivalent to fixing each session's k_real
    # label slots in place and instead permuting its `held` VALUES among
    # its own observations — a uniformly random bijection between labels
    # and values either way. This lets every session do all
    # H2_PERMUTATION_DRAWS draws in one vectorized numpy call instead of
    # H2_PERMUTATION_DRAWS separate Python-level permutation calls, which
    # matters once the log has enough sessions for `evaluate` to be run
    # for real (this is still a run-once-per-analysis command, not a hot
    # path, but a naive O(draws * sessions) Python loop was measured to
    # take about a minute even in tests with ~30 sessions).
    rng = np.random.default_rng(H2_PERMUTATION_SEED)
    real_held_sum = np.zeros(H2_PERMUTATION_DRAWS)
    placebo_held_sum = np.zeros(H2_PERMUTATION_DRAWS)

    for session_obs in by_session.values():
        k_real = sum(1 for o in session_obs if o.arm == "real")
        held = np.array([float(o.held) for o in session_obs])
        tiled = np.tile(held, (H2_PERMUTATION_DRAWS, 1))
        permuted = rng.permuted(tiled, axis=1)
        real_held_sum += permuted[:, :k_real].sum(axis=1)
        placebo_held_sum += permuted[:, k_real:].sum(axis=1)

    permuted_stats = (real_held_sum / n_real) - (placebo_held_sum / n_placebo)
    count_ge = int(np.sum(permuted_stats >= observed))

    # Standard unbiased permutation-test estimator (never exactly 0).
    p_value = (1 + count_ge) / (H2_PERMUTATION_DRAWS + 1)

    passed = observed > 0 and p_value < BONFERRONI_ALPHA
    detail = (
        f"observed held-rate diff (real - placebo)={observed:.4f}, "
        f"permutation p={p_value:.4f} ({H2_PERMUTATION_DRAWS} draws, "
        f"seed={H2_PERMUTATION_SEED}), n_real_reaches={n_real}, n_placebo_reaches={n_placebo}"
    )
    return HypothesisResult("H2", "PASS" if passed else "FAIL", detail, p_value, n_real)


# ── H3 ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class H3Trade:
    session_date: str
    arm: str  # "real" | "placebo"
    return_pct: float


def extract_h3_trades(pairs: list[SessionPair]) -> list[H3Trade]:
    trades: list[H3Trade] = []
    for p in pairs:
        h3 = p.postclose["h3_trade"]
        real = h3.get("real")
        if real and real.get("triggered"):
            trades.append(H3Trade(p.session_date, "real", real["return_pct"]))
        placebo = h3.get("placebo")
        if placebo and placebo.get("triggered"):
            trades.append(H3Trade(p.session_date, "placebo", placebo["return_pct"]))
    return trades


def compute_h3(trades: list[H3Trade]) -> HypothesisResult:
    real_returns = [t.return_pct for t in trades if t.arm == "real"]
    placebo_returns = [t.return_pct for t in trades if t.arm == "placebo"]

    if len(real_returns) < H3_MIN_TRADES:
        return HypothesisResult(
            "H3", "INCONCLUSIVE",
            f"real trades n={len(real_returns)} (need >= {H3_MIN_TRADES}); "
            f"placebo trades n={len(placebo_returns)} reported alongside, not gating",
            None, len(real_returns),
        )

    result = stats.ttest_1samp(real_returns, popmean=0.0, alternative="greater")
    p_value = float(result.pvalue)
    mean_real = float(np.mean(real_returns))
    mean_placebo = float(np.mean(placebo_returns)) if placebo_returns else None

    passed = mean_real > 0 and p_value < BONFERRONI_ALPHA
    placebo_desc = f"{mean_placebo:.5f}" if mean_placebo is not None else "n/a"
    detail = (
        f"mean real-trade return={mean_real:.5f}, one-sided t-test p={p_value:.4f}, "
        f"n_real_trades={len(real_returns)}; placebo (control, not gating): "
        f"mean return={placebo_desc}, n_placebo_trades={len(placebo_returns)}"
    )
    return HypothesisResult("H3", "PASS" if passed else "FAIL", detail, p_value, len(real_returns))


# ── Orchestration ──────────────────────────────────────────────────────


def run_evaluate(log_dir: Path, *, interim: bool = False) -> EvaluateReport:
    pairs = valid_session_pairs(log_dir)
    n_valid = len(pairs)

    if n_valid < VALID_SESSIONS_REQUIRED and not interim:
        return EvaluateReport(
            interim=False, valid_sessions=n_valid, refused=True,
            refusal_reason=(
                f"only {n_valid} valid sessions (< {VALID_SESSIONS_REQUIRED} required); "
                "pass --interim to run anyway (every line will be labeled INTERIM)"
            ),
            h1=None, h2=None, h3=None,
        )

    h1 = compute_h1(extract_h1_rows(pairs))
    h2 = compute_h2(extract_h2_observations(pairs))
    h3 = compute_h3(extract_h3_trades(pairs))

    return EvaluateReport(interim=interim, valid_sessions=n_valid, refused=False, refusal_reason=None, h1=h1, h2=h2, h3=h3)


def format_evaluate(report: EvaluateReport) -> str:
    if report.refused:
        return f"evaluate refused: {report.refusal_reason}"

    prefix = "[INTERIM] " if report.interim else ""
    lines = [
        f"{prefix}paper_log gex_levels v1 — evaluate (valid_sessions={report.valid_sessions})",
        f"{prefix}Bonferroni one-sided alpha = {BONFERRONI_ALPHA}",
    ]
    for hr in (report.h1, report.h2, report.h3):
        lines.append(f"{prefix}{hr.name}: {hr.outcome} — {hr.detail}")
    return "\n".join(lines)
