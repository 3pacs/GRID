"""Tests for `evaluate`: H1/H2/H3 statistics (unit-tested directly against
hand-built Row/Observation/Trade data, so the exact statistical property
under test is unambiguous) plus the refusal/--interim wiring (tested
end-to-end through a seeded JSONL log).
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from paper_log.gex_levels.config import BONFERRONI_ALPHA, H1_MIN_GROUP_SESSIONS, H2_MIN_REAL_REACHES, H3_MIN_TRADES
from paper_log.gex_levels.evaluate import (
    H1Row,
    H2Observation,
    H3Trade,
    compute_h1,
    compute_h2,
    compute_h3,
    format_evaluate,
    run_evaluate,
)
from paper_log.gex_levels.storage import PaperLogStore

# ── H1 ─────────────────────────────────────────────────────────────────


def _make_h1_rows(n_short: int, n_long: int, n_neutral: int = 0, *, b_true: float = 0.3, seed: int = 1) -> list[H1Row]:
    rng = np.random.default_rng(seed)
    rows: list[H1Row] = []

    def _row(regime: str, is_short: bool) -> H1Row:
        vix = rng.uniform(12, 30)
        ln_vix = math.log(vix)
        noise = rng.normal(0, 0.01)
        ln_range = 0.01 + (b_true if is_short else 0.0) + 0.05 * ln_vix + noise
        return H1Row(session_date="x", regime=regime, ln_range=ln_range, ln_vix=ln_vix)

    rows.extend(_row("LONG_GAMMA", False) for _ in range(n_long))
    rows.extend(_row("SHORT_GAMMA", True) for _ in range(n_short))
    rows.extend(_row("NEUTRAL", False) for _ in range(n_neutral))
    return rows


def test_h1_inconclusive_when_a_group_is_too_small() -> None:
    rows = _make_h1_rows(n_short=H1_MIN_GROUP_SESSIONS - 1, n_long=20)
    result = compute_h1(rows)
    assert result.outcome == "INCONCLUSIVE"
    assert result.p_value is None


def test_h1_passes_on_a_strong_clean_short_gamma_effect() -> None:
    rows = _make_h1_rows(n_short=20, n_long=20, n_neutral=3, b_true=0.5, seed=7)
    result = compute_h1(rows)
    assert result.outcome == "PASS"
    assert result.p_value is not None
    assert result.p_value < BONFERRONI_ALPHA
    assert "NEUTRAL n=3" in result.detail


def test_h1_fails_when_there_is_no_real_effect() -> None:
    rows = _make_h1_rows(n_short=20, n_long=20, b_true=0.0, seed=3)
    result = compute_h1(rows)
    assert result.outcome == "FAIL"


def test_h1_fails_when_effect_runs_the_wrong_direction() -> None:
    rows = _make_h1_rows(n_short=20, n_long=20, b_true=-0.5, seed=3)
    result = compute_h1(rows)
    assert result.outcome == "FAIL"


# ── H2 ─────────────────────────────────────────────────────────────────


def _make_h2_observations(n_sessions: int, *, real_held_rate: float, placebo_held_rate: float, seed: int = 1) -> list[H2Observation]:
    rng = np.random.default_rng(seed)
    obs = []
    for i in range(n_sessions):
        obs.append(H2Observation(f"s{i}", "real", bool(rng.random() < real_held_rate)))
        obs.append(H2Observation(f"s{i}", "placebo", bool(rng.random() < placebo_held_rate)))
    return obs


def test_h2_inconclusive_below_min_real_reaches() -> None:
    obs = _make_h2_observations(H2_MIN_REAL_REACHES - 1, real_held_rate=1.0, placebo_held_rate=0.0)
    result = compute_h2(obs)
    assert result.outcome == "INCONCLUSIVE"
    assert result.p_value is None


def test_h2_passes_when_real_holds_much_more_than_placebo() -> None:
    obs = _make_h2_observations(30, real_held_rate=1.0, placebo_held_rate=0.0)
    result = compute_h2(obs)
    assert result.outcome == "PASS"
    assert result.p_value is not None
    assert result.p_value < BONFERRONI_ALPHA


def test_h2_fails_when_real_and_placebo_hold_equally() -> None:
    obs = _make_h2_observations(30, real_held_rate=0.5, placebo_held_rate=0.5, seed=11)
    result = compute_h2(obs)
    assert result.outcome == "FAIL"


def test_h2_is_deterministic_given_the_fixed_seed() -> None:
    obs = _make_h2_observations(25, real_held_rate=0.7, placebo_held_rate=0.4, seed=42)
    r1 = compute_h2(obs)
    r2 = compute_h2(obs)
    assert r1.p_value == r2.p_value


# ── H3 ─────────────────────────────────────────────────────────────────


def _make_h3_trades(n_real: int, n_placebo: int, *, real_mean: float, placebo_mean: float, seed: int = 1) -> list[H3Trade]:
    rng = np.random.default_rng(seed)
    trades = [H3Trade(f"r{i}", "real", real_mean + rng.normal(0, 0.001)) for i in range(n_real)]
    trades += [H3Trade(f"p{i}", "placebo", placebo_mean + rng.normal(0, 0.001)) for i in range(n_placebo)]
    return trades


def test_h3_inconclusive_below_min_trades() -> None:
    trades = _make_h3_trades(H3_MIN_TRADES - 1, 5, real_mean=0.01, placebo_mean=0.0)
    result = compute_h3(trades)
    assert result.outcome == "INCONCLUSIVE"
    assert result.p_value is None


def test_h3_passes_on_a_clean_positive_mean_return() -> None:
    trades = _make_h3_trades(25, 25, real_mean=0.01, placebo_mean=0.0, seed=5)
    result = compute_h3(trades)
    assert result.outcome == "PASS"
    assert result.p_value < BONFERRONI_ALPHA
    assert "placebo" in result.detail


def test_h3_fails_on_a_negative_mean_return() -> None:
    trades = _make_h3_trades(25, 25, real_mean=-0.01, placebo_mean=0.0, seed=5)
    result = compute_h3(trades)
    assert result.outcome == "FAIL"


def test_h3_reports_placebo_even_when_placebo_has_no_trades() -> None:
    trades = _make_h3_trades(25, 0, real_mean=0.01, placebo_mean=0.0, seed=5)
    result = compute_h3(trades)
    assert result.outcome in ("PASS", "FAIL")  # still computable from real trades alone
    assert "n_placebo_trades=0" in result.detail


# ── run_evaluate: refusal / --interim wiring ────────────────────────


def _seed_minimal_valid_session(store: PaperLogStore, d: date) -> None:
    store.append({
        "kind": "preopen", "run_at": None, "session_date": d, "code_sha": "x",
        "excluded": False, "exclusion_reason": None,
        "engine": {"regime": "NEUTRAL"},
        "vix_prev_close": {"price": 18.0},
    })
    store.append({
        "kind": "postclose", "run_at": None, "session_date": d, "code_sha": "x",
        "excluded": False, "exclusion_reason": None,
        "range_ln": 0.01,
        "reaches": {"real": {}, "placebo": {}},
        "h3_trade": {"real": {"triggered": False}, "placebo": {"triggered": False}},
    })


def test_refuses_below_60_valid_sessions_without_interim(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    for i in range(5):
        _seed_minimal_valid_session(store, date(2026, 1, 1) + timedelta(days=i))

    report = run_evaluate(tmp_path, interim=False)
    assert report.refused is True
    assert "5 valid sessions" in report.refusal_reason
    assert report.h1 is None and report.h2 is None and report.h3 is None


def test_interim_runs_below_60_and_labels_every_line(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    for i in range(5):
        _seed_minimal_valid_session(store, date(2026, 1, 1) + timedelta(days=i))

    report = run_evaluate(tmp_path, interim=True)
    assert report.refused is False
    assert report.interim is True

    text = format_evaluate(report)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) >= 4  # header + alpha + H1 + H2 + H3
    assert all(ln.startswith("[INTERIM]") for ln in lines)


def test_refusal_message_is_not_interim_labeled(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    _seed_minimal_valid_session(store, date(2026, 1, 1))
    report = run_evaluate(tmp_path, interim=False)
    text = format_evaluate(report)
    assert "refused" in text
    assert "[INTERIM]" not in text


def test_runs_without_interim_flag_once_60_valid_sessions_reached(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    for i in range(60):
        _seed_minimal_valid_session(store, date(2026, 1, 1) + timedelta(days=i))

    report = run_evaluate(tmp_path, interim=False)
    assert report.refused is False
    assert report.interim is False
    assert report.valid_sessions == 60
    text = format_evaluate(report)
    assert "[INTERIM]" not in text
