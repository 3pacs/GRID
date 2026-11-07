"""v2 hypothesis scoring: one-look PIT holdout trials, BH per batch, S11 ledger.

``intelligence.hypothesis_trials`` replaces the scorer held since 2026-09-26.
These tests pin what makes it honest: GRID telemetry can never confirm, only
post-creation data is tested, a hypothesis is looked at once, BH runs over
every declared trial of a batch, and the batch is in the S11 ledger before any
row changes.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import analysis.ledger_steered_exploration as lse
from intelligence import hypothesis_trials as ht

UTC = timezone.utc


# ── eligibility ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("key", [
    "snap:regime", "snap:llm_task_briefing", "sig:llm_task_summary",
    "sig:oracle_consensus", "sig:hypothesis_boost", "sig:pipeline_health",
    "SNAP:Flows",
])
def test_self_generated_keys(key: str) -> None:
    assert ht.is_self_generated(key)


@pytest.mark.parametrize("key", ["sig:insider_buy", "sig:congress_trade", "sig:13f_change", "", None])
def test_external_keys(key) -> None:
    assert not ht.is_self_generated(key)


def _created(days_ago: float = 200, now: datetime | None = None) -> datetime:
    return (now or datetime(2026, 11, 6, 12, tzinfo=UTC)) - timedelta(days=days_ago)


def test_plan_convergence_is_ticker_move() -> None:
    plan = ht.plan_trial("h", "convergence", {"ticker": "aapl", "expected_direction": "CALL",
                                              "window_days": 14, "min_move_pct": 2.0}, _created())
    assert plan.status is None and plan.test_kind == "ticker_move"
    assert plan.params == {"ticker": "AAPL", "direction": "up", "window_days": 14, "min_move_pct": 2.0}


def test_plan_opposite_antithesis_negates_its_thesis() -> None:
    crit = {"ticker": "AAPL", "expected_direction": "opposite"}
    plan = ht.plan_trial("h_anti", "convergence", crit, _created(), thesis_direction="PUT")
    assert plan.status is None and plan.params["direction"] == "up"
    orphan = ht.plan_trial("h_anti", "convergence", crit, _created(), thesis_direction=None)
    assert (orphan.status, orphan.reason) == (ht.UNTESTABLE, ht.NO_DIRECTION)


def test_plan_convergence_without_direction_is_untestable() -> None:
    plan = ht.plan_trial("h", "convergence", {"ticker": "AAPL", "expected_direction": "opposite"},
                         _created())
    assert (plan.status, plan.reason) == (ht.UNTESTABLE, ht.NO_DIRECTION)


@pytest.mark.parametrize("watch,expect", [
    ("snap:regime", "sig:insider_buy"),
    ("sig:insider_buy", "snap:llm_task_x"),
    ("sig:llm_task_digest", "sig:insider_buy"),
])
def test_plan_lead_lag_on_telemetry_is_ineligible(watch: str, expect: str) -> None:
    plan = ht.plan_trial("h", "lead_lag", {"watch_signal": watch, "expect_signal": expect,
                                           "lag_days": 3, "expected_direction": "increases"},
                         _created())
    assert (plan.status, plan.reason) == (ht.INELIGIBLE, ht.SELF_TELEMETRY)


def test_plan_category_watch_is_telemetry_and_actor_watch_has_no_outcome() -> None:
    cat = ht.plan_trial("h", "volume_anomaly", {"watch_category": "insider_buy", "window_days": 30},
                        _created())
    actor = ht.plan_trial("h", "actor_shift", {"watch_actor": "Pelosi", "window_days": 45}, _created())
    assert (cat.status, cat.reason) == (ht.INELIGIBLE, ht.SELF_TELEMETRY)
    assert (actor.status, actor.reason) == (ht.UNTESTABLE, ht.NO_MARKET_OUTCOME)


def test_identity_is_stable_and_specific() -> None:
    crit = {"ticker": "AAPL", "expected_direction": "up", "window_days": 14}
    a = ht.plan_trial("h1", "convergence", crit, _created())
    b = ht.plan_trial("h2", "convergence", json.dumps(crit), _created())
    c = ht.plan_trial("h3", "convergence", {**crit, "window_days": 15}, _created())
    assert a.identity_sha256 == b.identity_sha256 != c.identity_sha256


# ── ticker-move test ────────────────────────────────────────────────────────


def _sessions(start: date, end: date) -> list[date]:
    days, d = [], start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def _walk(days: list[date], seed: int, vol: float = 0.01) -> dict[date, float]:
    rng = np.random.default_rng(seed)
    prices = 100 * np.exp(np.cumsum(rng.normal(0, vol, len(days))))
    return dict(zip(days, prices.tolist()))


CREATED = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)  # 10:00 ET, before the close


def _market_and_ticker(jump: float = 0.0) -> tuple[dict, dict]:
    """A market walk and a ticker that moves with it (beta 1) plus its own noise."""
    days = _sessions(date(2022, 1, 3), date(2026, 10, 30))
    market = _walk(days, 1)
    own = _walk(days, 2)
    ticker = {d: market[d] * own[d] / 100 for d in days}
    entry = min(d for d in days if d >= date(2026, 9, 1))
    for d in days:
        if d > entry:
            ticker[d] *= 1 + jump
    return market, ticker


def _move(market: dict, ticker: dict, **kw) -> ht.TrialResult:
    return ht.ticker_move_test(ticker, {"SPY": market}, **{
        "created_at": CREATED, "direction": "up", "window_days": 14, "min_move_pct": 2.0, **kw})


def test_ticker_move_large_excess_move_is_significant() -> None:
    market, ticker = _market_and_ticker(jump=0.40)
    r = _move(market, ticker)
    assert r.status == ht.TESTED
    assert r.p_value < 0.01 and r.effect_ok
    assert r.details["entry"] == "2026-09-01"  # created before the close: that day's close
    assert r.details["n_null"] >= ht.MIN_NULL_SAMPLES
    assert r.details["betas"]["SPY"] == pytest.approx(1.0, abs=0.15)


def test_ticker_move_wrong_direction_is_not_significant() -> None:
    market, ticker = _market_and_ticker(jump=0.40)
    r = _move(market, ticker, direction="down")
    assert r.status == ht.TESTED
    assert r.p_value > 0.5 and not r.effect_ok


def test_ticker_move_market_wide_rally_is_not_a_finding() -> None:
    # The ticker rises 15% because the whole market does: no excess return.
    market, ticker = _market_and_ticker()
    entry = date(2026, 9, 1)
    for series in (market, ticker):
        for d in series:
            if d > entry:
                series[d] *= 1.15
    r = _move(market, ticker)
    assert r.status == ht.TESTED
    assert r.p_value > 0.05


def test_ticker_move_entry_after_close_is_next_session() -> None:
    market, ticker = _market_and_ticker()
    after_close = datetime(2026, 9, 1, 21, 30, tzinfo=UTC)  # 17:30 ET
    assert _move(market, ticker, created_at=after_close).details["entry"] == "2026-09-02"


def test_ticker_move_ignores_data_after_the_window() -> None:
    market, ticker = _market_and_ticker(jump=0.40)
    base = _move(market, ticker)
    later = {d: (v * 3 if d > date(2026, 9, 15) else v) for d, v in ticker.items()}
    moved = _move(market, later)
    assert (base.p_value, base.details) == (moved.p_value, moved.details)


def test_ticker_move_pending_until_exit_close_exists() -> None:
    market, ticker = _market_and_ticker()
    cut = {d: v for d, v in ticker.items() if d <= date(2026, 9, 5)}
    assert _move(market, cut) is ht.PENDING


def test_ticker_move_short_history_is_untestable() -> None:
    market, ticker = _market_and_ticker()
    short = {d: v for d, v in ticker.items() if d >= date(2026, 6, 1)}
    r = _move(market, short)
    assert (r.status, r.reason) == (ht.UNTESTABLE, ht.INSUFFICIENT_HISTORY)


# ── v3: the crowd's move is not the stock's ─────────────────────────────────


def _crowd(drop: float) -> tuple[dict, dict, dict]:
    """SPY flat-ish; IWM (the small-cap crowd) drops ``drop`` in the window; a
    small cap that tracks IWM drops with it."""
    days = _sessions(date(2022, 1, 3), date(2026, 10, 30))
    spy, iwm, own = _walk(days, 1), _walk(days, 3, vol=0.012), _walk(days, 4)
    entry = min(d for d in days if d >= date(2026, 9, 1))
    for d in days:
        if d > entry:
            iwm[d] *= 1 - drop
    small = {d: iwm[d] * own[d] / 100 for d in days}
    return spy, iwm, small


def test_a_crowd_wide_drop_is_not_a_down_call_win() -> None:
    # 2026-09-14..10-01: ~77% of US stocks lagged SPY; v2 (SPY at beta 1) let
    # every "down" call on a small cap share that move.
    spy, iwm, small = _crowd(drop=0.12)
    kw = {"created_at": CREATED, "direction": "down", "window_days": 14, "min_move_pct": 2.0}
    spy_only = ht.ticker_move_test(small, {"SPY": spy}, **kw)
    v3 = ht.ticker_move_test(small, {"SPY": spy, "IWM": iwm}, **kw)
    assert spy_only.p_value < 0.05                 # the v2 failure: the crowd's drop, credited
    assert v3.status == ht.TESTED and v3.p_value > 0.10
    assert v3.details["betas"]["IWM"] == pytest.approx(1.0, abs=0.15)
    assert v3.details["excess_log_return"] < -0.08 < v3.details["residual_log_return"]


def test_a_stock_falling_past_its_crowd_still_counts() -> None:
    spy, iwm, small = _crowd(drop=0.12)
    entry = date(2026, 9, 1)
    worse = {d: (v * 0.65 if d > entry else v) for d, v in small.items()}
    r = ht.ticker_move_test(worse, {"SPY": spy, "IWM": iwm}, created_at=CREATED,
                            direction="down", window_days=14, min_move_pct=2.0)
    assert r.status == ht.TESTED and r.p_value < 0.01 and r.effect_ok


def test_betas_are_fitted_before_creation_only() -> None:
    spy, iwm, small = _crowd(drop=0.0)
    kw = {"created_at": CREATED, "direction": "up", "window_days": 14, "min_move_pct": 2.0}
    base = ht.ticker_move_test(small, {"SPY": spy, "IWM": iwm}, **kw)
    # Rewrite every post-creation day of the ticker: the betas must not move.
    after = {d: (v * (1.5 if d.day % 2 else 0.7) if d >= date(2026, 9, 1) else v)
             for d, v in small.items()}
    moved = ht.ticker_move_test(after, {"SPY": spy, "IWM": iwm}, **kw)
    assert base.details["betas"] == moved.details["betas"]
    assert base.details["n_beta"] == ht.BETA_LOOKBACK_SESSIONS


def test_too_little_history_for_betas_is_untestable() -> None:
    spy, iwm, small = _crowd(drop=0.0)
    short = {d: v for d, v in small.items() if d >= date(2026, 5, 1)}
    r = ht.ticker_move_test(short, {"SPY": spy, "IWM": iwm}, created_at=CREATED,
                            direction="up", window_days=14, min_move_pct=2.0)
    assert (r.status, r.reason) == (ht.UNTESTABLE, ht.INSUFFICIENT_HISTORY)


def test_a_missing_benchmark_close_keeps_the_trial_pending() -> None:
    spy, iwm, small = _crowd(drop=0.0)
    iwm_cut = {d: v for d, v in iwm.items() if d <= date(2026, 9, 5)}
    r = ht.ticker_move_test(small, {"SPY": spy, "IWM": iwm_cut}, created_at=CREATED,
                            direction="up", window_days=14, min_move_pct=2.0)
    assert r is ht.PENDING


@pytest.mark.parametrize("ticker,expected", [("NVDA", ["SPY", "IWM"]), ("SPY", ["IWM"]),
                                             ("IWM", ["SPY"])])
def test_run_trial_nets_out_the_market_and_the_crowd(ticker: str, expected: list[str]) -> None:
    plan = ht.plan_trial("h", "convergence", {"ticker": ticker, "expected_direction": "PUT"},
                         CREATED)
    seen: list[str] = []

    def fake_closes(conn, t, start, end, evaluated_at):
        seen.append(t)
        return {}

    with patch.object(ht, "load_completed_closes", side_effect=fake_closes), \
            patch.object(ht, "ticker_move_test", return_value=ht.PENDING) as test:
        ht.run_trial(plan, MagicMock(), datetime(2026, 11, 6, tzinfo=UTC))
    assert list(test.call_args.args[1]) == expected
    assert seen == [ticker, *expected]


def test_scorer_version_names_v3() -> None:
    assert ht.SCORER_VERSION == "hypothesis-trials/3"
    assert ht.BENCHMARK_TICKERS == ("SPY", "IWM")


# ── lead-lag holdout test ───────────────────────────────────────────────────


def _daily(start: date, values) -> dict[date, float]:
    return {start + timedelta(days=i): float(v) for i, v in enumerate(values)}


LL_CREATED = datetime(2026, 6, 1, 12, tzinfo=UTC)


def test_lead_lag_true_relation_in_holdout_is_significant() -> None:
    rng = np.random.default_rng(7)
    start = date(2026, 6, 2)
    x = rng.poisson(5, 120)
    y = np.roll(x, 3) + rng.normal(0, 0.5, 120)
    r = ht.lead_lag_test(_daily(start, x), _daily(start, y), created_at=LL_CREATED,
                         lag_days=3, direction="up", perms=999)
    assert r.status == ht.TESTED and r.p_value <= 0.01 and r.effect_ok


def test_lead_lag_uses_only_post_creation_data() -> None:
    # A perfect relation before creation, independent noise after it.
    rng = np.random.default_rng(11)
    pre_start = date(2026, 1, 1)
    pre_x = rng.poisson(5, 150)
    watch = _daily(pre_start, pre_x)
    expect = _daily(pre_start + timedelta(days=3), pre_x)
    post_start = date(2026, 6, 2)
    watch.update(_daily(post_start, rng.poisson(5, 120)))
    expect.update(_daily(post_start, rng.poisson(5, 120)))
    r = ht.lead_lag_test(watch, expect, created_at=LL_CREATED, lag_days=3, direction="up", perms=999)
    assert r.status == ht.TESTED
    assert r.p_value > 0.05


def test_lead_lag_declared_sign_is_one_sided() -> None:
    rng = np.random.default_rng(5)
    start = date(2026, 6, 2)
    x = rng.poisson(5, 120)
    y = -np.roll(x, 2) + rng.normal(0, 0.5, 120)
    up = ht.lead_lag_test(_daily(start, x), _daily(start, y), created_at=LL_CREATED,
                          lag_days=2, direction="up", perms=999)
    down = ht.lead_lag_test(_daily(start, x), _daily(start, y), created_at=LL_CREATED,
                            lag_days=2, direction="down", perms=999)
    assert up.p_value > 0.5 and down.p_value <= 0.01


def test_lead_lag_constant_holdout_is_untestable() -> None:
    r = ht.lead_lag_test({}, {}, created_at=LL_CREATED, lag_days=3, direction="up", perms=99)
    assert (r.status, r.reason) == (ht.UNTESTABLE, ht.NO_DATA)


# ── S11 ledger record ───────────────────────────────────────────────────────


def _trial(hid: str, p: float, status: str = ht.TESTED, effect: bool = True) -> dict:
    return {
        "hypothesis_id": hid, "identity_sha256": hid * 2, "pattern_type": "convergence",
        "test_kind": "ticker_move", "created_at": "2026-09-01T00:00:00+00:00",
        "status": status, "p_value": p if status == ht.TESTED else 1.0,
        "effect_ok": effect if status == ht.TESTED else False,
        "reason": None if status == ht.TESTED else ht.NO_MARKET_OUTCOME,
    }


def test_batch_record_applies_bh_over_every_declared_trial() -> None:
    ledger = ht.dry_run_ledger()
    trials = [_trial("a", 0.001), _trial("b", 0.004), _trial("c", 0.04)] + [
        _trial(f"u{i}", 1.0, status=ht.UNTESTABLE) for i in range(17)
    ]
    out = lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="2026-11-06T00:00:00Z",
                                      trials=trials)
    verdicts = {t["hypothesis_id"]: t["verdict"] for t in out["record"]["trials"]}
    # m = 20: BH at q = 0.10 rejects p <= k*0.1/20 -> a (0.005), b (0.01); c needs 0.015.
    assert verdicts["a"] == verdicts["b"] == "confirmed"
    assert verdicts["c"] == "invalidated"
    assert all(verdicts[f"u{i}"] == "expired" for i in range(17))
    assert out["record"]["trial_count"] == 20
    assert lse.declared_trial_count(ledger) == 20
    assert out["record"]["promotion_allowed"] is False


def test_significant_trial_without_effect_is_invalidated() -> None:
    ledger = ht.dry_run_ledger()
    out = lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="x",
                                      trials=[_trial("a", 0.0001, effect=False)])
    assert out["record"]["trials"][0]["verdict"] == "invalidated"


def test_a_hypothesis_is_tested_once() -> None:
    ledger = ht.dry_run_ledger()
    lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="x", trials=[_trial("a", 0.5)])
    with pytest.raises(ValueError, match="one look"):
        lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="y",
                                    trials=[_trial("a", 0.001)])
    with pytest.raises(ValueError, match="twice"):
        lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="y",
                                    trials=[_trial("b", 0.5), _trial("b", 0.5)])


def test_ledger_refuses_misstated_verdicts_and_bad_trials() -> None:
    ledger = ht.dry_run_ledger()
    forged = lse._hypothesis_verdicts([_trial("a", 0.5)], ledger.q)
    forged[0]["verdict"] = "confirmed"
    record = {"kind": lse.HYPOTHESIS_BATCH_KIND, "selection": "bh_batch", "q": ledger.q,
              "trial_count": 1, "trials": forged}
    with pytest.raises(ValueError, match="do not follow"):
        ledger._append(record)
    with pytest.raises(ValueError, match="p = 1"):
        lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="x",
                                    trials=[{**_trial("u", 1.0, status=ht.INELIGIBLE), "p_value": 0.01}])
    with pytest.raises(ValueError, match="at least one"):
        lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="x", trials=[])


def test_file_ledger_keeps_batches_across_reopen(tmp_path) -> None:
    path, anchor = tmp_path / "ledger.jsonl", tmp_path / "anchor.jsonl"
    lse.Ledger.create(path, anchor=anchor, catalog=lse.synthetic_catalog(), ledger_id="t-ledger")
    with ht.open_trial_ledger(path, anchor) as ledger:
        lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="x", trials=[_trial("a", 0.5)])
    with ht.open_trial_ledger(path, anchor) as ledger:
        assert set(lse.hypothesis_batch_trials(ledger)) == {"a"}
        assert lse.summarize_hypothesis_batches(ledger)["n_trials_declared_total"] == 1
        with pytest.raises(ValueError, match="one look"):
            lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="y",
                                        trials=[_trial("a", 0.01)])


def test_unconfigured_ledger_refuses() -> None:
    with patch("config.settings") as settings:
        settings.HYPOTHESIS_TRIAL_LEDGER_PATH = ""
        settings.HYPOTHESIS_TRIAL_LEDGER_ANCHOR_PATH = ""
        with pytest.raises(ht.TrialLedgerNotConfigured), ht.open_trial_ledger():
            pass


# ── batch scorer ────────────────────────────────────────────────────────────

NOW = datetime(2026, 11, 6, 12, tzinfo=UTC)


def _engine(rows: list[tuple]) -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = rows
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def _row(hid: str, ptype: str, criteria: dict, days_ago: float, tested: int = 0,
         thesis_direction: str | None = None) -> tuple:
    return (hid, ptype, criteria, NOW - timedelta(days=days_ago), tested, thesis_direction)


CONV = {"ticker": "AAPL", "expected_direction": "up", "window_days": 14, "min_move_pct": 2.0}


@pytest.fixture
def released(monkeypatch) -> None:
    """Batch machinery as it runs once ticker-move trials are released."""
    monkeypatch.setattr(ht, "TICKER_MOVE_HELD", False)


def test_held_ticker_moves_stay_active_and_unledgered() -> None:
    lead = {"watch_signal": "sig:insider_buy", "expect_signal": "sig:congress_trade",
            "lag_days": 3, "expected_direction": "increases"}
    rows = [
        _row("conv", "convergence", CONV, 60),           # testable ticker move: held
        _row("sleuth", "sleuth_intersection", {**CONV, "expected_direction": "down"}, 30),
        _row("nodir", "convergence", {**CONV, "expected_direction": "opposite"}, 60),
        _row("lead", "lead_lag", lead, 120),
    ]
    engine = _engine(rows)
    with patch.object(ht, "load_completed_closes", side_effect=AssertionError("no price read")), \
            patch.object(ht, "load_signal_series", return_value={}):
        out = ht.score_batch(engine, dry_run=True, evaluated_at=NOW)
    assert ht.TICKER_MOVE_HELD is True
    assert out["held_ticker_move"] == 2
    declared = {t["hypothesis_id"] for t in out["record"]["trials"]}
    assert declared == {"nodir", "lead"}      # needs no price test / unaffected test kind


def test_dry_run_writes_nothing_and_applies_bh(released: None) -> None:
    rows = [_row(f"c{i}", "convergence", CONV, 60) for i in range(10)] + [
        _row("cat", "volume_anomaly", {"watch_category": "x"}, 60),
    ]
    p = {"c0": 0.0005, "c1": 0.001}

    def fake_run(plan, conn, evaluated_at, cache=None):
        if plan.status is not None:
            return ht.TrialResult(status=plan.status, reason=plan.reason)
        return ht.TrialResult(status=ht.TESTED, p_value=p.get(plan.hypothesis_id, 0.6), effect_ok=True)

    engine = _engine(rows)
    with patch.object(ht, "run_trial", side_effect=fake_run):
        out = ht.score_batch(engine, dry_run=True, evaluated_at=NOW)
    engine.begin.assert_not_called()
    assert out["verdicts"] == {"confirmed": 2, "invalidated": 8, "expired": 1}
    assert out["trial_count"] == 11
    summary = ht.summarize_dry_run(out)
    assert summary["reasons"] == {ht.SELF_TELEMETRY: 1}
    assert summary["confirmed_with_no_v1_look"] == 2


def test_pending_rows_are_not_declared_until_they_expire(released: None) -> None:
    rows = [
        _row("young", "convergence", CONV, 5),          # window still open
        _row("waiting", "convergence", CONV, 20),       # matured, closes missing, < 2x window
        _row("gone", "convergence", CONV, 60),          # matured, closes missing, > 2x window
    ]
    engine = _engine(rows)
    with patch.object(ht, "load_completed_closes", return_value={}):
        out = ht.score_batch(engine, dry_run=True, evaluated_at=NOW)
    trials = {t["hypothesis_id"]: t for t in out["record"]["trials"]}
    assert set(trials) == {"gone"}
    assert trials["gone"]["reason"] == ht.NO_DATA and trials["gone"]["verdict"] == "expired"
    assert out["pending"] == 2


def test_already_ledgered_hypotheses_are_skipped() -> None:
    ledger = ht.dry_run_ledger()
    lse.record_hypothesis_batch(ledger, scorer_version="t", evaluated_at="x", trials=[_trial("cat", 1.0, status=ht.INELIGIBLE)])
    engine = _engine([_row("cat", "volume_anomaly", {"watch_category": "x"}, 60)])
    out = ht.score_batch(engine, ledger=ledger, dry_run=True, evaluated_at=NOW)
    assert out["declared"] == 0 and out["skipped_already_ledgered"] == 1


def test_unsupported_instrument_is_untestable(released: None) -> None:
    from evaluation.prices import UnsupportedInstrumentError

    engine = _engine([_row("btc", "convergence", {**CONV, "ticker": "BTC-USD"}, 60)])
    out = ht.score_batch(engine, dry_run=True, evaluated_at=NOW)
    (trial,) = out["record"]["trials"]
    assert trial["reason"] == ht.UNSUPPORTED_INSTRUMENT
    with pytest.raises(UnsupportedInstrumentError):
        ht.load_completed_closes(MagicMock(), "BTC-USD", date(2026, 1, 1), date(2026, 2, 1), NOW)


def test_live_batch_ledgers_before_writing() -> None:
    order: list[str] = []
    ledger = ht.dry_run_ledger()
    real_record = lse.record_hypothesis_batch

    def recording(*a, **k):
        order.append("ledger")
        return real_record(*a, **k)

    engine = _engine([_row("cat", "volume_anomaly", {"watch_category": "x"}, 60)])
    with patch.object(lse, "record_hypothesis_batch", side_effect=recording), \
         patch.object(ht, "ensure_trial_columns"), \
         patch.object(ht, "_replay_unapplied", return_value={}), \
         patch.object(ht, "apply_verdicts", side_effect=lambda *a: order.append("db") or {"expired": 1}):
        out = ht.score_batch(engine, ledger=ledger, evaluated_at=NOW)
    assert order == ["ledger", "db"]
    assert out["applied"] == {"expired": 1}


def test_live_batch_needs_a_ledger() -> None:
    with pytest.raises(ht.TrialLedgerNotConfigured):
        ht.score_batch(_engine([]), evaluated_at=NOW)


def test_apply_verdicts_only_touches_active_rows_with_bound_params() -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.rowcount = 1
    engine = MagicMock()
    engine.begin.return_value = conn
    ledger = ht.dry_run_ledger()
    rec = lse.record_hypothesis_batch(ledger, scorer_version="v", evaluated_at="x",
                                      trials=[_trial("a", 0.0001), _trial("b", 0.9)])
    with patch("intelligence.hypothesis_engine.HypothesisGenerator._write_postmortem") as pm, \
         patch("intelligence.hypothesis_engine.HypothesisGenerator._kill_counterpart") as kc:
        counts = ht.apply_verdicts(engine, rec["record"], rec["sha256"])
    assert counts == {"confirmed": 1, "invalidated": 1, "expired": 0, "unchanged": 0}
    sql = str(conn.execute.call_args_list[0].args[0])
    assert "WHERE id = :hid AND status = 'active'" in sql
    params = [c.args[1] for c in conn.execute.call_args_list]
    assert {p["hid"]: p["status"] for p in params} == {"a": "confirmed", "b": "invalidated"}
    assert all(p["sha"] == rec["sha256"] and p["version"] == "v" for p in params)
    assert params[1]["kill_reason"] == ht.NOT_SIGNIFICANT
    pm.assert_called_once_with("b", ht.NOT_SIGNIFICANT)
    kc.assert_called_once_with("a")


def test_read_only_engine_sets_every_new_connection_read_only() -> None:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = "on"
    with patch("sqlalchemy.event.listen") as listen:
        assert ht.read_only_engine(engine) is engine
    engine.dispose.assert_called_once()
    (target, name, fn), _ = listen.call_args
    assert target is engine and name == "connect"
    cursor = MagicMock()
    fn(MagicMock(cursor=MagicMock(return_value=cursor)), None)
    cursor.execute.assert_called_once_with("SET SESSION default_transaction_read_only = on")


def test_read_only_engine_refuses_when_not_read_only() -> None:
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = "off"
    with patch("sqlalchemy.event.listen"), pytest.raises(RuntimeError):
        ht.read_only_engine(engine)
