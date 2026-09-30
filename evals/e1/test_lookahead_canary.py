"""E1 gate 1: look-ahead canary (SQLite part; the PostgreSQL part is ``test_lookahead_canary_pg.py``).

Two invariants, checked on the main point-in-time consumers:

1. **Append-future determinism** -- output at a past ``as_of`` is
   byte-identical after appending rows no reader may use at that ``as_of``
   (revisions pulled later, observations dated or published later, FAILED /
   QUARANTINED markers).
2. **Planted future-leak** -- a series whose values equal the next-period
   outcome, available only after that outcome is realised, must not
   correlate with the consumer's output at the decision. Each leak test has
   a self-test proving the canary trips on a deliberately leaky reader.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta

import numpy as np
import pandas as pd
import pytest

from evals.e1 import vs1_world, world as W
from evals.e1.known_violations import known_violation
from store import observations as obs

AS_OF_MODELED = date(2025, 6, 10)  # every row backfilled later: modeled-lag path only
AS_OF_LIVE = date(2026, 7, 15)  # live pulls: pull-evidence path
LEAK_THRESHOLD = 0.6  # |corr| over ~30 decisions; noise sd ~0.18, a leak gives ~1


def _lags():
    from intelligence.regime.state_vector import PUBLICATION_LAGS

    return PUBLICATION_LAGS


def _end_of_day(d: date) -> datetime:
    return datetime.combine(d, time(23, 59, 59))


def _corr(x, y) -> float:
    x, y = np.asarray(x, float), np.asarray(y, float)
    keep = np.isfinite(x) & np.isfinite(y)
    x, y = x[keep], y[keep]
    if len(x) < 10 or x.std() == 0 or y.std() == 0:
        return float("nan")
    return float(np.corrcoef(x, y)[0, 1])


def _dump(observations) -> str:
    return json.dumps(
        [[o.series_id, o.obs_date.isoformat(), o.value, str(o.pull_timestamp), o.source,
          None if o.known_at is None else o.known_at.isoformat(), o.known_at_basis] for o in observations]
    )


def future_macro_rows(as_of: date, base: list[dict]) -> list[dict]:
    """Rows no point-in-time macro reader may use at ``as_of`` (see ``evals.e1.world``).

    Revisions of dates ``base`` already holds, pulled after ``as_of``; dates
    not yet published at ``as_of`` (or with no modeled lag), pulled after it;
    FAILED / QUARANTINED markers. Never a late backfill of an old date that
    was already public (by design that enters past modeled reads).
    """
    late = max(W.LATE_TS, datetime.combine(as_of + timedelta(days=2), time(6)))
    lags = _lags()
    existing = {(r["sid"], r["d"]) for r in base}
    rows = []
    for sid in W.DAILY + W.MONTHLY + ("ICSA",):
        if sid in W.DAILY:
            dates = list(W.bdays(as_of - timedelta(days=120), as_of + timedelta(days=90)))
        elif sid == "ICSA":
            dates = list(W.saturdays(as_of - timedelta(days=120), as_of + timedelta(days=90)))
        else:
            dates = list(W.months(as_of - timedelta(days=400), as_of + timedelta(days=90)))
        lag = lags.get(sid)
        for d in dates:
            unpublished = d > as_of or lag is None or lag.known_dates([d])[0] > as_of
            if (sid, d) in existing:
                rows.append(W.row(sid, d, -1e4, late))  # revision pulled after as_of
            if unpublished:
                rows.append(W.row(sid, d, 1e4, late + timedelta(hours=1)))  # not yet public at as_of
        rows.append(W.row(sid, as_of, 0.0, datetime.combine(as_of, time(12)), status="FAILED"))
        rows.append(W.row(sid, dates[0], 7e7, datetime.combine(as_of, time(12)), status="QUARANTINED"))
    return rows


# ── store.observations ─────────────────────────────────────────────────


@pytest.mark.parametrize("as_of", [AS_OF_MODELED, AS_OF_LIVE], ids=["modeled", "pulled"])
def test_read_window_known_at_ignores_future_rows(as_of):
    engine = W.sqlite_engine()
    base = W.macro_rows() + W.live_rows()
    W.insert(engine, base)
    lags = _lags()
    series = W.DAILY + W.MONTHLY + ("ICSA",)

    def snapshot() -> dict:
        with engine.connect() as conn:
            return {sid: _dump(obs.read_window_known_at(conn, sid, as_of=as_of, lag=lags.get(sid)))
                    for sid in series}

    before = snapshot()
    assert all(json.loads(v) for v in before.values()), "fixture must give every series history"
    W.insert(engine, future_macro_rows(as_of, base))
    assert snapshot() == before


def test_read_window_and_latest_bounded_by_as_of_ts_ignore_later_pulls():
    engine = W.sqlite_engine()
    base = W.macro_rows() + W.live_rows()
    W.insert(engine, base)
    cut = _end_of_day(AS_OF_LIVE)

    def snapshot() -> dict:
        out = {}
        with engine.connect() as conn:
            for sid in W.DAILY:
                out[sid] = (
                    _dump(obs.read_window(conn, sid, as_of=AS_OF_LIVE, as_of_ts=cut)),
                    _dump([obs.read_latest(conn, sid, as_of=AS_OF_LIVE, as_of_ts=cut)]),
                    _dump(obs.read_latest_n(conn, sid, 5, as_of=AS_OF_LIVE, as_of_ts=cut)),
                )
        return out

    before = snapshot()
    W.insert(engine, [r for r in future_macro_rows(AS_OF_LIVE, base) if r["st"] == "SUCCESS"])
    assert snapshot() == before


def _leak_world(target_dates, outcomes, *, lag_days: int):
    """LEAK(d) = outcome realised over (d, d + h]; pulled only after that (d + lag_days)."""
    engine = W.sqlite_engine()
    rows = []
    for d, z in zip(target_dates, outcomes):
        rows.append(W.row("LEAK", d, z, datetime.combine(d + timedelta(days=lag_days), time(6))))
    W.insert(engine, rows)
    return engine


def _leak_inputs():
    path = W.spy_path(date(2023, 1, 2), date(2025, 12, 31), seed=99)
    days = sorted(path)
    h = 10
    outcome = {days[i]: float(np.log(path[days[i + h]] / path[days[i]])) for i in range(len(days) - h)}
    decisions = days[250:len(days) - h:7]
    return days, outcome, decisions, h


def _leak_corr(reader) -> float:
    days, outcome, decisions, h = _leak_inputs()
    dated = [d for d in days if d in outcome]
    engine = _leak_world(dated, [outcome[d] for d in dated], lag_days=int(h * 1.5) + 2)
    seen = []
    with engine.connect() as conn:
        for t in decisions:
            got = reader(conn, t)
            seen.append(got[-1].value if got else np.nan)
    return _corr(seen, [outcome[t] for t in decisions])


def test_read_window_known_at_does_not_see_a_planted_future_leak():
    r = _leak_corr(lambda conn, t: obs.read_window_known_at(conn, "LEAK", as_of=t, lag=None))
    assert abs(r) < LEAK_THRESHOLD, f"corr with the next-period outcome {r:.2f}"


def test_leak_canary_self_test_trips_on_an_observation_date_reader():
    # read_window bounded by obs_date only (no as_of_ts) is the classic leak.
    r = _leak_corr(lambda conn, t: obs.read_window(conn, "LEAK", as_of=t))
    assert r > 0.95


# ── regime state vector (intelligence/regime/state_vector.py) ───────────


def _vector(engine, as_of) -> str:
    from intelligence.regime.state_vector import compute_state_vector

    sv = compute_state_vector(engine, as_of)
    return json.dumps({"values": list(sv.values), "stale": list(sv.stale_dimensions),
                       "completeness": sv.completeness, "price_basis": sv.price_basis})


@pytest.mark.parametrize("as_of", [AS_OF_MODELED, AS_OF_LIVE], ids=["modeled", "pulled"])
def test_state_vector_macro_dims_ignore_future_rows(as_of):
    engine = W.sqlite_engine()
    base = W.macro_rows() + W.live_rows()
    W.insert(engine, base)
    before = _vector(engine, as_of)
    assert sum(v is not None for v in json.loads(before)["values"]) >= 15
    W.insert(engine, future_macro_rows(as_of, base))
    assert _vector(engine, as_of) == before


@known_violation("E1-V1")
@pytest.mark.parametrize("as_of", [AS_OF_MODELED, AS_OF_LIVE], ids=["modeled", "pulled"])
def test_state_vector_spy_dims_ignore_prices_pulled_after_as_of(as_of):
    engine = W.sqlite_engine()
    W.insert(engine, W.macro_rows() + W.live_rows())
    before = _vector(engine, as_of)
    spy = W.spy_path(W.HIST_START, W.LIVE_END)
    # A re-pull after as_of that restates recent closes (e.g. a basis change).
    W.insert(engine, [W.row("YF:SPY:close", d, spy[d] * 1.25, W.LATE_TS, W.YF_SRC)
                      for d in W.bdays(as_of - timedelta(days=60), as_of)])
    assert _vector(engine, as_of) == before


@known_violation("E1-V2")
def test_state_vector_insider_dim_ignores_filings_pulled_after_as_of():
    as_of = AS_OF_MODELED
    engine = W.sqlite_engine()
    W.insert(engine, W.macro_rows())
    W.insert(engine, W.insider_rows(as_of - timedelta(days=90), as_of - timedelta(days=3)))
    before = _vector(engine, as_of)
    assert json.loads(before)["values"][-1] is not None  # insider_sentiment is computed
    # Form 4s for trades inside the 30-day window, filed and pulled after as_of.
    W.insert(engine, [W.row(f"INSIDER:BBB:late{i}:BUY", d, 9e5, W.LATE_TS, W.SEC_SRC)
                      for i, d in enumerate(W.bdays(as_of - timedelta(days=10), as_of))])
    assert _vector(engine, as_of) == before


def _vector_leak(engine, decisions, dim):
    from intelligence.regime.state_vector import DIM_NAMES, compute_state_vector

    k = DIM_NAMES.index(dim)
    return [compute_state_vector(engine, t).values[k] for t in decisions]


def _state_vector_leak_world(series: str):
    """VIXCLS or SPY restated at each decision date with the next-10-session SPY return, pulled late."""
    engine = W.sqlite_engine()
    # Only the series the probed dimension reads (keeps ~24 full vectors cheap).
    keep = "VIXCLS" if series == "VIXCLS" else "YF:SPY:close"
    W.insert(engine, [r for r in W.macro_rows() if r["sid"] == keep])
    spy = W.spy_path(W.HIST_START, W.HIST_END)
    days = sorted(spy)
    start = days.index(date(2024, 7, 1))
    decisions = days[start:start + 7 * 24:7]
    z = [float(np.log(spy[days[days.index(t) + 10]] / spy[t])) for t in decisions]
    if series == "VIXCLS":
        rows = [W.row("VIXCLS", t, 2.0 + 60.0 * zz, W.LATE_TS) for t, zz in zip(decisions, z)]
    else:
        rows = [W.row("YF:SPY:close", t, spy[t] * (1.0 + 4.0 * zz), W.LATE_TS, W.YF_SRC)
                for t, zz in zip(decisions, z)]
    W.insert(engine, rows)
    return engine, decisions, z


def test_state_vector_macro_dim_does_not_see_a_planted_future_leak():
    engine, decisions, z = _state_vector_leak_world("VIXCLS")
    r = _corr(_vector_leak(engine, decisions, "vix_level"), z)
    assert abs(r) < LEAK_THRESHOLD, f"vix_level corr with the next-period outcome {r:.2f}"


def test_state_vector_leak_self_test_trips_on_a_latest_vintage_reader(monkeypatch):
    from intelligence.regime import state_vector as sv_mod

    def leaky(engine, series_id, as_of, lookback_days=sv_mod.VALUE_LOOKBACK_DAYS):
        with engine.connect() as conn:
            got = obs.read_window(conn, series_id, as_of=as_of, start=as_of - timedelta(days=lookback_days))
        return pd.Series({o.obs_date: o.value for o in got}, dtype=float).sort_index()

    monkeypatch.setattr(sv_mod, "_fetch_series", leaky)
    engine, decisions, z = _state_vector_leak_world("VIXCLS")
    assert _corr(_vector_leak(engine, decisions, "vix_level"), z) > 0.9


@known_violation("E1-V1")
def test_state_vector_spy_dim_does_not_see_a_planted_future_leak():
    engine, decisions, z = _state_vector_leak_world("SPY")
    r = _corr(_vector_leak(engine, decisions, "spy_rsi"), z)
    assert abs(r) < LEAK_THRESHOLD, f"spy_rsi corr with the next-period outcome {r:.2f}"


# ── VS1 panel feature builders (analysis/panel_insider_density*) ────────


def _matrices(panels, n_decisions=None):
    out = {}
    for name, p in sorted(panels.items()):
        n = len(p.decision_at) if n_decisions is None else n_decisions[name]
        out[name] = (
            p.decision_at[:n],
            np.nan_to_num(np.asarray(p.feature[:n]), nan=-7.0).round(12).tolist(),
            np.nan_to_num(np.asarray(p.label[:n]), nan=-7.0).round(12).tolist(),
            np.nan_to_num(np.asarray(p.momentum[:n]), nan=-7.0).round(12).tolist(),
            np.nan_to_num(np.asarray(p.largest[:n]), nan=-7.0).round(12).tolist(),
        )
    return json.dumps(out)


def test_vs1_panels_ignore_filings_known_after_the_decision_and_post_split_closes():
    world = vs1_world.build()
    cut = pd.Timestamp("2016-06-30 23:59", tz="UTC")
    panels = vs1_world.trial_panels(world)
    n = {k: sum(pd.Timestamp(d) <= cut for d in p.decision_at) for k, p in panels.items()}
    assert min(n.values()) >= 20
    before = _matrices(panels, n)

    # Append what arrives after the cut: purchases, Section 16 accessions and
    # ticker-naming filings (now naming another ticker) known later, plus
    # closes after the split that discovery labels must never use.
    from analysis import panel_insider_density as v1
    from analysis import panel_insider_density_v2 as v2

    shift = pd.Timedelta(days=2000)  # every shifted instant lands after the cut

    def later(frame, **extra):
        return frame.assign(known_at=frame["known_at"] + shift, **extra)

    ev, ad = world.events, world.admission
    appended = vs1_world.VS1World(
        v1.Form4Events(
            pd.concat([ev.purchases, later(ev.purchases, actor=ev.purchases["actor"] + 1000)], ignore_index=True),
            pd.concat([ev.activity, later(ev.activity)], ignore_index=True),
            ev.receipt,
        ),
        v2.Admission(
            form4=pd.concat([ad.form4, later(ad.form4)], ignore_index=True),
            tickers=pd.concat([ad.tickers, later(ad.tickers, match=False)], ignore_index=True)
            .sort_values(["issuer_cik", "known_at"], kind="mergesort").reset_index(drop=True),
            receipt=ad.receipt,
        ),
        world.universe, world.closes, world.sessions,
    )
    assert (appended.events.purchases["known_at"] > cut).sum() >= len(ev.purchases)
    closes = world.closes.copy()
    closes.loc[closes.index >= pd.Timestamp("2020-01-01")] *= 3.0
    after = vs1_world.trial_panels(appended, closes=closes, as_of=vs1_world.POST_SPLIT_END)
    assert _matrices(after, n) == before


def _mean_ic(panels, trial="A30|fwd20") -> float:
    from analysis import panel_insider_density as v1

    ic, _ = v1.rank_ic_series(panels[trial].feature, panels[trial].label)
    assert np.isfinite(ic).sum() >= 30
    return float(np.nanmean(ic))


def test_vs1_features_do_not_see_a_planted_future_leak():
    ic = _mean_ic(vs1_world.trial_panels(vs1_world.build(leak=True)))
    assert abs(ic) < 0.1, f"mean rank IC with purchases filed after the outcome: {ic:.3f}"


def test_vs1_leak_self_test_trips_when_availability_is_the_trade_date():
    ic = _mean_ic(vs1_world.trial_panels(vs1_world.build(leak=True, leak_known_at_trade=True)))
    assert ic > 0.25
