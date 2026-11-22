"""Cross-section factory: declared before data, development days only, by-hand arithmetic, the sign-flip bar."""
import csv
import json
import shutil
import sqlite3
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from analysis import cross_section_factory as X
from analysis import ledger_steered_exploration as lse
from analysis.desk_conditional_cards import conditional_factory_cards
from analysis.desk_factory_report import factory_report_cards

SECTORS = ("Energy", "Information Technology", "Utilities")
HEADER = ["date", "close", "high", "low", "open", "volume", "adjClose", "adjHigh", "adjLow", "adjOpen", "adjVolume", "divCash", "splitFactor"]


def build(root, n_per=20, n_days=220, seed=3, extra_late_bar=False, revert=0.0, volume_of=None, tilt_of=None):
    """Synthetic bars: every stock has its own steady overnight tilt that it gives back during the day.

    ``revert`` adds a one-month reversal: each day's intraday return gives back that share of the stock's return over
    the 21 sessions before. ``volume_of(sym)`` sets a stock's daily volume, ``tilt_of(sym)`` scales its tilt. The
    defaults draw the same random numbers as before."""
    rng = np.random.default_rng(seed)
    days = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2015-01-02", periods=n_days)]
    (root / "prices").mkdir(parents=True)
    syms, tilt = [], {}
    for s, sector in enumerate(SECTORS):
        for i in range(n_per):
            sym = f"S{s}N{i:02d}"
            syms.append((sym, sector))
            tilt[sym] = rng.normal(0, 0.002) * (tilt_of(sym) if tilt_of else 1.0)
    for sym, _ in syms:
        prev, rows, closes = 100.0, [], []
        vol = volume_of(sym) if volume_of else 2_000_000
        for d in days:
            back = revert * (prev / closes[-21] - 1) if revert and len(closes) >= 21 else 0.0
            o = prev * (1 + tilt[sym] + rng.normal(0, 0.003))
            c = o * (1 - tilt[sym] + rng.normal(0, 0.003) - back)
            rows.append([d, c, max(o, c) * 1.001, min(o, c) * 0.999, o, vol, c, max(o, c) * 1.001, min(o, c) * 0.999, o, vol, 0.0, 1.0])
            closes.append(prev)
            prev = c
        if extra_late_bar:
            rows.append(["2023-01-03", prev, prev * 1.01, prev * 0.99, prev, 2_000_000, prev, prev * 1.01, prev * 0.99, prev, 2_000_000, 0.0, 1.0])
        with open(root / "prices" / f"{sym}.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            w.writerows(rows)
    with open(root / "members.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "tickers"])
        w.writerow(["2014-01-02", ",".join(s for s, _ in syms)])
    with open(root / "sectors.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["Symbol", "GICS Sector"])
        w.writerows(syms)
    (root / "renames.json").write_text(json.dumps({"renames": {}}))
    con = sqlite3.connect(root / "state.db")
    con.execute("CREATE TABLE desk_state (data_through TEXT PRIMARY KEY, record_json TEXT)")
    for i, d in enumerate(days):
        rec = {"market": {"phase": "rising, slowing" if i % 2 else "falling, slowing"}, "fed_balance_sheet": {"phase": "flat"},
               "vix": {"third": "calm", "spike_state": "normal"},
               "sectors": {s: {"phase": "rising, speeding up" if j == 0 else "falling, slowing", "leading": j == 0}
                           for j, s in enumerate(SECTORS)}}
        con.execute("INSERT INTO desk_state VALUES (?, ?)", (d, json.dumps(rec)))
    con.commit()
    con.close()
    return days, tilt


RUN = {"run_id": "xs-test", "first_session": "2015-03-02", "last_session": "2015-10-30",
       "signals": [{"name": "on21", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"]},
                   {"name": "mom63", "kind": "momentum", "lookback": 63, "skip": 5, "horizons": ["close_1d"]}],
       "rankings": ["pooled", "within_sector"], "fractions": [0.2], "perms": 1000, "min_ranked": 40, "min_session_bars": 40}


def write_run(root, body):
    p = root / "run.json"
    p.write_text(json.dumps(body))
    return p


def ledger_at(root, name="l"):
    return X.open_ledger(root / f"{name}.jsonl", root / f"{name}.anchors.jsonl", "xs-test-ledger")


def test_run_declares_first_selects_the_planted_effect_and_feeds_the_evidence_adapter(tmp_path):
    build(tmp_path)
    ledger = ledger_at(tmp_path)
    rep = X.run(write_run(tmp_path, RUN), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    assert rep["state"] == "CROSS_SECTION_DEV_RANKING_ONLY" and rep["promotion_allowed"] is False
    assert rep["trial_count"] == 4 and rep["tested"] == 4
    assert set(rep["selected"]) == {"on21|pooled|q20|overnight", "on21|within_sector|q20|overnight"}
    assert rep["ranking"][0]["trial_id"].startswith("on21|")
    assert rep["coverage"]["last"] <= X.DEV_LAST_BAR
    kinds = [json.loads(line)["kind"] for line in (tmp_path / "l.jsonl").read_text().splitlines()][-2:]
    assert kinds == [lse.FACTORY_DECLARED, lse.FACTORY_RESULT]
    assert lse.factory_open_run(ledger) is None
    card = rep["cards"]["on21|within_sector|q20|overnight"]
    assert set(card["sector"]) == set(SECTORS) and set(card["market_phase"]) == {"rising, slowing", "falling, slowing"}
    assert set(card["sector_phase"]) == {"rising, speeding up", "falling, slowing"}
    assert card["sector_vs_market"]["leading"]["nights"] > 100
    # the desk evidence adapter reads the report as written (it admits the cross-section state)
    raw = (tmp_path / "out" / "report.json").read_bytes()
    import hashlib
    now = datetime.now(UTC)
    cards = factory_report_cards(raw, expected_sha256=hashlib.sha256(raw).hexdigest(), receipt_id="r1", observed_at=now,
                                 available_at=now, as_of=now, max_age=timedelta(days=1))
    assert cards["selected_count"] == 2 and cards["tested_trials"] == 4


def test_spreads_match_a_by_hand_computation_and_ignore_later_bars(tmp_path):
    build(tmp_path)
    run = X.RunFile.from_dict(RUN)
    panel = X.load_panel(tmp_path, run)
    states = X.state_arrays(panel, X.load_state(tmp_path / "state.db"))
    res = X.evaluate(panel, run, states)
    r = panel.sessions.index("2015-06-15")
    on = pd.DataFrame(panel.on, index=panel.sessions, columns=panel.cols)
    sig = on.iloc[r - 20:r + 1].clip(-0.10, 0.10).mean()
    order = sig.sort_values(kind="mergesort").index
    k = int(len(order) * 0.2)
    nxt = on.iloc[r + 1]
    assert res["on21|pooled|q20|overnight"]["gross"][r] == pytest.approx(nxt[order[-k:]].mean() - nxt[order[:k]].mean(), abs=1e-12)
    num = den = 0.0
    for s in range(len(SECTORS)):
        names = [c for c in order if c.startswith(f"S{s}")]
        ks = max(1, int(len(names) * 0.2))
        num, den = num + ks * (nxt[names[-ks:]].mean() - nxt[names[:ks]].mean()), den + ks
    assert res["on21|within_sector|q20|overnight"]["gross"][r] == pytest.approx(num / den, abs=1e-12)
    # change every bar after the outcome session: row r must not move
    for p in (tmp_path / "prices").glob("*.csv"):
        df = pd.read_csv(p)
        df.loc[df["date"] > panel.sessions[r + 1], ["open", "close", "high", "low", "adjOpen", "adjClose"]] *= 1.2
        df.to_csv(p, index=False)
    res2 = X.evaluate(X.load_panel(tmp_path, run), run, states)
    for tid in res:
        assert res2[tid]["gross"][r] == res[tid]["gross"][r]


def test_bars_after_the_development_days_are_never_loaded(tmp_path):
    build(tmp_path, n_days=120, extra_late_bar=True)
    panel = X.load_panel(tmp_path, X.RunFile.from_dict({**RUN, "last_session": "2015-05-29"}))
    assert max(panel.sessions) <= X.DEV_LAST_BAR and "2023-01-03" not in panel.sessions


@pytest.mark.parametrize("change", [
    {"last_session": "2023-01-03"}, {"first_session": "2009-12-31"}, {"bogus": 1}, {"rankings": ["by_vibes"]},
    {"signals": [{"name": "x", "kind": "astrology", "lookback": 21, "horizons": ["overnight"]}]},
    {"signals": [{"name": "x", "kind": "momentum", "lookback": 21, "horizons": ["five_years"]}]},
    {"signals": RUN["signals"] + [dict(RUN["signals"][0])]}, {"fractions": [0.9]}, {"alpha": 0.5},
])
def test_run_files_it_refuses(change):
    with pytest.raises(X.RunFileError):
        X.RunFile.from_dict({**RUN, **change})


def test_a_failed_run_is_closed_as_abandoned_and_a_missing_state_record_is_refused_first(tmp_path):
    build(tmp_path, n_days=120)
    ledger = ledger_at(tmp_path)
    body = write_run(tmp_path, {**RUN, "last_session": "2015-05-29"})
    before = len((tmp_path / "l.jsonl").read_text().splitlines())
    with pytest.raises(X.RunFileError):                       # refused before anything is declared
        X.run(body, tmp_path, tmp_path / "missing-state.db", ledger, tmp_path / "out0")
    assert len((tmp_path / "l.jsonl").read_text().splitlines()) == before and not (tmp_path / "out0").exists()
    (tmp_path / "junk.db").write_bytes(b"not a database, long enough to be read as one " * 40)
    with pytest.raises(sqlite3.DatabaseError):
        X.run(body, tmp_path, tmp_path / "junk.db", ledger, tmp_path / "out")
    last = json.loads((tmp_path / "l.jsonl").read_text().splitlines()[-1])
    assert last["kind"] == lse.FACTORY_ABANDONED and lse.factory_open_run(ledger) is None


def test_gates_trade_only_the_nights_and_sectors_they_name(tmp_path):
    build(tmp_path)
    body = {**RUN, "min_ranked_gated": 10, "gates_chosen_after_seeing": False, "signals": [
        {"name": "on21", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"]},
        {"name": "on21_rising", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"], "gate": {"market_phase": ["rising, slowing"]}},
        {"name": "on21_leaders", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"], "gate": {"sector_leading": True}},
        {"name": "on21_no_defensive", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"], "gate": {"groups": ["cyclical", "growth"]}}]}
    run = X.RunFile.from_dict(body)
    panel = X.load_panel(tmp_path, run)
    states = X.state_arrays(panel, X.load_state(tmp_path / "state.db"))
    res = X.evaluate(panel, run, states)
    plain, rising = res["on21|pooled|q20|overnight"]["gross"], res["on21_rising|pooled|q20|overnight"]["gross"]
    on_nights = np.array([p == "rising, slowing" for p in states.night["market_phase"]])
    traded = ~np.isnan(plain)
    assert np.isnan(rising[traded & ~on_nights]).all()                      # turned away
    np.testing.assert_array_equal(rising[traded & on_nights], plain[traded & on_nights])   # same picks when let through
    assert 0.3 < (~np.isnan(rising)).sum() / traded.sum() < 0.7
    leaders = res["on21_leaders|within_sector|q20|overnight"]
    assert np.isnan(leaders["per_sector"][:, 1:]).all() and (~np.isnan(leaders["per_sector"][:, 0])).sum() > 100   # only Energy leads
    np.testing.assert_allclose(leaders["gross"][traded], res["on21|within_sector|q20|overnight"]["per_sector"][traded, 0])
    no_def = res["on21_no_defensive|within_sector|q20|overnight"]
    assert np.isnan(no_def["per_sector"][:, 2]).all()                       # Utilities is defensive
    for change in ({"gate": {"weather": ["sunny"]}}, {"gate": {"groups": ["speculative"]}}, {"gate": {"sector_leading": "yes"}},
                   {"gate": {"market_phase": []}}, {"gate": {"market_phase": ["rising slowly"]}},   # a typo traded nothing before
                   {"gate": {"vix_third": ["scary"]}}, {"gate": {"spike_state": ["spike"]}}):
        with pytest.raises(X.RunFileError):
            X.RunFile.from_dict({**body, "signals": [{**body["signals"][0], **change}]})
    gated = {k: v for k, v in body.items() if k != "gates_chosen_after_seeing"}
    with pytest.raises(X.RunFileError, match="gates_chosen_after_seeing"):          # a gated run must say how its gates were picked
        X.RunFile.from_dict(gated)
    with pytest.raises(X.RunFileError):
        X.RunFile.from_dict({**RUN, "gates_chosen_after_seeing": True})             # and an ungated one must not
    for bad in (0, 64, 2.5):
        with pytest.raises(X.RunFileError):
            X.RunFile.from_dict({**RUN, "null_block_sessions": bad})


def test_turnover_is_full_overnight_and_partial_for_a_slow_signal_held_close_to_close(tmp_path):
    build(tmp_path)
    run = X.RunFile.from_dict(RUN)
    panel = X.load_panel(tmp_path, run)
    states = X.state_arrays(panel, X.load_state(tmp_path / "state.db"))
    res = X.evaluate(panel, run, states)
    over, slow = res["on21|pooled|q20|overnight"], res["mom63|pooled|q20|close_1d"]
    assert np.nanmin(over["turnover"]) == 1.0 == np.nanmax(over["turnover"])
    t = slow["turnover"][~np.isnan(slow["turnover"])]
    assert t[0] == 1.0 and 0.0 < t[1:].mean() < 0.5                         # the first night buys everything, later ones a part
    assert X._turnover(None, np.array([1, 2]), np.array([3, 4])) == 1.0
    assert X._turnover((np.array([1, 2]), np.array([3, 4])), np.array([1, 9]), np.array([3, 4])) == 0.25


def test_sign_flip_bar():
    rng = np.random.default_rng(1)
    x = rng.normal(0, 0.01, (1200, 25))
    x[:150, 3] = np.nan
    bar = X.sign_flip_max_t(x, 2000, 7, 0.05)
    assert bar["critical_t"] > 2.2                       # 25 tries: well above one module's 1.645
    assert (bar["adjusted_p"] <= 0.05).sum() <= 2
    assert (bar["adjusted_p"] >= bar["p"] - 1e-12).all()
    x[:, 9] += 0.0015
    bar = X.sign_flip_max_t(x, 2000, 7, 0.05)
    assert bar["adjusted_p"][9] <= 0.01 and bar["t"][9] > bar["critical_t"]
    thin = np.full((100, 2), np.nan)
    thin[:10, 0] = 0.01
    assert not X.sign_flip_max_t(thin, 1000, 7, 0.05)["tested"].any()


def test_block_flips_widen_the_null_for_clustered_nights():
    rng = np.random.default_rng(5)
    e = rng.normal(0, 0.01, 3000 + 19)
    x = (np.convolve(e, np.ones(20) / 20, mode="valid") + 0.00016)[:, None]   # each night shares 19 of its 20 shocks
    plain = X.sign_flip_max_t(x, 4000, 7, 0.05, block=1)
    blocks = X.sign_flip_max_t(x, 4000, 7, 0.05, block=20)
    assert plain["t"][0] == blocks["t"][0]                     # the statistic is the same; only its null changes
    assert plain["p"][0] < 0.01 and blocks["p"][0] > 5 * plain["p"][0]
    assert blocks["block"] == 20 and plain["block"] == 1
    iid = rng.normal(0, 0.01, (3000, 1))                       # without clustering the two nulls agree closely
    assert abs(X.sign_flip_max_t(iid, 4000, 7, 0.05, block=1)["critical_t"]
               - X.sign_flip_max_t(iid, 4000, 7, 0.05, block=10)["critical_t"]) < 0.2


def test_the_report_names_the_first_step_bar_and_the_lowest_passing_t(tmp_path):
    build(tmp_path)
    rep = X.run(write_run(tmp_path, RUN), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    mt = rep["max_t"]
    assert mt["block_sessions"] == 10 and "blocks of 10" in mt["null"]
    passing = [t["t"] for t in rep["ranking"] if t["passed_step_down"]]
    assert mt["lowest_passing_t"] == min(passing) and rep["hypothesis_only"] is False
    text = (tmp_path / "out" / "report.md").read_text()
    assert "first step's bar" in text and f"{min(passing):.2f}" in text
    assert all(0.0 <= t["dropped_pick_share"] <= 1.0 for t in rep["ranking"])


def test_a_run_whose_gates_were_chosen_after_seeing_selects_nothing(tmp_path):
    build(tmp_path)
    body = {**RUN, "gates_chosen_after_seeing": True, "signals": [
        {"name": "on21", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"], "gate": {"fed_balance_sheet": ["flat"]}}]}
    ledger = ledger_at(tmp_path)
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    assert rep["hypothesis_only"] is True and rep["selected"] == []
    assert any(t["passed_step_down"] for t in rep["ranking"]) and not any(t["selected"] for t in rep["ranking"])
    assert set(rep["cards"]) >= {t["trial_id"] for t in rep["ranking"] if t["passed_step_down"]}
    result = json.loads((tmp_path / "l.jsonl").read_text().splitlines()[-1])
    assert result["kind"] == lse.FACTORY_RESULT and result["selected_count"] == 0
    assert "source of hypotheses only" in rep["note"] and "nothing here is selected" in (tmp_path / "out" / "report.md").read_text()


def test_a_run_file_gets_one_look_per_ledger(tmp_path):
    build(tmp_path, n_days=120)
    ledger = ledger_at(tmp_path)
    body = write_run(tmp_path, {**RUN, "last_session": "2015-05-29"})
    X.run(body, tmp_path, tmp_path / "state.db", ledger, tmp_path / "out1")
    before = (tmp_path / "l.jsonl").read_text()
    with pytest.raises(X.RunFileError, match="already declared"):
        X.run(body, tmp_path, tmp_path / "state.db", ledger, tmp_path / "out2")
    renamed = write_run(tmp_path, {**RUN, "last_session": "2015-05-29", "run_id": "xs-test"})   # same id, same file
    with pytest.raises(X.RunFileError, match="already declared"):
        X.run(renamed, tmp_path, tmp_path / "state.db", ledger, tmp_path / "out3")
    assert (tmp_path / "l.jsonl").read_text() == before and not (tmp_path / "out2").exists()
    (tmp_path / "taken").mkdir()
    other = write_run(tmp_path, {**RUN, "last_session": "2015-05-29", "run_id": "xs-test-2"})
    with pytest.raises(X.RunFileError, match="already exists"):                  # refused before anything is declared
        X.run(other, tmp_path, tmp_path / "state.db", ledger, tmp_path / "taken")
    assert (tmp_path / "l.jsonl").read_text() == before


def test_an_interrupted_run_is_closed_as_abandoned(tmp_path, monkeypatch):
    build(tmp_path, n_days=120)
    ledger = ledger_at(tmp_path)

    def stop(*_a, **_k):
        raise KeyboardInterrupt

    monkeypatch.setattr(X, "load_panel", stop)
    with pytest.raises(KeyboardInterrupt):
        X.run(write_run(tmp_path, {**RUN, "last_session": "2015-05-29"}), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    last = json.loads((tmp_path / "l.jsonl").read_text().splitlines()[-1])
    assert last["kind"] == lse.FACTORY_ABANDONED and "KeyboardInterrupt" in last["reason"] and lse.factory_open_run(ledger) is None


def test_a_pick_with_no_outcome_is_dropped_and_counted():
    sig = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6])
    tick = np.array(list("abcdef"), dtype=object)
    y = np.array([0.01, np.nan, 0.0, 0.0, 0.03, 0.05])
    v, _lo, _hi, dropped = X._spread(sig, tick, np.arange(6), 2, y)
    assert dropped == 1 and v == pytest.approx(0.04 - 0.01)


def test_a_state_record_that_changes_after_declaration_abandons_the_run(tmp_path, monkeypatch):
    build(tmp_path, n_days=120)
    ledger = ledger_at(tmp_path)
    real = X.load_state

    def load_then_append(path):
        rows = real(path)
        con = sqlite3.connect(path)
        con.execute("INSERT INTO desk_state VALUES (?, ?)", ("2030-01-02", "{}"))
        con.commit()
        con.close()
        return rows

    monkeypatch.setattr(X, "load_state", load_then_append)
    with pytest.raises(X.RunFileError, match="changed between"):
        X.run(write_run(tmp_path, {**RUN, "last_session": "2015-05-29"}), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    assert json.loads((tmp_path / "l.jsonl").read_text().splitlines()[-1])["kind"] == lse.FACTORY_ABANDONED


def _rescale(root, sym, price=1.0, volume=1.0):
    p = root / "prices" / f"{sym}.csv"
    df = pd.read_csv(p)
    df[["open", "close", "high", "low", "adjOpen", "adjClose", "adjHigh", "adjLow"]] *= price
    df[["volume", "adjVolume"]] = (df[["volume", "adjVolume"]] * volume).round()
    df.to_csv(p, index=False)


def test_floors_default_to_the_constants_and_a_run_file_may_set_them(tmp_path):
    build(tmp_path, n_days=120)
    _rescale(tmp_path, "S0N00", price=0.03)                     # about $3 a share, $6M a day
    _rescale(tmp_path, "S0N01", volume=0.0025)                  # about $100 a share, $0.5M a day
    body = {**RUN, "last_session": "2015-05-29"}
    default = X.RunFile.from_dict(body)
    assert (default.min_price, default.min_dollar_volume) == (X.MIN_PRICE, X.MIN_DOLLAR_VOLUME) == (5.0, 10e6)
    panel = X.load_panel(tmp_path, default)
    at = {c: j for j, c in enumerate(panel.cols)}
    assert not panel.base[:, at["S0N00"]].any() and not panel.base[:, at["S0N01"]].any() and panel.base[:, at["S0N02"]].any()

    low = X.RunFile.from_dict({**body, "min_price": 2, "min_dollar_volume": 250_000})
    wide = X.load_panel(tmp_path, low)
    assert wide.base[:, at["S0N00"]].any() and wide.base[:, at["S0N01"]].any()
    others = [j for c, j in at.items() if c not in ("S0N00", "S0N01")]
    np.testing.assert_array_equal(wide.base[:, others], panel.base[:, others])   # a floor moves only the names near it
    price_only = X.load_panel(tmp_path, X.RunFile.from_dict({**body, "min_price": 2.0}))
    assert price_only.base[:, at["S0N00"]].sum() == 0           # $6M a day is still under the default $10M
    assert X.load_panel(tmp_path, X.RunFile.from_dict({**body, "min_price": 2.0, "min_dollar_volume": 5e6})).base[:, at["S0N00"]].any()

    # the report names the floors only when the run file set them, so a run on the defaults reads as it did before
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    assert "floors" not in rep["coverage"]
    low_body = {**body, "run_id": "xs-low", "min_price": 2, "min_dollar_volume": 250_000}
    rep2 = X.run(write_run(tmp_path, low_body), tmp_path, tmp_path / "state.db", ledger_at(tmp_path, "l2"), tmp_path / "out2")
    assert rep2["coverage"]["floors"] == {"min_price": 2, "min_dollar_volume": 250_000, "dollar_volume_window": 21}


@pytest.mark.parametrize("change", [
    {"min_price": 0.5}, {"min_price": True}, {"min_price": "5"}, {"min_price": float("nan")}, {"min_price": None},
    {"min_dollar_volume": 10}, {"min_dollar_volume": 1e12}, {"min_dollar_volume": [1e6]},
])
def test_floors_it_refuses(change):
    with pytest.raises(X.RunFileError, match="min_"):
        X.RunFile.from_dict({**RUN, **change})


def test_a_store_with_no_sector_labels_ranks_pooled_and_refuses_anything_that_needs_a_sector(tmp_path):
    build(tmp_path, n_days=120)
    body = {**RUN, "last_session": "2015-05-29", "rankings": ["pooled"]}
    run = X.RunFile.from_dict(body)
    state = X.load_state(tmp_path / "state.db")
    labelled = X.load_panel(tmp_path, run)
    with_labels = X.evaluate(labelled, run, X.state_arrays(labelled, state))

    (tmp_path / "sectors.csv").write_text("Symbol,GICS Sector\n")          # the header alone: no labels
    panel = X.load_panel(tmp_path, run)
    assert panel.sectors == [] and (panel.sector == -1).all()
    assert panel.assignment == {"labels": "none", "labelled_checked": 0, "leave_one_out_accuracy": None,
                                "placed_by_correlation": 0, "unplaced": len(panel.cols)}
    np.testing.assert_array_equal(panel.base, labelled.base)
    states = X.state_arrays(panel, state)
    without = X.evaluate(panel, run, states)
    assert set(without) == set(with_labels) == {"on21|pooled|q20|overnight", "mom63|pooled|q20|close_1d"}
    for tid in without:                                                      # a pooled ranking never read a sector
        np.testing.assert_array_equal(without[tid]["gross"], with_labels[tid]["gross"])
        np.testing.assert_array_equal(without[tid]["turnover"], with_labels[tid]["turnover"])

    night = X.RunFile.from_dict({**body, "gates_chosen_after_seeing": False,   # a gate on the night still works
                                 "signals": [{**RUN["signals"][0], "gate": {"market_phase": ["rising, slowing"]}}]})
    g = X.evaluate(panel, night, states)["on21|pooled|q20|overnight"]["gross"]
    assert 20 < (~np.isnan(g)).sum() < (~np.isnan(without["on21|pooled|q20|overnight"]["gross"])).sum()

    for change, word in (
        ({"rankings": ["pooled", "within_sector"]}, "within_sector ranking"),
        ({"gates_chosen_after_seeing": False, "signals": [{**RUN["signals"][0], "gate": {"groups": ["growth"]}}]}, "gate groups on signal on21"),
        ({"gates_chosen_after_seeing": False, "signals": [{**RUN["signals"][0], "gate": {"sector_leading": True}}]}, "gate sector_leading"),
        ({"signals": [{"name": "sm", "kind": "sector_momentum", "lookback": 63, "horizons": ["close_1d"]}]}, "signal sm"),
    ):
        with pytest.raises(X.RunFileError, match=f"no sector labels.*{word}"):
            X.load_panel(tmp_path, X.RunFile.from_dict({**body, **change}))
    assert run.sector_uses() == [] and X.RunFile.from_dict(RUN).sector_uses() == ["the within_sector ranking"]

    ledger = ledger_at(tmp_path)
    declared = len((tmp_path / "l.jsonl").read_text().splitlines())
    with pytest.raises(X.RunFileError, match="must be a completed export"):   # refused before anything is declared
        X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    assert len((tmp_path / "l.jsonl").read_text().splitlines()) == declared and not (tmp_path / "out").exists()
    sealed = X.seal_export(tmp_path, discovery_first_session="2015-03-02", source={"provider": "synthetic"})
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    assert rep["coverage"]["panel_export"]["generation"] == sealed["generation"]
    assert rep["trial_count"] == 2 and rep["coverage"]["sector_assignment"]["labels"] == "none"
    assert any("no sector labels" in limit for limit in rep["known_limits"])
    assert not any("today's" in limit for limit in rep["known_limits"])
    for c in rep["cards"].values():
        assert "market_phase" in c and "sector" not in c and "group" not in c
    assert "Development ranking only" in (tmp_path / "out" / "report.md").read_text()
    refused = write_run(tmp_path, {**body, "run_id": "xs-needs-sectors", "rankings": ["within_sector"]})
    with pytest.raises(X.RunFileError, match="no sector labels"):          # found on load, so the declared run is closed
        X.run(refused, tmp_path, tmp_path / "state.db", ledger, tmp_path / "out-refused")
    assert json.loads((tmp_path / "l.jsonl").read_text().splitlines()[-1])["kind"] == lse.FACTORY_ABANDONED
    assert lse.factory_open_run(ledger) is None


def test_a_labelled_store_reports_its_sector_limit_as_before(tmp_path):
    build(tmp_path, n_days=120)
    rep = X.run(write_run(tmp_path, {**RUN, "last_session": "2015-05-29"}), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    assert rep["known_limits"][1].startswith("sector labels are today's") and "labels" not in rep["coverage"]["sector_assignment"]
    assert "panel_export" not in rep["coverage"] and "panel_export" not in json.loads((tmp_path / "out" / "data-manifest.json").read_text())


def test_a_members_row_longer_than_the_csv_default_field_limit_loads(tmp_path):
    build(tmp_path, n_days=120)
    run = X.RunFile.from_dict({**RUN, "last_session": "2015-05-29"})
    before = X.load_panel(tmp_path, run)
    with open(tmp_path / "members.csv", newline="") as fh:
        date, tickers = list(csv.reader(fh))[1]
    absent = ",".join(f"US{n:012d}" for n in range(10_000))               # ids with no price file: 150,000 characters
    with open(tmp_path / "members.csv", "w", newline="") as fh:
        csv.writer(fh).writerows([["date", "tickers"], [date, f"{tickers},{absent}"]])
    assert len(f"{tickers},{absent}") > csv.field_size_limit() == 131_072
    after = X.load_panel(tmp_path, run)
    np.testing.assert_array_equal(after.member, before.member)
    np.testing.assert_array_equal(after.base, before.base)
    assert csv.field_size_limit() == 131_072                              # the wider limit does not outlive the read


def _ledger_lines(root, name="l"):
    return (root / f"{name}.jsonl").read_text().splitlines()


def test_an_export_binds_its_manifest_and_its_discovery_start_before_the_run_is_declared(tmp_path):
    body = {**RUN, "last_session": "2015-05-29", "rankings": ["pooled"]}
    # a store whose export says discovery starts 2016-01-04 refuses any earlier window, and declares nothing
    late = tmp_path / "late"
    late.mkdir()
    build(late, n_days=120)
    (late / "sectors.csv").write_text("Symbol,GICS Sector\n")
    X.seal_export(late, discovery_first_session="2016-01-04", source={"provider": "synthetic"})
    ledger = ledger_at(late)
    before = _ledger_lines(late)
    with pytest.raises(X.RunFileError, match="discovery on this export starts 2016-01-04; the run file's window starts 2015-03-02"):
        X.run(write_run(late, body), late, late / "state.db", ledger, late / "out")
    assert _ledger_lines(late) == before and not (late / "out").exists()

    root = tmp_path / "store"
    root.mkdir()
    build(root, n_days=120)
    sealed = X.seal_export(root, discovery_first_session="2015-04-01", source={"provider": "synthetic", "start": "2015-01-02"})
    assert sealed["complete"] is True and sealed["generation"].startswith("g-") and len(sealed["files"]) == 63
    with pytest.raises(FileExistsError):                                    # an export is sealed once
        X.seal_export(root, discovery_first_session="2015-04-01", source={})
    ledger = ledger_at(root)
    before = _ledger_lines(root)
    with pytest.raises(X.RunFileError, match="starts 2015-04-01"):          # one session early is still early
        X.run(write_run(root, {**body, "first_session": "2015-03-31"}), root, root / "state.db", ledger, root / "early")
    assert _ledger_lines(root) == before
    rep = X.run(write_run(root, {**body, "first_session": "2015-04-01"}), root, root / "state.db", ledger, root / "out")
    man = json.loads((root / "out" / "data-manifest.json").read_text())
    raw = (root / X.EXPORT_MANIFEST).read_bytes()
    import hashlib
    assert man["panel_export"] == {"file": "panel-manifest.json", "sha256": hashlib.sha256(raw).hexdigest(),
                                   "generation": sealed["generation"], "discovery_first_session": "2015-04-01"}
    assert rep["coverage"]["panel_export"] == {k: man["panel_export"][k] for k in ("generation", "discovery_first_session", "sha256")}
    declared = json.loads(_ledger_lines(root)[-2])                          # the declaration carries the bound manifest
    assert declared["kind"] == lse.FACTORY_DECLARED and declared["data_manifest_sha256"] == X.digest(man)
    with pytest.raises(X.RunFileError, match="must be a day"):
        X.seal_export(late, discovery_first_session="2016-1-4", source={})


def test_a_directory_that_is_not_the_export_its_manifest_describes_is_refused_before_declaration(tmp_path):
    build(tmp_path, n_days=120)
    X.seal_export(tmp_path, discovery_first_session="2015-03-02", source={"provider": "synthetic"})
    ledger = ledger_at(tmp_path)
    body = write_run(tmp_path, {**RUN, "last_session": "2015-05-29"})
    before = _ledger_lines(tmp_path)
    manifest_path, price = tmp_path / X.EXPORT_MANIFEST, tmp_path / "prices" / "S0N03.csv"
    sealed_text, price_bytes = manifest_path.read_text(), price.read_bytes()
    sealed = json.loads(sealed_text)

    def refused(match):
        with pytest.raises(X.RunFileError, match=match):
            X.run(body, tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
        assert _ledger_lines(tmp_path) == before and not (tmp_path / "out").exists()

    price.write_bytes(price_bytes.replace(b"2000000", b"2000001", 1))       # one file replaced in place
    refused(r"not the export its manifest describes: 1 file\(s\) differ, first \['prices/S0N03.csv'\]")
    price.write_bytes(price_bytes)
    (tmp_path / "prices" / "EXTRA.csv").write_bytes(price_bytes)            # a file the export never listed
    refused("1 file")
    (tmp_path / "prices" / "EXTRA.csv").unlink()
    price.unlink()                                                          # a listed file gone
    refused("1 file")
    price.write_bytes(price_bytes)
    for change, match in (({"complete": False}, "not a completed export"), ({"schema": "something-else/1"}, "not a completed export"),
                          ({"generation": "g-0000000000000000"}, "own generation id"),
                          ({"discovery_first_session": "2015-01-02"}, "own generation id"),   # edited without a new id
                          ({"extra": 1}, "own generation id")):
        manifest_path.write_text(json.dumps({**sealed, **change}))
        refused(match)
    manifest_path.write_text("not json")
    refused("cannot be read")
    manifest_path.write_text(sealed_text)
    rep = X.run(body, tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")   # restored, the export runs
    assert rep["coverage"]["panel_export"]["generation"] == sealed["generation"]


@pytest.mark.parametrize("change", ["replace", "add", "remove", "members", "sectors"])
def test_an_input_file_that_changes_after_declaration_abandons_the_run(tmp_path, monkeypatch, change):
    build(tmp_path, n_days=120)
    if change in ("add", "remove"):
        X.seal_export(tmp_path, discovery_first_session="2015-03-02", source={"provider": "synthetic"})   # holds for an export too
    ledger = ledger_at(tmp_path)
    real = lse.record_factory_declared

    def declare_then_change(*a, **k):
        out = real(*a, **k)
        price = tmp_path / "prices" / "S1N07.csv"
        if change == "replace":                                             # a new version of one file under the same name
            price.write_bytes(price.read_bytes().replace(b"2000000", b"2000001", 1))
        elif change == "add":
            (tmp_path / "prices" / "ZZNEW.csv").write_bytes(price.read_bytes())
        elif change == "remove":
            price.unlink()
        elif change == "members":
            (tmp_path / "members.csv").write_text("date,tickers\n2014-01-02,S0N00\n")
        else:
            (tmp_path / "sectors.csv").write_text("Symbol,GICS Sector\nS0N00,Energy\n")
        return out

    monkeypatch.setattr(lse, "record_factory_declared", declare_then_change)
    with pytest.raises(X.RunFileError, match="changed between the run's declaration and its read"):
        X.run(write_run(tmp_path, {**RUN, "last_session": "2015-05-29"}), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
    assert json.loads(_ledger_lines(tmp_path)[-1])["kind"] == lse.FACTORY_ABANDONED and lse.factory_open_run(ledger) is None
    assert not (tmp_path / "out" / "report.json").exists()


def test_the_universe_uses_the_store_s_sessions_and_the_adjusted_bar_checks(tmp_path):
    days, _ = build(tmp_path, n_days=120)
    floors = {"min_session_bars": 40, "min_price": 5.0, "min_dollar_volume": 10e6}
    whole = X.universe(tmp_path, **floors)
    gap, bad = 60, 80
    for sym, edit in (("S0N02", lambda df: df[df["date"] != days[gap]]),                      # no bar on one session
                      ("S0N03", lambda df: df.assign(adjClose=df["adjClose"].where(df["date"] != days[bad], 0.0)))):   # a zero adjusted close
        df = pd.read_csv(tmp_path / "prices" / f"{sym}.csv")
        edit(df).to_csv(tmp_path / "prices" / f"{sym}.csv", index=False)
    u = X.universe(tmp_path, **floors)
    at = {c: j for j, c in enumerate(u.cols)}
    assert u.sessions == whole.sessions == days                             # the session calendar is the store's, not one stock's
    a, b = at["S0N02"], at["S0N03"]
    # the stock's own previous row (session gap-1) closed above the floor, but the session before gap+1 has no bar: out
    assert whole.base[gap:gap + 2, a].all() and not u.base[gap, a] and not u.base[gap + 1, a] and u.base[gap + 2, a]
    assert whole.base[bad, b] and not u.base[bad, b] and u.base[bad + 1, b]   # the adjusted bar is checked like the raw one
    others = [j for c, j in at.items() if c not in ("S0N02", "S0N03")]
    np.testing.assert_array_equal(u.base[:, others], whole.base[:, others])
    run = X.RunFile.from_dict({**RUN, "last_session": "2015-05-29"})
    panel = X.load_panel(tmp_path, run)                                     # the run's universe is this one, by construction
    np.testing.assert_array_equal(panel.base, u.base)
    assert panel.sessions == u.sessions and panel.cols == u.cols
    assert np.isnan(panel.cc[bad, b]) and np.isnan(panel.cc[bad + 1, b]) and np.isnan(panel.cc[gap + 1, a])


# ── The residual card ──────────────────────────────────────────────────────

def _parts(m):
    att = m["attribution_bp"]
    return sum(att[f] for f in X.FACTORS) + att["sector"] + att["unidentified"] + att["residual"]


def test_the_residual_card_splits_each_spread_exactly_and_never_reaches_the_bar(tmp_path, monkeypatch):
    build(tmp_path)
    rep = X.run(write_run(tmp_path, RUN), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    res = rep["residual"]
    assert res["schema"] == X.RESIDUAL_SCHEMA and set(res["factors"]) == set(X.FACTORS)
    assert set(res["modules"]) == set(rep["cards"])
    for tid, m in res["modules"].items():
        att = m["attribution_bp"]
        assert m["nights"] == rep["cards"][tid]["overall"]["nights"]          # every traded night was fitted
        assert att["gross"] == pytest.approx(rep["cards"][tid]["overall"]["gross_bp"], abs=1e-3)
        assert _parts(m) == pytest.approx(att["gross"], abs=0.01)             # factors + sector + residual = the spread
        assert m["fit"]["identified"] == m["nights"] and att["unidentified"] == 0.0   # independent factors: every night
        assert m["residual"]["gross_bp"] == pytest.approx(att["residual"], abs=1e-3)
        top = [n["bp"] for n in m["names"]["top"]]
        assert len(top) <= X.TOP_NAMES and top == sorted(top, reverse=True) and all(b > 0 for b in top)
        assert m["names"]["gross_without_top_bp"] == pytest.approx(att["gross"] - sum(top), abs=0.02)
        assert m["factor_known_share"]["momentum_12_1"] == 0.0 and m["exposure"]["momentum_12_1"] == 0.0   # 220 days: none
        if "within_sector" in tid:
            assert abs(att["sector"]) < 1e-6                                   # a book balanced inside each sector
    corr = res["correlation"]
    for a, row in corr.items():
        assert a not in row and all(v is None or -1 <= v <= 1 for v in row.values())
        assert all(corr[b][a] == v for b, v in row.items())
    assert corr["on21|pooled|q20|overnight"]["on21|within_sector|q20|overnight"] > 0.5
    assert "## Residual cards" in (tmp_path / "out" / "report.md").read_text()

    # the same run with no residual card: the frozen discovery and every other report field are byte for byte the same
    other = tmp_path / "again"
    other.mkdir()
    monkeypatch.setattr(X, "residual_cards", lambda *a, **k: {})
    rep2 = X.run(write_run(other, RUN), tmp_path, tmp_path / "state.db", ledger_at(other), other / "out")
    assert (tmp_path / "out" / "discovery-frozen.json").read_bytes() == (other / "out" / "discovery-frozen.json").read_bytes()
    assert {k: v for k, v in rep.items() if k != "residual"} == {k: v for k, v in rep2.items() if k != "residual"}

    # the desk evidence and conditional-card adapters still read the report as written
    import hashlib
    raw = (tmp_path / "out" / "report.json").read_bytes()
    now = datetime.now(UTC)
    args = {"expected_sha256": hashlib.sha256(raw).hexdigest(), "receipt_id": "r1", "observed_at": now,
            "available_at": now, "as_of": now, "max_age": timedelta(days=1)}
    assert factory_report_cards(raw, **args)["tested_trials"] == 4
    conditional = conditional_factory_cards(raw, **args)
    assert {c["trial_id"] for c in conditional["cards"]} == set(rep["cards"])
    assert conditional["promotion_allowed"] is False


def test_a_module_that_is_a_known_factor_is_explained_by_it_and_a_new_effect_is_not(tmp_path):
    build(tmp_path, revert=0.15)
    body = {**RUN, "signals": [{"name": "on21", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"]},
                               {"name": "rev21", "kind": "reversal", "lookback": 21, "horizons": ["close_1d"]}],
            "rankings": ["pooled"]}
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    rev, on = rep["residual"]["modules"]["rev21|pooled|q20|close_1d"], rep["residual"]["modules"]["on21|pooled|q20|overnight"]
    assert rev["attribution_bp"]["gross"] > 20                                 # the planted reversal pays
    assert rev["exposure"]["reversal_21"] > 0.75                               # its book is the factor's top and bottom fifth
    assert rev["attribution_bp"]["reversal_21"] > 0.6 * rev["attribution_bp"]["gross"]
    assert abs(rev["attribution_bp"]["residual"]) < 0.35 * rev["attribution_bp"]["gross"]
    assert on["attribution_bp"]["gross"] > 20                                  # the overnight tilt is no factor in the set
    assert on["attribution_bp"]["residual"] > 0.8 * on["attribution_bp"]["gross"]


def test_liquidity_halves_rank_again_inside_each_half(tmp_path):
    liquid = {f"S{s}N{i:02d}" for s in range(len(SECTORS)) for i in range(0, 20, 2)}
    build(tmp_path, volume_of=lambda sym: 8_000_000 if sym in liquid else 2_000_000,
          tilt_of=lambda sym: 0.0 if sym in liquid else 1.0)                   # only the less liquid half has a tilt
    body = {**RUN, "signals": RUN["signals"][:1], "rankings": ["pooled"]}
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    halves = rep["residual"]["modules"]["on21|pooled|q20|overnight"]["liquidity_halves"]
    assert halves["liquid"]["nights"] == halves["other"]["nights"] > 100
    assert halves["other"]["gross_bp"] > 20 and abs(halves["liquid"]["gross_bp"]) < 5
    # longs and shorts both come from the less liquid half, so the book's liquidity gap (longs minus shorts) stays small
    assert abs(rep["residual"]["modules"]["on21|pooled|q20|overnight"]["exposure"]["liquidity_21"]) < 0.2


def test_each_liquidity_half_pays_for_its_own_turnover(tmp_path):
    liquid = {f"S{s}N{i:02d}" for s in range(len(SECTORS)) for i in range(0, 20, 2)}
    full = tmp_path / "full"
    full.mkdir()
    build(full, volume_of=lambda sym: 8_000_000 if sym in liquid else 2_000_000,
          tilt_of=lambda sym: 3.0 if sym in liquid else 0.0)                   # liquid ranks hold still, the others churn
    # the less liquid half sits below the median every night, so it is also a store of its own
    alone = tmp_path / "alone"
    shutil.copytree(full, alone)
    for sym in liquid:
        (alone / "prices" / f"{sym}.csv").unlink()
    for name in ("members.csv", "sectors.csv"):
        with open(alone / name, newline="") as fh:
            rows = list(csv.reader(fh))
        if name == "members.csv":
            rows[1][1] = ",".join(s for s in rows[1][1].split(",") if s not in liquid)
        else:
            rows = [r for r in rows if r[0] not in liquid]
        with open(alone / name, "w", newline="") as fh:
            csv.writer(fh).writerows(rows)

    body = {**RUN, "signals": [{"name": "on21", "kind": "mean_overnight", "lookback": 21, "horizons": ["close_1d"]}],
            "rankings": ["pooled"]}
    tid = "on21|pooled|q20|close_1d"
    rep = X.run(write_run(full, body), full, full / "state.db", ledger_at(full), full / "out")
    halves = rep["residual"]["modules"][tid]["liquidity_halves"]
    liq, oth = halves["liquid"], halves["other"]
    run = X.RunFile.from_dict(body)
    panel = X.load_panel(full, run)
    parent = X.evaluate(panel, run, X.state_arrays(panel, X.load_state(full / "state.db")))[tid]["turnover"]
    assert oth["turnover"] > 3 * liq["turnover"] and oth["turnover"] > 2 * float(np.nanmean(parent))
    for h in (liq, oth):                                       # net = gross - 4 sides x 1 bp x that half's own turnover
        assert h["net_bp"] == pytest.approx(h["gross_bp"] - 4 * run.cost_bp_per_side * h["turnover"], abs=2e-3)

    # ranked on its own, the less liquid half trades the same books on the same nights
    run_alone = X.RunFile.from_dict({**body, "min_ranked": 20, "min_session_bars": 20})   # 30 names, every day
    p_alone = X.load_panel(alone, run_alone)
    res = X.evaluate(p_alone, run_alone, X.state_arrays(p_alone, X.load_state(alone / "state.db")))[tid]
    keep = ~np.isnan(res["gross"])
    assert oth["nights"] == int(keep.sum())
    assert oth["gross_bp"] == pytest.approx(round(float(res["gross"][keep].mean()) * 1e4, 3), abs=1e-3)
    assert oth["turnover"] == pytest.approx(float(res["turnover"][keep].mean()), abs=1e-4)
    assert "Other half: " in (full / "out" / "report.md").read_text() and "turnover" in (full / "out" / "report.md").read_text()


def test_the_identification_check_names_collinear_factors():
    rng = np.random.default_rng(0)
    n = 200
    x = rng.uniform(-0.5, 0.5, (n, 3))
    d = np.column_stack([np.arange(n) % 4 == s for s in range(4)]).astype(float)
    status, cond = X._identified(x, d)
    assert status == X.IDENTIFIED and cond < 3
    assert X._identified(np.column_stack([x, x[:, 0]]), d) == (X.RANK_DEFICIENT, None)               # a factor twice
    assert X._identified(np.column_stack([x, x[:, 0] - 2 * x[:, 1]]), d) == (X.RANK_DEFICIENT, None) # a mix of two
    assert X._identified(np.column_stack([x, d @ [-0.3, -0.1, 0.1, 0.3]]), d) == (X.RANK_DEFICIENT, None)  # the sectors
    status, cond = X._identified(np.column_stack([x, x[:, 0] + 1e-3 * rng.normal(size=n)]), d)
    assert status == X.ILL_CONDITIONED and cond > X.MAX_CONDITION                                    # nearly a twin
    assert X._identified(np.column_stack([x, np.zeros(n)]), d)[0] == X.IDENTIFIED                    # absent: left out
    assert X._identified(np.zeros((n, 2)), d) == (X.IDENTIFIED, None)
    assert X._identified(x, None)[0] == X.IDENTIFIED                                                 # no sector labels


def test_twin_factors_leave_the_named_split_unidentified_and_the_spread_still_adds_up(tmp_path, monkeypatch):
    build(tmp_path)
    body = {**RUN, "signals": RUN["signals"][:1], "rankings": ["pooled"]}
    tid = "on21|pooled|q20|overnight"
    plain = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    plain = plain["residual"]["modules"][tid]
    real = X.factor_matrices

    def twin(panel):                                       # momentum (absent in 220 days) becomes a copy of reversal
        pos, fac = real(panel)
        fac["momentum_12_1"] = fac["reversal_21"].copy()
        return pos, fac

    monkeypatch.setattr(X, "factor_matrices", twin)
    other = tmp_path / "twin"
    other.mkdir()
    rep = X.run(write_run(other, body), tmp_path, tmp_path / "state.db", ledger_at(other), other / "out")
    m = rep["residual"]["modules"][tid]
    att = m["attribution_bp"]
    assert m["fit"]["rank_deficient"] == m["nights"] == plain["nights"] and m["fit"]["identified"] == 0
    assert all(att[f] == 0.0 for f in X.FACTORS) and att["sector"] == 0.0     # nothing is credited to a named part
    assert att["unidentified"] == pytest.approx(att["gross"] - att["residual"], abs=2e-3)
    assert _parts(m) == pytest.approx(att["gross"], abs=0.01)
    assert att["residual"] == pytest.approx(plain["attribution_bp"]["residual"], abs=1e-3)   # the residual is unchanged
    assert m["exposure"]["momentum_12_1"] == m["exposure"]["reversal_21"]     # exposure is a fact about the book
    assert f"Named split identified on 0 of {m['nights']:,} nights" in (other / "out" / "report.md").read_text()


def test_a_run_with_nothing_carded_has_an_empty_residual_section(tmp_path):
    build(tmp_path)
    body = {**RUN, "signals": RUN["signals"][1:], "cards": 0}
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger_at(tmp_path), tmp_path / "out")
    assert rep["cards"] == {} and rep["residual"]["modules"] == {} and rep["residual"]["correlation"] == {}
    assert "Residual cards" not in (tmp_path / "out" / "report.md").read_text()


PRICES = ("open", "high", "low", "close", "adjOpen", "adjHigh", "adjLow", "adjClose")


def _edit_row(root, sym, day, **cells):
    """Rewrite one served row of a price file, cell by cell as text; every other row keeps its bytes."""
    p = root / "prices" / f"{sym}.csv"
    lines = p.read_text().splitlines()
    n = next(i for i, line in enumerate(lines) if line.startswith(day + ","))
    row = lines[n].split(",")
    for name, text in cells.items():
        row[HEADER.index(name)] = text
    lines[n] = ",".join(row)
    p.write_text("\n".join(lines) + "\n")


@pytest.mark.parametrize("volume", [0, 2_000_000])
@pytest.mark.parametrize("field", PRICES)
def test_a_row_with_any_one_of_its_eight_prices_blank_is_not_a_valid_bar(tmp_path, field, volume):
    days, _ = build(tmp_path, n_per=10, n_days=60)
    floors = {"min_session_bars": 20, "min_price": 5.0, "min_dollar_volume": 10e6}
    whole = X.universe(tmp_path, **floors)
    sym, r = "S1N04", 40
    j = whole.cols.index(sym)
    assert whole.ok.iloc[r, j] and whole.base[r, j]
    _edit_row(tmp_path, sym, days[r], **{field: "", "volume": str(volume), "adjVolume": str(volume)})
    u = X.universe(tmp_path, **floors)
    assert u.sessions == whole.sessions == days and u.cols == whole.cols
    assert np.isnan(u.frames[field].iloc[r, j])                              # the blank is read as served, not filled
    assert all(u.frames[k].iloc[r, j] > 0 for k in PRICES if k != field)     # and it is the row's only blank
    assert not u.ok.iloc[r, j] and not u.base[r, j]
    assert u.ok.iloc[r - 1, j] and u.base[r - 1, j]
    others = [k for k in range(len(u.cols)) if k != j]
    np.testing.assert_array_equal(u.ok.values[:, others], whole.ok.values[:, others])
    np.testing.assert_array_equal(u.base[:, others], whole.base[:, others])
    panel = X.load_panel(tmp_path, X.RunFile.from_dict({**RUN, "min_session_bars": 20}))
    np.testing.assert_array_equal(panel.base, u.base)
    assert not panel.base[r, j] and np.isnan(panel.intra[r, j]) and np.isnan(panel.cc[r, j]) and np.isnan(panel.on[r, j])


def test_a_sealed_export_with_a_row_blank_only_in_its_adjusted_high_is_out_that_session(tmp_path):
    # 25 companies, 60 weekday sessions from 2016-02-01, $20 bars, $2M a day, no sector labels; sealed directly with
    # seal_export, which hashes the files and never parses a row
    days = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2016-02-01", periods=60)]
    syms = [f"C{i:02d}" for i in range(25)]
    witness = "2016-04-01,20,21,19,20,0,20,,19,20,0,0,1"
    (tmp_path / "prices").mkdir()
    for sym in syms:
        rows = [witness if (sym, d) == ("C07", "2016-04-01") else f"{d},20,21,19,20,100000,20,21,19,20,100000,0,1" for d in days]
        (tmp_path / "prices" / f"{sym}.csv").write_text("\n".join([",".join(HEADER), *rows]) + "\n")
    (tmp_path / "members.csv").write_text(f'date,tickers\n2016-01-04,"{",".join(syms)}"\n')
    (tmp_path / "sectors.csv").write_text("Symbol,GICS Sector\n")
    (tmp_path / "renames.json").write_text(json.dumps({"renames": {}}))
    sealed = X.seal_export(tmp_path, discovery_first_session="2016-02-01", source={"provider": "synthetic"})
    files = X.input_files(tmp_path)
    assert X.read_export(tmp_path, files)["generation"] == sealed["generation"]
    assert (witness + "\n").encode() in (tmp_path / "prices" / "C07.csv").read_bytes()

    u = X.universe(tmp_path, min_session_bars=20, min_price=1.0, min_dollar_volume=1e6, files=files)
    r, j = u.sessions.index("2016-04-01"), u.cols.index("C07")
    assert (r, len(u.sessions), len(u.cols)) == (44, 60, 25)
    assert not u.ok.iloc[r, j] and not u.base[r, j]
    assert u.ok.iloc[r - 1, j] and u.base[r - 1, j] and u.ok.iloc[r + 1, j] and u.base[r + 1, j]   # one session, not the company
    others = [k for k in range(25) if k != j]
    assert u.ok.values[:, others].all() and u.base[r, others].all()
    run = X.RunFile.from_dict({**RUN, "first_session": "2016-03-01", "last_session": "2016-04-20", "rankings": ["pooled"],
                               "min_ranked": 20, "min_session_bars": 20, "min_price": 1, "min_dollar_volume": 1_000_000})
    panel = X.load_panel(tmp_path, run, files)                              # the verified read a run makes
    np.testing.assert_array_equal(panel.base, u.base)
    assert not panel.base[r, j] and panel.base[r, others].all()
    assert np.isnan(panel.cc[r, j]) and np.isnan(panel.cc[r + 1, j])        # no return into or out of the blank session


FEATURES = ({"kind": "momentum", "lookback": 5, "skip": 0}, {"kind": "reversal", "lookback": 5},
            {"kind": "high_52w", "lookback": 10}, {"kind": "sector_momentum", "lookback": 5, "skip": 0},
            {"kind": "mean_overnight", "lookback": 5}, {"kind": "mean_intraday", "lookback": 5})
SMALL = {"min_session_bars": 20, "min_price": 5.0, "min_dollar_volume": 10e6}


def _small_run():
    return X.RunFile.from_dict({**RUN, "min_session_bars": 20})


@pytest.mark.parametrize("text", ["inf", "-inf"])
@pytest.mark.parametrize("field", PRICES)
def test_a_price_that_is_not_a_finite_number_is_read_as_a_blank(tmp_path, field, text):
    days, _ = build(tmp_path, n_per=10, n_days=60)
    whole = X.universe(tmp_path, **SMALL)
    sym, r = "S1N04", 40
    j = whole.cols.index(sym)
    _edit_row(tmp_path, sym, days[r], **{field: text})
    u = X.universe(tmp_path, **SMALL)
    assert np.isnan(u.frames[field].iloc[r, j])                              # never held as inf
    assert all(np.isfinite(v.values[~np.isnan(v.values)]).all() for v in u.frames.values())
    assert whole.ok.iloc[r, j] and whole.base[r, j] and not u.ok.iloc[r, j] and not u.base[r, j]
    assert u.base[r + 1, j] == (field != "close")                            # no finite prior close, no price floor passed
    others = [k for k in range(len(u.cols)) if k != j]
    np.testing.assert_array_equal(u.ok.values[:, others], whole.ok.values[:, others])
    np.testing.assert_array_equal(u.base[:, others], whole.base[:, others])
    panel = X.load_panel(tmp_path, _small_run())
    np.testing.assert_array_equal(panel.base, u.base)
    for a in (panel.on, panel.intra, panel.cc, panel.adj, *(X.signal_matrix(panel, spec) for spec in FEATURES)):
        assert not np.isinf(a).any()                                         # no return, close or signal is infinite
    assert np.isnan(panel.adj[r, j]) and np.isnan(panel.cc[r, j]) and np.isnan(panel.on[r, j]) and np.isnan(panel.intra[r, j])


def test_a_volume_that_is_not_a_finite_number_does_not_pass_the_liquidity_floor(tmp_path):
    days, _ = build(tmp_path, n_per=10, n_days=60)
    sym = "S1N04"
    _rescale(tmp_path, sym, volume=0.0025)                                   # about $0.5M a day against a $10M floor
    thin = X.universe(tmp_path, **SMALL)
    j = thin.cols.index(sym)
    assert thin.ok.values[:, j].all() and not thin.base[:, j].any()
    for d in days[25:40]:                                                    # 15 of any 21 sessions: the median would be inf
        _edit_row(tmp_path, sym, d, volume="inf", adjVolume="inf")
    u = X.universe(tmp_path, **SMALL)
    assert np.isnan(u.frames["volume"].values[25:40, j]).all()
    assert u.ok.values[:, j].all() and not u.base[:, j].any()                # the bars stay valid; the floor is still not met
    others = [k for k in range(len(u.cols)) if k != j]
    np.testing.assert_array_equal(u.base[:, others], thin.base[:, others])


def _same(a, b):
    return (a == b) | (np.isnan(a) & np.isnan(b))


@pytest.mark.parametrize("blank", [("adjHigh",), ("low", "adjLow")])
def test_an_invalid_bar_s_adjusted_close_reaches_no_feature(tmp_path, blank):
    # the bar is invalid for a blank price; its adjusted close is present and wrong by a factor of 10, then of 100
    days, _ = build(tmp_path, n_per=10, n_days=60)
    run = _small_run()
    clean = X.load_panel(tmp_path, run)
    sym, r = "S1N04", 40
    j = clean.cols.index(sym)
    mates = [k for k, c in enumerate(clean.cols) if c.startswith("S1") and k != j]
    panels = []
    for factor in (10, 100):
        _edit_row(tmp_path, sym, days[r], adjClose=repr(float(clean.adj[r, j]) * factor), **{name: "" for name in blank})
        panels.append(X.load_panel(tmp_path, run))
    p = panels[0]

    # the panel is the clean one with that one bar taken out: nothing else moved, and the wrong close is nowhere
    want = {"adj": clean.adj.copy(), "on": clean.on.copy(), "cc": clean.cc.copy(), "intra": clean.intra.copy(), "base": clean.base.copy()}
    want["adj"][r, j] = want["intra"][r, j] = np.nan
    want["on"][r:r + 2, j] = want["cc"][r:r + 2, j] = np.nan
    want["base"][r, j] = False
    for q in panels:
        for name, a in want.items():
            np.testing.assert_array_equal(getattr(q, name), a)
        np.testing.assert_array_equal(q.sector, clean.sector)

    for spec in FEATURES:
        before, sig = X.signal_matrix(clean, spec), X.signal_matrix(p, spec)
        np.testing.assert_array_equal(sig, X.signal_matrix(panels[1], spec))   # what the invalid close says changes nothing
        assert np.nanmax(np.abs(sig)) <= np.nanmax(np.abs(before))             # no signal is larger than on the clean store
        if spec["kind"] in ("momentum", "reversal"):
            # adjacent sessions: the two whose window ends or starts on the invalid bar have no signal; the rest are untouched
            assert np.flatnonzero(~_same(sig, before)[:, j]).tolist() == [r + 1, r + 6]
            assert np.isnan(sig[[r + 1, r + 6], j]).all() and _same(np.delete(sig, j, axis=1), np.delete(before, j, axis=1)).all()
        if spec["kind"] == "high_52w":
            assert np.isnan(sig[r + 1, j])                                     # the close of the session before is the invalid one
            for d in range(r + 2, r + 11):                                     # later windows take their highest close without it
                window = [clean.adj[s, j] for s in range(d - 10, d) if s != r]
                assert sig[d, j] == clean.adj[d - 1, j] / max(window)
        if spec["kind"] == "sector_momentum":
            # another symbol's sector aggregate: the mean of its sector's other members, the invalid name left out
            own = pd.DataFrame(clean.adj).shift(1) / pd.DataFrame(clean.adj).shift(6) - 1
            for d in (r + 1, r + 6):
                np.testing.assert_allclose(sig[d, mates], own.values[d, mates].mean(), rtol=1e-12)
            rest = [d for d in range(len(days)) if d not in (r + 1, r + 6)]
            assert _same(sig[rest], before[rest]).all()
            assert _same(sig[:, [k for k in range(len(clean.cols)) if k not in mates and k != j]],
                         before[:, [k for k in range(len(clean.cols)) if k not in mates and k != j]]).all()


def test_on_a_clean_store_the_reader_holds_every_cell_as_served(tmp_path):
    days, _ = build(tmp_path, n_per=10, n_days=60)
    u = X.universe(tmp_path, **SMALL)
    assert u.sessions == days and u.ok.values.all()                          # every row of a clean store is a valid bar
    for field, frame in u.frames.items():                                    # each cell is the file's own number
        for sym in u.cols:
            served = pd.read_csv(tmp_path / "prices" / f"{sym}.csv").set_index("date")[field]
            np.testing.assert_array_equal(frame[sym].values, served.reindex(days).values)
    panel = X.load_panel(tmp_path, _small_run())
    f = u.frames
    np.testing.assert_array_equal(panel.adj, f["adjClose"].values)           # nothing is masked where every bar is valid
    np.testing.assert_array_equal(panel.intra, (f["close"] / f["open"] - 1).values)
    np.testing.assert_array_equal(panel.cc, (f["adjClose"] / f["adjClose"].shift(1) - 1).values)
    np.testing.assert_array_equal(panel.on, (f["adjOpen"] / f["adjClose"].shift(1) - 1).values)
