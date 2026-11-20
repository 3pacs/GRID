"""Cross-section factory: declared before data, development days only, by-hand arithmetic, the sign-flip bar."""
import csv
import json
import sqlite3
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from analysis import cross_section_factory as X
from analysis import ledger_steered_exploration as lse
from analysis.desk_factory_report import factory_report_cards

SECTORS = ("Energy", "Information Technology", "Utilities")
HEADER = ["date", "close", "high", "low", "open", "volume", "adjClose", "adjHigh", "adjLow", "adjOpen", "adjVolume", "divCash", "splitFactor"]


def build(root, n_per=20, n_days=220, seed=3, extra_late_bar=False):
    """Synthetic bars: every stock has its own steady overnight tilt that it gives back during the day."""
    rng = np.random.default_rng(seed)
    days = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2015-01-02", periods=n_days)]
    (root / "prices").mkdir(parents=True)
    syms, tilt = [], {}
    for s, sector in enumerate(SECTORS):
        for i in range(n_per):
            sym = f"S{s}N{i:02d}"
            syms.append((sym, sector))
            tilt[sym] = rng.normal(0, 0.002)
    for sym, _ in syms:
        prev, rows = 100.0, []
        for d in days:
            o = prev * (1 + tilt[sym] + rng.normal(0, 0.003))
            c = o * (1 - tilt[sym] + rng.normal(0, 0.003))
            rows.append([d, c, max(o, c) * 1.001, min(o, c) * 0.999, o, 2_000_000, c, max(o, c) * 1.001, min(o, c) * 0.999, o, 2_000_000, 0.0, 1.0])
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
    rep = X.run(write_run(tmp_path, body), tmp_path, tmp_path / "state.db", ledger, tmp_path / "out")
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
