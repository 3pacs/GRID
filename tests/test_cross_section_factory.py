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
    # every field the desk evidence adapter reads is there; only the state label is new
    raw = (tmp_path / "out" / "report.json").read_bytes().replace(b"CROSS_SECTION_DEV_RANKING_ONLY", b"INTRADAY_DEV_RANKING_ONLY")
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
    body = {**RUN, "min_ranked_gated": 10, "signals": [
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
                   {"gate": {"market_phase": []}}):
        with pytest.raises(X.RunFileError):
            X.RunFile.from_dict({**body, "signals": [{**body["signals"][0], **change}]})


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
