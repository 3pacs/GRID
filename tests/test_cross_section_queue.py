"""Hypothesis queue from published effects (#1212): the file loads, its order is kept, each entry gets one look."""
import copy
import json

import numpy as np
import pandas as pd
import pytest

from analysis import cross_section_factory as X
from analysis import cross_section_queue as Q
from tests.test_cross_section_factory import RUN, SECTORS, build


def small_queue(**defaults) -> dict:
    q = {"schema": Q.SCHEMA, "defaults": {"rankings": ["pooled", "within_sector"], "fractions": [0.2], "perms": 1000,
                                          "first_session": "2015-03-02", "last_session": "2015-10-30",
                                          "min_ranked": 40, "min_session_bars": 40, **defaults},
         "entries": []}
    base = {"version": 1, "status": "queued", "paper": {"authors": "A. Author", "year": 2000, "title": "T"},
            "paper_sample": "s", "stated_effect": {"text": "x", "checked": "not_checked", "source": None},
            "mechanism": "m", "needs": ["daily prices"], "deviation": "d"}
    q["entries"] = [
        {**base, "id": "first", "signals": [{"name": "on21", "kind": "mean_overnight", "lookback": 21, "horizons": ["overnight"]}]},
        {**base, "id": "blocked-one", "status": "blocked", "blocked_reason": "needs data", "signals": []},
        {**base, "id": "second", "signals": [{"name": "rev5", "kind": "reversal", "lookback": 5, "horizons": ["close_1d"]}],
         "prior_dev_looks": ["xs-earlier-1"]},
    ]
    return q


def write(root, q):
    p = root / "queue.json"
    p.write_text(json.dumps(q))
    return p


def test_the_real_queue_loads_and_every_queued_entry_is_a_run_the_factory_accepts():
    q = Q.load_queue()
    ids = [e["id"] for e in q["entries"]]
    assert len(ids) == len(set(ids))
    queued = [e for e in q["entries"] if e["status"] == "queued"]
    assert len(queued) >= 3
    for e in queued:
        body = Q.run_file(q, e)
        assert X.RunFile.from_dict(body).run_id == f"xsq-{e['id']}-v{e['version']}"
        assert Q.run_file(q, e) == body                                    # the same entry gives the same file
    assert any(e["status"] == "blocked" and e["blocked_reason"] for e in q["entries"])
    for e in q["entries"]:
        if e["stated_effect"]["checked"] != "not_checked":
            assert e["stated_effect"]["source"]


@pytest.mark.parametrize("change", [
    lambda q: q["entries"][0].update(stated_effect={"text": "x", "checked": "secondary", "source": None}),
    lambda q: q["entries"][0].update(stated_effect={"text": "x", "checked": "peer review"}),
    lambda q: q["entries"][0].pop("deviation"),
    lambda q: q["entries"][1].pop("blocked_reason"),
    lambda q: q["entries"][0].update(colour="blue"),
    lambda q: q["entries"].append(copy.deepcopy(q["entries"][0])),
    lambda q: q["entries"][0].update(signals=[{"name": "x", "kind": "astrology", "lookback": 21, "horizons": ["overnight"]}]),
    lambda q: q["defaults"].update(bogus=1),
    lambda q: q.update(schema="other/1"),
])
def test_queue_files_it_refuses(tmp_path, change):
    q = small_queue()
    change(q)
    with pytest.raises(Q.QueueError):
        Q.load_queue(write(tmp_path, q))


def test_the_queue_runs_in_order_one_look_per_entry_and_cards_say_where_it_paid(tmp_path):
    build(tmp_path)
    qp = write(tmp_path, small_queue())
    ledger = X.open_ledger(tmp_path / "l.jsonl", tmp_path / "l.anchors.jsonl", "xs-queue-test")
    q = Q.load_queue(qp)
    assert Q.next_entry(q, ledger)["id"] == "first"
    rep = Q.run_next(qp, tmp_path, tmp_path / "state.db", ledger, tmp_path / "runs")
    assert rep["run_id"] == "xsq-first-v1" and (tmp_path / "runs" / "xsq-first-v1.run.json").exists()
    card = next(iter(rep["cards"].values()))
    assert isinstance(card["where_it_paid"], list) and isinstance(card["where_it_did_not"], list)
    for row in card["where_it_paid"]:
        assert row["net_bp"] > 0 and row["nights"] >= X.PAID_MIN_NIGHTS
    assert "Paid after cost" in (tmp_path / "runs" / "xsq-first-v1" / "report.md").read_text()
    assert Q.next_entry(q, ledger)["id"] == "second"                        # the blocked entry is passed over
    rep2 = Q.run_next(qp, tmp_path, tmp_path / "state.db", ledger, tmp_path / "runs")
    assert rep2["max_t"]["prior_dev_looks"] == ["xs-earlier-1"] and "xs-earlier-1" in rep2["max_t"]["multiplicity"]
    st = {s["id"]: s for s in Q.status(q, ledger)}
    assert st["first"]["run"] == st["second"]["run"] == "result" and st["blocked-one"]["run"] is None
    with pytest.raises(Q.QueueError, match="no queued entry"):
        Q.run_next(qp, tmp_path, tmp_path / "state.db", ledger, tmp_path / "runs")
    bumped = small_queue()
    bumped["entries"][0]["version"] = 2                                    # a changed entry is a new version
    assert Q.next_entry(Q.load_queue(write(tmp_path, bumped)), ledger)["id"] == "first"


def test_sector_momentum_gives_each_stock_its_sectors_mean_and_ranks_pooled_only(tmp_path):
    build(tmp_path)
    spec = {"name": "smom", "kind": "sector_momentum", "lookback": 21, "skip": 0, "horizons": ["close_1d"]}
    run = X.RunFile.from_dict({**RUN, "rankings": ["pooled"], "signals": [spec]})
    panel = X.load_panel(tmp_path, run)
    sig = X.signal_matrix(panel, spec)
    adj = pd.DataFrame(panel.adj)
    own = (adj.shift(1) / adj.shift(22) - 1).values
    r = panel.sessions.index("2015-06-15")
    for k in range(len(SECTORS)):
        cols = panel.sector == k
        assert np.allclose(sig[r, cols], np.nanmean(np.where(panel.member[r, cols], own[r, cols], np.nan)))
    with pytest.raises(X.RunFileError, match="pooled only"):
        X.RunFile.from_dict({**RUN, "signals": [spec]})                   # RUN ranks within sector too


def test_paid_split_names_cuts_with_enough_nights_by_sign_and_size():
    c = {"overall": {"nights": 900, "net_bp": 1.0, "t_gross": 1.0},
         "year": {"2015": {"nights": 250, "net_bp": 2.0, "t_gross": 1.2}, "2016": {"nights": 250, "net_bp": -1.5, "t_gross": -0.4},
                  "2017": {"nights": 40}},
         "vix_third": {"calm": {"nights": 300, "net_bp": 4.0, "t_gross": 2.0}, "fearful": {"nights": 99, "net_bp": 9.0, "t_gross": 1.0}}}
    paid, not_paid = X.paid_split(c)
    assert [(r["cut"], r["value"]) for r in paid] == [("vix_third", "calm"), ("year", "2015")]
    assert [(r["cut"], r["value"]) for r in not_paid] == [("year", "2016")]
