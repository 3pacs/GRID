"""trade_edge v2: scoreboard math (mean, median, win rate, clustered t) and label rule."""

from __future__ import annotations

import math
import statistics
from datetime import date, timedelta

import pytest

from paper_log.trade_edge import scoreboard as sb
from paper_log.trade_edge.config import (
    LABEL_CONTRARY,
    LABEL_NOT_SUPPORTED,
    LABEL_SUPPORTED,
    LABEL_UNPROVEN,
)


def test_clustered_t_equals_ordinary_t_with_one_obs_per_cluster():
    x = [0.05, -0.02, 0.10, 0.03, -0.01, 0.04]
    t, g = sb.clustered_t(x, list(range(len(x))))
    ordinary = statistics.mean(x) / (statistics.stdev(x) / math.sqrt(len(x)))
    assert g == len(x)
    assert t == pytest.approx(ordinary)


def test_clustered_t_hand_computed():
    x = [0.10, 0.06, -0.02, 0.02]
    c = ["d1", "d1", "d2", "d2"]
    # mean 0.04; residual sums: d1 0.06+0.02=0.08, d2 -0.06-0.02=-0.08
    var = (2 / 1) * (0.08 ** 2 + 0.08 ** 2) / 16
    t, g = sb.clustered_t(x, c)
    assert g == 2
    assert t == pytest.approx(0.04 / math.sqrt(var))


def test_clustering_widens_se_for_correlated_dates():
    x = [0.05, 0.05, 0.05, -0.01, -0.01, -0.01, 0.02, 0.02, 0.02]
    t_ind, _ = sb.clustered_t(x, list(range(9)))
    t_cl, g = sb.clustered_t(x, [0, 0, 0, 1, 1, 1, 2, 2, 2])
    assert g == 3 and abs(t_cl) < abs(t_ind)


def test_clustered_t_undefined_cases():
    assert sb.clustered_t([], []) == (None, 0)
    assert sb.clustered_t([0.1], ["a"]) == (None, 1)
    assert sb.clustered_t([0.1, 0.2], ["a", "a"]) == (None, 1)
    assert sb.clustered_t([0.1, 0.1], ["a", "b"])[0] is None  # zero variance
    with pytest.raises(ValueError):
        sb.clustered_t([0.1], [])


def row(net, session="2026-10-07", status="closed", gross=None):
    return {"net_excess": net, "gross_excess": net + 0.001 if gross is None else gross,
            "entry_session": session, "status": status}


def test_summarize_mean_median_win_rate_and_costs():
    rows = [row(0.10), row(-0.05, "2026-10-08"), row(-0.01, "2026-10-09", "closed_delisted"), row(0.02, "2026-10-09")]
    s = sb.summarize(rows, n_open=3)
    assert s["n_open"] == 3 and s["n_closed"] == 4 and s["n_closed_delisted"] == 1
    assert s["mean_net_excess"] == pytest.approx(0.015)
    assert s["median_net_excess"] == pytest.approx(0.005)
    assert s["win_rate"] == pytest.approx(0.5)
    assert s["mean_gross_excess"] == pytest.approx(0.016)
    assert s["mean_net_v1_cost"] == pytest.approx(0.016 - 0.0005)
    assert s["mean_net_delist_penalty"] == pytest.approx(0.015 - 0.30 / 4)
    assert s["n_clusters"] == 3


def test_summarize_empty():
    s = sb.summarize([], n_open=2)
    assert s["n_closed"] == 0 and s["mean_net_excess"] is None and s["clustered_t"] is None


def test_cost_table():
    assert sb.cost_round_trip(">=2B") == pytest.approx(0.0010)
    assert sb.cost_round_trip("300M-2B") == pytest.approx(0.0030)
    assert sb.cost_round_trip("<300M") == pytest.approx(0.0100)
    assert sb.cost_round_trip("unknown") == pytest.approx(0.0100)


# ── label rule ──────────────────────────────────────────────────────────────


def closed_rows(values, n_dates=50):
    start = date(2026, 10, 8)
    out = []
    for i, v in enumerate(values):
        d = start + timedelta(days=i % n_dates)
        out.append({"net_excess": v, "entry_session": d.isoformat(),
                    "exit_session": (d + timedelta(days=45 + i // n_dates)).isoformat(),
                    "position_id": f"cik:{i}|{d.isoformat()}"})
    return out


def alternating(n, hi, lo):
    return [hi if i % 2 == 0 else lo for i in range(n)]


def test_label_unproven_before_first_look_even_if_strong():
    lab = sb.label_state(closed_rows([0.05] * 50 + [0.04] * 49))
    assert lab["label"] == LABEL_UNPROVEN
    assert lab["looks_done"] == 0 and lab["next_look_at_n_closed"] == 100


def test_label_supported_at_first_look():
    lab = sb.label_state(closed_rows(alternating(100, 0.08, 0.01)))
    assert lab["label"] == LABEL_SUPPORTED and lab["decided_at_look"] == 100
    assert lab["look_stats"]["clustered_t"] >= 2.3


def test_label_mean_driven_by_outliers_with_negative_median_stays_unproven():
    vals = [-0.01] * 90 + [0.60] * 10  # mean +5.1%, median -1%
    lab = sb.label_state(closed_rows(vals))
    assert lab["look_stats"]["median_net_excess"] < 0
    assert lab["label"] == LABEL_UNPROVEN and lab["next_look_at_n_closed"] == 200


def test_label_requires_enough_entry_dates():
    lab = sb.label_state(closed_rows(alternating(100, 0.08, 0.01), n_dates=20))
    assert lab["label"] == LABEL_UNPROVEN


def test_label_contrary():
    lab = sb.label_state(closed_rows(alternating(100, -0.08, -0.01)))
    assert lab["label"] == LABEL_CONTRARY


def test_label_not_supported_after_third_look_and_uses_first_n_only():
    noise = alternating(300, 0.05, -0.05)
    lab = sb.label_state(closed_rows(noise))
    assert lab["label"] == LABEL_NOT_SUPPORTED and lab["looks_done"] == 3
    # 250 closed: two looks done, still UNPROVEN, waiting for 300
    lab2 = sb.label_state(closed_rows(noise[:250]))
    assert lab2["label"] == LABEL_UNPROVEN and lab2["looks_done"] == 2


def test_banner_text():
    assert sb.banner("UNPROVEN") == "UNPROVEN — not investment advice, research paper log"


def test_build_scoreboard_splits_and_survivorship_warning():
    entries = [
        {"position_id": "a", "status": "opened", "stratum": "large", "cap_bucket": "<300M", "entry_session": "2026-10-08"},
        {"position_id": "b", "status": "opened", "stratum": "large", "cap_bucket": ">=2B", "entry_session": "2026-10-09"},
        {"position_id": "c", "status": "opened", "stratum": "small", "cap_bucket": ">=2B", "entry_session": "2026-10-09"},
        {"position_id": "d", "status": "no_price", "stratum": "large", "cap_bucket": "unknown", "entry_session": "2026-10-09"},
        {"position_id": "e", "status": "unresolved_ticker", "stratum": "small", "cap_bucket": "unknown",
         "entry_session": "2026-10-09", "late_logged": True},
    ]
    exits = [
        {"position_id": "a", "horizon": 30, "status": "closed_delisted", "net_excess": -0.2, "gross_excess": -0.19,
         "exit_session": "2026-11-19"},
        {"position_id": "a", "horizon": 5, "status": "closed", "net_excess": 0.03, "gross_excess": 0.04,
         "exit_session": "2026-10-15"},
        {"position_id": "c", "horizon": 30, "status": "closed", "net_excess": 0.01, "gross_excess": 0.011,
         "exit_session": "2026-11-20"},
    ]
    board = sb.build_scoreboard(entries, exits, late_filing_lines=2, grid_db_accessions=1)
    t = board["tables"]
    assert t["h30_large"]["all"]["n_closed"] == 1 and t["h30_large"]["all"]["n_open"] == 1
    assert t["h30_large"]["<300M"]["n_closed_delisted"] == 1
    assert t["h30_large"][">=2B"]["n_open"] == 1
    assert t["h30_small"]["all"]["n_closed"] == 1
    assert t["h5_large"]["all"]["mean_net_excess"] == pytest.approx(0.03)
    m = board["missing_labels"]
    assert m["no_price"] == 1 and m["unresolved_ticker"] == 1 and m["closed_delisted_h30"] == 1
    assert m["late_filing_lines"] == 2 and m["grid_db_accessions"] == 1 and m["late_logged"] == 1
    # primary stratum: 3 large positions, 2 missing (no_price + delisted)
    assert m["primary_missing_share"] == pytest.approx(2 / 3, abs=1e-4)
    assert m["survivorship_warning"] is True
    assert board["label"]["label"] == LABEL_UNPROVEN
    assert board["banner"].startswith("UNPROVEN — not investment advice")


# ── §10 looks are decided once and read back (review finding F1) ───────────


def look_rec(n, decision, members=None, boundary="2026-12-31"):
    members = members if members is not None else [f"cik:{i}|2026-10-08" for i in range(n)]
    return {"kind": "look", "n": n, "boundary_exit_session": boundary, "members": list(members),
            "stats": {"n": n, "clustered_t": 0.1, "n_clusters": 50, "median_net_excess": 0.0, "mean_net_excess": 0.0},
            "decision": decision, "terminal": decision in sb.TERMINAL_LABELS}


def test_look_waits_for_a_delayed_earlier_exit_then_decides_once():
    rows = closed_rows(alternating(100, 0.08, 0.01))
    boundary = sb.ordered_closed(rows)[99]["exit_session"]
    blocked = sb.label_state(rows, {}, pending_exit_sessions=[boundary], decide=True)
    assert blocked["label"] == LABEL_UNPROVEN and blocked["new_looks"] == []
    assert blocked["look_pending"] == {"n": 100, "boundary_exit_session": boundary, "blocked_by_positions": 1,
                                       "deferred_filings": 0, "decidable": False}
    later = (date.fromisoformat(boundary) + timedelta(days=1)).isoformat()
    decided = sb.label_state(rows, {}, pending_exit_sessions=[later], decide=True)
    assert decided["label"] == LABEL_SUPPORTED and decided["basis"] == "decided_this_run"
    (rec,) = decided["new_looks"]
    assert rec["kind"] == "look" and rec["n"] == 100 and rec["terminal"] is True
    assert rec["members"] == [r["position_id"] for r in sb.ordered_closed(rows)[:100]]
    assert rec["boundary_exit_session"] == boundary and rec["stats"] == decided["look_stats"]


def test_decided_look_is_immutable_when_an_earlier_exit_arrives_later():
    rows = closed_rows(alternating(100, 0.08, 0.01))
    (rec,) = sb.label_state(rows, {}, decide=True)["new_looks"]
    late = {"net_excess": -0.90, "entry_session": "2026-09-01", "exit_session": "2026-10-01",
            "position_id": "cik:late|2026-09-01"}
    # re-selecting the first 100 would now include the late row
    assert late["position_id"] in {r["position_id"] for r in sb.ordered_closed(rows + [late])[:100]}
    state = sb.label_state(rows + [late], {100: rec}, decide=True)
    assert state["label"] == LABEL_SUPPORTED and state["basis"] == "journal"
    assert state["decided_at_look"] == 100 and state["look_stats"] == rec["stats"]
    assert state["late_arrivals_after_look"] == 1 and state["new_looks"] == []


def test_unsuccessful_look_is_not_rerun_with_changed_membership():
    noise = closed_rows(alternating(100, 0.05, -0.05))
    (rec,) = sb.label_state(noise, {}, decide=True)["new_looks"]
    assert rec["decision"] == LABEL_UNPROVEN and rec["terminal"] is False
    strong = [{"net_excess": 0.10, "entry_session": (date(2026, 8, 1) + timedelta(days=i % 40)).isoformat(),
               "exit_session": (date(2026, 9, 15) + timedelta(days=i % 10)).isoformat(),
               "position_id": f"cik:early{i}|2026-08"} for i in range(60)]
    assert sb.label_state(strong + noise, {}, decide=True)["label"] == LABEL_SUPPORTED  # a re-selection would pass
    state = sb.label_state(strong + noise, {100: rec}, decide=True)
    assert state["label"] == LABEL_UNPROVEN and state["looks_done"] == 1
    assert state["next_look_at_n_closed"] == 200 and state["look_stats"] == rec["stats"]
    assert state["late_arrivals_after_look"] == 60 and state["new_looks"] == []


def test_terminal_looks_read_back_from_journal_and_policy_refusals():
    contrary = sb.label_state([], {100: look_rec(100, LABEL_CONTRARY)}, decide=True)
    assert contrary["label"] == LABEL_CONTRARY and contrary["looks_done"] == 1 and contrary["new_looks"] == []
    three = {100: look_rec(100, LABEL_UNPROVEN), 200: look_rec(200, LABEL_UNPROVEN),
             300: look_rec(300, LABEL_NOT_SUPPORTED)}
    ns = sb.label_state([], three, decide=True)
    assert ns["label"] == LABEL_NOT_SUPPORTED and ns["looks_done"] == 3 and ns["decided_at_look"] == 300
    with pytest.raises(sb.LookPolicyError):
        sb.label_state([], {200: look_rec(200, LABEL_UNPROVEN)})
    with pytest.raises(sb.LookPolicyError):
        sb.label_state([], {100: look_rec(100, LABEL_SUPPORTED), 200: look_rec(200, LABEL_UNPROVEN)})
    with pytest.raises(sb.LookPolicyError):
        sb.label_state([], {100: look_rec(100, LABEL_UNPROVEN, members=["x"] * 99)})


def test_decide_false_and_deferred_filings_never_decide():
    rows = closed_rows(alternating(100, 0.08, 0.01))
    view = sb.label_state(rows, {}, decide=False)
    assert view["label"] == LABEL_UNPROVEN and view["new_looks"] == []
    assert view["look_pending"]["decidable"] is True and view["basis"] == "interim"
    deferred = sb.label_state(rows, {}, pending_filings=1, decide=True)
    assert deferred["label"] == LABEL_UNPROVEN and deferred["new_looks"] == []
    assert deferred["look_pending"]["deferred_filings"] == 1 and deferred["look_pending"]["decidable"] is False


def test_two_looks_decided_in_one_run_and_third_is_terminal():
    noise = alternating(300, 0.05, -0.05)
    first = sb.label_state(closed_rows(noise[:250]), {}, decide=True)
    assert [r["n"] for r in first["new_looks"]] == [100, 200]
    assert all(r["decision"] == LABEL_UNPROVEN and not r["terminal"] for r in first["new_looks"])
    assert first["label"] == LABEL_UNPROVEN and first["looks_done"] == 2 and first["next_look_at_n_closed"] == 300
    journal = {r["n"]: r for r in first["new_looks"]}
    final = sb.label_state(closed_rows(noise), journal, decide=True)
    assert [r["n"] for r in final["new_looks"]] == [300] and final["new_looks"][0]["terminal"] is True
    assert final["label"] == LABEL_NOT_SUPPORTED and final["looks_done"] == 3


def test_build_scoreboard_reads_journal_looks_and_only_decides_when_asked():
    entries = [{"position_id": f"cik:{i}|2026-10-08", "status": "opened", "stratum": "large",
                "cap_bucket": ">=2B", "entry_session": (date(2026, 10, 8) + timedelta(days=i % 50)).isoformat()}
               for i in range(100)]
    exits = [{"position_id": e["position_id"], "horizon": 30, "status": "closed", "net_excess": 0.08 if i % 2 == 0 else 0.01,
              "gross_excess": 0.09, "exit_session": (date(2026, 11, 20) + timedelta(days=i % 50)).isoformat()}
             for i, e in enumerate(entries)]
    quiet = sb.build_scoreboard(entries, exits)
    assert quiet["label"]["label"] == LABEL_UNPROVEN and quiet["new_looks"] == []
    assert quiet["label"]["look_pending"]["decidable"] is True
    decided = sb.build_scoreboard(entries, exits, looks={}, decide=True)
    assert decided["label"]["label"] == LABEL_SUPPORTED and [r["n"] for r in decided["new_looks"]] == [100]
    journal = sb.build_scoreboard(entries, exits, looks={100: look_rec(100, LABEL_CONTRARY)})
    assert journal["label"]["label"] == LABEL_CONTRARY and journal["banner"].startswith("CONTRARY")
