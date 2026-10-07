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
