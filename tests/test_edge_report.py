"""evaluation/edge_report.py: prune_rule_v1 statistics, verdicts, report, CLI.

Pure tests (no database). The SQL is proven against real PostgreSQL in
tests/test_edge_report_pg.py.
"""

from __future__ import annotations

import json
import math
from datetime import date, timedelta

import numpy as np
import pytest

from evaluation import edge_report as er


def _unit(family: str, unit: str, per_date: list[tuple[int, int]], horizon: float = 1.0) -> er.UnitData:
    data = er.UnitData(family=family, unit=unit, horizon_days=horizon)
    start = date(2026, 1, 1)
    for i, (h, n) in enumerate(per_date):
        data.cell(start + timedelta(days=i)).add(er.DateCell(hits=h, n=n))
    return data


# ---------------------------------------------------------------- effective N


def test_effective_n_equals_n_for_iid_balanced_clusters():
    # Every date has exactly the pooled rate: no between-date variance beyond
    # zero -> N_eff is capped at N.
    per_date = [(5, 10)] * 40
    assert er.clustered_effective_n(per_date) == 400


def test_effective_n_collapses_to_dates_when_clusters_are_perfectly_correlated():
    # All-or-nothing dates (every outcome on a date agrees): N_eff ~ D, not N.
    per_date = [(10, 10), (0, 10)] * 20
    n_eff = er.clustered_effective_n(per_date)
    assert 35 <= n_eff <= 41
    assert n_eff < 400


def test_effective_n_degenerate_rates_use_date_count():
    assert er.clustered_effective_n([(3, 3)] * 7) == 7
    assert er.clustered_effective_n([(0, 4)] * 5) == 5
    assert er.clustered_effective_n([]) == 0.0
    assert er.clustered_effective_n([(1, 2)]) == 1.0


# ------------------------------------------------------------ block bootstrap


def test_bootstrap_p_is_deterministic_and_never_zero():
    per_date = [(8, 10)] * 50
    p1 = er.block_bootstrap_p(per_date, baseline=0.5, block=3, seed=7, n_boot=499)
    p2 = er.block_bootstrap_p(per_date, baseline=0.5, block=3, seed=7, n_boot=499)
    assert p1 == p2 == 1 / 500


def test_bootstrap_p_is_one_at_the_baseline():
    assert er.block_bootstrap_p([(5, 10)] * 30, baseline=0.5, block=2, seed=1) == 1.0


def test_bootstrap_size_under_a_null_is_about_nominal():
    # 200 null units (true p = 0.5, iid within and across dates): the share
    # with p < 0.05 should sit near 5%.
    rng = np.random.default_rng(20261106)
    rejections = 0
    trials = 200
    for t in range(trials):
        per_date = [(int(rng.binomial(5, 0.5)), 5) for _ in range(60)]
        if er.block_bootstrap_p(per_date, baseline=0.5, block=2, seed=t, n_boot=499) < 0.05:
            rejections += 1
    assert rejections / trials < 0.12


def test_choose_block_grows_for_autocorrelated_dates():
    # Regime-like runs of good and bad dates: the per-date hit-rate series is
    # strongly autocorrelated, so the block must exceed the overlap floor.
    per_date = [(9, 10)] * 15 + [(1, 10)] * 15 + [(9, 10)] * 15 + [(1, 10)] * 15
    assert er.choose_block(per_date, horizon_days=1.0) > 1


def test_choose_block_respects_overlap_floor_and_cap():
    per_date = [(5, 10), (6, 10)] * 100  # 200 dates, little autocorrelation
    assert er.choose_block(per_date, horizon_days=30) == 30
    short = [(5, 10), (6, 10)] * 20  # 40 dates -> cap 10
    assert er.choose_block(short, horizon_days=30) == 10


# -------------------------------------------------------------------- verdict


@pytest.mark.parametrize(
    "n_eff,dates,ci,bh,expected",
    [
        (29.9, 100, (0.6, 0.7), 0.001, "INSUFFICIENT"),
        (100, 19, (0.6, 0.7), 0.001, "INSUFFICIENT"),
        (100, 50, (0.51, 0.7), 0.01, "EDGE"),
        (100, 50, (0.51, 0.7), 0.06, "NO_EDGE"),
        (100, 50, (0.45, 0.7), 0.001, "NO_EDGE"),
        (100, 50, (0.2, 0.49), 0.01, "ANTI_EDGE"),
        (100, 50, (0.2, 0.49), 0.2, "NO_EDGE"),
        (100, 50, None, 0.01, "INSUFFICIENT"),
    ],
)
def test_classify_matches_preregistration(n_eff, dates, ci, bh, expected):
    assert er.classify(n_eff, dates, ci, bh) == expected


def test_evaluate_family_bh_includes_insufficient_units_as_p_one():
    strong = _unit("signal_source_type", "good", [(16, 20)] * 60)
    null = _unit("signal_source_type", "coin", [(10, 20), (11, 20), (9, 20)] * 20)
    tiny = _unit("signal_source_type", "tiny", [(3, 4)] * 3)
    anti = _unit("signal_source_type", "bad", [(4, 20)] * 60)
    stats = {s.unit: s for s in er.evaluate_family([strong, null, tiny, anti], n_boot=499)}
    assert stats["good"].verdict == "EDGE"
    assert stats["coin"].verdict == "NO_EDGE"
    assert stats["tiny"].verdict == "INSUFFICIENT" and stats["tiny"].bh_p == 1.0
    assert stats["bad"].verdict == "ANTI_EDGE"
    # BH over all four (tiny counted): adjusted >= raw.
    assert all(s.bh_p >= s.boot_p - 1e-12 for s in stats.values())


def test_clustered_ci_is_wider_than_naive_for_correlated_dates():
    unit = _unit("oracle_model", "m", [(20, 20), (0, 20)] * 20 + [(20, 20)] * 5)
    s = er.unit_stats(unit, n_boot=99)
    naive_w = s.wilson_naive[1] - s.wilson_naive[0]
    clustered_w = s.wilson_clustered[1] - s.wilson_clustered[0]
    assert clustered_w > 2 * naive_w


def test_brier_and_climatology_from_cells():
    unit = er.UnitData(family="oracle_model", unit="m")
    unit.cell(date(2026, 1, 1)).add(er.DateCell(hits=1, n=2, prob_n=2, prob_sq=0.25 + 0.36, prob_y=1))
    s = er.unit_stats(unit, n_boot=9)
    assert s.brier == pytest.approx(0.305)
    assert s.brier_climatology == pytest.approx(0.25)


def test_exploratory_stats_never_get_a_verdict():
    unit = _unit("oracle_model", "m [rescore:hindsight]", [(16, 20)] * 60)
    (s,) = er.exploratory_stats([unit])
    assert s.verdict == "EXPLORATORY" and math.isnan(s.boot_p)


# ------------------------------------------------------- row -> family builders


def test_build_signal_families_splits_forward_and_feeds():
    rows = [
        ("congressional", "Pelosi", date(2026, 1, 2), True, 3, 5, 1, 4, 0),
        ("congressional", "Pelosi", date(2026, 1, 2), False, 1, 1, 0, 1, 0),
        ("congressional", "Tuberville", date(2026, 1, 3), True, 0, 2, 2, 2, 2),
        ("darkpool", "qq_offexchange", date(2026, 1, 3), True, 1, 2, 0, 1, 0),
    ]
    by_type, by_feed = er.build_signal_families(rows)
    cong = by_type.forward["congressional"]
    assert cong.horizon_days == 30.0
    assert sum(c.n for c in cong.cells.values()) == 7
    assert sum(c.no_move for c in cong.cells.values()) == 3
    assert "congressional [not forward]" in by_type.exploratory
    feed = by_feed.forward[er.feed_unit_key("congressional", "Pelosi")]
    assert feed.meta == {"source_type": "congressional", "source_id": "Pelosi"}


def test_build_oracle_family_routes_rescores_to_exploratory():
    rows = [("m1", date(2026, 1, 1), True, 6, 10, 2, 7, 10, 2.5, 6, 5.0),
            ("m1", date(2026, 1, 2), False, 1, 1, 1, 1, 0, 0.0, 0, 30.0)]
    rescore = [("m1", date(2025, 12, 1), 3, 4, 1, 2, 5.0)]
    fam = er.build_oracle_family(rows, rescore)
    assert set(fam.forward) == {"m1"}
    assert fam.forward["m1"].horizon_days == pytest.approx(5.0)
    assert set(fam.exploratory) == {"m1 [not forward]", "m1 [rescore:hindsight]"}
    assert fam.exploratory["m1 [not forward]"].meta["reason"] == "recorded_or_scored_after_expiry_close"


def test_build_astrogrid_family_routes_legacy_rescores_to_exploratory():
    rows = [("grid-crypto-v1", date(2026, 11, 6), True, 2, 5, 1, 3, 7)]
    rescore = [("grid-crypto-v1", date(2026, 10, 1), 1, 4, 0, 4, 7)]
    fam = er.build_astrogrid_family(rows, rescore)
    assert set(fam.forward) == {"grid-crypto-v1"}
    assert set(fam.exploratory) == {"grid-crypto-v1 [legacy_rescore]"}


def test_astrogrid_legacy_predicate_matches_store():
    from store.astrogrid import _legacy_spy_score_predicate

    assert er.ASTROGRID_LEGACY_SPY_PREDICATE == _legacy_spy_score_predicate("ps", "pr")


def test_astrogrid_schema_name_is_validated():
    assert "astrogrid.prediction_score" in er._ag_sql(er._ASTROGRID_CELLS_SQL, "astrogrid")
    with pytest.raises(ValueError):
        er._ag_sql(er._ASTROGRID_CELLS_SQL, "astrogrid; DROP TABLE x")


# ---------------------------------------------------------- report + candidate


def _loaded() -> dict:
    sig = er.FamilyColumns()
    sig.forward["good"] = _unit("signal_source_type", "good", [(16, 20)] * 60, 7)
    sig.forward["coin"] = _unit("signal_source_type", "coin", [(10, 20), (11, 20), (9, 20)] * 20, 7)
    feed = er.FamilyColumns()
    f = _unit("signal_feed", er.feed_unit_key("coin", "x"), [(10, 20), (11, 20), (9, 20)] * 20, 7)
    f.meta.update(source_type="coin", source_id="x")
    feed.forward[f.unit] = f
    oracle = er.FamilyColumns()
    oracle.forward["m1"] = _unit("oracle_model", "m1", [(4, 20)] * 60, 5)
    oracle.exploratory["m1 [rescore:hindsight]"] = _unit("oracle_model", "m1 [rescore:hindsight]", [(9, 10)] * 5)
    return {"signal_source_type": sig, "signal_feed": feed, "oracle_model": oracle,
            "astrogrid_model": er.FamilyColumns(), "context": {"signal_outcome_mix": {"CORRECT": 5}}}


def test_build_report_and_candidate_prune_list():
    report = er.build_report(_loaded(), n_boot=499)
    fam = report["families"]
    assert fam["signal_source_type"]["verdict_counts"]["EDGE"] == 1
    assert fam["oracle_model"]["forward"][0].verdict == "ANTI_EDGE"
    cand = er.candidate_prune_list(report)
    assert cand["rule_version"] == "prune_rule_v1"
    assert cand["status"].startswith("candidate")
    by_family = {(u["family"], u["unit"]): u for u in cand["units"]}
    assert ("signal_source_type", "coin") in by_family
    assert by_family[("signal_source_type", "coin")]["source_type"] == "coin"
    assert by_family[("signal_feed", "coin / x")]["source_id"] == "x"
    assert ("oracle_model", "m1") in by_family
    assert ("signal_source_type", "good") not in by_family  # EDGE is never pruned
    assert not any("rescore" in u["unit"] for u in cand["units"])  # exploratory never pruned

    # The candidate parses as a prune list the trust scorer accepts.
    from intelligence.trust_scorer import parse_prune_list

    prune = parse_prune_list(json.loads(json.dumps(cand)))
    assert prune.is_source_pruned("coin") and not prune.is_source_pruned("good")
    assert prune.oracle_models == frozenset({"m1"})


def test_render_markdown_and_json():
    report = er.build_report(_loaded(), n_boot=99)
    md = er.render_markdown(report, title_date="2026-11-06")
    assert md.startswith("# Forward edge report — 2026-11-06")
    assert "prune_rule_v1" in md and "Exploratory (not used by the rule)" in md
    assert "`m1 [rescore:hindsight]`" in md
    payload = json.loads(er.report_json(report))
    assert payload["families"]["oracle_model"]["exploratory"][0]["boot_p"] is None


# ----------------------------------------------------------------- trust diff


def test_trust_diff_reports_distribution_and_movers():
    legacy = {"outcome_basis": "legacy", "sources": [
        {"source_type": "insider", "source_id": "a", "trust_score": 0.1, "hit_count": 1, "miss_count": 9},
        {"source_type": "insider", "source_id": "b", "trust_score": 0.5, "hit_count": 5, "miss_count": 5},
        {"source_type": "social", "source_id": "gone", "trust_score": 0.2, "hit_count": 0, "miss_count": 4},
    ]}
    corrected = {"outcome_basis": "corrected", "sources": [
        {"source_type": "insider", "source_id": "a", "trust_score": 0.55, "hit_count": 1, "miss_count": 1,
         "no_move_count": 8, "ineligible_count": 0},
        {"source_type": "insider", "source_id": "b", "trust_score": 0.45, "hit_count": 4, "miss_count": 5,
         "no_move_count": 1, "ineligible_count": 0},
        {"source_type": "darkpool", "source_id": "new", "trust_score": 0.5, "hit_count": 0, "miss_count": 0,
         "no_move_count": 0, "ineligible_count": 3},
    ]}
    diff = er.trust_diff(legacy, corrected, top=5)
    assert diff["sources_in_both"] == 2 and diff["only_legacy"] == 1 and diff["only_corrected"] == 1
    assert diff["top_up"][0]["source_id"] == "a" and diff["top_up"][0]["delta"] == pytest.approx(0.45)
    assert diff["top_down"][0]["source_id"] == "b"
    assert diff["by_source_type"]["insider"]["delta_mean"] == pytest.approx(0.2)
    assert diff["legacy"]["n"] == 3 and diff["corrected"]["share_ge_0_6"] == 0.0


# ------------------------------------------------------------------------ CLI


def test_cli_engine_is_read_only_and_time_bounded(monkeypatch):
    import importlib

    cli = importlib.import_module("scripts.edge_report")
    captured = {}

    def _fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(cli, "create_engine", _fake_create_engine)
    cli.read_only_engine("postgresql://u@h/db", 120)
    opts = captured["connect_args"]["options"]
    assert "default_transaction_read_only=on" in opts
    assert "statement_timeout=120000" in opts


def test_cli_parser_subcommands():
    import importlib

    cli = importlib.import_module("scripts.edge_report")
    args = cli.build_arg_parser().parse_args(["report", "--candidate-out", "/tmp/x.json"])
    assert args.command == "report" and str(args.candidate_out) == "/tmp/x.json"
    args = cli.build_arg_parser().parse_args(["trust-diff", "--top", "5"])
    assert args.command == "trust-diff" and args.top == 5
