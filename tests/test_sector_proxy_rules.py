"""GD7a: declared PROXY/SELF_LAG rules for sector-relative targets.

Synthetic data and in-memory ledgers only; no DB, no prices.

* every PROXY_TABLES member of every declared REL/SECTOR target is a forced
  SELF_LAG in the S11 catalog, and no allocatable arm contains one (4.1);
* a channel family is one vote: QuiverQuant/EDGAR and A/S variants share one
  feature class, so the allocator counts one arm (4.2);
* a ``contains_price`` construct is refused unless the same run tested the
  momentum family and it beats it; in a world where only momentum is real the
  flywheel-like feature is refused (4.3);
* a target with no declared group is refused (mirrors ``proxy_group``).
"""

from datetime import date

import numpy as np
import pytest
import yaml

import analysis.ledger_steered_exploration as lse
import analysis.sector_proxy_rules as spr
from analysis.offline_research_proof import block_permutation_pvalue

FIXED = "2026-10-01T12:00:00+00:00"
MEMBERS = {
    "XOM": "Energy",
    "CVX": "Energy",
    "JPM": "Financials",
    "AAPL": "Technology",
    "PLD": "Real Estate",
    "BRK-B": "Financials",
}


def window(k, days=400):
    import pandas as pd

    start = pd.Timestamp("2001-01-01", tz="UTC") + pd.Timedelta(days=k * days)
    return {
        "start": start.isoformat(),
        "split": (start + pd.Timedelta(days=days // 2)).isoformat(),
        "end": (start + pd.Timedelta(days=days - 1)).isoformat(),
    }


def expected_proxies(target: str) -> set[str]:
    """Hand-written expectation (independent of the rule code) per target."""
    if target.startswith("SECTOR:"):
        sector = target.split(":", 1)[1]
        etf = spr.SECTOR_BENCHMARKS[sector]
        constituents = [t for t, s in MEMBERS.items() if s == sector]
        own = [etf, *constituents]
    else:
        left, right = target[len("REL:") :].rsplit("-", 1)
        if right == "SPY":
            sector = spr.ETF_SECTOR[left]
            own = [left, *[t for t, s in MEMBERS.items() if s == sector]]
        else:
            sector = spr.ETF_SECTOR[right]
            own = [left, right, *[t for t, s in MEMBERS.items() if s == sector and t != left]]
    out = set()
    for t in [*own, "SPY"]:
        out |= {
            f"MOM:{t}|chg20",
            f"RET:{t}|chg5",
            f"TIINGO:{t}:adj_close|chg20",
            f"OPTIONS:{t}:max_pain|z60",
            f"OPTIONS:{t}:spot_price|z60",
            f"OPTIONS:{t}:gamma_flip|z60",
            f"FUNDAMENTAL_DIVERGENCE:{t}:price_score|z60",
            f"FUNDAMENTAL_DIVERGENCE:{t}:value_score|z60",
            f"FUNDAMENTAL_DIVERGENCE:{t}:divergence|z60",
            f"TICKER_METRICS_DAILY:{t}:market_cap_usd|z60",
            f"TICKER_METRICS_DAILY:{t}:close_price|chg20",
        }
    # entity-own (no subject), any ETF's flows, sector health: always
    out |= {"MOM|k20", "PX|k5"}
    out |= {f"ETF_FLOWS:{e}|chg5" for e in spr.ETF_SECTOR} | {"ETF_FLOWS:SPY|chg5"}
    out |= {"SECTOR_HEALTH_SNAPSHOTS:Energy|z60", "SECTOR_HEALTH_SNAPSHOTS|z60"}
    # every REL:{ETF}-SPY feature shares the SPY leg (research_real_panel rule (b))
    out |= {f"REL:{e}-SPY|chg20" for e in spr.ETF_SECTOR}
    return out


def universe() -> list[str]:
    tickers = [*spr.ETF_SECTOR, "SPY", *MEMBERS]
    feats = []
    for t in tickers:
        feats += [
            f"MOM:{t}|chg20",
            f"RET:{t}|chg5",
            f"TIINGO:{t}:adj_close|chg20",
            f"OPTIONS:{t}:max_pain|z60",
            f"OPTIONS:{t}:spot_price|z60",
            f"OPTIONS:{t}:put_call_ratio|z60",
            f"OPTIONS:{t}:gamma_flip|z60",
            f"FUNDAMENTAL_DIVERGENCE:{t}:price_score|z60",
            f"FUNDAMENTAL_DIVERGENCE:{t}:value_score|z60",
            f"FUNDAMENTAL_DIVERGENCE:{t}:divergence|z60",
            f"TICKER_METRICS_DAILY:{t}:market_cap_usd|z60",
            f"TICKER_METRICS_DAILY:{t}:close_price|chg20",
            f"ETF_FLOWS:{t}|chg5" if t in spr.ETF_SECTOR or t == "SPY" else None,
        ]
    feats += [f"REL:{e}-SPY|chg20" for e in spr.ETF_SECTOR]
    feats += ["MOM|k20", "PX|k5", "SECTOR_HEALTH_SNAPSHOTS:Energy|z60", "SECTOR_HEALTH_SNAPSHOTS|z60"]
    # features under test: people densities (never proxies)
    feats += [
        "A_insider_buy@quiverquant|W30",
        "A_insider_buy@edgar|W30",
        "S_insider|W30",
        "A_congress|W90",
        "D_peer_congress|W90",
        "A_contract|W90",
        "A_lobby|W90",
        "A_multi|W90",
        "SECTOR_DENSITY:Energy:A_insider_buy:W30|chg4",
    ]
    return [f for f in feats if f]


def declared_targets() -> list[str]:
    out = [f"REL:{etf}-SPY" for etf in spr.ETF_SECTOR]
    out += [f"SECTOR:{s}" for s in spr.SECTOR_BENCHMARKS]
    out += ["REL:XOM-XLE", "REL:JPM-XLF", "REL:BRK-B-XLF", "REL:PLD-XLRE"]
    return out


def families_for(targets, label="return"):
    out = []
    for t in targets:
        lab = "rel_ret" if t.startswith("SECTOR:") else label
        out += [f"{t}|{lab}|fwd5", f"{t}|{lab}|fwd20"]
    return out


# --- pins ---------------------------------------------------------------------------


def test_benchmarks_pinned_to_vs1_and_sector_map():
    import analysis.panel_insider_density as v1
    import analysis.panel_insider_density_sectors_v2 as s2
    import analysis.panel_insider_density_sectors_v4 as s4

    assert spr.SECTOR_BENCHMARKS == v1.EQUITY_SECTORS
    assert spr.LATE_ETF_START == s2.LATE_ETF_START
    assert spr.LATE_ETF_START == s4.LATE_ETF_START
    with open(spr.__file__.replace("sector_proxy_rules.py", "sector_map_data.yaml"), encoding="utf-8") as fh:
        sector_map = yaml.safe_load(fh)["SECTOR_MAP"]
    for sector, etf in spr.SECTOR_BENCHMARKS.items():
        assert sector_map[sector]["etf"] == etf
    assert len(spr.NON_TECH_SECTORS) == 10 and spr.TECHNOLOGY not in spr.NON_TECH_SECTORS


def test_pre_inception_rule():
    assert spr.pre_inception("XLRE", date(2015, 9, 30))
    assert not spr.pre_inception("XLRE", date(2015, 10, 1))
    assert spr.pre_inception("XLC", date(2018, 5, 31))
    assert not spr.pre_inception("XLC", date(2018, 6, 18))
    assert not spr.pre_inception("XLE", date(1999, 1, 4))
    assert "equal-weighted" in spr.PRE_INCEPTION_RULE


def test_rules_manifest_is_versioned_and_hashed(monkeypatch):
    first = spr.rules_sha256()
    assert first == spr.rules_sha256() and len(first) == 64
    assert spr.rules_manifest()["version"] == spr.RULES_VERSION
    assert all(rule.reason for rule in spr.PROXY_TABLES)
    monkeypatch.setattr(spr, "MOMENTUM_GATE_RULE", spr.MOMENTUM_GATE_RULE + " (edited)")
    assert spr.rules_sha256() != first


# --- 4.1: every PROXY member is a forced SELF_LAG -----------------------------------


@pytest.mark.parametrize("target", declared_targets())
def test_every_proxy_member_is_self_lag(target):
    feats = universe()
    expected = expected_proxies(target)
    assert expected <= set(feats), sorted(expected - set(feats))
    fams = families_for([target])
    catalog = spr.rel_catalog(fams, feats, members=MEMBERS)
    for family in fams:
        got = {f for f in feats if catalog.feature_class(family, f) == lse.SELF_LAG_CLASS}
        assert got == expected, (sorted(got - expected), sorted(expected - got))
        for f in got:
            assert spr.self_lag_reasons([family], [f], members=MEMBERS)[(family, f)]["reason"]
    for arm in catalog.arms().values():
        if arm["feature_class"] == lse.SELF_LAG_CLASS:
            assert "never" in arm
            continue
        assert not set(arm["pool"]) & expected_proxies(spr.family_target(arm["family"]))


def test_no_allocated_trial_is_a_proxy():
    feats = universe()
    targets = ["REL:XLE-SPY", "REL:XLF-SPY", "SECTOR:Energy", "REL:XOM-XLE"]
    fams = families_for(targets)
    catalog = spr.rel_catalog(fams, feats, members=MEMBERS)
    ledger = lse.Ledger.create(None, catalog=catalog, ledger_id="gd7a-test", q=0.10, recorded_at=FIXED)
    alloc = lse.allocate(ledger, catalog, lse.Policy(budget=2000), run_id="r1", windows=window(1), recorded_at=FIXED)
    trials = alloc["record"]["trials"]
    assert trials
    for trial in trials:
        target = spr.family_target(trial["family"])
        assert trial["feature"] not in expected_proxies(target)
        assert not trial["family_key"].startswith(lse.SELF_LAG_CLASS)
    for arm in alloc["record"]["arms"].values():
        if arm["feature_class"] == lse.SELF_LAG_CLASS:
            assert arm["eligible"] is False and "self_lag" in arm["ineligible"]


def test_people_densities_are_never_proxies():
    feats = universe()
    people = [f for f in feats if spr.people_channel(f) is not None]
    assert len(people) == 9
    for target in declared_targets():
        assert not set(spr.rel_self_lag_pairs(families_for([target]), people, members=MEMBERS))


def test_unmapped_ticker_is_a_possible_constituent():
    assert spr.proxy_rule("REL:XLE-SPY", "MOM:ZZZZ|chg20", MEMBERS).rule_id == "own_price"
    # mapped to another sector: not a proxy, but it carries price
    assert spr.proxy_rule("REL:XLE-SPY", "MOM:JPM|chg20", MEMBERS) is None
    assert spr.contains_price("MOM:JPM|chg20")
    # another sector's benchmark is not own
    assert spr.proxy_rule("REL:XLE-SPY", "MOM:XLK|chg20", MEMBERS) is None


def test_etf_flows_and_sector_health_are_proxy_only_everywhere():
    for target in declared_targets():
        assert spr.proxy_rule(target, "ETF_FLOWS:XLK|chg5", MEMBERS).rule_id == "etf_flows"
        assert spr.proxy_rule(target, "SECTOR_HEALTH_SNAPSHOTS:Utilities|z60", MEMBERS).rule_id == (
            "sector_health_snapshots"
        )


# --- refusals -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "target",
    [
        "REL:XYZ-SPY",
        "REL:XLE-QQQ",
        "REL:XLE-XLK",
        "REL:SPY-XLE",
        "SECTOR:Crypto",
        "SECTOR:energy",
        "VIXCLS",
        "REL:XLE",
        "REL:AAPL-SPY",
    ],
)
def test_target_without_declared_group_is_refused(target):
    with pytest.raises(ValueError, match="no declared proxy group"):
        spr.rel_proxy_group(target, universe(), members=MEMBERS)
    with pytest.raises(ValueError, match="no declared proxy group"):
        spr.rel_self_lag_pairs([f"{target}|return|fwd5"], ["MOM:XLE|chg20"], members=MEMBERS)


def test_issuer_target_needs_a_matching_sector():
    with pytest.raises(ValueError, match="no sector in the declared members map"):
        spr.parse_target("REL:ZZZZ-XLE", MEMBERS)
    with pytest.raises(ValueError, match="mapped to 'Financials'"):
        spr.parse_target("REL:JPM-XLE", MEMBERS)
    assert spr.parse_target("REL:BRK-B-XLF", MEMBERS).issuer == "BRK-B"


def test_declared_group_lacking_a_rule_member_is_refused():
    feats = universe()
    required = spr.required_rel_proxies("REL:XLE-SPY", feats, MEMBERS)
    assert spr.rel_proxy_group("REL:XLE-SPY", feats, members=MEMBERS, declared=required) == required
    short = set(required) - {"MOM:SPY"}
    with pytest.raises(ValueError, match="lacks rule members"):
        spr.rel_proxy_group("REL:XLE-SPY", feats, members=MEMBERS, declared=short)


@pytest.mark.parametrize(
    "feature",
    ["signal_data:XOM|z60", "insider_trades:XOM|z60", "sector_density:Energy|z60", "A_insider_is_cluster_buy|W30"],
)
def test_r6_inputs_are_refused(feature):
    with pytest.raises(ValueError, match="R6"):
        spr.vote_class(feature)
    with pytest.raises(ValueError, match="R6"):
        spr.rel_catalog(["REL:XLE-SPY|return|fwd5"], [feature, "A_congress|W90"], members=MEMBERS)


def test_unclassifiable_feature_is_refused_unless_named():
    with pytest.raises(ValueError, match="no declared feature class"):
        spr.vote_class("FW_A|W30")
    with pytest.raises(ValueError, match="several channels"):
        spr.vote_class("A_insider_congress|W30")
    with pytest.raises(ValueError, match="unknown source variant"):
        spr.vote_class("A_insider_buy@reddit|W30")
    with pytest.raises(ValueError, match="SECTOR_DENSITY"):
        spr.vote_class("SECTOR_DENSITY:Crypto:A_insider_buy:W30")
    cat = spr.rel_catalog(
        ["REL:XLE-SPY|return|fwd5"], ["FW_A|W30", "A_congress|W90"], classes={"FW_A|W30": "flywheel"}
    )
    assert cat.feature_class("REL:XLE-SPY|return|fwd5", "FW_A|W30") == "flywheel"


# --- 4.2: one vote per channel ------------------------------------------------------


def test_channel_variants_share_one_vote_class():
    insider = ["A_insider_buy@quiverquant|W30", "A_insider_buy@edgar|W30", "S_insider|W30",
               "D_peer_insider_buy|W90", "D_self_form4|W90", "SECTOR_DENSITY:Energy:A_insider_buy:W30|chg4"]
    assert {spr.vote_class(f) for f in insider} == {"people_density_insider"}
    assert spr.vote_class("A_congress|W90") == spr.vote_class("D_peer_congress|W90") == "people_density_congress"
    assert spr.vote_class("A_thirteen_f|W90") == "people_density_inst"
    assert spr.vote_class("A_gov_contract|W90") == "people_density_contract"
    assert spr.vote_class("A_multi|W90") == spr.vote_class("C_form4_congress|W90") == "people_density_multi"


def test_allocator_counts_one_arm_per_channel():
    feats = ["A_insider_buy@quiverquant|W30", "A_insider_buy@edgar|W30", "S_insider|W30", "A_congress|W90"]
    fams = ["SECTOR:Energy|rel_ret|fwd20", "SECTOR:Utilities|rel_ret|fwd20"]
    catalog = spr.rel_catalog(fams, feats, members=MEMBERS)
    arms = catalog.arms()
    insider_keys = [k for k in arms if k.startswith("people_density_insider::")]
    assert sorted(insider_keys) == [f"people_density_insider::{f}" for f in sorted(fams)]
    for key in insider_keys:
        assert sorted(arms[key]["pool"]) == sorted(feats[:3])
    ledger = lse.Ledger.create(None, catalog=catalog, ledger_id="gd7a-vote", q=0.10, recorded_at=FIXED)
    alloc = lse.allocate(ledger, catalog, lse.Policy(budget=8), run_id="r1", windows=window(1), recorded_at=FIXED)
    counted = [k for k, a in alloc["record"]["arms"].items() if a["eligible"]]
    assert len(counted) == 4  # (insider, congress) x 2 families, not (3 + 1) x 2
    # a people channel cannot be re-classed into a second arm
    with pytest.raises(ValueError, match="one vote"):
        spr.rel_catalog(fams, feats, classes={"S_insider|W30": "people_density_insider_sell"})


# --- 4.3: contains_price must beat momentum in the same run ------------------------

RUN = "run-1"
FAMILY = "REL:XLE-SPY|return|fwd5"
MOMENTUM = "MOM:XLE-SPY|chg20"
FLY = "FW_A|W30"


def _stats(features: dict, y, momentum_names, perms=999, seed=7, run_id=RUN):
    """What a run's harness records per trial: run id, p, statistic, incremental p."""
    mom = np.column_stack([features[m] for m in momentum_names]) if momentum_names else None
    out = {}
    for name, x in features.items():
        r, p = block_permutation_pvalue(x, y, 1, perms, seed)
        entry = {"run_id": run_id, "statistic": r, "p": p}
        if mom is not None and name not in momentum_names:
            entry["incremental_p"] = spr.incremental_pvalue(x, mom, y, block=1, perms=perms, seed=seed)[1]
            entry["incremental_on"] = sorted(momentum_names)
        out[name] = entry
    return out


def test_flywheel_refused_where_only_momentum_is_real():
    rng = np.random.default_rng(20261001)
    n = 600
    m = rng.standard_normal(n)
    y = 0.35 * m + rng.standard_normal(n)
    flywheel = m + 0.8 * rng.standard_normal(n)  # a PX-stage construct: a noisy copy of momentum
    noise = rng.standard_normal(n)
    stats = _stats({MOMENTUM: m, FLY: flywheel, "A_insider_buy@edgar|W30": noise}, y, [MOMENTUM])
    assert spr.proxy_rule("REL:XLE-SPY", MOMENTUM) is not None  # momentum itself is SELF_LAG
    assert spr.momentum_features(FAMILY, stats) == (MOMENTUM,)
    assert stats[FLY]["p"] < 0.01  # without the gate it would be "discovered"
    gate = spr.momentum_gate(FAMILY, stats, run_id=RUN)
    assert set(gate) == {FLY}  # fail-closed contains_price, no flag needed
    assert gate[FLY] and gate[FLY].startswith("refused: contains_price does not beat momentum")
    kept, refused = spr.gate_selections(FAMILY, [FLY, MOMENTUM, "A_insider_buy@edgar|W30"], stats, run_id=RUN)
    assert kept == ("A_insider_buy@edgar|W30",)
    assert set(refused) == {FLY, MOMENTUM}
    assert refused[MOMENTUM].startswith("refused: SELF_LAG")


def test_near_copies_of_momentum_are_refused_across_seeds():
    """A near-copy (m + 0.1 noise) can edge past momentum on raw strength; the incremental test stops it."""
    admitted_raw = admitted = 0
    for seed in range(40):
        rng = np.random.default_rng([7, seed])
        n = 400
        m = rng.standard_normal(n)
        y = 0.35 * m + rng.standard_normal(n)
        near = m + 0.1 * rng.standard_normal(n)
        stats = _stats({MOMENTUM: m, FLY: near}, y, [MOMENTUM], perms=499)
        s, mo = stats[FLY], stats[MOMENTUM]
        admitted_raw += s["p"] <= mo["p"] and abs(s["statistic"]) > abs(mo["statistic"])
        admitted += spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY] is None
    assert admitted_raw >= 4  # the raw-strength rule alone is weak
    assert admitted <= 2  # ~ raw rate x 5%


def test_momentum_family_includes_relative_and_close_series():
    rng = np.random.default_rng(5)
    n = 600
    rel = rng.standard_normal(n)
    y = 0.4 * rel + rng.standard_normal(n)
    weak_mom = rng.standard_normal(n)
    fly = 0.4 * rel + rng.standard_normal(n)  # weaker than the target's own relative momentum
    names = ["REL:XLE-SPY|chg20", "MOM:XLE|chg20", "TIINGO:XLE:adj_close|chg20", "MOM:SPY|chg5"]
    feats = {names[0]: rel, names[1]: weak_mom, names[2]: weak_mom + rng.standard_normal(n),
             names[3]: rng.standard_normal(n), FLY: fly}
    stats = _stats(feats, y, names)
    assert set(spr.momentum_features(FAMILY, stats)) == set(names)
    assert stats[FLY]["p"] < 0.01 and stats[FLY]["p"] <= stats["MOM:XLE|chg20"]["p"]
    assert spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY].startswith("refused: contains_price does not beat")


def test_flywheel_refused_when_momentum_not_tested_or_untestable_or_other_run():
    rng = np.random.default_rng(3)
    n = 400
    g = rng.standard_normal(n)
    y = 0.5 * g + rng.standard_normal(n)
    stats = _stats({FLY: g}, y, [])
    stats[FLY]["incremental_p"] = 0.001
    assert spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY] == (
        "refused: contains_price and the run did not test the momentum family"
    )
    stats[MOMENTUM] = {"run_id": RUN, "statistic": float("nan"), "p": 1.0}
    assert spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY] == (
        "refused: contains_price and the momentum family was untestable"
    )
    # another sector's momentum is contains_price, not momentum for this family
    stats = _stats({FLY: g, "MOM:XLF|chg20": g + rng.standard_normal(n)}, y, [])
    gate = spr.momentum_gate(FAMILY, stats, run_id=RUN)
    assert gate[FLY].endswith("did not test the momentum family")
    assert gate["MOM:XLF|chg20"].endswith("did not test the momentum family")
    # momentum from another run does not count
    m = rng.standard_normal(n)
    stats = _stats({MOMENTUM: m, FLY: g}, y, [MOMENTUM])
    stats[MOMENTUM]["run_id"] = "run-0"
    assert spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY].startswith("refused: stats are not all from run")
    # a missing incremental p is refused
    stats = _stats({MOMENTUM: m, FLY: g}, y, [MOMENTUM])
    del stats[FLY]["incremental_p"]
    assert "incremental_p" in spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY]
    # an incremental p residualized on a different momentum set is refused
    stats = _stats({MOMENTUM: m, FLY: g, "MOM:XLE|chg5": m + rng.standard_normal(n)}, y, [MOMENTUM])
    assert "not residualized on this run's momentum family" in spr.momentum_gate(FAMILY, stats, run_id=RUN)[FLY]


def test_flywheel_with_incremental_signal_is_admitted():
    rng = np.random.default_rng(11)
    n = 600
    m = rng.standard_normal(n)
    g = rng.standard_normal(n)
    y = 0.1 * m + 0.5 * g + rng.standard_normal(n)
    flywheel = g + 0.3 * m
    stats = _stats({MOMENTUM: m, FLY: flywheel}, y, [MOMENTUM])
    assert spr.momentum_gate(FAMILY, stats, run_id=RUN) == {FLY: None}
    kept, refused = spr.gate_selections(FAMILY, [FLY], stats, run_id=RUN)
    assert kept == (FLY,) and not refused


def test_contains_price_is_fail_closed():
    stats = {MOMENTUM: {"run_id": RUN, "statistic": 0.5, "p": 0.001},
             "A_congress|W90": {"run_id": RUN, "statistic": 0.01, "p": 0.6}}
    assert spr.momentum_gate(FAMILY, stats, run_id=RUN) == {}
    assert not spr.contains_price("A_congress|W90")
    assert not spr.contains_price("SECTOR_DENSITY:Energy:A_insider_buy:W30|chg4")
    for f in ["OPTIONS:XLF:delta|z60", "TICKER_METRICS_DAILY:JPM:employees|z60", "FW_A|W30", "XYZ|k5"]:
        assert spr.contains_price(f), f
    assert not spr.contains_price("FW_B|W30", nonprice={"FW_B"})


# --- review regressions: spellings, whole price tables, class overrides ---------------


@pytest.mark.parametrize(
    "feature,rule",
    [
        ("TICKER_METRICS_DAILY:XLE:close_price|chg20", "ticker_metrics_daily"),
        ("TICKER_METRICS_DAILY:XLE|z60", "ticker_metrics_daily"),
        ("FUNDAMENTAL_DIVERGENCE:XOM:divergence|z60", "fundamental_divergence"),
        ("FUNDAMENTAL_DIVERGENCE:XOM:classification|z60", "fundamental_divergence"),
        ("fundamental_divergence:xom:PRICE_SCORE|z60", "fundamental_divergence"),
        ("TICKER_METRICS_DAILY:XOM:market_cap|z60", "ticker_metrics_daily"),
        ("OPTIONS:XLE:MAX_PAIN|z60", "options_spot"),
        ("OPTIONS:XLE:gamma_flip|z60", "options_spot"),
        ("OPTIONS:SPY:call_wall|z60", "options_spot"),
        ("mom:XLE|chg20", "own_price"),
        ("Mom:xle|chg20", "own_price"),
        ("rel:xle-spy|chg20", "own_price"),
        ("tiingo:spy:adj_close|chg20", "market_price"),
        ("etf_flows:XLK|chg5", "etf_flows"),
        ("Sector_Health_Snapshots|z60", "sector_health_snapshots"),
    ],
)
def test_spellings_and_price_tables_are_proxies(feature, rule):
    assert spr.proxy_rule("REL:XLE-SPY", feature, MEMBERS).rule_id == rule
    cat = spr.rel_catalog(["REL:XLE-SPY|return|fwd5"], [feature, "A_congress|W90"], members=MEMBERS)
    assert cat.feature_class("REL:XLE-SPY|return|fwd5", feature) == lse.SELF_LAG_CLASS


@pytest.mark.parametrize(
    "feature",
    ["Insider_Trades:XOM|z60", "SIGNAL_DATA:XOM|z60", "Sector_Density:Energy|z60",
     "sector_density:Energy:A_insider_buy:W30|chg4", "YF:XLE:close|chg20", "yf_adj:XLE|chg20"],
)
def test_refused_inputs_in_any_case(feature):
    with pytest.raises(ValueError, match="refused"):
        spr.rel_catalog(["REL:XLE-SPY|return|fwd5"], [feature, "A_congress|W90"],
                        classes={feature: "flywheel"})
    with pytest.raises(ValueError, match="refused"):
        spr.proxy_rule("REL:XLE-SPY", feature, MEMBERS)


@pytest.mark.parametrize(
    "feature,cls",
    [
        ("mom:XLE|chg20", "flywheel"),  # classifiable: price
        ("MOM:XLB|chg20", "people_density_congress"),
        ("MOM:XLF|chg20", "fresh_arm"),
        ("etf_flows:XLE|chg5", "flow"),
        ("FW_A:XLE|W30", "flywheel"),  # a construct with a subject
        ("TIINGO|k5", "flywheel"),  # classifiable namespace
        ("FW_A|W30", "people_density_insider"),  # people class on a non-people construct
        ("A_congress|W90", "people_density_congress_2"),
        ("XLE|chg20", "flywheel"),  # bare tickers: own price as a "construct"
        ("SPY|chg5", "flywheel"),
        ("XOM|chg20", "flywheel"),
        ("AAPL|z60", "flywheel"),  # any ticker-shaped name, mapped or not
        ("FW-A|W30", "flywheel"),  # construct names need an underscore
    ],
)
def test_class_overrides_cannot_smuggle_or_split(feature, cls):
    with pytest.raises(ValueError, match="refused"):
        spr.rel_catalog(["REL:XLE-SPY|return|fwd5"], [feature], members=MEMBERS, classes={feature: cls})


def test_people_grammar_is_strict():
    assert spr.people_channel("A_insider_px_flywheel|W30") is None
    assert spr.contains_price("A_insider_px_flywheel|W30")
    with pytest.raises(spr.Unclassified):
        spr.vote_class("A_insider_px_flywheel|W30")
    assert spr.people_channel("D_mean_insider|W30") is None
    for f, ch in [("A_insider_buy@edgar|W30", "insider"), ("S_insider_sell|W30", "insider"),
                  ("D_self_form4|W90", "insider"), ("A_thirteen_f|W90", "inst"),
                  ("C_form4_congress|W90", "multi")]:
        assert spr.people_channel(f) == ch, f
