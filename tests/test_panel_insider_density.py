"""Tests for the VS1 panel harness (``analysis/panel_insider_density.py``).

Synthetic data only: no production DB, no price or outcome of any real issuer.
Prices for the end-to-end test live in an in-memory SQLite ``raw_series``
shaped like production and are read through ``store.observations.read_window``.
"""

from __future__ import annotations

import functools
import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

from analysis import panel_insider_density as vs1
from analysis.offline_research_proof import bh_adjusted, holm_adjusted

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

UTC = timezone.utc


# --- pre-registration pin ---------------------------------------------------------------


def test_repository_preregistration_hashes_to_the_pinned_body_sha():
    assert vs1.check_prereg() == vs1.PREREG_BODY_SHA256
    assert len(vs1.PREREG_BODY_SHA256) == 64


def test_body_hash_ignores_line_endings_and_text_outside_the_markers(tmp_path):
    body = "\n## spec\nW = 90\n"
    a = tmp_path / "a.md"
    b = tmp_path / "b.md"
    a.write_bytes(f"intro\n{vs1.BODY_START}{body}{vs1.BODY_END}\ntrailer 1\n".encode())
    b.write_bytes(
        f"other intro\r\n{vs1.BODY_START}{body}{vs1.BODY_END}\r\ntrailer 2\r\n".replace("\n", "\r\n").encode()
    )
    assert vs1.prereg_body_sha256(a) == vs1.prereg_body_sha256(b)
    c = tmp_path / "c.md"
    c.write_text(f"{vs1.BODY_START}{body.replace('90', '91')}{vs1.BODY_END}", encoding="utf-8")
    assert vs1.prereg_body_sha256(c) != vs1.prereg_body_sha256(a)
    d = tmp_path / "d.md"
    d.write_text(f"{vs1.BODY_START}{body}", encoding="utf-8")
    with pytest.raises(ValueError):
        vs1.prereg_body_sha256(d)


def test_prereg_mismatch_is_refused(tmp_path):
    (tmp_path / vs1.PREREG_PATH).parent.mkdir(parents=True)
    (tmp_path / vs1.PREREG_PATH).write_text(f"{vs1.BODY_START}\nedited\n{vs1.BODY_END}", encoding="utf-8")
    with pytest.raises(ValueError, match="spec changed"):
        vs1.check_prereg(tmp_path)


def test_run_alpha_is_s11_spending():
    assert vs1.run_alpha(1) == pytest.approx(0.05)
    assert vs1.run_alpha(2) == pytest.approx(0.10 / 6)
    assert sum(vs1.run_alpha(k) for k in range(1, 500)) < vs1.LEDGER_Q
    assert vs1.trial_names() == ("A90|fwd5", "A90|fwd20", "A30|fwd5", "A30|fwd20")


# --- sector membership ---------------------------------------------------------------


def _map(entries):
    """entries: [(sector, subsector, ticker, weight, type)]"""
    out = {}
    for sector, sub, ticker, weight, kind in entries:
        subs = out.setdefault(sector, {"etf": "X", "subsectors": {}})["subsectors"]
        subs.setdefault(sub, {"weight": 0.1, "actors": []})["actors"].append(
            {"ticker": ticker, "weight": weight, "type": kind}
        )
    return out


def test_primary_sector_is_the_unique_max_weight_and_ties_are_excluded():
    sector_map = _map([
        ("Technology", "a", "AAA", 0.2, "company"),
        ("Communication Services", "b", "AAA", 0.22, "company"),
        ("Technology", "a", "BBB", 0.05, "company"),
        ("Materials", "c", "BBB", 0.05, "company"),
        ("Technology", "a", "CCC", 0.01, "company"),
        ("Technology", "d", "CCC", 0.3, "company"),
        ("Technology", "a", "PPP", 0.5, "person"),
    ])
    primary = vs1.primary_sectors(sector_map)
    assert primary == {"AAA": "Communication Services", "BBB": None, "CCC": "Technology"}


def test_sector_universe_resolves_ciks_and_collapses_share_classes():
    sector_map = _map([
        ("Technology", "a", "GOOGA", 0.2, "company"),
        ("Technology", "a", "GOOGC", 0.1, "company"),
        ("Technology", "a", "NOCIK", 0.1, "company"),
        ("Technology", "a", "TIE", 0.1, "company"),
        ("Energy", "e", "TIE", 0.1, "company"),
    ])
    issuers = pd.DataFrame({"ticker": ["GOOGA", "GOOGC", "TIE"], "cik": [1652044, 1652044, 7]})
    universe, info = vs1.sector_universe("Technology", sector_map, issuers)
    assert universe.to_dict("records") == [{"ticker": "GOOGA", "cik": 1652044}]
    assert info["unmapped_no_cik"] == ["NOCIK"]
    assert info["ambiguous_tie_excluded"] == ["TIE"]
    assert info["share_class_duplicates_dropped"] == 1
    with pytest.raises(ValueError):
        vs1.sector_universe("Crypto", sector_map, issuers)


def test_pinned_sector_map_gives_the_preregistered_technology_list():
    primary = vs1.primary_sectors(vs1.load_sector_map())
    tech = sorted(t for t, s in primary.items() if s == "Technology")
    assert len(tech) == 88
    assert {"AAPL", "MSFT", "NVDA", "TSLA"} <= set(tech)
    assert not {"AMZN", "GOOGL", "META", "ALB", "SQM"} & set(tech)


def test_issuer_map_reads_sec_company_tickers_json(tmp_path):
    path = tmp_path / "company_tickers.json"
    path.write_text(json.dumps({"0": {"cik_str": 320193, "ticker": "aapl", "title": "Apple"}}))
    assert vs1.load_issuer_map(path).to_dict("records") == [{"ticker": "AAPL", "cik": 320193}]


# --- Form 4 events -------------------------------------------------------------------


def _row(**overrides):
    base = {
        "accession_number": "0000000001-12-000001",
        "filing_date": "2012-03-02",
        "issuer_cik": "100",
        "document_type": "4",
        "amended": "False",
        "owner_cik": "5000",
        "nonderiv_trans_sk": "1",
        "transaction_date": "2012-03-01",
        "transaction_code": "P",
        "shares": "1000",
        "price_per_share": "20.0",
        "acquired_disposed_code": "A",
    }
    return {**base, **overrides}


def _events(rows):
    frame = pd.DataFrame(rows).astype("string")
    frame.columns = [c.upper() for c in frame.columns]
    return vs1.build_events(frame)


def test_event_rules_exclude_amendments_other_codes_tiny_and_bad_dates():
    rows = [
        _row(),  # kept
        _row(accession_number="a2", document_type="4/A", nonderiv_trans_sk="1"),
        _row(accession_number="a3", amended="True"),
        _row(accession_number="a4", document_type="5"),
        _row(accession_number="a5", transaction_code="S", acquired_disposed_code="D"),
        _row(accession_number="a6", acquired_disposed_code="D"),
        _row(accession_number="a7", shares="99"),
        _row(accession_number="a8", price_per_share="5", shares="1000"),  # $5,000
        _row(accession_number="a9", price_per_share=""),
        _row(accession_number="a10", transaction_date="2012-03-05"),  # after filing
        _row(accession_number="a11", transaction_date="2010-01-01"),  # > 365 d lag
        _row(accession_number="a12", issuer_cik=""),
    ]
    events = _events(rows)
    counts = events.receipt["counts"]
    assert len(events.purchases) == 1
    assert counts["excluded_not_form_4"] == 2
    assert counts["excluded_amended"] == 1
    assert counts["excluded_not_acquired"] == 1
    assert counts["excluded_small_or_unpriced"] == 3
    assert counts["excluded_transaction_date"] == 2
    assert counts["excluded_missing_accession_issuer_or_filing_date"] == 1
    # every valid accession (any code, form, amendment) is Section 16 activity
    assert counts["activity_accessions"] == 11


def test_joint_filings_are_one_purchase_by_one_actor():
    rows = [
        # one accession, fanned out over three owners (the derived file's layout)
        _row(owner_cik="7003"), _row(owner_cik="7001"), _row(owner_cik="7002"),
        # the same purchase filed separately a day later by another owner
        _row(accession_number="b1", filing_date="2012-03-03", owner_cik="6999"),
        # a second line in the first accession: a different purchase
        _row(nonderiv_trans_sk="2", shares="2000", owner_cik="7001"),
    ]
    events = _events(rows)
    purchases = events.purchases.sort_values("shares")
    assert len(purchases) == 2
    first = purchases.iloc[0]
    assert first["actor"] == 6999 and first["n_reports"] == 2
    assert first["filing_date"] == pd.Timestamp("2012-03-02")  # earliest filing
    assert purchases.iloc[1]["actor"] == 7001


def test_known_at_is_filing_date_22h_new_york_in_utc_across_dst():
    known = vs1.filing_known_at(pd.Series(pd.to_datetime(["2019-01-15", "2019-07-15"])))
    assert list(known) == [
        pd.Timestamp("2019-01-16T03:00", tz="UTC"),
        pd.Timestamp("2019-07-16T02:00", tz="UTC"),
    ]


def test_sec_dataset_date_format_is_parsed():
    parsed = vs1.parse_dates(pd.Series(["31-MAR-2023", "2023-03-31", "junk"]))
    assert parsed.iloc[0] == parsed.iloc[1] == pd.Timestamp("2023-03-31")
    assert pd.isna(parsed.iloc[2])


def test_missing_owner_cik_is_refused_without_an_owner_table():
    frame = pd.DataFrame([_row()]).drop(columns=["owner_cik"]).astype("string")
    with pytest.raises(ValueError, match="owner"):
        vs1.build_events(frame)
    owners = pd.DataFrame({"ACCESSION_NUMBER": ["0000000001-12-000001"], "RPTOWNERCIK": ["42"]})
    events = vs1.build_events(frame, owners)
    assert list(events.purchases["actor"]) == [42]


def test_parquet_reader_filters_issuers_and_keeps_declared_columns(tmp_path):
    frame = pd.DataFrame([_row(), _row(issuer_cik="200", accession_number="z")]).assign(extra="x")
    path = tmp_path / "nonderiv.parquet"
    frame.to_parquet(path)
    events = vs1.load_events(path, issuers=[100])
    assert list(events.purchases["issuer_cik"]) == [100]
    assert events.receipt["inputs"]["transactions"]["sha256"] == vs1.data_sha256(path)


# --- features ------------------------------------------------------------------------


def _purchases(rows):
    frame = pd.DataFrame(rows, columns=["issuer_cik", "actor", "filing_date"])
    frame["filing_date"] = pd.to_datetime(frame["filing_date"])
    frame["known_at"] = vs1.filing_known_at(frame["filing_date"])
    return frame


def test_density_never_counts_an_event_before_it_is_known():
    purchases = _purchases([(1, 10, "2015-06-10")])
    decisions = vs1.decision_instants([date(2015, 6, 10), date(2015, 6, 11)])
    a = vs1.density(purchases, [1], decisions, 90, 45.0)
    # 16:00 ET on the filing date precedes the 22:00 ET known_at: not counted yet
    assert a.iloc[0, 0] == 0.0
    # next session close: counted, age 18 hours
    assert a.iloc[1, 0] == pytest.approx(np.exp(-(18 / 24) / 45.0))


def test_density_counts_distinct_actors_once_with_their_latest_event_inside_the_window():
    purchases = _purchases([
        (1, 10, "2015-01-02"),  # actor 10, old
        (1, 10, "2015-03-02"),  # actor 10, latest -> this one counts
        (1, 11, "2015-03-20"),
        (1, 12, "2014-10-01"),  # outside the 90-day window
        (2, 10, "2015-03-20"),  # another issuer
    ])
    t = vs1.decision_instants([date(2015, 3, 31)])
    a = vs1.density(purchases, [1, 2, 3], t, 90, 45.0)
    known = purchases["known_at"]
    age = [(t[0] - known.iloc[i]).total_seconds() / 86400 for i in (1, 2)]
    assert a.loc[t[0], 1] == pytest.approx(sum(np.exp(-x / 45.0) for x in age))
    assert a.loc[t[0], 3] == 0.0
    unweighted = vs1.density(purchases, [1], t, 90, 1e12)
    assert unweighted.iloc[0, 0] == pytest.approx(2.0)


def test_section16_activity_mask_uses_a_trailing_730_day_window():
    activity = pd.DataFrame({"issuer_cik": [1], "known_at": vs1.filing_known_at(pd.Series(pd.to_datetime(["2013-01-02"])))})
    t = vs1.decision_instants([date(2013, 1, 2), date(2013, 1, 3), date(2014, 12, 31), date(2015, 1, 5)])
    mask = vs1.active_mask(activity, [1, 2], t)
    assert mask[1].tolist() == [False, True, True, False]
    assert not mask[2].any()


def test_entry_positions_are_one_per_issuer_and_first_close_after_known_at():
    purchases = _purchases([
        (1, 10, "2015-06-10"),  # Wednesday filing -> entry close Thursday 06-11
        (1, 11, "2015-06-10"),  # same issuer, same entry session: one position
        (1, 10, "2015-06-11"),  # next day -> entry Friday 06-12
        (2, 12, "2015-06-12"),  # Friday filing -> Monday 06-15
        (3, 13, "2015-06-30"),  # after the last session: reported, not dropped
    ]).assign(value=[1e5, 2e5, 3e5, 4e5, 5e5])
    sessions = [date(2015, 6, d) for d in (10, 11, 12, 15, 16)]
    positions = vs1.entry_positions(purchases, sessions).sort_values(["issuer_cik", "entry_close"])
    first = positions.iloc[0]
    assert first["entry_close"] == pd.Timestamp("2015-06-11T20:00", tz="UTC")
    assert first["actors"] == [10, 11] and first["n_actors"] == 2 and first["purchases"] == 2
    assert first["total_value"] == pytest.approx(3e5) and first["largest_value"] == pytest.approx(2e5)
    assert positions.iloc[1]["entry_close"] == pd.Timestamp("2015-06-12T20:00", tz="UTC")
    assert positions.iloc[2]["entry_close"] == pd.Timestamp("2015-06-15T20:00", tz="UTC")
    last = positions[positions["issuer_cik"] == 3].iloc[0]
    assert last["status"] == "no_entry_session" and pd.isna(last["entry_close"])
    # no entry close ever precedes the filing's known_at
    opened = positions[positions["status"] == "opened"]
    assert (opened["entry_close"] > opened["last_known_at"]).all()


def test_missing_labels_are_counted_not_silently_dropped():
    feature = np.array([[1.0, 0.0, 0.0, np.nan], [0.0, 2.0, 0.0, 0.0]])
    label = np.array([[np.nan, 0.1, np.nan, 0.2], [0.1, np.nan, 0.3, 0.0]])
    panel = vs1.TrialPanel("A90|fwd20", "discovery", 20, ["a", "b"], ["c", "d"], list("wxyz"), feature, label)
    counts = vs1.missing_labels(panel)
    assert counts == {"issuer_dates_with_feature": 7, "missing_label": 3, "buyer_issuer_dates": 2,
                      "buyer_missing_label": 2, "buyer_missing_share": 1.0}
    verdict = vs1.verdict({"calibration": {"state": "WEAK_POSITIVE"},
                           "ledger": [{"trial": "A90|fwd20", "labels": counts}]}, [], {"gate_passed": False})
    assert any("SURVIVORSHIP_WARNING" in note for note in verdict["notes"])


def test_decision_instant_is_the_new_york_close():
    assert list(vs1.decision_instants([date(2019, 1, 15), date(2019, 7, 15)])) == [
        pd.Timestamp("2019-01-15T21:00", tz="UTC"),
        pd.Timestamp("2019-07-15T20:00", tz="UTC"),
    ]


# --- statistics ----------------------------------------------------------------------


@functools.lru_cache(maxsize=4)
def _window_days(window: str) -> pd.DatetimeIndex:
    lo, hi = vs1.window_bounds(window)
    return pd.bdate_range(lo, hi - pd.Timedelta(days=1), tz="UTC")


def _synthetic_trials(rng, ic=(0.0, 0.0, 0.0, 0.0), T=120, E=40, window="discovery",
                      factor_phi=0.3, prevalence=0.2):
    """Four correlated trial panels; labels load on a persistent common factor
    whose exposure is correlated with the feature (the hard null case)."""
    days = _window_days(window)
    beta = rng.standard_normal(E)
    panels = {}
    base = np.where(rng.random((T, E)) < prevalence, rng.exponential(1.0, (T, E)), 0.0)
    base = base + 0.5 * np.clip(beta, 0, None)[None, :] * (rng.random((T, E)) < prevalence)
    for k, trial in enumerate(vs1.trial_names()):
        h = int(trial.split("fwd")[1])
        decided = days[::h][: T + 1]
        n = len(decided) - 1
        feature = base[:n] if trial.startswith("A90") else base[:n] * (rng.random((n, E)) < 0.6)
        f = np.zeros(n)
        for t in range(1, n):
            f[t] = factor_phi * f[t - 1] + rng.standard_normal()
        z = feature - feature.mean(1, keepdims=True)
        z = z / (z.std(1, keepdims=True) + 1e-12)
        label = ic[k] * 1.5 * z + 0.8 * beta[None, :] * f[:, None] + rng.standard_normal((n, E))
        panels[trial] = vs1.TrialPanel(
            trial=trial, window=window, horizon=h,
            decision_at=[d.isoformat() for d in decided[:n]],
            label_end=[d.isoformat() for d in decided[1 : n + 1]],
            entities=[f"T{i}" for i in range(E)],
            feature=feature.astype(float), label=label,
        )
    return panels


def _pvalues(panels, perms=499):
    return [
        vs1.measure_trial(panels[t], perms=perms, sensitivity=False)["p"] for t in vs1.trial_names()
    ]


def test_signflip_null_controls_familywise_and_false_discovery_rates_under_the_global_null():
    rng = np.random.default_rng(7)
    reps = 150
    holm_any = bh_any = 0
    for _ in range(reps):
        p = _pvalues(_synthetic_trials(rng))
        holm_any += any(x <= vs1.run_alpha(1) for x in holm_adjusted(p))
        bh_any += any(x <= vs1.BH_Q for x in bh_adjusted(p))
    # nominal 0.05 (Holm) and 0.10 (BH: FDR = FWER under the global null); 3 SE slack
    assert holm_any / reps <= 0.05 + 3 * np.sqrt(0.05 * 0.95 / reps)
    assert bh_any / reps <= 0.10 + 3 * np.sqrt(0.10 * 0.90 / reps)


def test_bh_false_discovery_proportion_is_controlled_with_two_true_effects():
    rng = np.random.default_rng(11)
    reps, fdp = 100, []
    for _ in range(reps):
        p = _pvalues(_synthetic_trials(rng, ic=(0.25, 0.25, 0.0, 0.0)))
        rejected = [i for i, x in enumerate(bh_adjusted(p)) if x <= vs1.BH_Q]
        false = [i for i in rejected if i >= 2]
        fdp.append(len(false) / max(1, len(rejected)))
    assert np.mean(fdp) <= vs1.BH_Q + 0.05


def test_planted_effect_is_found_in_discovery_and_survives_the_holdout():
    rng = np.random.default_rng(3)
    planted = (0.0, 0.25, 0.0, 0.0)  # the primary trial only
    spec = vs1.RunSpec(run_id="test", sector="Technology", trials=vs1.trial_names(), perms=999)
    frozen = vs1.discover_panel(spec, _synthetic_trials(rng, planted), inputs={}, sensitivity=False)
    ledger = {t["trial"]: t for t in frozen["payload"]["ledger"]}
    assert ledger["A90|fwd20"]["selected"] and ledger["A90|fwd20"]["mean_ic"] > 0
    assert frozen["payload"]["calibration"]["state"] == "CONSISTENT"
    key = vs1.open_holdout(frozen, allow_holdout=True, prereg_sha256=vs1.PREREG_BODY_SHA256)
    result = vs1.evaluate_panel_holdout(
        frozen, _synthetic_trials(rng, planted, window="holdout"), key, power={"gate_passed": True}
    )
    survivor = next(c for c in result["holdout_checks"] if c["trial"] == "A90|fwd20")
    assert survivor["retrospective_survivor"]
    assert result["verdict"]["state"] == "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    assert result["promotion_allowed"] is False


def test_null_discovery_yields_no_survivor_and_flags_underpowered():
    rng = np.random.default_rng(5)
    spec = vs1.RunSpec(run_id="null", sector="Technology", trials=vs1.trial_names(), perms=999)
    frozen = vs1.discover_panel(spec, _synthetic_trials(rng, factor_phi=0.0), inputs={}, sensitivity=False)
    key = vs1.open_holdout(frozen, allow_holdout=True, prereg_sha256=vs1.PREREG_BODY_SHA256)
    result = vs1.evaluate_panel_holdout(frozen, _synthetic_trials(rng, window="holdout"), key, power=None)
    assert result["verdict"]["state"] in ("NO_SURVIVOR", "MACHINERY_SUSPECT")
    if result["verdict"]["state"] == "NO_SURVIVOR":
        assert any("UNDERPOWERED" in n for n in result["verdict"]["notes"])


def test_contrary_discovery_is_machinery_suspect():
    ledger = [
        {"trial": t, "status": "tested", "mean_ic": -0.05 if t == "A30|fwd5" else 0.01,
         "p": 0.01 if t == "A30|fwd5" else 0.5, "p_one_sided_positive": 0.3, "selected": False}
        for t in vs1.trial_names()
    ]
    calib = vs1.calibration(ledger)
    assert calib["state"] == "CONTRARY" and calib["contrary_trials"] == ["A30|fwd5"]
    verdict = vs1.verdict({"calibration": calib}, [], {"gate_passed": False})
    assert verdict["state"] == "MACHINERY_SUSPECT"


def test_absent_effect_when_powered_is_machinery_suspect_but_not_when_underpowered():
    calib = {"state": "ABSENT"}
    assert vs1.verdict({"calibration": calib}, [], {"gate_passed": True})["state"] == "MACHINERY_SUSPECT"
    assert vs1.verdict({"calibration": calib}, [], {"gate_passed": False})["state"] == "NO_SURVIVOR"


def test_rank_ic_abstains_on_constant_feature_or_too_few_issuers():
    feature = np.zeros((3, 25))
    feature[1, :3] = 1.0
    label = np.random.default_rng(0).standard_normal((3, 25))
    label[2, :] = np.nan
    label[2, :5] = 1.0
    ic, counts = vs1.rank_ic_series(feature, label)
    assert np.isnan(ic[0]) and np.isfinite(ic[1]) and np.isnan(ic[2])
    assert counts.tolist() == [25, 25, 5]


def test_signflip_pvalue_resolution_and_direction():
    ic = np.full(40, 0.05)
    mean, two, one = vs1.signflip_pvalues(ic, 1, 999, 1, 1)
    assert mean == pytest.approx(0.05)
    assert two == pytest.approx(1 / 1000) and one == pytest.approx(1 / 1000)
    _, _, other_way = vs1.signflip_pvalues(ic, 1, 999, 1, -1)
    assert other_way == 1.0


def test_planted_power_grows_with_the_effect_and_is_zero_without_buyers():
    rng = np.random.default_rng(1)
    feature = np.where(rng.random((150, 40)) < 0.2, 1.0, 0.0)
    weak = vs1.planted_power(feature, 0.02, sims=30, perms=199)
    strong = vs1.planted_power(feature, 0.2, sims=30, perms=199)
    assert strong["power"] > weak["power"] and strong["power"] >= 0.9
    assert vs1.planted_power(np.zeros((150, 40)), 0.2, sims=5, perms=99)["power"] == 0.0


# --- holdout and discovery refusals ------------------------------------------------------


@pytest.fixture()
def frozen_null():
    rng = np.random.default_rng(9)
    spec = vs1.RunSpec(run_id="r", sector="Technology", trials=vs1.trial_names(), perms=999)
    return vs1.discover_panel(spec, _synthetic_trials(rng, T=40), inputs={}, sensitivity=False)


def test_holdout_is_refused_without_the_flag_or_the_matching_hash(frozen_null):
    with pytest.raises(PermissionError):
        vs1.open_holdout(frozen_null, allow_holdout=False, prereg_sha256=vs1.PREREG_BODY_SHA256)
    with pytest.raises(PermissionError):
        vs1.open_holdout(frozen_null, allow_holdout="yes", prereg_sha256=vs1.PREREG_BODY_SHA256)
    with pytest.raises(PermissionError):
        vs1.open_holdout(frozen_null, allow_holdout=True, prereg_sha256="0" * 64)
    tampered = json.loads(json.dumps(frozen_null))
    tampered["payload"]["ledger"][0]["selected"] = True
    with pytest.raises(PermissionError):
        vs1.open_holdout(tampered, allow_holdout=True, prereg_sha256=vs1.PREREG_BODY_SHA256)


def test_holdout_evaluation_needs_a_key_for_this_manifest(frozen_null):
    with pytest.raises(TypeError):
        vs1.HoldoutKey(object(), frozen_null["sha256"])
    other = vs1.open_holdout(frozen_null, allow_holdout=True, prereg_sha256=vs1.PREREG_BODY_SHA256)
    other.frozen_sha256 = "f" * 64
    with pytest.raises(PermissionError):
        vs1.evaluate_panel_holdout(frozen_null, {}, other)


def test_discovery_refuses_holdout_panels_and_undeclared_trials():
    rng = np.random.default_rng(2)
    spec = vs1.RunSpec(run_id="r", sector="Technology", trials=vs1.trial_names(), perms=999)
    with pytest.raises(ValueError):
        vs1.discover_panel(spec, _synthetic_trials(rng, T=40, window="holdout"), inputs={})
    panels = _synthetic_trials(rng, T=40)
    panels.pop("A30|fwd5")
    with pytest.raises(ValueError):
        vs1.discover_panel(spec, panels, inputs={})
    with pytest.raises(ValueError):
        vs1.RunSpec(run_id="r", sector="Energy", run_k=2, trials=vs1.trial_names()).validate()
    with pytest.raises(ValueError):
        vs1.RunSpec(run_id="r", sector="Technology", run_k=2, trials=vs1.trial_names()).validate()


def test_overlapping_or_out_of_window_decisions_are_refused():
    rng = np.random.default_rng(4)
    panel = _synthetic_trials(rng, T=40)["A90|fwd20"]
    panel.label_end[0] = panel.decision_at[3]
    with pytest.raises(ValueError, match="overlapping"):
        vs1.validate_panel(panel)
    panel = _synthetic_trials(rng, T=40)["A90|fwd20"]
    panel.label_end[-1] = "2020-01-02T00:00:00+00:00"
    with pytest.raises(ValueError, match="outside"):
        vs1.validate_panel(panel)


# --- prices through store.observations (SQLite) --------------------------------------------

TIINGO, YFINANCE = 1, 2


def _price_db(series: dict[str, pd.Series], extra_yf: dict[str, pd.Series] | None = None):
    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table(
        "raw_series", md,
        Column("series_id", String), Column("source_id", Integer), Column("obs_date", Date),
        Column("pull_timestamp", DateTime), Column("value", Float), Column("raw_payload", Text),
        Column("pull_status", String),
    )
    md.create_all(engine)
    pulled = datetime(2026, 9, 20, 6, 0)
    rows = []
    for source, table in ((TIINGO, series), (YFINANCE, extra_yf or {})):
        for ticker, values in table.items():
            rows.extend(
                {"series_id": f"YF:{ticker}:close", "source_id": source, "obs_date": d.date(),
                 "pull_timestamp": pulled, "value": float(v), "raw_payload": "{}", "pull_status": "SUCCESS"}
                for d, v in values.items()
            )
    with engine.begin() as c:
        c.execute(catalog.insert(), [{"id": TIINGO, "name": "tiingo"}, {"id": YFINANCE, "name": "yfinance"}])
        c.execute(raw.insert(), rows)
    return engine


def _manifest(tickers, source="tiingo"):
    return vs1.PriceManifest(
        source=source, series_template="YF:{ticker}:close", basis="split+dividend adjusted",
        benchmark="XLK", admitted=tuple(sorted(set(tickers) | {"XLK"})), probe_report_sha256="a" * 64,
    )


def test_price_reader_refuses_unadmitted_sources_tickers_and_reads_past_the_split():
    dates = pd.bdate_range("2019-12-20", "2020-01-10")
    engine = _price_db({"XLK": pd.Series(100.0, index=dates), "AAA": pd.Series(10.0, index=dates)})
    ts = datetime(2026, 9, 26, tzinfo=UTC)
    with engine.connect() as conn:
        with pytest.raises(ValueError):
            vs1.load_price_panel(conn, _manifest(["AAA"], "yfinance"), ["AAA"], start=date(2019, 12, 1),
                                 as_of=date(2019, 12, 31), as_of_ts=ts, window="discovery")
        with pytest.raises(PermissionError):
            vs1.load_price_panel(conn, _manifest([]), ["AAA"], start=date(2019, 12, 1),
                                 as_of=date(2019, 12, 31), as_of_ts=ts, window="discovery")
        with pytest.raises(PermissionError):
            vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                 as_of=date(2020, 1, 1), as_of_ts=ts, window="discovery")
        with pytest.raises(PermissionError):
            vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                 as_of=date(2020, 1, 10), as_of_ts=ts, window="holdout")
        panel = vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                     as_of=date(2019, 12, 31), as_of_ts=ts, window="discovery")
    assert panel.receipt["series"]["AAA"]["last"] == "2019-12-31"
    assert panel.closes().index.max() == pd.Timestamp("2019-12-31")


def test_price_reader_takes_only_the_manifest_source_on_a_shared_series_id():
    dates = pd.bdate_range("2019-12-02", "2019-12-31")
    engine = _price_db(
        {"XLK": pd.Series(100.0, index=dates), "AAA": pd.Series(10.0, index=dates)},
        extra_yf={"AAA": pd.Series(99.0, index=dates)},
    )
    with engine.connect() as conn:
        panel = vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                     as_of=date(2019, 12, 31), as_of_ts=datetime(2026, 9, 26, tzinfo=UTC),
                                     window="discovery")
    assert set(panel.closes()["AAA"]) == {10.0}


def test_labels_are_purged_at_the_split_and_horizon_spaced():
    dates = pd.bdate_range("2019-11-01", "2020-02-28")
    closes = pd.DataFrame({"AAA": np.linspace(10, 20, len(dates)), "XLK": 100.0}, index=dates)
    positions, labels, momentum = vs1.relative_labels(closes, "XLK", ["AAA"], 5, "discovery")
    ends = [dates[i + 5] for i in positions]
    assert max(ends) < pd.Timestamp("2020-01-01")
    assert all(b - a == 5 for a, b in zip(positions, positions[1:]))
    assert labels.shape == (len(positions), 1) and np.isfinite(labels).all()
    positions, _, _ = vs1.relative_labels(closes, "XLK", ["AAA"], 5, "holdout")
    assert dates[positions[0]] >= pd.Timestamp("2020-01-01")


def test_end_to_end_planted_insider_effect_through_the_price_reader():
    """Buyers' stocks drift up after the filing; the harness must find it, and only
    through admitted, source-filtered, split-bounded reads."""
    rng = np.random.default_rng(12)
    tickers = [f"T{i:02d}" for i in range(24)]
    ciks = list(range(1000, 1024))
    dates = pd.bdate_range("2010-01-04", "2026-06-30")
    n = len(dates)
    rets = rng.normal(0.0, 0.01, (n, len(tickers)))
    rows, activity_rows = [], []
    for j, cik in enumerate(ciks):
        # quarterly routine filings keep every issuer a Section 16 filer
        for d in dates[::60]:
            activity_rows.append(_row(accession_number=f"act-{cik}-{d.date()}", issuer_cik=str(cik),
                                      filing_date=str(d.date()), transaction_date=str(d.date()),
                                      transaction_code="A", price_per_share="0"))
        for i in np.flatnonzero(rng.random(n) < 0.004):
            filed = dates[min(i + 1, n - 1)]
            rows.append(_row(accession_number=f"p-{cik}-{i}", issuer_cik=str(cik),
                             owner_cik=str(int(rng.integers(1, 6))), filing_date=str(filed.date()),
                             transaction_date=str(dates[i].date())))
            k = min(i + 2, n - 1)
            rets[k:k + 20, j] += 0.004  # +8% over the 20 sessions after the filing is known
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=dates, columns=tickers)
    prices["XLK"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.008, n)))
    engine = _price_db({c: prices[c] for c in prices.columns})
    events = _events(rows + activity_rows)
    universe = pd.DataFrame({"ticker": tickers, "cik": ciks})
    manifest = _manifest(tickers)
    ts = datetime(2026, 9, 26, tzinfo=UTC)
    with engine.connect() as conn:
        discovery = vs1.load_price_panel(conn, manifest, tickers, start=date(2011, 11, 1),
                                         as_of=date(2019, 12, 31), as_of_ts=ts, window="discovery")
    assert max(o["last"] for o in discovery.receipt["series"].values()) <= "2019-12-31"
    panels = vs1.build_trial_panels(events, universe, discovery, "discovery")
    spec = vs1.RunSpec(run_id="e2e", sector="Technology", trials=vs1.trial_names(), perms=999)
    frozen = vs1.discover_panel(spec, panels, inputs={"price": discovery.receipt_sha}, sensitivity=True)
    ledger = {t["trial"]: t for t in frozen["payload"]["ledger"]}
    primary = ledger["A90|fwd20"]
    assert primary["selected"] and primary["mean_ic"] > 0
    assert primary["magnitude"]["buyer_minus_nonbuyer"] > 0
    assert "momentum_mean_ic" in primary["baseline"]
    assert primary["magnitude"]["small_line_buyer_issuer_dates"] > 0  # $20k lines
    assert primary["labels"]["buyer_missing_share"] == 0.0
    key = vs1.open_holdout(frozen, allow_holdout=True, prereg_sha256=vs1.PREREG_BODY_SHA256)
    with engine.connect() as conn:
        holdout = vs1.load_price_panel(conn, manifest, tickers, start=date(2019, 11, 1),
                                       as_of=date(2026, 6, 30), as_of_ts=ts, window="holdout",
                                       holdout_key=key)
    result = vs1.evaluate_panel_holdout(frozen, vs1.build_trial_panels(events, universe, holdout, "holdout"),
                                        key, power={"gate_passed": True})
    assert result["verdict"]["state"] == "HOLDOUT_SURVIVOR_FORWARD_PENDING"


# --- registry (research_forward_log chain) ---------------------------------------------------


def test_registry_appends_a_pinned_header_and_one_registration(tmp_path):
    now = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)
    records = vs1.register(tmp_path, now, "c" * 40)
    assert [r["kind"] for r in records] == ["header", "preregistration"]
    assert records[0]["prereg_sha256"] == vs1.PREREG_BODY_SHA256
    assert records[1]["runs"]["vs1"]["alpha"] == pytest.approx(0.05)
    log = vs1.registry(tmp_path)
    check = log.verify_chain()
    assert check["ok"] and check["records"] == 2 and check["anchored_records"] == 2
    with pytest.raises(ValueError, match="already registered"):
        vs1.register(tmp_path, now, "c" * 40)
    # the S10 forward log's pin does not accept this registry's header
    from analysis.research_forward_log import ForwardLog

    other = ForwardLog(tmp_path, log_filename=vs1.REGISTRY_LOG, anchor_filename=vs1.REGISTRY_ANCHORS,
                       lock_filename=vs1.REGISTRY_LOCK)
    assert not other.verify_chain()["ok"]


def test_registry_detects_an_edited_line(tmp_path):
    vs1.register(tmp_path, datetime(2026, 9, 27, tzinfo=UTC), "c" * 40)
    path = tmp_path / vs1.REGISTRY_LOG
    lines = path.read_bytes().split(b"\n")
    lines[1] = lines[1].replace(b'"ledger_id":"grid-granular-panel"', b'"ledger_id":"grid-granular-panel2"')
    path.write_bytes(b"\n".join(lines))
    assert not vs1.registry(tmp_path).verify_chain()["ok"]


def test_forward_log_defaults_are_unchanged():
    from analysis import research_forward_log as fl

    log = fl.ForwardLog(Path("x"))
    assert log.path.name == fl.LOG_FILENAME and log.prereg_sha256 == fl.PREREG_SHA256
    with pytest.raises(ValueError):
        fl.ForwardLog(Path("x"), prereg_sha256="nothex")
