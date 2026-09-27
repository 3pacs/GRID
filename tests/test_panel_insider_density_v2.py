"""Tests for the VS1 v2 panel harness (``analysis/panel_insider_density_v2.py``).

Synthetic data only: no production DB, no price or outcome of any real issuer,
no network. Prices for the end-to-end test live in an in-memory SQLite
``raw_series`` shaped like production and are read through
``store.observations.read_window``. The v1 harness keeps its own tests
(``tests/test_panel_insider_density.py``); the v1 pins are re-asserted here so a
v2 change can never move them.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import asdict
from datetime import date, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from tests.test_panel_insider_density import (
    AS_OF_TS,
    NOW,
    _git,
    _map,
    _power_file,
    _price_db,
    _row,
    _submission,
    _synthetic_trials,
)

UTC = timezone.utc


# --- pins ----------------------------------------------------------------------------------


def test_v2_preregistration_hashes_to_the_pinned_body_sha():
    assert v2.check_prereg() == v2.PREREG_BODY_SHA256
    assert v2.PREREG_BODY_SHA256 != v1.PREREG_BODY_SHA256


def test_v1_pins_are_untouched_by_v2():
    assert v1.check_prereg() == "85078eeeb08fe292f4a01a295261c6594d865423cdfd505621949ba43dea7c5a"
    assert v1.REGISTERED_RECORD_SHA256[1] == "5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508"
    assert v1.WITNESS_PATH == "05-GRID/Paper-Log/vs1/granular_panel_prereg_v1.anchors.jsonl"
    assert v1.REGISTRY_LOG == "granular_panel_prereg_v1.jsonl"
    assert (v2.REGISTRY_LOG, v2.REGISTRY_ANCHORS, v2.REGISTRY_LOCK) != (
        v1.REGISTRY_LOG, v1.REGISTRY_ANCHORS, v1.REGISTRY_LOCK)


def test_v2_keeps_v1_design_constants():
    """Features, horizons, trials, statistic settings, windows and the Stage-0 gate are v1's."""
    assert v1.FEATURES == {"A90": (90, 45.0), "A30": (30, 15.0)} and v1.HORIZONS == (5, 20)
    assert v2.run_spec().trials == v1.trial_names() and v2.run_spec().alpha == pytest.approx(0.05)
    v2.run_spec().validate()
    assert (v1.DISCOVERY_START, v1.SPLIT, v1.END) == (
        "2012-01-01T00:00:00+00:00", "2020-01-01T00:00:00+00:00", "2026-07-01T00:00:00+00:00")
    assert v1.power_settings()["threshold"] == pytest.approx(0.0125)


def test_the_v2_witness_location_is_pinned():
    assert v2.WITNESS_REMOTE_URL == "https://github.com/3pacs/obsidian-vault.git"
    assert v2.WITNESS_BRANCH == "main"
    assert v2.WITNESS_PATH == "05-GRID/Paper-Log/vs1/granular_panel_prereg_v2.anchors.jsonl"
    records = v2.registration_records(v2.REGISTERED_AT, v2.REGISTERED_CODE_SHA)
    heads = v1.chained_sha256(records)
    assert tuple(heads) == v2.REGISTERED_RECORD_SHA256
    assert json.loads(v2.REGISTERED_ANCHOR_LINE) == {
        "head_sha256": heads[1], "prev_anchor_sha256": None, "records": 2,
        "run_at": v2.REGISTERED_AT.isoformat()}


def test_the_pin_is_the_real_v2_registration():
    """Header + preregistration as registered 2026-09-27T22:43:29Z against 6f3d2892."""
    assert v2.REGISTERED_RECORD_SHA256[1] == "05b20c31273a926d1be56d2afe4fce4e3cf7a8ca2769f394cad9e2fb773ceff8"
    assert v2.REGISTERED_CODE_SHA == "6f3d2892cbf66b23f408b0794c397b9ff1798dc4"
    assert v2.REGISTERED_PREREG_SHA256 == v2.PREREG_BODY_SHA256


def test_the_prereg_states_the_pinned_inputs():
    body = v1.prereg_body((v2.REPO / v2.PREREG_PATH).read_text(encoding="utf-8"))
    for pin in (v2.ISSUER_MAP_SHA256, v2.SIC_MAP_SHA256, v1.SECTOR_MAP_SHA256, v1.PREREG_BODY_SHA256):
        assert pin in body
    for lo, hi in v2.SIC_RANGES:
        assert f"{lo}–{hi}" in body


# --- universe ------------------------------------------------------------------------------


def _issuers(rows):
    return pd.DataFrame(rows, columns=["ticker", "cik"])


def _sic(rows):
    return pd.DataFrame([{"cik": c, "sic": s, "name": f"N{c}", "tickers": [], "former_names": [],
                          "http_status": 200} for c, s in rows])


def test_sic_ranges_and_groups():
    assert [v2.sic_in_ranges(s) for s in (3569, 3570, 3579, 3672, 3674, 3680, 7372, 7379, 7380, None, "x")] == [
        False, True, True, True, True, False, True, True, False, False, False]
    assert v2.sic_group(3674) == "3660-3679" and v2.sic_group(3711) == "other" and v2.sic_group(None) == "other"


def test_universe_is_sector_map_members_plus_sic_issuers_with_a_current_ticker():
    sector_map = _map([("Technology", "a", "AAPL", 0.3, "company"), ("Technology", "a", "TSLA", 0.2, "company"),
                       ("Energy", "e", "XOM", 0.3, "company")])
    issuers = _issuers([("AAPL", 1), ("TSLA", 2), ("XOM", 3), ("SOFT", 4), ("SOFTW", 4), ("CHIP", 5),
                        ("BRK-A", 6), ("BRK-B", 6), ("MED", 7)])
    sic = _sic([(1, 3571), (2, 3711), (3, 2911), (4, 7372), (5, 3674), (6, 7374), (7, 2834), (8, 7372)])
    universe, info = v2.v2_universe(sector_map, issuers, sic)
    rows = {r["cik"]: r for r in universe.to_dict("records")}
    assert sorted(rows) == [1, 2, 4, 5, 6]  # XOM/MED out of range; CIK 8 has no current ticker
    assert rows[1]["source"] == "both" and rows[2]["source"] == "sector_map" and rows[4]["source"] == "sic"
    assert rows[2]["sic_group"] == "other" and rows[5]["sic_group"] == "3660-3679"
    assert rows[4]["ticker"] == "SOFT" and rows[4]["current_tickers"] == ["SOFT", "SOFTW"]
    assert rows[6]["ticker"] == "BRK-A" and rows[6]["current_tickers"] == ["BRKA", "BRKB"]
    assert info["sic_in_ranges_without_current_ticker"] == 1
    assert info["by_source"] == {"both": 1, "sector_map": 1, "sic": 3}
    json.dumps(universe.to_dict("records"))  # sic None/int only: hashable into receipts


def test_pinned_input_files_are_refused_when_they_differ(tmp_path):
    path = tmp_path / "m.jsonl"
    path.write_text(json.dumps({"cik": 1, "sic": 7372}) + "\n")
    with pytest.raises(ValueError, match="SIC map differs"):
        v2.load_sic_map(path)
    assert v2.load_sic_map(path, pinned=None)["sic"].tolist() == [7372]
    tickers = tmp_path / "t.json"
    tickers.write_text(json.dumps({"0": {"cik_str": 1, "ticker": "a"}}))
    with pytest.raises(ValueError, match="issuer map differs"):
        v2.load_issuer_map(tickers)


# --- ticker rule and Form 4 history -----------------------------------------------------------


@pytest.mark.parametrize("raw,expected", [
    ("AAPL", {"AAPL"}), (" aapl ", {"AAPL"}), ("NYSE: KRC", {"KRC"}), ("(NYSE:FBC)", {"FBC"}),
    ("ISCA, ISCB", {"ISCA", "ISCB"}), ("MOGA/MOGB", {"MOGA", "MOGB", "MOGAMOGB"}), ("BRK.B", {"BRKB"}),
    ("Z AND ZG", {"Z", "ZG"}), ('"""WM"""', {"WM"}), ("NONE", set()), ("N/A", set()), ("[NONE]", set()),
    ("1314152", set()), (None, set()),
])
def test_filed_symbols_parse_the_symbols_as_filed(raw, expected):
    got = set(v2.filed_symbols(raw))
    assert expected <= got
    if not expected:
        assert got == set()


def _subs(rows):
    return pd.DataFrame([{**_submission(**r), "issuer_ticker": r.get("issuer_ticker", "AAA")} for r in rows])


def _universe_frame(rows):
    return pd.DataFrame([{"ticker": t, "cik": c, "source": "sic", "sic": 7372, "sic_group": "7370-7379",
                          "current_tickers": sorted(v2.canonical_symbol(x) for x in cur)} for t, c, cur in rows])


def _decisions(*days):
    return v1.decision_instants([date.fromisoformat(d) for d in days])


def test_ticker_rule_admits_only_while_the_issuers_own_filings_name_a_current_ticker():
    """ESI-style reuse: the CIK filed as PAH until 2019, then as ESI (today's ticker)."""
    rows = [
        {"accession_number": "a1", "filing_date": "2014-03-01", "issuer_cik": "100", "issuer_ticker": "PAH"},
        {"accession_number": "a2", "filing_date": "2019-02-01", "issuer_cik": "100", "issuer_ticker": "PAH"},
        {"accession_number": "a3", "filing_date": "2019-02-05", "issuer_cik": "100", "issuer_ticker": "ESI"},
        {"accession_number": "a4", "filing_date": "2019-06-01", "issuer_cik": "100", "issuer_ticker": "NONE"},
        {"accession_number": "a5", "filing_date": "2019-09-01", "issuer_cik": "100", "issuer_ticker": "PAH"},
        {"accession_number": "a6", "filing_date": "2019-09-01", "issuer_cik": "100", "issuer_ticker": "ESI"},
        {"accession_number": "a7", "filing_date": "2019-10-01", "issuer_cik": "100", "issuer_ticker": "EIS"},
    ]
    universe = _universe_frame([("ESI", 100, ["ESI"])])
    admission = v2.build_admission(_subs(rows), universe)
    decisions = _decisions("2014-01-02", "2015-06-01", "2019-02-04", "2019-02-06", "2019-07-01", "2019-09-03",
                           "2019-10-02")
    got = v2.ticker_mask(admission, [100], decisions)[100].tolist()
    # before any filing; PAH; still PAH; ESI; NONE ignored (still ESI); same-instant PAH+ESI passes; typo EIS
    assert got == [False, False, False, True, True, True, False]
    assert admission.receipt["counts"]["accessions_naming_no_ticker"] == 1


def test_form4_history_needs_two_form4_accessions_in_the_trailing_730_days():
    rows = [
        {"accession_number": "f1", "filing_date": "2014-01-10", "document_type": "4"},
        {"accession_number": "f2", "filing_date": "2014-05-10", "document_type": "3"},
        {"accession_number": "f3", "filing_date": "2014-06-10", "document_type": "4/A"},
        {"accession_number": "f3", "filing_date": "2014-06-10", "document_type": "4/A", "owner_cik": "6000"},
    ]
    admission = v2.build_admission(_subs(rows), _universe_frame([("AAA", 100, ["AAA"])]))
    got = v2.form4_history_mask(admission, [100], _decisions("2014-03-03", "2014-06-11", "2016-01-11",
                                                             "2016-06-13"))[100].tolist()
    assert got == [False, True, False, False]  # joint-owner fan-out of f3 counts once


def test_feature_abstains_where_the_issuer_is_not_admitted():
    tx = [_row(accession_number="p1", issuer_cik="100", filing_date="2015-03-02", transaction_date="2015-03-01")]
    subs = _subs([{"accession_number": "p1", "filing_date": "2015-03-02", "issuer_ticker": "OLD"},
                  {"accession_number": "h1", "filing_date": "2015-01-05", "issuer_ticker": "OLD"},
                  {"accession_number": "h2", "filing_date": "2015-04-01", "issuer_ticker": "NEW"}])
    frame = pd.DataFrame(tx).astype("string")
    frame.columns = [c.upper() for c in frame.columns]
    events = v1.build_events(frame, submissions=subs.rename(columns=str.upper))
    admission = v2.build_admission(subs, _universe_frame([("NEW", 100, ["NEW"])]))
    feature = v2.feature_panel(events, admission, [100], _decisions("2015-03-10", "2015-04-02"), "A90")[100]
    assert np.isnan(feature.iloc[0])  # the filings still named OLD: abstain
    assert feature.iloc[1] > 0  # NEW named, two Form 4s in 730 days, the purchase in the window


# --- reported-only statistics ------------------------------------------------------------------


def _panel(feature, label, groups=None):
    n, e = feature.shape
    days = pd.bdate_range("2013-01-01", periods=n + 1, tz="UTC")
    return v2.TrialPanel(trial="A90|fwd20", window="discovery", horizon=20,
                         decision_at=[d.isoformat() for d in days[:-1]], label_end=[d.isoformat() for d in days[1:]],
                         entities=[f"T{i}" for i in range(e)], feature=feature, label=label,
                         groups=groups or ["7370-7379"] * e)


def test_delisting_bounds_order_the_ic_and_flag_a_sign_flip():
    rng = np.random.default_rng(2)
    feature = np.where(rng.random((60, 40)) < 0.2, 1.0, 0.0)
    label = 0.02 * feature + rng.normal(0, 0.05, feature.shape)
    missing = (feature > 0) & (rng.random(feature.shape) < 0.5)
    label[missing] = np.nan
    out = v2.delisting_sensitivity(_panel(feature, label))
    assert out["pessimistic"]["mean_ic"] < out["neutral"]["mean_ic"] < out["optimistic"]["mean_ic"]
    assert out["buyer_missing_labels"] == int(missing.sum()) and out["sign_survives_pessimistic"] is False
    clean = v2.delisting_sensitivity(_panel(feature, 0.02 * feature + rng.normal(0, 0.05, feature.shape)))
    assert clean["missing_labels"] == 0 and clean["sign_survives_pessimistic"] is True


def test_industry_neutral_ic_removes_a_group_level_effect():
    rng = np.random.default_rng(4)
    groups = ["3660-3679"] * 20 + ["7370-7379"] * 20
    feature = np.zeros((60, 40))
    feature[:, :20] = np.where(rng.random((60, 20)) < 0.4, 1.0, 0.0)  # buyers only in one group
    label = rng.normal(0, 0.05, feature.shape)
    label[:, :20] += 0.05  # that group outperforms, whoever buys
    panel = _panel(feature, label, groups)
    raw = v2._mean_ic(feature, label)
    neutral = v2.industry_neutral_ic(panel)["mean_ic"]
    assert raw > 0.15 and abs(neutral) < 0.05


def test_measure_trial_adds_the_negative_one_sided_p_from_the_same_draws():
    panels = _synthetic_trials(np.random.default_rng(8), ic=(0.0, -0.3, 0.0, 0.0))
    out = v2.measure_trial(panels["A90|fwd20"], perms=999, sensitivity=False)
    base = v1.measure_trial(panels["A90|fwd20"], perms=999, sensitivity=False)
    assert out["p"] == base["p"] and out["mean_ic"] == base["mean_ic"]
    assert out["mean_ic"] < 0 and out["p_one_sided_negative"] <= 0.01 and out["p_one_sided_positive"] > 0.9


# --- calibration and verdict (C3) ---------------------------------------------------------------


def _ledger(**by_trial):
    out = []
    for t in v1.trial_names():
        mean, p_neg = by_trial.get(t, (0.01, 0.6))
        two = min(1.0, 2 * min(p_neg, 1 - p_neg))
        out.append({"trial": t, "status": "tested", "mean_ic": mean, "p": two, "p_one_sided_negative": p_neg,
                    "p_one_sided_positive": 1 - p_neg, "selected": False})
    return out


def test_contrary_needs_a_holm_significant_negative_p_not_one_unadjusted_hit():
    ledger = _ledger(**{"A30|fwd5": (-0.05, 0.02)})  # two-sided 0.04: v1's CONTRARY
    calib = v2.calibration(ledger)
    assert calib["v1_rule"]["state"] == "CONTRARY"
    assert calib["state"] != "CONTRARY" and calib["contrary_trials"] == []
    strong = v2.calibration(_ledger(**{"A30|fwd5": (-0.05, 0.001)}))
    assert strong["state"] == "CONTRARY" and strong["contrary_trials"] == ["A30|fwd5"]
    verdict = v2.verdict({"calibration": strong, "ledger": []}, [], {"gate_passed": False})
    assert verdict["state"] == "MACHINERY_SUSPECT"


def test_absent_while_powered_is_no_longer_an_alarm_but_evidence_against_the_sign_is():
    absent = v2.calibration(_ledger(**{"A90|fwd20": (-0.002, 0.4)}))
    assert absent["state"] == "ABSENT" and not absent["primary_against_expectation"]
    powered = v2.verdict({"calibration": absent, "ledger": []}, [], {"gate_passed": True})
    assert powered["state"] == "NO_SURVIVOR" and powered["v1_rule_state"] == "MACHINERY_SUSPECT"
    against = v2.calibration(_ledger(**{"A90|fwd20": (-0.02, 0.03)}))
    assert against["primary_against_expectation"] and against["state"] == "ABSENT"
    assert v2.verdict({"calibration": against, "ledger": []}, [], None)["state"] == "MACHINERY_SUSPECT"


def test_machinery_alarm_false_positive_rate_under_the_synthetic_null():
    """The v2 alarm (CONTRARY or primary-against) stays near its union bound of 0.10 under the
    correlated-trial null; v1's (CONTRARY or ABSENT while powered) fires about half the time."""
    rng = np.random.default_rng(17)
    reps, v2_alarm, v1_alarm = 80, 0, 0
    for _ in range(reps):
        panels = _synthetic_trials(rng)
        ledger = [{"trial": t, **v2.measure_trial(panels[t], perms=499, sensitivity=False), "selected": False}
                  for t in v1.trial_names()]
        calib = v2.calibration(ledger)
        v2_alarm += calib["state"] == "CONTRARY" or calib["primary_against_expectation"]
        v1_alarm += calib["v1_rule"]["state"] in ("CONTRARY", "ABSENT")
    assert v2_alarm / reps <= 0.10 + 3 * np.sqrt(0.1 * 0.9 / reps)
    assert v1_alarm / reps >= 0.3


# --- registry, v1 supersession and the off-host witness ------------------------------------------


class _Vault:
    """A bare 'remote' standing in for the GitHub vault, seeded with v1's registration anchor."""

    def __init__(self, root: Path, v1_opened: bool = False) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.remote, self.worktree, self.cache = root / "remote.git", root / "worktree", root / "cache"
        _git(root, "init", "-q", "--bare", str(self.remote))
        _git(self.remote, "symbolic-ref", "HEAD", "refs/heads/main")
        _git(root, "init", "-q", str(self.worktree))
        _git(self.worktree, "checkout", "-q", "-b", "main")
        v1_file = self.worktree / v1.WITNESS_PATH
        v1_file.parent.mkdir(parents=True)
        v1_file.write_bytes(v1.REGISTERED_ANCHOR_LINE + b"\n")
        _git(self.worktree, "add", "-A")
        _git(self.worktree, "commit", "-q", "-m", "seed v1")
        if v1_opened:  # a later v1 anchor line (v1 opened a discovery)
            with open(v1_file, "ab") as stream:
                stream.write(b'{"head_sha256":"' + b"e" * 64 + b'","prev_anchor_sha256":"x","records":4,'
                             b'"run_at":"2026-10-01T00:00:00+00:00"}\n')
            _git(self.worktree, "add", "-A")
            _git(self.worktree, "commit", "-q", "-m", "v1 opened")
        _git(self.worktree, "remote", "add", "origin", str(self.remote))
        _git(self.worktree, "push", "-q", "origin", "HEAD:refs/heads/main")
        _git(root, "init", "-q", str(self.cache))

    def publish(self, log_dir, push=True):
        """The operator's step: append the v2 anchor lines, commit, push to main."""
        v2.export_anchors(log_dir, self.worktree)
        _git(self.worktree, "add", "-A")
        _git(self.worktree, "commit", "-q", "-m", "vs1 v2 anchors")
        if push:
            self.push()

    def push(self):
        _git(self.worktree, "push", "-q", "origin", "HEAD:refs/heads/main")

    def witness(self):
        return v2.check_offhost(self.cache, remote_url=str(self.remote))

    def v1_witness(self):
        return v1.check_offhost(self.cache, remote_url=str(self.remote))


def _register(log_dir):
    return v2.register(log_dir, v2.REGISTERED_AT, v2.REGISTERED_CODE_SHA)


def _inputs(manifest=None, **overrides):
    manifest = manifest or _manifest([])
    return {"sector": "Technology", "price_manifest_sha256": manifest.digest(),
            "probe_report_sha256": manifest.probe_report_sha256, "form4_sha256": "1" * 64,
            "submissions_sha256": "2" * 64, "issuer_map_sha256": "3" * 64, "power_sha256": "4" * 64,
            "sic_map_sha256": "5" * 64, "accept_underpowered": False, "as_of_ts": AS_OF_TS, **overrides}


def _observed(inputs):
    return {k: v for k, v in inputs.items() if k not in ("as_of_ts", "accept_underpowered")}


def _manifest(tickers, source="tiingo", **extra):
    return v2.PriceManifest(source=source, series_template="YF:{ticker}:close", basis="split+dividend adjusted",
                            benchmark="XLK", admitted=tuple(sorted(set(tickers) | {"XLK"})),
                            probe_report_sha256="a" * 64, **extra)


def _discovery_key(log_dir, vault, manifest=None):
    _register(log_dir)
    inputs = _inputs(manifest)
    v2.freeze_inputs(log_dir, NOW, inputs)
    v2.open_discovery(log_dir, NOW, _observed(inputs), vault.v1_witness())
    vault.publish(log_dir)
    return v2.resume_discovery(log_dir, _observed(inputs), vault.witness(), vault.v1_witness()), inputs


def test_register_rematerialises_only_the_pinned_registration(tmp_path):
    records = _register(tmp_path / "a")
    assert [r["kind"] for r in records] == ["header", "preregistration"]
    assert records[1]["supersedes"]["registry_head_sha256"] == v1.REGISTERED_RECORD_SHA256[1]
    assert records[1]["runs"]["vs1"]["alpha"] == pytest.approx(0.05)
    assert records[1]["sic_map_sha256"] == v2.SIC_MAP_SHA256
    check = v2.registry(tmp_path / "a").verify_chain()
    assert check["ok"] and check["head_sha256"] == v2.REGISTERED_RECORD_SHA256[1]
    assert (tmp_path / "a" / v2.REGISTRY_ANCHORS).read_bytes() == v2.REGISTERED_ANCHOR_LINE + b"\n"
    with pytest.raises(ValueError, match="already registered"):
        _register(tmp_path / "a")
    with pytest.raises(PermissionError, match="fork"):
        v2.register(tmp_path / "b", NOW, "c" * 40)
    forged = tmp_path / "forged"
    v2.registry(forged).append(v2.registration_records(NOW, v2.REGISTERED_CODE_SHA))
    with pytest.raises(PermissionError, match="fork"):
        v2.freeze_inputs(forged, NOW, _inputs())
    # a v1 registry is not a v2 registry (different files, header and pin)
    v1.register(tmp_path / "v1", v1.REGISTERED_AT, v1.REGISTERED_CODE_SHA)
    with pytest.raises(PermissionError, match="not registered"):
        v2.freeze_inputs(tmp_path / "v1", NOW, _inputs())


def test_v2_discovery_is_refused_once_v1_was_opened(tmp_path):
    vault = _Vault(tmp_path / "vault", v1_opened=True)
    _register(tmp_path / "reg")
    inputs = _inputs()
    v2.freeze_inputs(tmp_path / "reg", NOW, inputs)
    with pytest.raises(PermissionError, match="v1 was opened"):
        v2.open_discovery(tmp_path / "reg", NOW, _observed(inputs), vault.v1_witness())
    with pytest.raises(PermissionError, match="v1's pinned off-host witness"):
        v2.open_discovery(tmp_path / "reg", NOW, _observed(inputs), None)


def test_v2_discovery_needs_its_own_witness_on_main_and_runs_once(tmp_path):
    vault = _Vault(tmp_path / "vault")
    log_dir = tmp_path / "reg"
    _register(log_dir)
    inputs = _inputs()
    v2.freeze_inputs(log_dir, NOW, inputs)
    v2.open_discovery(log_dir, NOW, _observed(inputs), vault.v1_witness())
    with pytest.raises(PermissionError, match="is not on main"):
        vault.witness()
    vault.publish(log_dir, push=False)
    with pytest.raises(PermissionError, match="is not on main"):
        vault.witness()
    vault.push()
    key = v2.resume_discovery(log_dir, _observed(inputs), vault.witness(), vault.v1_witness())
    assert isinstance(key, v2.DiscoveryKey)
    with pytest.raises(PermissionError, match="one shot"):
        v2.open_discovery(log_dir, NOW, _observed(inputs), vault.v1_witness())
    with pytest.raises(PermissionError, match="inputs differ"):
        v2.resume_discovery(log_dir, _observed(_inputs(form4_sha256="9" * 64)), vault.witness(), vault.v1_witness())
    # a v1 key never opens v2 prices and vice versa
    with pytest.raises(TypeError):
        v1.DiscoveryKey(object(), "x", {"as_of_ts": AS_OF_TS}, log_dir=log_dir, witness_tip="t")


def test_price_reader_needs_a_v2_key_and_the_frozen_manifest(tmp_path):
    dates = pd.bdate_range("2019-12-02", "2019-12-31")
    engine = _price_db({"XLK": pd.Series(100.0, index=dates), "AAA": pd.Series(10.0, index=dates)})
    vault = _Vault(tmp_path / "vault")
    key, _ = _discovery_key(tmp_path / "reg", vault, _manifest(["AAA"]))
    read = {"start": date(2019, 12, 1), "as_of": date(2019, 12, 31), "window": "discovery"}
    with engine.connect() as conn:
        with pytest.raises(PermissionError, match="v2 DiscoveryKey"):
            v2.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], key=None, **read)
        with pytest.raises(PermissionError, match="inputs_frozen"):
            v2.load_price_panel(conn, _manifest(["AAA"], listed_from=(("AAA", "2010-01-04"),)), ["AAA"],
                                key=key, **read)
        with pytest.raises(PermissionError):
            v2.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], key=key, start=date(2019, 12, 1),
                                as_of=date(2020, 1, 2), window="discovery")
        panel = v2.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], key=key, **read)
    assert panel.receipt["series"]["AAA"]["last"] == "2019-12-31"
    assert [r["kind"] for r in v2.registry(tmp_path / "reg").read_all()][-1] == "prices_read"


def test_manifest_listed_from_must_name_admitted_tickers(tmp_path):
    with pytest.raises(ValueError, match="not admitted"):
        _manifest(["AAA"], listed_from=(("BBB", "2015-01-02"),)).validate()
    path = tmp_path / "manifest.json"
    manifest = _manifest(["AAA"], listed_from=(("AAA", "2015-01-02"),))
    path.write_text(json.dumps({**asdict(manifest), "admitted": list(manifest.admitted),
                                "listed_from": {"AAA": "2015-01-02"}}))
    assert v2.PriceManifest.from_file(path).digest() == manifest.digest()


# --- end to end: planted effect, the ticker rule and the listing cross-check ---------------------


def _holdings(ciks, dates, ticker_of):
    return [{**_submission(accession_number=f"act-{cik}-{d.date()}", issuer_cik=str(cik), filing_date=str(d.date()),
                           document_type="4", owner_cik="9"), "issuer_ticker": ticker_of(cik, d)}
            for cik in ciks for d in dates[::40]]


def test_end_to_end_planted_effect_with_admission_through_the_v2_price_reader(tmp_path):
    rng = np.random.default_rng(12)
    tickers = [f"T{i:02d}" for i in range(26)]
    ciks = list(range(1000, 1026))
    dates = pd.bdate_range("2010-01-04", "2026-06-30")
    n = len(dates)
    rets = rng.normal(0.0, 0.01, (n, len(tickers)))
    rows = []
    for j, cik in enumerate(ciks):
        for i in np.flatnonzero(rng.random(n) < 0.004):
            rows.append(_row(accession_number=f"p-{cik}-{i}", issuer_cik=str(cik), owner_cik=str(int(rng.integers(1, 6))),
                             filing_date=str(dates[min(i + 1, n - 1)].date()), transaction_date=str(dates[i].date())))
            rets[min(i + 2, n - 1):min(i + 22, n), j] += 0.004
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=dates, columns=tickers)
    prices["XLK"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.008, n)))
    engine = _price_db({c: prices[c] for c in prices.columns})
    switch = pd.Timestamp("2016-01-04")
    # issuer 1025 filed under another ticker until 2016: its earlier issuer-dates abstain

    def ticker_of(cik, d):
        return "OLDT" if cik == 1025 and d < switch else tickers[cik - 1000]

    subs = [{**_submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                           issuer_cik=r["issuer_cik"], owner_cik=r["owner_cik"]),
             "issuer_ticker": ticker_of(int(r["issuer_cik"]), pd.Timestamp(r["filing_date"]))} for r in rows]
    subs += _holdings(ciks, dates, ticker_of)
    tx = pd.DataFrame(rows).astype("string")
    tx.columns = [c.upper() for c in tx.columns]
    submissions = pd.DataFrame(subs)
    events = v1.build_events(tx, submissions=submissions.rename(columns=str.upper))
    universe = _universe_frame([(t, c, [t]) for t, c in zip(tickers, ciks)])
    admission = v2.build_admission(submissions, universe)
    manifest = _manifest(tickers, listed_from=(("T00", "2014-01-02"),))
    vault = _Vault(tmp_path / "vault")
    key, inputs = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        panel = v2.load_price_panel(conn, manifest, tickers, start=date(2011, 11, 1), as_of=date(2019, 12, 31),
                                    window="discovery", key=key)
    panels = v2.build_trial_panels(events, admission, universe, panel, "discovery")
    primary = panels["A90|fwd20"]
    decided = pd.DatetimeIndex(primary.decision_at)
    assert np.isnan(primary.feature[decided < pd.Timestamp("2015-12-01", tz="UTC"), 25]).all()
    assert np.isfinite(primary.feature[decided > pd.Timestamp("2016-06-01", tz="UTC"), 25]).all()
    assert np.isnan(primary.feature[decided < pd.Timestamp("2013-12-01", tz="UTC"), 0]).all()
    frozen = v2.discover_panel(v2.run_spec("e2e"), panels, sensitivity=True,
                               inputs={"inputs_frozen_sha256": key.inputs_frozen_sha256})
    v2.seal_discovery(tmp_path / "reg", NOW, key, frozen)
    ledger = {t["trial"]: t for t in frozen["payload"]["ledger"]}
    assert ledger["A90|fwd20"]["selected"] and ledger["A90|fwd20"]["mean_ic"] > 0
    assert frozen["payload"]["calibration"]["state"] == "CONSISTENT"
    assert "delisting_sensitivity" in ledger["A90|fwd20"] and "industry_neutral" in ledger["A90|fwd20"]
    # holdout: flag, pinned hash, the chain, the witness
    with pytest.raises(PermissionError, match="allow_holdout"):
        v2.open_holdout(frozen, allow_holdout=False, prereg_sha256=v2.PREREG_BODY_SHA256, log_dir=tmp_path / "reg",
                        now=NOW, observed=_observed(inputs))
    with pytest.raises(PermissionError, match="pinned v2"):
        v2.open_holdout(frozen, allow_holdout=True, prereg_sha256=v1.PREREG_BODY_SHA256, log_dir=tmp_path / "reg",
                        now=NOW, observed=_observed(inputs))
    v2.open_holdout(frozen, allow_holdout=True, prereg_sha256=v2.PREREG_BODY_SHA256, log_dir=tmp_path / "reg",
                    now=NOW, observed=_observed(inputs))
    vault.publish(tmp_path / "reg")
    hkey = v2.resume_holdout(frozen, allow_holdout=True, prereg_sha256=v2.PREREG_BODY_SHA256, log_dir=tmp_path / "reg",
                             observed=_observed(inputs), witness=vault.witness())
    with engine.connect() as conn:
        holdout = v2.load_price_panel(conn, manifest, tickers, start=date(2019, 11, 1), as_of=date(2026, 6, 30),
                                      window="holdout", key=hkey)
    result = v2.evaluate_panel_holdout(frozen, v2.build_trial_panels(events, admission, universe, holdout, "holdout"),
                                       hkey, power={"gate_passed": True})
    assert result["verdict"]["state"] == "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    v2.seal_holdout(tmp_path / "reg", NOW, hkey, result)
    kinds = [r["kind"] for r in v2.registry(tmp_path / "reg").read_all()]
    assert kinds == ["header", "preregistration", "inputs_frozen", "discovery_opened", "prices_read",
                     "discovery_frozen", "holdout_opened", "prices_read", "holdout_result"]


# --- Stage 0 ----------------------------------------------------------------------------------


def test_power_file_must_be_a_v2_file_at_the_v1_settings():
    power = {**_power_file(gate_power=0.6), "version": v2.VERSION}
    v2.verify_power(power)
    with pytest.raises(ValueError, match="v2 harness"):
        v2.verify_power(_power_file(gate_power=0.6))
    with pytest.raises(ValueError, match="pre-registered settings"):
        v2.verify_power({**power, "settings": {**v1.power_settings(), "sims": 30}})


def test_admission_report_counts_admitted_issuers_and_events_per_year():
    tx = [_row(accession_number=f"p{i}", issuer_cik="100", filing_date=f"{2013 + i}-03-04",
               transaction_date=f"{2013 + i}-03-01") for i in range(3)]
    subs = _subs([{"accession_number": f"p{i}", "filing_date": f"{2013 + i}-03-04", "issuer_ticker": "AAA"}
                  for i in range(3)] + [{"accession_number": "h0", "filing_date": "2012-06-01", "issuer_ticker": "AAA"}])
    frame = pd.DataFrame(tx).astype("string")
    frame.columns = [c.upper() for c in frame.columns]
    events = v1.build_events(frame, submissions=subs.rename(columns=str.upper))
    universe = _universe_frame([("AAA", 100, ["AAA"]), ("BBB", 200, ["BBB"])])
    report = v2.admission_report(events, v2.build_admission(subs, universe), universe)
    assert report["universe_issuers"] == 2 and report["admitted_issuers"] == 1
    assert report["purchase_events_by_year"] == {2013: 1, 2014: 1, 2015: 1}
    assert report["admitted_purchase_events"] == 3


# --- the SIC fetcher (no network) --------------------------------------------------------------


def test_sic_fetcher_is_resumable_and_derives_the_map(tmp_path, monkeypatch):
    from scripts import fetch_sec_issuer_sic as fetcher

    subs = tmp_path / "submissions.parquet"
    pd.DataFrame({"issuer_cik": ["0000000100", "200", "300", "100", "x"]}).to_parquet(subs)
    bodies = {100: json.dumps({"cik": "100", "name": "Soft Inc", "sic": "7372", "sicDescription": "Prepackaged",
                               "entityType": "operating", "tickers": ["soft"], "exchanges": ["Nasdaq"],
                               "formerNames": [{"name": "Old Soft", "from": "2001-01-01T00:00:00.000Z",
                                                "to": "2015-06-30T00:00:00.000Z"}]}).encode()}
    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        cik = int(url.rsplit("CIK", 1)[1].split(".")[0])
        if cik == 300:
            return 503, b""
        return (200, bodies[cik]) if cik in bodies else (404, b"")

    monkeypatch.setattr(fetcher, "_get", fake_get)
    monkeypatch.setattr(fetcher.time, "sleep", lambda s: None)
    counts = fetcher.fetch(subs, tmp_path / "out", rate=8.0, retries=1, workers=2)
    assert counts == {"issuer_ciks": 3, "todo": 3, "200": 1, "404": 1, "other": 1}
    assert len([c for c in calls if c.endswith("CIK0000000300.json")]) == 2  # retried, then non-final
    calls.clear()
    counts = fetcher.fetch(subs, tmp_path / "out", rate=8.0, retries=0)
    assert counts["todo"] == 1 and calls == ["https://data.sec.gov/submissions/CIK0000000300.json"]
    with pytest.raises(SystemExit, match="ceiling"):
        fetcher.fetch(subs, tmp_path / "out", rate=9.0)
    receipt = fetcher.derive(tmp_path / "out", subs)
    rows = [json.loads(line) for line in (tmp_path / "out" / fetcher.MAP_NAME).read_text().splitlines()]
    soft = next(r for r in rows if r["cik"] == 100)
    assert soft["sic"] == 7372 and soft["tickers"] == ["SOFT"]
    assert soft["former_names"] == [{"name": "Old Soft", "from": "2001-01-01", "to": "2015-06-30"}]
    assert soft["body_sha256"] == hashlib.sha256(bodies[100]).hexdigest()
    assert receipt["counts"]["status_404"] == 1 and receipt["counts"]["status_other"] == 1
    assert "CURRENT" in receipt["caveat"]
    stored = gzip.decompress((tmp_path / "out" / fetcher.JSON_DIR / "CIK0000000100.json.gz").read_bytes())
    assert stored == bodies[100]
    with pytest.raises(SystemExit, match="write-once"):
        fetcher.derive(tmp_path / "out")
    sic_map = v2.load_sic_map(tmp_path / "out" / fetcher.MAP_NAME, pinned=None)
    assert sic_map.set_index("cik")["sic"].to_dict()[100] == 7372


# --- CLI ------------------------------------------------------------------------------------------


def test_cli_power_writes_the_stage0_files_with_the_pinned_inputs(tmp_path, monkeypatch):
    from scripts import run_vs1_v2_insider_density as cli

    rng = np.random.default_rng(31)
    tickers = [f"T{i:02d}" for i in range(22)]
    ciks = list(range(1000, 1022))
    dates = pd.bdate_range("2011-10-03", "2019-12-31")
    rows = [_row(accession_number=f"p-{c}-{i}", issuer_cik=str(c), owner_cik="7",
                 filing_date=str(dates[min(i + 1, len(dates) - 1)].date()), transaction_date=str(dates[i].date()))
            for c in ciks for i in np.flatnonzero(rng.random(len(dates)) < 0.004)]
    subs = ([{**_submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                            issuer_cik=r["issuer_cik"], owner_cik="7"), "issuer_ticker": tickers[int(r["issuer_cik"]) - 1000]}
             for r in rows] + _holdings(ciks, dates, lambda c, d: tickers[c - 1000]))
    files = {n: tmp_path / n for n in ("form4.parquet", "submissions.parquet", "company_tickers.json", "sic.jsonl")}
    pd.DataFrame(rows).to_parquet(files["form4.parquet"])
    pd.DataFrame(subs).to_parquet(files["submissions.parquet"])
    files["company_tickers.json"].write_text(json.dumps(
        {str(k): {"cik_str": c, "ticker": t, "title": t} for k, (t, c) in enumerate(zip(tickers, ciks))}))
    files["sic.jsonl"].write_text("".join(json.dumps({"cik": c, "sic": 7372, "name": t, "tickers": [t],
                                                      "former_names": [], "http_status": 200}) + "\n"
                                          for t, c in zip(tickers, ciks)))
    monkeypatch.setattr(v1, "load_sector_map", lambda repo_root=None: _map([("Technology", "a", "T00", 0.1, "company")]))
    monkeypatch.setattr(v2, "ISSUER_MAP_SHA256", v1.data_sha256(files["company_tickers.json"]))
    monkeypatch.setattr(v2, "SIC_MAP_SHA256", v1.data_sha256(files["sic.jsonl"]))
    seen = {}

    def fake_power(features):
        seen["shapes"] = {k: v.shape for k, v in features.items()}
        return {**_power_file(gate_power=0.1), "version": v2.VERSION}

    monkeypatch.setattr(v2, "stage0_power", fake_power)
    out = tmp_path / "power"
    cli.main(["power", "--form4", str(files["form4.parquet"]), "--submissions", str(files["submissions.parquet"]),
              "--issuer-map", str(files["company_tickers.json"]), "--sic-map", str(files["sic.jsonl"]),
              "--out", str(out)])
    universe = json.loads((out / "universe.json").read_text())
    assert universe["universe"] == 22 and universe["by_source"] == {"both": 1, "sic": 21}
    assert set(seen["shapes"]) == set(v1.trial_names()) and seen["shapes"]["A90|fwd20"][1] == 22
    report = json.loads((out / "admission-report.json").read_text())
    assert report["admitted_issuers"] == 22 and sum(report["purchase_events_by_year"].values()) > 0
    power = json.loads((out / "power.json").read_text())
    assert power["inputs"]["prereg_sha256"] == v2.PREREG_BODY_SHA256
    with pytest.raises(FileExistsError):
        cli.main(["power", "--form4", str(files["form4.parquet"]), "--submissions", str(files["submissions.parquet"]),
                  "--issuer-map", str(files["company_tickers.json"]), "--sic-map", str(files["sic.jsonl"]),
                  "--out", str(out)])


def test_cli_has_no_power_override(capsys):
    from scripts import run_vs1_v2_insider_density as cli

    with pytest.raises(SystemExit):
        cli.main(["power", "--form4", "f", "--submissions", "s", "--issuer-map", "m", "--sic-map", "x",
                  "--out", "o", "--sims", "30"])
    assert "unrecognized arguments: --sims" in capsys.readouterr().err
