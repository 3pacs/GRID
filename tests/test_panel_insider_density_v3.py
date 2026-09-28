"""Tests for the VS1 v3 harness (``analysis/panel_insider_density_v3.py``): v2 with ``A90|fwd5`` primary.

Synthetic data only: no production DB, no price or outcome of any real issuer, no network.
"""

from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from tests.test_panel_insider_density import AS_OF_TS, NOW, _power_file, _price_db, _row, _submission, _synthetic_trials
from tests.test_panel_insider_density_v2 import (
    _SEEDS,
    _discovery_key,
    _holdings,
    _inputs,
    _manifest,
    _observed,
    _register,
    _universe_frame,
    _Vault,
)


def _vault(root, **kwargs):
    return _Vault(root, h=v3, seeds=("vs1-v1", "vs1-v2"), **kwargs)


# --- pins -------------------------------------------------------------------------------------


def test_v3_preregistration_hashes_to_the_pinned_body_sha():
    assert v3.check_prereg() == v3.PREREG_BODY_SHA256
    assert len({v1.PREREG_BODY_SHA256, v2.PREREG_BODY_SHA256, v3.PREREG_BODY_SHA256}) == 3


def test_v3_changes_only_the_primary_trial():
    assert v3.PRIMARY_TRIAL == "A90|fwd5" and v3.V3.primary == "A90|fwd5"
    assert v2.PRIMARY_TRIAL == v1.PRIMARY_TRIAL == "A90|fwd20"
    assert set(v3.SECONDARY_TRIALS) == set(v1.trial_names()) - {"A90|fwd5"}
    assert v3.run_spec().trials == v1.trial_names() and v3.run_spec().alpha == pytest.approx(0.05)
    assert (v3.ISSUER_MAP_SHA256, v3.SIC_MAP_SHA256) == (v2.ISSUER_MAP_SHA256, v2.SIC_MAP_SHA256)
    assert v3.v2_universe is v2.v2_universe and v3.build_admission is v2.build_admission


def test_the_v3_registry_and_witness_are_their_own():
    assert v3.WITNESS_PATH == "05-GRID/Paper-Log/vs1/granular_panel_prereg_v3.anchors.jsonl"
    assert v3.WITNESS_REMOTE_URL == "https://github.com/3pacs/obsidian-vault.git" and v3.WITNESS_BRANCH == "main"
    assert len({v1.REGISTRY_LOG, v2.REGISTRY_LOG, v3.REGISTRY_LOG}) == 3
    records = v3.registration_records(v3.REGISTERED_AT, v3.REGISTERED_CODE_SHA)
    heads = v1.chained_sha256(records)
    assert tuple(heads) == v3.REGISTERED_RECORD_SHA256
    assert json.loads(v3.REGISTERED_ANCHOR_LINE) == {
        "head_sha256": heads[1], "prev_anchor_sha256": None, "records": 2, "run_at": v3.REGISTERED_AT.isoformat()}
    record = records[1]
    assert record["runs"]["vs1"]["primary_trial"] == "A90|fwd5"
    assert [s["version"] for s in record["supersedes"]] == ["vs1-v1", "vs1-v2"]
    assert record["supersedes"][1]["registry_head_sha256"] == v2.REGISTERED_RECORD_SHA256[1]


def test_v1_v2_and_v3_are_pinned_as_superseded_by_the_registered_v4():
    from analysis import panel_insider_density_v4 as v4

    for pin in (v1.SUPERSEDED_BY, v2.SUPERSEDED_BY, v3.SUPERSEDED_BY):
        assert pin == {"version": "vs1-v4", "prereg_sha256": v4.PREREG_BODY_SHA256,
                       "registry_head_sha256": v4.REGISTERED_RECORD_SHA256[1]}
    assert v4.SUPERSEDED_BY is None


@pytest.fixture(autouse=True)
def _v3_not_superseded(request, monkeypatch):
    """v3 is superseded by v4 (pinned); the machinery tests exercise v3 as if it were current."""
    if request.node.name != "test_v1_v2_and_v3_are_pinned_as_superseded_by_the_registered_v4":
        monkeypatch.setattr(v3, "SUPERSEDED_BY", None)


def test_the_prereg_states_the_primary_choice_and_history():
    body = v1.prereg_body((v3.REPO / v3.PREREG_PATH).read_text(encoding="utf-8"))
    for pin in (v1.PREREG_BODY_SHA256, v2.PREREG_BODY_SHA256, v1.REGISTERED_RECORD_SHA256[1],
                v2.REGISTERED_RECORD_SHA256[1], v2.SIC_MAP_SHA256, v2.ISSUER_MAP_SHA256):
        assert pin in body
    assert r"| `A90\|fwd5` | A90 | 5 sessions | **primary** |" in body
    assert "0.665" in body and "0.675" in body


# --- statistics on the v3 primary ------------------------------------------------------------------


def _ledger(**by_trial):
    out = []
    for t in v1.trial_names():
        mean, p_neg = by_trial.get(t, (0.01, 0.6))
        out.append({"trial": t, "status": "tested", "mean_ic": mean, "p": min(1.0, 2 * min(p_neg, 1 - p_neg)),
                    "p_one_sided_negative": p_neg, "p_one_sided_positive": 1 - p_neg, "selected": False})
    return out


def test_calibration_and_verdict_use_the_v3_primary():
    against = v3.calibration(_ledger(**{"A90|fwd5": (-0.02, 0.03)}))
    assert against["primary_trial"] == "A90|fwd5" and against["primary_against_expectation"]
    assert v3.verdict({"calibration": against, "ledger": []}, [], None)["state"] == "MACHINERY_SUSPECT"
    # the same numbers on the v2 primary raise nothing under v3
    other = v3.calibration(_ledger(**{"A90|fwd20": (-0.02, 0.03)}))
    assert not other["primary_against_expectation"]
    assert v2.calibration(_ledger(**{"A90|fwd20": (-0.02, 0.03)}))["primary_against_expectation"]
    consistent = v3.calibration(_ledger(**{"A90|fwd5": (0.02, 0.95)}))
    assert consistent["state"] == "CONSISTENT"


def test_stage0_gate_is_on_the_v3_primary(monkeypatch):
    rows = {"A90|fwd5": 0.665, "A30|fwd5": 0.675, "A90|fwd20": 0.16, "A30|fwd20": 0.12}
    table = {t: [{"target_ic": ic, "power": rows[t] if ic == 0.01 else 1.0, "sims": 200, "usable_dates": 400}
                 for ic in v1.POWER_TARGET_ICS] for t in v1.trial_names()}
    monkeypatch.setattr(v1, "planted_power", lambda feature, ic, **kw: next(
        r for r in table[feature] if r["target_ic"] == ic))
    power = v3.stage0_power({t: t for t in v1.trial_names()})
    assert power["gate_passed"] is True and power["primary_trial"] == "A90|fwd5"
    v3.verify_power(json.loads(json.dumps(power)))
    v2_power = v2.stage0_power({t: t for t in v1.trial_names()})
    assert v2_power["gate_passed"] is False
    with pytest.raises(ValueError, match="vs1-v3 harness"):
        v3.verify_power(v2_power)
    with pytest.raises(ValueError, match="gate_passed"):
        v3.verify_power({**json.loads(json.dumps(power)), "gate_passed": False})


def test_power_file_of_v3_needs_the_v3_version_and_primary():
    power = {**_power_file(gate_power=0.6), "version": v3.VERSION, "primary_trial": "A90|fwd5"}
    power["table"]["A90|fwd5"][0]["power"] = 0.6
    v3.verify_power(power)
    with pytest.raises(ValueError, match="primary trial"):
        v3.verify_power({**power, "primary_trial": "A90|fwd20"})


# --- supersession ------------------------------------------------------------------------------------


def test_v3_opens_only_while_v1_and_v2_stay_unopened(tmp_path):
    for opened in (("vs1-v1",), ("vs1-v2",)):
        vault = _vault(tmp_path / f"vault-{opened[0]}", opened=opened)
        log_dir = tmp_path / f"reg-{opened[0]}"
        _register(log_dir, v3)
        vault.publish(log_dir)
        inputs = _inputs()
        v3.freeze_inputs(log_dir, NOW, inputs)
        with pytest.raises(PermissionError, match="VS1 registry was opened"):
            v3.open_discovery(log_dir, NOW, _observed(inputs), vault.witness())


def test_v3_needs_the_v2_witness_on_main(tmp_path):
    vault = _Vault(tmp_path / "vault", h=v3, seeds=("vs1-v1",))
    log_dir = tmp_path / "reg"
    _register(log_dir, v3)
    vault.publish(log_dir)
    with pytest.raises(PermissionError, match="granular_panel_prereg_v2.anchors.jsonl is not on main"):
        vault.witness()


def test_v3_discovery_key_is_v3_only(tmp_path):
    vault = _vault(tmp_path / "vault")
    key, inputs = _discovery_key(tmp_path / "reg", vault)
    assert key.version == "vs1-v3"
    assert vault.witness().earlier == {"vs1-v1": 2, "vs1-v2": 2}
    dates = pd.bdate_range("2019-12-02", "2019-12-31")
    engine = _price_db({"XLK": pd.Series(100.0, index=dates)})
    with engine.connect() as conn:
        with pytest.raises(PermissionError, match="vs1-v2 DiscoveryKey"):
            v2.load_price_panel(conn, _manifest([]), [], key=key, start=date(2019, 12, 1), as_of=date(2019, 12, 31),
                                window="discovery")
        v3.load_price_panel(conn, _manifest([]), [], key=key, start=date(2019, 12, 1), as_of=date(2019, 12, 31),
                            window="discovery")
    with pytest.raises(PermissionError, match="one shot"):
        v3.open_discovery(tmp_path / "reg", NOW, _observed(inputs), vault.witness())


def test_v3_register_refuses_forks(tmp_path):
    records = _register(tmp_path / "a", v3)
    assert v3.registry(tmp_path / "a").verify_chain()["head_sha256"] == v3.REGISTERED_RECORD_SHA256[1]
    assert records[0]["version"] == "vs1-v3"
    with pytest.raises(PermissionError, match="fork"):
        v3.register(tmp_path / "b", NOW, "c" * 40)
    v2.register(tmp_path / "v2", v2.REGISTERED_AT, v2.REGISTERED_CODE_SHA)
    with pytest.raises(PermissionError, match="not registered"):
        v3.freeze_inputs(tmp_path / "v2", NOW, _inputs())


# --- end to end with the v3 primary ------------------------------------------------------------------


def test_end_to_end_planted_5_session_effect_is_the_v3_primary_survivor(tmp_path):
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

    def ticker_of(cik, d):
        return tickers[cik - 1000]

    subs = [{**_submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                           issuer_cik=r["issuer_cik"], owner_cik=r["owner_cik"]),
             "issuer_ticker": ticker_of(int(r["issuer_cik"]), None)} for r in rows]
    subs += _holdings(ciks, dates, ticker_of)
    tx = pd.DataFrame(rows).astype("string")
    tx.columns = [c.upper() for c in tx.columns]
    submissions = pd.DataFrame(subs)
    events = v1.build_events(tx, submissions=submissions.rename(columns=str.upper))
    universe = _universe_frame([(t, c, [t]) for t, c in zip(tickers, ciks)])
    admission = v3.build_admission(submissions, universe)
    manifest = _manifest(tickers)
    vault = _vault(tmp_path / "vault")
    key, inputs = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        panel = v3.load_price_panel(conn, manifest, tickers, start=date(2011, 11, 1), as_of=date(2019, 12, 31),
                                    window="discovery", key=key)
    frozen = v3.discover_panel(v3.run_spec("e2e"), v3.build_trial_panels(events, admission, universe, panel, "discovery"),
                               sensitivity=False, inputs={"inputs_frozen_sha256": key.inputs_frozen_sha256})
    payload = frozen["payload"]
    assert payload["primary_trial"] == "A90|fwd5" and payload["version"] == "vs1-v3"
    ledger = {t["trial"]: t for t in payload["ledger"]}
    assert ledger["A90|fwd5"]["primary"] and ledger["A90|fwd5"]["selected"]
    v3.seal_discovery(tmp_path / "reg", NOW, key, frozen)
    v3.open_holdout(frozen, allow_holdout=True, prereg_sha256=v3.PREREG_BODY_SHA256, log_dir=tmp_path / "reg",
                    now=NOW, observed=_observed(inputs), witness=vault.witness())
    vault.publish(tmp_path / "reg")
    hkey = v3.resume_holdout(frozen, allow_holdout=True, prereg_sha256=v3.PREREG_BODY_SHA256,
                             log_dir=tmp_path / "reg", observed=_observed(inputs), witness=vault.witness())
    with engine.connect() as conn:
        holdout = v3.load_price_panel(conn, manifest, tickers, start=date(2019, 11, 1), as_of=date(2026, 6, 30),
                                      window="holdout", key=hkey)
    result = v3.evaluate_panel_holdout(frozen, v3.build_trial_panels(events, admission, universe, holdout, "holdout"),
                                       hkey, power={"gate_passed": True})
    assert "A90|fwd5" in [c["trial"] for c in result["holdout_checks"]]
    assert result["verdict"]["state"] == "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    assert result["verdict"]["primary_trial"] == "A90|fwd5"
    v3.seal_holdout(tmp_path / "reg", NOW, hkey, result)


# --- CLI -------------------------------------------------------------------------------------------


def test_v3_cli_is_the_v2_cli_driven_by_the_v3_harness(capsys):
    from scripts import run_vs1_v3_insider_density as cli

    cli.main(["hash-prereg"])
    out = json.loads(capsys.readouterr().out)
    assert out["pinned"] == v3.PREREG_BODY_SHA256 and out["matches_pinned"] is True
    with pytest.raises(SystemExit):
        cli.main(["power", "--form4", "f", "--submissions", "s", "--issuer-map", "m", "--sic-map", "x",
                  "--out", "o", "--sims", "30"])
    assert "unrecognized arguments: --sims" in capsys.readouterr().err


def test_seeds_cover_every_earlier_version():
    assert set(_SEEDS) == {e.version for e in v3.V3.pins.earlier}
    assert AS_OF_TS and _synthetic_trials
