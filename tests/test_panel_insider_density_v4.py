"""Tests for the VS1 v4 harness (``analysis/panel_insider_density_v4.py``).

Synthetic data only: no production DB, no network, no price or outcome of any real
issuer, no TwelveData call. The cross-check function is exercised on synthetic
price paths only.
"""

from __future__ import annotations

import json
from dataclasses import asdict, replace

import numpy as np
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis import panel_insider_density_v4 as v4
from tests.test_panel_insider_density import NOW
from tests.test_panel_insider_density_v2 import _discovery_key, _inputs, _observed, _register, _Vault


def _vault(root, **kw):
    return _Vault(root, h=v4, seeds=("vs1-v1", "vs1-v2"), **kw)


@pytest.fixture(autouse=True)
def _v4_not_superseded(request, monkeypatch):
    """v4 is superseded by v5 (pinned); the machinery tests exercise v4 as if it were current."""
    if request.node.name != "test_v4_is_superseded_by_v5":
        monkeypatch.setattr(v4, "SUPERSEDED_BY", None)


# --- pins and text ---------------------------------------------------------------------------------


def test_v4_prereg_hashes_to_the_pin_and_states_the_rule():
    assert v4.check_prereg() == v4.PREREG_BODY_SHA256
    body = v1.prereg_body((v4.REPO / v4.PREREG_PATH).read_text(encoding="utf-8"))
    for text in ("**n ≥ N = 250**", "**≥ X = 99%**", "Y = 10 basis points (0.0010)", "TWELVEDATA_API_KEY",
                 "adjust=all", "adjust=none", "more than 10% of its pairs excluded"):
        assert text in body, text
    for pin in (v3.PREREG_BODY_SHA256, v3.REGISTERED_RECORD_SHA256[1], v2.SIC_MAP_SHA256):
        assert pin in body


def test_v4_changes_only_the_price_rule_and_gates():
    assert v4.PRIMARY_TRIAL == v3.PRIMARY_TRIAL == "A90|fwd5"
    assert v4.V4.pins.number == 4 and v4.V4.pins.holdout_probe_required and v4.POST_ADMISSION_POWER
    assert v4.WITNESS_PATH == v1.canonical_witness_path("vs1-v4")
    assert [e.version for e in v4.V4.pins.earlier] == ["vs1-v1", "vs1-v2", "vs1-v3"]
    rule = v4.CROSSCHECK
    assert (rule.min_share_within, rule.tolerance, rule.min_pairs, rule.max_excluded_share) == (0.99, 0.0010, 250, 0.10)
    records = v4.registration_records(v4.REGISTERED_AT, v4.REGISTERED_CODE_SHA)
    assert tuple(v1.chained_sha256(records)) == v4.REGISTERED_RECORD_SHA256
    assert records[1]["price_admission"]["crosscheck"]["N_min_pairs"] == 250


def test_v4_is_superseded_by_v5():
    assert v3.SUPERSEDED_BY["version"] == v4.SUPERSEDED_BY["version"] == "vs1-v5"
    with pytest.raises(PermissionError, match="superseded by vs1-v5"):
        v1.refuse_superseded(4, v4.SUPERSEDED_BY)


# --- the price manifest --------------------------------------------------------------------------------


def _manifest(**over):
    base = dict(source="TIINGO", series_template="YF:{ticker}:adj_close", basis="split+dividend adjusted",
                benchmark="XLK", admitted=("AAA", "XLK"), probe_report_sha256="a" * 64,
                crosscheck_report_sha256="c" * 64)
    return v4.PriceManifest(**{**base, **over})


@pytest.mark.parametrize("over,match", [
    ({"source": "yfinance"}, "refused|TIINGO"), ({"source": "KAGGLE_BULK"}, "refused|TIINGO"),
    ({"source": "TWELVEDATA"}, "TIINGO"), ({"series_template": "YF:{ticker}:close"}, "adj_close"),
    ({"crosscheck_report_sha256": ""}, "cross-check"), ({"basis": "split adjusted"}, "basis"),
])
def test_the_manifest_admits_only_tiingo_adj_close_with_a_crosscheck(over, match):
    _manifest().validate()
    with pytest.raises(ValueError, match=match):
        _manifest(**over).validate()


def test_the_manifest_digest_pins_the_crosscheck_report(tmp_path):
    m = _manifest()
    assert m.digest() != replace(m, crosscheck_report_sha256="d" * 64).digest()
    path = tmp_path / "m.json"
    path.write_text(json.dumps({**asdict(m), "admitted": list(m.admitted), "listed_from": {}}))
    assert v4.PriceManifest.from_file(path).digest() == m.digest()


# --- the cross-check statistic (synthetic paths only) ------------------------------------------------------


def _paths(n=400, seed=1, noise_bp=0.0, dividend_every=0, td_dividend_shift=0, drop=()):
    rng = np.random.default_rng(seed)
    sessions = [f"2015-{(i // 28) % 12 + 1:02d}-{i % 28 + 1:02d}T{i:05d}" for i in range(n)]
    close = 50 * np.exp(np.cumsum(rng.normal(0, 0.02, n)))
    factor = np.ones(n)
    if dividend_every:
        for i in range(dividend_every, n, dividend_every):
            factor[:i] *= 0.99  # back-adjusted distribution at i
    t_adj = close * factor
    td_factor = factor.copy()
    if td_dividend_shift:
        td_factor = np.roll(td_factor, td_dividend_shift)
    td_close = close * (1 + rng.normal(0, noise_bp / 1e4, n))  # the second vendor's print (raw and adjusted alike)
    td_adj = td_close * td_factor
    tiingo_adj = {d: float(p) for d, p in zip(sessions, t_adj)}
    tiingo_close = {d: float(p) for d, p in zip(sessions, close)}
    td = {d: float(p) for i, (d, p) in enumerate(zip(sessions, td_adj)) if i not in drop}
    td_raw = {d: float(p) for i, (d, p) in enumerate(zip(sessions, td_close)) if i not in drop}
    return sessions, tiingo_adj, tiingo_close, td, td_raw


def test_identical_series_pass():
    out = v4.crosscheck_statistics(*_paths())
    assert out["passed"] and out["pairs"] == 399 and out["share_within"] == 1.0 and out["reason"] == "pass"


def test_rounding_level_noise_passes_and_large_disagreement_fails():
    assert v4.crosscheck_statistics(*_paths(noise_bp=0.5))["passed"]  # rounding-level print differences
    wrong = v4.crosscheck_statistics(*_paths(noise_bp=200))
    assert not wrong["passed"] and wrong["reason"] == "disagreement" and wrong["share_within"] < 0.5


def test_a_different_instrument_fails():
    sessions, t_adj, t_close, _, _ = _paths(seed=1)
    _, _, _, other, other_raw = _paths(seed=2)
    out = v4.crosscheck_statistics(sessions, t_adj, t_close, other, other_raw)
    assert not out["passed"] and out["reason"] == "disagreement"


def test_adjustment_dates_are_excluded_and_counted():
    out = v4.crosscheck_statistics(*_paths(dividend_every=63))
    assert out["passed"] and out["excluded_adjustment_pairs"] == 6 and out["pairs"] == 399 - 6


def test_an_adjustment_dominated_series_is_not_admitted():
    out = v4.crosscheck_statistics(*_paths(dividend_every=5))
    assert not out["passed"] and out["reason"] == "adjustment_dominated"


def test_too_few_pairs_and_nonconsecutive_pairs():
    short = v4.crosscheck_statistics(*_paths(n=200))
    assert not short["passed"] and short["reason"] == "too_few_pairs"
    gaps = v4.crosscheck_statistics(*_paths(drop=(10, 20, 30)))
    assert gaps["dropped_nonconsecutive"] == 3 and gaps["passed"]


def test_two_bad_prints_in_250_pairs_still_pass_three_fail():
    sessions, t_adj, t_close, td, td_raw = _paths(n=251)
    for k, d in enumerate(sessions[50:52]):
        td[d] *= 1.05 if k == 0 else 1.0
    assert v4.crosscheck_statistics(sessions, t_adj, t_close, td, td_raw)["within_tolerance"] >= 248


# --- the holdout-period basis check (only at open-holdout) ---------------------------------------------------


@pytest.fixture()
def sealed_v4(tmp_path):
    vault = _vault(tmp_path / "vault")
    vault.add_file(v3.WITNESS_PATH, v3.REGISTERED_ANCHOR_LINE + b"\n")
    key, inputs = _discovery_key(tmp_path / "reg", vault)
    frozen = {"payload": {"version": v4.VERSION, "prereg_sha256": v4.PREREG_BODY_SHA256,
                          "state": "DISCOVERY_FROZEN", "inputs": {"inputs_frozen_sha256": key.inputs_frozen_sha256},
                          "calibration": {"state": "CONSISTENT"}, "ledger": []}}
    frozen["sha256"] = v1.digest(frozen["payload"])
    sealed = v4.seal_discovery(tmp_path / "reg", NOW, key, frozen)
    return tmp_path / "reg", vault, frozen, inputs, sealed


def _open(log_dir, vault, frozen, inputs, probe):
    return v4.open_holdout(frozen, allow_holdout=True, prereg_sha256=v4.PREREG_BODY_SHA256, log_dir=log_dir,
                           now=NOW, observed=_observed(inputs), witness=vault.witness(), holdout_probe=probe)


def test_the_holdout_opens_only_with_a_later_holdout_basis_report(sealed_v4):
    log_dir, vault, frozen, inputs, sealed = sealed_v4
    with pytest.raises(PermissionError, match="holdout-period basis report"):
        _open(log_dir, vault, frozen, inputs, None)
    with pytest.raises(PermissionError, match="window 'holdout'"):
        _open(log_dir, vault, frozen, inputs, {"sha256": "e" * 64, "report": {"window": "discovery",
                                                                             "snapshot_as_of_ts": "2030-01-01T00:00:00+00:00"}})
    with pytest.raises(PermissionError, match="after the discovery was sealed"):
        _open(log_dir, vault, frozen, inputs, {"sha256": "e" * 64, "report": {"window": "holdout",
                                                                             "snapshot_as_of_ts": "2020-01-01T00:00:00+00:00"}})
    later = "2030-01-01T00:00:00+00:00"
    assert later > sealed["run_at"]
    _open(log_dir, vault, frozen, inputs, {"sha256": "e" * 64, "report": {"window": "holdout",
                                                                         "snapshot_as_of_ts": later,
                                                                         "admitted": ["AAA"]}})
    opened = [r for r in v4.registry(log_dir).read_all() if r["kind"] == "holdout_opened"][0]
    assert opened["holdout_probe"]["sha256"] == "e" * 64


# --- supersession ---------------------------------------------------------------------------------------


def test_v4_refuses_while_v3_is_opened(tmp_path):
    vault = _vault(tmp_path / "vault")
    vault.add_file(v3.WITNESS_PATH, v3.REGISTERED_ANCHOR_LINE + b"\n" + b'{"head_sha256":"' + b"e" * 64
                   + b'","prev_anchor_sha256":"x","records":4,"run_at":"2026-10-01T00:00:00+00:00"}\n')
    _register(tmp_path / "reg", v4)
    vault.publish(tmp_path / "reg")
    v4.freeze_inputs(tmp_path / "reg", NOW, _inputs())
    with pytest.raises(PermissionError, match="VS1 registry was opened"):
        v4.open_discovery(tmp_path / "reg", NOW, _observed(_inputs()), vault.witness())


def test_post_admission_power_needs_the_manifest(capsys):
    from scripts import run_vs1_v4_insider_density as cli

    with pytest.raises(SystemExit):
        cli.main(["power", "--form4", "f", "--submissions", "s", "--issuer-map", "m", "--sic-map", "x",
                  "--out", "o", "--sims", "30"])
    assert "unrecognized arguments: --sims" in capsys.readouterr().err
