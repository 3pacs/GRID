"""Synthetic v7 window, registration and probe gates; no provider or DB access."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_sectors_v5 as s5
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4
from scripts.run_vs1_v7_insider_density import check_early_probe


def test_v7_window_is_scoped_and_holdout_fixed():
    original = v1.window_bounds("discovery")
    assert original[0] == pd.Timestamp("2012-01-01T00:00:00Z")
    with v1.discovery_window(v7.DISCOVERY_START):
        discovery = v1.window_bounds("discovery")
        holdout = v1.window_bounds("holdout")
        assert discovery == (pd.Timestamp("2011-10-01T00:00:00Z"), pd.Timestamp(v1.SPLIT))
        assert holdout == (pd.Timestamp(v1.SPLIT), pd.Timestamp(v1.END))
    assert v1.window_bounds("discovery") == original


def test_vendor_window_and_probe_rule_are_scoped():
    old = fetch.td_params("XLK", "all")["start_date"]
    with fetch.discovery_vendor_window("2011-08-02"), \
            gd4.preregistered_window("2011-08-02", v7.VERSION, v7.PREREG_BODY_SHA256):
        assert fetch.td_params("XLK", "all")["start_date"] == "2011-08-02"
        assert gd4.probe_rule().discovery_window == ("2011-08-02", "2019-12-31")
        assert gd4.probe_identity() == (v7.VERSION, v7.PREREG_BODY_SHA256)
    assert fetch.td_params("XLK", "all")["start_date"] == old
    assert gd4.probe_rule() == gd4.CROSSCHECK


def test_old_probe_refused_and_exact_early_metadata_passes(tmp_path):
    probe = tmp_path / "probe.json"
    cross = tmp_path / "cross.json"
    common = {"read_window": {"start": "2011-11-02", "end": "2019-12-31"},
              "prereg": {"study": v7.VERSION, "body_sha256": v7.PREREG_BODY_SHA256}}
    probe.write_text(json.dumps({**common, "benchmark_admitted": True, "source": {"name": "TIINGO"},
                                 "tolerances": {"crosscheck": {"discovery_window": ["2011-11-02", "2019-12-31"]}}}))
    cross.write_text(json.dumps({**common, "rule": {"discovery_window": ["2011-11-02", "2019-12-31"]}}))
    with pytest.raises(PermissionError, match="new 2011"):
        check_early_probe(probe, cross)
    expected = {"start": "2011-08-02", "end": "2019-12-31"}
    probe.write_text(json.dumps({"read_window": expected, "prereg": common["prereg"], "benchmark_admitted": True,
                                 "source": {"name": "TIINGO"},
                                 "tolerances": {"crosscheck": {"discovery_window": list(expected.values())}}}))
    cross.write_text(json.dumps({"read_window": expected, "prereg": common["prereg"],
                                 "rule": {"discovery_window": list(expected.values())}}))
    check_early_probe(probe, cross)
    cross.write_text(json.dumps({"read_window": expected, "prereg": common["prereg"],
                                 "rule": {"discovery_window": ["2011-11-02", "2019-12-31"]}}))
    with pytest.raises(PermissionError, match="earlier-window"):
        check_early_probe(probe, cross)


def test_registration_requires_witness_and_sectors_wait_for_v7_holdout(monkeypatch, tmp_path):
    with pytest.raises(PermissionError, match="witnessed v6 STOP"):
        v7.register(tmp_path / "v7", pd.Timestamp("2026-09-29T00:00:00Z").to_pydatetime(), "a" * 40)
    monkeypatch.setattr(s5, "V7_REGISTRATION_HEAD_SHA256", "a" * 64)
    monkeypatch.setattr(v7, "REGISTERED_RECORD_SHA256", ("b" * 64, "a" * 64))
    monkeypatch.setattr(s5, "verify_v7_registration", lambda *args: {"records": 2})
    monkeypatch.setattr(v7, "_v6_anchor_at_tip", lambda *args: None)
    monkeypatch.setattr(v7, "V6_STOP_HEAD_SHA256", "c" * 64)
    counts = {key: 2 for key in v7.BASELINE_REGISTRIES | {v7.REGISTRY_ID}}
    counts[v7.v6.VERSION] = 3
    files = {key: v1.canonical_witness_path(key) for key in counts}
    census = {"tip": "tip", "files": files, "records": counts, "unknown": []}
    s5.check_registration_census(census, v7_log_dir=tmp_path, witness_repo=tmp_path)
    def no_holdout(*_args):
        raise PermissionError("v7 has no witnessed terminal holdout_result")
    monkeypatch.setattr(s5, "verify_v7_holdout_result", no_holdout)
    with pytest.raises(PermissionError, match="holdout_result"):
        s5.check_census({**census, "files": {**files, s5.REGISTRY_ID: s5.WITNESS_PATH},
                         "records": {**counts, s5.REGISTRY_ID: 2}},
                        v7_log_dir=tmp_path, witness_repo=tmp_path)
