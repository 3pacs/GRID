"""Generic sector-relative prices (GD6): TIINGO only, admitted only, guarded before any read, and the
XLRE/XLC pre-inception equal-weight benchmark rule. Synthetic closes only."""

from __future__ import annotations

import dataclasses
from datetime import date

import numpy as np
import pandas as pd
import pytest

from analysis import panel_mode as pm
from analysis import panel_prices as pp
from tests.panel_mode_support import NOW, flow_setup


def _key(f):
    pm.register(f["reg"], NOW, "c0de" * 10, constructs=f["construct"], run=f["run"], prereg_path=f["prereg_path"],
                repo_root=f["repo_root"])
    pm.freeze_inputs(f["reg"], NOW, f["inputs"])
    pm.open_discovery(f["reg"], NOW, f["inputs"], repo_root=f["repo_root"])
    f["vault"].publish(f["reg"])
    return pm.resume_discovery(f["reg"], f["inputs"], f["vault"].witness(f["reg"].registry_id),
                               repo_root=f["repo_root"])


@pytest.mark.parametrize("change", [
    {"source": "yfinance"}, {"source": "TwelveData"}, {"series_template": "TIINGO:{ticker}:close"},
    {"admitted": ("XLE", "E00")}, {"benchmark": "SPY"}, {"probe_report_sha256": "nope"},
    {"calendar": "E00"},
])
def test_manifest_contract(tmp_path, monkeypatch, change):
    f = flow_setup(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        dataclasses.replace(f["manifest"], **change).validate()


def test_refusals_happen_before_any_read(tmp_path, monkeypatch):
    f = flow_setup(tmp_path, monkeypatch)
    key = _key(f)
    guard = pm.OutcomeWindowGuard()
    with pytest.raises(PermissionError, match="R3"):  # a read reaching back into the quarantine window
        pp.load_panel_prices(None, f["manifest"], start=date(2026, 6, 1), as_of=date(2027, 1, 1),
                             key=key, guard=guard)
    other = dataclasses.replace(f["manifest"], probe_report_sha256="ee" * 32)
    with pytest.raises(PermissionError, match="inputs_frozen"):
        pp.load_panel_prices(None, other, start=date(2026, 7, 1), as_of=date(2027, 1, 1), key=key, guard=guard)
    with pytest.raises(PermissionError, match="itself"):
        pp.load_panel_prices(None, f["manifest"], start=date(2026, 7, 1), as_of=date(2027, 1, 1),
                             key=key, guard=object())
    assert f["reader"].calls == []


def test_pre_inception_equal_weight_benchmark():
    days = pd.bdate_range("2026-07-01", periods=60)
    rng = np.random.default_rng(3)
    issuers = np.exp(np.cumsum(0.01 * rng.standard_normal((60, 4)), axis=0)) * 20
    bench = np.full(60, np.nan)
    bench[30:] = 100 * np.exp(np.cumsum(0.01 * rng.standard_normal(30)))
    closes = pd.DataFrame(issuers, index=days, columns=["A", "B", "C", "D"])
    closes["XLRE"] = bench
    lo, hi = pd.Timestamp("2026-07-01", tz="UTC"), pd.Timestamp("2027-01-01", tz="UTC")
    first = days[30].date()
    positions, labels, _ = pp.relative_labels(closes, "XLRE", ["A", "B", "C", "D"], 5, lo, hi,
                                              benchmark_first_close=first)
    for row, i in enumerate(positions):
        issuer = closes.iloc[i + 5, :4].to_numpy() / closes.iloc[i, :4].to_numpy() - 1.0
        if days[i].date() < first:
            expected = issuer - issuer.mean()
        else:
            expected = issuer - (bench[i + 5] / bench[i] - 1.0)
        np.testing.assert_allclose(labels[row], expected, rtol=0, atol=1e-15)
    # The per-date rank IC is unchanged by the benchmark (it is common to every issuer).
    _, plain, _ = pp.relative_labels(closes.assign(XLRE=1.0), "XLRE", ["A", "B", "C", "D"], 5, lo, hi)
    for a, b in zip(labels, plain):
        assert list(np.argsort(a)) == list(np.argsort(b))
