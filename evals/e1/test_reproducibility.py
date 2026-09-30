"""E1 gate 4: reproducibility -- same inputs, same code, byte-identical outputs.

* The VS1 v7 discovery ledger (v6 panel construction, ``v1.discover_panel``
  with the pre-registered 20,000 block sign flips, Holm/BH, plus v2's
  reported-only statistics and the seeded sensitivity nulls) run twice on the
  frozen synthetic VS1 world.
* The S09 real-panel scan (``scripts/run_real_panel_scan.scan``) run twice on
  a frozen SQLite ``raw_series`` fixture, every artifact compared byte for
  byte. The second run is timed on a "slower host" (a different wall clock):
  identical inputs and code must still give identical bytes.
"""

from __future__ import annotations

import argparse
import hashlib
import types
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from evals.e1 import vs1_world, world as W
from evals.e1.known_violations import known_violation


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*")) if p.is_file()
    }


# ── VS1 discovery ledger ───────────────────────────────────────────────


def _vs1_discovery(out: Path) -> None:
    from analysis import panel_insider_density as v1
    from analysis import panel_insider_density_v2 as v2
    from analysis import panel_insider_density_v7 as v7
    from analysis.offline_research_proof import write_once

    world = vs1_world.build(seed=23)
    panels = vs1_world.trial_panels(world)
    spec = v7.run_spec("e1-reproducibility")
    with v1.discovery_window(v7.DISCOVERY_START):
        frozen = v1.discover_panel(spec, panels, inputs={"fixture": "evals.e1.vs1_world", "seed": 23},
                                   sensitivity=False)
        # v2's reported-only statistics, and the seeded sensitivity nulls on the primary trial.
        reported = {t: v2.measure_trial(panels[t], sensitivity=False) for t in sorted(panels)}
        reported["sensitivity"] = v1.measure_trial(panels[v7.v6.PRIMARY_TRIAL], sensitivity=True,
                                                   sensitivity_perms=500)
    out.mkdir(parents=True)
    write_once(out / "discovery-frozen.json", frozen)
    write_once(out / "reported.json", reported)


def test_vs1_discovery_ledger_is_byte_identical_across_runs(tmp_path):
    _vs1_discovery(tmp_path / "a")
    _vs1_discovery(tmp_path / "b")
    a, b = _tree_digest(tmp_path / "a"), _tree_digest(tmp_path / "b")
    assert a and a == b


# ── S09 real-panel scan ────────────────────────────────────────────────

AS_OF = date(2021, 12, 31)
PULLED = datetime(2022, 1, 5, 6, 0)
AS_OF_TS = datetime(2022, 2, 1, tzinfo=timezone.utc)


def _panel_engine():
    engine = W.sqlite_engine()
    rng = np.random.default_rng(9)
    days = pd.bdate_range(date(2020, 1, 1), AS_OF)
    tgt = np.cumsum(rng.normal(0, 0.05, len(days))) + 4
    feat = np.cumsum(rng.normal(0, 1, len(days)))
    rows = []
    for i, d in enumerate(days):
        rows.append(W.row("TGT", d.date(), tgt[i], PULLED))
        rows.append(W.row("FEAT_D", d.date(), feat[i], PULLED))
    for d in pd.date_range(date(2020, 1, 1), AS_OF, freq="W-SAT"):
        rows.append(W.row("FEAT_W", d.date(), 100 + rng.normal(), PULLED))
    rows.append(W.row("FEAT_D", date(2021, 6, 1), 0.0, PULLED, status="FAILED"))
    rows.append(W.row("TGT", date(2021, 3, 3), 99.0, datetime(2022, 3, 1)))  # pulled after as_of_ts
    W.insert(engine, rows)
    return engine


def _scan(out: Path, *, seconds_per_tick: float) -> None:
    """One scan into ``out``; ``seconds_per_tick`` sets how fast this host's wall clock runs."""
    from analysis import research_real_panel as rp
    from scripts import run_real_panel_scan as scan_script

    args = argparse.Namespace(
        as_of=AS_OF.isoformat(), as_of_ts=AS_OF_TS.isoformat(), read_start="2020-01-01",
        discovery_start="2020-05-01", split="2021-01-04", perms=199, seed=1, code_sha="e1-fixed-sha",
    )
    clock = iter(1_700_000_000.0 + seconds_per_tick * k for k in range(10_000))
    with pytest.MonkeyPatch.context() as mp:
        mp.setitem(rp.PROXY_GROUPS, "TGT", frozenset({"TGT", "FEAT_D"}))
        mp.setattr(scan_script, "FEATURES", (
            rp.SeriesSpec("FEAT_D", "diff", "FRB_H15"),
            rp.SeriesSpec("FEAT_W", "pct", "FRB_H10", stale_sessions=10),
        ))
        mp.setattr(scan_script, "TARGETS", (rp.TargetSpec("TGT", "change", "FRB_H15"),))
        mp.setattr(scan_script, "HORIZONS", (1, 5))
        mp.setattr(scan_script, "time", types.SimpleNamespace(time=lambda: next(clock)))
        out.mkdir(parents=True)
        engine = _panel_engine()
        with engine.connect() as conn:
            scan_script.scan(conn, out, args)


@pytest.fixture(scope="module")
def scans(tmp_path_factory) -> dict[str, dict[str, str]]:
    """Three runs, same inputs and code: two on one host, one on a slower host."""
    root = tmp_path_factory.mktemp("e1-scan")
    runs = {"a": 0.0, "b": 0.0, "slow": 7.3}
    for name, tick in runs.items():
        _scan(root / name, seconds_per_tick=tick)
    return {name: _tree_digest(root / name) for name in runs}


def test_real_panel_scan_artifacts_are_byte_identical_across_runs(scans):
    assert {"summary.json", "trial-ledger.csv", "frozen-candidates.json",
            "run/discovery-frozen.json", "run/holdout-result.json"} <= set(scans["a"])
    assert scans["a"] == scans["b"]


def test_real_panel_scan_ledger_does_not_depend_on_host_speed(scans):
    ledgers = sorted(k for k in scans["a"] if k != "summary.json")
    assert {k: scans["a"][k] for k in ledgers} == {k: scans["slow"][k] for k in ledgers}


@known_violation("E1-V5")
def test_real_panel_scan_report_does_not_depend_on_host_speed(scans):
    assert scans["a"]["summary.json"] == scans["slow"]["summary.json"]
