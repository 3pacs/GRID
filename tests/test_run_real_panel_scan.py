"""S09 scan script: declared universe, read-only engine bounds, end-to-end on SQLite."""

from __future__ import annotations

import argparse
import json

import pytest

from analysis.research_real_panel import refusal
from scripts import run_real_panel_scan as scan_script
from tests.test_research_real_panel import (  # noqa: F401
    AS_OF,
    AS_OF_TS,
    FEATURES,
    TARGETS,
    engine,
)


def test_declared_universe_is_small_clean_and_non_price():
    ids = [s.series_id for s in scan_script.FEATURES]
    assert len(ids) == len(set(ids)) <= 40
    assert all(refusal(sid) is None for sid in ids)
    assert all(not s.revised for s in scan_script.FEATURES)
    assert all(t.label == "change" and refusal(t.series_id) is None for t in scan_script.TARGETS)
    assert not any(sid.startswith(("YF", "snap:")) for sid in ids)
    trials = len(ids) * 3 * len(scan_script.TARGETS) * len(scan_script.HORIZONS)
    # one trial alone must be able to pass BH at 10%: 1/(perms+1) <= q/m
    assert 1 / (20000 + 1) <= 0.10 / trials


@pytest.mark.parametrize("seconds", [0, 61, 120])
def test_statement_timeout_above_sixty_seconds_is_refused(seconds):
    with pytest.raises(ValueError, match="statement timeout"):
        scan_script.read_only_engine(seconds)


def test_bh_threshold_reports_the_step_up_cut():
    ledger = [{"p": p} for p in (0.001, 0.004, 0.2, 1.0, 1.0)]
    cut = scan_script.bh_threshold(ledger, 0.10)
    assert cut["rejections"] == 2 and cut["critical_p"] == 0.004
    assert cut["first_rank_cut"] == pytest.approx(0.02)
    assert scan_script.bh_threshold([{"p": 1.0}], 0.1)["critical_p"] is None


def test_scan_end_to_end_writes_a_new_ledger_artifact(engine, tmp_path, monkeypatch):  # noqa: F811
    monkeypatch.setattr(scan_script, "FEATURES", FEATURES)
    monkeypatch.setattr(scan_script, "TARGETS", TARGETS)
    monkeypatch.setattr(scan_script, "HORIZONS", (1, 5))
    args = argparse.Namespace(
        as_of=AS_OF.isoformat(),
        as_of_ts=AS_OF_TS.isoformat(),
        read_start="2020-01-01",
        discovery_start="2020-05-01",
        split="2021-01-04",
        perms=199,
        seed=1,
        code_sha="test",
    )
    with engine.connect() as conn:
        summary = scan_script.scan(conn, tmp_path, args)
    assert summary["trials"] == 6 * 2 == summary["testable"] + summary["untestable"]
    assert summary["state"] == "PIT_VINTAGE_READ_EXPLORATORY"
    assert summary["reader"] == "store.observations.read_window"
    assert summary["holdout_end_exclusive"] == "2022-01-01T00:00:00+00:00"
    for name in ("summary.json", "trial-ledger.csv", "frozen-candidates.json",
                 "run/discovery-frozen.json", "run/holdout-result.json"):
        assert (tmp_path / name).exists()
    assert json.loads((tmp_path / "summary.json").read_text())["trials"] == 12
    with engine.connect() as conn, pytest.raises(FileExistsError):
        scan_script.scan(conn, tmp_path, args)
