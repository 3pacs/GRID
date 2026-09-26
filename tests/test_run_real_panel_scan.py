"""S09/S09b scan script: declared universe, publication sources, proxy groups,
read-only engine bounds, end-to-end on SQLite, and the candidate relabelling."""

from __future__ import annotations

import argparse
import json

import pytest

from analysis.research_real_panel import (
    PROXY_GROUPS,
    PUBLICATIONS,
    proxy_group,
    refusal,
    relabel_frozen_candidates,
    self_lag_pairs,
)
from scripts import relabel_frozen_candidates as relabel_script
from scripts import run_real_panel_scan as scan_script
from tests.test_research_real_panel import (  # noqa: F401
    AS_OF,
    AS_OF_TS,
    FEATURES,
    TARGETS,
    engine,
    tgt_proxy_group,
)

# The 8 candidates frozen by the first real-panel scan (ef0d564b), as
# (family, feature, direction). Artifact: hypothesis-loop-20260926/scan-ef0d564b.
EF0D564B_CANDIDATES = (
    ("VIXCLS|change|fwd1", "VIXCLS|chg5", -1),
    ("VIXCLS|change|fwd1", "VIXCLS|z60", -1),
    ("VIXCLS|change|fwd5", "BAMLH0A0HYM2|z60", -1),
    ("VIXCLS|change|fwd5", "BAMLH0A1HYBB|z60", -1),
    ("VIXCLS|change|fwd5", "BAMLH0A2HYB|z60", -1),
    ("VIXCLS|change|fwd5", "VIXCLS|z60", -1),
    ("DGS2|change|fwd5", "DGS1|chg20", 1),
    ("T10Y2Y|change|fwd5", "DGS1|z60", -1),
)


def test_declared_universe_is_small_clean_and_non_price():
    ids = [s.series_id for s in scan_script.FEATURES]
    assert len(ids) == len(set(ids)) <= 40
    assert all(refusal(sid) is None for sid in ids)
    assert "DTWEXBGS" not in ids  # revised broad dollar index dropped in S09b
    assert all(s.source in PUBLICATIONS for s in scan_script.FEATURES)
    assert all(t.source in PUBLICATIONS for t in scan_script.TARGETS)
    assert all(t.label == "change" and refusal(t.series_id) is None for t in scan_script.TARGETS)
    assert not any(sid.startswith(("YF", "snap:")) for sid in ids)
    trials = len(ids) * 3 * len(scan_script.TARGETS) * len(scan_script.HORIZONS)
    # one trial alone must be able to pass BH at 10%: 1/(perms+1) <= q/m
    assert 1 / (20000 + 1) <= 0.10 / trials


def test_reviewer_verified_publication_lags_are_declared():
    by_id = {s.series_id: PUBLICATIONS[s.source] for s in scan_script.FEATURES}
    for sid in ("DGS1", "DGS2", "DFF", "DFII10"):
        assert (by_id[sid].lag, by_id[sid].unit) == (1, "business")
        assert by_id[sid].time_utc >= "20:17"
    assert (by_id["DEXJPUS"].lag, by_id["DEXJPUS"].unit) == (8, "calendar")
    assert (by_id["WALCL"].lag, by_id["WALCL"].unit) == (2, "calendar")
    assert (by_id["MORTGAGE30US"].lag, by_id["MORTGAGE30US"].unit) == (1, "calendar")
    assert (by_id["aaii.bull_bear_spread"].lag, by_id["aaii.bull_bear_spread"].unit) == (
        1,
        "calendar",
    )


def test_proxy_groups_cover_every_target_and_known_near_copies():
    for target in scan_script.TARGETS:
        assert target.series_id in proxy_group(target.series_id)
    assert {"DGS1", "DGS3", "T10Y2Y"} <= PROXY_GROUPS["DGS2"]
    assert {"DGS10", "DGS2", "DGS1", "T10Y3M"} <= PROXY_GROUPS["T10Y2Y"]
    assert {"BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC"} <= PROXY_GROUPS["BAMLH0A0HYM2"]
    assert "VXVCLS" in PROXY_GROUPS["VIXCLS"]
    # cross-series links stay testable: they are not near-copies of the target
    assert "BAMLH0A0HYM2" not in PROXY_GROUPS["VIXCLS"]
    assert "DGS5" not in PROXY_GROUPS["DGS2"]
    assert "BAMLC0A0CM" not in PROXY_GROUPS["BAMLH0A0HYM2"]


def test_self_lag_pairs_over_the_declared_universe():
    names = tuple(
        f"{s.series_id}|{suffix}"
        for s in scan_script.FEATURES
        for suffix in ("chg5", "chg20", "z60")
    )
    families = tuple(
        f"{t.series_id}|{t.label}|fwd{h}"
        for t in scan_script.TARGETS
        for h in scan_script.HORIZONS
    )
    pairs = self_lag_pairs(families, names)
    series = {}
    for family, feature in pairs:
        series.setdefault(family.split("|")[0], set()).add(feature.split("|")[0])
    assert series == {
        "VIXCLS": {"VIXCLS"},
        "DGS2": {"DGS1", "DGS2", "T10Y2Y"},
        "T10Y2Y": {"DGS1", "DGS2", "T10Y2Y", "T10Y3M"},
        "BAMLH0A0HYM2": {"BAMLH0A0HYM2", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC"},
    }
    assert len(pairs) == 3 * 3 * (1 + 3 + 4 + 4)  # suffixes x horizons x series


def test_relabelling_marks_the_five_self_lag_candidates(tmp_path):
    candidates = [
        {
            "specification": {
                "family": family,
                "feature": feature,
                "direction": direction,
                "origin": "pit_vintage_read",
            },
            "sha256": f"{i:064x}",
            "state": "FORWARD_EVIDENCE_PENDING",
            "promotion_allowed": False,
        }
        for i, (family, feature, direction) in enumerate(EF0D564B_CANDIDATES)
    ]
    result = relabel_frozen_candidates(candidates)
    assert result["counts"] == {"total": 8, "self_lag": 5, "cross_series": 3}
    remaining = [
        (r["family"], r["feature"])
        for r in result["relabelled"]
        if r["remains_candidate_under_proxy_rule"]
    ]
    assert remaining == [
        ("VIXCLS|change|fwd5", "BAMLH0A0HYM2|z60"),
        ("VIXCLS|change|fwd5", "BAMLH0A1HYBB|z60"),
        ("VIXCLS|change|fwd5", "BAMLH0A2HYB|z60"),
    ]
    for r in result["relabelled"]:
        assert r["promotion_allowed"] is False
        assert r["origin_relabel"] == {
            "recorded": "pit_vintage_read",
            "correct": "latest_vintage_read",
        }
        assert r["original_state"] == "FORWARD_EVIDENCE_PENDING"
        assert r["state"] == (
            "SELF_LAG_NEVER_A_CANDIDATE" if r["proxy_label"] == "SELF_LAG" else "RESCAN_REQUIRED"
        )
    # the script writes a copy beside the original and never overwrites
    source = tmp_path / "frozen-candidates.json"
    source.write_text(json.dumps(candidates), encoding="utf-8")
    before = source.read_bytes()
    output = relabel_script.relabel(source)
    assert output.name == "frozen-candidates.relabelled.json"
    assert source.read_bytes() == before
    assert json.loads(output.read_text())["counts"]["self_lag"] == 5
    with pytest.raises(FileExistsError):
        relabel_script.relabel(source)


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
    assert summary["self_lag_trials"] == 3 * 2
    assert summary["state"] == "LATEST_VINTAGE_READ_EXPLORATORY"
    assert summary["origin"] == "latest_vintage_read"
    assert summary["reader"] == "store.observations.read_window"
    assert summary["holdout_end_exclusive"] == "2022-01-01T00:00:00+00:00"
    assert set(summary["block_basis"]) == set(summary["blocks"])
    for name in ("summary.json", "trial-ledger.csv", "frozen-candidates.json",
                 "run/discovery-frozen.json", "run/holdout-result.json"):
        assert (tmp_path / name).exists()
    assert json.loads((tmp_path / "summary.json").read_text())["trials"] == 12
    assert "self_lag_p" in (tmp_path / "trial-ledger.csv").read_text().splitlines()[0]
    with engine.connect() as conn, pytest.raises(FileExistsError):
        scan_script.scan(conn, tmp_path, args)
