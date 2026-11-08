"""Unit tests for scripts/repair_full_panel_vintages.py (pure planning, no DB).

The fixture reproduces the 2026-11-08 ``xlp_full`` shape in miniature: three
pull-date vintages per observation date that alternate between the Tiingo
adj_close basis and the raw close, plus one ~0.6 holiday junk print written
into adj_close. The plan must retract exactly the raw-basis rows and the
junk print, re-vintage the cells that would go dark, and turn a served
series with -70 % / +230 % day jumps into a flat one.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

import scripts.repair_full_panel_vintages as rep
from scripts.repair_full_panel_vintages import (
    AMBIGUOUS,
    OFF_BASIS,
    ON_BASIS,
    OUTLIER,
    UNMATCHED,
    RawCandidate,
    ResolvedRow,
    check_window,
    plan_feature,
    target_field,
    ticker_for_feature,
)

REPAIR_DAY = date(2026, 11, 10)
V1, V2, V3 = date(2026, 4, 24), date(2026, 4, 29), date(2026, 5, 2)


def _bdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _fixture(junk_day: date | None = None):
    days = _bdays(date(2024, 1, 2), 30)
    rows: list[ResolvedRow] = []
    raw = {"close": {}, "adj_close": {}}
    rid = 0
    for i, d in enumerate(days):
        adj, close = 70.0 + i * 0.05, 78.0 + i * 0.05
        raw["adj_close"][d] = [RawCandidate(adj, V1, 3), RawCandidate(adj - 0.02, V3, 2)]
        raw["close"][d] = [RawCandidate(close, V1, 3), RawCandidate(close, V2, 2), RawCandidate(close, V3, 2)]
        # vintage V1: adj won; V2: raw-only pull; V3: raw won the race on
        # even days, adj on odd days -> the latest vintage alternates basis.
        for vd, val in ((V1, adj), (V2, close), (V3, close if i % 2 == 0 else adj - 0.02)):
            rid += 1
            rows.append(ResolvedRow(id=rid, obs_date=d, vintage_date=vd, value=val, source_id=3))
    if junk_day is not None:
        raw["adj_close"][junk_day].append(RawCandidate(0.6, V3, 2))
        rows = [r for r in rows if not (r.obs_date == junk_day and r.vintage_date == V3)]
        rid += 1
        rows.append(ResolvedRow(id=rid, obs_date=junk_day, vintage_date=V3, value=0.6, source_id=2))
    return days, rows, raw


class TestHelpers:
    def test_ticker_for_feature(self) -> None:
        assert ticker_for_feature("xlp_full") == "XLP"
        assert ticker_for_feature("brk-b_full") == "BRK-B"
        assert ticker_for_feature("btc_usd_full") == "BTC-USD"
        with pytest.raises(ValueError):
            ticker_for_feature("vix_spot")

    def test_target_field_follows_the_allowlist(self) -> None:
        assert target_field("xlp_full") == "adj_close"
        assert target_field("spy_full") == "close"
        assert target_field("sp500_full") == "close"

    def test_db_window_refused(self) -> None:
        inside = datetime(2026, 11, 8, 7, 41, tzinfo=timezone.utc)
        with pytest.raises(rep.DbWindowRefused):
            check_window(inside, False)
        check_window(inside, True)
        check_window(datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc), False)


class TestPlan:
    def test_alternating_vintages_retract_only_raw_rows(self) -> None:
        days, rows, raw = _fixture()
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        by = plan.by_class
        assert by[OFF_BASIS] == 30 + 15          # every V2 row + the even-day V3 rows
        assert by[ON_BASIS] == 30 + 15
        assert by.get(AMBIGUOUS, 0) == 0 and by.get(UNMATCHED, 0) == 0 and by.get(OUTLIER, 0) == 0
        assert all(r["cls"] == OFF_BASIS for r in plan.retract)
        assert len(plan.retract) == 45
        # V1 (adj) survives in every cell, so nothing goes dark.
        assert plan.cells_dark_after == 0 and plan.revintage == []
        # Served series: alternating raw/adj flips before, flat after.
        assert plan.served_before["jumps_gt_10pct"] >= 10
        assert plan.served_after["jumps_gt_10pct"] == 0
        assert 0.99 < plan.served_after["ratio"] < 1.05
        assert "YF:XLP:close" in plan.retract[0]["reason"]

    def test_junk_print_is_an_outlier_and_retracted(self) -> None:
        junk = _bdays(date(2024, 1, 2), 30)[9]
        days, rows, raw = _fixture(junk_day=junk)
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        outliers = [r for r in plan.retract if r["cls"] == OUTLIER]
        assert len(outliers) == 1 and outliers[0]["obs_date"] == junk.isoformat()
        assert outliers[0]["value"] == pytest.approx(0.6)
        assert "junk print" in outliers[0]["reason"]
        assert plan.served_after["jumps_gt_10pct"] == 0

    def test_dark_cell_is_revintaged_from_the_on_basis_raw_row(self) -> None:
        days, rows, raw = _fixture()
        d = days[3]
        # Drop this cell's adj vintages so only raw-basis rows remain.
        rows = [r for r in rows if not (r.obs_date == d and r.vintage_date == V1)]
        rows = [r for r in rows if not (r.obs_date == d and r.vintage_date == V3)]
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert plan.cells_dark_after == 1
        assert len(plan.revintage) == 1
        rv = plan.revintage[0]
        assert rv["obs_date"] == d.isoformat() and rv["vintage_date"] == REPAIR_DAY.isoformat()
        assert rv["value"] == pytest.approx(raw["adj_close"][d][-1].value)   # latest pull wins
        assert rv["from_pull_date"] == V3.isoformat()
        assert plan.cells_left_dark == 0
        assert plan.served_after["jumps_gt_10pct"] == 0

    def test_cell_without_on_basis_raw_row_stays_dark(self) -> None:
        days, rows, raw = _fixture()
        d = days[5]
        rows = [r for r in rows if not (r.obs_date == d and r.vintage_date in (V1, V3))]
        raw["adj_close"].pop(d)
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert plan.cells_dark_after == 1 and plan.revintage == [] and plan.cells_left_dark == 1

    def test_already_retracted_rows_are_not_retracted_again(self) -> None:
        days, rows, raw = _fixture()
        for r in rows:
            if r.vintage_date == V2:
                r.retracted = True
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert plan.by_class[rep.ALREADY_RETRACTED] == 30
        assert len(plan.retract) == 15

    def test_raw_basis_feature_retracts_adjusted_rows(self) -> None:
        # spy_full is the receipted raw close: here the adj rows are the wrong basis.
        days, rows, raw = _fixture()
        plan = plan_feature("spy_full", rows, raw, repair_day=REPAIR_DAY, feature_id=1)
        assert plan.target_field == "close"
        assert all("YF:SPY:adj_close" in r["reason"] for r in plan.retract)
        assert len(plan.retract) == 45
        assert plan.served_after["jumps_gt_10pct"] == 0

    def test_equal_values_are_ambiguous_and_kept(self) -> None:
        d = date(2026, 11, 3)
        rows = [ResolvedRow(1, d, d, 100.0, 3), ResolvedRow(2, d + timedelta(1), d + timedelta(1), 101.0, 3),
                ResolvedRow(3, d + timedelta(2), d + timedelta(2), 102.0, 3)]
        raw = {"close": {r.obs_date: [RawCandidate(r.value, r.vintage_date, 3)] for r in rows},
               "adj_close": {r.obs_date: [RawCandidate(r.value, r.vintage_date, 3)] for r in rows}}
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert plan.by_class == {AMBIGUOUS: 3}
        assert plan.retract == []

    def test_summary_is_json_shaped(self) -> None:
        days, rows, raw = _fixture()
        s = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7).summary()
        assert isinstance(s["retract"], int) and isinstance(s["revintage"], int)
        assert s["downstream"] if "downstream" in s else rep.DOWNSTREAM_RECORDS


class TestCli:
    def test_apply_requires_run_tag(self, capsys) -> None:
        assert rep.main(["--feature", "xlp_full", "--apply"], engine=object()) == 2
        assert "run-tag" in capsys.readouterr().err

    def test_needs_a_feature(self, capsys) -> None:
        assert rep.main([], engine=object()) == 2
