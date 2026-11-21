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
    EXCLUDED,
    IN_SCOPE,
    NEEDS_BACKFILL,
    OFF_BASIS,
    ON_BASIS,
    OUT_OF_SCOPE,
    OUTLIER,
    NO_BASIS_SPLIT,
    UNMATCHED,
    RawCandidate,
    ResolvedRow,
    check_window,
    feature_scope,
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


# ── Scope: the ticker comes from the entity-map feed, never from the name ──
#
# First dry run on grid-svr (2026-11-08): gold_full (fed by YF:GC=F:close) was
# compared against ticker GOLD = Barrick Gold and planned 100 % retraction;
# btc_full / eth_full (Binance, CoinGecko) against bare BTC / ETH equities;
# eth_usd_full lost 2,058 rows to pull-timing drift on 24/7 bars.

class TestScope:
    def test_static_adj_close_feed_is_in_scope_with_the_feed_ticker(self) -> None:
        sc = feature_scope("brk-b_full")
        assert (sc.status, sc.ticker, sc.target_field, sc.feed) == (IN_SCOPE, "BRK-B", "adj_close", "static")
        assert sc.feeds == ["YF:BRK-B:adj_close"]

    def test_spy_full_stays_in_scope_on_the_receipted_raw_close(self) -> None:
        sc = feature_scope("spy_full")
        assert (sc.status, sc.ticker, sc.target_field) == (IN_SCOPE, "SPY", "close")

    def test_gold_futures_are_never_compared_against_barrick(self) -> None:
        sc = feature_scope("gold_full")
        assert sc.ticker == "GC=F" and sc.ticker != ticker_for_feature("gold_full")
        assert sc.status == NO_BASIS_SPLIT

    def test_binance_and_coingecko_features_are_out_of_scope(self) -> None:
        for f in ("btc_full", "eth_full", "sol_full"):
            sc = feature_scope(f)
            assert sc.status == OUT_OF_SCOPE, f
            assert "not one yfinance price field" in (sc.reason or "")

    def test_crypto_and_index_allowlist_features_are_skipped(self) -> None:
        for f in ("btc_usd_full", "eth_usd_full", "sol_usd_full", "sp500_full"):
            sc = feature_scope(f)
            assert sc.status == NO_BASIS_SPLIT, f
            assert "pull-timing drift" in (sc.reason or "")

    def test_pattern_only_feature_is_in_scope_with_the_name_ticker(self) -> None:
        sc = feature_scope("zzz_full")
        assert (sc.status, sc.ticker, sc.feed, sc.feeds) == (IN_SCOPE, "ZZZ", "pattern", [])

    def test_exclude_wins_over_in_scope(self) -> None:
        assert feature_scope("xlp_full", exclude={"xlp_full"}).status == EXCLUDED
        assert feature_scope("xlp_full").status == IN_SCOPE

    def test_explicit_mappings_two_feeds_or_wrong_field(self) -> None:
        two = {"YF:ACME:adj_close": "acme_full", "ACME:px": "acme_full"}
        assert feature_scope("acme_full", mappings=two).status == OUT_OF_SCOPE
        wrong = {"YF:ACME:close": "acme_full"}
        sc = feature_scope("acme_full", mappings=wrong)
        assert sc.status == OUT_OF_SCOPE and "policy basis field" in (sc.reason or "")

    def test_every_static_full_feature_has_a_definite_status(self) -> None:
        from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS
        feats = {n for n in {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}.values() if n.endswith("_full")}
        statuses = {f: feature_scope(f).status for f in feats}
        assert set(statuses.values()) <= {IN_SCOPE, OUT_OF_SCOPE, NO_BASIS_SPLIT}
        assert statuses["gold_full"] == NO_BASIS_SPLIT
        assert statuses["btc_full"] == OUT_OF_SCOPE
        assert sum(1 for v in statuses.values() if v == IN_SCOPE) >= 100


# ── Dark-cell accounting and the coverage floor ─────────────────────────────

def _backfill_gap_fixture(missing_first_n: int):
    """ACN/HON shape: the adj_close feed starts at cell ``missing_first_n``.

    Before it, every vintage came from the raw close (there was no adj_close
    pull to race with) and there is no adj_close raw row to re-vintage from.
    """
    days, rows, raw = _fixture()
    gap = set(days[:missing_first_n])
    for d in gap:
        raw["adj_close"].pop(d, None)
    close_only = []
    for r in rows:
        if r.obs_date in gap:
            close_val = raw["close"][r.obs_date][0].value
            if r.value != close_val:
                r.value = close_val
        close_only.append(r)
    return days, close_only, raw


class TestDarkAccounting:
    def test_newly_dark_restored_and_dark_before_are_separate(self) -> None:
        days, rows, raw = _fixture()
        # cell 0: every row already retracted (the earlier fill retraction) -> dark today
        for r in rows:
            if r.obs_date == days[0]:
                r.retracted = True
        # cell 1: a dark-today cell with an on-basis raw row -> restored by the plan
        for r in rows:
            if r.obs_date == days[1]:
                r.retracted = True
        raw["adj_close"].pop(days[0])
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert plan.cells_dark_before == 2
        assert plan.cells_restored == 1          # days[1] re-vintaged from its adj_close raw row
        assert plan.cells_newly_dark == 0        # every served cell keeps or regains a vintage
        assert plan.cells_left_dark == 1         # days[0]: dark before, still dark, no raw row
        assert plan.status == IN_SCOPE and plan.applicable

    def test_coverage_floor_marks_needs_backfill_with_the_missing_range(self) -> None:
        days, rows, raw = _backfill_gap_fixture(15)
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        # The first 15 cells hold raw-close rows only: all OFF_BASIS, retracted,
        # and no adj_close raw row exists to re-vintage them from.
        assert plan.cells_newly_dark == 15
        assert plan.newly_dark_share == 0.5
        assert plan.status == NEEDS_BACKFILL
        assert not plan.applicable
        assert plan.newly_dark_range["obs_min"] == days[0].isoformat()
        assert plan.newly_dark_range["obs_max"] <= days[14].isoformat()
        assert "backfill YF:XLP:adj_close for obs_date" in plan.backfill_hint
        assert plan.retract  # the plan is still shown; it is just not applicable

    def test_floor_is_configurable(self) -> None:
        days, rows, raw = _backfill_gap_fixture(15)
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, coverage_floor=1.0)
        assert plan.status == IN_SCOPE and plan.applicable

    def test_holiday_junk_cell_going_dark_is_phantom_not_a_loss(self) -> None:
        days, rows, raw = _fixture()
        holiday = date(2024, 1, 6)  # a Saturday: no raw close, one junk 0.6 adj_close print
        assert holiday not in raw["close"]
        raw["adj_close"][holiday] = [RawCandidate(0.6, V3, 2)]
        rows.append(ResolvedRow(id=9999, obs_date=holiday, vintage_date=V3, value=0.6, source_id=2))
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        junk = [r for r in plan.retract if r["obs_date"] == holiday.isoformat()]
        assert len(junk) == 1 and junk[0]["cls"] == OUTLIER
        assert plan.cells_phantom_cleared == 1
        assert plan.cells_newly_dark == 0 and plan.newly_dark_share == 0.0
        assert plan.cells_left_dark == 1
        assert not [v for v in plan.revintage if v["value"] == 0.6]
        assert plan.revintage_rejected == [{"obs_date": holiday.isoformat(), "why": "no raw close row: not a trading day"}]
        assert plan.status == IN_SCOPE and plan.applicable

    def test_many_phantom_cells_never_trip_the_floor(self) -> None:
        days, rows, raw = _fixture()
        # 10 weekend/holiday cells of junk against 30 trading days: 25 % of cells
        d = date(2024, 1, 6)
        added = 0
        while added < 10:
            if d.weekday() >= 5:
                raw["adj_close"][d] = [RawCandidate(0.6, V3, 2)]
                rows.append(ResolvedRow(id=5000 + added, obs_date=d, vintage_date=V3, value=0.6, source_id=2))
                added += 1
            d += timedelta(days=1)
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert plan.cells_phantom_cleared == 10 and plan.cells_newly_dark == 0
        assert plan.status == IN_SCOPE
        assert not plan.revintage and len(plan.revintage_rejected) == 10

    def test_junk_print_is_never_revintaged(self) -> None:
        # A real trading day whose only adj_close raw candidate left after the
        # retraction is the 0.6 junk print, and a later honest one: the honest
        # value is chosen; with junk only, the cell stays dark and says why.
        days, rows, raw = _fixture()
        d = days[10]
        raw["adj_close"][d] = [RawCandidate(0.6, V3, 2)]
        rows = [r for r in rows if r.obs_date != d]
        rows.append(ResolvedRow(id=7001, obs_date=d, vintage_date=V2, value=raw["close"][d][0].value, source_id=2))
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert not [v for v in plan.revintage if v["obs_date"] == d.isoformat()]
        rejected = [v for v in plan.revintage_rejected if v["obs_date"] == d.isoformat()]
        assert len(rejected) == 1 and "outlier" in rejected[0]["why"]
        assert plan.cells_newly_dark == 1  # an honest loss on a trading day, reported
        # now add an honest earlier adj_close pull: it is used although it is older
        raw["adj_close"][d].append(RawCandidate(70.5, V1, 3))
        plan = plan_feature("xlp_full", [ResolvedRow(**{**r.__dict__, "cls": "", "matched_field": None,
                                                        "outlier_pct": None}) for r in rows], raw, repair_day=REPAIR_DAY)
        got = [v for v in plan.revintage if v["obs_date"] == d.isoformat()]
        assert len(got) == 1 and got[0]["value"] == 70.5 and got[0]["from_pull_date"] == V1.isoformat()
        assert plan.cells_newly_dark == 0

    def test_xlp_shape_holiday_junk_first_cell_is_cleared_and_the_ratio_moves(self) -> None:
        """r2 on grid-svr: xlp_full served 2010-01-01 (a holiday) = 0.7245 first and 90.08 last,
        ratio 124, 301 jumps, before AND after -- the dark holiday cell was refilled from the
        junk print. The repaired series must start on a trading day with a sane ratio."""
        days, rows, raw = _fixture()
        holidays = [date(2024, 1, 1), date(2024, 1, 6), date(2024, 1, 7), date(2024, 1, 14)]
        for i, h in enumerate(holidays):
            assert h not in raw["close"]
            raw["adj_close"][h] = [RawCandidate(0.7245, V3, 2)]
            rows.append(ResolvedRow(id=8000 + i, obs_date=h, vintage_date=V3, value=0.7245, source_id=2))
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert plan.served_before["first"][0] == "2024-01-01" and plan.served_before["ratio"] > 90
        assert plan.served_after["first"][0] == days[0].isoformat()
        assert plan.after_first_on_trading_day is True
        assert 1.0 < plan.served_after["ratio"] < 1.1 and plan.served_after["jumps_gt_10pct"] == 0
        assert plan.cells_phantom_cleared == 4 and plan.cells_newly_dark == 0
        assert not plan.revintage or all(v["value"] != 0.7245 for v in plan.revintage)
        assert plan.status == IN_SCOPE and plan.applicable
        assert plan.summary()["after_first_on_trading_day"] is True

    def test_clean_fixture_has_no_newly_dark_cells(self) -> None:
        _, rows, raw = _fixture()
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert (plan.cells_dark_before, plan.cells_newly_dark, plan.cells_restored) == (0, 0, 0)
        assert plan.newly_dark_share == 0.0 and plan.newly_dark_range is None

    def test_summary_carries_status_and_applicable(self) -> None:
        _, rows, raw = _fixture()
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        d = plan.summary()
        for k in ("status", "scope_reason", "feed", "feeds", "cells_dark_before", "cells_newly_dark",
                  "cells_phantom_cleared", "cells_restored", "newly_dark_share", "applicable"):
            assert k in d
        assert d["status"] == IN_SCOPE and d["applicable"] is True


class TestRunScope:
    def test_non_in_scope_features_never_touch_the_database(self) -> None:
        from unittest.mock import MagicMock
        engine = MagicMock()
        report = rep.run(engine, ["gold_full", "btc_full", "eth_usd_full", "xlp_full"], start=date(2010, 1, 1),
                         apply=False, run_tag="t", exclude={"xlp_full"},
                         clock=lambda: datetime(2026, 11, 10, 15, 0, tzinfo=timezone.utc))
        engine.connect.assert_not_called()
        engine.begin.assert_not_called()
        statuses = {p["feature"]: p["status"] for p in report["features"]}
        assert statuses == {"gold_full": NO_BASIS_SPLIT, "btc_full": OUT_OF_SCOPE,
                            "eth_usd_full": NO_BASIS_SPLIT, "xlp_full": EXCLUDED}
        assert report["by_status"] == {NO_BASIS_SPLIT: 2, OUT_OF_SCOPE: 1, EXCLUDED: 1}
        assert report["applicable_totals"]["features"] == 0
        assert all(p["applied"] is None for p in report["features"])
        assert report["complete"] is True and report["errors"] == 0

    def test_apply_refuses_a_non_applicable_plan(self) -> None:
        from unittest.mock import MagicMock
        days, rows, raw = _backfill_gap_fixture(15)
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert plan.status == NEEDS_BACKFILL
        with pytest.raises(RuntimeError, match="NEEDS_BACKFILL"):
            rep.apply_plan(MagicMock(), plan, run_tag="t", timeout_ms=1000)


class TestCliScope:
    def test_coverage_floor_must_be_a_share(self, capsys) -> None:
        assert rep.main(["--feature", "xlp_full", "--coverage-floor", "5"], engine=object()) == 2
        assert "--coverage-floor" in capsys.readouterr().err

    def test_exclude_is_repeatable(self) -> None:
        args = rep.build_parser().parse_args(["--feature", "a_full", "--exclude", "x_full", "--exclude", "y_full"])
        assert args.exclude == ["x_full", "y_full"] and args.coverage_floor == rep.DEFAULT_COVERAGE_FLOOR


class TestReviewOnlySafety:
    @staticmethod
    def manifest(rows=None, raw=None):
        if rows is None:
            _, rows, raw = _fixture()
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        return rep.build_manifest([plan], repair_day=REPAIR_DAY, start=date(2010, 1, 1), coverage_floor=0.01)

    def test_all_apply_entrypoints_refuse_without_connection(self, monkeypatch, capsys):
        from unittest.mock import MagicMock
        engine = MagicMock()
        clock = MagicMock(side_effect=AssertionError("clock must not run"))
        with pytest.raises(RuntimeError, match="apply disabled"):
            rep.run(engine, ["xlp_full"], start=V1, apply=True, run_tag="tag", clock=clock)
        for status in (IN_SCOPE, NEEDS_BACKFILL, EXCLUDED):
            with pytest.raises(RuntimeError, match="apply disabled"):
                rep.apply_plan(engine, rep.FeaturePlan("xlp_full", "XLP", "adj_close", status=status),
                               run_tag="tag", timeout_ms=1)
        monkeypatch.setattr(rep, "list_full_features", lambda *a: pytest.fail("enumeration attempted"))
        assert rep.main(["--all-full-features", "--apply", "--run-tag", "tag"], engine=engine) == 2
        assert "apply disabled" in capsys.readouterr().err
        assert not engine.mock_calls and not clock.mock_calls

    def test_surviving_first_holiday_is_not_applicable(self):
        _, rows, raw = _fixture()
        holiday = date(2024, 1, 1)
        rows.append(ResolvedRow(9999, holiday, V1, 70.0, 3))
        raw["adj_close"][holiday] = [RawCandidate(70.0, V1, 3)]
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert plan.retract and plan.after_first_on_trading_day is False
        assert plan.status == rep.UNSAFE_FIRST_CELL and not plan.applicable
        plan.status = IN_SCOPE
        plan.after_first_on_trading_day = None
        assert not plan.applicable

    def test_full_actions_and_canonical_order(self):
        _, rows, raw = _fixture()
        a = self.manifest(rows, raw)
        for dates in raw.values():
            for candidates in dates.values():
                candidates.reverse()
        b = self.manifest(list(reversed(rows)), raw)
        assert a == b
        assert len(a["payload"]["features"][0]["retract"]) == 45
        assert a["payload"]["executor"] == "disabled"
        rep.validate_review_manifest(a, reviewed_sha256=a["sha256"], current_manifest=b)

    @pytest.mark.parametrize("mutation", ["raw_value", "raw_source", "raw_pull", "raw_timestamp", "retracted", "insert"])
    def test_changed_inputs_are_not_admitted(self, mutation):
        _, rows, raw = _fixture()
        a = self.manifest(rows, raw)
        c = next(iter(raw["adj_close"].values()))[0]
        if mutation == "raw_value":
            c.value += 0.01
        if mutation == "raw_source":
            c.source_id += 1
        if mutation == "raw_pull":
            c.pull_date += timedelta(days=1)
        if mutation == "raw_timestamp":
            c.pull_timestamp = "2026-04-24T12:00:00+00:00"
        if mutation == "retracted":
            rows[0].retracted = True
        if mutation == "insert":
            rows.append(ResolvedRow(9999, rows[0].obs_date, REPAIR_DAY, 70.0, 3))
        b = self.manifest(rows, raw)
        with pytest.raises(ValueError, match="changed"):
            rep.validate_review_manifest(a, reviewed_sha256=a["sha256"], current_manifest=b)

    def test_tamper_and_missing_review_identity_rejected(self):
        import copy
        a = self.manifest()
        with pytest.raises(ValueError, match="required"):
            rep.validate_review_manifest(a, reviewed_sha256="", current_manifest=a)
        b = copy.deepcopy(a)
        b["payload"]["features"][0]["retract"][0]["value"] += 1
        with pytest.raises(ValueError, match="digest mismatch"):
            rep.validate_review_manifest(b, reviewed_sha256=a["sha256"], current_manifest=a)
        b["sha256"] = rep.digest(b["payload"])
        with pytest.raises(ValueError, match="changed"):
            rep.validate_review_manifest(b, reviewed_sha256=a["sha256"], current_manifest=b)

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_nonfinite_inputs_rejected(self, value):
        _, rows, raw = _fixture()
        rows[0].value = value
        with pytest.raises(ValueError, match="non-finite"):
            self.manifest(rows, raw)
        with pytest.raises(ValueError):
            rep.canonical_bytes({"value": value})

    def test_duplicate_action_rejected_even_if_rehashed(self):
        a = self.manifest()
        actions = a["payload"]["features"][0]["retract"]
        actions.append(actions[0].copy())
        a["sha256"] = rep.digest(a["payload"])
        with pytest.raises(ValueError, match="duplicate action"):
            rep.validate_review_manifest(a, reviewed_sha256=a["sha256"], current_manifest=a)

    def test_repair_day_collision_is_not_revintaged(self):
        days, rows, raw = _fixture()
        d = days[0]
        rows = [r for r in rows if r.obs_date != d]
        rows.append(ResolvedRow(9999, d, REPAIR_DAY, raw["close"][d][0].value, 3))
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert not any(r["obs_date"] == d.isoformat() for r in plan.revintage)
        assert any("key already used" in r["why"] for r in plan.revintage_rejected)

    def test_same_time_candidate_tie_is_deterministic_and_blocks_admission(self):
        days, rows, raw = _fixture()
        d = days[0]
        rows = [r for r in rows if r.obs_date != d or r.vintage_date == V2]
        raw["adj_close"][d] = [RawCandidate(70.0, V3, 3), RawCandidate(71.0, V3, 4)]
        a = self.manifest(rows, raw)
        raw["adj_close"][d].reverse()
        b = self.manifest(rows, raw)
        assert a == b
        p = a["payload"]["features"][0]
        assert p["candidate_ties"] and p["error"]


    @pytest.mark.parametrize("field,value", [("repair_day", "2026-11-11"), ("start", "2011-01-01"),
                                             ("run_tag", "changed")])
    def test_review_context_changes_require_new_identity(self, field, value):
        import copy
        a = self.manifest()
        b = copy.deepcopy(a)
        b["payload"][field] = value
        b["sha256"] = rep.digest(b["payload"])
        with pytest.raises(ValueError, match="changed"):
            rep.validate_review_manifest(a, reviewed_sha256=a["sha256"], current_manifest=b)

    @pytest.mark.parametrize("mutation", ["schema", "executor", "snapshot", "incomplete", "duplicate_feature"])
    def test_invalid_manifest_shapes_refused(self, mutation):
        a = self.manifest()
        p = a["payload"]
        if mutation == "schema":
            del p["policy"]
        if mutation == "executor":
            p["executor"] = "enabled"
        if mutation == "snapshot":
            p["snapshot_semantics"] = "trusted"
        if mutation == "incomplete":
            p["requested_features"].append("missing_full")
        if mutation == "duplicate_feature":
            p["features"].append(p["features"][0].copy())
        a["sha256"] = rep.digest(p)
        with pytest.raises(ValueError):
            rep.validate_review_manifest(a, reviewed_sha256=a["sha256"], current_manifest=a)

    def test_duplicate_resolved_identity_refused(self):
        _, rows, raw = _fixture()
        rows.append(rows[0])
        with pytest.raises(ValueError, match="duplicate resolved"):
            self.manifest(rows, raw)

    def test_raw_nonfinite_input_refused(self):
        _, rows, raw = _fixture()
        next(iter(raw["adj_close"].values()))[0].value = float("nan")
        with pytest.raises(ValueError, match="non-finite"):
            self.manifest(rows, raw)

    def test_timestamp_fractional_precision_order(self):
        from unittest.mock import MagicMock
        d = date(2024, 1, 2)
        conn = MagicMock()
        conn.execute.return_value = [(d, 70.0, datetime(2026, 5, 2, 6, tzinfo=timezone.utc), 3),
                                     (d, 71.0, datetime(2026, 5, 2, 6, microsecond=1, tzinfo=timezone.utc), 3)]
        into = {}
        rep.read_raw_chunk(conn, "YF:XLP:adj_close", d, d, into)
        assert max(into[d], key=rep.candidate_order).value == 71.0

    def test_output_never_overwrites_review_file(self, monkeypatch, tmp_path):
        output = tmp_path / "review.json"
        output.write_text("reviewed original")
        monkeypatch.setattr(rep, "run", lambda *a, **k: {"complete": True, "errors": 0})
        with pytest.raises(FileExistsError):
            rep.main(["--feature", "xlp_full", "--out", str(output)], engine=object())
        assert output.read_text() == "reviewed original"


    def test_cli_refuses_before_db_import_with_no_engine(self, monkeypatch):
        import builtins
        original = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name == "db":
                pytest.fail("database module imported for rejected apply")
            return original(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", guarded)
        assert rep.main(["--all-full-features", "--apply", "--run-tag", "t"], engine=None) == 2

    def test_incomplete_prefix_manifest_is_not_admitted(self):
        _, rows, raw = _fixture()
        plan = plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        m = rep.build_manifest([plan], repair_day=REPAIR_DAY, start=V1, coverage_floor=0.01,
                               requested_features=["xlp_full", "hon_full"], stopped="db_window")
        assert not m["payload"]["complete"]
        with pytest.raises(ValueError, match="incomplete"):
            rep.validate_review_manifest(m, reviewed_sha256=m["sha256"], current_manifest=m)

    def test_unsafe_first_cell_manifest_is_not_admitted(self):
        _, rows, raw = _fixture()
        holiday = date(2024, 1, 1)
        rows.append(ResolvedRow(9999, holiday, V1, 70.0, 3))
        raw["adj_close"][holiday] = [RawCandidate(70.0, V1, 3)]
        m = self.manifest(rows, raw)
        with pytest.raises(ValueError, match="unsafe feature"):
            rep.validate_review_manifest(m, reviewed_sha256=m["sha256"], current_manifest=m)
