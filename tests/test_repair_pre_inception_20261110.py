"""Pre-inception junk and the trading-day flag in scripts/repair_full_panel_vintages.py.

Why (measured on grid-svr 2026-11-09/10): ``xlre_full`` serves a 2010-01-01 cell
(and others) dated before XLRE was listed (2015-10-08). The value (~1.0339) is
present in BOTH raw fields, so the classifier calls it AMBIGUOUS and keeps it, and
the neighbouring junk cells anchor the outlier median, so the outlier check misses
them too: the r3 plan retracted 61,692 rows and still served the junk first
(first-to-last ratio ~42.6). The same run flagged nasdaq_full, russell2000_full,
pypl_full and rxt_full as "first served cell is not a trading day" on ordinary
Mondays, because the test was "a raw close row exists".

Pure planning, no DB. The module is read through ``rep.<name>`` so the tests fail
(not error at import) on a script that lacks the new names.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import scripts.repair_full_panel_vintages as rep
from ingestion.market_calendar import is_market_open

REPAIR_DAY = date(2026, 11, 10)
V1, V2, V3 = date(2026, 4, 24), date(2026, 4, 29), date(2026, 5, 2)
JUNK = 1.0339


def _trading_days(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if is_market_open(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def _xlre_fixture():
    """Pre-inception junk equal in both raw fields + real post-inception history.

    Junk: the 2010-01-01 holiday, plus 12 weekdays before the 2015-10-08 listing.
    Real: 40 trading days from 2015-10-08 at ~30 (adj) / ~33 (close), three
    vintages whose latest alternates basis (the original defect).
    """
    rows: list[rep.ResolvedRow] = []
    raw: dict[str, dict[date, list[rep.RawCandidate]]] = {"close": {}, "adj_close": {}}
    rid = 0
    junk_days = [date(2010, 1, 1)] + _trading_days(date(2015, 9, 21), 12)
    assert all(d < date(2015, 10, 8) for d in junk_days)
    for d in junk_days:
        for fld in ("close", "adj_close"):
            raw[fld][d] = [rep.RawCandidate(JUNK, V3, 2)]
        rid += 1
        rows.append(rep.ResolvedRow(id=rid, obs_date=d, vintage_date=V3, value=JUNK, source_id=2))
    days = _trading_days(date(2015, 10, 8), 40)
    for i, d in enumerate(days):
        adj, close = 30.0 + i * 0.05, 33.0 + i * 0.05
        raw["adj_close"][d] = [rep.RawCandidate(adj, V1, 3)]
        raw["close"][d] = [rep.RawCandidate(close, V1, 3), rep.RawCandidate(close, V2, 2)]
        for vd, val in ((V1, adj), (V2, close), (V3, close if i % 2 == 0 else adj)):
            rid += 1
            rows.append(rep.ResolvedRow(id=rid, obs_date=d, vintage_date=vd, value=val, source_id=3))
    return junk_days, days, rows, raw


class TestPreInception:
    def test_inception_map_is_explicit_and_cited(self) -> None:
        assert rep.INCEPTION_DATES["XLRE"] == date(2015, 10, 8)
        assert rep.INCEPTION_DATES["XLC"] == date(2018, 6, 19)
        assert all(rep.is_trading_day(d) for d in rep.INCEPTION_DATES.values())
        src = Path(rep.__file__).read_text()
        assert "ssga.com" in src  # the primary source is cited next to the map

    def test_xlre_shape_junk_retracted_and_served_after_starts_at_inception(self) -> None:
        junk_days, days, rows, raw = _xlre_fixture()
        plan = rep.plan_feature("xlre_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        # before: the junk cells are served first, the ratio is the junk-to-real jump
        assert plan.served_before["first"][0] == "2010-01-01"
        assert plan.served_before["ratio"] > 30
        # every junk row is classed PRE_INCEPTION and retracted under its own reason
        pre = [r for r in plan.retract if r["cls"] == rep.PRE_INCEPTION]
        assert {r["obs_date"] for r in pre} == {d.isoformat() for d in junk_days}
        assert all("pre-inception" in r["reason"] and "2015-10-08" in r["reason"] for r in pre)
        assert plan.by_class[rep.PRE_INCEPTION] == len(junk_days)
        assert plan.inception_date == "2015-10-08"
        # never re-vintaged, although both raw fields hold a "valid" candidate for the date
        assert not [v for v in plan.revintage if v["obs_date"] < "2015-10-08"]
        assert not [v for v in plan.revintage_rejected if v["obs_date"] < "2015-10-08"]
        # after: starts on the listing day, a trading day, sane ratio, no jumps
        assert plan.served_after["first"][0] == days[0].isoformat() == "2015-10-08"
        assert plan.after_first_on_trading_day is True
        assert 1.0 < plan.served_after["ratio"] < 1.2 and plan.served_after["jumps_gt_10pct"] == 0
        # pre-inception cells going dark are phantom, never against the floor
        assert plan.cells_pre_inception_cleared == len(junk_days)
        assert plan.cells_phantom_cleared == len(junk_days) and plan.cells_newly_dark == 0
        assert plan.newly_dark_share == 0.0
        assert plan.status == rep.IN_SCOPE and plan.applicable

    def test_junk_does_not_anchor_the_post_inception_outlier_check(self) -> None:
        """The first real cells (~30) sit next to ten-odd junk cells (~1.03). With the junk as
        neighbours they would be 'outliers' themselves; they must survive."""
        _, days, rows, raw = _xlre_fixture()
        plan = rep.plan_feature("xlre_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert rep.OUTLIER not in plan.by_class
        assert not [r for r in plan.retract if r["obs_date"] >= days[0].isoformat() and r["cls"] == rep.OUTLIER]

    def test_without_the_override_the_junk_survives(self) -> None:
        """The failure being fixed: a ticker outside the map keeps the AMBIGUOUS junk and serves it first."""
        _, _, rows, raw = _xlre_fixture()
        plan = rep.plan_feature("xlq_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert plan.served_after["first"][0] == "2010-01-01"
        assert plan.after_first_on_trading_day is False  # a holiday: the flag now fires for the right reason

    def test_already_retracted_pre_inception_rows_are_left_alone(self) -> None:
        junk_days, _, rows, raw = _xlre_fixture()
        for r in rows:
            if r.obs_date == junk_days[1]:
                r.retracted = True
        plan = rep.plan_feature("xlre_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert junk_days[1].isoformat() not in {r["obs_date"] for r in plan.retract}
        assert plan.cells_dark_before == 1
        assert plan.cells_pre_inception_cleared == len(junk_days) - 1

    def test_apply_writes_retractions_with_the_pre_inception_reason(self) -> None:
        junk_days, _, rows, raw = _xlre_fixture()
        plan = rep.plan_feature("xlre_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)

        class _Res:
            rowcount = 1

        calls: list[dict] = []

        class _Conn:
            def execute(self, sql, params=None):
                if params and "reason" in params:
                    calls.append(params)
                else:
                    return type("S", (), {"scalar": lambda self: True})()
                return _Res()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class _Engine:
            def begin(self):
                return _Conn()

        done = rep.apply_plan(_Engine(), plan, run_tag="t", timeout_ms=1000)  # type: ignore[arg-type]
        assert done["retracted"] == len(plan.retract)
        assert sum("pre-inception" in c["reason"] for c in calls) == len(junk_days)


class TestTradingDayFlag:
    def test_calendar_corrections(self) -> None:
        assert rep.is_trading_day(date(2015, 6, 19))       # Juneteenth was not an NYSE holiday before 2022
        assert rep.is_trading_day(date(2021, 6, 18))       # nor its Friday observance
        assert not rep.is_trading_day(date(2022, 6, 20))   # first observed Monday
        assert not rep.is_trading_day(date(2012, 10, 29))  # Sandy
        assert not rep.is_trading_day(date(2018, 12, 5))   # Bush day of mourning
        assert not rep.is_trading_day(date(2025, 1, 9))    # Carter day of mourning
        assert not rep.is_trading_day(date(2026, 11, 26))  # Thanksgiving
        assert not rep.is_trading_day(date(2026, 11, 7))   # Saturday
        assert rep.is_trading_day(date(2026, 11, 2))       # an ordinary Monday

    def _monday_fixture(self, with_close_row: bool):
        days = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4), date(2026, 11, 5), date(2026, 11, 6)]
        assert days[0].weekday() == 0
        rows, raw = [], {"close": {}, "adj_close": {}}
        for i, d in enumerate(days):
            v = 50.0 + i * 0.1
            raw["adj_close"][d] = [rep.RawCandidate(v, V1, 3)]
            if with_close_row or i > 0:
                raw["close"][d] = [rep.RawCandidate(v + 5, V1, 3)]
            rows.append(rep.ResolvedRow(id=i + 1, obs_date=d, vintage_date=V1, value=v, source_id=3))
        return rows, raw

    def test_ordinary_monday_without_a_raw_close_row_is_no_longer_flagged(self) -> None:
        rows, raw = self._monday_fixture(with_close_row=False)
        assert date(2026, 11, 2) not in raw["close"]
        plan = rep.plan_feature("nasdaq_full", rows, raw, repair_day=REPAIR_DAY, feature_id=3)
        assert plan.served_after["first"][0] == "2026-11-02"
        assert plan.after_first_on_trading_day is True

    def test_a_holiday_first_cell_is_still_flagged_even_with_a_raw_close_row(self) -> None:
        d = date(2026, 11, 26)  # Thanksgiving
        rows = [rep.ResolvedRow(id=1, obs_date=d, vintage_date=V1, value=50.0, source_id=3),
                rep.ResolvedRow(id=2, obs_date=d + timedelta(days=2), vintage_date=V1, value=50.1, source_id=3)]
        raw = {"close": {d: [rep.RawCandidate(55.0, V1, 3)]},
               "adj_close": {d: [rep.RawCandidate(50.0, V1, 3)],
                             d + timedelta(days=2): [rep.RawCandidate(50.1, V1, 3)]}}
        plan = rep.plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert plan.after_first_on_trading_day is False

    def test_trading_day_without_raw_close_going_dark_is_a_real_loss_not_phantom(self) -> None:
        """Same proxy, mirror fault: a served cell on a real trading day whose raw close row is
        absent used to be hidden as 'phantom'. It is an honest newly-dark cell."""
        days = _trading_days(date(2024, 1, 2), 30)
        rows, raw = [], {"close": {}, "adj_close": {}}
        for i, d in enumerate(days):
            v = 70.0 + i * 0.05
            raw["adj_close"][d] = [rep.RawCandidate(v, V1, 3)]
            raw["close"][d] = [rep.RawCandidate(v + 8, V1, 3)]
            rows.append(rep.ResolvedRow(id=i + 1, obs_date=d, vintage_date=V1, value=v, source_id=3))
        gap = days[12]
        raw["adj_close"].pop(gap)
        raw["close"].pop(gap)
        rows = [r for r in rows if r.obs_date != gap]
        rows.append(rep.ResolvedRow(id=900, obs_date=gap, vintage_date=V3, value=0.6, source_id=2))  # junk, no raw row
        plan = rep.plan_feature("xlp_full", rows, raw, repair_day=REPAIR_DAY)
        assert [r["obs_date"] for r in plan.retract] == [gap.isoformat()]
        assert plan.cells_newly_dark == 1 and plan.cells_phantom_cleared == 0

    def test_junk_print_on_a_market_closed_day_is_phantom_even_with_a_raw_close_row(self) -> None:
        """grid-svr 2026-11-10: XLRE/XLP hold 0.91-1.0 prints (one writer, one pull) in BOTH raw
        fields on 2018-12-05 and 2025-01-09, the days of mourning. They are not a loss."""
        mourning = date(2018, 12, 5)
        days = [d for d in _trading_days(date(2018, 11, 15), 31) if d != mourning]  # is_market_open lacks the closure
        assert mourning not in days and not rep.is_trading_day(mourning)
        rows, raw = [], {"close": {}, "adj_close": {}}
        for i, d in enumerate(days):
            v = 26.0 + i * 0.05
            raw["adj_close"][d] = [rep.RawCandidate(v, V1, 3)]
            raw["close"][d] = [rep.RawCandidate(v + 7, V1, 3)]
            rows.append(rep.ResolvedRow(id=i + 1, obs_date=d, vintage_date=V1, value=v, source_id=3))
        for fld in ("close", "adj_close"):
            raw[fld][mourning] = [rep.RawCandidate(0.9972, V3, 2)]
        rows.append(rep.ResolvedRow(id=900, obs_date=mourning, vintage_date=V3, value=0.9972, source_id=2))
        plan = rep.plan_feature("xlre_full", rows, raw, repair_day=REPAIR_DAY, feature_id=7)
        assert [r["obs_date"] for r in plan.retract] == [mourning.isoformat()]
        assert plan.cells_phantom_cleared == 1 and plan.cells_newly_dark == 0
        assert plan.status == rep.IN_SCOPE
