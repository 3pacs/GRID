"""The re-vintage guard must not judge a dark cell against cells years away.

r3 on grid-svr (2026-11-09) rejected every honest DIS/DUK/LOW/MCD/LLY/MMM/MSTR
2010-2024 adj_close candidate, and the fresh ACN/HON Tiingo backfill, as
"-50..-94 % vs neighbours": the long stretch was raw-basis only, so its
nearest on-basis cells were the 2021+ prices. The fixture reproduces that
shape: 20 cells in 2010 served only on the raw close with on-basis adj_close
candidates, then 20 cells in 2021 on the adjusted basis at ~5x the level.
"""

from __future__ import annotations

from datetime import date, timedelta

from scripts.repair_full_panel_vintages import (
    IN_SCOPE,
    NeighbourReference,
    RawCandidate,
    ResolvedRow,
    plan_feature,
)

REPAIR_DAY = date(2026, 11, 10)
V_OLD, V_NEW = date(2026, 4, 1), date(2026, 5, 7)


def _bdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _long_gap_fixture(junk_index: int | None = None):
    early = _bdays(date(2010, 1, 4), 20)
    late = _bdays(date(2021, 4, 1), 20)
    rows: list[ResolvedRow] = []
    raw: dict[str, dict[date, list[RawCandidate]]] = {"close": {}, "adj_close": {}}
    rid = 0
    for i, d in enumerate(early):
        adj, close = 27.0 + i * 0.05, 32.0 + i * 0.05
        raw["close"][d] = [RawCandidate(close, V_OLD, 3)]
        raw["adj_close"][d] = [RawCandidate(0.6 if i == junk_index else adj, V_NEW, 3)]
        rid += 1
        rows.append(ResolvedRow(id=rid, obs_date=d, vintage_date=V_OLD, value=close, source_id=3))
    for i, d in enumerate(late):
        adj, close = 140.0 + i * 0.2, 150.0 + i * 0.2
        raw["close"][d] = [RawCandidate(close, V_OLD, 3)]
        raw["adj_close"][d] = [RawCandidate(adj, V_OLD, 3)]
        rid += 1
        rows.append(ResolvedRow(id=rid, obs_date=d, vintage_date=V_OLD, value=adj, source_id=3))
    return early, late, rows, raw


def test_long_dark_span_is_revintaged_not_rejected() -> None:
    early, _late, rows, raw = _long_gap_fixture()
    plan = plan_feature("dis_full", rows, raw, repair_day=REPAIR_DAY, feature_id=11)
    assert len(plan.retract) == 20                     # every raw-basis 2010 cell
    assert len(plan.revintage) == 20 and plan.revintage_rejected == []
    assert plan.cells_newly_dark == 0
    assert plan.status == IN_SCOPE and plan.applicable
    assert plan.served_after["first"] == [early[0].isoformat(), 27.0]


def test_junk_print_inside_a_long_dark_span_is_still_rejected() -> None:
    early, _late, rows, raw = _long_gap_fixture(junk_index=7)
    plan = plan_feature("dis_full", rows, raw, repair_day=REPAIR_DAY, feature_id=11)
    rejected = plan.revintage_rejected
    assert [x["obs_date"] for x in rejected] == [early[7].isoformat()]
    assert "outlier" in rejected[0]["why"]
    assert not [v for v in plan.revintage if v["value"] == 0.6]
    assert len(plan.revintage) == 19 and plan.cells_newly_dark == 1
    assert plan.summary()["revintage_rejected_by_why"] == {"every on-basis raw value is an outlier": 1}


def test_far_cells_are_no_evidence() -> None:
    far = {date(2021, 4, 1) + timedelta(days=i): 150.0 for i in range(10)}
    ref = NeighbourReference.from_values(far)
    assert ref.deviation(date(2010, 1, 4), 27.0) is None
    near = {**far, **{date(2010, 1, 5) + timedelta(days=i): 27.0 for i in range(5)}}
    ref = NeighbourReference.from_values(near)
    assert abs(ref.deviation(date(2010, 1, 4), 27.0)) < 1e-9
    assert ref.deviation(date(2010, 1, 4), 0.6) < -0.9
