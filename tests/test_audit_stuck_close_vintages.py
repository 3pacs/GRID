"""scripts/audit_stuck_close_vintages.py: cell classification (pure) and the DB-window refusal (#1194)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from price_close_contract import raw_capture_payload
from scripts import audit_stuck_close_vintages as audit
from scripts.audit_backdated_vintages import DbWindowRefused

UTC = timezone.utc
GSPC = "YF:^GSPC:close"
OD = date(2026, 10, 27)
INTRADAY = datetime(2026, 10, 27, 13, 31, tzinfo=UTC)   # before NY midnight: INCOMPLETE
LATE_SAME_DAY = datetime(2026, 10, 27, 22, 2, tzinfo=UTC)  # after the close, still INCOMPLETE
ELIGIBLE = datetime(2026, 10, 28, 13, 31, tzinfo=UTC)   # after 2026-10-28 04:00Z: POLICY_ELIGIBLE


def _raw(ts: datetime, value: float, *, series: str = GSPC, source: str = "yfinance",
         od: date = OD, payload: dict | None = None) -> dict:
    ticker, fld = series.split(":")[1:]
    return {"series_id": series, "obs_date": od, "pull_timestamp": ts, "value": value, "source_name": source,
            "raw_payload": payload if payload is not None else raw_capture_payload(ticker, fld, od, ts, "1d")}


def _res(vintage: date, value: float, *, od: date = OD, retracted: bool = False) -> dict:
    return {"obs_date": od, "vintage_date": vintage, "value": value, "retracted": retracted}


def test_fixture_payloads_carry_the_contract_states():
    assert _raw(INTRADAY, 1)["raw_payload"]["capture_state"] == "INCOMPLETE"
    assert _raw(ELIGIBLE, 1)["raw_payload"]["capture_state"] == "POLICY_ELIGIBLE"


def test_the_2026_10_27_shape_is_stuck_with_its_eligible_vintage_dropped():
    raw = [_raw(INTRADAY, 7701.57), _raw(LATE_SAME_DAY, 7722.60), _raw(ELIGIBLE, 7722.60)]
    out = audit.classify(raw, [_res(date(2026, 10, 27), 7701.57)])
    assert out["cells_with_eligible"] == 1
    assert out["stuck_cells"] == 1
    assert out["eligible_vintage_missing_cells"] == 1
    assert out["missing_vintage_cells"] == 1 and out["missing_vintages"] == 1
    (cell,) = out["stuck"]
    assert cell == {
        "obs_date": "2026-10-27", "served_vintage": "2026-10-27", "served_value": 7701.57,
        "eligible_vintage": "2026-10-28", "eligible_series": GSPC,
        "eligible_pull": ELIGIBLE.isoformat(), "eligible_value": 7722.60, "eligible_vintage_missing": True,
    }


def test_a_resolved_eligible_vintage_is_not_stuck():
    raw = [_raw(INTRADAY, 7701.57), _raw(ELIGIBLE, 7722.60)]
    out = audit.classify(raw, [_res(date(2026, 10, 27), 7701.57), _res(date(2026, 10, 28), 7722.60)])
    assert out["cells_with_eligible"] == 1
    assert out["stuck_cells"] == 0 and out["missing_vintages"] == 0


def test_a_later_repair_vintage_also_clears_the_cell():
    raw = [_raw(INTRADAY, 7701.57), _raw(ELIGIBLE, 7722.60)]
    out = audit.classify(raw, [_res(date(2026, 10, 27), 7701.57), _res(date(2026, 11, 14), 7722.60)])
    assert out["stuck_cells"] == 0
    assert out["missing_vintages"] == 1  # the 10-28 vintage itself is still absent (context only)


def test_a_retracted_eligible_vintage_is_stuck_but_not_dropped():
    raw = [_raw(INTRADAY, 7701.57), _raw(ELIGIBLE, 7722.60)]
    out = audit.classify(raw, [_res(date(2026, 10, 27), 7701.57),
                               _res(date(2026, 10, 28), 7722.60, retracted=True)])
    assert out["stuck_cells"] == 1
    assert out["eligible_vintage_missing_cells"] == 0
    assert out["stuck"][0]["eligible_vintage_missing"] is False
    assert out["missing_vintages"] == 0


def test_no_resolved_row_at_all_is_stuck():
    out = audit.classify([_raw(ELIGIBLE, 7722.60)], [])
    assert out["stuck_cells"] == 1
    assert out["stuck"][0]["served_vintage"] is None and out["stuck"][0]["served_value"] is None


def test_cells_without_an_eligible_row_are_not_judged():
    out = audit.classify([_raw(INTRADAY, 7701.57), _raw(LATE_SAME_DAY, 7722.60)],
                         [_res(date(2026, 10, 27), 7701.57)])
    assert out["cells_with_eligible"] == 0 and out["stuck_cells"] == 0


def test_a_spoofed_marker_on_the_wrong_series_is_not_eligible():
    payload = raw_capture_payload("^GSPC", "close", OD, ELIGIBLE, "1d")
    out = audit.classify([_raw(ELIGIBLE, 1.0, series="YF:^DJI:close", payload=payload)], [])
    assert out["cells_with_eligible"] == 0


def test_foreign_tiingo_rows_are_ignored_like_the_resolver_does(monkeypatch):
    monkeypatch.setattr(audit, "is_foreign_tiingo_row", lambda sid, src, od: src == "TIINGO")
    raw = [_raw(ELIGIBLE, 7722.60, source="TIINGO")]
    out = audit.classify(raw, [])
    assert out["raw_rows"] == 0 and out["cells"] == 0


def test_examples_are_capped_but_counts_are_not():
    raw, res = [], []
    for day in range(5, 10):  # 2026-10-05 .. 10-09, Mon-Fri
        od = date(2026, 10, day)
        raw.append(_raw(datetime(2026, 10, day + 1, 13, 31, tzinfo=UTC), 1.0, od=od))
        res.append(_res(od, 0.5, od=od))
    out = audit.classify(raw, res, max_examples=2)
    assert out["stuck_cells"] == 5
    assert len(out["stuck"]) == 2


def test_default_series_cover_the_index_and_both_price_fields():
    series = audit.default_series()
    assert GSPC in series and "YF:SPY:close" in series and "YF:XLP:adj_close" in series
    assert "YF:BTC-USD:close" in series
    assert len(series) == len(set(series))


def test_run_refuses_inside_the_db_window():
    with pytest.raises(DbWindowRefused):
        audit.run(object(), [GSPC], date(2026, 1, 1), date(2026, 11, 1),
                  clock=lambda: datetime(2026, 11, 14, 7, 30, tzinfo=UTC))


def test_main_refuses_an_inverted_window():
    with pytest.raises(SystemExit):
        audit.main(["--since", "2026-11-01", "--until", "2026-10-01"])
