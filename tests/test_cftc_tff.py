"""CFTC Traders in Financial Futures: fail-closed parsing and PIT release payloads.

``requests.get`` is patched in the module under test and sockets are refused,
so no test reaches the network. The engine is a MagicMock; no database.
"""

from __future__ import annotations

import math
import socket
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.altdata import cftc_tff
from ingestion.altdata.cftc_markets import RELEASE_RULE
from ingestion.altdata.cftc_tff import (
    TFF_METRICS,
    CFTCTFFPuller,
    parse_tff_records,
    tff_series_id,
)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    calls: list[object] = []

    def _refuse(*args, **kwargs):
        calls.append(args)
        raise AssertionError("network access attempted in an offline test")

    monkeypatch.setattr(socket.socket, "connect", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    monkeypatch.setattr(cftc_tff.time, "sleep", lambda _s: None)
    yield
    assert calls == []


def _row(code: str = "043602", report_date: str = "2026-11-17T00:00:00.000", **over: Any) -> dict[str, Any]:
    row = {
        "cftc_contract_market_code": code,
        "report_date_as_yyyy_mm_dd": report_date,
        "market_and_exchange_names": "UST 10Y NOTE - CHICAGO BOARD OF TRADE",
        "dealer_positions_long_all": "500000",
        "dealer_positions_short_all": "400000",
        "asset_mgr_positions_long": "3000000",
        "asset_mgr_positions_short": "1000000",
        "lev_money_positions_long": "400000",
        "lev_money_positions_short": "2200000",
        "open_interest_all": "6000000",
        "id": "row-1",
    }
    row.update(over)
    return row


def test_cftc_tff_parser_fail_closed() -> None:
    assert tff_series_id("043602", "lev_money_net") == "cftc_tff.043602.lev_money_net"
    with pytest.raises(ValueError):
        tff_series_id("BOGUS", "lev_money_net")
    with pytest.raises(ValueError):
        tff_series_id("043602", "net_speculative")

    rows = [
        _row(),
        # Wrong market code must be rejected, never substituted.
        {"cftc_contract_market_code": "999999", "report_date_as_yyyy_mm_dd": "2026-11-17"},
        # Missing a raw field: skipped, not zero-filled.
        _row(report_date="2026-11-10T00:00:00.000", lev_money_positions_short=None),
    ]
    out = parse_tff_records("043602", rows)
    assert len(out.reports) == 1
    assert [s["reason"] for s in out.skipped] == ["market_code_mismatch", "incomplete_metrics"]
    rep = out.reports[0]
    assert rep.report_date == date(2026, 11, 17)
    assert rep.metrics["lev_money_net"] == -1_800_000.0
    assert rep.metrics["asset_mgr_net"] == 2_000_000.0
    assert rep.metrics["dealer_net"] == 100_000.0
    assert rep.metrics["lev_money_pct_oi"] == pytest.approx(-30.0)


def test_conflicting_rows_for_one_report_date_drop_the_date() -> None:
    out = parse_tff_records("043602", [_row(), _row(open_interest_all="6000001", id="row-2")])
    assert out.reports == []
    assert out.skipped[0]["reason"] == "conflicting_rows_same_report_date"


def _offline_puller(monkeypatch, pages: list[list[dict[str, Any]]]) -> tuple[CFTCTFFPuller, list[dict], MagicMock]:
    puller = CFTCTFFPuller.__new__(CFTCTFFPuller)
    puller.engine = MagicMock()
    puller.source_id = 77
    puller._get_latest_date = lambda sid: None  # bootstrap window
    puller._get_existing_dates = lambda sid, conn, start_date=None, end_date=None: set()
    inserted: list[dict] = []
    puller._insert_raw = lambda **kw: inserted.append(kw)
    responses = []
    for page in pages:
        resp = MagicMock()
        resp.json.return_value = page
        resp.raise_for_status.return_value = None
        responses.append(resp)
    fake_get = MagicMock(side_effect=responses)
    monkeypatch.setattr(cftc_tff.requests, "get", fake_get)
    return puller, inserted, fake_get


def test_pull_stores_every_metric_with_the_cftc_release_payload(monkeypatch) -> None:
    puller, inserted, fake_get = _offline_puller(monkeypatch, [[_row()]])
    result = puller.pull_all(codes=["043602"])

    assert result["status"] == "SUCCESS" and result["rows_inserted"] == len(TFF_METRICS)
    assert fake_get.call_count == 1
    url = fake_get.call_args.args[0]
    params = fake_get.call_args.kwargs["params"]
    assert url.endswith("/resource/gpe5-46if.json")
    assert params["cftc_contract_market_code"] == "043602"

    assert {r["series_id"] for r in inserted} == {tff_series_id("043602", m) for m in TFF_METRICS}
    for r in inserted:
        assert r["obs_date"] == date(2026, 11, 17)
        payload = r["raw_payload"]
        # compute_release(report_date).to_payload(): Tuesday positions,
        # released Friday 15:30 ET (20:30 UTC in November), stored as a floor.
        assert payload["report_date"] == "2026-11-17"
        assert datetime.fromisoformat(payload["release_at"]) == datetime(2026, 11, 20, 20, 30, tzinfo=timezone.utc)
        assert payload["release_rule"] == RELEASE_RULE
        assert payload["release_is_floor"] is True
        assert payload["market_code"] == "043602" and payload["root"] == "ZN"
        assert payload["socrata_dataset"] == "gpe5-46if"


def test_report_date_without_a_release_rule_is_not_stored(monkeypatch) -> None:
    # A Thursday position date has no CFTC release rule -> release_at None.
    puller, inserted, _ = _offline_puller(monkeypatch, [[_row(report_date="2026-11-12T00:00:00.000")]])
    result = puller.pull_all(codes=["043602"])
    assert inserted == []
    assert result["status"] == "PARTIAL" and result["no_release_rows"] == len(TFF_METRICS)


def test_one_market_failing_does_not_stop_the_rest(monkeypatch) -> None:
    puller, inserted, _ = _offline_puller(monkeypatch, [[_row(code="043602")], [_row(code="044601")]])
    calls = {"n": 0}

    def _flaky_insert(**kw):
        calls["n"] += 1
        if kw["series_id"].startswith("cftc_tff.043602."):
            raise RuntimeError("synthetic database failure")
        inserted.append(kw)

    puller._insert_raw = _flaky_insert
    result = puller.pull_all(codes=["043602", "044601"])
    assert result["failed"] == ["043602"]
    assert result["status"] == "PARTIAL"
    assert {r["series_id"].split(".")[1] for r in inserted} == {"044601"}


def test_untracked_codes_are_reported_not_fetched(monkeypatch) -> None:
    puller, inserted, fake_get = _offline_puller(monkeypatch, [])
    result = puller.pull_all(codes=["088691"])  # gold: legacy COT only, not in TFF
    assert fake_get.call_count == 0 and inserted == []
    assert result["status"] == "FAILED" and result["unknown_codes"] == ["088691"]


def test_a_malformed_first_market_still_lets_a_later_valid_market_run(monkeypatch) -> None:
    """Codex (6011669449): parsing sat outside the per-market guard, so one bad row shape stopped every later
    market. The whole lifecycle is guarded now, and the error is recorded against that market with its stage."""
    puller, inserted, _ = _offline_puller(monkeypatch, [["not a record"], [_row(code="044601")]])
    result = puller.pull_all(codes=["043602", "044601"])
    assert result["failed"] == ["043602"] and result["status"] == "PARTIAL"
    assert result["errors"]["043602"].startswith("parse: ")
    assert {r["series_id"].split(".")[1] for r in inserted} == {"044601"}
    assert result["rows_inserted"] == len(TFF_METRICS)


def test_a_failed_latest_date_lookup_is_recorded_for_that_market_only(monkeypatch) -> None:
    puller, inserted, fake_get = _offline_puller(monkeypatch, [[_row(code="044601")]])

    def _latest(sid):
        if ".043602." in sid:
            raise RuntimeError("synthetic latest-date failure")
        return None

    puller._get_latest_date = _latest
    result = puller.pull_all(codes=["043602", "044601"])
    assert result["failed"] == ["043602"]
    assert result["errors"]["043602"].startswith("latest_date: RuntimeError")
    assert fake_get.call_count == 1                         # the failing market was never fetched
    assert {r["series_id"].split(".")[1] for r in inserted} == {"044601"}


def test_every_failure_names_its_market_and_stage(monkeypatch) -> None:
    puller, inserted, _ = _offline_puller(monkeypatch, [[_row(code="043602")], [_row(code="044601")]])

    def _flaky_insert(**kw):
        if kw["series_id"].startswith("cftc_tff.043602."):
            raise RuntimeError("synthetic database failure")
        inserted.append(kw)

    puller._insert_raw = _flaky_insert
    result = puller.pull_all(codes=["043602", "044601"])
    assert result["errors"] == {"043602": "write: RuntimeError: synthetic database failure"}


# ── Non-finite and out-of-range inputs (Codex 6011729288) ──────────────────

_BAD_NUMBERS = ["nan", "NaN", "inf", "-inf", "Infinity", "1e400", float("nan"), float("inf"),
                float("-inf"), True, False, "abc", "0x10", [1], {"v": 1}]


@pytest.mark.parametrize("field", list(cftc_tff.TFF_RAW_FIELDS.values()))
@pytest.mark.parametrize("bad", _BAD_NUMBERS, ids=repr)
def test_non_finite_or_non_numeric_fields_skip_the_row(field: str, bad: Any) -> None:
    out = parse_tff_records("043602", [_row(**{field: bad})])
    assert out.reports == []
    assert len(out.skipped) == 1
    assert out.skipped[0]["reason"] == "incomplete_metrics"
    assert out.skipped[0]["detail"] == f"non_finite_or_non_numeric field {field}"


def test_nan_open_interest_no_longer_slips_past_the_positive_check() -> None:
    # Before: float("nan") <= 0 is False, so a NaN OI reached lev_money_pct_oi.
    for oi in ("nan", float("nan")):
        out = parse_tff_records("043602", [_row(open_interest_all=oi)])
        assert out.reports == [] and "non_finite" in out.skipped[0]["detail"]
    for oi in ("0", "-1", 0, -6000000):
        out = parse_tff_records("043602", [_row(open_interest_all=oi)])
        assert out.reports == [] and out.skipped[0]["detail"] == "non_positive_open_interest"


@pytest.mark.parametrize("field,value,detail", [
    ("lev_money_positions_long", "-1", "negative_position lev_money_positions_long"),
    ("dealer_positions_short_all", "-0.5", "negative_position dealer_positions_short_all"),
    ("asset_mgr_positions_long", "6000001", "position_exceeds_open_interest asset_mgr_positions_long"),
])
def test_out_of_range_positions_skip_the_row(field: str, value: str, detail: str) -> None:
    out = parse_tff_records("043602", [_row(**{field: value})])
    assert out.reports == [] and out.skipped[0]["detail"] == detail


@pytest.mark.parametrize("raw_rd", ["", "NaT", "nat", "garbage", "2026-13-01", True, float("nan"), 20261117,
                                    20261117.0, "20261117", "Nov 17 2026"], ids=repr)
def test_bad_report_dates_skip_the_row(raw_rd: Any) -> None:
    # pandas parses "" and "NaT" to NaT; that is not a date and must not reach a write.
    out = parse_tff_records("043602", [_row(report_date=raw_rd)])
    assert out.reports == [] and out.skipped[0]["reason"] == "bad_report_date"


def test_every_accepted_metric_is_finite() -> None:
    out = parse_tff_records("043602", [_row(open_interest_all="1e-300", lev_money_positions_long="0",
                                            lev_money_positions_short="0", dealer_positions_long_all="0",
                                            dealer_positions_short_all="0", asset_mgr_positions_long="0",
                                            asset_mgr_positions_short="0")])
    assert len(out.reports) == 1
    assert all(math.isfinite(v) for v in out.reports[0].metrics.values())


def test_rejected_rows_never_reach_a_write(monkeypatch) -> None:
    page = [
        _row(report_date="2026-11-10T00:00:00.000", open_interest_all="nan", id="bad-oi"),
        _row(report_date="2026-11-03T00:00:00.000", lev_money_positions_long="inf", id="bad-inf"),
        _row(report_date="", id="bad-date"),
        _row(id="good"),  # 2026-11-17
    ]
    puller, inserted, _ = _offline_puller(monkeypatch, [page])
    result = puller.pull_all(codes=["043602"])
    assert result["skipped_rows"] == 3 and result["status"] == "PARTIAL"
    assert {r["obs_date"] for r in inserted} == {date(2026, 11, 17)}
    assert len(inserted) == len(TFF_METRICS)
    assert all(math.isfinite(r["value"]) for r in inserted)
