"""Pure tests for godview/read_model.py (G8): as_of, staleness, ledger reasons, payload shape."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from godview import read_model as rm

NOW = datetime(2026, 10, 2, 18, 0, tzinfo=timezone.utc)  # Friday 14:00 ET


# ---------------------------------------------------------------------------
# as_of
# ---------------------------------------------------------------------------


def test_as_of_default_is_server_now():
    assert rm.resolve_as_of(None, now=NOW) == (NOW, "server_now")
    assert rm.resolve_as_of("  ", now=NOW) == (NOW, "server_now")


def test_as_of_date_is_end_of_that_et_day():
    cutoff, source = rm.resolve_as_of("2026-09-25", now=NOW)
    assert source == "request"
    # 2026-09-25 23:59:59.999999 EDT == 2026-09-26 03:59:59.999999Z
    assert cutoff == datetime(2026, 9, 26, 3, 59, 59, 999999, tzinfo=timezone.utc)
    assert rm.et_date(cutoff) == date(2026, 9, 25)


def test_as_of_today_is_capped_at_now():
    cutoff, _ = rm.resolve_as_of("2026-10-02", now=NOW)
    assert cutoff == NOW


def test_as_of_datetime_with_offset():
    cutoff, _ = rm.resolve_as_of("2026-09-25T20:00:00Z", now=NOW)
    assert cutoff == datetime(2026, 9, 25, 20, tzinfo=timezone.utc)
    cutoff, _ = rm.resolve_as_of("2026-09-25T16:00:00-04:00", now=NOW)
    assert cutoff == datetime(2026, 9, 25, 20, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "raw",
    ["2026-10-03", "2026-10-05T00:00:00Z", "2026-09-25T20:00:00", "yesterday", "2026-13-01"],
)
def test_as_of_refuses_future_naive_and_garbage(raw):
    with pytest.raises(rm.AsOfError):
        rm.resolve_as_of(raw, now=NOW)


# ---------------------------------------------------------------------------
# Staleness and ledger reasons
# ---------------------------------------------------------------------------


def test_fed_stale_after_nine_days():
    as_of = datetime(2026, 10, 2, 18, tzinfo=timezone.utc)
    assert not rm.fed_is_stale(date(2026, 9, 23), as_of)  # 9 days
    assert rm.fed_is_stale(date(2026, 9, 22), as_of)  # 10 days


def test_cftc_stale_ten_days_after_release():
    release = datetime(2026, 9, 25, 19, 30, tzinfo=timezone.utc)
    assert not rm.cftc_is_stale(release, release + timedelta(days=10))
    assert rm.cftc_is_stale(release, release + timedelta(days=10, seconds=1))


def test_gex_stale_when_older_than_previous_session():
    monday = datetime(2026, 9, 28, 15, tzinfo=timezone.utc)
    assert not rm.gex_is_stale(date(2026, 9, 25), monday)  # Friday is the previous session
    assert rm.gex_is_stale(date(2026, 9, 24), monday)
    tuesday = datetime(2026, 9, 29, 15, tzinfo=timezone.utc)
    assert rm.gex_is_stale(date(2026, 9, 25), tuesday)


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (None, "never_run"),
        ("failed", "writer_failed"),
        ("partial_blocked_by_legacy", "blocked_by_legacy_rows"),
        ("inputs_missing", "inputs_missing"),
        ("inputs_stale", "inputs_stale"),
        ("non_session", "non_session"),
        ("no_completed_capture", "no_completed_capture"),
        ("no_verified_spot", "no_verified_spot"),
        ("complete", "no_rows_available_at_as_of"),
        ("noop", "no_rows_available_at_as_of"),
    ],
)
def test_reason_from_ledger(status, reason):
    run = None if status is None else {"status": status}
    assert rm.reason_from_ledger(run) == reason


def test_cftc_pillar_status():
    A, S, U = rm.STATUS_AVAILABLE, rm.STATUS_STALE, rm.STATUS_UNAVAILABLE
    assert rm.cftc_pillar_status([A, A]) == A
    assert rm.cftc_pillar_status([S, S]) == S
    assert rm.cftc_pillar_status([A, U]) == rm.STATUS_PARTIAL
    assert rm.cftc_pillar_status([A, S]) == rm.STATUS_PARTIAL
    assert rm.cftc_pillar_status([U, U]) == U


# ---------------------------------------------------------------------------
# Payloads
# ---------------------------------------------------------------------------

_RUN = {
    "pillar": "fed_liquidity",
    "run_id": "11111111-1111-1111-1111-111111111111",
    "status": "complete",
    "started_at": datetime(2026, 9, 24, 21, 30, tzinfo=timezone.utc),
    "finished_at": datetime(2026, 9, 24, 21, 31, tzinfo=timezone.utc),
    "rows_written": 1,
    "rows_skipped": 0,
    "reasons": {},
    "code_sha": "abc123",
}


def _receipt(obs: date, release: datetime, available: datetime, provenance: str = "measured") -> dict:
    return {
        "release_at": release,
        "available_at": available,
        "availability_basis": "observed_acquisition",
        "provenance": provenance,
        "source_ref": {"inputs": []},
        "run_id": _RUN["run_id"],
        "code_sha": "abc123",
        "coverage_fraction": 1.0,
    }


def _fed(obs: date = date(2026, 9, 23), **over) -> dict:
    row = {
        "obs_date": obs,
        "fed_assets_walcl": 6_746_548.0,
        "treasury_tga_wtregen": 877_028.0,
        "reverse_repo_rrp": 5_375.0,
        "net_liquidity_usd_m": 5_864_145.0,
        "rrp_as_pct_of_peak": 0.2,
        "delta_1w_m": -12_000.0,
        "delta_4w_m": None,
        "liquidity_regime": "insufficient_history",
        **_receipt(obs, datetime(2026, 9, 24, 20, 30, tzinfo=timezone.utc), datetime(2026, 9, 24, 21, 2, tzinfo=timezone.utc)),
    }
    row.update(over)
    return row


def test_unavailable_fed_pillar_has_no_values():
    p = rm.build_fed_pillar(None, None, NOW)
    assert p["status"] == "unavailable" and p["available"] is False
    assert p["reason"] == "never_run"
    assert p["data"] is None and p["as_of"] is None and p["available_at"] is None


def test_available_fed_pillar_keeps_nulls_null():
    p = rm.build_fed_pillar(_fed(), _RUN, NOW)
    assert p["status"] == "available" and p["reason"] is None
    assert p["as_of"] == "2026-09-23"
    assert p["available_at"] == "2026-09-24T21:02:00+00:00"
    assert p["provenance"] == "measured" and p["estimated"] is False
    assert p["data"]["delta_4w_m"] is None  # never 0
    assert p["data"]["delta_1w_m"] == -12_000.0
    assert p["data"]["unit"] == "USD millions"
    assert p["last_run"]["status"] == "complete"


def test_fed_pillar_zero_delta_is_a_real_zero():
    p = rm.build_fed_pillar(_fed(delta_1w_m=0), _RUN, NOW)
    assert p["data"]["delta_1w_m"] == 0.0


def test_stale_fed_pillar_still_serves_the_value_with_reason():
    as_of = datetime(2026, 10, 3, 18, tzinfo=timezone.utc)  # 10 days after 09-23
    p = rm.build_fed_pillar(_fed(), _RUN, as_of)
    assert p["status"] == "stale" and p["reason"] == "stale" and p["available"] is True
    assert p["data"]["net_liquidity_usd_m"] == 5_864_145.0


def _cftc(root: str, report: date, release: datetime, *, z3: float | None = 1.2) -> dict:
    return {
        "contract_code": root,
        "cftc_market_code": rm.CFTC_TRACKED[[r for r, _, _ in rm.CFTC_TRACKED].index(root)][1],
        "market_name": "label",
        "contract_name": "label",
        "asset_class": "EQUITY",
        "report_date": report,
        "total_open_interest": 1000,
        "commercial_long": 400,
        "commercial_short": 500,
        "commercial_net": -100,
        "noncommercial_long": 300,
        "noncommercial_short": 100,
        "noncommercial_net": 200,
        "spec_net_pct_oi": 20.0,
        "z_score_1y": None,
        "z_score_3y": z3,
        "percentile_3y": None if z3 is None else 88.0,
        "crowding_regime": None if z3 is None else "ELEVATED_LONG",
        **_receipt(report, release, release + timedelta(hours=1)),
    }


def test_cftc_partial_coverage_lists_every_tracked_market():
    release = datetime(2026, 9, 25, 19, 30, tzinfo=timezone.utc)
    rows = [_cftc("ES", date(2026, 9, 22), release), _cftc("GC", date(2026, 9, 22), release, z3=None)]
    p = rm.build_cftc_pillar(rows, {**_RUN, "pillar": "cftc"}, NOW)
    assert p["status"] == "partial" and p["reason"] == "partial_coverage"
    assert p["coverage"] == {"tracked": 16, "available": 2, "stale": 0, "unavailable": 14}
    markets = {m["market"]: m for m in p["data"]["markets"]}
    assert len(markets) == 16
    assert markets["ES"]["z_score_3y"] == 1.2 and markets["ES"]["crowding_regime"] == "ELEVATED_LONG"
    assert markets["GC"]["z_score_3y"] is None and markets["GC"]["crowding_regime"] is None
    assert markets["ZN"]["status"] == "unavailable" and markets["ZN"]["reason"] == "no_row_for_market"
    assert "z_score_3y" not in markets["ZN"]  # no value keys on an unavailable market
    assert p["as_of"] == "2026-09-22" and p["provenance"] == "measured"


def test_cftc_no_rows_is_unavailable_with_ledger_reason():
    p = rm.build_cftc_pillar([], {**_RUN, "pillar": "cftc", "status": "failed"}, NOW)
    assert p["status"] == "unavailable" and p["reason"] == "writer_failed"
    assert p["data"] is None and p["coverage"]["available"] == 0


def test_gex_never_run_is_unavailable_and_labelled_modeled():
    p = rm.build_gex_pillar(None, None, NOW)
    assert p["status"] == "unavailable" and p["reason"] == "never_run"
    assert p["estimated"] is True and p["ticker"] == "SPY"
    assert "not measured" in p["model_note"]
    assert p["data"] is None


def test_gex_row_is_estimated_with_basis():
    obs = date(2026, 10, 1)
    completed = datetime(2026, 10, 1, 20, 31, tzinfo=timezone.utc)
    row = {
        "obs_date": obs, "ticker": "SPY", "spot": 661.2, "gex_aggregate": -1.5e9,
        "gex_normalized": -0.3, "gamma_flip": None, "gamma_flip_crossings": 0,
        "regime": "SHORT_GAMMA", "gamma_wall": 665.0, "put_wall": 650.0, "call_wall": 670.0,
        "dealer_delta": 1.2e6, "model_basis": "bs_own_dte", "sign_convention": "long calls / short puts",
        "chain_capture_batch_id": "b-1", "chain_capture_ordinal": 7,
        "chain_capture_started_at": completed - timedelta(minutes=1), "chain_capture_completed_at": completed,
        "spot_source": "astrogrid.price_close_receipt", "spot_basis": "prior_close",
        "spot_obs_date": date(2026, 9, 30), "spot_available_at": completed, "spot_receipt_id": 9,
        **_receipt(obs, completed, completed, provenance="modeled"),
    }
    p = rm.build_gex_pillar(row, {**_RUN, "pillar": "dealer_gex"}, NOW)
    assert p["status"] == "available" and p["estimated"] is True and p["provenance"] == "modeled"
    assert p["basis"] == "bs_own_dte"
    assert p["data"]["gamma_flip"] is None  # no zero-crossing stays null
    assert p["data"]["chain_capture_batch_id"] == "b-1"


def test_nan_values_serialise_as_null():
    p = rm.build_fed_pillar(_fed(delta_1w_m=float("nan")), _RUN, NOW)
    assert p["data"]["delta_1w_m"] is None
