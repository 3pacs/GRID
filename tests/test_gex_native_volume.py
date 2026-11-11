"""ingestion.gex_native_volume: the snapshot gex-watch's hardened private reader
(gex repo ``grid_native_volume``, 897741a) accepts, built from Schwab one-minute
bars, ``ready`` only inside the measured bar-scope gap. No network: synthetic
candles. ``READER_FIELDS`` and the basis strings mirror the reader; the gex repo
tests the reader against this producer's output.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from datetime import date, datetime, timedelta

import pytest

from ingestion import gex_native_volume as nv

ET = nv.ET
TODAY = date(2026, 11, 10)   # a Tuesday
PRIOR = [TODAY - timedelta(days=d) for d in range(1, 20) if (TODAY - timedelta(days=d)).weekday() < 5][:12]
READER_FIELDS = frozenset((
    "schema", "symbol", "source", "price_basis", "as_of", "session", "calendar_version",
    "end_of_completed_window", "latest_receipt_at", "receipt_age_seconds", "event_age_seconds",
    "completed_minutes", "status", "reason", "cumulative_shares", "bar_vwap_estimate", "rvol",
    "baseline_sessions", "baseline_errors", "exact_trade_vwap", "signal_confirmation",
    "cross_source_reconciled", "live", "vwap_basis", "rvol_basis", "source_revisions",
    "baseline_median_shares", "served_at", "historical_request"))


def _ts(d: date, hh: int, mm: int, ss: int = 0) -> float:
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=ET).timestamp()


def _candle(ts: float, v: int, px: float = 750.0) -> dict:
    return {"datetime": int(ts * 1000), "open": px, "high": px + 0.3, "low": px - 0.3, "close": px + 0.1,
            "volume": v}


def _day(d: date, minutes: int, v: int = 100_000, pre: int = 0) -> list[dict]:
    out = [_candle(_ts(d, 8, m), pre) for m in range(30) if pre]
    out += [_candle(_ts(d, 9, 30) + 60 * i, v) for i in range(minutes)]
    return out


def _history(today_minutes: int = 60, prior_v: int = 100_000, today_v: int = 150_000, pre: int = 0) -> dict:
    candles = []
    for d in sorted(PRIOR):
        candles += _day(d, 390, prior_v)
    candles += _day(TODAY, today_minutes, today_v, pre)
    return {"symbol": "SPY", "candles": candles}


RECEIPT = _ts(TODAY, 10, 30, 7)     # 10:30:07 ET: bars 09:30..10:29 completed (>= 5 s past their end)
NOW = RECEIPT + 1.5                  # snapshot generated after the receipt


def _quote(bars_total: int, gap: float = 0.13) -> int:
    """A day counter ``gap`` above the bars, as Schwab's is (~13%)."""
    return round(bars_total / (1 - gap))


def _run(hist=None, quote=None, now=NOW, receipt=RECEIPT):
    hist = _history() if hist is None else hist
    return nv.compute(hist, _quote(60 * 150_000) if quote is None else quote, now=now, receipt_at=receipt)


def test_ready_payload_is_exactly_the_readers_contract():
    p, _ = _run()
    assert set(p) == READER_FIELDS
    assert (p["schema"], p["symbol"], p["source"], p["price_basis"], p["status"], p["reason"]) == (
        "grid.native-volume.v1", "SPY", "schwab_pricehistory", "provider_unspecified", "ready", None)
    for k in ("historical_request", "signal_confirmation", "exact_trade_vwap", "cross_source_reconciled", "live"):
        assert p[k] is False, k
    assert p["baseline_errors"] == {}
    assert p["vwap_basis"] == "Volume-weighted HLC3 of native one-minute OHLCV; not trade VWAP"
    assert p["rvol_basis"] == "Cumulative shares / median same-elapsed shares of exact prior ten sessions"
    assert p["cumulative_shares"] == 60 * 150_000 and type(p["cumulative_shares"]) is int
    assert p["completed_minutes"] == 60 and len(p["source_revisions"]) == 60
    assert p["rvol"] == p["cumulative_shares"] / p["baseline_median_shares"]   # exact, as the reader checks
    assert math.isclose(p["rvol"], 1.5)
    assert math.isclose(p["bar_vwap_estimate"], 750.0 + 0.1 / 3, abs_tol=1e-3)
    assert len(p["baseline_sessions"]) == 10 == len(set(p["baseline_sessions"]))
    assert p["session"] == TODAY.isoformat() and p["session"] not in p["baseline_sessions"]


def test_clocks_follow_the_readers_rules():
    p, _ = _run()
    t = {k: datetime.fromisoformat(p[k]).timestamp()
         for k in ("as_of", "served_at", "latest_receipt_at", "end_of_completed_window")}
    assert t["as_of"] == t["served_at"]
    assert t["end_of_completed_window"] % 60 == 0 and t["end_of_completed_window"] == _ts(TODAY, 10, 30)
    assert t["latest_receipt_at"] >= t["end_of_completed_window"] + 5
    assert math.isclose(p["receipt_age_seconds"], t["as_of"] - t["latest_receipt_at"], abs_tol=1e-6)
    assert math.isclose(p["event_age_seconds"], t["as_of"] - t["end_of_completed_window"], abs_tol=1e-6)


def test_a_bar_counts_only_five_seconds_after_it_ends():
    early = _ts(TODAY, 10, 30, 3)      # the 10:29 bar ended 3 s ago: not yet completed
    p, _ = _run(receipt=early, now=early + 1)
    assert p["completed_minutes"] == 59


def test_bars_older_than_the_reader_allows_are_not_ready():
    p, _ = _run(now=RECEIPT + 200)
    assert (p["status"], p["reason"]) == ("unavailable", "bars_older_than_reader_limit")


@pytest.mark.parametrize(("gap", "reason"), [
    (0.0, "bars_at_or_above_quote_total"),     # bars equal the counter: not Schwab's bar scope
    (-0.5, "bars_at_or_above_quote_total"),    # phantom / cumulative-sized minutes (Yahoo's failure)
    (0.45, "bars_missing_volume"),             # minutes missing
])
def test_bars_outside_the_scope_band_are_unreconciled(gap, reason):
    p, info = _run(quote=_quote(60 * 150_000, gap))
    assert (p["status"], p["reason"]) == ("unreconciled", reason)
    assert set(p) == READER_FIELDS and "reconciliation" not in p
    assert math.isclose(info["reconciliation"]["relative_difference"], gap, abs_tol=1e-5)


@pytest.mark.parametrize("gap", [0.06, 0.13, 0.19, 0.29])
def test_the_measured_scope_gaps_are_ready(gap):
    assert _run(quote=_quote(60 * 150_000, gap))[0]["status"] == "ready"


def test_missing_quote_counter_is_unreconciled_not_ready():
    assert nv.compute(_history(), None, now=NOW, receipt_at=RECEIPT)[0]["status"] == "unreconciled"


def test_extended_hours_bars_count_toward_the_gate_only():
    p, info = _run(hist=_history(pre=10_000), quote=_quote(60 * 150_000 + 30 * 10_000))
    assert p["status"] == "ready" and p["cumulative_shares"] == 60 * 150_000
    assert info["reconciliation"]["bars_total_volume"] == 60 * 150_000 + 30 * 10_000


def test_outside_the_regular_session_is_closed():
    t = _ts(TODAY, 16, 30)
    p, _ = nv.compute(_history(), 1, now=t, receipt_at=t)
    assert (p["status"], p["reason"]) == ("closed", "outside_regular_session") and set(p) == READER_FIELDS


def test_too_few_baseline_sessions_is_not_ready():
    hist = {"symbol": "SPY", "candles": [c for c in _history()["candles"]
                                         if datetime.fromtimestamp(c["datetime"] / 1000, ET).date() >= PRIOR[4]]}
    assert _run(hist=hist)[0]["reason"] == "insufficient_baseline_sessions"


def test_malformed_candles_are_dropped_not_trusted():
    hist = _history()
    hist["candles"] += [{"datetime": "x"}, {"datetime": int(RECEIPT * 1000) - 600_000, "high": 1, "low": 1,
                                            "close": 1, "volume": -5}, "junk"]
    assert _run(hist=hist)[0]["status"] == "ready"


def test_envelope_matches_the_readers_canonical_form():
    p, info = _run()
    env = nv.envelope(p, nv.provenance(info["bars"]))
    assert set(env) == {"schema", "generated_at", "payload", "payload_sha256", "provenance"}
    assert env["schema"] == "grid.native-volume.snapshot.v1" and env["generated_at"] == p["as_of"]
    canonical = json.dumps(env["payload"], sort_keys=True, separators=(",", ":"), allow_nan=False)
    assert env["payload_sha256"] == hashlib.sha256(canonical.encode()).hexdigest()
    prov = env["provenance"]
    assert set(prov) == {"producer", "retained_rows_consulted", "retained_rows_sha256", "integrity_scope"}
    assert prov["producer"] == "GRID Schwab pricehistory snapshot"
    assert prov["retained_rows_consulted"] == len(info["bars"]) and len(prov["retained_rows_sha256"]) == 64
    assert prov["integrity_scope"] == ("content integrity only; trust requires verified local producer "
                                       "and owner-only custody")


def test_history_params_cover_ten_prior_sessions():
    q = nv.history_params(NOW)
    assert q["frequencyType"] == "minute" and q["frequency"] == 1
    assert (q["endDate"] - q["startDate"]) / 86_400_000 >= 14


def test_snapshot_file_is_private_and_single_linked(tmp_path):
    import scripts.gex_native_volume_snapshot as snap

    d = tmp_path / "nv"
    d.mkdir(mode=0o700)
    os.chmod(d, 0o700)
    p, info = _run()
    snap.write_private(d / "snapshot.json", nv.envelope(p, nv.provenance(info["bars"])))
    st = (d / "snapshot.json").stat()
    assert stat.S_IMODE(st.st_mode) == 0o600 and st.st_nlink == 1
    assert json.loads((d / "snapshot.json").read_text())["payload"]["status"] == "ready"
    os.chmod(d, 0o755)
    with pytest.raises(SystemExit):
        snap.write_private(d / "snapshot.json", {})


# ── validation and coverage (single-producer kit, 2026-11-11) ───────────────


def test_a_response_for_another_symbol_is_refused():
    hist = _history()
    hist["symbol"] = "QQQ"
    p, _ = _run(hist=hist)
    assert (p["status"], p["reason"]) == ("failed", "wrong_symbol")


def test_a_duplicate_minute_is_refused_not_double_counted():
    hist = _history()
    hist["candles"].append(dict(hist["candles"][-1]))
    p, _ = _run(hist=hist)
    assert (p["status"], p["reason"]) == ("failed", "duplicate_minute")


def test_a_missing_minute_today_fails_coverage():
    hist = _history()
    gap = int(_ts(TODAY, 10, 0) * 1000)
    hist["candles"] = [c for c in hist["candles"] if c["datetime"] != gap]
    p, _ = _run(hist=hist)
    assert (p["status"], p["reason"]) == ("coverage_failed", "missing_current_minute")


def test_a_missing_minute_in_a_baseline_session_fails_coverage():
    hist = _history()
    gap = int(_ts(PRIOR[0], 9, 45) * 1000)     # the most recent prior session
    hist["candles"] = [c for c in hist["candles"] if c["datetime"] != gap]
    p, _ = _run(hist=hist)
    assert (p["status"], p["reason"]) == ("coverage_failed", "missing_baseline_minute")


def test_a_baseline_gap_after_the_current_minute_does_not_matter():
    hist = _history()
    late = int(_ts(PRIOR[0], 15, 0) * 1000)    # 15:00 is after 10:29, the last completed minute
    hist["candles"] = [c for c in hist["candles"] if c["datetime"] != late]
    assert _run(hist=hist)[0]["status"] == "ready"


def test_an_incomplete_baseline_session_is_not_replaced_by_an_older_one():
    hist = _history()
    newest = PRIOR[0]
    hist["candles"] = [c for c in hist["candles"]
                       if not (datetime.fromtimestamp(c["datetime"] / 1000, ET).date() == newest
                               and datetime.fromtimestamp(c["datetime"] / 1000, ET).hour == 10)]
    p, _ = _run(hist=hist)
    assert p["status"] == "coverage_failed" and p["baseline_sessions"] == []
