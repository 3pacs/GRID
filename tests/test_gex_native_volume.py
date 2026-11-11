"""ingestion.gex_native_volume: the payload gex-watch's private native-volume
reader accepts (gex repo ``grid_native_volume.project``), built from Schwab
one-minute bars, ``ready`` only when the bars reconcile with the quote's day
counter. No network: synthetic candles.
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


def _ts(d: date, hh: int, mm: int) -> float:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=ET).timestamp()


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


NOW = _ts(TODAY, 10, 30) + 5       # 10:30:05 ET: bars 09:30..10:29 completed


def _quote(bars_total: int, gap: float = 0.13) -> int:
    """A day counter ``gap`` above the bars, as Schwab's is (~13%)."""
    return round(bars_total / (1 - gap))


def _ready_payload():
    return nv.compute(_history(), _quote(60 * 150_000), now=NOW, receipt_at=NOW)


def test_ready_payload_has_the_contract_gex_watch_checks():
    p = _ready_payload()
    assert (p["schema"], p["symbol"], p["source"], p["status"]) == ("grid.native-volume.v1", "SPY",
                                                                     "schwab_pricehistory", "ready")
    assert p["historical_request"] is False and p["signal_confirmation"] is False and p["exact_trade_vwap"] is False
    assert p["price_basis"] == "provider_unspecified" and p["calendar_version"]
    assert p["cumulative_shares"] == 60 * 150_000 and type(p["cumulative_shares"]) is int
    assert p["bar_vwap_estimate"] > 0 and math.isclose(p["bar_vwap_estimate"], 750.0 + 0.1 / 3, abs_tol=1e-3)
    assert math.isclose(p["rvol"], 1.5)
    assert len(p["baseline_sessions"]) == 10 == len(set(p["baseline_sessions"]))
    for k in ("as_of", "served_at", "latest_receipt_at", "end_of_completed_window"):
        ts = datetime.fromisoformat(p[k]).timestamp()
        assert 0 <= NOW - ts <= 180, k


def test_the_minute_in_progress_is_not_counted():
    hist = _history(today_minutes=61)          # the 10:30 bar exists but has not closed at 10:30:05
    p = nv.compute(hist, _quote(60 * 150_000), now=NOW, receipt_at=NOW)
    assert p["cumulative_shares"] == 60 * 150_000
    assert datetime.fromisoformat(p["end_of_completed_window"]).timestamp() == _ts(TODAY, 10, 30)


@pytest.mark.parametrize(("gap", "reason"), [
    (0.0, "bars_at_or_above_quote_total"),     # bars equal the counter: not Schwab's bar scope
    (-0.5, "bars_at_or_above_quote_total"),    # phantom / cumulative-sized minutes (Yahoo's failure)
    (0.45, "bars_missing_volume"),             # minutes missing
])
def test_bars_outside_the_scope_band_are_unreconciled(gap, reason):
    p = nv.compute(_history(), _quote(60 * 150_000, gap), now=NOW, receipt_at=NOW)
    assert (p["status"], p["reason"]) == ("unreconciled", reason)


@pytest.mark.parametrize("gap", [0.06, 0.13, 0.19, 0.29])
def test_the_measured_scope_gaps_are_ready(gap):
    assert nv.compute(_history(), _quote(60 * 150_000, gap), now=NOW, receipt_at=NOW)["status"] == "ready"


def test_missing_quote_counter_is_unreconciled_not_ready():
    assert nv.compute(_history(), None, now=NOW, receipt_at=NOW)["status"] == "unreconciled"


def test_extended_hours_bars_count_toward_the_reconciliation_only():
    hist = _history(pre=10_000)                # 30 premarket minutes of 10k
    p = nv.compute(hist, _quote(60 * 150_000 + 30 * 10_000), now=NOW, receipt_at=NOW)
    assert p["status"] == "ready" and p["cumulative_shares"] == 60 * 150_000
    assert p["reconciliation"]["bars_total_volume"] == 60 * 150_000 + 30 * 10_000


def test_outside_the_regular_session_is_closed():
    p = nv.compute(_history(), 1, now=_ts(TODAY, 16, 30), receipt_at=_ts(TODAY, 16, 30))
    assert (p["status"], p["reason"]) == ("closed", "outside_regular_session")


def test_too_few_baseline_sessions_is_not_ready():
    hist = {"candles": [c for c in _history()["candles"]
                        if datetime.fromtimestamp(c["datetime"] / 1000, ET).date() >= PRIOR[4]]}
    p = nv.compute(hist, _quote(60 * 150_000), now=NOW, receipt_at=NOW)
    assert (p["status"], p["reason"]) == ("unavailable", "insufficient_baseline_sessions")


def test_malformed_candles_are_dropped_not_trusted():
    hist = _history()
    hist["candles"] += [{"datetime": "x"}, {"datetime": int(NOW * 1000) - 600_000, "high": 1, "low": 1,
                                            "close": 1, "volume": -5}, "junk"]
    assert nv.compute(hist, _quote(60 * 150_000), now=NOW, receipt_at=NOW)["status"] == "ready"


def test_envelope_hash_matches_the_readers_canonical_form():
    p = _ready_payload()
    env = nv.envelope(p, now=NOW)
    canonical = json.dumps(env["payload"], sort_keys=True, separators=(",", ":"), allow_nan=False)
    assert env["schema"] == "grid.native-volume.snapshot.v1"
    assert env["payload_sha256"] == hashlib.sha256(canonical.encode()).hexdigest()


def test_history_params_cover_ten_prior_sessions():
    q = nv.history_params(NOW)
    assert q["frequencyType"] == "minute" and q["frequency"] == 1
    assert (q["endDate"] - q["startDate"]) / 86_400_000 >= 14


def test_snapshot_file_is_private(tmp_path, monkeypatch):
    import scripts.gex_native_volume_snapshot as snap

    d = tmp_path / "nv"
    d.mkdir(mode=0o700)
    os.chmod(d, 0o700)
    snap.write_private(d / "snapshot.json", nv.envelope(_ready_payload(), now=NOW))
    st = (d / "snapshot.json").stat()
    assert stat.S_IMODE(st.st_mode) == 0o600
    assert json.loads((d / "snapshot.json").read_text())["payload"]["status"] == "ready"
    os.chmod(d, 0o755)
    with pytest.raises(SystemExit):
        snap.write_private(d / "snapshot.json", {})
