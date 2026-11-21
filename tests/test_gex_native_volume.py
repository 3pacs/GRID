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
    assert p["vwap_basis"] == ("HLC3 of native one-minute bars weighted by Schwab quote totalVolume increments "
                               "(bar volume where no earlier sample); not trade VWAP")
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
    assert _run(hist=hist)[0]["reason"] == "missing_baseline_session"


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


# Exact exchange-session coverage (source-only calendar correction).
def test_an_entire_missing_expected_session_is_not_replaced_by_older_data():
    hist = _history()
    missing = PRIOR[0]
    hist["candles"] = [c for c in hist["candles"]
                       if datetime.fromtimestamp(c["datetime"] / 1000, ET).date() != missing]
    observed = {datetime.fromtimestamp(c["datetime"] / 1000, ET).date()
                for c in hist["candles"]} - {TODAY}
    assert len(observed) == 11  # Enough older dates hid the gap before this fix.
    p, _ = _run(hist=hist)
    assert (p["status"], p["reason"]) == ("coverage_failed", "missing_baseline_session")
    assert p["rvol"] is None and p["baseline_median_shares"] is None


@pytest.mark.parametrize("day", [date(2026, 11, 14), date(2026, 11, 26), date(2026, 12, 25)])
def test_weekends_and_exchange_holidays_are_closed(day):
    now = _ts(day, 10, 30)
    p, _ = _run(now=now, receipt=now)
    assert (p["status"], p["reason"]) == ("closed", "outside_regular_session")


@pytest.mark.parametrize("day", [date(2026, 9, 30), date(2028, 1, 3)])
def test_dates_outside_the_pinned_calendar_fail_closed(day):
    now = _ts(day, 10, 30)
    p, _ = _run(now=now, receipt=now)
    assert (p["status"], p["reason"]) == ("unavailable", "calendar_unavailable")


def test_early_close_boundary_is_closed():
    now = _ts(date(2026, 11, 27), 13, 0)
    p, _ = _run(now=now, receipt=now)
    assert (p["status"], p["reason"]) == ("closed", "outside_regular_session")


def _scheduled_history(day: date, count: int) -> dict:
    """Synthetic OHLCV only; days and their bounds come from the pinned schedule."""
    cal = nv._calendar()
    current = cal.for_date(day)
    assert current is not None
    prior = cal.prior_sessions(current.key, 12)
    candles = []
    for session in prior:
        candles += _day(nv._session(session.opens.timestamp()),
                        int((session.closes - session.opens).total_seconds() / 60))
    candles += _day(day, count, 150_000)
    return {"symbol": "SPY", "candles": candles}


def test_pinned_calendar_identity_and_dst_boundaries():
    cal = nv._calendar()
    assert cal.version == nv.CALENDAR_VERSION
    assert hashlib.sha256(nv.CALENDAR_PATH.read_bytes()).hexdigest() == nv.CALENDAR_SHA256
    assert cal.for_date(date(2026, 10, 30)).opens.hour == 13
    assert cal.for_date(date(2026, 11, 2)).opens.hour == 14
    assert cal.for_date(date(2027, 3, 12)).opens.hour == 14
    assert cal.for_date(date(2027, 3, 15)).opens.hour == 13
    # New Year's Day 2028 is Saturday: XNYS remains open on 2027-12-31.
    assert cal.for_date(date(2027, 12, 31)) is not None
    p, _ = _run()
    assert p["calendar_version"] == nv.CALENDAR_VERSION
    assert p["baseline_sessions"] == [
        "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30", "2026-11-02",
        "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-09"]


@pytest.mark.parametrize("day", [date(2026, 11, 27), date(2026, 12, 24)])
def test_shortened_current_session_uses_real_open_close(day):
    now = _ts(day, 12, 59, 7)
    p, _ = _run(hist=_scheduled_history(day, 209), quote=_quote(209 * 150_000), now=now, receipt=now)
    assert p["status"] == "ready" and p["completed_minutes"] == 209
    assert "2026-11-26" not in p["baseline_sessions"]
    p, _ = _run(now=_ts(day, 13, 0), receipt=_ts(day, 13, 0))
    assert p["status"] == "closed"


@pytest.mark.parametrize("minutes,reason", [(60, None), (210, None), (211, "incomparable_baseline_session")])
def test_short_prior_session_is_never_extended_rescaled_or_replaced(minutes, reason):
    day = date(2026, 11, 30)
    now = _ts(day, 9, 30) + minutes * 60 + 7
    history = _scheduled_history(day, minutes)
    # Even bogus provider bars after the prior close cannot make it comparable.
    history["candles"] += [_candle(_ts(date(2026, 11, 27), 13, 0), 100_000)]
    p, _ = _run(hist=history, quote=_quote(minutes * 150_000), now=now, receipt=now)
    assert p["reason"] == reason
    if reason is None:
        assert p["status"] == "ready" and math.isclose(p["rvol"], 1.5)
        assert p["baseline_sessions"] == [
            "2026-11-13", "2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19",
            "2026-11-20", "2026-11-23", "2026-11-24", "2026-11-25", "2026-11-27"]
    else:
        assert p["status"] == "unavailable" and p["rvol"] is None
        assert p["baseline_errors"] == {"2026-11-27": "early_close_insufficient_elapsed_minutes"}


def test_missing_expected_session_after_holiday_is_not_substituted():
    day, missing = date(2026, 11, 30), date(2026, 11, 25)
    history = _scheduled_history(day, 60)
    history["candles"] = [c for c in history["candles"]
                          if datetime.fromtimestamp(c["datetime"] / 1000, ET).date() != missing]
    now = _ts(day, 10, 30, 7)
    p, _ = _run(hist=history, now=now, receipt=now)
    assert p["reason"] == "missing_baseline_session"
    assert p["baseline_errors"] == {missing.isoformat(): "missing_baseline_session"}


def test_calendar_start_with_insufficient_expected_history_is_unavailable():
    now = _ts(date(2026, 10, 2), 10, 30)
    p, _ = _run(now=now, receipt=now)
    assert (p["status"], p["reason"]) == ("unavailable", "calendar_unavailable")


@pytest.mark.parametrize("fault", ["missing", "corrupt", "writable", "symlink"])
def test_untrusted_or_unavailable_manifest_cannot_fall_back(tmp_path, monkeypatch, fault):
    manifest = tmp_path / "calendar.json"
    if fault != "missing":
        manifest.write_bytes(nv.CALENDAR_PATH.read_bytes() + (b" " if fault == "corrupt" else b""))
        manifest.chmod(0o666 if fault == "writable" else 0o644)
    if fault == "symlink":
        link = tmp_path / "link"
        link.symlink_to(manifest)
        manifest = link
    monkeypatch.setattr(nv, "CALENDAR_PATH", manifest)
    p, _ = _run()
    assert (p["status"], p["reason"]) == ("unavailable", "calendar_unavailable")
    assert set(p) == READER_FIELDS and p["rvol"] is None


def test_a_calendar_file_corrupted_after_success_is_not_cached(tmp_path, monkeypatch):
    manifest = tmp_path / "calendar.json"
    manifest.write_bytes(nv.CALENDAR_PATH.read_bytes())
    manifest.chmod(0o644)
    monkeypatch.setattr(nv, "CALENDAR_PATH", manifest)
    assert _run()[0]["status"] == "ready"
    manifest.write_bytes(b"{}")
    assert _run()[0]["reason"] == "calendar_unavailable"


@pytest.mark.parametrize("day,hour,reason", [
    (date(2026, 11, 14), 11, "outside_regular_session"),
    (date(2026, 11, 26), 11, "outside_regular_session"),
    (date(2026, 11, 27), 13, "outside_regular_session"),
    (date(2028, 1, 3), 11, "calendar_unavailable"),
    (date(2026, 10, 2), 11, "calendar_unavailable"),
])
def test_runner_calls_no_provider_and_writes_nothing_when_calendar_withholds(monkeypatch, day, hour, reason):
    import scripts.gex_native_volume_snapshot as snap

    def refuse(*args, **kwargs):
        pytest.fail("Provider call or snapshot write happened outside verified session coverage")

    class RefusingClient:
        quotes = price_history = refuse

    now = _ts(day, hour, 0)
    monkeypatch.setattr(snap.time, "time", lambda: now)
    monkeypatch.setattr(snap, "write_private", refuse)
    env, _ = snap.run_once(RefusingClient(), nv.CALENDAR_PATH.parent / "must-not-write.json")
    assert env["payload"]["reason"] == reason


def test_runner_calendar_failure_never_contacts_provider(tmp_path, monkeypatch):
    import scripts.gex_native_volume_snapshot as snap

    class RefusingClient:
        def __getattr__(self, name):
            pytest.fail("Provider called without a verified calendar")

    monkeypatch.setattr(nv, "CALENDAR_PATH", tmp_path / "missing")
    monkeypatch.setattr(snap.time, "time", lambda: NOW)
    env, _ = snap.run_once(RefusingClient(), None)
    assert env["payload"]["reason"] == "calendar_unavailable"


@pytest.mark.parametrize("day", [TODAY, PRIOR[0]])
def test_shifted_second_bars_do_not_satisfy_exact_minute_coverage(day):
    history = _history()
    stamp = int(_ts(day, 9, 45) * 1000)
    for candle in history["candles"]:
        if candle["datetime"] == stamp:
            candle["datetime"] += 1000
    p, _ = _run(hist=history)
    assert p["status"] == "coverage_failed" and p["rvol"] is None


@pytest.mark.parametrize("scenario", ["ready", "missing_session", "short_baseline"])
def test_optional_sibling_gex_reader_compatibility(tmp_path, scenario):
    """Exact deployed-reader integration when an operator supplies its source path."""
    import importlib.util
    from pathlib import Path
    from scripts.gex_native_volume_snapshot import write_private

    reader_path = os.environ.get("GEX_NATIVE_READER")
    if not reader_path:
        pytest.skip("Set GEX_NATIVE_READER to the sibling GEX grid_native_volume.py")
    assert Path(reader_path).is_absolute() and Path(reader_path).is_file()
    spec = importlib.util.spec_from_file_location("gex_native_reader_contract", reader_path)
    reader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reader)
    now, receipt = NOW, RECEIPT
    history, quote = _history(), _quote(60 * 150_000)
    if scenario == "missing_session":
        history["candles"] = [c for c in history["candles"]
                              if datetime.fromtimestamp(c["datetime"] / 1000, ET).date() != PRIOR[0]]
    elif scenario == "short_baseline":
        day = date(2026, 11, 30)
        now = receipt = _ts(day, 13, 1, 7)
        history, quote = _scheduled_history(day, 211), _quote(211 * 150_000)
    p, info = nv.compute(history, quote, now=now, receipt_at=receipt)
    directory = tmp_path / "snapshot"
    directory.mkdir(mode=0o700)
    path = directory / "snapshot.json"
    write_private(path, nv.envelope(p, nv.provenance(info["bars"])))
    projected = reader.read_private_snapshot(str(path), now)
    if scenario == "ready":
        assert p["calendar_version"] == nv.CALENDAR_VERSION
        assert projected["status"] == "ready", projected
        assert projected["rvol"] == p["rvol"]
    else:
        assert projected["status"] == "unavailable", projected
    assert projected["signal_confirmation"] is False
    assert projected["exact_trade_vwap"] is False


def test_baseline_with_only_later_bars_reports_a_minute_gap():
    history = _history()
    missing = PRIOR[0]
    history["candles"] = [c for c in history["candles"] if
                          not (_ts(missing, 9, 30) <= c["datetime"] / 1000 < _ts(missing, 10, 30))]
    p, _ = _run(hist=history)
    assert (p["status"], p["reason"]) == ("coverage_failed", "missing_baseline_minute")


def test_manifest_symlink_cycle_is_unavailable(tmp_path, monkeypatch):
    path = tmp_path / "cycle"
    path.symlink_to(path)
    monkeypatch.setattr(nv, "CALENDAR_PATH", path)
    assert _run()[0]["reason"] == "calendar_unavailable"


def test_runner_open_session_still_uses_one_quote_and_one_history_call(monkeypatch):
    from types import SimpleNamespace
    import scripts.gex_native_volume_snapshot as snap

    calls = []

    class FakeClient:
        def quotes(self, symbol):
            calls.append(("quotes", symbol))
            return {symbol: SimpleNamespace(total_volume=_quote(60 * 150_000))}

        def price_history(self, symbol, **params):
            calls.append(("history", symbol, params))
            return _history()

    monkeypatch.setattr(snap.time, "time", lambda: NOW)
    env, _ = snap.run_once(FakeClient(), None)
    assert env["payload"]["status"] == "ready"
    assert calls == [("quotes", "SPY"), ("history", "SPY", nv.history_params(NOW))]



# ---------------------------------------------------------------- quote-volume weighting (2026-11-19)

def _bar(d: date, hh: int, mm: int, v: int, px: float):
    t = _ts(d, hh, mm)
    return (t, px + 0.3, px - 0.3, px + 0.1, v)          # HLC3 = px + 1/30


def test_without_earlier_samples_the_vwap_is_the_bar_weighted_one():
    bars = [_bar(TODAY, 9, 30 + i, 100 + 50 * i, 700.0 + i) for i in range(5)]
    vwap, used = nv.quote_weighted_vwap(bars, [(_ts(TODAY, 9, 35, 7), 10_000)])
    bar_vwap = sum((b[1] + b[2] + b[3]) / 3 * b[4] for b in bars) / sum(b[4] for b in bars)
    assert abs(vwap - bar_vwap) < 1e-12 and used == {"quote_weighted_blocks": 0, "bar_weighted_blocks": 1}


def test_blocks_are_weighted_by_the_counters_rise_since_the_previous_run():
    # runs at 09:31:07, 09:32:07, 09:33:07: bars 09:30, 09:31, 09:32 each complete in one run
    bars = [_bar(TODAY, 9, 30, 100, 700.0), _bar(TODAY, 9, 31, 100, 701.0), _bar(TODAY, 9, 32, 100, 702.0)]
    samples = [(_ts(TODAY, 9, 31, 7), 5_000), (_ts(TODAY, 9, 32, 7), 5_300), (_ts(TODAY, 9, 33, 7), 6_200)]
    vwap, used = nv.quote_weighted_vwap(bars, samples)
    p = [700.0 + 1 / 30, 701.0 + 1 / 30, 702.0 + 1 / 30]
    want = (p[0] * 100 + p[1] * 300 + p[2] * 900) / 1300           # first block by its bar volume, then the rises
    assert abs(vwap - want) < 1e-9 and used == {"quote_weighted_blocks": 2, "bar_weighted_blocks": 1}


def test_a_missed_run_merges_its_bars_and_a_falling_counter_falls_back_to_bar_volume():
    bars = [_bar(TODAY, 9, 30, 100, 700.0), _bar(TODAY, 9, 31, 300, 701.0), _bar(TODAY, 9, 32, 100, 702.0)]
    samples = [(_ts(TODAY, 9, 31, 7), 5_000), (_ts(TODAY, 9, 33, 7), 4_000)]       # 09:32:07 missed; counter fell
    vwap, used = nv.quote_weighted_vwap(bars, samples)
    p1 = ((701.0 + 1 / 30) * 300 + (702.0 + 1 / 30) * 100) / 400
    assert abs(vwap - ((700.0 + 1 / 30) * 100 + p1 * 400) / 500) < 1e-9 and used["bar_weighted_blocks"] == 2


def test_compute_uses_the_samples_and_keeps_the_payload_contract():
    hist = _history()
    early = [(_ts(TODAY, 9, 30) + 60 * (i + 1) + 7, 1_000_000 + 200_000 * i) for i in range(59)]
    pl, info = nv.compute(hist, _quote(60 * 150_000), now=NOW, receipt_at=RECEIPT, quote_samples=early)
    assert pl["status"] == "ready" and set(pl) == READER_FIELDS and pl["vwap_basis"] == nv.VWAP_BASIS
    assert info["vwap_weighting"]["quote_weighted_blocks"] >= 58


def test_the_runner_keeps_todays_samples_privately_and_reads_them_back(tmp_path, monkeypatch):
    import scripts.gex_native_volume_snapshot as snap
    d = tmp_path / "snap"
    d.mkdir(mode=0o700)
    out = d / "snapshot.json"
    clock = {"t": RECEIPT}

    class Quote:
        def __init__(self, v):
            self.total_volume = v

    class Client:
        n = 0

        def quotes(self, sym):
            Client.n += 1
            return {sym: Quote(_quote(60 * 150_000) + Client.n * 1000)}

        def price_history(self, sym, **kw):
            return _history()

    monkeypatch.setattr(snap.time, "time", lambda: clock["t"])
    snap.run_once(Client(), out)
    clock["t"] += 60
    env, info = snap.run_once(Client(), out)
    path = d / f"quote-samples-{TODAY.isoformat()}.jsonl"
    lines = [json.loads(x) for x in path.read_text().splitlines()]
    assert len(lines) == 2 and lines[1]["q"] - lines[0]["q"] == 1000 and stat.S_IMODE(path.stat().st_mode) == 0o600
    memory = []
    snap.run_once(Client(), None, memory)
    assert len(memory) == 1


# ---------------------------------------------------------------- scope gate since the open (2026-11-19)

PRE_OPEN = (_ts(TODAY, 9, 29, 9), 5_000_000)     # the 09:29:08 run's reading: a large pre-open lump, as on 18 Nov
CUM = 60 * 150_000


def _since_open_quote(gap: float, pre: int = PRE_OPEN[1]) -> int:
    return pre + round(CUM / (1 - gap))


def test_a_pre_open_lump_fails_the_whole_day_gate_without_a_pre_open_sample():
    p, info = nv.compute(_history(), _since_open_quote(0.15), now=NOW, receipt_at=RECEIPT)
    assert (p["status"], p["reason"]) == ("unreconciled", "bars_missing_volume")
    assert info["reconciliation"]["gate_basis"] == "whole_day" and info["reconciliation"]["relative_difference"] > 0.30


def test_the_pre_open_sample_judges_the_volume_since_the_open():
    q = _since_open_quote(0.15)
    p, info = nv.compute(_history(), q, now=NOW, receipt_at=RECEIPT, quote_samples=[PRE_OPEN])
    r = info["reconciliation"]
    assert p["status"] == "ready" and set(p) == READER_FIELDS
    assert r["gate_basis"] == "since_open" and r["scope_gap_band"] == [0.0, 0.30]
    assert math.isclose(r["since_open_difference"], 0.15, abs_tol=1e-5)
    assert math.isclose(r["relative_difference"], (q - CUM) / q, abs_tol=1e-5)    # the log's diff= stays whole-day
    assert r["pre_open_quote_total_volume"] == PRE_OPEN[1]


@pytest.mark.parametrize(("gap", "status", "reason"), [
    (0.02, "ready", None),                                 # Schwab's opening-bar shortfall is small: floor 0
    (0.0, "ready", None),
    (-0.05, "unreconciled", "bars_at_or_above_quote_total"),   # bars above the counter still fail (Yahoo)
    (0.45, "unreconciled", "bars_missing_volume"),
])
def test_the_since_open_band(gap, status, reason):
    p, _ = nv.compute(_history(), _since_open_quote(gap), now=NOW, receipt_at=RECEIPT, quote_samples=[PRE_OPEN])
    assert (p["status"], p["reason"]) == (status, reason)


@pytest.mark.parametrize("sample", [
    (_ts(TODAY, 9, 27, 0), 5_000_000),          # older than PRE_OPEN_MAX_AGE_S
    (_ts(TODAY, 9, 30, 10), 5_000_000),         # the 09:30 run: in session, not pre-open
    (_ts(TODAY, 9, 29, 9), 99_000_000),         # counter above the current one: did not rise
])
def test_an_unusable_pre_open_sample_leaves_the_whole_day_gate(sample):
    q = _quote(CUM, 0.13)
    p, info = nv.compute(_history(), q, now=NOW, receipt_at=RECEIPT, quote_samples=[sample])
    assert p["status"] == "ready" and info["reconciliation"]["gate_basis"] == "whole_day"


def test_split_samples_keeps_the_pre_open_base_and_drops_the_opening_minute_sample():
    session = nv.calendar_window(NOW)[1]
    samples = [(_ts(TODAY, 9, 20, 9), 1), PRE_OPEN, (_ts(TODAY, 9, 30, 10), 5_600_000), (_ts(TODAY, 9, 31, 7), 6_000_000)]
    pre, kept = nv.split_samples(samples, session)
    assert pre == PRE_OPEN and kept == [PRE_OPEN, (_ts(TODAY, 9, 31, 7), 6_000_000)]


def test_the_opening_bar_is_weighted_from_the_pre_open_sample():
    # The 09:30 run's counter already holds the opening cross; without dropping it the 09:30 bar would be
    # weighted by 09:30:10..09:31:07 only.
    bars = [_bar(TODAY, 9, 30, 100, 700.0), _bar(TODAY, 9, 31, 100, 701.0)]
    pre = (PRE_OPEN[0], 5_000)
    _, kept = nv.split_samples([pre, (_ts(TODAY, 9, 30, 10), 5_900), (_ts(TODAY, 9, 31, 7), 6_000),
                                (_ts(TODAY, 9, 32, 7), 6_200)], nv.calendar_window(_ts(TODAY, 9, 32, 7))[1])
    vwap, used = nv.quote_weighted_vwap(bars, kept)
    want = ((700.0 + 1 / 30) * 1000 + (701.0 + 1 / 30) * 200) / 1200
    assert abs(vwap - want) < 1e-9 and used == {"quote_weighted_blocks": 2, "bar_weighted_blocks": 0}


@pytest.mark.parametrize(("hh", "mm", "ss", "expected"), [
    (9, 29, 8, True), (9, 28, 31, True), (9, 28, 29, False), (9, 25, 8, False), (9, 30, 0, False)])
def test_pre_open_session_is_only_the_last_run_before_the_bell(hh, mm, ss, expected):
    assert (nv.pre_open_session(_ts(TODAY, hh, mm, ss)) is not None) is expected


def test_no_pre_open_sample_on_a_closed_day():
    assert nv.pre_open_session(_ts(date(2026, 11, 26), 9, 29, 8)) is None      # Thanksgiving


def test_the_pre_open_run_makes_one_quote_call_and_writes_only_the_sample(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import scripts.gex_native_volume_snapshot as snap

    calls = []

    class Client:
        def quotes(self, symbol):
            calls.append("quotes")
            return {symbol: SimpleNamespace(total_volume=5_000_000)}

        def price_history(self, symbol, **kw):
            pytest.fail("no bars before the bell")

    d = tmp_path / "snap"
    d.mkdir(mode=0o700)
    out = d / "snapshot.json"
    t = _ts(TODAY, 9, 29, 8)
    monkeypatch.setattr(snap.time, "time", lambda: t)
    env, info = snap.run_once(Client(), out)
    assert calls == ["quotes"] and env["payload"]["status"] == "closed" and info["pre_open_sample"] is True
    assert not out.exists()
    path = d / f"quote-samples-{TODAY.isoformat()}.jsonl"
    assert [json.loads(x)["q"] for x in path.read_text().splitlines()] == [5_000_000]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    calls.clear()
    monkeypatch.setattr(snap.time, "time", lambda: _ts(TODAY, 9, 20, 8))     # an earlier pre-open run: no call
    assert snap.run_once(Client(), out)[1]["pre_open_sample"] is False and calls == []


def test_ready_log_lines_keep_the_form_the_burn_in_jobs_parse(monkeypatch, capsys):
    import re
    import sys
    from types import ModuleType, SimpleNamespace

    import scripts.gex_native_volume_snapshot as snap

    class Client:
        def quotes(self, symbol):
            return {symbol: SimpleNamespace(total_volume=_quote(CUM))}

        def price_history(self, symbol, **kw):
            return _history()

    fake = ModuleType("ingestion.schwab_market")     # the real one loads GRID config (needs the service .env)
    fake.SchwabError, fake.SchwabMarketClient = RuntimeError, Client
    monkeypatch.setitem(sys.modules, "ingestion.schwab_market", fake)
    monkeypatch.setattr(snap.time, "time", lambda: NOW)
    assert snap.main(["--dry-run"]) == 0
    line = capsys.readouterr().out.strip()
    assert re.match(r"^(\S+) ready - bars=(\d+) quote=(\d+) diff=([0-9.]+)$", line), line
