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
