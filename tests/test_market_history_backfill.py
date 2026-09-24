"""Unit tests for scripts/market_history_backfill.py (no database, no network).

The PostgreSQL behaviour (idempotency, revisions, resume, locking, bounded
writes, resolver isolation) is proven in test_market_history_backfill_pg.py.
"""

from __future__ import annotations

import importlib
import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from binance_close_contract import CANONICAL_CLOSE_SERIES, is_completed_canonical_close
from normalization.entity_map import SEED_MAPPINGS, yf_candidate_feature_names
from price_close_contract import SPY_CLOSE_SERIES, is_policy_capture

mhb = importlib.import_module("scripts.market_history_backfill")

DAY_MS = 86_400_000


def _ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def kline(d: date, close: str, *, open_: str | None = None, high: str | None = None,
          low: str | None = None) -> list:
    open_ = open_ or close
    high = high or str(max(float(open_), float(close)) + 1)
    low = low or str(min(float(open_), float(close)) - 1)
    return [_ms(d), open_, high, low, close, "10.5", _ms(d) + DAY_MS - 1, "1000.0", 42, "5", "500", "0"]


# --------------------------------------------------------------------------
# Windows, calendars, guards
# --------------------------------------------------------------------------

def test_month_chunks_cover_window_exactly_and_respect_month_ends():
    chunks = mhb.month_chunks(date(2024, 1, 15), date(2024, 3, 2))
    assert chunks == [(date(2024, 1, 15), date(2024, 1, 31)),
                      (date(2024, 2, 1), date(2024, 2, 29)),
                      (date(2024, 3, 1), date(2024, 3, 2))]
    assert mhb.month_chunks(date(2025, 5, 7), date(2025, 5, 7)) == [(date(2025, 5, 7), date(2025, 5, 7))]
    assert mhb.month_chunks(date(2025, 5, 8), date(2025, 5, 7)) == []


def test_proposed_window_expected_counts_match_the_production_reconciliation():
    start, end = date(2025, 1, 1), date(2026, 9, 22)
    assert len(mhb.expected_dates(mhb.SERIES["BTCUSDT"], start, end)) == 630
    sessions = mhb.expected_dates(mhb.SERIES["SPY"], start, end)
    # R1b: rule calendar minus the 2025-01-09 closure gave 431 sessions, all present.
    assert len(sessions) == 431
    assert date(2025, 1, 9) not in sessions  # national day of mourning
    assert date(2025, 7, 4) not in sessions  # Independence Day
    assert date(2025, 1, 10) in sessions


def test_window_guards_refuse_forward_owned_and_recent_dates():
    now = datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
    mhb.check_window(date(2025, 1, 1), date(2026, 9, 22), now)
    with pytest.raises(mhb.BackfillRefused, match="forward-owned"):
        mhb.check_window(date(2025, 1, 1), date(2026, 9, 23), now)
    with pytest.raises(mhb.BackfillRefused, match="run date minus"):
        mhb.check_window(date(2025, 1, 1), date(2026, 9, 20), datetime(2026, 9, 21, tzinfo=timezone.utc))
    with pytest.raises(mhb.BackfillRefused, match="precedes"):
        mhb.check_window(date(2025, 2, 1), date(2025, 1, 1), now)


# --------------------------------------------------------------------------
# Binance parsing: finalized-bar rules
# --------------------------------------------------------------------------

def test_binance_parser_keeps_only_completed_in_window_bars():
    acquired = datetime(2025, 1, 4, 12, tzinfo=timezone.utc)
    rows = [
        kline(date(2024, 12, 31), "93000.10"),   # out of window
        kline(date(2025, 1, 1), "94000.20"),
        kline(date(2025, 1, 2), "95000.30"),
        kline(date(2025, 1, 4), "96000.40"),      # still open at acquisition
    ]
    bars, notes = mhb.parse_binance_klines(rows, acquired, date(2025, 1, 1), date(2025, 1, 31))
    assert [b.obs_date for b in bars] == [date(2025, 1, 1), date(2025, 1, 2)]
    assert bars[0].value == 94000.2 and bars[0].value_text == "94000.20"
    kinds = sorted(n["kind"] for n in notes)
    assert kinds == ["out_of_window_bar_ignored", "unfinished_bar_rejected"]


def test_binance_parser_flags_malformed_and_microsecond_bars_instead_of_dropping_silently():
    acquired = datetime(2025, 3, 1, tzinfo=timezone.utc)
    micro = kline(date(2025, 1, 5), "97000")
    micro[0] = micro[0] * 1000          # the 2025+ archive's microsecond timestamps
    micro[6] = micro[6] * 1000 + 999
    inconsistent = kline(date(2025, 1, 6), "97000", high="1.0")
    shifted = kline(date(2025, 1, 7), "97000")
    shifted[0] += 3_600_000             # not a UTC day bar
    bars, notes = mhb.parse_binance_klines([micro, inconsistent, shifted], acquired,
                                           date(2025, 1, 1), date(2025, 1, 31))
    assert bars == []
    assert [n["kind"] for n in notes] == ["malformed_provider_bar"] * 3
    assert "malformed_provider_bar" in mhb.BLOCKING_ANOMALIES


class _Resp:
    def __init__(self, status: int, body: object) -> None:
        self.status_code = status
        self.text = body if isinstance(body, str) else json.dumps(body)


class _Session:
    def __init__(self, responses: list[_Resp]) -> None:
        self.responses = responses
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        return self.responses.pop(0)


def test_binance_provider_http_handling():
    spec = mhb.SERIES["BTCUSDT"]
    ok = _Session([_Resp(200, [kline(date(2025, 1, 1), "94000.20")])])
    chunk = mhb.BinanceKlinesProvider(ok, sleep=lambda s: None).fetch(spec, date(2025, 1, 1), date(2025, 1, 31))
    assert [b.obs_date for b in chunk.bars] == [date(2025, 1, 1)]
    assert ok.calls[0]["symbol"] == "BTCUSDT" and ok.calls[0]["interval"] == "1d"
    assert ok.calls[0]["startTime"] == _ms(date(2025, 1, 1))
    assert ok.calls[0]["endTime"] == _ms(date(2025, 1, 31))
    assert len(chunk.response_sha256) == 64
    assert chunk.acquired_at >= chunk.request_started_at

    blocked = _Session([_Resp(451, "")])
    with pytest.raises(mhb.ProviderAccessBlocked):
        mhb.BinanceKlinesProvider(blocked, sleep=lambda s: None).fetch(spec, date(2025, 1, 1), date(2025, 1, 2))
    assert len(blocked.calls) == 1  # never retried

    flaky = _Session([_Resp(502, ""), _Resp(503, ""), _Resp(500, "")])
    with pytest.raises(mhb.ProviderError):
        mhb.BinanceKlinesProvider(flaky, sleep=lambda s: None).fetch(spec, date(2025, 1, 1), date(2025, 1, 2))
    assert len(flaky.calls) == 3  # bounded retry

    limited = _Session([_Resp(429, "")])
    with pytest.raises(mhb.ProviderError):
        mhb.BinanceKlinesProvider(limited, sleep=lambda s: None).fetch(spec, date(2025, 1, 1), date(2025, 1, 2))
    assert len(limited.calls) == 1


# --------------------------------------------------------------------------
# Yahoo parsing
# --------------------------------------------------------------------------

def _yahoo_frame(rows: dict[date, float], multi: bool = True) -> pd.DataFrame:
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in rows])
    data = {"Open": list(rows.values()), "High": [v + 1 for v in rows.values()],
            "Low": [v - 1 for v in rows.values()], "Close": list(rows.values()),
            "Adj Close": [v - 3 for v in rows.values()], "Volume": [1000.0] * len(rows)}
    frame = pd.DataFrame(data, index=idx)
    if multi:
        frame.columns = pd.MultiIndex.from_product([frame.columns, ["SPY"]], names=["Price", "Ticker"])
    return frame


def test_yahoo_parser_uses_raw_close_and_rejects_non_sessions():
    acquired = datetime(2025, 7, 10, tzinfo=timezone.utc)
    frame = _yahoo_frame({date(2025, 7, 3): 620.5, date(2025, 7, 4): 620.5,   # Jul 4 phantom
                          date(2025, 7, 5): 621.0,                              # Saturday
                          date(2025, 7, 7): 618.25, date(2025, 7, 9): float("nan")})
    bars, notes, digest = mhb.parse_yahoo_frame(frame, acquired, date(2025, 7, 1), date(2025, 7, 31))
    assert [(b.obs_date, b.value) for b in bars] == [(date(2025, 7, 3), 620.5), (date(2025, 7, 7), 618.25)]
    assert bars[0].bar["adj_close"] == 617.5  # recorded for audit; the value is the unadjusted close
    kinds = sorted(n["kind"] for n in notes)
    assert kinds == ["malformed_provider_bar", "unexpected_provider_date", "unexpected_provider_date"]
    assert len(digest) == 64


def test_yahoo_parser_rejects_sessions_whose_utc_day_has_not_ended():
    acquired = datetime(2025, 7, 7, 21, 30, tzinfo=timezone.utc)
    bars, notes, _ = mhb.parse_yahoo_frame(_yahoo_frame({date(2025, 7, 7): 618.25}, multi=False),
                                           acquired, date(2025, 7, 1), date(2025, 7, 31))
    assert bars == [] and notes[0]["kind"] == "unfinished_bar_rejected"


def test_yahoo_provider_passes_auto_adjust_false_explicitly():
    seen: dict = {}

    def fake_download(ticker, **kwargs):
        seen.update(kwargs, ticker=ticker)
        return _yahoo_frame({date(2025, 7, 7): 618.25})

    chunk = mhb.YahooDailyProvider(fake_download, sleep=lambda s: None).fetch(
        mhb.SERIES["SPY"], date(2025, 7, 1), date(2025, 7, 31))
    assert seen["auto_adjust"] is False and seen["actions"] is False and seen["repair"] is False
    assert seen["end"] == "2025-08-01"  # yfinance end is exclusive
    assert [b.obs_date for b in chunk.bars] == [date(2025, 7, 7)]


# --------------------------------------------------------------------------
# Namespace isolation and payload markers
# --------------------------------------------------------------------------

def test_namespace_ids_cannot_map_to_any_canonical_feature():
    for spec in mhb.SERIES.values():
        assert spec.series_id.startswith(mhb.NAMESPACE_PREFIX)
        assert SEED_MAPPINGS.get(spec.series_id) is None
        assert yf_candidate_feature_names(spec.series_id) == []
        assert spec.series_id not in CANONICAL_CLOSE_SERIES
        assert spec.series_id != SPY_CLOSE_SERIES


def test_payload_is_never_accepted_by_the_canonical_close_contracts():
    acquired = datetime(2026, 9, 25, 10, tzinfo=timezone.utc)
    bars, _ = mhb.parse_binance_klines([kline(date(2025, 1, 1), "94000.20")], acquired,
                                       date(2025, 1, 1), date(2025, 1, 1))
    chunk = mhb.ProviderChunk(provider="binance_spot_klines_rest", request={}, request_started_at=acquired,
                              acquired_at=acquired, response_sha256="0" * 64, bars=bars)
    payload = mhb._payload(mhb.SERIES["BTCUSDT"], bars[0], chunk, run_id="r", attempt_id="a",
                           digest="d", code_sha="c", revision_of=None)
    assert "close_contract" not in payload and "price_contract_version" not in payload
    assert not is_completed_canonical_close(payload, date(2025, 1, 1), acquired)
    assert not is_policy_capture(payload, date(2025, 1, 1), acquired)
    assert payload["shadows_series_id"] == "binance.BTCUSDT.close"
    assert payload["acquired_at_utc"] == acquired.isoformat()


def test_plan_digest_binds_dates_values_and_sources():
    sid = "backfill_v1:binance.BTCUSDT.close"
    plan = {
        "contract": mhb.BACKFILL_CONTRACT, "forward_owned_from": "2026-09-23",
        "window": {"start": "2025-01-01", "end": "2025-01-31"}, "database": {"name": "x_test"},
        "total_targets": 1,
        "write_sources": {"binance_history_backfill_v1": dict(mhb.WRITE_SOURCE_SPECS["binance_history_backfill_v1"])},
        "series": {sid: {
            "key": "BTCUSDT", "coverage_source_id": 184, "write_source": "binance_history_backfill_v1",
            "targets": [{"obs_date": "2025-01-01", "value": 94000.2, "value_text": "94000.20"}]}},
    }
    base = mhb.plan_digest(plan)
    assert base == mhb.plan_digest(json.loads(json.dumps(plan)))
    plan["series"][sid]["targets"][0]["value_text"] = "94000.21"
    assert mhb.plan_digest(plan) != base
    plan["series"][sid]["targets"][0]["value_text"] = "94000.20"
    plan["series"][sid]["coverage_source_id"] = 185
    assert mhb.plan_digest(plan) != base
    plan["series"][sid]["coverage_source_id"] = 184
    plan["write_sources"]["binance_history_backfill_v1"]["active"] = True
    assert mhb.plan_digest(plan) != base


def test_dedicated_write_sources_are_inactive_non_pit_and_valid_for_schema_checks():
    for name, spec in mhb.WRITE_SOURCE_SPECS.items():
        assert name.endswith("_history_backfill_v1")
        assert spec["active"] is False and spec["pit_available"] is False
        assert spec["cost_tier"] in {"FREE", "LOW", "PAID"}
        assert spec["latency_class"] in {"REALTIME", "EOD", "WEEKLY", "MONTHLY"}
        assert spec["revision_behavior"] in {"NEVER", "RARE", "FREQUENT"}
        assert spec["trust_score"] in {"HIGH", "MED", "LOW"}
    for spec in mhb.SERIES.values():
        assert spec.write_source in mhb.WRITE_SOURCE_SPECS
        assert spec.write_source != spec.coverage_source


def test_cli_refuses_without_database_credentials(monkeypatch, tmp_path):
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    code = mhb.main(["plan", "--series", "BTCUSDT", "--start", "2025-01-01", "--end", "2025-01-31",
                     "--out", str(tmp_path / "plan.json")])
    assert code == mhb.EXIT_USAGE
    assert not (tmp_path / "plan.json").exists()


def test_advisory_lock_key_is_stable_signed_bigint():
    import hashlib

    expected = int.from_bytes(hashlib.sha256(b"grid:market_history_backfill_v1").digest()[:8], "big", signed=True)
    assert mhb.ADVISORY_LOCK_KEY == expected
    assert -(2 ** 63) <= mhb.ADVISORY_LOCK_KEY < 2 ** 63


def test_utc_day_end():
    assert mhb.utc_day_end(date(2025, 12, 31)) == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert mhb.utc_day_end(date(2024, 2, 28)) - timedelta(days=1) == datetime(2024, 2, 28, tzinfo=timezone.utc)
