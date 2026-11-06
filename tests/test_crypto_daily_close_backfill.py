"""Gap-free crypto UTC daily closes and horizon-end crypto scoring (W2-5 / S14).

* Convention: ``binance_utc_close_v1`` bars are dated by the UTC day they
  cover and available only at ``obs_date + 1 00:00 UTC``.
* ``BinancePuller.backfill_klines`` and ``scripts/backfill_crypto_daily_closes.py``
  fill missing completed bars append-only, dry run by default, idempotently.
* The AstroGrid scorer measures crypto predictions at the horizon end.

Unit tests run everywhere (provider and engine are fakes; no HTTP). The
PostgreSQL tests need ``GRID_TEST_DB_URL`` (a local disposable database whose
name contains ``test``); the scorer tests reset the ``astrogrid`` schema like
``test_legacy_crypto_rescore.py``, so run serially (``-p no:xdist``).
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from binance_close_contract import (
    CANONICAL_CLOSE_SERIES,
    DAY_MS,
    bar_available_at,
    completed_kline_payload,
    is_completed_canonical_close,
    last_completed_obs_date,
)
from ingestion.altdata import binance_puller
from ingestion.altdata.binance_puller import BACKFILL_DEFAULT_FIELDS, BinancePuller
from normalization.resolver import Resolver
from store.astrogrid import CRYPTO_EVALUATION_RULE, AstroGridStore, _is_crypto_target
from tests.test_binance_canonical_close_pg import (
    crypto_pg_engine,  # noqa: F401 (fixture)
)
from tests.test_legacy_crypto_rescore import (  # noqa: F401 (fixture)
    _price,
    _run,
    pg_engine,
)

UTC = timezone.utc


def _kline(day: date, close: float) -> list:
    open_ms = int(datetime.combine(day, time.min, UTC).timestamp() * 1000)
    return [open_ms, str(close * 0.99), str(close * 1.02), str(close * 0.98),
            str(close), "10", open_ms + DAY_MS - 1, "0", 1, "0", "0", "0"]


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


# --------------------------------------------------------------------------
# Convention and PIT timestamps
# --------------------------------------------------------------------------


def test_bar_is_available_only_after_its_utc_day_ends() -> None:
    day = date(2026, 4, 10)
    assert bar_available_at(day) == datetime(2026, 4, 11, 0, 0, tzinfo=UTC)
    # One microsecond before the boundary the bar is still open.
    assert completed_kline_payload(_kline(day, 1.0), bar_available_at(day) - timedelta(microseconds=1)) is None
    obs_date, evidence = completed_kline_payload(_kline(day, 1.0), bar_available_at(day) + timedelta(seconds=1))
    assert obs_date == day
    assert evidence["time_zone"] == "UTC" and evidence["interval"] == "1d"
    assert evidence["close_time_ms"] + 1 == int(bar_available_at(day).timestamp() * 1000)


def test_last_completed_obs_date_is_yesterday_utc_in_any_zone() -> None:
    assert last_completed_obs_date(datetime(2026, 11, 6, 0, 0, 1, tzinfo=UTC)) == date(2026, 11, 5)
    # 2026-11-05 22:00 in UTC-5 is 2026-11-06 03:00 UTC.
    est = timezone(timedelta(hours=-5))
    assert last_completed_obs_date(datetime(2026, 11, 5, 22, 0, tzinfo=est)) == date(2026, 11, 5)
    with pytest.raises(ValueError):
        last_completed_obs_date(datetime(2026, 11, 6))  # noqa: DTZ001 (naive on purpose)


def test_sol_close_is_guarded_by_the_completed_close_contract() -> None:
    assert {"binance.BTCUSDT.close", "binance.ETHUSDT.close", "binance.SOLUSDT.close"} <= CANONICAL_CLOSE_SERIES


# --------------------------------------------------------------------------
# Backfill unit tests (fake provider, no database)
# --------------------------------------------------------------------------


def test_range_fetch_pages_utc_daily_klines(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    start, end = date(2023, 1, 1), date(2025, 12, 31)  # > 1000 days: two pages

    class Response:
        status_code = 200

        def __init__(self, payload: list) -> None:
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list:
            return self._payload

    def fake_get(url: str, **kwargs):
        params = kwargs["params"]
        calls.append(params)
        first = datetime.fromtimestamp(params["startTime"] / 1000, UTC).date()
        last = datetime.fromtimestamp(params["endTime"] / 1000, UTC).date()
        days = _days(first, last)[: params["limit"]]
        return Response([_kline(day, 100.0) for day in days])

    monkeypatch.setattr(binance_puller.requests, "get", fake_get)
    monkeypatch.setattr(binance_puller.time, "sleep", lambda _s: None)
    puller = object.__new__(BinancePuller)
    klines = puller._fetch_kline_range("BTCUSDT", start, end)

    assert len(klines) == len(_days(start, end))
    assert len(calls) == 2
    assert all(c["interval"] == "1d" and c["timeZone"] == "0" and c["limit"] == 1000 for c in calls)
    assert calls[1]["startTime"] == int(klines[999][0]) + DAY_MS


def test_backfill_refuses_fields_without_close() -> None:
    puller = object.__new__(BinancePuller)
    with pytest.raises(ValueError, match="close"):
        puller.backfill_klines("BTCUSDT", date(2026, 3, 1), date(2026, 3, 2), fields=("open",))
    assert "volume" not in BACKFILL_DEFAULT_FIELDS and "close" in BACKFILL_DEFAULT_FIELDS


def test_backfill_never_requests_or_keeps_the_open_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    puller = object.__new__(BinancePuller)
    puller.engine = MagicMock()
    puller.source_id = 184
    seen: dict = {}

    def fake_range(_symbol: str, start: date, end: date) -> list:
        seen["end"] = end
        # A misbehaving provider also returns the still-open 2026-03-05 bar.
        return [_kline(day, 100.0) for day in _days(start, date(2026, 3, 5))]

    monkeypatch.setattr(puller, "_fetch_kline_range", fake_range)
    monkeypatch.setattr(puller, "_completed_days_by_field",
                        lambda *_a, **_k: {f: set() for f in BACKFILL_DEFAULT_FIELDS})
    summary = puller.backfill_klines("BTCUSDT", date(2026, 3, 1), date(2026, 3, 31), now=now)

    assert seen["end"] == date(2026, 3, 4)  # clamped to the last completed UTC day
    assert summary["end"] == "2026-03-04" and summary["bars_completed"] == 4
    assert summary["rows_to_insert"] == 4 * len(BACKFILL_DEFAULT_FIELDS)
    assert summary["rows_inserted"] == 0  # dry run
    puller.engine.begin.assert_not_called()


def test_runner_defaults_to_dry_run() -> None:
    from scripts import backfill_crypto_daily_closes as runner

    args = runner.parse_args([])
    assert args.apply is False
    assert args.start == date(2026, 3, 1) and args.end is None
    assert {"BTCUSDT", "ETHUSDT", "SOLUSDT"} <= set(args.symbols)
    assert args.fields == BACKFILL_DEFAULT_FIELDS
    with pytest.raises(SystemExit):
        runner.parse_args(["--fields", "open,high"])
    assert runner.feature_for_symbol("SOLUSDT") == "sol_full"


# --------------------------------------------------------------------------
# Backfill on PostgreSQL: append-only, idempotent, gap-free after resolution
# --------------------------------------------------------------------------


def _fake_provider(prices: dict[date, float]):
    def fetch(_symbol: str, start: date, end: date) -> list:
        return [_kline(day, prices[day]) for day in _days(start, end) if day in prices]
    return fetch


def _raw_count(engine: Engine, sid: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text("SELECT COUNT(*) FROM raw_series WHERE series_id = :s"),
                                {"s": sid}).scalar_one())


def test_backfill_dry_run_apply_idempotent_and_gap_free(
    crypto_pg_engine: Engine, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
) -> None:
    engine = crypto_pg_engine
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO feature_registry VALUES (103, 'sol_full', 'crypto')"))
    today = datetime.now(UTC).date()
    start, end = today - timedelta(days=10), today - timedelta(days=1)
    prices = {day: 100.0 + i for i, day in enumerate(_days(start, end))}
    puller = BinancePuller(engine)
    monkeypatch.setattr(puller, "_fetch_kline_range", _fake_provider(prices))

    # Existing state: one completed close (kept, not duplicated) and one
    # legacy unmarked row (must neither count nor suppress the fill).
    stored_day, legacy_day = start + timedelta(days=2), start + timedelta(days=5)
    evidence = completed_kline_payload(_kline(stored_day, prices[stored_day]),
                                       bar_available_at(stored_day) + timedelta(hours=1))[1]
    with engine.begin() as conn:
        for field in ("open", "high", "low", "close"):
            conn.execute(text("""
                INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload,
                                        pull_status, pull_timestamp)
                VALUES (:sid, 184, :day, :v, CAST(:p AS jsonb), 'SUCCESS', :pulled)
            """), {"sid": f"binance.SOLUSDT.{field}", "day": stored_day, "v": prices[stored_day],
                   "p": json.dumps({"symbol": "SOLUSDT", "field": field, **evidence}),
                   "pulled": bar_available_at(stored_day) + timedelta(hours=1)})
        conn.execute(text("""
            INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status)
            VALUES ('binance.SOLUSDT.close', 184, :day, 1.0, '{"field": "close"}'::jsonb, 'SUCCESS')
        """), {"day": legacy_day})
    before = _raw_count(engine, "binance.SOLUSDT.close")

    dry = puller.backfill_klines("SOLUSDT", start, end)
    assert dry["mode"] == "dry_run"
    assert dry["days_in_range"] == 10
    assert dry["missing_close_days_before"] == 9  # the legacy row does not count
    assert dry["close_days_to_fill"] == 9 and dry["rows_to_insert"] == 36
    assert dry["missing_close_days_after"] == 0 and dry["rows_inserted"] == 0
    assert _raw_count(engine, "binance.SOLUSDT.close") == before  # nothing written

    applied = puller.backfill_klines("SOLUSDT", start, end, apply=True)
    assert applied["rows_inserted"] == 36
    assert _raw_count(engine, "binance.SOLUSDT.close") == before + 9
    assert _raw_count(engine, "binance.SOLUSDT.volume") == 0  # volume excluded

    again = puller.backfill_klines("SOLUSDT", start, end, apply=True)
    assert (again["missing_close_days_before"], again["rows_inserted"]) == (0, 0)

    # Every stored close carries the contract and was captured after its bar closed.
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT obs_date, pull_timestamp, raw_payload FROM raw_series
            WHERE series_id = 'binance.SOLUSDT.close' AND raw_payload ? 'close_contract'
        """)).fetchall()
    assert sorted(r[0] for r in rows) == _days(start, end)  # incl. the legacy row's day
    assert all(is_completed_canonical_close(r[2], r[0], r[1]) for r in rows)

    # The normal resolver path makes sol_full gap-free; releases are never
    # earlier than the bar's availability (no back-dating).
    summary = Resolver(engine).resolve_pending(workers=1, since=datetime.now(UTC) - timedelta(days=30))
    assert summary["errors"] == 0
    with engine.connect() as conn:
        resolved = conn.execute(text("""
            SELECT rs.obs_date, rs.release_date, rs.value FROM resolved_series rs
            JOIN feature_registry fr ON fr.id = rs.feature_id
            WHERE fr.name = 'sol_full' ORDER BY rs.obs_date
        """)).fetchall()
    assert [r[0] for r in resolved] == _days(start, end)
    assert all(r[1] >= r[0] + timedelta(days=1) for r in resolved)
    assert all(r[2] == prices[r[0]] for r in resolved)  # the legacy 1.0 never resolves
    print("CRYPTO_BACKFILL_PG days=10 filled=9 kept=1 rerun_inserted=0 sol_full_gaps=0")


def test_runner_reports_feature_gaps_before_and_projected_after(
    crypto_pg_engine: Engine, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    capsys: pytest.CaptureFixture[str],
) -> None:
    from scripts import backfill_crypto_daily_closes as runner

    engine = crypto_pg_engine
    now = datetime.now(UTC)
    end = last_completed_obs_date(now)
    start = end - timedelta(days=6)
    with engine.begin() as conn:  # btc_full has 3 of 7 days resolved already
        for day in _days(start, start + timedelta(days=2)):
            conn.execute(text("""
                INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date,
                                             value, source_priority_used)
                VALUES (101, :d, :r, :r, 1.0, 184)
            """), {"d": day, "r": day + timedelta(days=1)})
    missing_day = end  # the provider has no bar for this day
    prices = {day: 50.0 for day in _days(start, end) if day != missing_day}

    def factory(eng: Engine) -> BinancePuller:
        puller = BinancePuller(eng)
        monkeypatch.setattr(puller, "_fetch_kline_range", _fake_provider(prices))
        return puller

    # The real dry-run path: every transaction is READ ONLY.
    assert runner.main(["--symbols", "BTCUSDT", "--start", start.isoformat()], engine=engine,
                       puller_factory=factory, now=now) == 0
    out = capsys.readouterr().out
    report = json.loads(out[out.index("{"):])
    assert report["mode"] == "dry_run"
    btc = report["symbols"]["BTCUSDT"]
    assert btc["feature_gaps"]["feature"] == "btc_full"
    assert btc["feature_gaps"]["gap_days_before"] == 4
    assert btc["feature_gaps"]["gap_days_after_projected"] == 1
    assert btc["feature_gaps"]["gap_dates_after_projected"] == [missing_day.isoformat()]
    assert _raw_count(engine, "binance.BTCUSDT.close") == 0  # dry run wrote nothing


def test_runner_dry_run_engine_is_read_only(crypto_pg_engine: Engine) -> None:  # noqa: F811
    from scripts.backfill_crypto_daily_closes import read_only_engine

    with pytest.raises(Exception, match="read-only"), read_only_engine(crypto_pg_engine).begin() as conn:
        conn.execute(text("INSERT INTO source_catalog VALUES (999, 'x', 1, NULL)"))


# --------------------------------------------------------------------------
# Horizon-end crypto scoring
# --------------------------------------------------------------------------


def test_crypto_target_detection() -> None:
    assert _is_crypto_target(["BTC"]) and _is_crypto_target(["BTC", "SOL"])
    assert not _is_crypto_target(["BTC", "SPY"])
    assert not _is_crypto_target(["AAPL"]) and not _is_crypto_target([])


def _candidate_row(symbols: list[str], as_of: datetime) -> tuple:
    """A ``score_predictions`` row: as_of_ts and created_at are tz-aware."""
    return (1, "pred", as_of, "swing", "liquid_market", symbols, "buy", "setup", "inv",
            "{}", "{}", "{}", "q", None, as_of, "astrogrid-seed-v1")


@pytest.mark.parametrize("symbols, expected_eval", [
    (["BTC"], date(2026, 3, 8)),     # crypto: start + 7
    (["AAPL"], date(2026, 3, 29)),   # equity: unchanged run-date rule
])
def test_scorer_evaluation_date_by_asset_class(symbols: list[str], expected_eval: date) -> None:
    store = AstroGridStore(MagicMock())
    store._build_prediction_score = MagicMock(return_value={"raw_payload": {}, "verdict": "hit"})
    row = _candidate_row(symbols, datetime(2026, 3, 1, 15, tzinfo=UTC))
    result = store._score_candidate_row(
        MagicMock(), row, evaluation_date=date(2026, 3, 29),
        score_cutoff=datetime(2026, 3, 29, 12, tzinfo=UTC),
    )
    kwargs = store._build_prediction_score.call_args.kwargs
    assert kwargs["evaluation_date"] == expected_eval
    assert kwargs["start_date"] == date(2026, 3, 1)
    if symbols == ["BTC"]:
        assert kwargs["release_cutoff"] == date(2026, 3, 29)
        assert result["score"]["raw_payload"]["evaluation_rule"] == CRYPTO_EVALUATION_RULE
        assert result["score"]["raw_payload"]["maturity_date"] == "2026-03-08"
    else:
        assert kwargs["release_cutoff"] is None
        assert "evaluation_rule" not in result["score"]["raw_payload"]


def test_crypto_horizon_end_bar_still_open_is_not_mature() -> None:
    store = AstroGridStore(MagicMock())
    store._build_prediction_score = MagicMock()
    row = _candidate_row(["ETH"], datetime(2026, 3, 1, 15, tzinfo=UTC))
    # Maturity day 2026-03-08 has started but its UTC bar closes at 03-09 00:00.
    result = store._score_candidate_row(
        MagicMock(), row, evaluation_date=date(2026, 3, 8),
        score_cutoff=datetime(2026, 3, 8, 23, 59, tzinfo=UTC),
    )
    assert result["state"] == "not_mature"
    store._build_prediction_score.assert_not_called()


def _clean_btc(engine: Engine, start: date, model: str = "grid-crypto-v1") -> int:
    as_of = datetime.combine(start, time(15), UTC)
    return _run(engine, model=model, created_at=as_of, as_of=as_of, symbols_json='["BTC"]',
                jsonb_string_payloads=False)


def _base_scores(engine: Engine) -> dict[int, dict]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT prediction_run_id, verdict, realized_return, raw_payload
            FROM astrogrid.prediction_score
        """)).fetchall()
    return {r[0]: {"verdict": r[1], "realized_return": r[2], "raw_payload": r[3]} for r in rows}


def test_non_legacy_crypto_is_scored_at_horizon_end(pg_engine: Engine) -> None:  # noqa: F811
    today = datetime.now(UTC).date()
    start = today - timedelta(days=20)
    run_id = _clean_btc(pg_engine, start)
    _price(pg_engine, 1, start, 100.0)
    _price(pg_engine, 1, start + timedelta(days=7), 105.0)   # horizon end: +5% -> hit
    _price(pg_engine, 1, today - timedelta(days=1), 60.0)    # run-date crash must not count

    summary = AstroGridStore(pg_engine).score_predictions(as_of_date=today, limit=50)

    assert summary["scored"] == 1
    score = _base_scores(pg_engine)[run_id]
    assert score["realized_return"] == pytest.approx(0.05)
    assert score["verdict"] == "hit"
    payload = score["raw_payload"]
    assert payload["evaluation_date"] == (start + timedelta(days=7)).isoformat()
    assert payload["evaluation_rule"] == CRYPTO_EVALUATION_RULE
    assert payload["close_contract"] == "binance_utc_close_v1"


def test_missing_horizon_end_close_stays_pending_never_wrong(pg_engine: Engine) -> None:  # noqa: F811
    today = datetime.now(UTC).date()
    start = today - timedelta(days=20)
    run_id = _clean_btc(pg_engine, start)
    _price(pg_engine, 1, start, 100.0)
    _price(pg_engine, 1, start + timedelta(days=6), 90.0)    # day before: not substituted
    _price(pg_engine, 1, today - timedelta(days=1), 130.0)   # later close: not substituted

    summary = AstroGridStore(pg_engine).score_predictions(as_of_date=today, limit=50)

    assert summary["scored"] == 0
    assert summary["unscored"][0]["reason"] == "stale_canonical_price"
    assert summary["unscored"][0]["phase"] == "outcome"
    assert run_id not in _base_scores(pg_engine)


def test_maturity_day_itself_is_not_mature_and_records_no_skip(pg_engine: Engine) -> None:  # noqa: F811
    today = datetime.now(UTC).date()
    start = today - timedelta(days=7)  # matures today; today's bar is open
    run_id = _clean_btc(pg_engine, start)
    _price(pg_engine, 1, start, 100.0)

    summary = AstroGridStore(pg_engine).score_predictions(as_of_date=today, limit=50)

    assert summary["skipped_not_mature"] == 1 and summary["scored"] == 0
    with pg_engine.connect() as conn:
        skips = conn.execute(text("SELECT COUNT(*) FROM astrogrid.prediction_score_skip")).scalar_one()
    assert skips == 0
    assert run_id not in _base_scores(pg_engine)


def test_vintage_released_after_score_cutoff_is_not_read(pg_engine: Engine) -> None:  # noqa: F811
    today = datetime.now(UTC).date()
    start = today - timedelta(days=30)
    run_id = _clean_btc(pg_engine, start)
    horizon = start + timedelta(days=7)
    _price(pg_engine, 1, start, 100.0)
    # The horizon-end close first became known 10 days after maturity.
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value)
            VALUES (1, :d, :r, :r, 110.0)
        """), {"d": horizon, "r": horizon + timedelta(days=10)})
    store = AstroGridStore(pg_engine)

    early = store.score_predictions(as_of_date=horizon + timedelta(days=2), limit=50)
    assert early["scored"] == 0 and run_id not in _base_scores(pg_engine)

    late = store.score_predictions(as_of_date=horizon + timedelta(days=11), limit=50)
    assert late["scored"] == 1
    assert _base_scores(pg_engine)[run_id]["realized_return"] == pytest.approx(0.10)
