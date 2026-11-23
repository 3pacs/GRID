"""Regression tests for verified defects in signal_convergence_scanner and conviction_scorer.

Covers:
1. Prediction-market double negation in _scan_prediction_market.
2. Neutral streams contributing to the directional numerator in _compute_convergence.
3. FINRA short_volume_ratio treated as directional in _scan_darkpool.
4. Stale/undated 13F filings mixed into the 7-day convergence window in _scan_institutional.
5. SMART_MONEY pooled whichever INST_FLOW row sorted last; it now reads no
   institutional series (no INST_FLOW writer; 13F is non-directional).
6. SMART_MONEY Form 4: only the reported P/S family from SEC_EDGAR (venue
   unknown, priced and share-valid counts matched per vintage, scored over
   the contiguous verified coverage run), never all-code NET_SHARES
   (awards/exercises); dead XBRL:{ticker}:* series.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
    text,
)
from sqlalchemy.pool import StaticPool

from alpha_research import conviction_scorer as cs
from intelligence import signal_convergence_scanner as scs

AS_OF = date(2026, 4, 13)
T3 = AS_OF - timedelta(days=3)


def _utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)


# ── Minimal SQL doubles for signal_convergence_scanner ────────────────────


class _FakeResult:
    def __init__(self, rows: list[Any], fetchone_val: Any = None) -> None:
        self._rows = rows
        self._fetchone_val = fetchone_val

    def fetchall(self) -> list[Any]:
        return list(self._rows)

    def fetchone(self) -> Any:
        if self._fetchone_val is not None:
            return self._fetchone_val
        return self._rows[0] if self._rows else None


class _ScannerConn:
    def __init__(
        self,
        stream_rows: dict[str, list[tuple[Any, ...]]],
        trust_scores: dict[str, float] | None = None,
    ) -> None:
        self._stream_rows = stream_rows
        self._trust_scores = trust_scores or {}

    def __enter__(self) -> "_ScannerConn":
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(stmt)
        params = params or {}
        if "AVG(trust_score)" in sql:
            stype = params.get("stype")
            return _FakeResult([], fetchone_val=(self._trust_scores.get(stype),))
        if "FROM signal_sources" in sql and "source_type = :stype" in sql:
            stype = params.get("stype")
            t0 = params.get("t0")
            as_of = params.get("as_of")
            as_of_ts = params.get("as_of_ts")
            out = []
            for row in self._stream_rows.get(stype, []):
                _sid, sdate, _st, _sval, _trust, created = row
                if t0 and isinstance(sdate, date) and sdate < t0:
                    continue
                if as_of and isinstance(sdate, date) and sdate > as_of:
                    continue
                if as_of_ts and isinstance(created, datetime) and created > as_of_ts:
                    continue
                out.append(row)
            return _FakeResult(out)
        return _FakeResult([])


class _ScannerEngine:
    def __init__(self, conn: _ScannerConn) -> None:
        self._conn = conn

    def connect(self) -> _ScannerConn:
        return self._conn


# ── Minimal SQL doubles for conviction_scorer ─────────────────────────────


@dataclass(frozen=True)
class _SQLCall:
    sql: str
    params: dict[str, Any]


class _RawSeriesConn:
    """In-memory raw_series mock for exact ``series_id = :s`` reads."""

    def __init__(self, rows_by_series: dict[str, list[tuple[date, float]]]) -> None:
        self._rows_by_series = rows_by_series
        self.calls: list[_SQLCall] = []

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql = str(stmt)
        bound = dict(params or {})
        self.calls.append(_SQLCall(sql=sql, params=bound))

        if "FROM raw_series" not in sql:
            return _FakeResult([])

        cutoff = bound.get("d")
        as_of = bound.get("as_of")

        matched: list[tuple[date, float]] = []
        if "series_id = :s" in sql:
            sid = str(bound.get("s", ""))
            matched.extend(self._rows_by_series.get(sid, []))

        filtered = [
            (obs_d, val)
            for obs_d, val in matched
            if (cutoff is None or obs_d >= cutoff) and (as_of is None or obs_d <= as_of)
        ]
        filtered.sort(key=lambda r: r[0])
        return _FakeResult(filtered)


class _DummyEngine:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def connect(self) -> "_DummyEngine":
        return self

    def __enter__(self) -> Any:
        return self._conn

    def __exit__(self, *_args: Any) -> None:
        return None


# ── Defect 1: Prediction-market double negation ───────────────────────────


class TestDefect1PredictionMarketSignedShift:
    def test_down_row_with_negative_signed_shift_is_bearish(self) -> None:
        """Writer stores shift = current_prob - previous_prob (< 0) with direction='DOWN'."""
        payload = json.dumps({
            "title": "Will NVDA beat?",
            "direction": "DOWN",
            "shift": -0.18,
            "current_prob": 0.42,
        })
        row = ("poly_nvda", T3, "RAPID_SHIFT", payload, 0.6, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_PREDICTION_MARKET: [row]}))

        sig = scs._scan_prediction_market(engine, "NVDA", AS_OF, 7, {})

        assert sig is not None
        assert sig.direction == scs.BEARISH
        assert sig.raw_payload["net_shift"] == pytest.approx(-0.18)

    def test_up_row_with_positive_signed_shift_is_bullish(self) -> None:
        payload = json.dumps({
            "title": "Will NVDA beat?",
            "direction": "UP",
            "shift": 0.15,
            "current_prob": 0.65,
        })
        row = ("poly_nvda", T3, "RAPID_SHIFT", payload, 0.6, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_PREDICTION_MARKET: [row]}))

        sig = scs._scan_prediction_market(engine, "NVDA", AS_OF, 7, {})

        assert sig is not None
        assert sig.direction == scs.BULLISH
        assert sig.raw_payload["net_shift"] == pytest.approx(0.15)

    def test_mixed_up_and_down_rows_aggregate_signed_shifts(self) -> None:
        rows = [
            (
                "poly_1",
                T3,
                "RAPID_SHIFT",
                json.dumps({"title": "A", "direction": "UP", "shift": 0.10, "current_prob": 0.60}),
                0.6,
                _utc(T3),
            ),
            (
                "poly_2",
                T3,
                "RAPID_SHIFT",
                json.dumps({"title": "B", "direction": "DOWN", "shift": -0.25, "current_prob": 0.35}),
                0.6,
                _utc(T3),
            ),
        ]
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_PREDICTION_MARKET: rows}))

        sig = scs._scan_prediction_market(engine, "NVDA", AS_OF, 7, {})

        assert sig is not None
        assert sig.direction == scs.BEARISH
        assert sig.raw_payload["net_shift"] == pytest.approx(-0.15)
        assert sig.raw_payload["max_shift"] == pytest.approx(0.25)


# ── Defect 2: Neutral weight in denominator only ──────────────────────────


class TestDefect2NeutralDenominatorOnly:
    def test_neutral_streams_dilute_and_never_promote_multiplier_tier(self) -> None:
        """Two 0.30-intensity aligned streams (score 0.30) + two neutral streams must dilute to 0.15, not lift to 0.40."""
        aligned_only = [
            scs.StreamSignal("congressional", 0.30, scs.BULLISH, 1.0, "2026-04-10  C  x"),
            scs.StreamSignal("insider", 0.30, scs.BULLISH, 1.0, "2026-04-10  I  x"),
        ]
        with_neutrals = aligned_only + [
            scs.StreamSignal("darkpool", 1.0, scs.NEUTRAL, 1.0, "2026-04-10  D  x"),
            scs.StreamSignal("social", 1.0, scs.NEUTRAL, 1.0, "2026-04-10  S  x"),
        ]

        base_score, _, _, _, _ = scs._compute_convergence(aligned_only, scs.BULLISH)
        score, n_al, n_op, w_a, w_o = scs._compute_convergence(with_neutrals, scs.BULLISH)
        mult = scs._pick_multiplier(n_al, n_op, score, w_a, w_o)

        assert base_score == pytest.approx(0.30)
        assert score == pytest.approx(0.15)
        assert score < base_score
        assert (n_al, n_op) == (2, 0)
        assert w_a == pytest.approx(0.60)
        assert w_o == pytest.approx(0.0)
        assert mult == scs.MULTIPLIER_NEUTRAL

    def test_neutral_only_streams_score_zero_for_either_direction(self) -> None:
        sigs = [
            scs.StreamSignal("darkpool", 1.0, scs.NEUTRAL, 1.0, "2026-04-10  D  x"),
            scs.StreamSignal("social", 0.8, scs.NEUTRAL, 1.0, "2026-04-10  S  x"),
        ]
        bull_score, n_al_b, n_op_b, _, _ = scs._compute_convergence(sigs, scs.BULLISH)
        bear_score, n_al_s, n_op_s, _, _ = scs._compute_convergence(sigs, scs.BEARISH)

        assert bull_score == pytest.approx(0.0)
        assert bear_score == pytest.approx(0.0)
        assert (n_al_b, n_op_b, n_al_s, n_op_s) == (0, 0, 0, 0)


# ── Defect 3: FINRA short_volume_ratio non-directional ────────────────────


class TestDefect3FinraShortVolumeNonDirectional:
    @pytest.mark.parametrize("ratio", [0.75, 0.90, 0.15, 0.05])
    def test_short_volume_ratio_stays_neutral_and_preserves_context(self, ratio: float) -> None:
        payload = json.dumps({"spike_ratio": 3.0, "short_volume_ratio": ratio})
        row = ("dp_aapl", T3, "UNUSUAL_VOLUME", payload, 0.6, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_DARKPOOL: [row]}))

        sig = scs._scan_darkpool(engine, "AAPL", AS_OF, 7, {})

        assert sig is not None
        assert sig.direction == scs.NEUTRAL
        assert sig.raw_payload["avg_short_ratio"] == pytest.approx(ratio)
        assert f"short% {ratio:.2f}" in sig.evidence_line

    def test_high_short_volume_ratio_does_not_count_as_opposed_vote(self) -> None:
        rows = {
            scs.STREAM_CONGRESSIONAL: [
                ("Rep X", T3, "BUY", json.dumps({"amount_midpoint": 500_000.0}), 0.8, _utc(T3))
            ],
            scs.STREAM_DARKPOOL: [
                ("dp_aapl", T3, "UNUSUAL_VOLUME", json.dumps({"spike_ratio": 3.2, "short_volume_ratio": 0.78}), 0.6, _utc(T3))
            ],
        }
        engine = _ScannerEngine(_ScannerConn(rows, trust_scores={scs.STREAM_CONGRESSIONAL: 1.0, scs.STREAM_DARKPOOL: 1.0}))

        report = scs.scan_convergence(engine, ticker="AAPL", as_of=AS_OF, target_direction=scs.BULLISH)

        assert report.n_aligned == 1
        assert report.n_opposed == 0
        dp = [s for s in report.stream_signals if s.stream_name == scs.STREAM_DARKPOOL]
        assert len(dp) == 1
        assert dp[0].direction == scs.NEUTRAL
        assert dp[0].raw_payload["avg_short_ratio"] == pytest.approx(0.78)


# ── Defect 4: 13F stale quarter-end holdings excluded from 7-day window ───


class TestDefect4Stale13FExcluded:
    @pytest.fixture(autouse=True)
    def _test_only_directional_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """These cases test the period-end filter, which sits on the test-only
        re-enable path: 13F is non-directional by default (owner decision
        2026-11-23), and the default abstention is tested in
        tests/test_signal_convergence_scanner.py::TestExtractorInstitutional."""
        monkeypatch.setattr(scs, "INSTITUTIONAL_13F_DIRECTIONAL", True)

    def test_13f_without_period_end_is_excluded_from_7d_window(self) -> None:
        """institutional_flows.py writes filing date only (no holdings period_end)."""
        payload = json.dumps({
            "manager": "BERKSHIRE HATHAWAY INC",
            "action": "INCREASED",
            "new_positions": 4,
            "increased": 2,
            "closed_positions": 0,
            "decreased": 0,
        })
        row = ("0001067983:BRK", T3, "NET_POSITION_DELTA", payload, 0.9, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_INSTITUTIONAL: [row]}))

        sig = scs._scan_institutional(engine, "AAPL", AS_OF, 7, {})

        assert sig is None

    def test_13f_with_45_day_old_period_end_excluded_from_7d_window_but_allowed_in_60d(self) -> None:
        quarter_end = AS_OF - timedelta(days=45)
        payload = json.dumps({
            "action": "INCREASED",
            "new_positions": 5,
            "closed_positions": 0,
            "period_end": quarter_end.isoformat(),
        })
        row = ("0001067983:BRK", T3, "NET_POSITION_DELTA", payload, 0.9, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_INSTITUTIONAL: [row]}))

        assert scs._scan_institutional(engine, "AAPL", AS_OF, 7, {}) is None

        wide_sig = scs._scan_institutional(engine, "AAPL", AS_OF, 60, {})
        assert wide_sig is not None
        assert wide_sig.direction == scs.BULLISH

    def test_13f_with_in_window_period_end_is_included(self) -> None:
        fresh_period = AS_OF - timedelta(days=5)
        payload = json.dumps({
            "new_positions": 4,
            "closed_positions": 0,
            "period_end": fresh_period.isoformat(),
        })
        row = ("0001067983:BRK", T3, "NET_POSITION_DELTA", payload, 0.9, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_INSTITUTIONAL: [row]}))

        sig = scs._scan_institutional(engine, "AAPL", AS_OF, 7, {})

        assert sig is not None
        assert sig.direction == scs.BULLISH

    def test_13f_with_future_period_end_excluded_pit(self) -> None:
        future_period = AS_OF + timedelta(days=1)
        payload = json.dumps({
            "new_positions": 4,
            "closed_positions": 0,
            "period_end": future_period.isoformat(),
        })
        row = ("0001067983:BRK", T3, "NET_POSITION_DELTA", payload, 0.9, _utc(T3))
        engine = _ScannerEngine(_ScannerConn({scs.STREAM_INSTITUTIONAL: [row]}))

        assert scs._scan_institutional(engine, "AAPL", AS_OF, 7, {}) is None


# ── Defects 5/6: SMART_MONEY reads only reported P/S Form 4 from SEC_EDGAR ──
#
# Real SQL against an in-memory SQLite raw_series + source_catalog shaped like
# production (as in tests/test_store_observations.py), so every read goes
# through store.observations the way it does on griddb.
#
# ingestion/edgar.py writes, for every date of a pull's verified coverage
# interval, in one transaction (one pull_timestamp):
# - the all-code NET_SHARES / NET_VALUE / TXN_COUNT;
# - the P/S family PS_NET_SHARES / PS_NET_VALUE / PS_TXN_COUNT /
#   PS_SHARES_VALID_COUNT / PS_PRICED_COUNT;
# - FILINGS_LISTED and FILINGS_UNREAD.
#
# Writer-to-reader fixtures live in tests/test_form4_writer_to_smart_money.py.

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

EDGAR_SRC = 1
OTHER_SRC = 2
LABEL = "Insider reported P/S (non-derivative, venue unknown)"
FULL_FIRST = AS_OF - timedelta(days=90)  # 90 verified days ending the day before as_of
LAST = AS_OF - timedelta(days=1)


def _evening(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) + timedelta(hours=22)


class _Form4DB:
    def __init__(self) -> None:
        # One shared connection: the API test reads it from a worker thread.
        self.engine = create_engine(
            "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False},
        )
        md = MetaData()
        source_catalog = Table(
            "source_catalog", md,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
        )
        self._raw = Table(
            "raw_series", md,
            Column("series_id", String, nullable=False),
            Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
            Column("obs_date", Date, nullable=False),
            Column("pull_timestamp", DateTime, nullable=False),
            Column("value", Float, nullable=False),
            Column("raw_payload", Text),
            Column("pull_status", String, nullable=False),
        )
        md.create_all(self.engine)
        with self.engine.begin() as c:
            c.execute(source_catalog.insert(), [
                {"id": EDGAR_SRC, "name": "SEC_EDGAR"},
                {"id": OTHER_SRC, "name": "openinsider"},
            ])
        self.series_read: list[str] = []
        self.sql: list[str] = []

        @event.listens_for(self.engine, "before_cursor_execute")
        def _record(_conn, _cursor, statement, parameters, _context, _many):  # type: ignore[no-untyped-def]
            self.sql.append(statement)
            values = parameters.values() if isinstance(parameters, dict) else (parameters or ())
            self.series_read.extend(str(v) for v in values if isinstance(v, str) and ":" in v)

    def add(
        self, series_id: str, obs_date: date, value: float, *,
        pulled: datetime | None = None, source_id: int = EDGAR_SRC, status: str = "SUCCESS",
    ) -> None:
        with self.engine.begin() as c:
            c.execute(self._raw.insert(), [{
                "series_id": series_id, "source_id": source_id, "obs_date": obs_date,
                "pull_timestamp": pulled or _evening(obs_date),
                "value": value, "raw_payload": "{}", "pull_status": status,
            }])

    def filing_day(
        self, obs_date: date, *, ps_shares: float = 0.0, ps_value: float = 0.0, txns: float = 0.0,
        valid: float | None = None, priced: float | None = None,
        listed: float | None = None, unread: float = 0.0,
        all_code_shares: float = 0.0, all_code_value: float = 0.0, all_code_txns: float = 0.0,
        pulled: datetime | None = None, skip: tuple[str, ...] = (), ticker: str = "AAPL",
    ) -> None:
        """One covered date as ingestion/edgar.py writes it: one vintage for all series."""
        values = {
            "NET_SHARES": all_code_shares, "NET_VALUE": all_code_value, "TXN_COUNT": all_code_txns,
            "PS_NET_SHARES": ps_shares, "PS_NET_VALUE": ps_value, "PS_TXN_COUNT": txns,
            "PS_SHARES_VALID_COUNT": txns if valid is None else valid,
            "PS_PRICED_COUNT": txns if priced is None else priced,
            "FILINGS_LISTED": (1.0 if txns else 0.0) if listed is None else listed,
            "FILINGS_UNREAD": unread,
        }
        for field, value in values.items():
            if field not in skip:
                self.add(f"FORM4:{ticker}:{field}", obs_date, value, pulled=pulled or _evening(obs_date))

    def cover(
        self, first: date, last: date, days: dict[date, dict] | None = None,
        *, pulled: datetime | None = None, skip_dates: tuple[date, ...] = (),
    ) -> None:
        """Every date in [first, last] written, as the writer covers an interval."""
        d = first
        while d <= last:
            if d not in skip_dates:
                self.filing_day(d, pulled=pulled, **(days or {}).get(d, {}))
            d += timedelta(days=1)

    def score(self, ticker: str = "AAPL", as_of: date = AS_OF) -> cs.LayerResult:
        self.series_read.clear()
        self.sql.clear()
        with self.engine.connect() as c:
            return cs.score_smart_money(c, ticker, as_of_date=as_of)


def _no_venue_claim(result: cs.LayerResult) -> bool:
    return not any("open-market" in s.lower() or "open market" in s.lower() for s in result.signals)


class TestDefect5NoPooledInstitutionalRead:
    def test_no_institutional_series_is_read_and_the_gap_is_named(self) -> None:
        db = _Form4DB()
        # A stray INST_FLOW row (no writer exists in GRID) must not be scored.
        db.add("INST_FLOW:AAPL:net", T3, 9_000_000.0, source_id=OTHER_SRC)

        result = db.score()

        assert result.data_available is False
        assert result.score == 0
        assert not any(sid.startswith("INST_FLOW") for sid in db.series_read)
        assert not any("LIKE" in sql.upper() for sql in db.sql)
        assert any("Institutional flow unavailable" in s for s in result.signals)


class TestDefect6Form4AndCompanyUnavailable:
    @pytest.mark.parametrize(
        ("ps_value", "expected_score", "signal"),
        [
            (750_000.0, 5, f"{LABEL} net buy $750,000"),
            (100_000.0, 2, f"{LABEL} net buy $100,000"),
            # Insider selling is capped at -2 (routine liquidity / 10b5-1).
            (-750_000.0, -2, f"{LABEL} net sell $750,000"),
            (-100_000.0, -2, f"{LABEL} net sell $100,000"),
            (0.0, 0, f"{LABEL} buys and sells offset (fully priced, net $0"),
        ],
    )
    def test_full_verified_window_fully_priced_uses_the_dollar_rule(
        self, ps_value: float, expected_score: int, signal: str,
    ) -> None:
        db = _Form4DB()
        db.cover(FULL_FIRST, LAST, {T3: {"ps_shares": 1.0, "ps_value": ps_value, "txns": 2.0}})

        result = db.score()

        assert result.data_available is True
        assert result.score == expected_score
        assert result.trust == 0.85
        assert any(s.startswith(signal) and "90d of 90d" in s for s in result.signals), result.signals
        assert _no_venue_claim(result)
        # Exact zero is a measured offset, never labelled as an outflow.
        assert not (ps_value == 0 and any("net sell" in s for s in result.signals))

    @pytest.mark.parametrize(
        ("ps_shares", "ps_value", "txns", "priced", "expected_score", "signal"),
        [
            # P +100 unpriced and S 10 @ $10: the priced dollars alone read -$100.
            (90.0, -100.0, 2.0, 1.0, 2, "net share buy +90 sh (dollars unverified: 1 of 2 P/S rows unpriced"),
            (60.0, 0.0, 2.0, 0.0, 2, "net share buy +60 sh (dollars unverified: 2 of 2 P/S rows unpriced"),
            (-25_000.0, 0.0, 1.0, 0.0, -2, "net share sell -25,000 sh (dollars unverified: 1 of 1"),
            (0.0, 500.0, 2.0, 1.0, 0, "share buys and sells offset (net 0 sh; dollars unverified"),
        ],
    )
    def test_unpriced_rows_fall_back_to_share_direction_only(
        self, ps_shares: float, ps_value: float, txns: float, priced: float,
        expected_score: int, signal: str,
    ) -> None:
        db = _Form4DB()
        db.cover(FULL_FIRST, LAST, {T3: {"ps_shares": ps_shares, "ps_value": ps_value,
                                         "txns": txns, "priced": priced}})

        result = db.score()

        assert result.data_available is True
        assert result.score == expected_score
        assert any(signal in s for s in result.signals), result.signals
        assert not any("$" in s for s in result.signals if "P/S" in s)  # no dollar total claimed

    def test_seven_day_cold_start_uses_the_short_window_policy(self) -> None:
        db = _Form4DB()
        db.cover(AS_OF - timedelta(days=7), LAST, {T3: {"ps_shares": 9_000.0, "ps_value": 750_000.0,
                                                        "txns": 1.0}})

        result = db.score()

        assert result.data_available is True
        assert result.score == 2  # direction only: never the +5 tier on 7 verified days
        assert result.trust == 0.5
        span = f"verified {AS_OF - timedelta(days=7):%Y-%m-%d}..{LAST:%Y-%m-%d}, 7d of 90d"
        assert f"{LABEL} net buy (net $750,000; {span}; short window, direction only)" in result.signals

    def test_fewer_than_seven_verified_days_is_not_scored(self) -> None:
        db = _Form4DB()
        db.cover(AS_OF - timedelta(days=6), LAST, {T3: {"ps_shares": 9_000.0, "ps_value": 750_000.0,
                                                        "txns": 1.0}})

        result = db.score()

        assert result.data_available is False
        assert result.score == 0
        assert any("coverage too short" in s and "6d of 90d" in s for s in result.signals)

    def test_a_gap_in_coverage_ends_the_verified_run(self) -> None:
        db = _Form4DB()
        gap = AS_OF - timedelta(days=30)
        # A big sale before the gap must not reach the score.
        db.cover(FULL_FIRST, LAST, {
            AS_OF - timedelta(days=60): {"ps_shares": -90_000.0, "ps_value": -9_000_000.0, "txns": 1.0},
            T3: {"ps_shares": 900.0, "ps_value": 750_000.0, "txns": 1.0},
        }, skip_dates=(gap,))

        result = db.score()

        assert result.score == 2
        assert result.trust == 0.5
        assert any("29d of 90d" in s and "net buy" in s for s in result.signals)

    @pytest.mark.parametrize(
        "damage",
        ["missing_series", "mixed_vintage", "unread_filing", "invalid_share_rows",
         "priced_over_valid", "txns_without_a_listed_filing"],
    )
    def test_an_unverified_date_ends_the_run(self, damage: str) -> None:
        bad = AS_OF - timedelta(days=1)
        kwargs: dict[str, Any] = {"ps_shares": -50.0, "ps_value": -5_000.0, "txns": 1.0}
        if damage == "missing_series":
            kwargs["skip"] = ("PS_PRICED_COUNT",)
        elif damage == "mixed_vintage":
            kwargs["skip"] = ("PS_NET_VALUE",)
        elif damage == "unread_filing":
            kwargs["unread"] = 1.0
        elif damage == "invalid_share_rows":
            kwargs.update(txns=2.0, valid=1.0, priced=1.0)
        elif damage == "priced_over_valid":
            kwargs["priced"] = 2.0
        else:
            kwargs["listed"] = 0.0
        db = _Form4DB()
        db.cover(FULL_FIRST, AS_OF - timedelta(days=2), {T3: {"ps_shares": 900.0, "ps_value": 750_000.0,
                                                              "txns": 1.0}})
        db.filing_day(bad, **kwargs)
        if damage == "mixed_vintage":
            db.add("FORM4:AAPL:PS_NET_VALUE", bad, -5_000.0, pulled=_evening(bad) + timedelta(hours=1))

        result = db.score()

        # The last closed date is unverified: nothing verified ends at as_of.
        assert result.data_available is False
        assert result.score == 0
        assert any("coverage not verified through the as-of date" in s for s in result.signals)

    def test_award_only_window_scores_zero_without_claiming_award_codes(self) -> None:
        db = _Form4DB()
        # Grants and exercises: all-code sums large and positive, no P/S rows.
        db.cover(FULL_FIRST, LAST, {T3: {"listed": 1.0, "all_code_shares": 250_000.0,
                                         "all_code_value": 40_000_000.0, "all_code_txns": 4.0}})

        result = db.score()

        assert result.score == 0
        assert result.data_available is True
        assert any(
            s.startswith("No reported non-derivative P/S insider transactions in the observed "
                         "Form 4 data (verified")
            for s in result.signals
        )
        assert not any("award" in s.lower() for s in result.signals if "P/S" in s)
        assert "FORM4:AAPL:NET_SHARES" not in db.series_read
        assert "FORM4:AAPL:NET_VALUE" not in db.series_read

    def test_legacy_rows_alone_are_unavailable(self) -> None:
        db = _Form4DB()
        # Rows the writer stored before 2026-11-23: all-code and OPEN_MKT_*,
        # with an imputed-$0 price and no validity or coverage evidence.
        for field, value in (
            ("NET_SHARES", 25_000.0), ("NET_VALUE", 4_000_000.0), ("TXN_COUNT", 3.0),
            ("OPEN_MKT_NET_SHARES", 25_000.0), ("OPEN_MKT_NET_VALUE", 750_000.0),
            ("OPEN_MKT_TXN_COUNT", 2.0),
        ):
            db.add(f"FORM4:AAPL:{field}", T3, value)

        result = db.score()

        assert result.data_available is False
        assert result.score == 0
        assert any("P/S series unavailable" in s for s in result.signals)
        assert not any("OPEN_MKT" in sid for sid in db.series_read)

    def test_other_source_rows_on_the_same_series_and_date_are_ignored(self) -> None:
        db = _Form4DB()
        db.cover(FULL_FIRST, LAST, {T3: {"ps_shares": 500.0, "ps_value": 100_000.0, "txns": 1.0}})
        # Simultaneous rows from another source for the same series and date,
        # pulled later: neither summed in nor allowed to replace EDGAR's rows.
        for field, value in (("PS_NET_VALUE", 9_000_000.0), ("PS_PRICED_COUNT", 0.0)):
            db.add(f"FORM4:AAPL:{field}", T3, value,
                   pulled=_evening(T3) + timedelta(hours=1), source_id=OTHER_SRC)

        result = db.score()

        assert result.data_available is True
        assert result.score == 2
        assert any(s.startswith(f"{LABEL} net buy $100,000") for s in result.signals)

        only_other = _Form4DB()
        only_other.cover(FULL_FIRST, LAST, {T3: {"ps_shares": 500.0, "ps_value": 9_000_000.0, "txns": 1.0}})
        with only_other.engine.begin() as c:
            c.execute(text("UPDATE raw_series SET source_id = :o"), {"o": OTHER_SRC})
        assert only_other.score().data_available is False

    def test_recovery_pull_never_rewrites_an_earlier_as_of(self) -> None:
        db = _Form4DB()
        recovered = AS_OF + timedelta(days=2)
        # Routine pulls covered the last 10 days. The 80 days before were
        # covered only by a recovery pull made two days after as_of.
        db.cover(AS_OF - timedelta(days=10), LAST,
                 {T3: {"ps_shares": 900.0, "ps_value": 750_000.0, "txns": 1.0}})
        db.cover(FULL_FIRST, AS_OF - timedelta(days=11), pulled=_evening(recovered))

        before = db.score()
        assert before.score == 2 and before.trust == 0.5
        assert any("10d of 90d" in s for s in before.signals)

        # At the recovery date, its last closed date is not covered yet.
        assert db.score(as_of=recovered).data_available is False
        db.cover(AS_OF, recovered - timedelta(days=1), pulled=_evening(recovered))
        after = db.score(as_of=recovered)
        assert after.score == 5 and after.trust == 0.85

        assert db.score().signals == before.signals  # the earlier as-of read is unchanged

    def test_only_vintages_pulled_by_the_end_of_as_of_count(self) -> None:
        db = _Form4DB()
        db.cover(FULL_FIRST, LAST, {T3: {"ps_shares": 500.0, "ps_value": 100_000.0, "txns": 1.0}})
        # A full re-pull of T3 after as_of, and a FAILED marker: neither counts.
        db.filing_day(T3, ps_shares=9_000.0, ps_value=900_000.0, txns=2.0,
                      pulled=_evening(AS_OF + timedelta(days=1)))
        db.add("FORM4:AAPL:PS_NET_VALUE", T3, 5_000_000.0, status="FAILED")

        assert db.score().score == 2

    def test_window_sums_verified_days_within_90_days(self) -> None:
        db = _Form4DB()
        db.cover(AS_OF - timedelta(days=120), LAST, {
            T3: {"ps_shares": 1_000.0, "ps_value": 300_000.0, "txns": 1.0},
            AS_OF - timedelta(days=60): {"ps_shares": 1_000.0, "ps_value": 300_000.0, "txns": 1.0},
            AS_OF - timedelta(days=100): {"ps_shares": -90_000.0, "ps_value": -9_000_000.0, "txns": 1.0},
        })

        result = db.score()

        assert result.score == 5
        assert any(s.startswith(f"{LABEL} net buy $600,000") for s in result.signals)

    def test_company_layer_returns_none_because_no_xbrl_writer_exists(self) -> None:
        conn = _RawSeriesConn({})
        assert cs.score_company(conn, "AAPL", as_of_date=AS_OF) is None
        assert conn.calls == []

    def test_score_ticker_excludes_unavailable_layers_from_total_and_lists_them(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        idx = pd.date_range(end=AS_OF, periods=91, freq="D")
        monkeypatch.setattr(cs, "_load_price", lambda *_a, **_k: pd.Series([100.0] * 91, index=idx))
        monkeypatch.setattr(
            cs, "score_setup",
            lambda *_a, **_k: cs.LayerResult("SETUP", 13.6, 17, 0.95, (), True),
        )
        monkeypatch.setattr(cs, "score_company", lambda *_a, **_k: None)
        monkeypatch.setattr(
            cs, "score_smart_money",
            lambda *_a, **_k: cs.LayerResult("SMART_MONEY", 12.0, 15, 0.85, (), True),
        )
        monkeypatch.setattr(
            cs, "score_crowd",
            lambda *_a, **_k: cs.LayerResult("CROWD", 12.0, 15, 0.75, (), True),
        )
        monkeypatch.setattr(
            cs, "score_narrative",
            lambda *_a, **_k: cs.LayerResult("NARRATIVE", 8.0, 10, 0.50, (), True),
        )
        monkeypatch.setattr(
            cs, "score_flow",
            lambda *_a, **_k: cs.LayerResult("FLOW", 12.0, 15, 0.60, (), True),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs.LayerResult("CONFIRM", 8.0, 10, 0.90, (), True),
        )

        report = cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF)

        # All 6 available layers scored exactly 80%; unavailable COMPANY must not dilute total_score.
        assert report.total_score == 80
        assert report.alert_level == "FIRE"
        assert report.unavailable_layers == ("COMPANY",)
        assert all(isinstance(layer, cs.LayerResult) for layer in report.layers)
        assert all(layer.name != "COMPANY" for layer in report.layers)
        assert report.confidence_pct == pytest.approx(round(80 * 6 / 7, 1))

    def test_score_ticker_two_layers_available_caps_alert_at_watch(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        idx = pd.date_range(end=AS_OF, periods=91, freq="D")
        monkeypatch.setattr(cs, "_load_price", lambda *_a, **_k: pd.Series([100.0] * 91, index=idx))
        monkeypatch.setattr(
            cs, "score_setup",
            lambda *_a, **_k: cs._layer_result("SETUP", 17.0, 0.95, (), True),
        )
        monkeypatch.setattr(cs, "score_company", lambda *_a, **_k: None)
        monkeypatch.setattr(
            cs, "score_smart_money",
            lambda *_a, **_k: cs._layer_result("SMART_MONEY", 0.0, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_crowd",
            lambda *_a, **_k: cs._layer_result("CROWD", 0.0, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_narrative",
            lambda *_a, **_k: cs._layer_result("NARRATIVE", 0.0, 0.10, (), False),
        )
        monkeypatch.setattr(
            cs, "score_flow",
            lambda *_a, **_k: cs._layer_result("FLOW", 0.0, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs._layer_result("CONFIRM", 10.0, 0.90, (), True),
        )

        report = cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF)

        # COMPANY (None) is excluded; data_available=False layers remain in the
        # weighted total as 0 at their full-data weight (25.15 / 63.15 = 40
        # PILOT; this test asserted 25.15 / 32.90 = 76 while missing data
        # shrank the denominator), and coverage 2/7 < 0.5 caps alert at WATCH.
        assert report.total_score == 40
        assert report.alert_level == "WATCH"
        assert report.unavailable_layers == ("COMPANY", "SMART_MONEY", "CROWD", "NARRATIVE", "FLOW")
        assert report.confidence_pct == pytest.approx(round(40 * 2 / 7, 1))

        # Low score with thin coverage still stays PASS (floor caps, never promotes).
        monkeypatch.setattr(
            cs, "score_setup",
            lambda *_a, **_k: cs._layer_result("SETUP", 1.0, 0.95, (), True),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs._layer_result("CONFIRM", 1.0, 0.90, (), True),
        )
        low_report = cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF)
        assert low_report.total_score < 25
        assert low_report.alert_level == "PASS"

    def test_score_ticker_sufficient_coverage_preserves_alert_level(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        idx = pd.date_range(end=AS_OF, periods=91, freq="D")
        monkeypatch.setattr(cs, "_load_price", lambda *_a, **_k: pd.Series([100.0] * 91, index=idx))
        monkeypatch.setattr(
            cs, "score_setup",
            lambda *_a, **_k: cs._layer_result("SETUP", 17.0, 0.95, (), True),
        )
        monkeypatch.setattr(cs, "score_company", lambda *_a, **_k: None)
        monkeypatch.setattr(
            cs, "score_smart_money",
            lambda *_a, **_k: cs._layer_result("SMART_MONEY", 15.0, 0.85, (), True),
        )
        monkeypatch.setattr(
            cs, "score_crowd",
            lambda *_a, **_k: cs._layer_result("CROWD", 15.0, 0.75, (), True),
        )
        monkeypatch.setattr(
            cs, "score_narrative",
            lambda *_a, **_k: cs._layer_result("NARRATIVE", 0.0, 0.10, (), False),
        )
        monkeypatch.setattr(
            cs, "score_flow",
            lambda *_a, **_k: cs._layer_result("FLOW", 0.0, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs._layer_result("CONFIRM", 10.0, 0.90, (), True),
        )

        report = cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF)

        # 4/7 layers available (>= 0.5, < 0.8): 49.15 / 63.15 = 78 -> alert stays
        # FIRE (this test asserted 49.15 / 52.40 = 94 while missing data shrank
        # the denominator).
        assert report.total_score == 78
        assert report.alert_level == "FIRE"
        assert report.unavailable_layers == ("COMPANY", "NARRATIVE", "FLOW")
        assert report.confidence_pct == pytest.approx(round(78 * 4 / 7, 1))


# ── Defect 7: SMART_MONEY certainty never promotes the composite total ────
#
# Codex HOLD on #1353 57514cc5: score_ticker divides by sum(max * trust), so a
# neutral SMART_MONEY with lower trust (short window 0.5, unavailable 0.15)
# shrank the denominator and raised the total: 51 / (63.9 + 15 x 0.85) = 67
# SCALE with full coverage, but 71 FIRE short and 77 unavailable. The other
# layers below hold Codex's 51 earned of 63.9 weighted maximum.


def _fix_other_layers(monkeypatch: pytest.MonkeyPatch, as_of: date) -> None:
    idx = pd.date_range(end=as_of, periods=91, freq="D")
    monkeypatch.setattr(cs, "_load_price", lambda *_a, **_k: pd.Series([100.0] * 91, index=idx))
    fixed = {
        "score_setup": cs.LayerResult("SETUP", 17.0, 17, 0.95, (), True),        # 16.15 / 16.15
        "score_company": cs.LayerResult("COMPANY", 15.0, 15, 0.90, (), True),    # 13.50 / 13.50
        "score_crowd": cs.LayerResult("CROWD", 10.0, 15, 0.75, (), True),        #  7.50 / 11.25
        "score_narrative": cs.LayerResult("NARRATIVE", 4.9, 10, 0.50, (), True),  # 2.45 / 5.00
        "score_flow": cs.LayerResult("FLOW", 7.0, 15, 0.60, (), True),           #  4.20 / 9.00
        "score_confirmation": cs.LayerResult("CONFIRM", 8.0, 10, 0.90, (), True),  # 7.20 / 9.00
    }
    for name, layer in fixed.items():
        monkeypatch.setattr(cs, name, lambda *_a, _layer=layer, **_k: _layer)


def _coverage_db(kind: str, as_of: date, ps_value: float = 0.0, ps_shares: float = 0.0) -> _Form4DB:
    db = _Form4DB()
    last = as_of - timedelta(days=1)
    trade = {"ps_shares": ps_shares, "ps_value": ps_value, "txns": 1.0} if ps_value else {}
    days = {as_of - timedelta(days=3): trade} if trade else {}
    if kind == "full":
        db.cover(as_of - timedelta(days=90), last, days)
    elif kind == "short":
        db.cover(as_of - timedelta(days=7), last, days)
    return db


def _smart_money(report: cs.ConvictionReport) -> cs.LayerResult:
    return next(layer for layer in report.layers if layer.name == "SMART_MONEY")


class TestDefect7CompositeNoPromotion:
    def test_neutral_smart_money_full_short_unavailable_leave_the_total_unchanged(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _fix_other_layers(monkeypatch, AS_OF)
        totals = {}
        for kind, trust in (("full", 0.85), ("short", 0.5), ("unavailable", 0.15)):
            report = cs.score_ticker(_coverage_db(kind, AS_OF).engine, "AAPL", as_of_date=AS_OF)
            layer = _smart_money(report)
            assert layer.score == 0
            assert layer.trust == trust
            assert layer.denominator_trust == 0.85
            totals[kind] = (report.total_score, report.alert_level)

        # round(100 x 51 / (63.9 + 15 x 0.85)) = 67 in every case: certainty
        # lost on a neutral layer moves nothing (it was 67 / 71 FIRE / 77 FIRE).
        assert totals == {"full": (67, "SCALE"), "short": (67, "SCALE"), "unavailable": (67, "SCALE")}

    @pytest.mark.parametrize(
        ("ps_value", "ps_shares"),
        [(750_000.0, 9_000.0), (-750_000.0, -9_000.0)],
        ids=["insider-buy", "insider-sell"],
    )
    def test_less_certainty_never_raises_the_total_above_its_evidence(
        self, monkeypatch: pytest.MonkeyPatch, ps_value: float, ps_shares: float,
    ) -> None:
        _fix_other_layers(monkeypatch, AS_OF)
        total = {
            kind: cs.score_ticker(
                _coverage_db(kind, AS_OF, ps_value, ps_shares).engine, "AAPL", as_of_date=AS_OF,
            ).total_score
            for kind in ("full", "short", "unavailable")
        }
        neutral = 67
        if ps_value > 0:
            # Buying: less certainty, less lift; never below neutral.
            assert total["full"] >= total["short"] >= total["unavailable"] == neutral
        else:
            # Selling: less certainty, less penalty; never above neutral.
            assert total["full"] <= total["short"] <= total["unavailable"] == neutral

    def test_conviction_api_reports_the_unpromoted_total_and_the_weights(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from api.auth import require_auth
        from api.routers import signals as signals_module

        today = date.today()  # the route scores as of today
        _fix_other_layers(monkeypatch, today)
        app = FastAPI()
        app.include_router(signals_module.router)
        app.dependency_overrides[require_auth] = lambda: "test-token"
        client = TestClient(app)

        bodies = {}
        for kind in ("full", "short", "unavailable"):
            db = _coverage_db(kind, today)
            monkeypatch.setattr(signals_module, "get_db_engine", lambda _e=db.engine: _e)
            resp = client.get("/api/v1/signals/conviction/aapl")
            assert resp.status_code == 200, resp.text
            bodies[kind] = resp.json()

        assert {k: (b["total_score"], b["alert_level"]) for k, b in bodies.items()} == {
            "full": (67, "SCALE"), "short": (67, "SCALE"), "unavailable": (67, "SCALE"),
        }
        layers = {k: next(x for x in b["layers"] if x["name"] == "SMART_MONEY") for k, b in bodies.items()}
        assert {k: (x["trust"], x["denominator_trust"]) for k, x in layers.items()} == {
            "full": (0.85, 0.85), "short": (0.5, 0.85), "unavailable": (0.15, 0.85),
        }


# ── Defect 8: no layer's missing data promotes the composite total ────────
#
# The Defect 7 hole in every other layer: CROWD (0.75 -> 0.15), NARRATIVE
# (0.50 -> 0.10), FLOW (0.60 -> 0.15 or 0.10) and CONFIRM (0.90 -> 0.10)
# lowered their trust when their data was missing, and score_ticker divided
# by sum(max * trust), so a ticker missing a layer scored above the same
# ticker whose layer read neutral. SETUP's no-price branch also carried a
# 20-point max against 17 everywhere else. Every layer now reports its fixed
# weight (LAYER_MAX_SCORE x LAYER_FULL_TRUST) as its denominator weight.
#
# Evidence held fixed below: SETUP 12 of 17 (VIX 36, HY 5.5, curve -0.25)
# and CONFIRM 10 of 10 (a turn off a 50 low), 11.40 + 9.00 = 20.40 earned of
# a fixed 63.15 maximum (COMPANY is None and excluded), so 32 WATCH.

_D8_TICKER = "AAPL"
_D8_NEUTRAL_TOTAL = 32  # round(100 x 20.40 / 63.15)


def _d8_turning_price(as_of: date) -> pd.Series:
    # 100 -> 50 over 50 days, then 50 -> 74 over 40: CONFIRM 10 (5d +4.2 %,
    # 30d up while 90d down, above the 30-day low by > 5 %), drawdown -26 %
    # (no SETUP drawdown points), no volatility spike (no FLOW points).
    values = [100.0 - i for i in range(51)] + [50.0 + 0.6 * i for i in range(1, 41)]
    return pd.Series(values, index=pd.date_range(end=as_of, periods=len(values), freq="D"))


def _d8_neutral_data(as_of: date) -> dict[str, Any]:
    """Every layer has data and reads neutral (0 points) except SETUP and CONFIRM."""
    t = _D8_TICKER
    return {
        "latest": {
            "vix_spot": 36.0, "hy_oas_spread": 5.5, "yld_curve_2s10s": -0.25,
            "gdelt_article_count": 500.0,
        },
        "raw_series": {
            f"SHORT:{t}:ratio": [0.0] * 20,        # CROWD data, 0 points
            f"FTD:{t}:qty": [100.0] * 40,          # CROWD data, 0 points
            f"NEWS:{t}:daily_count": [1.0] * 40,   # NARRATIVE data, 0 points
        },
        "raw_latest": {"CBOE:totalpc": 0.9, "CBOE:equitypc": 0.6},  # FLOW data, 0 points
        # SMART_MONEY: a full verified 90-day window with no P/S rows, 0 points.
        "form4": cs.Form4PSWindow(start=as_of - timedelta(days=89), end=as_of),
        "price": _d8_turning_price(as_of),
    }


def _d8_drop(data: dict[str, Any], layer: str) -> dict[str, Any]:
    """The same data with one layer's ticker inputs missing."""
    t = _D8_TICKER
    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in data.items()}
    if layer == "CROWD":
        out["raw_series"].pop(f"SHORT:{t}:ratio")
        out["raw_series"].pop(f"FTD:{t}:qty")
    elif layer == "NARRATIVE":
        out["raw_series"].pop(f"NEWS:{t}:daily_count")
    elif layer == "FLOW":
        out["raw_latest"].pop("CBOE:equitypc")
    elif layer == "SMART_MONEY":
        out["form4"] = cs.Form4PSWindow()
    else:
        raise AssertionError(layer)
    return out


def _d8_install(monkeypatch: pytest.MonkeyPatch, data: dict[str, Any]) -> None:
    monkeypatch.setattr(cs, "_load_price", lambda *_a, **_k: data["price"])
    monkeypatch.setattr(cs, "_load_latest", lambda _c, name, **_k: data["latest"].get(name))
    monkeypatch.setattr(
        cs, "_load_raw_series",
        lambda _c, sid, *_a, **_k: pd.Series(data["raw_series"].get(sid, []), dtype=float),
    )
    monkeypatch.setattr(cs, "_load_raw_latest", lambda _c, pat, *_a, **_k: data["raw_latest"].get(pat))
    monkeypatch.setattr(cs, "_form4_ps_window", lambda *_a, **_k: data["form4"])


def _d8_score(monkeypatch: pytest.MonkeyPatch, data: dict[str, Any]) -> cs.ConvictionReport:
    _d8_install(monkeypatch, data)
    return cs.score_ticker(_DummyEngine(_RawSeriesConn({})), _D8_TICKER, as_of_date=AS_OF)


def _d8_layer(report: cs.ConvictionReport, name: str) -> cs.LayerResult:
    return next(layer for layer in report.layers if layer.name == name)


class TestDefect8NoLayerPromotesByMissingData:
    def test_neutral_baseline_is_32_watch_with_every_weight_fixed(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        report = _d8_score(monkeypatch, _d8_neutral_data(AS_OF))

        assert (report.total_score, report.alert_level) == (_D8_NEUTRAL_TOTAL, "WATCH")
        assert report.unavailable_layers == ("COMPANY",)
        assert {layer.name: layer.score for layer in report.layers} == {
            "SETUP": 12, "SMART_MONEY": 0, "CROWD": 0, "NARRATIVE": 0, "FLOW": 0, "CONFIRM": 10,
        }
        assert sum(layer.max_score * layer.denominator_trust for layer in report.layers) == pytest.approx(63.15)

    @pytest.mark.parametrize(
        "missing",
        # Before the fix: CROWD 38, NARRATIVE 34, FLOW 36 (SMART_MONEY was 32
        # since Defect 7).
        ["CROWD", "NARRATIVE", "FLOW", "SMART_MONEY"],
    )
    def test_a_missing_layer_scores_the_same_as_that_layer_read_neutral(
        self, monkeypatch: pytest.MonkeyPatch, missing: str,
    ) -> None:
        report = _d8_score(monkeypatch, _d8_drop(_d8_neutral_data(AS_OF), missing))

        layer = _d8_layer(report, missing)
        assert layer.data_available is False
        assert layer.trust < cs.LAYER_FULL_TRUST[missing]
        assert layer.denominator_trust == cs.LAYER_FULL_TRUST[missing]
        assert report.unavailable_layers == ("COMPANY", missing)
        assert (report.total_score, report.alert_level) == (_D8_NEUTRAL_TOTAL, "WATCH")

    def test_every_ticker_layer_missing_still_scores_32_not_47(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        data = _d8_neutral_data(AS_OF)
        for layer in ("CROWD", "NARRATIVE", "FLOW", "SMART_MONEY"):
            data = _d8_drop(data, layer)

        report = _d8_score(monkeypatch, data)

        # Was round(100 x 20.40 / 43.40) = 47 (SMART_MONEY already held its
        # weight since Defect 7); the evidence is the same 20.40 of 63.15.
        assert report.total_score == _D8_NEUTRAL_TOTAL
        assert report.alert_level == "WATCH"
        assert report.unavailable_layers == ("COMPANY", "SMART_MONEY", "CROWD", "NARRATIVE", "FLOW")

    def test_evidence_raises_the_total_and_missing_data_never_does(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        neutral = _d8_neutral_data(AS_OF)
        shorted = {**neutral, "raw_series": {**neutral["raw_series"], f"SHORT:{_D8_TICKER}:ratio": [0.6] * 20}}

        totals = {
            "missing": _d8_score(monkeypatch, _d8_drop(neutral, "CROWD")).total_score,
            "neutral": _d8_score(monkeypatch, neutral).total_score,
            # Heavily shorted: CROWD +5 at trust 0.75 -> (20.40 + 3.75) / 63.15.
            "evidence": _d8_score(monkeypatch, shorted).total_score,
        }

        assert totals == {"missing": 32, "neutral": 32, "evidence": 38}

    def test_every_layer_branch_reports_its_fixed_weight(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn = _RawSeriesConn({})
        short_price = _d8_turning_price(AS_OF).tail(10)
        empty_price = pd.Series(dtype=float)
        results: list[cs.LayerResult] = []
        for data in (_d8_neutral_data(AS_OF), *(
            _d8_drop(_d8_neutral_data(AS_OF), name)
            for name in ("CROWD", "NARRATIVE", "FLOW", "SMART_MONEY")
        )):
            _d8_install(monkeypatch, data)
            price = data["price"]
            results += [
                cs.score_setup(conn, _D8_TICKER, price, as_of_date=AS_OF),
                cs.score_smart_money(conn, _D8_TICKER, as_of_date=AS_OF),
                cs.score_crowd(conn, _D8_TICKER, as_of_date=AS_OF),
                cs.score_narrative(conn, _D8_TICKER, as_of_date=AS_OF),
                cs.score_flow(conn, _D8_TICKER, price, as_of_date=AS_OF),
                cs.score_confirmation(price),
            ]
        # Too little or no price history: SETUP, FLOW and CONFIRM still carry
        # their full weights (SETUP's no-price max was 20 against 17).
        results += [
            cs.score_setup(conn, _D8_TICKER, empty_price, as_of_date=AS_OF),
            cs.score_flow(conn, _D8_TICKER, short_price, as_of_date=AS_OF),
            cs.score_confirmation(short_price),
        ]

        assert {r.name for r in results} == set(cs.LAYER_FULL_TRUST) - {"COMPANY"}
        assert any(not r.data_available for r in results if r.name == "SETUP")
        for r in results:
            assert r.max_score == cs.LAYER_MAX_SCORE[r.name], r
            assert r.baseline_trust == cs.LAYER_FULL_TRUST[r.name], r
            assert r.denominator_trust == cs.LAYER_FULL_TRUST[r.name], r
            assert 0 <= r.trust <= r.denominator_trust, r

    def test_conviction_api_reports_the_fixed_weight_of_every_layer(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from api.auth import require_auth
        from api.routers import signals as signals_module

        today = date.today()  # the route scores as of today
        data = _d8_neutral_data(today)
        for layer in ("CROWD", "NARRATIVE", "FLOW"):
            data = _d8_drop(data, layer)
        _d8_install(monkeypatch, data)
        monkeypatch.setattr(signals_module, "get_db_engine", lambda: _DummyEngine(_RawSeriesConn({})))
        app = FastAPI()
        app.include_router(signals_module.router)
        app.dependency_overrides[require_auth] = lambda: "test-token"

        resp = TestClient(app).get("/api/v1/signals/conviction/aapl")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert (body["total_score"], body["alert_level"]) == (_D8_NEUTRAL_TOTAL, "WATCH")
        assert {x["name"]: (x["trust"], x["denominator_trust"]) for x in body["layers"]} == {
            "SETUP": (0.95, 0.95),
            "SMART_MONEY": (0.85, 0.85),
            "CROWD": (0.15, 0.75),
            "NARRATIVE": (0.1, 0.5),
            "FLOW": (0.15, 0.6),
            "CONFIRM": (0.9, 0.9),
        }


# ── Defect 9: lost evidence never promotes the alert; CONVICTION is dormant ─
#
# Codex HOLD on #1394 7da0c760. (1) With a fixed denominator a missing layer
# scores as neutral, so losing an adverse reading still lifts the total:
# other support 16.15 of 63.15, verified SMART_MONEY -2 at 0.85 scored 23
# PASS, the same sell on a short window 24 PASS, and SMART_MONEY unavailable
# 26 WATCH. The alert now comes from score_low (every unknown at its worst
# attainable value) and total_score stays the neutral point estimate.
# (2) The current scorers top out at 50.95 of 63.15 = 81, so CONVICTION (85)
# is unreachable; reports say so instead of implying it is live.

_LEVEL_ORDER = ("PASS", "WATCH", "PILOT", "SCALE", "FIRE", "CONVICTION")


def _d9_window(as_of: date, *, days: int = 0, value: float = 0.0) -> cs.Form4PSWindow:
    if days == 0:
        return cs.Form4PSWindow()  # unavailable
    trade = (
        {"txns": 1.0, "shares_valid": 1.0, "priced": 1.0,
         "net_shares": 9_000.0 if value > 0 else -9_000.0, "net_value": value}
        if value else {}
    )
    return cs.Form4PSWindow(start=as_of - timedelta(days=days - 1), end=as_of, **trade)


def _d9_data(as_of: date, form4: cs.Form4PSWindow) -> dict[str, Any]:
    """Codex's case: SETUP 17 of 17 (16.15), every other layer measured at 0."""
    data = _d8_neutral_data(as_of)
    # Steady 1.5 %/day decline: drawdown -74 % (SETUP 5 of 5), no turn
    # (CONFIRM 0) and constant returns (no FLOW volatility points).
    values = [100.0 * 0.985 ** i for i in range(91)]
    data["price"] = pd.Series(values, index=pd.date_range(end=as_of, periods=91, freq="D"))
    data["form4"] = form4
    return data


_D9_CASES = {
    # case: (form4 window kwargs, total_score, score_low, score_high, alert)
    "adverse-full": ({"days": 90, "value": -750_000.0}, 23, 23, 23, "PASS"),
    "adverse-short": ({"days": 7, "value": -750_000.0}, 24, 23, 27, "PASS"),
    "neutral-full": ({"days": 90}, 26, 26, 26, "WATCH"),
    "neutral-short": ({"days": 7}, 26, 24, 28, "PASS"),
    "unavailable": ({}, 26, 23, 32, "PASS"),  # was 26 WATCH at 7da0c760
    "buy-full": ({"days": 90, "value": 750_000.0}, 32, 32, 32, "WATCH"),
}


def _d9_max_data(as_of: date) -> dict[str, Any]:
    """Inputs that drive every current layer scorer to its attainable maximum."""
    t = _D8_TICKER
    # Crash 100 -> 20, flat, then a 5-day rebound to 27: drawdown -73 %
    # (SETUP 5), a volatility spike (FLOW 5) and a confirmed turn (CONFIRM 10).
    values = (
        [100.0 - 80.0 * i / 60 for i in range(61)]
        + [20.0] * 25
        + [21.0, 22.5, 24.0, 25.5, 27.0]
    )
    return {
        "latest": {
            "vix_spot": 36.0, "hy_oas_spread": 5.5, "yld_curve_2s10s": -0.25,
            f"wiki_{t.lower()}": 1.0, "gdelt_article_count": 1_500.0,
        },
        "raw_series": {
            f"SHORT:{t}:ratio": [0.6] * 20,
            f"FTD:{t}:qty": [1_000.0] * 80 + [50_000.0] * 10,
            f"NEWS:{t}:daily_count": [1.0] * 33 + [10.0] * 7,
        },
        "raw_latest": {"CBOE:totalpc": 1.3, "CBOE:equitypc": 0.9, f"WHALE:{t}:%": 1.0},
        "form4": _d9_window(as_of, days=90, value=750_000.0),
        "price": pd.Series(values, index=pd.date_range(end=as_of, periods=len(values), freq="D")),
    }


class TestDefect9LostEvidenceAndDormantConviction:
    @pytest.mark.parametrize("case", list(_D9_CASES))
    def test_codex_case_alert_comes_from_the_lower_bound(
        self, monkeypatch: pytest.MonkeyPatch, case: str,
    ) -> None:
        window, total, low, high, alert = _D9_CASES[case]

        report = _d8_score(monkeypatch, _d9_data(AS_OF, _d9_window(AS_OF, **window)))

        assert (report.total_score, report.score_low, report.score_high, report.alert_level) == (
            total, low, high, alert,
        )

    def test_missing_smart_money_alerts_no_higher_than_its_adverse_reading(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        reports = {
            case: _d8_score(monkeypatch, _d9_data(AS_OF, _d9_window(AS_OF, **_D9_CASES[case][0])))
            for case in _D9_CASES
        }
        missing = reports.pop("unavailable")
        # The unknown sits at the -2 sell cap over its full weight: exactly the
        # verified adverse reading, and no higher than any measured reading.
        assert missing.score_low == reports["adverse-full"].score_low
        for case, measured in reports.items():
            assert _LEVEL_ORDER.index(missing.alert_level) <= _LEVEL_ORDER.index(measured.alert_level), case
        # Less certainty never raises the bound for the same reading.
        assert reports["adverse-short"].score_low <= reports["adverse-full"].score_low
        assert reports["neutral-short"].score_low <= reports["neutral-full"].score_low

    @pytest.mark.parametrize("layer", ["SMART_MONEY", "CROWD", "NARRATIVE", "FLOW"])
    def test_losing_a_layer_never_promotes_the_alert_across_the_ladder(
        self, monkeypatch: pytest.MonkeyPatch, layer: str,
    ) -> None:
        idx = pd.date_range(end=AS_OF, periods=91, freq="D")
        monkeypatch.setattr(cs, "_load_price", lambda *_a, **_k: pd.Series([100.0] * 91, index=idx))
        monkeypatch.setattr(cs, "score_company", lambda *_a, **_k: None)
        scorers = {
            "SETUP": "score_setup", "SMART_MONEY": "score_smart_money", "CROWD": "score_crowd",
            "NARRATIVE": "score_narrative", "FLOW": "score_flow", "CONFIRM": "score_confirmation",
        }
        full = cs.LAYER_FULL_TRUST[layer]
        lo, hi = cs.LAYER_MIN_SCORE[layer], cs.LAYER_ATTAINABLE_SCORE[layer]
        readings = [(s, t) for s in sorted({lo, 0.0, (lo + hi) / 2, hi}) for t in (0.5 * full, full)]

        def alert(other: float, target: cs.LayerResult) -> str:
            # "other" support spread over SETUP and CONFIRM at full trust.
            fixed = {
                "SETUP": cs._layer_result("SETUP", min(other, 17.0), 0.95, (), True),
                "CONFIRM": cs._layer_result("CONFIRM", max(0.0, other - 17.0), 0.90, (), True),
            }
            for name, fn in scorers.items():
                result = target if name == layer else fixed.get(name) or cs._layer_result(
                    name, 0.0, cs.LAYER_FULL_TRUST[name], (), True,
                )
                monkeypatch.setattr(cs, fn, lambda *_a, _r=result, **_k: _r)
            return cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF).alert_level

        promoted = []
        for step in range(55):
            other = step * 0.5  # 0 .. 27 points of other support
            missing = alert(other, cs._layer_result(layer, 0.0, 0.1, (), False))
            for score, trust in readings:
                measured = alert(other, cs._layer_result(layer, score, trust, (), True))
                if _LEVEL_ORDER.index(missing) > _LEVEL_ORDER.index(measured):
                    promoted.append((other, score, trust, missing, measured))
        assert promoted == []

    def test_conviction_is_dormant_for_the_current_scorers(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        report = _d8_score(monkeypatch, _d9_max_data(AS_OF))

        # Every real layer scorer at its attainable maximum ...
        assert {layer.name: layer.score for layer in report.layers} == {
            name: cs.LAYER_ATTAINABLE_SCORE[name] for name in cs.LAYER_FULL_TRUST if name != "COMPANY"
        }
        # ... earns 50.95 of 63.15 = 81: FIRE is the highest reachable alert.
        assert (report.total_score, report.score_low, report.score_high) == (81, 81, 81)
        assert report.alert_level == "FIRE"
        assert report.max_attainable_score == 81
        assert report.dormant_alert_levels == ("CONVICTION",)
        assert dict(cs.ALERT_THRESHOLDS)["CONVICTION"] > report.max_attainable_score
        assert all(floor <= 81 for name, floor in cs.ALERT_THRESHOLDS if name != "CONVICTION")

    def test_bound_tables_agree_with_the_scorers(self) -> None:
        assert set(cs.LAYER_MIN_SCORE) == set(cs.LAYER_ATTAINABLE_SCORE) == set(cs.LAYER_MAX_SCORE)
        for name in cs.LAYER_MAX_SCORE:
            assert cs.LAYER_MIN_SCORE[name] <= 0 <= cs.LAYER_ATTAINABLE_SCORE[name] <= cs.LAYER_MAX_SCORE[name]
        assert cs.LAYER_MIN_SCORE["SMART_MONEY"] == cs._FORM4_SELL_CAP
        assert cs.LAYER_ATTAINABLE_SCORE["SETUP"] == cs.SETUP_MAX_SCORE

    def test_conviction_api_reports_bounds_basis_and_dormancy(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from api.auth import require_auth
        from api.routers import signals as signals_module

        today = date.today()  # the route scores as of today
        app = FastAPI()
        app.include_router(signals_module.router)
        app.dependency_overrides[require_auth] = lambda: "test-token"
        monkeypatch.setattr(signals_module, "get_db_engine", lambda: _DummyEngine(_RawSeriesConn({})))
        client = TestClient(app)

        bodies = {}
        for case in ("adverse-full", "unavailable", "neutral-full"):
            _d8_install(monkeypatch, _d9_data(today, _d9_window(today, **_D9_CASES[case][0])))
            resp = client.get("/api/v1/signals/conviction/aapl")
            assert resp.status_code == 200, resp.text
            bodies[case] = resp.json()

        assert {
            case: (b["total_score"], b["score_low"], b["score_high"], b["alert_level"])
            for case, b in bodies.items()
        } == {
            "adverse-full": (23, 23, 23, "PASS"),
            "unavailable": (26, 23, 32, "PASS"),
            "neutral-full": (26, 26, 26, "WATCH"),
        }
        for body in bodies.values():
            assert body["alert_basis"] == "score_low"
            assert body["max_attainable_score"] == 81
            assert body["dormant_alert_levels"] == ["CONVICTION"]
