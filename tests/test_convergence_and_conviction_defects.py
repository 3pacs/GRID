"""Regression tests for verified defects in signal_convergence_scanner and conviction_scorer.

Covers:
1. Prediction-market double negation in _scan_prediction_market.
2. Neutral streams contributing to the directional numerator in _compute_convergence.
3. FINRA short_volume_ratio treated as directional in _scan_darkpool.
4. Stale/undated 13F filings mixed into the 7-day convergence window in _scan_institutional.
5. SMART_MONEY pooled whichever INST_FLOW row sorted last; it now reads no
   institutional series (no INST_FLOW writer; 13F is non-directional).
6. SMART_MONEY Form 4: only the open-market (P/S) series from SEC_EDGAR, never
   all-code NET_SHARES (awards/exercises); dead XBRL:{ticker}:* series.
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
)

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


# ── Defects 5/6: SMART_MONEY reads only open-market Form 4 from SEC_EDGAR ──
#
# Real SQL against an in-memory SQLite raw_series + source_catalog shaped like
# production (as in tests/test_store_observations.py), so every read goes
# through store.observations the way it does on griddb. The writer
# (ingestion/edgar.py) records six series per filing day: all-code NET_SHARES,
# NET_VALUE, TXN_COUNT, and the open-market (P/S codes) OPEN_MKT_* trio.

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

EDGAR_SRC = 1
OTHER_SRC = 2


class _Form4DB:
    def __init__(self) -> None:
        self.engine = create_engine("sqlite://")
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
                "pull_timestamp": pulled or datetime.combine(obs_date, datetime.min.time()) + timedelta(hours=22),
                "value": value, "raw_payload": "{}", "pull_status": status,
            }])

    def filing_day(
        self, obs_date: date, *, net_shares: float, net_value: float, txns: float,
        open_shares: float, open_value: float, open_txns: float, ticker: str = "AAPL",
    ) -> None:
        """One filing day as ingestion/edgar.py writes it (all six series)."""
        for field, value in (
            ("NET_SHARES", net_shares), ("NET_VALUE", net_value), ("TXN_COUNT", txns),
            ("OPEN_MKT_NET_SHARES", open_shares), ("OPEN_MKT_NET_VALUE", open_value),
            ("OPEN_MKT_TXN_COUNT", open_txns),
        ):
            self.add(f"FORM4:{ticker}:{field}", obs_date, value)

    def score(self, ticker: str = "AAPL", as_of: date = AS_OF) -> cs.LayerResult:
        self.series_read.clear()
        self.sql.clear()
        with self.engine.connect() as c:
            return cs.score_smart_money(c, ticker, as_of_date=as_of)


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
        ("open_value", "expected_score", "signal"),
        [
            (750_000.0, 5, "Insider open-market net buy $750,000"),
            (100_000.0, 2, "Insider open-market net buy $100,000"),
            # Open-market insider selling is capped at -2 (routine liquidity / 10b5-1).
            (-750_000.0, -2, "Insider open-market net sell $750,000"),
            (-100_000.0, -2, "Insider open-market net sell $100,000"),
            (0.0, 0, "Insider open-market buys and sells offset (net $0)"),
        ],
    )
    def test_open_market_net_value_buy_sell_and_exact_zero(
        self, open_value: float, expected_score: int, signal: str,
    ) -> None:
        db = _Form4DB()
        db.filing_day(
            T3, net_shares=1.0, net_value=open_value, txns=2.0,
            open_shares=1.0, open_value=open_value, open_txns=2.0,
        )

        result = db.score()

        assert result.data_available is True
        assert result.score == expected_score
        assert signal in result.signals
        # Exact zero is a measured offset, never labelled as an outflow.
        assert not (open_value == 0 and any("net sell" in s for s in result.signals))

    def test_award_only_issuer_is_not_scored_as_buying(self) -> None:
        db = _Form4DB()
        # Grants and option exercises (codes A/M): all-code NET_SHARES and
        # NET_VALUE are large and positive, the open-market series are zero.
        db.filing_day(
            T3, net_shares=250_000.0, net_value=40_000_000.0, txns=4.0,
            open_shares=0.0, open_value=0.0, open_txns=0.0,
        )

        result = db.score()

        assert result.score == 0
        assert result.data_available is True
        assert "No open-market insider transactions in 90d (awards/exercises only)" in result.signals
        assert "FORM4:AAPL:NET_SHARES" not in db.series_read
        assert "FORM4:AAPL:NET_VALUE" not in db.series_read

    def test_legacy_all_code_series_alone_is_unavailable(self) -> None:
        db = _Form4DB()
        # Rows written before the open-market split: only the all-code trio.
        db.add("FORM4:AAPL:NET_SHARES", T3, 25_000.0)
        db.add("FORM4:AAPL:NET_VALUE", T3, 4_000_000.0)
        db.add("FORM4:AAPL:TXN_COUNT", T3, 3.0)

        result = db.score()

        assert result.data_available is False
        assert result.score == 0
        assert any("open-market Form 4 series unavailable" in s for s in result.signals)

    def test_other_source_rows_on_the_same_series_and_date_are_ignored(self) -> None:
        db = _Form4DB()
        db.filing_day(
            T3, net_shares=500.0, net_value=100_000.0, txns=1.0,
            open_shares=500.0, open_value=100_000.0, open_txns=1.0,
        )
        # Simultaneous rows from another source for the same series and date,
        # pulled later: neither summed in nor allowed to replace EDGAR's row.
        db.add("FORM4:AAPL:OPEN_MKT_NET_VALUE", T3, 9_000_000.0,
               pulled=_utc(T3) + timedelta(hours=11), source_id=OTHER_SRC)
        db.add("FORM4:AAPL:OPEN_MKT_TXN_COUNT", T3, 0.0,
               pulled=_utc(T3) + timedelta(hours=11), source_id=OTHER_SRC)

        result = db.score()

        assert result.data_available is True
        assert result.score == 2
        assert "Insider open-market net buy $100,000" in result.signals

        only_other = _Form4DB()
        only_other.add("FORM4:AAPL:OPEN_MKT_NET_VALUE", T3, 9_000_000.0, source_id=OTHER_SRC)
        assert only_other.score().data_available is False

    def test_only_rows_pulled_by_the_end_of_as_of_count(self) -> None:
        db = _Form4DB()
        db.filing_day(
            T3, net_shares=500.0, net_value=100_000.0, txns=1.0,
            open_shares=500.0, open_value=100_000.0, open_txns=1.0,
        )
        # A revision of the same date pulled the day after as_of, a FAILED
        # marker, and a later filing day pulled after as_of: none counts.
        db.add("FORM4:AAPL:OPEN_MKT_NET_VALUE", T3, 900_000.0,
               pulled=datetime.combine(AS_OF + timedelta(days=1), datetime.min.time()))
        db.add("FORM4:AAPL:OPEN_MKT_NET_VALUE", AS_OF, 5_000_000.0, status="FAILED")
        db.add("FORM4:AAPL:OPEN_MKT_NET_VALUE", AS_OF, 5_000_000.0,
               pulled=datetime.combine(AS_OF + timedelta(days=2), datetime.min.time()))

        assert db.score().score == 2

        # A row pulled late on the as-of day itself is known by its end.
        db.add("FORM4:AAPL:OPEN_MKT_NET_VALUE", AS_OF - timedelta(days=1), 450_000.0,
               pulled=datetime.combine(AS_OF, datetime.min.time()) + timedelta(hours=23))
        assert db.score().score == 5

    def test_window_sums_filing_days_within_90_days(self) -> None:
        db = _Form4DB()
        for d in (T3, AS_OF - timedelta(days=60)):
            db.filing_day(
                d, net_shares=1_000.0, net_value=300_000.0, txns=1.0,
                open_shares=1_000.0, open_value=300_000.0, open_txns=1.0,
            )
        db.filing_day(
            AS_OF - timedelta(days=120), net_shares=-90_000.0, net_value=-9_000_000.0, txns=1.0,
            open_shares=-90_000.0, open_value=-9_000_000.0, open_txns=1.0,
        )

        result = db.score()

        assert result.score == 5
        assert "Insider open-market net buy $600,000" in result.signals

    @pytest.mark.parametrize(
        ("open_shares", "open_txns", "expected_score"),
        [(25_000.0, 2.0, 5), (1_000.0, 1.0, 2), (-25_000.0, 1.0, -2), (-1_000.0, 1.0, -2), (0.0, 2.0, 0)],
    )
    def test_open_market_shares_fallback_when_value_series_absent(
        self, open_shares: float, open_txns: float, expected_score: int,
    ) -> None:
        db = _Form4DB()
        db.add("FORM4:AAPL:OPEN_MKT_NET_SHARES", T3, open_shares)
        db.add("FORM4:AAPL:OPEN_MKT_TXN_COUNT", T3, open_txns)
        # Legacy all-code shares disagree in sign: never read.
        db.add("FORM4:AAPL:NET_SHARES", T3, 999_999.0)

        result = db.score()

        assert result.data_available is True
        assert result.score == expected_score
        assert "FORM4:AAPL:NET_SHARES" not in db.series_read

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
            lambda *_a, **_k: cs.LayerResult("SETUP", 17.0, 17, 0.95, (), True),
        )
        monkeypatch.setattr(cs, "score_company", lambda *_a, **_k: None)
        monkeypatch.setattr(
            cs, "score_smart_money",
            lambda *_a, **_k: cs.LayerResult("SMART_MONEY", 0.0, 15, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_crowd",
            lambda *_a, **_k: cs.LayerResult("CROWD", 0.0, 15, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_narrative",
            lambda *_a, **_k: cs.LayerResult("NARRATIVE", 0.0, 10, 0.10, (), False),
        )
        monkeypatch.setattr(
            cs, "score_flow",
            lambda *_a, **_k: cs.LayerResult("FLOW", 0.0, 15, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs.LayerResult("CONFIRM", 10.0, 10, 0.90, (), True),
        )

        report = cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF)

        # COMPANY (None) is excluded; data_available=False layers remain in the
        # weighted total as 0 (25.15 / 32.90 = 76), and coverage 2/7 < 0.5 caps alert at WATCH.
        assert report.total_score == 76
        assert report.alert_level == "WATCH"
        assert report.unavailable_layers == ("COMPANY", "SMART_MONEY", "CROWD", "NARRATIVE", "FLOW")
        assert report.confidence_pct == pytest.approx(round(76 * 2 / 7, 1))

        # Low score with thin coverage still stays PASS (floor caps, never promotes).
        monkeypatch.setattr(
            cs, "score_setup",
            lambda *_a, **_k: cs.LayerResult("SETUP", 1.0, 17, 0.95, (), True),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs.LayerResult("CONFIRM", 1.0, 10, 0.90, (), True),
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
            lambda *_a, **_k: cs.LayerResult("SETUP", 17.0, 17, 0.95, (), True),
        )
        monkeypatch.setattr(cs, "score_company", lambda *_a, **_k: None)
        monkeypatch.setattr(
            cs, "score_smart_money",
            lambda *_a, **_k: cs.LayerResult("SMART_MONEY", 15.0, 15, 0.85, (), True),
        )
        monkeypatch.setattr(
            cs, "score_crowd",
            lambda *_a, **_k: cs.LayerResult("CROWD", 15.0, 15, 0.75, (), True),
        )
        monkeypatch.setattr(
            cs, "score_narrative",
            lambda *_a, **_k: cs.LayerResult("NARRATIVE", 0.0, 10, 0.10, (), False),
        )
        monkeypatch.setattr(
            cs, "score_flow",
            lambda *_a, **_k: cs.LayerResult("FLOW", 0.0, 15, 0.15, (), False),
        )
        monkeypatch.setattr(
            cs, "score_confirmation",
            lambda *_a, **_k: cs.LayerResult("CONFIRM", 10.0, 10, 0.90, (), True),
        )

        report = cs.score_ticker(_DummyEngine(_RawSeriesConn({})), "AAPL", as_of_date=AS_OF)

        # 4/7 layers available (>= 0.5, < 0.8): 49.15 / 52.40 = 94 -> alert stays FIRE.
        assert report.total_score == 94
        assert report.alert_level == "FIRE"
        assert report.unavailable_layers == ("COMPANY", "NARRATIVE", "FLOW")
        assert report.confidence_pct == pytest.approx(round(94 * 4 / 7, 1))
