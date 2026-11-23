"""Regression tests for verified defects in signal_convergence_scanner and conviction_scorer.

Covers:
1. Prediction-market double negation in _scan_prediction_market.
2. Neutral streams contributing to the directional numerator in _compute_convergence.
3. FINRA short_volume_ratio treated as directional in _scan_darkpool.
4. Stale/undated 13F filings mixed into the 7-day convergence window in _scan_institutional.
5. INST_FLOW:{ticker}:% passed to exact-match SQL (=) in conviction_scorer.
6. Dead SEC_FORM4:{ticker}:monthly and XBRL:{ticker}:* series in conviction_scorer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pytest

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
    """In-memory raw_series mock supporting exact '=' and escaped LIKE queries."""

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
        if "series_id LIKE" in sql:
            pattern = str(bound.get("p", ""))
            for sid, series_rows in self._rows_by_series.items():
                if _sql_like_match(sid, pattern, escape="\\"):
                    matched.extend(series_rows)
        elif "series_id = :s" in sql:
            sid = str(bound.get("s", ""))
            matched.extend(self._rows_by_series.get(sid, []))

        filtered = [
            (obs_d, val)
            for obs_d, val in matched
            if (cutoff is None or obs_d >= cutoff) and (as_of is None or obs_d <= as_of)
        ]
        filtered.sort(key=lambda r: r[0])
        return _FakeResult(filtered)


def _sql_like_match(text_val: str, pattern: str, escape: str = "\\") -> bool:
    """Minimal SQL LIKE matcher with ESCAPE support for unit tests."""
    import re

    regex_parts: list[str] = ["^"]
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == escape and i + 1 < len(pattern):
            regex_parts.append(re.escape(pattern[i + 1]))
            i += 2
        elif ch == "%":
            regex_parts.append(".*")
            i += 1
        elif ch == "_":
            regex_parts.append(".")
            i += 1
        else:
            regex_parts.append(re.escape(ch))
            i += 1
    regex_parts.append("$")
    return re.match("".join(regex_parts), text_val) is not None


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


# ── Defect 5: INST_FLOW LIKE with bound escaped pattern ───────────────────


class TestDefect5InstFlowLikeQuery:
    def test_inst_flow_uses_bound_like_and_escapes_ticker_wildcards(self) -> None:
        conn = _RawSeriesConn({
            "INST_FLOW:A_B%C:net": [(T3, 250_000.0)],
            # Wildcard-injection trap: must NOT match A_B%C when _ and % are escaped.
            "INST_FLOW:AXBZZZC:net": [(T3, -999_999.0)],
        })

        result = cs.score_smart_money(conn, "A_B%C", as_of_date=AS_OF)

        like_calls = [c for c in conn.calls if "LIKE" in c.sql]
        assert len(like_calls) == 1
        call = like_calls[0]
        assert "ESCAPE" in call.sql
        assert "A_B%C" not in call.sql  # never interpolated into SQL
        assert "obs_date <= :as_of" in call.sql
        assert "pull_timestamp::date <= :as_of" in call.sql
        assert call.params["p"] == r"INST_FLOW:A\_B\%C:%"
        assert call.params["as_of"] == AS_OF
        assert result.data_available is True
        assert result.score == 5
        assert "Institutional net inflow" in result.signals

    def test_inst_flow_does_not_match_other_tickers_via_unescaped_underscore(self) -> None:
        conn = _RawSeriesConn({
            "INST_FLOW:AXB:net": [(T3, 500_000.0)],
        })

        result = cs.score_smart_money(conn, "A_B", as_of_date=AS_OF)

        assert result.data_available is False
        assert result.score == 0


# ── Defect 6: SMART_MONEY Form 4 signed series & COMPANY unavailable ──────


class TestDefect6Form4AndCompanyUnavailable:
    def test_smart_money_reads_signed_open_mkt_net_value_buy_and_sell(self) -> None:
        buy_conn = _RawSeriesConn({
            "FORM4:AAPL:OPEN_MKT_NET_VALUE": [(T3, 750_000.0)],
        })
        buy_res = cs.score_smart_money(buy_conn, "AAPL", as_of_date=AS_OF)
        assert buy_res.data_available is True
        assert buy_res.score == 5
        assert all(c.params.get("s") != "SEC_FORM4:AAPL:monthly" for c in buy_conn.calls)
        assert any(
            c.params.get("s") == "FORM4:AAPL:OPEN_MKT_NET_VALUE"
            and "obs_date <= :as_of" in c.sql
            and "pull_timestamp::date <= :as_of" in c.sql
            for c in buy_conn.calls
        )

        mod_buy_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:OPEN_MKT_NET_VALUE": [(T3, 100_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert mod_buy_res.data_available is True
        assert mod_buy_res.score == 2

        sell_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:OPEN_MKT_NET_VALUE": [(T3, -750_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert sell_res.data_available is True
        # Open-market insider selling is capped at -2 (routine liquidity / 10b5-1).
        assert sell_res.score == -2

        mod_sell_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:OPEN_MKT_NET_VALUE": [(T3, -100_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert mod_sell_res.data_available is True
        assert mod_sell_res.score == -2

    def test_smart_money_falls_back_to_form4_net_shares(self) -> None:
        buy_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:NET_SHARES": [(T3, 25_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert buy_res.data_available is True
        assert buy_res.score == 5

        mod_buy_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:NET_SHARES": [(T3, 1_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert mod_buy_res.data_available is True
        assert mod_buy_res.score == 2

        sell_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:NET_SHARES": [(T3, -25_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert sell_res.data_available is True
        assert sell_res.score == -2

        mod_sell_res = cs.score_smart_money(
            _RawSeriesConn({"FORM4:AAPL:NET_SHARES": [(T3, -1_000.0)]}),
            "AAPL",
            as_of_date=AS_OF,
        )
        assert mod_sell_res.data_available is True
        assert mod_sell_res.score == -2

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
