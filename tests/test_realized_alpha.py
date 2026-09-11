"""Tests for ``alpha_research.realized_alpha`` — the GRID-4 §8.1 truth gate.

No live database: every engine is a ``MagicMock`` whose ``conn.execute``
dispatches on the SQL text, and ``PITStore.get_feature_matrix`` is patched
to return a synthetic SPY matrix.

Covers:
  * ``compute_trade_alpha`` on synthetic paths — long win, long loss,
    short win, cost drag, SPY outperforming, benchmark gaps
  * window bucketing (trading-day calendar, hit rate, annualization,
    empty windows)
  * AstroGrid exclusion in ``collect_trades`` (SQL filter + Python guard)
  * ``run_daily`` with a MagicMock engine asserting the INSERTs are issued
  * ``fetch_realized_alpha`` / router shape per ``.claude/rules/security.md``
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from alpha_research import realized_alpha as ra


# ─────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────

AS_OF = date(2026, 9, 10)


def _flat_spy(start: str = "2025-06-02", end: str = "2026-09-10", level: float = 500.0) -> pd.Series:
    idx = pd.bdate_range(start, end)
    return pd.Series([level] * len(idx), index=idx, dtype="float64")


def _spy_with_move(entry: date, exit_: date, pct: float) -> pd.Series:
    """Flat at 500 until ``exit_`` (inclusive) where it steps to 500*(1+pct)."""
    s = _flat_spy()
    s.loc[s.index >= pd.Timestamp(exit_)] = 500.0 * (1.0 + pct)
    assert pd.Timestamp(entry) in s.index or True
    return s


def _make_engine(dispatch: Callable[[str, Any], MagicMock]) -> tuple[MagicMock, MagicMock]:
    """MagicMock engine whose ``conn.execute(stmt, params)`` → ``dispatch``."""
    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, *args, **kwargs):
        sql = str(getattr(stmt, "text", stmt))
        params = args[0] if args else kwargs.get("parameters")
        return dispatch(sql, params)

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _res(rows: list | None = None, one: Any = None) -> MagicMock:
    m = MagicMock()
    m.fetchall.return_value = rows or []
    m.fetchone.return_value = one if one is not None else (rows[0] if rows else None)
    return m


def _scored(source: str, exit_date: date, alpha: float, hold: int = 10,
            gross: float | None = None, spy: float = 0.0) -> ra.ScoredTrade:
    g = alpha + 0.001 + spy if gross is None else gross
    return ra.ScoredTrade(
        trade=ra.TradeRow(
            source=source, source_id=f"{source}:{exit_date.isoformat()}:{alpha}",
            ticker="AAPL", direction="LONG", entry_date=date(2026, 1, 2),
            exit_date=exit_date, entry_price=100.0, exit_price=100.0 * (1 + g),
        ),
        result=ra.TradeAlpha(
            gross_return=g, cost_drag=0.001, net_return=g - 0.001,
            spy_return=spy, alpha=alpha, holding_days=hold,
        ),
    )


# ─────────────────────────────────────────────────────────────────
# compute_trade_alpha — pure function
# ─────────────────────────────────────────────────────────────────


class TestComputeTradeAlpha:
    ENTRY = date(2026, 3, 2)
    EXIT = date(2026, 3, 16)

    def test_long_win_flat_spy(self) -> None:
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 110.0, "LONG", _flat_spy())
        assert r.gross_return == pytest.approx(0.10)
        assert r.cost_drag == pytest.approx(0.001)  # 2 × 5 bp
        assert r.net_return == pytest.approx(0.099)
        assert r.spy_return == pytest.approx(0.0)
        assert r.alpha == pytest.approx(0.099)
        assert r.holding_days == 10  # ten business days in (Mar 2, Mar 16]

    def test_long_loss(self) -> None:
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 90.0, "LONG", _flat_spy())
        assert r.gross_return == pytest.approx(-0.10)
        assert r.alpha == pytest.approx(-0.101)

    def test_short_win(self) -> None:
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 90.0, "SHORT", _flat_spy())
        assert r.gross_return == pytest.approx(0.10)
        assert r.alpha == pytest.approx(0.099)

    def test_short_loss_and_bearish_alias(self) -> None:
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 105.0, "bearish", _flat_spy())
        assert r.gross_return == pytest.approx(-0.05)

    def test_cost_drag_scales_with_bps(self) -> None:
        zero = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 101.0, "LONG", _flat_spy(), cost_bps=0.0)
        ten = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 101.0, "LONG", _flat_spy(), cost_bps=10.0)
        assert zero.net_return == pytest.approx(zero.gross_return)
        assert ten.cost_drag == pytest.approx(0.002)
        assert zero.alpha - ten.alpha == pytest.approx(0.002)

    def test_spy_outperforming_makes_alpha_negative(self) -> None:
        spy = _spy_with_move(self.ENTRY, self.EXIT, 0.08)
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 105.0, "LONG", spy)
        assert r.gross_return == pytest.approx(0.05)
        assert r.spy_return == pytest.approx(0.08)
        assert r.alpha == pytest.approx(0.05 - 0.001 - 0.08)
        assert r.alpha < 0

    def test_short_during_spy_rally_is_penalised_twice(self) -> None:
        spy = _spy_with_move(self.ENTRY, self.EXIT, 0.03)
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 100.0, "SHORT", spy)
        assert r.gross_return == pytest.approx(0.0)
        assert r.alpha == pytest.approx(-0.001 - 0.03)

    def test_as_of_lookup_uses_last_close_on_or_before(self) -> None:
        # Saturday entry → uses Friday's close; no lookahead to Monday.
        spy = _flat_spy()
        spy.loc[pd.Timestamp("2026-03-09")] = 999.0  # Monday after
        r = ra.compute_trade_alpha(date(2026, 3, 7), date(2026, 3, 8), 100.0, 100.0, "LONG", spy)
        assert r.spy_return == pytest.approx(0.0)

    def test_benchmark_gap_raises(self) -> None:
        spy = _flat_spy(start="2026-05-01")
        with pytest.raises(ValueError):
            ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 110.0, "LONG", spy)

    def test_bad_inputs_raise(self) -> None:
        with pytest.raises(ValueError):
            ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 110.0, "neutral", _flat_spy())
        with pytest.raises(ValueError):
            ra.compute_trade_alpha(self.ENTRY, self.EXIT, 0.0, 110.0, "LONG", _flat_spy())
        with pytest.raises(ValueError):
            ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, float("nan"), "LONG", _flat_spy())
        with pytest.raises(ValueError):
            ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 110.0, "LONG", _flat_spy(), cost_bps=-1)

    def test_result_is_frozen(self) -> None:
        r = ra.compute_trade_alpha(self.ENTRY, self.EXIT, 100.0, 110.0, "LONG", _flat_spy())
        with pytest.raises(Exception):
            r.alpha = 0.0  # type: ignore[misc]


class TestNormalizeDirection:
    @pytest.mark.parametrize("raw,expected", [
        ("LONG", "LONG"), ("long", "LONG"), ("bullish", "LONG"), ("CALL", "LONG"),
        ("SHORT", "SHORT"), ("bearish", "SHORT"), ("PUT", "SHORT"),
        ("neutral", None), (None, None), ("", None),
    ])
    def test_vocab(self, raw, expected) -> None:
        assert ra.normalize_direction(raw) == expected


# ─────────────────────────────────────────────────────────────────
# Window bucketing
# ─────────────────────────────────────────────────────────────────


class TestWindowBucketing:
    def test_window_start_uses_trading_calendar(self) -> None:
        cal = _flat_spy().index
        assert ra.window_start(AS_OF, 5, cal) == date(2026, 9, 4)   # 5 sessions back incl. as_of
        assert ra.window_start(AS_OF, 1, cal) == AS_OF
        # Calendar fallback ≈ 252/365 scaling
        assert ra.window_start(AS_OF, 20, None) == AS_OF - pd.Timedelta(days=29).to_pytimedelta()

    def test_trades_bucket_by_exit_date(self) -> None:
        cal = _flat_spy().index
        scored = [
            _scored("paper_trades", date(2026, 9, 9), 0.02),    # inside 5d
            _scored("paper_trades", date(2026, 8, 31), -0.01),  # inside 10d, outside 5d
            _scored("paper_trades", date(2026, 6, 1), 0.03),    # inside 90d, outside 60d
            _scored("paper_trades", date(2025, 12, 1), 0.10),   # outside 180d
            _scored("oracle_predictions", date(2026, 9, 9), 0.05),  # other source
        ]
        windows = ra.bucket_windows(scored, AS_OF, "paper_trades", trading_calendar=cal)
        by_h = {w.horizon_days: w for w in windows}
        assert set(by_h) == set(ra.HORIZONS)
        assert by_h[5].n_trades == 1
        assert by_h[10].n_trades == 2
        assert by_h[20].n_trades == 2
        assert by_h[60].n_trades == 2
        assert by_h[90].n_trades == 3
        assert by_h[180].n_trades == 3  # Dec 1 2025 is > 180 sessions back
        assert by_h[5].mean_alpha == pytest.approx(0.02)
        assert by_h[10].mean_alpha == pytest.approx(0.005)
        assert by_h[10].hit_rate == pytest.approx(0.5)
        assert by_h[90].hit_rate == pytest.approx(2 / 3)
        # Only the other-source trade must be excluded from this source's rows
        oracle = {w.horizon_days: w for w in ra.bucket_windows(scored, AS_OF, "oracle_predictions", trading_calendar=cal)}
        assert oracle[5].n_trades == 1
        assert oracle[5].mean_alpha == pytest.approx(0.05)

    def test_empty_window_reports_zero_not_missing(self) -> None:
        windows = ra.bucket_windows([], AS_OF, "paper_trades", trading_calendar=_flat_spy().index)
        assert len(windows) == len(ra.HORIZONS)
        for w in windows:
            assert w.n_trades == 0
            assert w.mean_alpha is None
            assert w.mean_alpha_annualized is None
            assert w.hit_rate is None
            assert w.cost_bps == ra.DEFAULT_COST_BPS

    def test_annualization_is_linear_in_holding_days(self) -> None:
        scored = [_scored("paper_trades", date(2026, 9, 9), 0.01, hold=21)]
        w = {x.horizon_days: x for x in ra.bucket_windows(scored, AS_OF, "paper_trades")}[5]
        assert w.mean_alpha_annualized == pytest.approx(0.01 * 252 / 21)

    def test_future_exits_are_excluded(self) -> None:
        scored = [_scored("paper_trades", date(2026, 9, 11), 0.01)]  # after as_of
        w = {x.horizon_days: x for x in ra.bucket_windows(scored, AS_OF, "paper_trades")}[180]
        assert w.n_trades == 0


# ─────────────────────────────────────────────────────────────────
# collect_trades — AstroGrid exclusion
# ─────────────────────────────────────────────────────────────────


def _oracle_row(pid: str, fc_source: str | None = None, direction: str = "bullish") -> tuple:
    return (
        pid, "NVDA", direction, 100.0, 105.0,
        date(2026, 8, 3), date(2026, 8, 17), "hit", fc_source,
    )


class TestAstrogridExclusion:
    def test_sql_filters_and_python_guard(self) -> None:
        seen_sql: list[str] = []

        def dispatch(sql: str, params: Any) -> MagicMock:
            seen_sql.append(sql)
            if "FROM oracle_predictions" in sql:
                assert params == {"as_of": AS_OF}
                # Simulate a DB that ignored the filters — the Python guard must catch them.
                return _res([
                    _oracle_row("pred_ok_1"),
                    _oracle_row("astrogrid:abc"),
                    _oracle_row("pred_tagged", fc_source="astrogrid"),
                    _oracle_row("pred_neutral", direction="neutral"),
                    _oracle_row("pred_ok_2", fc_source="grid"),
                ])
            return _res([])

        engine, _ = _make_engine(dispatch)
        rows = ra.collect_trades(engine, "oracle_predictions", AS_OF)
        assert [r.source_id for r in rows] == ["pred_ok_1", "pred_ok_2"]
        assert all(r.source == "oracle_predictions" and not r.is_open for r in rows)
        assert rows[0].entry_date == date(2026, 8, 3)
        assert rows[0].exit_date == date(2026, 8, 17)
        assert rows[0].exit_price == 105.0

        sql = next(s for s in seen_sql if "FROM oracle_predictions" in s)
        assert "left(id, 10) <> 'astrogrid:'" in sql
        assert "flow_context->>'source', '') <> 'astrogrid'" in sql
        assert "verdict <> 'pending'" in sql
        assert "dedup_keep = TRUE" in sql
        assert ":as_of" in sql

    def test_unknown_source_rejected(self) -> None:
        with pytest.raises(ValueError):
            ra.collect_trades(MagicMock(), "decision_journal", AS_OF)


class TestCollectPaperTrades:
    def test_closed_uses_exit_and_open_is_marked(self) -> None:
        def dispatch(sql: str, params: Any) -> MagicMock:
            if "FROM paper_trades" in sql:
                return _res([
                    (1, "AAPL", "LONG", 100.0, 110.0, date(2026, 8, 3), date(2026, 8, 20), "CLOSED"),
                    (2, "AAPL", "SHORT", 100.0, None, date(2026, 8, 25), None, "OPEN"),
                    (3, "ZZZZ", "LONG", 50.0, None, date(2026, 8, 25), None, "OPEN"),  # no price feature
                ])
            if "FROM feature_registry" in sql:
                names = params["names"]
                if "aapl_full" in names:
                    return _res([(77, "aapl_full")])
                return _res([])
            return _res([])

        engine, _ = _make_engine(dispatch)
        idx = pd.bdate_range("2026-08-20", "2026-09-10")
        matrix = pd.DataFrame({77: [200.0] * len(idx)}, index=idx)
        with patch("store.pit.PITStore.get_feature_matrix", return_value=matrix):
            rows = ra.collect_trades(engine, "paper_trades", AS_OF)

        by_id = {r.source_id: r for r in rows}
        assert set(by_id) == {"1", "2"}  # ZZZZ has no PIT price → skipped
        assert by_id["1"].is_open is False
        assert by_id["1"].exit_date == date(2026, 8, 20)
        assert by_id["2"].is_open is True
        assert by_id["2"].exit_date == date(2026, 9, 10)
        assert by_id["2"].exit_price == 200.0


# ─────────────────────────────────────────────────────────────────
# SPY resolution — must go through PITStore, never raw_series
# ─────────────────────────────────────────────────────────────────


class TestSpyResolution:
    def test_prefers_spy_full_then_fallback(self) -> None:
        def dispatch(sql: str, params: Any) -> MagicMock:
            assert "feature_registry" in sql
            assert "spy_full" in params["names"] and "sp500_full" in params["names"]
            return _res([(9, "sp500_full"), (3, "spy_full")])

        engine, _ = _make_engine(dispatch)
        assert ra.resolve_spy_feature(engine) == (3, "spy_full")

        engine2, _ = _make_engine(lambda sql, p: _res([(9, "sp500_full")]))
        assert ra.resolve_spy_feature(engine2) == (9, "sp500_full")

        engine3, _ = _make_engine(lambda sql, p: _res([]))
        with pytest.raises(LookupError):
            ra.resolve_spy_feature(engine3)

    def test_load_spy_path_goes_through_pit_store(self) -> None:
        engine, conn = _make_engine(lambda sql, p: _res([(3, "spy_full")]))
        idx = pd.bdate_range("2026-08-03", "2026-08-07")
        matrix = pd.DataFrame({3: [1.0, 2.0, None, 4.0, 5.0]}, index=idx)
        with patch("store.pit.PITStore.get_feature_matrix", return_value=matrix) as gfm:
            s = ra.load_spy_path(engine, date(2026, 8, 3), date(2026, 8, 7), AS_OF)
        gfm.assert_called_once()
        args, kwargs = gfm.call_args
        assert args[0] == [3]
        assert args[3] == AS_OF  # as_of passed through for release_date <= as_of
        assert list(s.values) == [1.0, 2.0, 4.0, 5.0]
        # Never touches raw_series directly
        for call in conn.execute.call_args_list:
            assert "raw_series" not in str(call.args[0])


# ─────────────────────────────────────────────────────────────────
# run_daily — end to end with a MagicMock engine
# ─────────────────────────────────────────────────────────────────


class TestRunDaily:
    def _engine(self) -> tuple[MagicMock, MagicMock, list[tuple[str, Any]]]:
        executed: list[tuple[str, Any]] = []

        def dispatch(sql: str, params: Any) -> MagicMock:
            executed.append((sql, params))
            if "FROM feature_registry" in sql:
                return _res([(3, "spy_full")])
            if "FROM paper_trades" in sql:
                return _res([
                    (1, "AAPL", "LONG", 100.0, 110.0, date(2026, 8, 3), date(2026, 8, 20), "CLOSED"),
                    (2, "MSFT", "SHORT", 100.0, 105.0, date(2026, 8, 3), date(2026, 8, 20), "STOPPED"),
                ])
            if "FROM oracle_predictions" in sql:
                return _res([_oracle_row("pred_1"), _oracle_row("astrogrid:x")])
            return _res([])

        engine, conn = _make_engine(dispatch)
        return engine, conn, executed

    def test_inserts_daily_and_trade_rows(self) -> None:
        engine, conn, executed = self._engine()
        spy = _flat_spy()
        matrix = pd.DataFrame({3: spy.values}, index=spy.index)
        with patch("store.pit.PITStore.get_feature_matrix", return_value=matrix):
            summary = ra.run_daily(engine, as_of=AS_OF)

        assert summary["spy_feature"] == "spy_full"
        assert summary["collected"] == {"paper_trades": 2, "oracle_predictions": 1}
        assert summary["scored"] == 3
        assert summary["skipped"] == 0
        assert summary["daily_rows"] == len(ra.SOURCES) * len(ra.HORIZONS)
        assert summary["trade_rows"] == 3

        daily = [(s, p) for s, p in executed if "INSERT INTO realized_alpha_daily" in s]
        trades = [(s, p) for s, p in executed if "INSERT INTO realized_alpha_trades" in s]
        assert len(daily) == 1 and len(trades) == 1

        daily_sql, daily_params = daily[0]
        assert "ON CONFLICT (as_of, source, horizon_days)" in daily_sql
        assert isinstance(daily_params, list) and len(daily_params) == 12
        assert {p["source"] for p in daily_params} == set(ra.SOURCES)
        assert {p["horizon_days"] for p in daily_params} == set(ra.HORIZONS)
        assert all(p["as_of"] == AS_OF for p in daily_params)
        # No value was interpolated into the SQL string
        assert "2026-09-10" not in daily_sql and "paper_trades'" not in daily_sql

        trade_sql, trade_params = trades[0]
        assert "ON CONFLICT (as_of, source, source_id)" in trade_sql
        assert {p["source_id"] for p in trade_params} == {"1", "2", "pred_1"}
        by_id = {p["source_id"]: p for p in trade_params}
        assert by_id["1"]["gross_return"] == pytest.approx(0.10)
        assert by_id["2"]["gross_return"] == pytest.approx(-0.05)   # short, price rose
        assert by_id["pred_1"]["gross_return"] == pytest.approx(0.05)
        assert all(p["cost_bps"] == pytest.approx(5.0) for p in trade_params)
        assert all(p["spy_return"] == pytest.approx(0.0) for p in trade_params)

        # Both writes happen inside engine.begin() (one transaction)
        assert engine.begin.called

        # Headline: the 20d window contains all three exits (Aug 17/20 are
        # within 20 sessions of Sep 10); 60d too.
        p60 = next(p for p in daily_params if p["source"] == "paper_trades" and p["horizon_days"] == 60)
        assert p60["n_trades"] == 2
        assert p60["mean_alpha"] == pytest.approx(((0.10 - 0.001) + (-0.05 - 0.001)) / 2)
        assert p60["hit_rate"] == pytest.approx(0.5)
        p5 = next(p for p in daily_params if p["source"] == "paper_trades" and p["horizon_days"] == 5)
        assert p5["n_trades"] == 0 and p5["mean_alpha"] is None
        assert "paper_trades/60d" in summary["headline_annualized"]

    def test_empty_spy_path_raises_and_writes_nothing(self) -> None:
        engine, conn, executed = self._engine()
        empty = pd.DataFrame(index=pd.DatetimeIndex([], name="obs_date"))
        with patch("store.pit.PITStore.get_feature_matrix", return_value=empty):
            with pytest.raises(LookupError):
                ra.run_daily(engine, as_of=AS_OF)
        assert not any("INSERT INTO" in s for s, _ in executed)

    def test_compute_realized_alpha_returns_windows_only(self) -> None:
        engine, conn, executed = self._engine()
        spy = _flat_spy()
        matrix = pd.DataFrame({3: spy.values}, index=spy.index)
        with patch("store.pit.PITStore.get_feature_matrix", return_value=matrix):
            windows = ra.compute_realized_alpha(engine, as_of=AS_OF)
        assert len(windows) == len(ra.SOURCES) * len(ra.HORIZONS)
        assert all(isinstance(w, ra.AlphaWindow) for w in windows)
        assert not any("INSERT INTO" in s for s, _ in executed)


# ─────────────────────────────────────────────────────────────────
# Read path + router
# ─────────────────────────────────────────────────────────────────


class TestFetchAndRouter:
    def test_fetch_shape_and_params(self) -> None:
        seen: list[tuple[str, Any]] = []

        def dispatch(sql: str, params: Any) -> MagicMock:
            seen.append((sql, params))
            if "COUNT(*)" in sql:
                return _res(one=(7,))
            return _res([(
                AS_OF, "paper_trades", 60, 12, 0.012, 0.05, 0.02, 0.008, 0.58, 5.0,
                datetime(2026, 9, 10, 6, 30, tzinfo=timezone.utc),
            )])

        engine, _ = _make_engine(dispatch)
        out = ra.fetch_realized_alpha(engine, source="paper_trades", horizon_days=60, days=30, limit=5, offset=5)
        assert set(out) == {"entries", "total", "limit", "offset", "has_more"}
        assert out["total"] == 7 and out["limit"] == 5 and out["offset"] == 5
        assert out["has_more"] is False
        e = out["entries"][0]
        assert e["as_of"] == "2026-09-10" and e["horizon_days"] == 60
        assert e["mean_alpha_annualized"] == pytest.approx(0.05)
        assert e["computed_at"].startswith("2026-09-10T06:30")

        select_sql, select_params = seen[0]
        assert "make_interval(days => :days)" in select_sql
        assert "source = :source" in select_sql and "horizon_days = :horizon_days" in select_sql
        assert "LIMIT :limit OFFSET :offset" in select_sql
        assert select_params == {"days": 30, "source": "paper_trades", "horizon_days": 60, "limit": 5, "offset": 5}
        count_sql, count_params = seen[1]
        assert "COUNT(*)" in count_sql and "LIMIT" not in count_sql
        assert count_params == {"days": 30, "source": "paper_trades", "horizon_days": 60}

    def test_fetch_rejects_unknown_source(self) -> None:
        with pytest.raises(ValueError):
            ra.fetch_realized_alpha(MagicMock(), source="decision_journal")

    def test_router_delegates_and_maps_errors(self, monkeypatch) -> None:
        import asyncio

        from fastapi import HTTPException

        from api.routers import realized_alpha as router_mod

        monkeypatch.setattr("api.routers.realized_alpha.get_db_engine", lambda: MagicMock())
        payload = {"entries": [], "total": 0, "limit": 50, "offset": 0, "has_more": False}
        monkeypatch.setattr(ra, "fetch_realized_alpha", lambda *a, **k: payload)
        out = asyncio.run(router_mod.get_realized_alpha(None, None, 90, 50, 0, "tok"))
        assert out == payload

        def boom(*a, **k):
            raise RuntimeError("db down")

        monkeypatch.setattr(ra, "fetch_realized_alpha", boom)
        with pytest.raises(HTTPException) as exc:
            asyncio.run(router_mod.get_realized_alpha(None, None, 90, 50, 0, "tok"))
        assert exc.value.status_code == 500

    def test_router_is_registered_in_main(self) -> None:
        from pathlib import Path

        main_src = Path(__file__).resolve().parent.parent.joinpath("api", "main.py").read_text()
        assert '("realized_alpha", "api.routers.realized_alpha", False)' in main_src

    def test_scheduler_registration(self) -> None:
        from pathlib import Path

        src = Path(__file__).resolve().parent.parent.joinpath("intelligence", "scheduler.py").read_text()
        assert '_sched.every().day.at("06:30").do(_realized_alpha_daily)' in src
        assert "from alpha_research.realized_alpha import run_daily" in src
