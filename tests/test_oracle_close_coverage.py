"""Close coverage for every ticker the oracle predicts on (S15, 2026-11-06).

Root cause: ~45 biotech tickers (ERAS, TGTX, GOSS, MESO, ...) had no close
after 2026-09-09. Their only routine close writer was
small_cap_enrichment.ensure_prices, whose sub-$2B cap gate dropped them after
their first enrichment on 2026-09-10; neither Tiingo's nor yfinance's routine
list held them, while the oracle kept predicting on them from
options_daily_signals. Fix: TiingoPuller.default_tickers adds
oracle.engine.oracle_close_universe (active ∪ pending), and
scripts/backfill_oracle_closes.py fills the gap (dry run by default).

Same gap for forward hypothesis trials (2026-11-07): 36 of the 115 tickers
Sleuth intersection leads named from June to October had no routine close,
so none of their 72 backtest trials could be scored. default_tickers also adds
intelligence.hypothesis_trials.ticker_move_close_universe (every ticker an
active ticker-move hypothesis will be scored on).

And for insider buying (2026-11-08): 287 of the 332 tickers with 2+
open-market insider buyers in 90 days had no routine close, so Sleuth's
insider-only control was 65% untestable. default_tickers adds
TiingoPuller.insider_buyer_tickers.

All engines here are local SQLite or fakes and all HTTP is mocked.
"""

from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import ingestion.tiingo_pull as tp
from ingestion.tiingo_pull import TiingoPuller, is_tiingo_equity
from intelligence.hypothesis_trials import (
    ACTIVE_TICKER_MOVE_TICKERS_SQL,
    plan_trial,
    ticker_move_close_universe,
)
from oracle.engine import (
    ORACLE_ACTIVE_TICKERS_SQL,
    ORACLE_PENDING_TICKERS_SQL,
    OracleEngine,
    oracle_close_universe,
)

ROOT = Path(__file__).resolve().parents[1]
SRC = 524


def _load_backfill() -> Any:
    spec = importlib.util.spec_from_file_location(
        "backfill_oracle_closes", ROOT / "scripts" / "backfill_oracle_closes.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


class _FakeConn:
    """Answers the universe queries by SQL text."""

    def __init__(self, active: list[Any], pending: list[Any],
                 hypotheses: list[Any] | None = None, insiders: list[Any] | None = None) -> None:
        self.by_sql = {ORACLE_ACTIVE_TICKERS_SQL: active, ORACLE_PENDING_TICKERS_SQL: pending,
                       ACTIVE_TICKER_MOVE_TICKERS_SQL: hypotheses or [],
                       tp.INSIDER_BUYER_TICKERS_SQL: insiders or []}

    def execute(self, clause: Any, params: Any = None) -> Any:
        rows = self.by_sql[str(clause)]
        res = MagicMock()
        res.fetchall.return_value = [(t,) for t in rows]
        return res


class _FakeEngine:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def connect(self) -> Any:
        conn = self.conn

        class _Ctx:
            def __enter__(self) -> Any:
                return conn

            def __exit__(self, *exc: object) -> None:
                return None

        return _Ctx()


def _sqlite_raw_series() -> Any:
    eng = create_engine("sqlite://", poolclass=StaticPool,
                        connect_args={"check_same_thread": False})
    with eng.begin() as c:
        c.execute(text(
            "CREATE TABLE raw_series (series_id TEXT, source_id INT, obs_date DATE, "
            "value REAL, pull_status TEXT)"
        ))
        c.execute(text(
            "CREATE TABLE oracle_predictions (id TEXT, ticker TEXT, expiry DATE, verdict TEXT)"
        ))
    return eng


# ── oracle universe ──────────────────────────────────────────────────────


def test_close_universe_is_active_union_pending() -> None:
    conn = _FakeConn(active=["ERAS", "SPY"], pending=["TGTX", "spy ", None, ""])
    assert oracle_close_universe(conn) == ["ERAS", "SPY", "TGTX"]


def test_engine_default_tickers_use_the_shared_active_sql() -> None:
    oe = OracleEngine.__new__(OracleEngine)
    oe.engine = _FakeEngine(_FakeConn(active=["GOSS", "MESO"], pending=["X"]))
    assert oe._get_active_tickers() == ["GOSS", "MESO"]


def test_pending_sql_matches_the_partial_index_predicate() -> None:
    # idx_oracle_pred_expiry is WHERE verdict = 'pending': keep the query on it.
    assert "verdict = 'pending'" in ORACLE_PENDING_TICKERS_SQL


# ── Tiingo routine list ──────────────────────────────────────────────────


def test_default_tickers_add_oracle_universe_without_crypto() -> None:
    eng = _FakeEngine(_FakeConn(active=["ERAS", "BTC", "ETH-USD"], pending=["GOSS", "^VIX"]))
    assert TiingoPuller.oracle_tickers(eng) == ["ERAS", "GOSS"]
    tickers = TiingoPuller.default_tickers(eng)
    assert {"ERAS", "GOSS"} <= set(tickers)
    # The static lists are still there.
    assert "SPY" in tickers
    # Without an engine nothing oracle-specific is added (old behaviour).
    assert "ERAS" not in TiingoPuller.default_tickers()


def test_default_tickers_degrade_when_db_unreachable() -> None:
    eng = MagicMock()
    eng.connect.side_effect = RuntimeError("db down")
    assert TiingoPuller.oracle_tickers(eng) == []
    assert "SPY" in TiingoPuller.default_tickers(eng)


def test_pull_incremental_default_list_includes_oracle_tickers(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    class _P(TiingoPuller):
        def __init__(self) -> None:
            self.engine = _FakeEngine(_FakeConn(active=["ERAS"], pending=["TGTX"]))
            self.source_id = SRC

        def _try_acquire_advisory_lock(self) -> Any:
            return object()

        def _release_advisory_lock(self, conn: Any) -> None:
            return None

        def _latest_tiingo_obs(self, ticker: str) -> date | None:
            seen.append(ticker)
            return date(2099, 1, 1)  # CURRENT: no fetch

    _P().pull_incremental(now=None)
    assert {"ERAS", "TGTX", "SPY"} <= set(seen)


@pytest.mark.parametrize("sym, ok", [("ERAS", True), ("BRK-B", True), ("BTC", False),
                                     ("SOL-USD", False), ("^GSPC", False), ("DXY", False)])
def test_is_tiingo_equity(sym: str, ok: bool) -> None:
    assert is_tiingo_equity(sym) is ok


# ── active-hypothesis universe ───────────────────────────────────────────


_HYPOTHESES = [
    # (id, status, test_criteria): only active rows naming a ticker and a direction.
    ("h_gme", "active", '{"ticker": " gme", "expected_direction": "down", "window_days": 14}'),
    ("h_kweb", "active", '{"ticker": "KWEB", "expected_direction": "up"}'),
    ("h_dup", "active", '{"ticker": "GME", "expected_direction": "up"}'),
    ("h_crypto", "active", '{"ticker": "BTC-USD", "expected_direction": "up"}'),
    ("h_nodir", "active", '{"ticker": "RXRX"}'),
    ("h_closed", "invalidated", '{"ticker": "AI", "expected_direction": "up"}'),
    ("h_leadlag", "active", '{"watch_signal": "sig:x", "expect_signal": "sig:y", "lag_days": 2}'),
    ("h_blank", "active", '{"ticker": "", "expected_direction": "up"}'),
]


def _sqlite_hypotheses() -> Any:
    eng = create_engine("sqlite://", poolclass=StaticPool,
                        connect_args={"check_same_thread": False})
    with eng.begin() as c:
        c.execute(text("CREATE TABLE discovered_hypotheses (id TEXT, status TEXT, test_criteria TEXT)"))
        for row in _HYPOTHESES:
            c.execute(text("INSERT INTO discovered_hypotheses VALUES (:i, :s, :c)"),
                      {"i": row[0], "s": row[1], "c": row[2]})
    return eng


def test_hypothesis_universe_is_active_ticker_move_rows() -> None:
    with _sqlite_hypotheses().connect() as conn:
        assert ticker_move_close_universe(conn) == ["BTC-USD", "GME", "KWEB"]


def test_hypothesis_universe_matches_the_scorers_ticker_move_rule() -> None:
    # Every active row the universe takes is one plan_trial scores as a ticker
    # move, and every active ticker-move row is in the universe.
    import json
    from datetime import datetime, timezone

    with _sqlite_hypotheses().connect() as conn:
        universe = set(ticker_move_close_universe(conn))
    created = datetime(2026, 11, 7, tzinfo=timezone.utc)
    for hid, status, crit in _HYPOTHESES:
        if status != "active":
            continue
        plan = plan_trial(hid, "convergence", json.loads(crit), created)
        ticker = (json.loads(crit).get("ticker") or "").strip().upper()
        if plan.test_kind == "ticker_move" and ticker:
            assert ticker in universe, hid
        elif ticker:
            assert hid == "h_nodir" and ticker not in universe


def test_default_tickers_add_hypothesis_universe_without_crypto() -> None:
    eng = _sqlite_hypotheses()
    assert TiingoPuller.hypothesis_tickers(eng) == ["GME", "KWEB"]
    tickers = set(TiingoPuller.default_tickers(eng))
    assert {"GME", "KWEB", "SPY"} <= tickers
    assert "RXRX" not in tickers and "AI" not in tickers


def test_default_tickers_union_oracle_and_hypotheses() -> None:
    eng = _FakeEngine(_FakeConn(active=["ERAS"], pending=["TGTX"], hypotheses=["GRRR"]))
    assert {"ERAS", "TGTX", "GRRR"} <= set(TiingoPuller.default_tickers(eng))


def test_hypothesis_tickers_degrade_when_db_unreachable() -> None:
    eng = MagicMock()
    eng.connect.side_effect = RuntimeError("db down")
    assert TiingoPuller.hypothesis_tickers(eng) == []


# ── insider-buyer universe (2026-11-08) ──────────────────────────────────

TODAY = date(2026, 11, 8)


def _sqlite_insiders(rows: list[tuple[str, str, str, str, int, dict]]) -> Any:
    """rows: (source_type, source_id, ticker, signal_type, days_ago, signal_value)."""
    import json

    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    with eng.begin() as c:
        c.execute(text("CREATE TABLE signal_sources (source_type TEXT, source_id TEXT, ticker TEXT, "
                       "signal_type TEXT, signal_date DATE, signal_value TEXT)"))
        for st, sid, t, sty, ago, val in rows:
            c.execute(text("INSERT INTO signal_sources VALUES (:a, :b, :c, :d, :e, :f)"),
                      {"a": st, "b": sid, "c": t, "d": sty,
                       "e": (TODAY - timedelta(days=ago)).isoformat(), "f": json.dumps(val)})
    return eng


def _sec(name: str, ticker: str, kind: str = "BUY", ago: int = 5) -> tuple:
    return ("insider", name, ticker, kind, ago, {"transaction_code": "P"})


def _qq(name: str, ticker: str, code: str = "P", ago: int = 5) -> tuple:
    return ("quiverquant:insider", f"qq_insider_trading:{name.lower()}|{code.lower()}|1|1", ticker,
            "insider_buy", ago, {"Name": name, "TransactionCode": code, "AcquiredDisposedCode": "A"})


def test_insider_universe_needs_two_open_market_buyers_in_one_feed() -> None:
    eng = _sqlite_insiders([
        _sec("Doe Jane", "ACME"), _sec("Roe Rick", "acme ", "UNUSUAL_BUY"),   # 2 SEC buyers
        _sec("Doe Jane", "SOLO"), _sec("Doe Jane", "SOLO", ago=20),           # one buyer twice
        _sec("Doe Jane", "XFED"), _qq("Doe Jane", "XFED"),                    # same person, two feeds
        _qq("Ann Lee", "QQP"), _qq("Bo Kim", "QQP"),                          # 2 QQ open-market buyers
        _qq("Ann Lee", "GRNT", "A"), _qq("Bo Kim", "GRNT", "M"),              # grants/exercises, not buys
        _sec("Doe Jane", "SELLR", "SELL"), _sec("Roe Rick", "SELLR", "SELL"),
        _sec("Doe Jane", "OLD", ago=91), _sec("Roe Rick", "OLD", ago=91),     # outside the window
        _sec("cluster_none", "NONE", "CLUSTER_BUY"),
        _sec("Fund A", "NONE"), _sec("Fund B", "NONE"),                       # unlisted issuers
        _sec("A1", "HEI, HEI.A"), _sec("A2", "HEI, HEI.A"),                   # malformed symbol
    ])
    assert TiingoPuller.insider_buyer_tickers(eng, today=TODAY) == ["ACME", "QQP"]


def test_insider_universe_ranks_by_buyers_and_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    eng = _sqlite_insiders([_sec(f"B{i}", "WIDE") for i in range(4)]
                           + [_sec("B1", "NARR"), _sec("B2", "NARR")])
    assert TiingoPuller.insider_buyer_tickers(eng, today=TODAY) == ["WIDE", "NARR"]
    monkeypatch.setattr(tp, "INSIDER_MAX_TICKERS", 1)
    assert TiingoPuller.insider_buyer_tickers(eng, today=TODAY) == ["WIDE"]


def test_default_tickers_union_insider_buyers() -> None:
    eng = _FakeEngine(_FakeConn(active=["ERAS"], pending=[], hypotheses=["GRRR"],
                                insiders=["ACME", "BTC-USD", "NONE"]))
    tickers = set(TiingoPuller.default_tickers(eng))
    assert {"ERAS", "GRRR", "ACME"} <= tickers
    assert "NONE" not in tickers


def test_insider_tickers_degrade_when_db_unreachable() -> None:
    eng = MagicMock()
    eng.connect.side_effect = RuntimeError("db down")
    assert TiingoPuller.insider_buyer_tickers(eng) == []


# ── pull_ticker dry run ──────────────────────────────────────────────────


def _tiingo_puller(eng: Any) -> TiingoPuller:
    p = TiingoPuller.__new__(TiingoPuller)
    p.engine = eng
    p.source_id = SRC
    return p


def _resp(days: list[date]) -> MagicMock:
    resp = MagicMock(status_code=200)
    resp.json.return_value = [
        {"date": f"{d.isoformat()}T00:00:00.000Z", "open": 1.0, "high": 2.0, "low": 0.5,
         "close": 10.0 + i, "volume": 100, "adjClose": 10.0 + i}
        for i, d in enumerate(days)
    ]
    return resp


def test_pull_ticker_dry_run_counts_and_never_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    eng = _sqlite_raw_series()
    held = date(2026, 9, 10)
    with eng.begin() as c:
        c.execute(text("INSERT INTO raw_series VALUES ('YF:ERAS:close', :s, :d, 9.0, 'SUCCESS')"),
                  {"s": SRC, "d": held})
    days = [held, date(2026, 9, 11), date(2026, 9, 12)]
    monkeypatch.setattr(tp.requests, "get", MagicMock(return_value=_resp(days)))
    p = _tiingo_puller(eng)
    p._insert_rows = MagicMock(side_effect=AssertionError("dry run wrote"))
    res = p.pull_ticker("ERAS", start_date=held, end_date=days[-1], dry_run=True)
    assert res["status"] == "SUCCESS"
    assert res["rows_inserted"] == 0
    # 6 fields x 3 days, minus the one close already held for this source.
    assert res["rows_would_insert"] == 17
    assert [d for d, _ in res["close_obs"]] == days
    with eng.connect() as c:
        assert c.execute(text("SELECT count(*) FROM raw_series")).scalar() == 1


def test_pull_ticker_real_run_still_inserts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tp.requests, "get", MagicMock(return_value=_resp([date(2026, 9, 11)])))
    p = _tiingo_puller(MagicMock())
    p._insert_rows = MagicMock(return_value=6)
    res = p.pull_ticker("ERAS", start_date="2026-09-11", end_date="2026-09-11")
    assert res["rows_inserted"] == 6 and "rows_would_insert" not in res


# ── backfill script ──────────────────────────────────────────────────────


class _FakeTiingo:
    source_id = SRC

    def __init__(self, unserved: set[str] | None = None) -> None:
        self.calls: list[tuple[str, date, date, bool]] = []
        self.unserved = unserved or set()

    def pull_ticker(self, ticker: str, *, start_date: date, end_date: date,
                    limiter: Any = None, dry_run: bool = False) -> dict[str, Any]:
        self.calls.append((ticker, start_date, end_date, dry_run))
        if ticker in self.unserved:
            return {"ticker": ticker, "status": "PARTIAL", "rows_inserted": 0, "errors": ["404"]}
        days = [d for d in _sessions(start_date, end_date)]
        if dry_run:
            return {"status": "SUCCESS", "rows_inserted": 0, "rows_would_insert": 6 * len(days),
                    "close_obs": [(d, 5.0) for d in days], "errors": []}
        return {"status": "SUCCESS", "rows_inserted": 6 * len(days), "errors": []}


def _sessions(a: date, b: date) -> list[date]:
    return [a + timedelta(days=i) for i in range((b - a).days + 1)
            if (a + timedelta(days=i)).weekday() < 5]


@pytest.fixture()
def bf(monkeypatch: pytest.MonkeyPatch) -> Any:
    mod = _load_backfill()
    monkeypatch.setattr(mod, "is_market_open", lambda d: d.weekday() < 5)
    return mod


def test_backfill_dry_run_skips_covered_tickers_and_projects_gap(bf: Any) -> None:
    eng = _sqlite_raw_series()
    start, end = date(2026, 9, 14), date(2026, 9, 18)  # Mon..Fri
    with eng.begin() as c:
        for d in _sessions(start, end):
            c.execute(text("INSERT INTO raw_series VALUES ('YF:SPY:close', :s, :d, 1.0, 'SUCCESS')"),
                      {"s": SRC, "d": d})
        # ERAS: only the first two sessions held -> fetch from the first gap.
        for d in _sessions(start, end)[:2]:
            c.execute(text("INSERT INTO raw_series VALUES ('YF:ERAS:close', :s, :d, 1.0, 'SUCCESS')"),
                      {"s": SRC, "d": d})
    tiingo = _FakeTiingo(unserved={"APGE"})
    yf = MagicMock()
    rep = bf.run(eng, start=start, end=end, apply=False, tickers=["SPY", "ERAS", "GOSS", "APGE"],
                 tiingo=tiingo, yfinance=yf)
    assert rep["mode"] == "dry_run" and rep["sessions"] == 5
    assert rep["current"] == 1 and rep["fetched"] == 3
    assert rep["missing_close_sessions"] == 3 + 5 + 5
    assert rep["tiingo_close_rows"] == 3 + 5
    assert rep["tiingo_unserved"] == ["APGE"]
    assert all(dry for *_, dry in tiingo.calls)
    assert ("ERAS", date(2026, 9, 16), end, True) in tiingo.calls
    yf.pull_ticker.assert_not_called()  # dry run never falls back to a writer


def test_backfill_apply_writes_and_falls_back_to_yfinance(bf: Any) -> None:
    eng = _sqlite_raw_series()
    start, end = date(2026, 9, 14), date(2026, 9, 18)
    tiingo = _FakeTiingo(unserved={"APGE"})
    yf = MagicMock()
    yf.pull_ticker.return_value = {"status": "SUCCESS", "rows_inserted": 30}
    rep = bf.run(eng, start=start, end=end, apply=True, tickers=["GOSS", "APGE"],
                 tiingo=tiingo, yfinance=yf)
    assert rep["mode"] == "apply"
    assert rep["tiingo_rows"] == 30 and rep["yfinance_rows"] == 30
    assert not any(dry for *_, dry in tiingo.calls)
    yf.pull_ticker.assert_called_once_with("APGE", start, end + timedelta(days=1))


def test_backfill_candidates_are_oracle_tickers_equities_only(bf: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    eng = _sqlite_raw_series()
    with eng.begin() as c:
        for i, (t, exp) in enumerate([("ERAS", "2026-09-18"), ("OLD", "2026-08-01"),
                                      ("BTC", "2026-10-01"), ("MESO", "2026-11-20")]):
            c.execute(text("INSERT INTO oracle_predictions VALUES (:i, :t, :e, 'no_data')"),
                      {"i": str(i), "t": t, "e": exp})
    monkeypatch.setattr(TiingoPuller, "oracle_tickers", staticmethod(lambda _e: ["GOSS"]))
    assert bf.candidate_tickers(eng, date(2026, 9, 10)) == ["ERAS", "GOSS", "MESO"]


def test_backfill_cli_defaults_to_dry_run(bf: Any) -> None:
    args = bf._parse_args([])
    assert args.apply is False and args.start == date(2026, 9, 10)
    with pytest.raises(SystemExit):
        bf._parse_args(["--start", "2026-10-01", "--end", "2026-09-01"])
