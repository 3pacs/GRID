"""``grid/signals/trial_signal.py`` gates: non-INDUSTRY skip, enforced mcap
gate, GRID-first company data order, dedupe, ``run_daily`` counts, and the
Hermes registry pin. No DB (fake psycopg2 connection), no network.
"""
from __future__ import annotations

import datetime
import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest

import grid.signals.trial_signal as ts
from grid.signals.sponsor_resolver import ResolvedSponsor

TODAY = datetime.date.today()


class _Cursor:
    """Dispatches fetchone() on a SQL substring -> row map; records executes."""

    def __init__(self, conn: "_Conn") -> None:
        self.conn = conn
        self.rowcount = 1

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.executed.append((sql, params))
        self._sql = sql
        if "trial_signals" in sql and "INSERT" in sql:
            self.rowcount = 0 if self.conn.signal_exists else 1
        elif "catalyst_calendar" in sql and "INSERT" in sql:
            self.rowcount = 1

    def fetchone(self) -> Any:
        for needle, row in self.conn.rows.items():
            if needle in self._sql:
                return row
        return None

    def close(self) -> None:
        pass


class _Conn:
    def __init__(self, rows: dict[str, Any] | None = None) -> None:
        self.rows = rows or {}
        self.executed: list[tuple[str, Any]] = []
        self.commits = 0
        self.rollbacks = 0
        self.signal_exists = False

    def cursor(self, **kwargs: Any) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        pass


def _trial(nct: str = "NCT001", sponsor: str = "Acme Bio", cls: str = "INDUSTRY", days: int = 90) -> ts.TrialRecord:
    return ts.TrialRecord(
        nct_id=nct, title="Phase 3 Overall Survival Breakthrough Study", sponsor=sponsor, sponsor_class=cls,
        phase="PHASE3", status="ACTIVE_NOT_RECRUITING", conditions=["Breast Cancer"], interventions=["drug"],
        enrollment_target=100, enrollment_actual=100, primary_completion=TODAY + datetime.timedelta(days=days),
        start_date=TODAY - datetime.timedelta(days=400), why_stopped=None, has_results=False,
    )


def _signal(monkeypatch: pytest.MonkeyPatch, trials: list[ts.TrialRecord], company: dict[str, Any] | None,
            resolver: Any = None, rows: dict[str, Any] | None = None) -> tuple[ts.TrialGemSignal, _Conn, MagicMock]:
    conn = _Conn(rows)
    sig = ts.TrialGemSignal(db_conn=conn, engine="ENGINE")
    monkeypatch.setattr(sig, "_load_from_cache", lambda: trials)
    monkeypatch.setattr(sig, "_get_regime", lambda: "GROWTH")
    if company is not None:
        monkeypatch.setattr(sig, "_fetch_company_data", lambda ticker, as_of=None: dict(company))
    resolve = MagicMock(side_effect=resolver or (lambda engine, name, cls: ResolvedSponsor("ACME", "sec_exact", 0.95)))
    monkeypatch.setattr(ts, "resolve_sponsor", resolve)
    return sig, conn, resolve


# ── non-INDUSTRY skip ─────────────────────────────────────────────────────


def test_non_industry_sponsors_are_skipped_before_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    trials = [_trial("NCT1", "Some University", "OTHER"), _trial("NCT2", "NIH", "NIH"), _trial("NCT3", "Acme Bio", "INDUSTRY")]
    sig, _conn, resolve = _signal(monkeypatch, trials, {"market_cap_mm": 500.0})
    out = sig.generate(top_n=10)
    assert [r.nct_id for r in out] == ["NCT3"]
    assert sig.stats["skipped_non_industry"] == 2
    resolve.assert_called_once_with("ENGINE", "Acme Bio", "INDUSTRY")


def test_unresolved_sponsor_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, _conn, _ = _signal(monkeypatch, [_trial()], {"market_cap_mm": 500.0},
                            resolver=lambda e, n, c: ResolvedSponsor(None, "unresolved", 0.0, "unresolved"))
    assert sig.generate() == [] and sig.stats["skipped_unresolved"] == 1


# ── enforced market-cap gate ──────────────────────────────────────────────


def test_cap_gate_skips_large_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, _conn, _ = _signal(monkeypatch, [_trial()], {"market_cap_mm": 2500.0})
    assert sig.generate() == []
    assert sig.stats["skipped_cap_gate"] == 1
    assert sig._company_gate({"market_cap_mm": 2500.0}) == "skip"
    assert sig._company_gate({"market_cap_mm": 5.0}) == "skip"       # shell
    assert sig._company_gate({"market_cap_mm": 1999.0}) == "pass"
    assert sig._company_gate({}) == "cap_unknown"
    assert sig._passes_company_gates({"market_cap_mm": 5400.0}) is False  # Celcuity at $5.4B (April bug)


def test_unknown_cap_is_capped_at_watchlist_with_red_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, _conn, _ = _signal(monkeypatch, [_trial()], {"market_cap_mm": 500.0})
    known = sig.generate()[0]
    assert known.signal_type == "BUY" and known.suggested_position_pct is not None  # strong trial, known small cap

    sig2, _conn2, _ = _signal(monkeypatch, [_trial()], {"market_cap_mm": None})
    unknown = sig2.generate()[0]
    assert unknown.signal_type == "WATCHLIST"
    assert unknown.suggested_position_pct is None
    assert "market_cap_unknown" in unknown.red_flags
    assert unknown.penalty_factors.get("market_cap_unknown") == 1.0
    assert unknown.market_cap_mm is None
    assert sig2.stats["cap_unknown"] == 1


def test_generate_dedupes_on_nct_and_ticker(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, _conn, _ = _signal(monkeypatch, [_trial("NCT1"), _trial("NCT1")], {"market_cap_mm": 500.0})
    assert len(sig.generate()) == 1 and sig.stats["deduped"] == 1


# ── GRID-first company data ───────────────────────────────────────────────


def _company(monkeypatch: pytest.MonkeyPatch, rows: dict[str, Any], fmp_key: str = "") -> tuple[ts.TrialGemSignal, _Conn]:
    conn = _Conn(rows)
    sig = ts.TrialGemSignal(db_conn=conn)
    monkeypatch.setenv("FMP_API_KEY", fmp_key)
    if not fmp_key:
        monkeypatch.setattr(ts, "_fmp_api_key", lambda: "")
    return sig, conn


def test_company_data_prefers_ticker_metrics_daily(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, conn = _company(monkeypatch, {
        "ticker_metrics_daily": (1.2e9,),
        "company_profiles": ({"market_cap": 800e6, "cash": 120e6, "quarterly_burn": 30e6},),
    })
    data = sig._fetch_company_data("ACME", as_of=TODAY)
    assert data["market_cap_mm"] == 1200.0 and data["market_cap_source"] == "ticker_metrics_daily"
    assert data["cash_runway_months"] == 12.0  # cash / (burn/3)
    metrics_sql, params = next((s, p) for s, p in conn.executed if "ticker_metrics_daily" in s)
    assert "obs_date <= %s" in metrics_sql and params == ("ACME", TODAY)  # PIT-bounded, parameterised


def test_company_data_falls_back_profile_then_tiingo_then_none(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, _ = _company(monkeypatch, {"company_profiles": ({"market_cap": 800e6, "cash_runway_months": 9.5},)})
    data = sig._fetch_company_data("ACME")
    assert (data["market_cap_mm"], data["market_cap_source"], data["cash_runway_months"]) == (800.0, "company_profiles", 9.5)

    sig, conn = _company(monkeypatch, {"raw_series": (3.0e9,)})
    data = sig._fetch_company_data("ACME", as_of=TODAY)
    assert (data["market_cap_mm"], data["market_cap_source"]) == (3000.0, "tiingo_fundamentals")
    tiingo_sql, params = next((s, p) for s, p in conn.executed if "raw_series" in s)
    assert params[0] == "TIINGO_FUND:ACME:market_cap" and "obs_date <= %s" in tiingo_sql

    sig, _ = _company(monkeypatch, {})
    data = sig._fetch_company_data("ACME")
    assert data["market_cap_mm"] is None and data["market_cap_source"] is None and data["cash_runway_months"] is None
    assert sig._fmp_calls == 0  # no key -> FMP never touched


def test_company_data_uses_fmp_only_with_key(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, _ = _company(monkeypatch, {}, fmp_key="k")
    resp = MagicMock(status_code=200)
    resp.json.return_value = [{"mktCap": 650e6}]
    get = MagicMock(return_value=resp)
    monkeypatch.setattr(ts.requests, "get", get)
    monkeypatch.setattr(ts.time, "sleep", lambda s: None)
    data = sig._fetch_company_data("ACME")
    assert (data["market_cap_mm"], data["market_cap_source"]) == (650.0, "fmp")
    assert get.call_args.kwargs["params"] == {"apikey": "k"}
    # network failure degrades to None
    monkeypatch.setattr(ts.requests, "get", MagicMock(side_effect=ConnectionError("down")))
    assert sig._fetch_company_data("ACME")["market_cap_mm"] is None


def test_company_lookup_failure_rolls_back_and_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Broken(_Conn):
        def cursor(self, **kwargs: Any) -> Any:
            raise RuntimeError("relation does not exist")

    sig = ts.TrialGemSignal(db_conn=_Broken())
    monkeypatch.setattr(ts, "_fmp_api_key", lambda: "")
    assert sig._fetch_company_data("ACME")["market_cap_mm"] is None


# ── write_to_db ───────────────────────────────────────────────────────────


def test_write_to_db_dedupes_and_writes_catalyst_calendar_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, conn, _ = _signal(monkeypatch, [_trial("NCT1"), _trial("NCT1")], {"market_cap_mm": 500.0})
    results = sig.generate()
    results = results + results  # duplicate (nct_id, ticker) in the batch
    written = sig.write_to_db(results, run_id="t")
    assert written == 1
    signal_sql, params = conn.executed[0]
    assert "INSERT INTO trial_signals" in signal_sql and "WHERE NOT EXISTS" in signal_sql
    assert "created_at >= CURRENT_DATE" in signal_sql
    assert params["ticker"] == "ACME" and params["regime_at_signal"] == "GROWTH" and params["sponsor_name"] == "Acme Bio"
    calendar_sql, cparams = conn.executed[1]
    assert "INSERT INTO catalyst_calendar" in calendar_sql and "'trial_signal'" in calendar_sql
    assert "NOT EXISTS" in calendar_sql and cparams["nct_id"] == "NCT1" and cparams["ticker"] == "ACME"
    assert cparams["expected_date"] == TODAY + datetime.timedelta(days=90)
    assert len(conn.executed) == 2 and conn.commits == 1


def test_write_to_db_reports_zero_when_row_exists_today(monkeypatch: pytest.MonkeyPatch) -> None:
    sig, conn, _ = _signal(monkeypatch, [_trial("NCT1")], {"market_cap_mm": 500.0})
    conn.signal_exists = True
    assert sig.write_to_db(sig.generate()) == 0


def test_storage_regime_maps_unknown_labels() -> None:
    assert ts._storage_regime("trending") == "UNKNOWN"
    assert ts._storage_regime("growth") == "GROWTH"
    assert ts._storage_regime(None) == "UNKNOWN"


# ── run_daily ─────────────────────────────────────────────────────────────


def test_run_daily_returns_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, Any] = {}

    class FakeSignal:
        def __init__(self, db_config: dict, engine: Any) -> None:
            created.update(db_config=db_config, engine=engine)
            self.stats = {"trials": 10, "skipped_non_industry": 6, "skipped_unresolved": 1,
                          "skipped_cap_gate": 1, "cap_unknown": 1, "scored": 2, "deduped": 0}
            self.closed = False

        def generate(self, top_n: int) -> list[Any]:
            created["top_n"] = top_n
            return [MagicMock(signal_type="BUY"), MagicMock(signal_type="WATCHLIST")]

        def write_to_db(self, results: list[Any]) -> int:
            return len(results)

        def close(self) -> None:
            created["closed"] = True

    monkeypatch.setattr(ts, "TrialGemSignal", FakeSignal)
    engine = MagicMock()
    engine.url = MagicMock(host="db.local", port=5433, database="griddb", username="grid", password="pw")
    out = ts.run_daily(engine)
    assert out["status"] == "SUCCESS" and out["scored"] == 2 and out["written"] == 2
    assert out["buy"] == 1 and out["watchlist"] == 1 and out["avoid"] == 0
    assert out["skipped_non_industry"] == 6 and out["cap_unknown"] == 1
    assert created["top_n"] == ts.RUN_DAILY_TOP_N == 60 and created["engine"] is engine and created["closed"]
    assert created["db_config"] == {"host": "db.local", "port": 5433, "dbname": "griddb", "user": "grid", "password": "pw"}


def test_run_daily_failure_returns_failed_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    class Boom:
        def __init__(self, **kwargs: Any) -> None:
            raise RuntimeError("no db")

    monkeypatch.setattr(ts, "TrialGemSignal", Boom)
    out = ts.run_daily(MagicMock(url=None))
    assert out["status"] == "FAILED" and out["written"] == 0 and "no db" in out["error"]


# ── wiring pins ───────────────────────────────────────────────────────────


def test_back_compat_aliases_point_at_the_resolver() -> None:
    import grid.signals.sponsor_resolver as sr

    assert ts._resolve_ticker_sec is sr.resolve_ticker_sec
    assert ts._load_sec_tickers is sr._load_sec_tickers
    assert not hasattr(ts, "AV_KEY")  # Alpha Vantage path removed


def test_hermes_registry_schedules_trial_signal_after_the_ingestor() -> None:
    from scripts import hermes_operator as ho

    entry = ho._SOURCE_REGISTRY["trial_signal"]
    assert entry == {"mod": "grid.signals.trial_signal", "fn": "run_daily", "interval_h": 24}
    assert ho._SOURCE_REGISTRY["trial_ingestor"]["interval_h"] == 24
    keys = list(ho._SOURCE_EXTRAS)
    assert keys.index("trial_ingestor") < keys.index("trial_signal") < keys.index("small_cap_enrichment")
    # _FunctionPuller injects the engine by parameter name
    assert "engine" in inspect.signature(ts.run_daily).parameters
