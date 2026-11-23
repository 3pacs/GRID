"""ticker_metrics_daily closes are filled from GRID's own stored prices; no
Twelve Data request is left in the scheduled path.

Owner decision 2026-11-23: "drop the twelve stuff, get it from our other
sources instead". ``scripts/close_backfill_universe.py`` replaces the fetch in
``scripts/td_backfill_universe.py`` and ``scripts/td_backfill_gem_tickers.py``
(kept as entry points for their systemd units) with a read of the stored
``YF:{TICKER}:close`` series.

Local fakes only: no network, no database.
"""

from __future__ import annotations

import re
import runpy
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import scripts.close_backfill_universe as mod

ROOT = Path(__file__).resolve().parents[1]
TODAY = date(2026, 11, 23)
SCRIPTS = ("close_backfill_universe.py", "td_backfill_universe.py", "td_backfill_gem_tickers.py")


def _obs(*pairs: tuple[date, float]) -> list[SimpleNamespace]:
    return [SimpleNamespace(obs_date=d, value=v) for d, v in pairs]


class _Conn:
    def __init__(self, engine: "_Engine", writing: bool) -> None:
        self.engine, self.writing = engine, writing

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> Any:
        statement = " ".join(str(sql).split())
        self.engine.statements.append((statement, dict(params or {})))
        if statement.startswith("INSERT INTO ticker_metrics_daily"):
            assert self.writing, "a write outside engine.begin()"
            if params["ticker"] in self.engine.fail_writes:
                raise RuntimeError("deadlock detected")
            self.engine.upserts.append(dict(params))
            return SimpleNamespace(rowcount=self.engine.rowcount(params))
        return SimpleNamespace(fetchall=lambda: list(self.engine.work_rows))


class _Engine:
    def __init__(self, work_rows: list[tuple], rowcount=lambda params: 1, fail_writes: tuple[str, ...] = ()) -> None:
        self.work_rows, self.rowcount, self.fail_writes = work_rows, rowcount, set(fail_writes)
        self.statements: list[tuple[str, dict]] = []
        self.upserts: list[dict] = []
        self.begins = 0

    def connect(self) -> _Conn:
        return _Conn(self, writing=False)

    def begin(self) -> _Conn:
        self.begins += 1
        return _Conn(self, writing=True)


@pytest.fixture()
def stored(monkeypatch):
    """``stored[(ticker, source)] = [(date, close), ...]`` served as read_window."""
    data: dict[tuple[str, str], list[tuple[date, float]]] = {}
    calls: list[dict] = []

    def fake_read_window(conn, series_id, *, source=None, start=None, as_of=None, as_of_ts=None):
        ticker = series_id.split(":")[1]
        calls.append({"series_id": series_id, "source": source, "start": start, "as_of": as_of})
        return _obs(*[(d, v) for d, v in data.get((ticker, source), []) if start <= d <= as_of])

    monkeypatch.setattr(mod, "read_window", fake_read_window)
    data_calls = SimpleNamespace(data=data, calls=calls)
    return data_calls


D = [TODAY - timedelta(days=k) for k in range(5)][::-1]     # five days ending today


# ── which stored source ─────────────────────────────────────────────────────

def test_the_source_that_reaches_the_latest_date_wins(stored):
    stored.data[("KPTI", "TIINGO")] = [(D[0], 0.05), (D[1], 0.04)]
    stored.data[("KPTI", "yfinance")] = [(D[1], 0.04), (D[3], 0.03)]
    assert mod.stored_closes(None, "KPTI", D[0], TODAY) == ("yfinance", [(D[1], 0.04), (D[3], 0.03)])


def test_tiingo_wins_a_tie_and_sources_are_never_mixed(stored):
    stored.data[("SPY", "TIINGO")] = [(D[3], 700.0), (D[4], 701.0)]
    stored.data[("SPY", "yfinance")] = [(D[2], 699.5), (D[4], 701.2)]
    assert mod.stored_closes(None, "SPY", D[0], TODAY) == ("TIINGO", [(D[3], 700.0), (D[4], 701.0)])
    assert {c["series_id"] for c in stored.calls} == {"YF:SPY:close"}
    assert [c["source"] for c in stored.calls] == list(mod.PRICE_SOURCES)


def test_bad_closes_are_dropped_and_nothing_stored_is_none(stored):
    stored.data[("AAA", "TIINGO")] = [(D[0], 0.0), (D[1], float("nan")), (D[2], -1.0), (D[3], 12.5)]
    assert mod.stored_closes(None, "AAA", D[0], TODAY) == ("TIINGO", [(D[3], 12.5)])
    assert mod.stored_closes(None, "GLEN", D[0], TODAY) == (None, [])


# ── the universe run ────────────────────────────────────────────────────────

def test_universe_run_writes_stored_closes_and_names_tickers_without_any(stored, capsys):
    stored.data[("AAA", "TIINGO")] = [(D[3], 10.0), (D[4], 11.0)]
    stored.data[("BBB", "yfinance")] = [(D[4], 5.5)]
    engine = _Engine([("AAA", "DEAD"), ("GLEN", "DEAD"), ("BBB", "STALE_7_30")])

    summary = mod.run(engine, "universe", today=TODAY)

    assert summary["filled"] == 2 and summary["written"] == 3 and summary["left_as_is"] == 0
    assert summary["no_stored_close"] == ["GLEN"] and summary["failed"] == []
    assert summary["by_source"] == {"TIINGO": 1, "yfinance": 1}
    assert engine.upserts == [
        {"ticker": "AAA", "obs_date": D[3], "close": 10.0, "source": "stored_close_backfill:TIINGO"},
        {"ticker": "AAA", "obs_date": D[4], "close": 11.0, "source": "stored_close_backfill:TIINGO"},
        {"ticker": "BBB", "obs_date": D[4], "close": 5.5, "source": "stored_close_backfill:yfinance"},
    ]
    # Window by bucket: 365 days for DEAD, 60 for STALE_7_30.
    starts = {c["series_id"].split(":")[1]: c["start"] for c in stored.calls}
    assert starts == {"AAA": TODAY - timedelta(days=365), "GLEN": TODAY - timedelta(days=365),
                      "BBB": TODAY - timedelta(days=60)}
    out = capsys.readouterr().out
    assert "GLEN: skipped, no stored close since" in out and "no stored close: ['GLEN']" in out
    assert "DONE filled=2/3 written=3" in out


def test_work_query_reads_the_latest_audit_and_only_stale_buckets(stored):
    engine = _Engine([])
    mod.run(engine, "universe", today=TODAY)
    statement, params = engine.statements[0]
    assert "FROM data_freshness_audit" in statement and "MAX(audited_at)" in statement
    assert params == {"buckets": ["DEAD", "STALE_30+", "STALE_7_30"]}
    assert engine.begins == 0


def test_only_bucket_and_limit_narrow_the_run(stored):
    engine = _Engine([("AAA", "DEAD"), ("BBB", "DEAD"), ("CCC", "DEAD")])
    summary = mod.run(engine, "universe", today=TODAY, only_bucket="DEAD", limit=2)
    assert engine.statements[0][1] == {"buckets": ["DEAD"]}
    assert summary["tickers"] == 2 and summary["no_stored_close"] == ["AAA", "BBB"]


def test_dry_run_reads_but_never_opens_a_write(stored, capsys):
    stored.data[("AAA", "TIINGO")] = [(D[4], 11.0)]
    engine = _Engine([("AAA", "DEAD")])
    summary = mod.run(engine, "universe", today=TODAY, dry_run=True)
    assert engine.begins == 0 and engine.upserts == []
    assert summary["filled"] == 1 and summary["written"] == 0
    assert "would write up to 1" in capsys.readouterr().out


def test_rows_the_upsert_leaves_alone_are_counted_not_failed(stored):
    """rowcount 0: the row is owned by sec_xbrl_shares or already holds this close."""
    stored.data[("AAA", "TIINGO")] = [(D[3], 10.0), (D[4], 11.0)]
    engine = _Engine([("AAA", "STALE_30+")], rowcount=lambda p: 0 if p["obs_date"] == D[3] else 1)
    summary = mod.run(engine, "universe", today=TODAY)
    assert (summary["filled"], summary["written"], summary["left_as_is"]) == (1, 1, 1)
    assert summary["failed"] == []


def test_the_upsert_never_touches_an_xbrl_owned_row_or_its_columns():
    statement = " ".join(str(mod._UPSERT_SQL).split())
    assert "WHERE ticker_metrics_daily.shares_outstanding IS NULL" in statement
    assert "close_price IS DISTINCT FROM EXCLUDED.close_price" in statement
    update = statement.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0]
    assert "shares_outstanding" not in update and "market_cap_usd" not in update
    assert "(ticker, obs_date, close_price, source, as_of)" in statement


def test_one_failed_write_does_not_stop_the_rest(stored, capsys):
    stored.data[("AAA", "TIINGO")] = [(D[4], 11.0)]
    stored.data[("BBB", "TIINGO")] = [(D[4], 5.5)]
    engine = _Engine([("AAA", "DEAD"), ("BBB", "DEAD")], fail_writes=("AAA",))
    summary = mod.run(engine, "universe", today=TODAY)
    assert summary["failed"] == ["AAA"] and summary["filled"] == 1
    assert [u["ticker"] for u in engine.upserts] == ["BBB"]
    assert "AAA: FAILED (RuntimeError" in capsys.readouterr().err


# ── the gem run ─────────────────────────────────────────────────────────────

def test_gem_run_takes_tickers_without_a_recent_close_and_a_60_day_window(stored):
    stored.data[("FLUT", "TIINGO")] = [(D[4], 250.0)]
    engine = _Engine([("FLUT",)])
    summary = mod.run(engine, "gem", today=TODAY)
    statement, params = engine.statements[0]
    assert "FROM gem_alerts" in statement and "NOT EXISTS" in statement
    assert params == {"fresh_since": TODAY - timedelta(days=mod.GEM_FRESH_DAYS)}
    assert stored.calls[0]["start"] == TODAY - timedelta(days=60)
    assert summary["filled"] == 1 and engine.upserts[0]["ticker"] == "FLUT"


def test_unknown_mode_is_refused():
    with pytest.raises(ValueError, match="unknown mode"):
        mod.run(_Engine([]), "twelvedata")
    assert mod.main(["twelvedata"]) == 2


# ── exit code and entry points ──────────────────────────────────────────────

def _main(monkeypatch, engine, argv, env=None):
    import db

    monkeypatch.setattr(db, "get_engine", lambda: engine)
    for key in ("DRY_RUN", "ONLY_BUCKET", "LIMIT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)
    return mod.main(argv)


def test_exit_code_is_zero_when_nothing_has_a_stored_close(stored, monkeypatch):
    """The 2026-11-22 run: only tickers the provider cannot serve were left."""
    assert _main(monkeypatch, _Engine([("GLEN", "DEAD"), ("US", "DEAD")]), ["universe"]) == 0


def test_exit_code_is_one_only_when_writes_failed_and_nothing_was_filled(stored, monkeypatch, capsys):
    stored.data[("AAA", "TIINGO")] = [(D[4], 11.0)]
    stored.data[("BBB", "TIINGO")] = [(D[4], 5.5)]
    rows = [("AAA", "DEAD"), ("BBB", "DEAD")]
    assert _main(monkeypatch, _Engine(rows, fail_writes=("AAA", "BBB")), ["universe"]) == 1
    assert "total failure: 0/2 attempted tickers were filled" in capsys.readouterr().err
    assert _main(monkeypatch, _Engine(rows, fail_writes=("AAA",)), ["universe"]) == 0


def test_environment_switches_reach_the_run(stored, monkeypatch):
    stored.data[("AAA", "TIINGO")] = [(D[4], 11.0)]
    engine = _Engine([("AAA", "DEAD"), ("BBB", "DEAD")])
    assert _main(monkeypatch, engine, ["universe"], {"DRY_RUN": "1", "ONLY_BUCKET": "DEAD", "LIMIT": "1"}) == 0
    assert engine.statements[0][1] == {"buckets": ["DEAD"]} and engine.begins == 0
    assert len({c["series_id"] for c in stored.calls}) == 1


@pytest.mark.parametrize(("script", "mode"), [("td_backfill_universe.py", "universe"),
                                               ("td_backfill_gem_tickers.py", "gem")])
def test_the_units_entry_points_run_the_stored_close_backfill(monkeypatch, script, mode):
    seen: list[list[str]] = []
    monkeypatch.setattr(mod, "main", lambda argv=None: seen.append(argv) or 0)
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(ROOT / "scripts" / script), run_name="__main__")
    assert exit_info.value.code == 0 and seen == [[mode]]


@pytest.mark.parametrize("script", SCRIPTS)
def test_no_twelve_data_call_key_or_credential_is_left(script):
    source = (ROOT / "scripts" / script).read_text(encoding="utf-8")
    code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
    code = re.sub(r'""".*?"""', "", code, flags=re.S)       # docstrings name the old provider
    for needle in ("twelvedata", "TWELVEDATA", "apikey", "requests", "psycopg2", "password", "gridmaster"):
        assert needle not in code, (script, needle)


def test_closes_are_read_through_the_observation_reader_only():
    source = (ROOT / "scripts" / "close_backfill_universe.py").read_text(encoding="utf-8")
    assert "from store.observations import read_window" in source
    assert not re.search(r"from\s+raw_series\b", source, re.I)
