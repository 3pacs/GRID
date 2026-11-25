"""scripts/close_backfill_universe.py fills missing ticker_metrics_daily
closes from GRID's own stored prices, and only from completed closes.

Owner decision 2026-11-23: "drop the twelve stuff, get it from our other
sources instead". Codex HOLD 6031399890 on the first version: it took a
generic latest positive close and could overwrite an existing close. This
version reads through ``store.observations.read_completed_closes`` (the real
reader runs in these tests, over scripted raw rows) and only fills what is
missing. The script is not wired to a unit; the Twelve Data scripts are
unchanged until its coverage has been measured.

Local fakes only: no network, no database.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import scripts.close_backfill_universe as mod

ROOT = Path(__file__).resolve().parents[1]
EVALUATED_AT = datetime(2026, 11, 24, 1, 0, tzinfo=timezone.utc)     # Mon 20:00 New York
THU, FRI, SAT, MON = date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 21), date(2026, 11, 23)


def _after_close(day: date, hour_utc: int = 23) -> datetime:
    """A pull after 17:00 New York (22:00 UTC in November) on ``day``."""
    return datetime(day.year, day.month, day.day, hour_utc, 30, tzinfo=timezone.utc)


def _intraday(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 19, 0, tzinfo=timezone.utc)   # 14:00 New York


def raw(ticker: str, day: date, value: float, source: str = "TIINGO", pulled: datetime | None = None,
        payload: Any = None) -> dict[str, Any]:
    return {"sid": f"YF:{ticker}:close", "d": day, "v": value, "ts": pulled or _after_close(day),
            "source": source, "payload": payload}


class _Conn:
    def __init__(self, engine: "_Engine", writing: bool) -> None:
        self.engine, self.writing = engine, writing

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> Any:
        statement = " ".join(str(sql).split())
        params = dict(params or {})
        self.engine.statements.append((statement, params))
        if statement.startswith("INSERT INTO ticker_metrics_daily"):
            assert self.writing, "a write outside engine.begin()"
            if params["ticker"] in self.engine.fail_writes:
                raise RuntimeError("deadlock detected")
            self.engine.fills.append(params)
            return SimpleNamespace(rowcount=self.engine.rowcount(params))
        if "FROM ticker_metrics_daily" in statement and statement.startswith("SELECT obs_date, close_price"):
            rows = [(d, c) for (t, d, c) in self.engine.existing
                    if t == params["ticker"] and d >= params["start"]]
            return SimpleNamespace(fetchall=lambda: rows)
        if "FROM raw_series" in statement:
            assert params["ok"] == "SUCCESS"
            rows = [
                (r["d"], r["v"], r["ts"], r["source"], r["payload"])
                for r in self.engine.raw_rows
                if r["sid"] == params["sid"] and r["source"].lower() == params["source"].lower()
                and params["start"] <= r["d"] <= params["as_of"] and r["ts"] <= params["as_of_ts"]
            ]
            return SimpleNamespace(fetchall=lambda: rows)
        return SimpleNamespace(fetchall=lambda: list(self.engine.work_rows))


class _Engine:
    def __init__(self, work_rows: list[tuple], raw_rows: list[dict] | None = None,
                 rowcount=lambda params: 1, fail_writes: tuple[str, ...] = (),
                 existing: list[tuple] | None = None) -> None:
        self.work_rows, self.raw_rows = work_rows, raw_rows or []
        self.existing = existing or []          # (ticker, obs_date, close_price) already in ticker_metrics_daily
        self.rowcount, self.fail_writes = rowcount, set(fail_writes)
        self.statements: list[tuple[str, dict]] = []
        self.fills: list[dict] = []
        self.begins = 0

    def connect(self) -> _Conn:
        return _Conn(self, writing=False)

    def begin(self) -> _Conn:
        self.begins += 1
        return _Conn(self, writing=True)


def _run(engine: _Engine, mode: str = "universe", **kwargs) -> dict[str, Any]:
    return mod.run(engine, mode, evaluated_at=EVALUATED_AT, **kwargs)


# ── which closes are eligible: adversarial rows through the real reader ─────

def test_a_same_day_provisional_price_never_qualifies_and_the_completed_one_does():
    engine = _Engine([("AAA", "DEAD")], [
        raw("AAA", FRI, 10.40, pulled=_intraday(FRI)),          # pulled 14:00 New York: provisional
        raw("AAA", THU, 10.00, pulled=_intraday(THU)),
        raw("AAA", THU, 10.10),                                  # the completed close of Thursday
    ])
    receipt = _run(engine)
    assert [(f["obs_date"], f["close"]) for f in engine.fills] == [(THU, 10.10)]
    assert receipt["eligible_closes"] == 1


def test_only_a_provisional_price_means_nothing_is_written():
    engine = _Engine([("AAA", "DEAD")], [raw("AAA", FRI, 10.40, pulled=_intraday(FRI))])
    receipt = _run(engine)
    assert engine.fills == [] and engine.begins == 0
    assert receipt["no_completed_close"] == ["AAA"] and receipt["eligible_tickers"] == 0


def test_a_label_that_is_not_a_session_is_skipped():
    engine = _Engine([("AAA", "DEAD")], [raw("AAA", SAT, 10.0), raw("AAA", FRI, 10.2)])
    _run(engine)
    assert [f["obs_date"] for f in engine.fills] == [FRI]


def test_sources_that_disagree_make_the_day_ambiguous_and_it_is_withheld():
    engine = _Engine([("AAA", "DEAD")], [
        raw("AAA", THU, 100.0, "TIINGO"), raw("AAA", THU, 50.0, "yfinance"),      # a split basis apart
        raw("AAA", FRI, 101.0, "TIINGO"), raw("AAA", FRI, 101.0, "yfinance"),
    ])
    receipt = _run(engine)
    assert [(f["obs_date"], f["close"], f["source"]) for f in engine.fills] == [
        (FRI, 101.0, "stored_close_backfill:TIINGO+yfinance")]
    assert receipt["ambiguous_days_withheld"] == 1
    assert receipt["by_source"] == {"TIINGO+yfinance": 1}


def test_a_newer_disagreeing_vintage_does_not_win_on_recency():
    engine = _Engine([("AAA", "DEAD")], [
        raw("AAA", FRI, 101.0, "TIINGO"),
        raw("AAA", FRI, 12.0, "TIINGO", pulled=_after_close(MON)),               # later, very different
    ])
    receipt = _run(engine)
    assert engine.fills == [] and receipt["ambiguous_days_withheld"] == 1
    assert receipt["no_completed_close"] == ["AAA"]


def test_a_tiingo_row_after_the_tickers_last_session_is_another_instrument():
    from tiingo_symbol_contract import tiingo_cutoff

    cutoff = tiingo_cutoff("YF:LXP:close")          # delisted in the lifecycle sidecar
    assert cutoff is not None and cutoff < THU
    engine = _Engine([("LXP", "DEAD")], [raw("LXP", THU, 9.99, "TIINGO"), raw("LXP", FRI, 9.99, "TIINGO")])
    receipt = _run(engine)
    assert engine.fills == [] and receipt["no_completed_close"] == ["LXP"]


@pytest.mark.parametrize("payload", [
    {"capture_state": "PROVISIONAL"},
    json.dumps({"capture_state": "REJECTED"}),
    {"price_basis": "YF:AAA:adj_close"},
])
def test_a_row_its_payload_marks_as_not_this_close_is_dropped(payload):
    engine = _Engine([("AAA", "DEAD")], [raw("AAA", FRI, 10.0, payload=payload)])
    _run(engine)
    assert engine.fills == []


def test_rows_labelled_eligible_or_unlabelled_are_kept_and_bad_values_dropped():
    engine = _Engine([("AAA", "DEAD")], [
        raw("AAA", THU, 10.0, payload={"capture_state": "POLICY_ELIGIBLE", "price_basis": "YF:AAA:close"}),
        raw("AAA", FRI, 0.0), raw("AAA", MON, float("nan")),
    ])
    _run(engine)
    assert [f["obs_date"] for f in engine.fills] == [THU]


@pytest.mark.parametrize("ticker", ["BTC-USD", "GLEN.L", "EURUSD=X", "^VIX", "vfiax"])
def test_an_unsupported_instrument_is_withheld_without_reading_prices(ticker):
    engine = _Engine([(ticker, "DEAD")], [raw(ticker, FRI, 10.0)])
    receipt = _run(engine)
    assert receipt["unsupported"] == [ticker] and engine.fills == []
    assert not any("FROM raw_series" in s for s, _p in engine.statements)
    with pytest.raises(ValueError, match="not a US-listed ticker"):
        mod.eligible_closes(None, ticker, THU, FRI, EVALUATED_AT)


def test_nothing_pulled_after_the_evaluation_instant_is_used():
    engine = _Engine([("AAA", "DEAD")], [raw("AAA", FRI, 10.0, pulled=datetime(2026, 11, 24, 2, 0, tzinfo=timezone.utc))])
    _run(engine)
    assert engine.fills == []


# ── what is written ─────────────────────────────────────────────────────────

def test_the_write_only_fills_a_missing_close():
    statement = " ".join(str(mod._FILL_SQL).split())
    assert statement.endswith(
        "WHERE ticker_metrics_daily.close_price IS NULL AND ticker_metrics_daily.shares_outstanding IS NULL")
    update = statement.split("DO UPDATE SET", 1)[1].split("WHERE", 1)[0]
    assert "shares_outstanding" not in update and "market_cap_usd" not in update
    assert "IS DISTINCT FROM" not in statement          # an existing close is never compared and replaced
    assert "(ticker, obs_date, close_price, source, as_of)" in statement


def test_rows_the_fill_leaves_alone_are_counted_not_failed():
    """rowcount 0: a close is already there (whoever wrote it), or sec_xbrl_shares owns the row."""
    engine = _Engine([("AAA", "STALE_30+")], [raw("AAA", THU, 10.0), raw("AAA", FRI, 11.0)],
                     rowcount=lambda p: 0 if p["obs_date"] == THU else 1)
    receipt = _run(engine)
    assert (receipt["eligible_tickers"], receipt["written"], receipt["left_as_is"]) == (1, 1, 1)
    assert receipt["failed"] == []


def test_windows_by_bucket_and_the_work_query():
    engine = _Engine([("AAA", "DEAD"), ("BBB", "STALE_7_30")])
    _run(engine)
    statement, params = engine.statements[0]
    assert "FROM data_freshness_audit" in statement and "MAX(audited_at)" in statement
    assert params == {"buckets": ["DEAD", "STALE_30+", "STALE_7_30"]}
    starts = {p["sid"].split(":")[1]: p["start"] for s, p in engine.statements if "FROM raw_series" in s}
    assert starts == {"AAA": date(2025, 11, 24), "BBB": date(2026, 9, 25)}       # 365 and 60 days back


def test_dry_run_measures_coverage_and_never_opens_a_write(capsys):
    engine = _Engine([("AAA", "DEAD"), ("GLEN", "DEAD"), ("BTC-USD", "DEAD")],
                     [raw("AAA", THU, 10.0), raw("AAA", FRI, 11.0)])
    receipt = _run(engine, dry_run=True)
    assert engine.begins == 0 and engine.fills == []
    assert receipt["eligible_tickers"] == 1 and receipt["eligible_closes"] == 2
    assert receipt["written"] == 0 and receipt["left_as_is"] == 0
    assert receipt["no_completed_close"] == ["GLEN"] and receipt["unsupported"] == ["BTC-USD"]
    out = capsys.readouterr().out
    line = [ln for ln in out.splitlines() if ln.startswith("RECEIPT ")][-1]
    assert json.loads(line[len("RECEIPT "):]) == receipt          # machine-readable, the same numbers


def test_one_failed_write_does_not_stop_the_rest(capsys):
    engine = _Engine([("AAA", "DEAD"), ("BBB", "DEAD")], [raw("AAA", FRI, 11.0), raw("BBB", FRI, 5.5)],
                     fail_writes=("AAA",))
    receipt = _run(engine)
    assert receipt["failed"] == ["AAA"] and receipt["eligible_tickers"] == 1
    assert [f["ticker"] for f in engine.fills] == ["BBB"]
    assert "AAA: FAILED (RuntimeError" in capsys.readouterr().err


def test_gem_run_takes_tickers_without_a_recent_close_and_a_60_day_window():
    engine = _Engine([("FLUT",)], [raw("FLUT", FRI, 250.0)])
    receipt = _run(engine, "gem")
    statement, params = engine.statements[0]
    assert "FROM gem_alerts" in statement and "NOT EXISTS" in statement
    assert params == {"fresh_since": date(2026, 11, 17)}
    assert [p["start"] for s, p in engine.statements if "FROM raw_series" in s][0] == date(2026, 9, 25)
    assert receipt["eligible_tickers"] == 1 and engine.fills[0]["ticker"] == "FLUT"


def test_unknown_mode_and_a_naive_clock_are_refused():
    with pytest.raises(ValueError, match="unknown mode"):
        mod.run(_Engine([]), "twelvedata")
    with pytest.raises(ValueError, match="timezone-aware"):
        mod.run(_Engine([]), "universe", evaluated_at=datetime(2026, 11, 24))
    assert mod.main(["twelvedata"]) == 2


# ── exit code ───────────────────────────────────────────────────────────────

def _main(monkeypatch, engine, argv, env=None):
    import db

    monkeypatch.setattr(db, "get_engine", lambda: engine)
    for key in ("DRY_RUN", "ONLY_BUCKET", "LIMIT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)
    return mod.main(argv)


def test_exit_code_is_zero_when_nothing_has_an_eligible_close(monkeypatch):
    assert _main(monkeypatch, _Engine([("GLEN", "DEAD"), ("US", "DEAD")]), ["universe"]) == 0


def test_exit_code_is_one_only_when_writes_failed_and_nothing_was_filled(monkeypatch, capsys):
    rows = [("AAA", "DEAD"), ("BBB", "DEAD")]
    prices = [raw("AAA", THU, 11.0), raw("BBB", THU, 5.5)]
    assert _main(monkeypatch, _Engine(rows, prices, fail_writes=("AAA", "BBB")), ["universe"]) == 1
    assert "total failure: 0/2 attempted tickers were filled" in capsys.readouterr().err
    assert _main(monkeypatch, _Engine(rows, prices, fail_writes=("AAA",)), ["universe"]) == 0


def test_environment_switches_reach_the_run(monkeypatch):
    engine = _Engine([("AAA", "DEAD"), ("BBB", "DEAD")], [raw("AAA", THU, 11.0)])
    assert _main(monkeypatch, engine, ["universe"], {"DRY_RUN": "1", "ONLY_BUCKET": "DEAD", "LIMIT": "1"}) == 0
    assert engine.statements[0][1] == {"buckets": ["DEAD"]} and engine.begins == 0
    assert len({p["sid"] for s, p in engine.statements if "FROM raw_series" in s}) == 1


# ── what the script is and is not ───────────────────────────────────────────

def test_no_twelve_data_call_key_or_credential_in_the_new_script():
    source = (ROOT / "scripts" / "close_backfill_universe.py").read_text(encoding="utf-8")
    code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
    code = re.sub(r'""".*?"""', "", code, flags=re.S)       # the docstring names the old provider
    for needle in ("twelvedata", "TWELVEDATA", "apikey", "requests", "psycopg2", "password"):
        assert needle not in code, needle


def test_closes_are_read_through_the_completed_close_reader_only():
    source = (ROOT / "scripts" / "close_backfill_universe.py").read_text(encoding="utf-8")
    assert "from store.observations import read_completed_closes" in source
    assert "boundary=us_listed_close_boundary" in source
    assert not re.search(r"from\s+raw_series\b", source, re.I)


def test_only_the_universe_unit_is_cut_over():
    """The universe entry point delegates to this script; the gem cohort's
    coverage is not yet measured good enough, so its entry point is still
    Twelve Data (cutover receipts: the universe-cutover PR)."""
    universe = (ROOT / "scripts" / "td_backfill_universe.py").read_text(encoding="utf-8")
    assert "close_backfill_universe" in universe and "api.twelvedata.com" not in universe
    gem = (ROOT / "scripts" / "td_backfill_gem_tickers.py").read_text(encoding="utf-8")
    assert "api.twelvedata.com" in gem and "close_backfill_universe" not in gem


# ── price basis: never two bases in one series ──────────────────────────────

def test_a_stored_close_on_another_basis_withholds_the_ticker_and_says_so(capsys):
    """SLQT / SUPN after a reverse split: a split-adjusted close of 6.08 sits in
    the table where the stored raw close is 0.30. Filling the days around it
    would put both bases in one series."""
    engine = _Engine([("SLQT", "STALE_30+"), ("CCC", "STALE_30+")],
                     [raw("SLQT", THU, 0.30), raw("SLQT", FRI, 0.31), raw("CCC", FRI, 7.0)],
                     existing=[("SLQT", THU, 6.08)])
    receipt = _run(engine)
    assert receipt["basis_conflict"] == ["SLQT"] and receipt["eligible_tickers"] == 1
    assert [f["ticker"] for f in engine.fills] == ["CCC"]
    assert "SLQT: withheld, 1 stored closes on another price basis" in capsys.readouterr().out


def test_two_decimal_rounding_of_a_sub_dollar_price_is_not_a_basis_conflict():
    engine = _Engine([("EYPT", "DEAD")], [raw("EYPT", THU, 0.2342), raw("EYPT", FRI, 0.2301)],
                     existing=[("EYPT", THU, 0.23), ("EYPT", FRI, 0.23)])
    receipt = _run(engine)
    assert receipt["basis_conflict"] == [] and receipt["eligible_tickers"] == 1


def test_basis_conflict_is_reported_in_a_dry_run_too_and_nothing_is_written():
    engine = _Engine([("SUPN", "DEAD")], [raw("SUPN", THU, 34.77)], existing=[("SUPN", THU, 41.23)])
    receipt = _run(engine, dry_run=True)
    assert receipt["basis_conflict"] == ["SUPN"] and engine.begins == 0 and receipt["eligible_closes"] == 0


def test_basis_conflicts_helper_ignores_days_without_a_pair_and_zero_prices():
    closes = [(THU, 10.0, "TIINGO")]
    assert mod.basis_conflicts([(FRI, 99.0), (THU, 0)], closes) == []
    assert mod.basis_conflicts([(THU, 10.05)], closes) == []            # 0.5% apart: rounding
    assert mod.basis_conflicts([(THU, 20.0)], closes) == [THU]
