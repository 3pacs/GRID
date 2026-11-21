"""SEC 8-K velocity: sector SIC from edgar_submissions, SEC Company.sic on a miss.

edgartools' Filing (5.62.1 and 6.3.0) has no ``sic`` attribute, so every 8-K
landed in SEC_VELOCITY:UNKNOWN. These tests pin the replacement:
- the local table is read once per load, with bound SQL;
- a miss goes to the capped, cached SEC fallback;
- errors stay UNKNOWN and never abort the run.
All offline: get_filings, Company and the engine are stubbed.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ingestion import sec_velocity

TODAY = date.today()


class FakeEngine:
    """Counts every execute on connect() (SIC read) and begin() (writes)."""

    def __init__(self, local_rows=None, local_error: Exception | None = None):
        self.local_rows = local_rows or []
        self.local_error = local_error
        self.read_calls: list[tuple[str, dict]] = []
        self.write_calls: list[tuple[str, dict]] = []

    def connect(self):
        engine = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, stmt, params=None):
                engine.read_calls.append((str(stmt), dict(params or {})))
                if engine.local_error is not None:
                    raise engine.local_error
                return SimpleNamespace(fetchall=lambda: list(engine.local_rows))

        return _Conn()

    def begin(self):
        engine = self

        class _Tx:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, stmt, params=None):
                engine.write_calls.append((str(stmt), dict(params or {})))
                return SimpleNamespace(fetchone=lambda: None)

        return _Tx()


def _puller(monkeypatch, engine) -> sec_velocity.SECVelocityPuller:
    monkeypatch.setattr(sec_velocity, "set_identity", lambda ident: None)
    monkeypatch.setattr(
        sec_velocity.BasePuller, "__init__",
        lambda self, eng: (setattr(self, "source_id", 7), setattr(self, "engine", eng)),
    )
    monkeypatch.setattr(sec_velocity.time, "sleep", lambda s: None)
    puller = sec_velocity.SECVelocityPuller(db_engine=engine)
    monkeypatch.setattr(puller, "_ensure_feature_registered", lambda sector: None)
    return puller


def _filings(*ciks: int, filed: date = TODAY) -> list[SimpleNamespace]:
    return [SimpleNamespace(cik=c, filing_date=filed) for c in ciks]


def _company_stub(monkeypatch, sics: dict[int, object]) -> list[int]:
    """Stub sec_velocity.Company; a value that is an Exception raises on .sic."""
    calls: list[int] = []

    class _Company:
        def __init__(self, cik):
            calls.append(cik)
            self._cik = cik

        @property
        def sic(self):
            val = sics.get(self._cik)
            if isinstance(val, Exception):
                raise val
            return val

    monkeypatch.setattr(sec_velocity, "Company", _Company)
    return calls


def _inserted(engine: FakeEngine) -> dict[str, tuple[float, dict]]:
    return {
        p["sid"]: (p["val"], json.loads(p["payload"]))
        for sql, p in engine.write_calls
        if "INSERT INTO raw_series" in sql
    }


def _weekly(monkeypatch, engine, filings):
    monkeypatch.setattr(sec_velocity, "get_filings", lambda **kw: filings)
    return _puller(monkeypatch, engine).pull_weekly_velocity(weeks_back=1)


# ── the library contract ─────────────────────────────────────────────────────


def test_installed_edgartools_has_company_sic_and_no_filing_sic():
    edgar = pytest.importorskip("edgar")
    from edgar._filings import Filing

    assert isinstance(getattr(edgar.Company, "sic", None), property)
    assert not hasattr(Filing, "sic")


def test_local_sic_sql_is_bound_and_point_in_time():
    sql = str(sec_velocity._LOCAL_SIC_SQL)
    assert ":as_of" in sql and "filed <= :as_of" in sql
    assert "edgar_submissions" in sql


# ── weekly path ──────────────────────────────────────────────────────────────


def test_local_table_hit_needs_no_sec_lookup(monkeypatch):
    engine = FakeEngine(local_rows=[(320193, 3571), (70858, 6021)])
    calls = _company_stub(monkeypatch, {})
    out = _weekly(monkeypatch, engine, _filings(320193, 320193, 70858))

    assert calls == []
    rows = _inserted(engine)
    assert rows["SEC_VELOCITY:MANUFACTURING"][0] == 2.0
    assert rows["SEC_VELOCITY:FIRE"][0] == 1.0
    assert rows["SEC_VELOCITY:MANUFACTURING"][1]["sic_sources"] == {"local": 2}
    assert out["errors"] == [] and "SEC_VELOCITY:UNKNOWN" not in rows
    assert engine.read_calls[0][1] == {"as_of": TODAY}


def test_miss_goes_to_sec_fallback_once_per_cik(monkeypatch):
    engine = FakeEngine(local_rows=[(320193, 3571)])
    calls = _company_stub(monkeypatch, {1999001: "7372"})
    _weekly(monkeypatch, engine, _filings(1999001, 1999001, 320193))

    assert calls == [1999001]  # cached for the run
    rows = _inserted(engine)
    assert rows["SEC_VELOCITY:SERVICES"][0] == 2.0
    assert rows["SEC_VELOCITY:SERVICES"][1]["sic_sources"] == {"sec": 2}


def test_fallback_cap_leaves_the_rest_unknown(monkeypatch):
    monkeypatch.setattr(sec_velocity, "SIC_FALLBACK_MAX_LOOKUPS", 2)
    warnings: list[str] = []
    monkeypatch.setattr(sec_velocity.log, "warning", lambda msg, **kw: warnings.append(msg))
    engine = FakeEngine()
    calls = _company_stub(monkeypatch, {1: "6021", 2: "6021", 3: "6021", 4: "6021"})
    out = _weekly(monkeypatch, engine, _filings(1, 2, 3, 4))

    assert calls == [1, 2]
    rows = _inserted(engine)
    assert rows["SEC_VELOCITY:FIRE"][0] == 2.0
    assert rows["SEC_VELOCITY:UNKNOWN"][0] == 2.0
    assert rows["SEC_VELOCITY:UNKNOWN"][1]["sic_sources"] == {"unknown": 2}
    assert sum("cap" in w for w in warnings) == 1  # logged once
    assert out["errors"] == []


def test_fallback_error_stays_unknown_and_run_completes(monkeypatch):
    engine = FakeEngine()
    calls = _company_stub(monkeypatch, {5: ConnectionError("SEC down"), 6: "1311"})
    out = _weekly(monkeypatch, engine, _filings(5, 6))

    assert calls == [5, 6]
    rows = _inserted(engine)
    assert rows["SEC_VELOCITY:UNKNOWN"][0] == 1.0
    assert rows["SEC_VELOCITY:MINING"][0] == 1.0
    assert out["errors"] == [] and out["status"] == "SUCCESS"


def test_consecutive_errors_stop_the_fallback(monkeypatch):
    engine = FakeEngine()
    calls = _company_stub(monkeypatch, {c: TimeoutError("slow") for c in range(10, 20)})
    out = _weekly(monkeypatch, engine, _filings(*range(10, 20)))

    assert len(calls) == sec_velocity.SIC_FALLBACK_MAX_CONSECUTIVE_FAILURES
    assert _inserted(engine)["SEC_VELOCITY:UNKNOWN"][0] == 10.0
    assert out["errors"] == []


def test_local_table_failure_falls_back_without_aborting(monkeypatch):
    engine = FakeEngine(local_error=RuntimeError('relation "edgar_submissions" does not exist'))
    calls = _company_stub(monkeypatch, {42: "4911"})
    out = _weekly(monkeypatch, engine, _filings(42))

    assert calls == [42]
    assert _inserted(engine)["SEC_VELOCITY:TRANSPORT_UTIL"][0] == 1.0
    assert out["errors"] == []


def test_filing_without_cik_is_unknown(monkeypatch):
    engine = FakeEngine()
    calls = _company_stub(monkeypatch, {})
    _weekly(monkeypatch, engine, [SimpleNamespace(filing_date=TODAY)])
    assert calls == []
    assert _inserted(engine)["SEC_VELOCITY:UNKNOWN"][0] == 1.0


def test_no_db_query_per_filing(monkeypatch):
    engine = FakeEngine(local_rows=[(c, 3571) for c in range(100, 150)])
    _company_stub(monkeypatch, {})
    _weekly(monkeypatch, engine, _filings(*range(100, 150)))

    assert len(engine.read_calls) == 1  # one SIC read for 50 filings
    # Writes are per sector-week (one existence check plus one insert), not per filing.
    assert len(engine.write_calls) == 2


# ── historical path ──────────────────────────────────────────────────────────


def test_historical_reads_local_table_once_per_year_as_of_year_end(monkeypatch):
    engine = FakeEngine(local_rows=[(320193, 3571)])
    calls = _company_stub(monkeypatch, {7: "6021"})  # would resolve, if asked

    def fake_get_filings(form, filing_date):
        year = int(filing_date[:4])
        return _filings(320193, 7, 7, filed=date(year, 3, 4))

    monkeypatch.setattr(sec_velocity, "get_filings", fake_get_filings)
    out = _puller(monkeypatch, engine).pull_historical_velocity(start_year=2018, end_year=2019)

    assert [p for _sql, p in engine.read_calls] == [
        {"as_of": date(2018, 12, 31)}, {"as_of": date(2019, 12, 31)},
    ]
    assert calls == []  # no SEC fallback in the backfill (lookahead)
    assert out["errors"] == []
    inserted = [p for sql, p in engine.write_calls if "INSERT INTO raw_series" in sql]
    assert {p["sid"] for p in inserted} == {"SEC_VELOCITY:MANUFACTURING", "SEC_VELOCITY:UNKNOWN"}
    assert len(inserted) == 4  # 2 sectors x 2 years
    unknown = [p for p in inserted if p["sid"] == "SEC_VELOCITY:UNKNOWN"]
    assert all(p["val"] == 2.0 for p in unknown)
    assert all(json.loads(p["payload"])["sic_sources"] == {"unknown": 2} for p in unknown)


def test_historical_backfill_makes_zero_company_lookups(monkeypatch):
    engine = FakeEngine()  # empty local table: every filing is a miss
    calls = _company_stub(monkeypatch, {c: "6021" for c in range(1, 31)})
    monkeypatch.setattr(
        sec_velocity, "get_filings",
        lambda form, filing_date: _filings(*range(1, 31), filed=date(int(filing_date[:4]), 6, 1)),
    )
    out = _puller(monkeypatch, engine).pull_historical_velocity(start_year=2005, end_year=2007)

    assert calls == []
    assert out["errors"] == []
    inserted = [p for sql, p in engine.write_calls if "INSERT INTO raw_series" in sql]
    assert {p["sid"] for p in inserted} == {"SEC_VELOCITY:UNKNOWN"}
    assert len(engine.read_calls) == 3  # one local read per year


def test_weekly_still_uses_the_fallback():
    resolver = sec_velocity.SICResolver(
        FakeEngine(), company_factory=lambda cik: SimpleNamespace(sic="6021")
    )
    assert resolver.allow_fallback is True
    assert resolver.resolve(SimpleNamespace(cik=9)) == ("6021", "sec")


@pytest.mark.parametrize(
    "raw,expected",
    [(3571, "3571"), ("3571", "3571"), ("6021.0", "6021"), ("", None), (None, None),
     ("0000", None), ("n/a", None)],
)
def test_normalize_sic(raw, expected):
    assert sec_velocity._normalize_sic(raw) == expected


def test_resolver_with_magicmock_engine_does_not_crash():
    # Callers elsewhere build pullers on MagicMock engines; an unconfigured
    # read must degrade to an empty table, not raise.
    resolver = sec_velocity.SICResolver(MagicMock(), company_factory=lambda cik: None)
    resolver.load_local(as_of=TODAY - timedelta(days=1))
    assert resolver.resolve(SimpleNamespace(cik=1)) == (None, "unknown")
