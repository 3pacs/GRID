"""``grid/signals/sponsor_resolver.py`` — layered sponsor → ticker resolution.

No network, no DB: the SEC payload is injected, the sector map is patched,
the persistent cache is a fake engine that records every statement, and the
local LLM is a stub installed on ``llm.router.get_llm``.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import grid.signals.sponsor_resolver as sr

ROOT = Path(__file__).resolve().parents[1]

_SEC_PAYLOAD = {
    "0": {"cik_str": 1682852, "ticker": "MRNA", "title": "Moderna, Inc."},
    "1": {"cik_str": 59478, "ticker": "LLY", "title": "ELI LILLY & Co"},
    "2": {"cik_str": 1114448, "ticker": "NVS", "title": "NOVARTIS AG"},
    "3": {"cik_str": 1603978, "ticker": "CELC", "title": "Celcuity Inc."},
    "4": {"cik_str": 5, "ticker": "ONCT", "title": "Oncternal Therapeutics, Inc."},
    "5": {"cik_str": 6, "ticker": "ONCY", "title": "ONCOLYTICS BIOTECH INC"},
    "6": {"cik_str": 2, "ticker": "GOOGL", "title": "Alphabet Inc."},
    "7": {"cik_str": 3, "ticker": "GOOG", "title": "Alphabet Inc."},
}


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, stmt: Any, params: Any = None) -> MagicMock:
        sql = str(stmt)
        self._engine.calls.append((sql, params or {}))
        result = MagicMock()
        if "FROM sponsor_ticker_map" in sql:
            result.first.return_value = self._engine.cache_rows.get((params or {}).get("norm"))
        elif "FROM company_profiles" in sql:
            result.fetchall.return_value = self._engine.profile_rows
        else:
            result.first.return_value = None
            result.fetchall.return_value = []
        return result


class _FakeEngine:
    """Records statements; serves cache rows keyed by sponsor_norm."""

    def __init__(self, cache_rows: dict[str, tuple] | None = None, profile_rows: list[tuple] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.cache_rows = cache_rows or {}
        self.profile_rows = profile_rows or []

    def connect(self) -> _FakeConn:
        return _FakeConn(self)

    def begin(self) -> _FakeConn:
        return _FakeConn(self)

    def writes(self) -> list[dict[str, Any]]:
        return [p for sql, p in self.calls if "INSERT INTO sponsor_ticker_map" in sql]


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch):
    sr.clear_caches()
    sr._SEC_LOADED = True  # never hit sec.gov
    sr._ingest_sec_payload(_SEC_PAYLOAD)
    monkeypatch.setattr(sr, "_sector_name_map", lambda: {"biox therapeutics": "BIOX"})
    yield
    sr.clear_caches()


# ── normalisation / hard reject ───────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Moderna, Inc.", "moderna"),
        ("ELI LILLY & Co", "eli lilly"),
        ("Eli Lilly and Company", "eli lilly"),
        ("Hoffmann-La Roche, Ltd.", "hoffmann la roche"),
        ("Janssen Research & Development, LLC", "janssen research development"),
        ("Acme Pharma Holdings Inc", "acme pharma"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_sponsor_name(raw: Any, expected: str) -> None:
    assert sr.normalize_sponsor_name(raw) == expected


@pytest.mark.parametrize(
    "name",
    [
        "University of Somewhere", "M.D. Anderson Cancer Center", "National Cancer Institute (NCI)",
        "Children's Hospital of Philadelphia", "Mayo Clinic", "Institut Pasteur", "Fondation ARC",
        "Hospices Civils de Lyon", "NIH Clinical Center", "Dana-Farber Cancer Institute",
        "Assistance Publique - Hôpitaux de Paris", "Bill & Melinda Gates Foundation",
    ],
)
def test_non_industry_names_are_rejected(name: str) -> None:
    assert sr.is_non_industry_name(name)
    out = sr.resolve_sponsor(None, name, "INDUSTRY")
    assert out.ticker is None and out.reason == "non_industry" and out.source == "non_industry"


@pytest.mark.parametrize("name", ["Moderna, Inc.", "Clinical Trials Co Inc", "Oncternal Therapeutics"])
def test_industry_names_pass_the_name_filter(name: str) -> None:
    assert not sr.is_non_industry_name(name)


def test_non_industry_class_rejected_even_with_a_listed_name() -> None:
    engine = _FakeEngine()
    out = sr.resolve_sponsor(engine, "Moderna, Inc.", "OTHER")
    assert out.ticker is None and out.reason == "non_industry"
    # negative result persisted with its reason
    assert engine.writes() == [
        {"norm": "moderna", "ticker": None, "source": "non_industry", "confidence": 1.0, "notes": "non_industry"}
    ]
    assert sr.is_industry_class("industry") is True
    assert sr.is_industry_class("NIH") is False
    assert sr.is_industry_class(None) is None


# ── SEC layer ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,ticker,source",
    [
        ("Moderna, Inc.", "MRNA", "sec_exact"),
        ("Eli Lilly and Company", "LLY", "sec_normalized"),
        ("Celcuity", "CELC", "sec_normalized"),
        ("Novartis Pharmaceuticals", "NVS", "sec_fuzzy"),      # subsidiary label -> listed parent
        ("Oncternal", "ONCT", "sec_normalized"),               # "{name} therapeutics" variant
        ("Oncolytics Biotech Canada", "ONCY", "sec_fuzzy"),    # SEC name is a whole-word prefix of the sponsor
    ],
)
def test_sec_exact_normalized_and_fuzzy(name: str, ticker: str, source: str) -> None:
    out = sr.resolve_sponsor(None, name, "INDUSTRY", use_llm=False)
    assert (out.ticker, out.source) == (ticker, source)
    assert 0.6 <= out.confidence <= 0.95
    assert sr.resolve_ticker_sec(name) == ticker


def test_sec_ambiguous_prefix_refuses_to_guess() -> None:
    # "onc" is too short for prefix matching at all; "alphabet" maps to one ticker (first class listed)
    assert sr.resolve_ticker_sec("Onc") is None
    assert sr.resolve_ticker_sec("Alphabet") == "GOOGL"
    assert sr.sec_cik_for_ticker("mrna") == "0001682852"
    assert "ONCY" in sr.sec_ticker_set()


def test_sec_unavailable_degrades_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    sr.clear_caches()
    sr._SEC_LOADED = True  # download "failed" -> empty maps
    out = sr.resolve_sponsor(None, "Moderna, Inc.", "INDUSTRY", use_llm=False)
    assert out.ticker is None and out.reason == "unresolved"


# ── GRID name maps ────────────────────────────────────────────────────────


def test_sector_map_whole_name_match() -> None:
    out = sr.resolve_sponsor(None, "BioX Therapeutics, Inc.", "INDUSTRY", use_llm=False)
    assert (out.ticker, out.source, out.confidence) == ("BIOX", "sector_map", 0.85)


def test_company_profiles_name_match_reads_parameterised_names() -> None:
    engine = _FakeEngine(profile_rows=[("SMLX", "Smallex Bio Inc"), ("", "junk")])
    out = sr.resolve_sponsor(engine, "Smallex Bio", "INDUSTRY", use_llm=False)
    assert (out.ticker, out.source) == ("SMLX", "company_profiles")
    assert any("FROM company_profiles" in sql for sql, _ in engine.calls)


# ── persistent cache ──────────────────────────────────────────────────────


def test_cache_hit_avoids_recompute(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(timezone.utc)
    engine = _FakeEngine(cache_rows={"moderna": ("MRNA", "sec_exact", 0.95, now, "Moderna, Inc.")})
    sec_layer = MagicMock(side_effect=AssertionError("SEC layer must not run on a cache hit"))
    monkeypatch.setattr(sr, "_resolve_sec_layer", sec_layer)
    out = sr.resolve_sponsor(engine, "Moderna, Inc.", "INDUSTRY")
    assert (out.ticker, out.source, out.confidence) == ("MRNA", "cache:sec_exact", 0.95)
    sec_layer.assert_not_called()
    assert engine.writes() == []  # nothing re-written
    reads = [p for sql, p in engine.calls if "FROM sponsor_ticker_map" in sql]
    assert reads == [{"norm": "moderna"}]
    # second call is served from the in-process memo: no further DB reads
    sr.resolve_sponsor(engine, "Moderna, Inc.", "INDUSTRY")
    assert len([1 for sql, _ in engine.calls if "FROM sponsor_ticker_map" in sql]) == 1


def test_resolution_is_written_to_cache_with_source_and_confidence() -> None:
    engine = _FakeEngine()
    out = sr.resolve_sponsor(engine, "Moderna, Inc.", "INDUSTRY", use_llm=False)
    assert out.ticker == "MRNA"
    assert any("CREATE TABLE IF NOT EXISTS sponsor_ticker_map" in sql for sql, _ in engine.calls)
    assert engine.writes() == [
        {"norm": "moderna", "ticker": "MRNA", "source": "sec_exact", "confidence": 0.95, "notes": "Moderna, Inc."}
    ]


def test_stale_negative_cache_row_is_recomputed_but_non_industry_is_not() -> None:
    old = datetime.now(timezone.utc) - timedelta(days=sr.NEGATIVE_TTL_DAYS + 1)
    engine = _FakeEngine(cache_rows={
        "moderna": (None, "unresolved", 0.0, old, "unresolved"),
        "acme bio": (None, "non_industry", 1.0, old, "non_industry"),
    })
    fresh = sr.resolve_sponsor(engine, "Moderna, Inc.", "INDUSTRY", use_llm=False)
    assert fresh.ticker == "MRNA"  # stale negative -> recomputed via SEC
    kept = sr.resolve_sponsor(engine, "Acme Bio", "INDUSTRY", use_llm=False)
    assert kept.ticker is None and kept.source == "cache:non_industry"


# ── local LLM last resort ─────────────────────────────────────────────────


class _Client:
    is_available = True
    _health_provider = "llamacpp"

    def __init__(self, answer: Any) -> None:
        self.answer = answer
        self.calls: list[Any] = []

    def chat(self, messages: list[dict[str, str]], **kwargs: Any) -> Any:
        self.calls.append((messages, kwargs))
        return self.answer


def _install_llm(monkeypatch: pytest.MonkeyPatch, client: Any) -> None:
    import llm.router as router

    monkeypatch.setattr(router, "get_llm", lambda tier=None, provider=None: client)


def test_llm_answer_accepted_only_when_in_sec_set(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _Client("The ticker is ONCT.")
    _install_llm(monkeypatch, client)
    out = sr.resolve_sponsor(None, "Oncternal Global Research", "INDUSTRY")
    assert (out.ticker, out.source, out.confidence) == ("ONCT", "llm_local", 0.6)
    assert "Oncternal Global Research" in client.calls[0][0][0]["content"]
    assert "Answer with the ticker only, or NONE" in client.calls[0][0][0]["content"]

    sr.clear_caches(); sr._SEC_LOADED = True; sr._ingest_sec_payload(_SEC_PAYLOAD)
    _install_llm(monkeypatch, _Client("RHHBY"))  # not in the SEC set -> rejected
    out = sr.resolve_sponsor(None, "Hoffmann-La Roche", "INDUSTRY")
    assert out.ticker is None and out.reason == "llm_not_in_sec" and out.confidence == 0.0


def test_llm_none_and_NONE_are_handled(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_llm(monkeypatch, _Client(None))
    out = sr.resolve_sponsor(None, "Boehringer Ingelheim", "INDUSTRY")
    assert out.ticker is None and out.reason == "llm_none"
    sr.clear_caches(); sr._SEC_LOADED = True; sr._ingest_sec_payload(_SEC_PAYLOAD)
    _install_llm(monkeypatch, _Client("NONE"))
    out = sr.resolve_sponsor(None, "Boehringer Ingelheim", "INDUSTRY")
    assert out.ticker is None and out.reason == "llm_none"


def test_llm_not_called_for_unknown_class_or_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _Client("MRNA")
    _install_llm(monkeypatch, client)
    out = sr.resolve_sponsor(None, "Boehringer Ingelheim", None)  # class unknown -> no LLM
    assert out.ticker is None and out.reason == "unresolved" and client.calls == []
    sr.clear_caches(); sr._SEC_LOADED = True; sr._ingest_sec_payload(_SEC_PAYLOAD)
    out = sr.resolve_sponsor(None, "Boehringer Ingelheim", "INDUSTRY", use_llm=False)
    assert out.ticker is None and client.calls == []


def test_paid_llm_client_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _Client("MRNA")
    client._health_provider = "openrouter"
    _install_llm(monkeypatch, client)
    out = sr.resolve_sponsor(None, "Boehringer Ingelheim", "INDUSTRY")
    assert out.ticker is None and out.reason == "llm_paid_refused" and client.calls == []


def test_llm_router_failure_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    import llm.router as router

    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("no provider")

    monkeypatch.setattr(router, "get_llm", boom)
    out = sr.resolve_sponsor(None, "Boehringer Ingelheim", "INDUSTRY")
    assert out.ticker is None and out.reason == "llm_unavailable"


# ── resolve_many / SQL hygiene ────────────────────────────────────────────


def test_resolve_many_dedupes_and_keys_by_raw_name() -> None:
    out = sr.resolve_many(None, [("Moderna, Inc.", "INDUSTRY"), ("Moderna, Inc.", "INDUSTRY"),
                                 ("University of X", "OTHER"), ("", None)], use_llm=False)
    assert set(out) == {"Moderna, Inc.", "University of X"}
    assert out["Moderna, Inc."].ticker == "MRNA" and out["University of X"].ticker is None


def test_module_sql_is_parameterised() -> None:
    src = Path(sr.__file__).read_text(encoding="utf-8")
    assert 'f"""' not in src and "f'''" not in src and ".format(" not in src.replace("LLM_PROMPT.format", "")
    for name in ("_CACHE_GET_SQL", "_CACHE_PUT_SQL"):
        block = src[src.index(name):]
        block = block[: block.index("\n)\n") + 3]
        assert ":norm" in block, name
    assert re.search(r"text\(\s*\"\"\"\s*INSERT INTO sponsor_ticker_map", src)
    assert "WHERE sponsor_ticker_map.source <> 'curated'" in src  # hand-curated rows are never overwritten


def test_migration_0060_matches_the_module_ddl() -> None:
    sql = (ROOT / "migrations" / "0060_sponsor_ticker_map.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS sponsor_ticker_map" in sql
    for col in ("sponsor_norm", "ticker", "source", "confidence", "resolved_at", "notes"):
        assert col in sql and col in str(sr._ENSURE_TABLE_SQL)
    assert "GRANT ALL ON sponsor_ticker_map TO grid;" in sql


# ── cache table readiness (migration owns the DDL; app role may not) ───────


class _DdlEngine(_FakeEngine):
    """Raises on the DDL statements the app role is not allowed to run."""

    def __init__(self, *, fail_table: bool = False, fail_index: bool = False, fail_select: bool = False) -> None:
        super().__init__()
        self.fail_table, self.fail_index, self.fail_select = fail_table, fail_index, fail_select

    def connect(self) -> "_DdlConn":
        return _DdlConn(self)

    def begin(self) -> "_DdlConn":
        return _DdlConn(self)


class _DdlConn(_FakeConn):
    def execute(self, stmt: Any, params: Any = None) -> MagicMock:
        sql = str(stmt)
        eng = self._engine
        eng.calls.append((sql, params or {}))  # record even when the statement fails
        if "CREATE TABLE" in sql and eng.fail_table:
            raise RuntimeError("permission denied for schema public")
        if "CREATE INDEX" in sql and eng.fail_index:
            raise RuntimeError("must be owner of table sponsor_ticker_map")
        if sql.strip().startswith("SELECT 1 FROM sponsor_ticker_map") and eng.fail_select:
            raise RuntimeError('relation "sponsor_ticker_map" does not exist')
        eng.calls.pop()  # the base class records it again
        return super().execute(stmt, params)


def test_ensure_table_tolerates_index_ownership_error_when_table_is_readable() -> None:
    eng = _DdlEngine(fail_index=True)
    assert sr.ensure_sponsor_map_table(eng) is True
    sqls = [s for s, _ in eng.calls]
    assert any("CREATE TABLE" in s for s in sqls) and any("CREATE INDEX" in s for s in sqls)
    assert any(s.strip().startswith("SELECT 1 FROM sponsor_ticker_map") for s in sqls)
    # Cached per engine: a second call issues no statements.
    n = len(eng.calls)
    assert sr.ensure_sponsor_map_table(eng) is True and len(eng.calls) == n


def test_ensure_table_tolerates_table_ddl_error_too() -> None:
    eng = _DdlEngine(fail_table=True, fail_index=True)
    assert sr.ensure_sponsor_map_table(eng) is True


def test_ensure_table_reports_unusable_when_select_fails_and_does_not_cache() -> None:
    eng = _DdlEngine(fail_table=True, fail_index=True, fail_select=True)
    assert sr.ensure_sponsor_map_table(eng) is False
    assert id(eng) not in sr._TABLE_ENSURED
    # Not cached -> retried next time (e.g. after the migration runs).
    eng.fail_select = False
    assert sr.ensure_sponsor_map_table(eng) is True
