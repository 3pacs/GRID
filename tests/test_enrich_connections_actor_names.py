"""Actor-name hygiene in the connection writers (2026-11-07 actor junk audit).

* ``scripts.enrich_connections._ensure_actor`` no longer overwrites an existing
  name, so curated / security-master names survive the daily run.
* New ``corp_<TICKER>`` actors take the issuer name from ``security_master``
  when exactly one entity holds the ticker today.
* Feed identifiers (``whale_aapl_100``, ``cluster_tsm``, ``gdelt:market``,
  ``2026-06851``) and null-ish tickers (``NONE``, ``N/A``) are skipped by both
  writers instead of becoming actors.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from intelligence import actor_identity as ai
from intelligence import signal_backlinker as sb
from scripts import enrich_connections as ec


@pytest.mark.parametrize("name", [
    "whale_aapl_100", "cluster_tsm", "qq_insider_trading", "corp_INTC",
    "crucix_conflict-energy_nexus_active", "gdelt:market", "2026-06851",
])
def test_feed_identifiers_are_not_actor_names(name: str) -> None:
    assert ai.looks_like_identifier(name)
    assert not sb.is_real_actor(name, "unusual_options")


@pytest.mark.parametrize("name", [
    "Jensen Huang", "NVIDIA Corp", "3M", "BlackRock", "O'Neil", "Smith-Jones",
    "J.P. Morgan", "Berkshire_Hathaway Inc", "",
])
def test_real_names_pass_the_identifier_gate(name: str) -> None:
    assert not ai.looks_like_identifier(name)


def test_placeholder_tickers() -> None:
    assert all(ai.is_placeholder_ticker(t) for t in (None, "", " ", "NONE", "nan", "N/A", " na ", "Null"))
    assert not any(ai.is_placeholder_ticker(t) for t in ("AAPL", "C", "BRK.B", "NAN.L"))


# ── insider pass with a scripted connection ───────────────────────────────


class _Result:
    def __init__(self, rows=None, scalar=None) -> None:
        self._rows, self._scalar = rows or [], scalar

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _ScriptedConn:
    def __init__(self, signal_rows, names) -> None:
        self.signal_rows, self.names = signal_rows, names
        self.actor_upserts: list[dict] = []
        self.connections: list[tuple[str, str]] = []

    def execute(self, stmt, params=None):
        sql = str(getattr(stmt, "text", stmt))
        if "FROM signal_data" in sql:
            return _Result(self.signal_rows)
        if "to_regclass" in sql:
            return _Result(scalar=True)
        if "FROM security_identifiers" in sql:
            return _Result(list(self.names.items()))
        if "INSERT INTO actors" in sql:
            self.actor_upserts.append(dict(params))
        elif "INSERT INTO actor_connections" in sql:
            self.connections.append((params["a"], params["b"]))
        return _Result()


class _Ctx:
    def __init__(self, conn) -> None:
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


class _Engine:
    def __init__(self, conn) -> None:
        self.conn = conn

    def begin(self):
        return _Ctx(self.conn)


def test_insider_pass_skips_non_actors_and_names_corps_from_security_master(monkeypatch) -> None:
    monkeypatch.setattr(ec, "_company_names", None)
    rows = [
        ("Jensen Huang", "NVDA", "sell", 1_000_000, None, "confirmed"),
        ("cluster_tsm", "TSM", "buy", 0, None, None),             # feed id as actor
        ("qq_insider_trading", "AAPL", "buy", 0, None, None),     # feed id as actor
        ("Jane Doe", "NONE", "buy", 0, None, None),               # null-ish ticker
        ("Jane Doe", "zzzz", "buy", 0, None, None),               # ticker unknown to security_master
    ]
    conn = _ScriptedConn(rows, {"NVDA": "NVIDIA CORP"})
    stats = ec.enrich_insider_connections(_Engine(conn))

    assert stats["skipped_non_actor"] == 3
    names = {u["id"]: u["name"] for u in conn.actor_upserts}
    assert names["corp_NVDA"] == "NVIDIA CORP"
    assert names["corp_ZZZZ"] == "ZZZZ Corp"  # placeholder only when nothing better is known
    assert not any(i.startswith(("ins_cluster", "ins_qq", "corp_NONE")) for i in names)
    assert ("ins_jensen_huang", "corp_NVDA") in conn.connections


# ── upsert semantics and the name lookup on Postgres ─────────────────────

_DDL = [
    ("CREATE TABLE actors (id TEXT PRIMARY KEY, name TEXT, tier TEXT, category TEXT, "
     "influence_score NUMERIC, trust_score NUMERIC, source TEXT, updated_at TIMESTAMPTZ)"),
    "CREATE TABLE security_master (entity_id TEXT PRIMARY KEY, name TEXT)",
    ("CREATE TABLE security_identifiers (id SERIAL PRIMARY KEY, entity_id TEXT, id_scheme TEXT, "
     "id_value TEXT, valid_from DATE, valid_to DATE)"),
]


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"enrich_names_{uuid4().hex[:10]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for stmt in _DDL:
                conn.execute(text(stmt))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def test_ensure_actor_keeps_an_existing_name(scratch: Engine) -> None:
    with scratch.begin() as conn:
        conn.execute(text("INSERT INTO actors (id, name) VALUES "
                          "('corp_C', 'CITIGROUP INC'), ('corp_X', ''), ('corp_Y', 'corp_Y')"))
        for actor_id in ("corp_C", "corp_X", "corp_Y", "corp_NEW"):
            ec._ensure_actor(conn, actor_id, f"{actor_id[5:]} Corp", "corporation", "institutional")
        got = dict(conn.execute(text("SELECT id, name FROM actors")).fetchall())
    assert got == {"corp_C": "CITIGROUP INC", "corp_X": "X Corp", "corp_Y": "Y Corp", "corp_NEW": "NEW Corp"}


def test_company_names_are_current_and_unambiguous(scratch: Engine) -> None:
    with scratch.begin() as conn:
        conn.execute(text("INSERT INTO security_master VALUES "
                          "('sm_1', 'CITIGROUP INC'), ('sm_2', 'Old Name Co'), ('sm_3', 'Twin A'), "
                          "('sm_4', 'Twin B'), ('sm_5', '')"))
        conn.execute(text(
            "INSERT INTO security_identifiers (entity_id, id_scheme, id_value, valid_from, valid_to) VALUES "
            "('sm_1', 'ticker', 'C', '2000-01-01', NULL), "
            "('sm_2', 'ticker', 'OLD', '2000-01-01', '2020-01-01'), "   # no longer listed
            "('sm_3', 'ticker', 'TWN', '2000-01-01', NULL), "
            "('sm_4', 'ticker', 'twn', '2010-01-01', NULL), "           # two entities hold it today
            "('sm_5', 'ticker', 'BLANK', '2000-01-01', NULL), "
            "('sm_1', 'cik', '831001', '2000-01-01', NULL)"
        ))
        assert ec._load_company_names(conn) == {"C": "CITIGROUP INC"}


def test_company_names_without_security_master_fall_back(scratch: Engine, monkeypatch) -> None:
    monkeypatch.setattr(ec, "_company_names", None)
    with scratch.begin() as conn:
        conn.execute(text("DROP TABLE security_identifiers"))
        assert ec._company_name(conn, "aapl") == "AAPL Corp"
        conn.execute(text("SELECT 1"))  # the transaction is still usable
