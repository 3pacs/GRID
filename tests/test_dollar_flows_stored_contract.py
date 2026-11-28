"""Stored-row contract for ``dollar_flows`` (SQLite; PostgreSQL in the ``_pg`` file).

Why it exists: the daily writer (``scripts/run_dollar_flows.py --days 7``)
rebuilds only the dates inside its window. On 2026-11-27 production still
held 49 13F portfolio summaries stored as ``inflow`` (2026-11-06..11-13),
written before the 13F rule became non-directional and never rewritten.

Pinned here:
  - one contract definition: the CHECK text the writer, the run-end audit,
    the alembic revision and ``schema.sql`` use is the same;
  - the writer refuses and counts a contract-breaking flow;
  - a 7-day run leaves an older row in a superseded shape untouched (the
    defect's mechanism) and now reports it, also in a dry run;
  - the runner exits 3 while any stored row breaks the contract, 0 once clean;
  - an insert error is raised and rolls the run back, never swallowed.
"""

from __future__ import annotations

import importlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Column, Date, Float, Integer, MetaData, String, Table, Text, create_engine, text

from intelligence import dollar_flows
from scripts import run_dollar_flows

REPO = Path(__file__).resolve().parent.parent
_RealDate = date
TODAY = date(2026, 11, 27)
YESTERDAY = TODAY - timedelta(days=1)
OLD = TODAY - timedelta(days=21)

class _FrozenDateMeta(type):
    def __instancecheck__(cls, obj):
        return isinstance(obj, _RealDate)


class _FrozenDate(_RealDate, metaclass=_FrozenDateMeta):
    """``date`` for the writer module with ``today()`` pinned; real dates stay instances."""

    def __new__(cls, *args, **kwargs):
        # Dates the module builds stay real dates (drivers bind the exact type).
        return _RealDate(*args, **kwargs)

    @classmethod
    def today(cls):
        return TODAY


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch):
    # No test here reads the real clock: the writer's window and future-date
    # guard use ``date.today()``, pinned to TODAY for every case.
    monkeypatch.setattr(dollar_flows, "date", _FrozenDate)


# Production's table before revision df_13f_observed_20261127: no CHECK.
_PRE_REVISION_DDL = """
CREATE TABLE dollar_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    actor_name TEXT,
    ticker TEXT,
    amount_usd NUMERIC NOT NULL,
    direction TEXT NOT NULL,
    confidence TEXT DEFAULT 'estimated',
    evidence TEXT,
    flow_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Shape of the 49 production rows (id 645625 on 2026-11-27, amount scaled down).
_LEGACY_13F = {
    "source_type": "13f",
    "actor_name": "0002012383",
    "ticker": "",
    "amount_usd": 6791591104981.0,
    "direction": "inflow",
    "confidence": "confirmed",
    "evidence": {"cusip": None, "action": "UNKNOWN", "shares": None, "pct_change": None, "filing_accession": None},
}


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", Date, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    Table(
        "signal_sources", md,
        Column("id", Integer, primary_key=True),
        Column("source_type", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("ticker", String),
        Column("signal_date", Date, nullable=False),
        Column("signal_type", String, nullable=False),
        Column("signal_value", Text),
        Column("trust_score", Float),
    )
    md.create_all(eng)
    return eng


def _pre_revision_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL))


def _insert_row(engine, row: dict, flow_date: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, "
                "confidence, evidence, flow_date) VALUES (:st, :a, :t, :amt, :d, :c, :ev, :fd)"
            ),
            {
                "st": row["source_type"], "a": row["actor_name"], "t": row["ticker"],
                "amt": row["amount_usd"], "d": row["direction"], "c": row["confidence"],
                "ev": json.dumps(row["evidence"]), "fd": flow_date,
            },
        )


def _insider_signal(engine, signal_date: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES ('congressional', 'rep-x', 'GRDX', :d, 'BUY', :v)"
            ),
            {"d": signal_date, "v": json.dumps({"amount_midpoint": 50_000})},
        )


def _rows(engine) -> list[tuple]:
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT source_type, direction, flow_date FROM dollar_flows ORDER BY id")
        ).fetchall()


# ── one definition ──────────────────────────────────────────────────────────

def test_each_contract_has_its_own_constraint_and_the_13f_repair_checks_only_its_own():
    """The 13F repair's constraint-definition check drops parentheses, which is exact
    only for its one ``a OR b`` CHECK. So every further contract gets its own
    constraint and its own remedy (etf_flow: scripts/rederive_dollar_flows_etf_legacy.py,
    which checks its CHECK by behaviour), and the 13F CHECK stays exactly as released."""
    contracts = dollar_flows.STORED_ROW_CONTRACTS
    assert list(contracts) == ["13f", "etf_flow"]
    assert contracts["13f"]["check_sql"] == dollar_flows.DOLLAR_FLOWS_CONTRACT_CHECK_SQL
    assert contracts["13f"]["constraint"] == dollar_flows.DOLLAR_FLOWS_CONTRACT_CONSTRAINT
    assert len({c["constraint"] for c in contracts.values()}) == len(contracts)
    assert {st: c["remedy"] for st, c in contracts.items()} == {"13f": "delete", "etf_flow": "rederive"}


def test_one_contract_text_for_writer_audit_migration_and_schema():
    migration = importlib.import_module("migrations.versions.df_13f_observed_20261127")
    check = dollar_flows.DOLLAR_FLOWS_CONTRACT_CHECK_SQL
    assert check == migration.CHECK_SQL == "(source_type <> '13f' OR direction = 'observed')"
    assert migration.CONSTRAINT == dollar_flows.DOLLAR_FLOWS_CONTRACT_CONSTRAINT
    expected = f"CONSTRAINT {dollar_flows.DOLLAR_FLOWS_CONTRACT_CONSTRAINT} CHECK {check}"
    assert expected in dollar_flows.CREATE_TABLE_SQL
    assert expected in (REPO / "schema.sql").read_text()
    assert dollar_flows.STORED_ROW_CONTRACTS["13f"]["value"] == dollar_flows.OBSERVED_13F_DIRECTION


@pytest.mark.parametrize(
    ("flow", "expected"),
    [
        ({"source_type": "13f", "direction": "observed"}, None),
        ({"source_type": "13f", "direction": "inflow"}, "13f"),
        ({"source_type": "13f", "direction": "outflow"}, "13f"),
        ({"source_type": "13f"}, "13f"),
        ({"source_type": "insider", "direction": "inflow"}, None),
        ({"source_type": "etf_flow", "direction": "inflow"}, "etf_flow"),                    # the old rule
        ({"source_type": "etf_flow", "direction": "inflow",
          "evidence": {"series_id": "ETF_FLOW:SPY:5d", "raw_value": 1.0}}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "neutral",
          "evidence": {"direction_basis": "unsigned_volume"}}, None),                        # the current rule
        ({"source_type": "etf_flow", "direction": "inflow",
          "evidence": {"direction_basis": "unsigned_volume"}}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "outflow",
          "evidence": {"direction_basis": "net_flow"}}, None),                               # a signed measure
        ({"source_type": "etf_flow", "direction": "neutral", "evidence": {"direction_basis": ""}}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "neutral", "evidence": None}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "inflow", "evidence": {"direction_basis": 5}}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "neutral", "evidence": {"direction_basis": None}}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "neutral", "evidence": [1]}, "etf_flow"),
        ({"source_type": "etf_flow", "direction": "neutral",
          "evidence": '{"direction_basis": "unsigned_volume"}'}, None),                      # JSON text evidence
    ],
)
def test_flow_contract_violation(flow, expected):
    assert dollar_flows.flow_contract_violation(flow) == expected


# ── writer ──────────────────────────────────────────────────────────────────

_GOOD_13F = {**_LEGACY_13F, "direction": "observed", "confidence": "derived", "ticker": "XOM", "actor_name": "W"}
_INSIDER = {"source_type": "insider", "actor_name": "A", "ticker": "GRDX", "amount_usd": 10.0,
            "direction": "inflow", "confidence": "confirmed", "evidence": {}}


def _seed_last_good_window(engine) -> list[tuple]:
    """A window an earlier good run wrote: rows of several sources, a valid 13F row."""
    _pre_revision_table(engine)
    _insert_row(engine, _GOOD_13F, YESTERDAY)
    _insert_row(engine, {**_INSIDER, "actor_name": "Old"}, YESTERDAY)
    _insert_row(engine, {**_INSIDER, "source_type": "options_flow", "ticker": "SPY"}, TODAY - timedelta(days=3))
    return _rows(engine)


@pytest.mark.parametrize("mixed", [False, True], ids=["all_invalid", "mixed"])
def test_a_refused_flow_keeps_the_last_good_window(engine, mixed):
    before = _seed_last_good_window(engine)
    flows = [{**_LEGACY_13F, "flow_date": YESTERDAY}]
    if mixed:
        flows += [{**_GOOD_13F, "flow_date": YESTERDAY}, {**_INSIDER, "flow_date": TODAY - timedelta(days=3)}]
    rejected: dict[str, int] = {}
    assert dollar_flows._persist_flows(engine, flows, rejected_counts=rejected) == 0
    assert rejected == {"13f": 1}
    assert _rows(engine) == before  # no DELETE, no INSERT: every source's rows kept


def test_persist_raises_on_an_insert_error_and_rolls_back_the_window(engine):
    """Previously logged at debug and skipped: on PostgreSQL that silently lost
    the whole run (the aborted transaction drops every later insert and the
    DELETE). Now the error surfaces and the stored window is untouched."""
    with engine.begin() as conn:
        conn.execute(text(_PRE_REVISION_DDL.replace("evidence TEXT,", "")))
        conn.execute(
            text(
                "INSERT INTO dollar_flows (source_type, actor_name, ticker, amount_usd, direction, flow_date) "
                "VALUES ('insider', 'A', 'GRDX', 1, 'inflow', :d)"
            ),
            {"d": YESTERDAY},
        )
    flows = [{"source_type": "insider", "actor_name": "B", "ticker": "GRDY", "amount_usd": 5.0,
              "direction": "inflow", "confidence": "confirmed", "evidence": {}, "flow_date": YESTERDAY}]
    with pytest.raises(Exception, match="(?i)evidence"):
        dollar_flows._persist_flows(engine, flows)
    assert _rows(engine) == [("insider", "inflow", YESTERDAY.isoformat())]


# ── the defect's mechanism, now reported ────────────────────────────────────

def test_seven_day_run_leaves_an_old_superseded_row_and_reports_it(engine):
    _pre_revision_table(engine)
    _insert_row(engine, _LEGACY_13F, OLD)
    _insider_signal(engine, YESTERDAY)

    summary = dollar_flows.normalize_all_flows(engine, days=7)

    # The run wrote its window ...
    assert summary.persisted == 1
    # ... never touched the older date (why the 49 rows outlived the 13F fix) ...
    assert ("13f", "inflow", OLD.isoformat()) in _rows(engine)
    # ... and now says so.
    assert summary.stored_contract_violations == {"13f": 1}
    assert summary.rejected_by_contract == {}
    assert summary.contract_ok is False
    out = summary.to_dict()
    assert out["stored_contract_violations"] == {"13f": 1}
    assert out["rejected_by_contract"] == {}


def test_dry_run_reports_stored_violations_without_writing(engine):
    _pre_revision_table(engine)
    _insert_row(engine, _LEGACY_13F, OLD)
    _insider_signal(engine, YESTERDAY)
    before = _rows(engine)

    summary = dollar_flows.normalize_all_flows(engine, days=7, dry_run=True)

    assert summary.stored_contract_violations == {"13f": 1}
    assert summary.persisted == 0
    assert _rows(engine) == before


def test_dry_run_without_a_table_reports_nothing_and_creates_nothing(engine):
    summary = dollar_flows.normalize_all_flows(engine, days=7, dry_run=True)
    assert summary.contract_ok is True
    with engine.connect() as conn:
        assert not dollar_flows._table_exists(conn, "dollar_flows")


def test_observed_13f_rows_are_within_the_contract(engine):
    _pre_revision_table(engine)
    _insert_row(engine, {**_LEGACY_13F, "direction": "observed", "confidence": "derived", "ticker": None}, OLD)
    summary = dollar_flows.normalize_all_flows(engine, days=7, dry_run=True)
    assert summary.stored_contract_violations == {}


# ── runner exit status ──────────────────────────────────────────────────────

def test_runner_exits_3_while_an_older_stored_row_breaks_the_contract_then_0(engine, capsys):
    _pre_revision_table(engine)
    _insert_row(engine, _LEGACY_13F, OLD)
    _insider_signal(engine, YESTERDAY)

    rc = run_dollar_flows.main(["--days", "7", "--json"], engine=engine)
    captured = capsys.readouterr()
    assert rc == run_dollar_flows.EXIT_STORED_ROWS_BREAK_CONTRACT == 3
    assert json.loads(captured.out)["stored_contract_violations"] == {"13f": 1}
    assert "broken by older stored rows" in captured.err
    assert "window was written" in captured.err
    assert "repair_dollar_flows_stored_contract.py" in captured.err
    # The window was still written before the non-zero exit.
    assert any(r[0] == "congressional" for r in _rows(engine))

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dollar_flows WHERE source_type = '13f'"))
    assert run_dollar_flows.main(["--days", "7"], engine=engine) == 0
    assert "contract" not in capsys.readouterr().err


@pytest.mark.parametrize("mixed", [False, True], ids=["all_invalid", "mixed"])
def test_runner_exits_4_and_keeps_the_window_when_the_writer_refuses(engine, monkeypatch, capsys, mixed):
    before = _seed_last_good_window(engine)
    bad = [{**_LEGACY_13F, "flow_date": YESTERDAY}]
    good = [{**_GOOD_13F, "flow_date": YESTERDAY}] if mixed else []
    monkeypatch.setattr(dollar_flows, "_normalize_13f_flows", lambda *a, **k: bad + good)
    if mixed:
        _insider_signal(engine, YESTERDAY)
    rc = run_dollar_flows.main(["--days", "7", "--json"], engine=engine)
    captured = capsys.readouterr()
    out = json.loads(captured.out)
    assert rc == run_dollar_flows.EXIT_WRITER_REFUSED == 4
    assert out["rejected_by_contract"] == {"13f": 1}
    assert out["persisted"] == 0
    assert "REFUSED" in captured.err and "last good window is kept" in captured.err
    assert _rows(engine) == before


def test_runner_exit_4_takes_precedence_over_older_stored_rows(engine, monkeypatch, capsys):
    before = _seed_last_good_window(engine)
    _insert_row(engine, _LEGACY_13F, OLD)
    monkeypatch.setattr(dollar_flows, "_normalize_13f_flows", lambda *a, **k: [{**_LEGACY_13F, "flow_date": YESTERDAY}])
    rc = run_dollar_flows.main(["--days", "7", "--json"], engine=engine)
    captured = capsys.readouterr()
    assert rc == 4
    assert json.loads(captured.out)["stored_contract_violations"] == {"13f": 1}
    assert "stored_contract_violations" in captured.err
    assert len(_rows(engine)) == len(before) + 1
