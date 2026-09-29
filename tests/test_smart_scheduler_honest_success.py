"""SmartScheduler records SUCCESS only for runs that actually wrote rows.

PR #727 review (2026-09-29): SmartScheduler recorded ANY list returned by a
puller as SUCCESS. OptionsPuller.pull_all returns one ``{"status":
"SKIPPED"}`` item per ticker outside an equity session (#653), so Hermes
bumped YFINANCE_OPTIONS' ``source_catalog.last_pull_at`` on runs that wrote
nothing -- a fake success in the freshness layer.

Covered here (SQLite stand-in for pull_log / source_catalog with the real
CHECK constraint; tests/test_smart_scheduler_honest_success_pg.py repeats
the pull_log contract on real PostgreSQL):

* the real OptionsPuller outside a session -> SKIPPED, no freshness bump,
  no pull_log row, flat retry (not the failure backoff);
* partial coverage preserves PARTIAL and committed rows without freshness;
* every item failed -> FAILED; a raised exception -> FAILED;
* a clean run that wrote 0 rows -> NO_NEW_DATA: cadence kept, no bump;
* ``_classify_outcome`` over every return shape pullers use.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

import ingestion.smart_scheduler as ss
from ingestion import options
from ingestion.smart_scheduler import (
    OUTCOME_FAILED,
    OUTCOME_NO_NEW_DATA,
    OUTCOME_PARTIAL,
    OUTCOME_SKIPPED,
    OUTCOME_SUCCESS,
    SKIP_RETRY_MINUTES,
    SMART_PULL_LOG_PREFIX,
    SmartScheduler,
    _classify_outcome,
)

# ── _classify_outcome: every return shape ────────────────────────────────


@pytest.mark.parametrize(
    ("out", "expected"),
    [
        # rows written > 0 -> SUCCESS with the count
        ({"status": "SUCCESS", "rows_inserted": 5}, (OUTCOME_SUCCESS, 5)),
        ({"inserted": 2}, (OUTCOME_SUCCESS, 2)),
        (7, (OUTCOME_SUCCESS, 7)),
        ([{"status": "SUCCESS", "rows_inserted": 4},
          {"status": "FAILED", "rows_inserted": 0, "error": "boom"},
          {"status": "SKIPPED", "rows_inserted": 0}], (OUTCOME_PARTIAL, 4)),
        # explicit nothing-to-do -> SKIPPED
        ({"status": "SKIPPED", "skipped_reason": "lock held"}, (OUTCOME_SKIPPED, None)),
        ([{"status": "SKIPPED", "reason": "non-equity-session"}] * 3, (OUTCOME_SKIPPED, 0)),
        ([{"status": "SKIPPED", "rows_inserted": 0}] * 2, (OUTCOME_SKIPPED, 0)),
        ([], (OUTCOME_SKIPPED, 0)),
        # every attempted item failed / explicit failure -> FAILED
        ([{"status": "FAILED", "error": "503"}, {"status": "FAILED", "error": "503"}],
         (OUTCOME_FAILED, None)),
        ([{"status": "FAILED", "rows_inserted": 0}, {"status": "SKIPPED"}], (OUTCOME_FAILED, 0)),
        ([{"status": "PARTIAL", "rows_inserted": 0, "errors": ["404"]}], (OUTCOME_FAILED, 0)),
        ({"status": "FAILED", "error": "x"}, (OUTCOME_FAILED, None)),
        ({"rows_inserted": 0, "error": "x"}, (OUTCOME_FAILED, 0)),
        (False, (OUTCOME_FAILED, None)),
        # explicit PARTIAL dict keeps its #685 meaning
        ({"status": "PARTIAL", "rows_inserted": 1}, (OUTCOME_PARTIAL, 1)),
        # clean run, 0 rows -> NO_NEW_DATA (never SUCCESS)
        ({"status": "SUCCESS", "rows_inserted": 0}, (OUTCOME_NO_NEW_DATA, 0)),
        (0, (OUTCOME_NO_NEW_DATA, 0)),
        ([{"status": "SUCCESS", "rows_inserted": 0},
          {"status": "PARTIAL", "rows_inserted": 0}], (OUTCOME_PARTIAL, 0)),
        # no row count reported -> FAILED contract; unknown stays unknown
        (None, (OUTCOME_FAILED, None)),
        ("done", (OUTCOME_FAILED, None)),
        (["ok"], (OUTCOME_FAILED, None)),
        ({"status": "SUCCESS"}, (OUTCOME_FAILED, None)),
        ([{"status": "SUCCESS"}], (OUTCOME_FAILED, None)),
        (True, (OUTCOME_FAILED, None)),
    ],
)
def test_classify_outcome(out: Any, expected: tuple[str, int | None]) -> None:
    outcome, rows, _note = _classify_outcome(out)
    assert (outcome, rows) == expected


def test_partial_list_note_counts_incomplete_items() -> None:
    outcome, rows, note = _classify_outcome([
        {"ticker": "A", "status": "SUCCESS", "rows_inserted": 3},
        {"ticker": "B", "status": "FAILED", "rows_inserted": 0, "error": "timeout"},
    ])
    assert (outcome, rows) == (OUTCOME_PARTIAL, 3)
    assert "1 of 2 items failed or partial" in note


def test_all_failed_note_carries_first_error() -> None:
    outcome, _rows, note = _classify_outcome([
        {"ticker": "A", "status": "FAILED", "error": "HTTP 503"},
    ])
    assert outcome == OUTCOME_FAILED
    assert "HTTP 503" in note and "unknown rows written" in note


# ── SmartScheduler end to end (SQLite stand-in) ──────────────────────────


class _SessionClosedOptions(options.OptionsPuller):
    """The real OptionsPuller.pull_all, minus the DDL in __init__."""

    def __init__(self, db_engine) -> None:  # noqa: D401 - test double
        self.engine = db_engine


class _ListPuller:
    RETURN: Any = None

    def __init__(self, db_engine) -> None:
        self.db_engine = db_engine

    def pull_all(self) -> Any:
        return self.RETURN


class _PartialPuller(_ListPuller):
    RETURN = [
        {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 120},
        {"ticker": "QQQ", "status": "FAILED", "rows_inserted": 0, "error": "Yahoo 503"},
        {"ticker": "IWM", "status": "SKIPPED", "rows_inserted": 0, "reason": "no expirations"},
    ]


class _AllFailedPuller(_ListPuller):
    RETURN = [
        {"ticker": "SPY", "status": "FAILED", "rows_inserted": 0, "error": "Yahoo 503"},
        {"ticker": "QQQ", "status": "FAILED", "rows_inserted": 0, "error": "Yahoo 503"},
    ]


class _NothingNewPuller(_ListPuller):
    RETURN = [
        {"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 0},
        {"ticker": "QQQ", "status": "SUCCESS", "rows_inserted": 0},
    ]


class _RaisingPuller(_ListPuller):
    def pull_all(self) -> Any:
        raise RuntimeError("upstream exploded")


def _entry(name: str, cls: str, **extra: Any) -> dict:
    return {
        "name": name, "mod": __name__, "cls": cls, "method": "pull_all",
        "freq_h": 6, "timeout_s": 10, **extra,
    }


def _engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )

    @event.listens_for(engine, "connect")
    def _now(dbapi_conn, _rec) -> None:  # Postgres NOW() stand-in
        dbapi_conn.create_function(
            "NOW", 0, lambda: datetime.now(timezone.utc).isoformat(sep=" ")
        )

    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT UNIQUE, "
            "last_pull_at TIMESTAMP)"
        ))
        conn.execute(text(
            "CREATE TABLE pull_log (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "puller_name TEXT NOT NULL, source_id INTEGER, started_at TIMESTAMP NOT NULL, "
            "completed_at TIMESTAMP, status TEXT NOT NULL CHECK (status IN "
            "('RUNNING','SUCCESS','PARTIAL','FAILED')), rows_inserted INTEGER DEFAULT 0, "
            "rows_expected INTEGER, error_message TEXT, node_name TEXT)"
        ))
    return engine


@pytest.fixture
def sched(monkeypatch):
    entries: list[dict] = []
    catalog_map: dict[str, str] = {}
    monkeypatch.setattr(ss, "PULLER_REGISTRY", entries)
    monkeypatch.setattr(ss, "REGISTRY_CATALOG_NAMES", catalog_map)
    monkeypatch.setattr(ss, "MAX_PULLERS_PER_TICK", 50)
    monkeypatch.setattr(SmartScheduler, "_warn_registry_divergence", lambda self: None)
    engine = _engine()

    def _make(*new_entries: dict) -> SmartScheduler:
        entries.extend(new_entries)
        with engine.begin() as conn:
            for i, e in enumerate(new_entries, start=len(entries) - len(new_entries) + 1):
                catalog = e.get("_catalog", e["name"].upper())
                catalog_map[e["name"]] = catalog
                conn.execute(
                    text("INSERT INTO source_catalog (id, name, last_pull_at) VALUES (:i, :n, NULL)"),
                    {"i": i, "n": catalog},
                )
        return SmartScheduler(engine)

    _make.engine = engine  # type: ignore[attr-defined]
    return _make


def _last_pull(engine, catalog: str):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT last_pull_at FROM source_catalog WHERE name = :n"), {"n": catalog}
        ).scalar()


def _pull_log(engine) -> list[tuple]:
    with engine.connect() as conn:
        return [tuple(r) for r in conn.execute(text(
            "SELECT puller_name, status, rows_inserted, error_message FROM pull_log ORDER BY id"
        )).fetchall()]


def test_options_all_tickers_skipped_is_not_success_and_not_fresh(sched, monkeypatch) -> None:
    """The reviewer's case, through the real OptionsPuller.pull_all."""
    monkeypatch.setattr(options, "is_market_open", lambda _day: False)
    monkeypatch.setattr(
        options, "YahooOptionsClient",
        lambda: pytest.fail("a closed session must not contact the provider"),
    )
    s = sched(_entry(
        "options", "_SessionClosedOptions",
        kwargs={"tickers": ["SPY", "QQQ", "IWM"]}, _catalog="YFINANCE_OPTIONS",
    ))
    summary = s.tick()

    result = summary["results"][0]
    assert result["status"] == OUTCOME_SKIPPED
    assert result["reason"] == "no ticker capture completed"
    assert (summary["succeeded"], summary["skipped"]) == (0, 1)
    assert _last_pull(sched.engine, "YFINANCE_OPTIONS") is None  # no freshness bump
    assert _pull_log(sched.engine) == []  # not an attempt -> no pull_log row
    state = s._state["options"]
    assert state.get("last_success") is None
    assert state["consecutive_fails"] == 0  # a skip is not a failure...
    retry_in = state["cooldown_until"] - datetime.now(timezone.utc)
    assert timedelta(minutes=SKIP_RETRY_MINUTES - 1) < retry_in <= timedelta(
        minutes=SKIP_RETRY_MINUTES
    )  # ...and gets a flat retry, not the escalating failure backoff

    s._state["options"]["cooldown_until"] = datetime.now(timezone.utc) - timedelta(seconds=1)
    s.tick()
    assert s._state["options"]["consecutive_fails"] == 0  # still no escalation


def test_partial_preserves_count_without_freshness(sched) -> None:
    s = sched(_entry("partial", "_PartialPuller"))
    summary = s.tick()

    result = summary["results"][0]
    assert result["status"] == OUTCOME_PARTIAL
    assert result["rows_inserted"] == 120
    assert summary["succeeded"] == 0
    assert _last_pull(sched.engine, "PARTIAL") is None
    (row,) = _pull_log(sched.engine)
    assert row[:3] == (SMART_PULL_LOG_PREFIX + "partial", "PARTIAL", 120)
    assert "1 of 3 items failed or partial" in row[3]


def test_every_item_failed_is_failed(sched) -> None:
    s = sched(_entry("allfail", "_AllFailedPuller"))
    summary = s.tick()

    result = summary["results"][0]
    assert result["status"] == OUTCOME_FAILED
    assert "Yahoo 503" in result["error"]
    assert summary["failed"] == 1
    assert _last_pull(sched.engine, "ALLFAIL") is None
    (row,) = _pull_log(sched.engine)
    assert row[:3] == (SMART_PULL_LOG_PREFIX + "allfail", "FAILED", 0)
    assert s._state["allfail"]["consecutive_fails"] == 1


def test_exception_is_failed(sched) -> None:
    s = sched(_entry("raiser", "_RaisingPuller"))
    summary = s.tick()

    result = summary["results"][0]
    assert result["status"] == OUTCOME_FAILED
    assert "upstream exploded" in result["error"]
    assert _last_pull(sched.engine, "RAISER") is None
    (row,) = _pull_log(sched.engine)
    assert row[:3] == (SMART_PULL_LOG_PREFIX + "raiser", "FAILED", 0)
    assert "upstream exploded" in row[3]


def test_clean_run_with_zero_rows_keeps_cadence_but_is_not_fresh(sched) -> None:
    s = sched(_entry("nothingnew", "_NothingNewPuller"))
    summary = s.tick()

    result = summary["results"][0]
    assert result["status"] == OUTCOME_NO_NEW_DATA
    assert (summary["succeeded"], summary["no_new_data"], summary["failed"]) == (0, 1, 0)
    assert _last_pull(sched.engine, "NOTHINGNEW") is None  # not fresh
    # pull_log has no NO_NEW_DATA status (CHECK constraint): SUCCESS, 0 rows, marked.
    (row,) = _pull_log(sched.engine)
    assert row[:3] == (SMART_PULL_LOG_PREFIX + "nothingnew", "SUCCESS", 0)
    assert row[3].startswith("NO_NEW_DATA:")
    # Cadence kept: not re-run next tick, not backed off like a failure,
    # and a restart does not re-run it either.
    assert s._state["nothingnew"]["consecutive_fails"] == 0
    assert s._get_due_pullers() == []
    assert SmartScheduler(sched.engine)._get_due_pullers() == []


def test_explicit_skipped_dict_still_skipped_and_not_logged(sched) -> None:
    class _Skip(_ListPuller):
        RETURN = {"status": "SKIPPED", "skipped_reason": "already running"}

    globals()["_SkipDictPuller"] = _Skip
    try:
        s = sched(_entry("lockskip", "_SkipDictPuller"))
        summary = s.tick()
    finally:
        globals().pop("_SkipDictPuller", None)
    assert summary["results"][0]["status"] == OUTCOME_SKIPPED
    assert summary["results"][0]["reason"] == "already running"
    assert _last_pull(sched.engine, "LOCKSKIP") is None
    assert _pull_log(sched.engine) == []


# ── Hermes repair wrapper (scripts/hermes_fixers._retry_source) ──────────


def _install_retry_puller(monkeypatch, name: str, returns: Any) -> None:
    import sys
    from types import ModuleType

    from scripts import hermes_operator

    module_name = f"_grid_test_retry_{name}"
    fake_module = ModuleType(module_name)

    class _Puller:
        def __init__(self, db_engine=None):
            self.db_engine = db_engine

        def pull_all(self):
            return returns

    fake_module._Puller = _Puller
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    monkeypatch.setitem(
        hermes_operator._SOURCE_REGISTRY,
        name,
        {"mod": module_name, "cls": "_Puller", "pull_method": "pull_all", "pull_kwargs": {}},
    )


@pytest.mark.parametrize(
    ("returns", "fresh"),
    [
        ([{"ticker": "SPY", "status": "SKIPPED", "reason": "non-equity-session"}] * 2, False),
        ([{"ticker": "SPY", "status": "FAILED", "error": "503"}], False),
        ({"status": "SKIPPED", "skipped_reason": "already running"}, False),
        ({"status": "FAILED", "error": "boom"}, False),
        ([{"ticker": "SPY", "status": "SUCCESS", "rows_inserted": 4}], True),
    ],
)
def test_retry_source_never_marks_a_skipped_or_failed_run_fresh(monkeypatch, returns, fresh) -> None:
    from unittest.mock import MagicMock

    from scripts import hermes_fixers

    _install_retry_puller(monkeypatch, "honest_src", returns)
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    result = hermes_fixers._retry_source("honest_src", engine)

    bumped = any(
        "last_pull_at" in str(call.args[0]) for call in conn.execute.call_args_list
    )
    assert bumped is fresh
    reason = hermes_fixers.retry_not_fresh_reason(result)
    assert (reason is None) is fresh
