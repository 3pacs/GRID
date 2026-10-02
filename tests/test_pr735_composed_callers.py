"""Options #735/#742 composition through real callers and SQLite logging.

Provider captures are boundary doubles. The real full-universe pull loop,
bounded catalog publisher, SmartScheduler tick, group/PullContext and repair
wrapper execute. PostgreSQL commands/trigger closure are bridge doubles; the
catalog UPDATE, explicit COMMIT/rollback and pull_log execute on SQLite. This
does not certify PostgreSQL trigger closure or server COMMIT acknowledgement.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import StaticPool

from ingestion import options, pull_context, scheduler, smart_scheduler as ss
from scripts import hermes_fixers as hf


def _engine():
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def now(dbapi_conn, _record):
        dbapi_conn.create_function("NOW", 0, lambda: datetime.now(timezone.utc).isoformat(sep=" "))

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT UNIQUE, last_pull_at TIMESTAMP)"))
        conn.execute(text("INSERT INTO source_catalog VALUES (185, 'YFINANCE_OPTIONS', NULL)"))
        conn.execute(text("CREATE TABLE pull_log (id INTEGER PRIMARY KEY AUTOINCREMENT, puller_name TEXT NOT NULL, "
                          "source_id INTEGER, started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP, "
                          "status TEXT CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')), rows_inserted INTEGER, "
                          "rows_expected INTEGER, error_message TEXT, node_name TEXT, features_affected TEXT)"))
    return engine


class _CatalogBridge:
    """Run the actual publisher's UPDATE/commit on SQLite; record PG limits."""
    def __init__(self, engine, fail):
        self.engine, self.fail = engine, fail
        self.limits, self.config = [], None
        self.started = self.committed = self.rolled_back = self.disposed = 0

    def connect(self):
        conn = self.engine.connect()
        bridge = self

        class Connection:
            info = {}

            def begin(self):
                bridge.started += 1
                transaction = conn.begin()

                class Transaction:
                    def commit(self):
                        transaction.commit()
                        bridge.committed += 1

                    def rollback(self):
                        transaction.rollback()
                        bridge.rolled_back += 1

                return Transaction()

            def execute(self, statement, params=None):
                sql = str(statement)
                if sql.startswith("SET LOCAL "):
                    if "grid.options_bounded" not in sql:
                        bridge.limits.append(sql)
                    return MagicMock()
                if sql.startswith("SET TRANSACTION") or "options_bounded_assert_trigger_closure" in sql:
                    return MagicMock()
                if "pg_stat_xact_user_tables" in sql:
                    return conn.execute(text("SELECT total_changes()"))
                result = conn.execute(statement, params or {})
                if bridge.fail:
                    raise RuntimeError("controlled catalog publication failure after UPDATE")
                return result

            def exec_driver_sql(self, statement):
                assert statement.startswith("LOCK TABLE ")

            def close(self):
                conn.close()

        return Connection()

    def dispose(self):
        self.disposed += 1


@pytest.fixture
def capture(monkeypatch):
    engine = _engine()
    # SQLite cannot COMMIT an UPDATE...RETURNING whose cursor is unread.
    # Buffer the actual rows at this test boundary; production PostgreSQL
    # logging and the physical UPDATE/commit assertions stay unchanged.
    original_execute = Connection.execute
    completion_rows = []

    def buffered_execute(conn, statement, *args, **kwargs):
        result = original_execute(conn, statement, *args, **kwargs)
        if conn.engine is engine and "UPDATE pull_log SET" in str(statement) and "RETURNING id" in str(statement):
            frozen = result.freeze()
            completion_rows.extend(frozen.data)
            return frozen()
        return result

    monkeypatch.setattr(Connection, "execute", buffered_execute)
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine, obj.source_id = engine, 185
    # Preserve the actual hand list and complete it to exactly 200 with
    # catalyst names. No production universe or budget is changed.
    hand_list = tuple(options.EQUITY_TICKERS)
    extra = [f"OFFLINE{i:03}" for i in range(200 - len(hand_list))]
    universe = list(hand_list) + extra
    assert len(universe) == len(set(universe)) == 200
    lookup = MagicMock(return_value=extra)
    monkeypatch.setattr(options, "catalyst_options_universe", lookup)
    monkeypatch.setattr(options, "is_market_open", lambda _day: True)
    monkeypatch.setattr(options, "_utc_now", lambda: datetime(2026, 9, 29, 15, tzinfo=timezone.utc))
    monkeypatch.setattr(options, "YahooOptionsClient", lambda: MagicMock(is_available=True))
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)
    bridge = _CatalogBridge(engine, fail=False)

    def publisher_engine(url, **kwargs):
        assert url == engine.url
        bridge.config = kwargs
        return bridge

    monkeypatch.setattr(options, "create_engine", publisher_engine)
    catalog_updates = []

    @event.listens_for(engine, "before_cursor_execute")
    def record_update(_conn, _cursor, statement, parameters, _context, _many):
        if "UPDATE source_catalog SET last_pull_at" in statement:
            catalog_updates.append((statement, parameters))

    controls = dict(engine=engine, obj=obj, bridge=bridge, universe=universe,
                    lookup=lookup, calls=[], updates=catalog_updates, case="full",
                    completion_rows=completion_rows)

    def ticker(ticker, _today, *, max_expirations, should_continue):
        assert max_expirations in (6, 12)
        assert should_continue is None or callable(should_continue)
        controls["calls"].append((ticker, max_expirations))
        index = universe.index(ticker) if ticker in universe else -1
        case = controls["case"]
        status, rows = "SUCCESS", 2
        if case == "mixed" and index in (0, 1):
            status, rows = ("FAILED", 0) if index == 0 else ("SKIPPED", 0)
        if case == "deferred" and index == 3:
            status, rows = "DEFERRED", 0
        if case == "unknown" and index == 0:
            rows = None
        if case == "zero":
            rows = 0
        if case == "skipped":
            status, rows = "SKIPPED", 0
        return dict(ticker=ticker, status=status, rows_inserted=rows, snapshots=123,
                    reason="controlled capture", signal_value=0.0)

    monkeypatch.setattr(obj, "_pull_ticker", ticker)
    yield controls
    engine.dispose()


def _last_pull(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT last_pull_at FROM source_catalog WHERE id=185")).scalar_one()


def _logs(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT status, rows_inserted, error_message FROM pull_log ORDER BY id")).fetchall()


def _smart(monkeypatch, capture, kwargs):
    entry = dict(name="options", mod="ingestion.smart_scheduler", cls="_OptionsSchedulerAdapter",
                 method="pull", freq_h=6, timeout_s=900, stop_margin_s=60, kwargs={})
    adapter = ss._OptionsSchedulerAdapter.__new__(ss._OptionsSchedulerAdapter)
    adapter._puller = capture["obj"]
    # The real adapter passes only should_continue; explicit smaller scopes
    # are exercised through the actual pull method with the same job name.
    if kwargs:
        instance, entry["method"], entry["kwargs"] = capture["obj"], "pull_all", kwargs
    else:
        instance = adapter
    monkeypatch.setattr(ss, "PULLER_REGISTRY", [entry])
    monkeypatch.setattr(ss.SmartScheduler, "_warn_registry_divergence", lambda _self: None)
    smart = ss.SmartScheduler(capture["engine"])
    monkeypatch.setattr(smart, "_build_puller_instance", lambda *_a: instance)
    duplicate = MagicMock(wraps=smart._update_last_pull)
    monkeypatch.setattr(smart, "_update_last_pull", duplicate)
    observed = smart.tick()["results"][0]
    duplicate.assert_not_called()
    assert smart._orphan_thread_count == 0 and not smart._active_threads
    return observed, smart


CASES = [
    ("full", "SUCCESS", 400, 200),
    ("gem", "PARTIAL", 16, 8),  # eight GEM tickers (BHRB dropped)
    ("subset", "PARTIAL", 6, 3),
    ("reduced_expiry", "PARTIAL", 400, 200),
    ("mixed", "PARTIAL", 396, 200),
    ("deferred", "PARTIAL", 6, 4),
    ("unknown", "PARTIAL", None, 200),
    ("publication_failure", "PARTIAL", 400, 200),
    ("zero", "NO_NEW_DATA", 0, 200),
    ("skipped", "SKIPPED", 0, 200),
]


@pytest.mark.parametrize("caller", ["smart", "group", "retry"])
@pytest.mark.parametrize("case,expected,rows,n_calls", CASES)
def test_composed_options_callers(capture, monkeypatch, caller, case, expected, rows, n_calls):
    capture["case"] = case
    capture["bridge"].fail = case == "publication_failure"
    kwargs = {}
    if case == "gem":
        from scripts.pull_options_gem_tickers import GEM_TICKERS
        kwargs = dict(tickers=list(GEM_TICKERS), include_catalyst_universe=False, max_expirations=6)
    if case == "subset":
        kwargs = dict(tickers=capture["universe"][:3])
    if case == "reduced_expiry":
        kwargs = dict(max_expirations=6)
    engine = capture["engine"]
    if caller == "smart" and case == "unknown":
        # A first failure also retries in 30m, so seed an actual older
        # streak to distinguish PARTIAL's flat retry from failure backoff.
        with engine.begin() as conn:
            for hours in (3, 2):
                at = datetime.now(timezone.utc) - timedelta(hours=hours)
                conn.execute(text("INSERT INTO pull_log (puller_name, started_at, completed_at, status) "
                                  "VALUES ('smart:options', :at, :at, 'FAILED')"), {"at": at})
    if caller == "smart":
        observed, smart = _smart(monkeypatch, capture, kwargs)
        status = observed["status"]
        assert observed["rows_inserted"] == rows
        state = smart._state["options"]
        if case == "unknown":
            assert state["consecutive_fails"] == 3
        if expected == "PARTIAL":
            assert state.get("last_success") is None
            assert timedelta(minutes=29) < state["cooldown_until"] - state["last_attempt"] <= timedelta(minutes=31)
            restarted = ss.SmartScheduler(engine)
            assert timedelta(minutes=29) < restarted._state["options"]["cooldown_until"] - state["last_attempt"] <= timedelta(minutes=31)
            assert restarted._state["options"]["consecutive_fails"] == state["consecutive_fails"]
        if expected == "NO_NEW_DATA":
            assert smart._get_due_pullers() == []
            assert ss.SmartScheduler(engine)._get_due_pullers() == []
        if expected == "SKIPPED":
            assert state.get("last_success") is None and state["consecutive_fails"] == 0
        records = _logs(engine)
        if case == "unknown":
            assert [row[0] for row in records[:-1]] == ["FAILED", "FAILED"]
            records = records[-1:]
        if expected == "SKIPPED":
            assert records == []
        else:
            assert len(records) == 1
            assert tuple(records[0][:2]) == ("SUCCESS" if expected == "NO_NEW_DATA" else expected, rows)
            if expected == "NO_NEW_DATA":
                assert records[0][2].startswith("NO_NEW_DATA:")
    elif caller == "group":
        monkeypatch.setattr(scheduler, "_get_pullers_for_group", lambda *_a: [
            ("Options_Alias", capture["obj"], "pull_all", kwargs)
        ])
        duplicate = MagicMock(wraps=scheduler._touch_source_catalog_last_pull)
        monkeypatch.setattr(scheduler, "_touch_source_catalog_last_pull", duplicate)
        emitted = MagicMock()
        monkeypatch.setattr(pull_context, "_emit_pull_event", emitted)
        result = scheduler.run_pull_group("daily", engine, config={})
        observed = result["results"][0]
        status = observed["status"]
        assert observed["rows_inserted"] == rows
        duplicate.assert_not_called()
        records = _logs(engine)
        assert len(capture["completion_rows"]) == 1
        assert len(records) == 1
        physical = "SUCCESS" if expected in {"NO_NEW_DATA", "SKIPPED"} else expected
        assert tuple(records[0][:2]) == (physical, rows)
        if expected in {"NO_NEW_DATA", "SKIPPED"}:
            assert records[0][2].startswith(expected + ":")
        emitted.assert_called_once()
        assert emitted.call_args.args[2:4] == (expected, rows)
    else:
        monkeypatch.setattr(hf, "_resolve_puller", lambda *_a: (capture["obj"], "pull_all", kwargs))
        # Use an alias so SOURCE_NAME, rather than a name-only guard, proves
        # that the options puller owns the catalog even through repair.
        observed = hf._retry_source("Options_Alias", engine)
        status = observed["outcome"]
        assert observed["rows_inserted"] == rows
        assert (hf.retry_not_fresh_reason(observed) is None) == (expected == "SUCCESS")
        assert "options_alias" not in hf._REPAIRS_IN_FLIGHT
        assert _logs(engine) == []  # repair itself has no pull_log contract
    assert status == expected
    assert len(capture["calls"]) == n_calls
    cap = 6 if case in {"gem", "reduced_expiry"} else 12
    assert {item[1] for item in capture["calls"]} == {cap}
    if case not in {"gem", "subset"}:
        capture["lookup"].assert_called_once_with(engine, require_complete=True)
    bridge = capture["bridge"]
    publisher_attempted = case in {"full", "publication_failure"}
    assert bridge.started == bridge.disposed == int(publisher_attempted)
    assert bridge.committed == int(case == "full")
    assert bridge.rolled_back == int(case == "publication_failure")
    assert len(capture["updates"]) == int(publisher_attempted)
    assert (_last_pull(engine) is not None) == (expected == "SUCCESS")
    if publisher_attempted:
        assert bridge.config["poolclass"].__name__ == "NullPool"
        assert bridge.config["connect_args"]["connect_timeout"] == 5
        assert bridge.limits == ["SET LOCAL lock_timeout = '3s'", "SET LOCAL statement_timeout = '5s'",
                                 "SET LOCAL idle_in_transaction_session_timeout = '5s'"]


@pytest.mark.parametrize("outcome", [
    {"status": "PARTIAL", "rows_inserted": None},
    {"status": "PARTIAL"},
])
def test_explicit_partial_unknown_count_keeps_partial_and_null(outcome):
    assert ss._classify_outcome(outcome)[:2] == ("PARTIAL", None)


@pytest.mark.parametrize("count", [True, -1, "5", 1.5])
def test_invalid_partial_count_still_fails_closed(count):
    assert ss._classify_outcome({"status": "PARTIAL", "rows_inserted": count})[:2] == ("FAILED", None)


@pytest.mark.parametrize("outcome", [
    {"status": "SUCCESS", "rows_inserted": None},
    {"status": "SUCCESS"},
    {"status": "SUCCESS", "rows_inserted": None, "stopped_by_budget": True},
])
def test_unknown_success_cannot_become_fresh(outcome):
    assert ss._classify_outcome(outcome)[:2] == ("FAILED", None)


@pytest.mark.parametrize("key", ss._ROW_COUNT_KEYS)
def test_explicit_partial_all_count_aliases_allow_null(key):
    assert ss._classify_outcome({"status": "PARTIAL", key: None})[:2] == ("PARTIAL", None)


@pytest.mark.parametrize("caller", ["smart", "group", "retry"])
def test_unchanged_owned_options_cadence_without_publication(capture, monkeypatch, caller):
    obj, engine = capture["obj"], capture["engine"]
    monkeypatch.setattr(obj, "pull_all", lambda: {"status": "UNCHANGED"})
    if caller == "smart":
        # The ordinary pull method handles UNCHANGED; the full-options
        # adapter's contract is specifically OptionsPullResults.summary.
        monkeypatch.setattr(ss, "PULLER_REGISTRY", [dict(
            name="options", mod="ingestion.options", cls="OptionsPuller", method="pull_all", freq_h=6, timeout_s=900
        )])
        monkeypatch.setattr(ss.SmartScheduler, "_warn_registry_divergence", lambda _self: None)
        smart = ss.SmartScheduler(engine)
        monkeypatch.setattr(smart, "_build_puller_instance", lambda *_a: obj)
        result = smart.tick()["results"][0]
        assert (result["status"], result["rows_inserted"]) == ("NO_NEW_DATA", 0)
        assert smart._get_due_pullers() == ss.SmartScheduler(engine)._get_due_pullers() == []
    elif caller == "group":
        monkeypatch.setattr(scheduler, "_get_pullers_for_group", lambda *_a: [("Options_Alias", obj, "pull_all", {})])
        monkeypatch.setattr(pull_context, "_emit_pull_event", MagicMock())
        result = scheduler.run_pull_group("daily", engine, config={})["results"][0]
        assert (result["status"], result["rows_inserted"]) == ("NO_NEW_DATA", 0)
    else:
        monkeypatch.setattr(hf, "_resolve_puller", lambda *_a: (obj, "pull_all", {}))
        result = hf._retry_source("Options_Alias", engine)
        assert (result["outcome"], result["rows_inserted"]) == ("NO_NEW_DATA", 0)
        assert hf.retry_not_fresh_reason(result)
    assert capture["bridge"].started == len(capture["updates"]) == 0
    assert _last_pull(engine) is None
    if caller != "retry":
        assert tuple(_logs(engine)[0][:2]) == ("SUCCESS", 0)
        assert _logs(engine)[0][2].startswith("NO_NEW_DATA:")
