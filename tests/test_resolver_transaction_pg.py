"""Native resolver transaction proposal; run only in the disposable QQ CI DB.

The runner must explicitly set GRID_RESOLVER_REQUIRE_PG=1 and supply the
existing GRID_QQ_TEST_DB_URL. No default URL or fallback to application DBs.
The test runner must provision and destroy the isolated container separately.
"""
from __future__ import annotations

import os
import re
import uuid
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError


def _checked_url(raw, writer, environ):
    if not raw or not writer or not re.fullmatch(r"[0-9a-f]{64}", writer):
        raise ValueError("explicit generated writer credential required")
    if any(key.upper().startswith("PG") for key in environ):
        raise ValueError("PostgreSQL environment overrides are forbidden")
    try:
        u = make_url(raw)
    except Exception:
        raise ValueError("invalid isolated PostgreSQL target") from None
    if (u.drivername != "postgresql" or u.password != writer or u.host != "127.0.0.1"
            or u.port != 15432 or u.database != "qq_batch_test"
            or u.username != "qq_ci_writer" or u.query):
        raise ValueError("not the isolated QQ qualification target")
    return u


_TEST_WRITER = "a" * 64  # inert synthetic value; never an actual credential
_TEST_URL = f"postgresql://qq_ci_writer:{_TEST_WRITER}@127.0.0.1:15432/qq_batch_test"


@pytest.mark.parametrize("url", [
    None, _TEST_URL.replace("qq_batch_test", "griddb"),
    _TEST_URL.replace("127.0.0.1", "localhost"),
    _TEST_URL.replace("15432", "5432"),
    _TEST_URL.replace("qq_ci_writer", "other"),
    _TEST_URL + "?host=elsewhere", _TEST_URL.replace("postgresql://", "postgresql+psycopg://"),
    _TEST_URL.replace(f":{_TEST_WRITER}@", "@"),
    _TEST_URL.replace(_TEST_WRITER, "b" * 64),
])
def test_target_refuses_unapproved_url_without_connecting(url, monkeypatch):
    monkeypatch.setattr("socket.socket.connect", lambda *a: pytest.fail("network forbidden"))
    with pytest.raises(ValueError):
        _checked_url(url, _TEST_WRITER, {})


def test_target_accepts_only_expected_url_without_connecting(monkeypatch):
    monkeypatch.setattr("socket.socket.connect", lambda *a: pytest.fail("network forbidden"))
    assert _checked_url(_TEST_URL, _TEST_WRITER, {}).database == "qq_batch_test"


@pytest.mark.parametrize("writer, overrides", [(None, {}), ("x", {}),
    (_TEST_WRITER, {"PGSERVICE": "other"}), (_TEST_WRITER, {"PGPASSFILE": "elsewhere"}),
    (_TEST_WRITER, {"PGOPTIONS": "override"})])
def test_credential_or_environment_override_rejected_before_connect(writer, overrides, monkeypatch):
    monkeypatch.setattr("socket.socket.connect", lambda *a: pytest.fail("network forbidden"))
    with pytest.raises(ValueError):
        _checked_url(_TEST_URL, writer, overrides)


def test_disabled_qualification_skips_before_engine_creation(monkeypatch):
    monkeypatch.delenv("GRID_RESOLVER_REQUIRE_PG", raising=False)
    monkeypatch.setattr(__import__(__name__, fromlist=["create_engine"]), "create_engine",
                        lambda *a, **k: pytest.fail("engine creation forbidden"))
    with pytest.raises(pytest.skip.Exception):
        next(pg.__wrapped__())


def test_enabled_qualification_rejects_wrong_url_before_engine(monkeypatch):
    monkeypatch.setenv("GRID_RESOLVER_REQUIRE_PG", "1")
    monkeypatch.setenv("GRID_QQ_TEST_DB_URL", _TEST_URL.replace("qq_batch_test", "griddb"))
    monkeypatch.setenv("DB_PASSWORD", _TEST_WRITER)
    monkeypatch.setattr(__import__(__name__, fromlist=["create_engine"]), "create_engine",
                        lambda *a, **k: pytest.fail("engine creation forbidden"))
    with pytest.raises(ValueError):
        next(pg.__wrapped__())


@pytest.mark.parametrize("field,value", [(2, "10.0.0.1"), (4, 150000), (5, True),
                                          (8, True), (9, "wrong marker")])
def test_wrong_server_identity_refuses_schema_creation(monkeypatch, field, value):
    identity = ["qq_batch_test", "qq_ci_writer", "127.0.0.1", 15432, 140000,
                False, False, False, False, "grid-quiver-synthetic-ci-only"]
    identity[field] = value
    statements = []
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, statement):
            statements.append(str(statement))
            return SimpleNamespace(one=lambda: tuple(identity))
    class Root:
        disposed = False
        dialect = SimpleNamespace(driver="psycopg2")
        def begin(self): return Connection()
        def dispose(self): self.disposed = True
    root = Root()
    monkeypatch.setenv("GRID_RESOLVER_REQUIRE_PG", "1")
    monkeypatch.setenv("GRID_QQ_TEST_DB_URL", _TEST_URL)
    monkeypatch.setenv("DB_PASSWORD", _TEST_WRITER)
    for key in list(os.environ):
        if key.upper().startswith("PG"):
            monkeypatch.delenv(key)
    monkeypatch.setattr(__import__(__name__, fromlist=["create_engine"]), "create_engine", lambda *a, **k: root)
    with pytest.raises(RuntimeError, match="unsafe PostgreSQL identity"):
        next(pg.__wrapped__())
    assert root.disposed
    assert len(statements) == 1 and "CREATE SCHEMA" not in statements[0]


def test_wrong_loaded_driver_refuses_connection(monkeypatch):
    class Root:
        dialect = SimpleNamespace(driver="other")
        disposed = False
        def begin(self): pytest.fail("connection forbidden")
        def dispose(self): self.disposed = True
    root = Root()
    monkeypatch.setenv("GRID_RESOLVER_REQUIRE_PG", "1")
    monkeypatch.setenv("GRID_QQ_TEST_DB_URL", _TEST_URL)
    monkeypatch.setenv("DB_PASSWORD", _TEST_WRITER)
    for key in list(os.environ):
        if key.upper().startswith("PG"):
            monkeypatch.delenv(key)
    monkeypatch.setattr(__import__(__name__, fromlist=["create_engine"]), "create_engine", lambda *a, **k: root)
    with pytest.raises(RuntimeError, match="unexpected PostgreSQL driver"):
        next(pg.__wrapped__())
    assert root.disposed


@pytest.fixture
def pg():
    if os.environ.get("GRID_RESOLVER_REQUIRE_PG") != "1":
        pytest.skip("explicit isolated resolver qualification not enabled")
    url = _checked_url(os.environ.get("GRID_QQ_TEST_DB_URL"), os.environ.get("DB_PASSWORD"), os.environ)
    root = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3,
                         "options": "-c statement_timeout=2000 -c lock_timeout=1000"})
    schema = "resolver_test_" + uuid.uuid4().hex
    created = False
    engine = None
    try:
        if root.dialect.driver != "psycopg2":
            raise RuntimeError("unexpected PostgreSQL driver")
        with root.begin() as conn:
            identity = conn.execute(text("""
                SELECT current_database(), current_user, host(inet_server_addr()),
                       inet_server_port(), current_setting('server_version_num')::int,
                       r.rolsuper, r.rolcreaterole, r.rolcreatedb, r.rolreplication,
                       shobj_description(d.oid, 'pg_database')
                FROM pg_database d JOIN pg_roles r ON r.rolname = current_user
                WHERE d.datname = current_database()
            """)).one()
            if (identity[:4] != ("qq_batch_test", "qq_ci_writer", "127.0.0.1", 15432)
                    or identity[4] // 10000 != 14
                    or tuple(identity[5:9]) != (False, False, False, False)
                    or identity[9] != "grid-quiver-synthetic-ci-only"):
                raise RuntimeError("unsafe PostgreSQL identity")
            print(f"resolver synthetic PostgreSQL server_version_num={identity[4]}")
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            # Cleanup is attempted even if the following transaction commit
            # has an uncertain outcome, but not after a failed CREATE.
            created = True
        engine = create_engine(url, connect_args={"connect_timeout": 3, "options": (
            f"-c search_path={schema} -c statement_timeout=2000 -c lock_timeout=1000"
        )})
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, family TEXT)"))
            conn.execute(text("""CREATE TABLE resolved_series (
                feature_id INTEGER NOT NULL REFERENCES feature_registry(id)
                    DEFERRABLE INITIALLY DEFERRED,
                obs_date DATE NOT NULL, release_date DATE NOT NULL,
                vintage_date DATE NOT NULL, value DOUBLE PRECISION NOT NULL CHECK(value >= 0),
                source_priority_used INTEGER NOT NULL, conflict_flag BOOLEAN NOT NULL,
                conflict_detail JSONB,
                UNIQUE(feature_id, obs_date, vintage_date))"""))
            conn.execute(text("INSERT INTO feature_registry VALUES (1, 'rates')"))
        yield engine
    finally:
        _cleanup(root, engine, schema, created)


def _cleanup(root, engine, schema, created):
    failures = []
    if engine is not None:
        try:
            engine.dispose()
        except BaseException as exc:
            failures.append(exc)
    if created:
        try:
            with root.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        except BaseException as exc:
            failures.append(exc)
    try:
        root.dispose()
    except BaseException as exc:
        failures.append(exc)
    if failures:
        raise BaseExceptionGroup("fixture cleanup failed or outcome uncertain", failures)


@pytest.mark.parametrize("bad", [set(), {"worker"}, {"drop"}, {"root"}, {"worker", "drop", "root"}])
def test_cleanup_attempts_every_phase_and_reports_failures(bad):
    calls = []
    class Worker:
        def dispose(self):
            calls.append("worker")
            if "worker" in bad: raise RuntimeError("worker dispose failed")
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, statement):
            calls.append("drop")
            if "drop" in bad: raise RuntimeError("drop acknowledgement uncertain")
    class Root:
        def begin(self): return Conn()
        def dispose(self):
            calls.append("root")
            if "root" in bad: raise RuntimeError("root dispose failed")
    if bad:
        with pytest.raises(BaseExceptionGroup) as caught:
            _cleanup(Root(), Worker(), "resolver_test_123", True)
        assert len(caught.value.exceptions) == len(bad)
    else:
        _cleanup(Root(), Worker(), "resolver_test_123", True)
    assert calls == ["worker", "drop", "root"]


def _row(day=1, *, fid=1, value=1.0):
    d = date(2026, 1, day)
    return {"fid": fid, "od": d, "rd": d, "vd": d, "val": value,
            "src": 1, "cf": False, "cd": None}


def _rows(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT feature_id, obs_date, value FROM resolved_series ORDER BY obs_date")).all()


def test_native_success_and_mixed_duplicate_count(pg):
    from normalization.resolver import _flush_batch
    first = _flush_batch(pg, [_row()])
    assert first is None or (type(first) is int and first == 1)
    mixed = _flush_batch(pg, [_row(), _row(2)])
    assert mixed is None or (type(mixed) is int and mixed == 1)
    assert [(r[0], r[1], r[2]) for r in _rows(pg)] == [(1, date(2026, 1, 1), 1.0), (1, date(2026, 1, 2), 1.0)]


def test_native_statement_error_rolls_back_entire_batch(pg):
    from normalization.resolver import _ResolvedBatchError, _flush_batch
    with pytest.raises(_ResolvedBatchError) as caught:
        _flush_batch(pg, [_row(), _row(2, value=-1)])
    assert caught.value.outcome == "rolled_back"
    assert _rows(pg) == []


def test_native_deferred_constraint_fails_at_commit(pg):
    from normalization.resolver import _ResolvedBatchError, _flush_batch
    with pytest.raises(_ResolvedBatchError) as caught:
        _flush_batch(pg, [_row(fid=999)])
    assert caught.value.outcome == "unknown"
    assert _rows(pg) == []


class _AfterCommitAckLoss:
    """Commit for real, then simulate a lost acknowledgement at the boundary."""
    def __init__(self, engine):
        self.engine = engine

    def begin(self):
        real = self.engine.begin()
        class Manager:
            def __enter__(self):
                return real.__enter__()
            def __exit__(self, typ, value, tb):
                result = real.__exit__(typ, value, tb)
                if typ is None:
                    raise OperationalError("COMMIT", {}, OSError("synthetic lost acknowledgement"))
                return result
        return Manager()


def test_real_commit_with_simulated_lost_ack_is_unknown(pg):
    from normalization.resolver import _ResolvedBatchError, _flush_batch
    with pytest.raises(_ResolvedBatchError) as caught:
        _flush_batch(_AfterCommitAckLoss(pg), [_row()])
    assert caught.value.outcome == "unknown"
    assert len(_rows(pg)) == 1  # acknowledgement failed after a real commit


class _UnknownCount:
    def __init__(self, engine):
        self.engine = engine

    def begin(self):
        real = self.engine.begin()
        class Manager:
            def __enter__(self):
                conn = real.__enter__()
                class Proxy:
                    def execute(self, statement, params):
                        result = conn.execute(statement, params)
                        return SimpleNamespace(rowcount=-1, supports_sane_multi_rowcount=lambda: False,
                                               real_result=result)
                return Proxy()
            def __exit__(self, typ, value, tb):
                return real.__exit__(typ, value, tb)
        return Manager()


def test_real_commit_with_injected_unknown_driver_count(pg):
    from normalization.resolver import _flush_batch
    assert _flush_batch(_UnknownCount(pg), [_row()]) is None
    assert len(_rows(pg)) == 1


@pytest.mark.parametrize("ack_loss", [False, True])
def test_operator_watermark_after_real_write(pg, monkeypatch, ack_loss):
    """Real worker read and committed write; only scan/entity mapping are fixed."""
    import normalization.resolver as resolver
    monkeypatch.chdir(os.getcwd())  # operator import changes cwd; restore on teardown
    from scripts import hermes_operator as operator

    with pg.begin() as conn:
        conn.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, priority_rank INTEGER, name TEXT)"))
        conn.execute(text("""CREATE TABLE raw_series (id INTEGER PRIMARY KEY, series_id TEXT,
            obs_date DATE, value DOUBLE PRECISION, source_id INTEGER, pull_timestamp TIMESTAMPTZ,
            pull_status TEXT, raw_payload JSONB)"""))
        conn.execute(text("INSERT INTO source_catalog VALUES (1, 1, 'fixture')"))
        conn.execute(text("""INSERT INTO raw_series VALUES
            (1, 'DFF', '2026-01-01', 1.0, 1, '2026-01-02T00:00:00Z', 'SUCCESS', NULL)"""))

    class EntityMap:
        def __init__(self, engine): pass
        def get_feature_id(self, series_id): return 1
        def missing_feature_report(self):
            return {"lookups_missed": 0, "series_ids": 0, "unregistered_features": [], "top_series": []}

    through = datetime(2026, 1, 3, tzinfo=timezone.utc)
    monkeypatch.setattr(resolver, "EntityMap", EntityMap)
    monkeypatch.setattr(resolver.Resolver, "_RESOLVE_STATEMENT_TIMEOUT_MS", 2000)
    monkeypatch.setattr(resolver.Resolver, "_scan_series_ids", lambda *a, **k: (["DFF"], through, True))
    real_flush = resolver._flush_batch
    if ack_loss:
        monkeypatch.setattr(resolver, "_flush_batch", lambda engine, batch: real_flush(_AfterCommitAckLoss(engine), batch))
    monkeypatch.setattr(operator, "_run_with_timeout", lambda name, fn, timeout, state: (fn(), True))
    before = datetime(2026, 1, 1, tzinfo=timezone.utc)
    calls = []
    state = SimpleNamespace(last_resolution=before, record_task=lambda *a, **k: calls.append((a, k)))
    result = operator._run_resolution_step(pg, state)
    assert [(r[0], r[1], r[2]) for r in _rows(pg)] == [(1, date(2026, 1, 1), 1.0)]
    if ack_loss:
        assert result["errors"] == 1 and result["resolved"] is None and result["resolved_count_status"] == "unknown"
        assert result["batch_failure_outcomes"]["unknown"] == 1
        assert state.last_resolution == before
        assert calls and calls[-1][0][1] is False
    else:
        assert result["errors"] == 0 and state.last_resolution == through
        assert ((type(result["resolved"]) is int and result["resolved"] == 1
                 and result["resolved_count_status"] == "confirmed")
                or (result["resolved"] is None and result["resolved_count_status"] == "unknown"))
        assert calls and calls[-1][0][1] is True
