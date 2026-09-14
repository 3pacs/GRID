"""``scripts/assimilator.py`` must write the real analytical_snapshots store.

``store_insight`` imported ``SnapshotStore`` from ``store.snapshots``, which
exports ``AnalyticalSnapshotStore`` and has never had an alias. Every call
raised ``ImportError: cannot import name 'SnapshotStore'``, so no
HUMAN_LLM_QUERY insight ever reached the table from this path.

It went unnoticed because of what surrounded the call, not the name:

* the ``ImportError`` propagated out of ``process_completed_jobs`` entirely,
  so one bad job aborted every remaining job in the batch, and
* the daemon loop caught it with a bare ``log.error("{e}")`` — no traceback,
  no job id — then slept and retried the same stuck job forever, and
* even with the name right, ``save_snapshot`` catches its own exceptions and
  returns ``None``; ``store_insight`` ignored that and logged success, so a
  failed INSERT would still get the job marked VALID → ASSIMILATED.

These tests pin all four. The import check resolves the name the script
actually reaches for against the real ``store.snapshots`` module — asserting
against a mocked store would have passed happily on the broken code.

Same family as ``tests/test_parse_datasets_snapshots.py`` (a writer naming
columns ``analytical_snapshots`` never had), and loaded the same way because
``scripts/`` is not a package.
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import re
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "assimilator.py"
SNAPSHOTS_MODULE = REPO_ROOT / "store" / "snapshots.py"


# ---------------------------------------------------------------------------
# Static reading of the script — no third-party imports, runs in a bare env
# ---------------------------------------------------------------------------

def _function_def(name: str) -> ast.FunctionDef:
    """Return the top-level ``def name`` node of scripts/assimilator.py."""
    tree = ast.parse(SCRIPT.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"scripts/assimilator.py has no top-level {name}()")


def _names_imported_from_store_snapshots() -> list[str]:
    """Names ``store_insight`` imports from ``store.snapshots``.

    Read from the AST rather than hard-coded, so the check fails for *any*
    wrong name, not only the one that was wrong on the day it was written.
    """
    return [
        alias.name
        for node in ast.walk(_function_def("store_insight"))
        if isinstance(node, ast.ImportFrom) and node.module == "store.snapshots"
        for alias in node.names
    ]


def _save_snapshot_keywords() -> list[str]:
    """Keyword argument names the ``save_snapshot`` call site passes."""
    for node in ast.walk(_function_def("store_insight")):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "save_snapshot"
        ):
            assert not node.args, (
                "the call site passes save_snapshot arguments positionally; "
                "these tests (and the reader) rely on it being keyword-only"
            )
            return [kw.arg for kw in node.keywords if kw.arg]
    raise AssertionError("store_insight() no longer calls save_snapshot()")


def _toplevel_names(path: Path) -> set[str]:
    """Names bound at module level in ``path`` — classes, defs, assignments."""
    names: set[str] = set()
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(
                t.id for t in node.targets if isinstance(t, ast.Name)
            )
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def test_imported_name_is_defined_in_store_snapshots_source() -> None:
    """The bare-environment guard: a pure source cross-check, no deps needed.

    This is the cheapest form of the regression and runs even where numpy,
    pandas and sqlalchemy are absent.
    """
    imported = _names_imported_from_store_snapshots()
    assert imported, "store_insight no longer imports from store.snapshots"

    defined = _toplevel_names(SNAPSHOTS_MODULE)
    for name in imported:
        assert name in defined, (
            f"store_insight imports '{name}' from store.snapshots, which "
            f"does not define it. store/snapshots.py exports the store class "
            f"as 'AnalyticalSnapshotStore'."
        )


def test_the_broken_name_is_gone() -> None:
    """``SnapshotStore`` was never exported — not under any alias.

    Matched on a word boundary so the correct ``AnalyticalSnapshotStore``,
    which contains it as a substring, does not trip the check.
    """
    assert "SnapshotStore" not in _names_imported_from_store_snapshots()
    assert not re.search(r"\bSnapshotStore\b", SCRIPT.read_text()), (
        "scripts/assimilator.py still names a symbol store.snapshots "
        "does not export"
    )


# ---------------------------------------------------------------------------
# The real store.snapshots module — the check a mock would have passed
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def snapshots_module() -> ModuleType:
    """The real ``store.snapshots``, imported for keeps — never a mock."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    try:
        import store.snapshots as module
    except ImportError as exc:  # pragma: no cover - bare env without numpy/sqlalchemy
        pytest.skip(f"store.snapshots deps unavailable: {exc}")
    return module


def test_imported_name_resolves_against_the_real_module(snapshots_module) -> None:
    """Resolve the symbol exactly as ``store_insight`` does at runtime."""
    for name in _names_imported_from_store_snapshots():
        assert hasattr(snapshots_module, name), (
            f"`from store.snapshots import {name}` raises ImportError — "
            f"this is the bug, and it fails at the first call to "
            f"store_insight(), not at import time"
        )


def test_the_imported_store_can_actually_save_snapshots(snapshots_module) -> None:
    """Guard against importing a real-but-wrong name."""
    for name in _names_imported_from_store_snapshots():
        symbol = getattr(snapshots_module, name)
        assert inspect.isclass(symbol), f"{name} is not a class"
        assert callable(getattr(symbol, "save_snapshot", None)), (
            f"{name} has no save_snapshot() — store_insight calls one"
        )


def test_constructor_call_site_matches_the_real_signature(snapshots_module) -> None:
    """``AnalyticalSnapshotStore(db_engine=...)`` binds without TypeError."""
    store_cls = snapshots_module.AnalyticalSnapshotStore
    inspect.signature(store_cls).bind(db_engine=MagicMock())


def test_save_snapshot_call_site_matches_the_real_signature(snapshots_module) -> None:
    """Every keyword the call site passes is a real parameter."""
    store_cls = snapshots_module.AnalyticalSnapshotStore
    signature = inspect.signature(store_cls.save_snapshot)
    keywords = _save_snapshot_keywords()

    assert "category" in keywords and "payload" in keywords, (
        "category and payload are required by save_snapshot"
    )
    # Raises TypeError if a keyword was renamed or removed upstream.
    signature.bind(MagicMock(), **{kw: MagicMock() for kw in keywords})


# ---------------------------------------------------------------------------
# The script, loaded and run against the real store over a mock engine
# ---------------------------------------------------------------------------

def _mock_engine(returned_row: tuple | None = (77,)):
    """MagicMock engine whose ``begin()`` yields one observable connection.

    Mirrors ``tests/test_snapshots.py`` so the real ``AnalyticalSnapshotStore``
    runs its real SQL without a live PostgreSQL. ``returned_row=None`` makes
    the INSERT come back empty, which is how ``save_snapshot`` returns None.
    """
    engine, conn = MagicMock(), MagicMock()
    conn.execute.return_value.fetchone.return_value = returned_row
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


@pytest.fixture(scope="module")
def assimilator() -> ModuleType:
    """Import scripts/assimilator.py as a module — ``scripts/`` is not a package.

    Prefers the real ``db``; falls back to a stub when psycopg2 or the config
    stack is absent, since ``store_insight`` only needs ``get_engine``.
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    try:
        import db  # noqa: F401
    except Exception:
        stub = ModuleType("db")
        stub.get_engine = lambda *a, **k: None  # type: ignore[attr-defined]
        sys.modules["db"] = stub

    spec = importlib.util.spec_from_file_location("assimilator", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ImportError as exc:  # pragma: no cover - bare env without requests/loguru
        pytest.skip(f"assimilator deps unavailable: {exc}")
    return module


@pytest.fixture
def engine_patch(monkeypatch):
    """Point ``store_insight``'s ``from db import get_engine`` at a mock."""
    import db

    def _install(returned_row: tuple | None = (77,)):
        engine, conn = _mock_engine(returned_row)
        monkeypatch.setattr(db, "get_engine", lambda *a, **k: engine)
        return engine, conn

    return _install


JOB = {"id": 42, "name": "chatgpt-macro-sweep", "params": {}}
PARSED = {"parsed": True, "model_used": "gpt-5", "sentiment": 0.4, "regime": "risk-on"}


def test_store_insight_writes_through_the_real_store(assimilator, engine_patch) -> None:
    """End to end over the real AnalyticalSnapshotStore: the row is inserted."""
    _engine, conn = engine_patch()

    snapshot_id = assimilator.store_insight(JOB, PARSED)

    assert snapshot_id == 77
    inserts = [
        call for call in conn.execute.call_args_list
        if "INSERT INTO analytical_snapshots" in str(call.args[0])
    ]
    assert len(inserts) == 1, "the insight did not reach analytical_snapshots"
    params = inserts[0].args[1]
    assert params["cat"] == assimilator.SNAPSHOT_CATEGORY
    assert params["sub"] == JOB["name"]


def test_store_insight_raises_when_the_row_never_landed(assimilator, engine_patch) -> None:
    """``save_snapshot`` swallows its own errors and returns None.

    Without this, a failed INSERT logged success and the job was marked
    ASSIMILATED with nothing in the table.
    """
    engine_patch(returned_row=None)

    with pytest.raises(RuntimeError, match="analytical_snapshots"):
        assimilator.store_insight(JOB, PARSED)


# ---------------------------------------------------------------------------
# The swallow: what process_completed_jobs does when storing fails
# ---------------------------------------------------------------------------

class _Response:
    def __init__(self, payload): self._payload = payload
    def raise_for_status(self) -> None: pass
    def json(self): return self._payload


@pytest.fixture
def coordinator(assimilator, monkeypatch):
    """Stub the coordinator HTTP + compute_results reads for one batch of jobs.

    Returns the list of URLs POSTed, so a test can assert that a job was —
    or was not — marked VALID/ASSIMILATED.
    """
    def _install(jobs: list[dict]):
        posted: list[str] = []
        monkeypatch.setattr(assimilator, "requests", SimpleNamespace(
            get=lambda url, **kw: _Response(jobs if url.endswith("/jobs") else {}),
            post=lambda url, **kw: (posted.append(url), _Response({}))[1],
        ))

        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "output": {"response": "raw model text", "model_used": "gpt-5"}
        }
        connection = MagicMock()
        connection.cursor.return_value = cursor

        coordinator_stub = ModuleType("scripts.compute_coordinator")
        coordinator_stub.get_conn = lambda: connection  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "scripts.compute_coordinator", coordinator_stub,
        )
        scripts_pkg = sys.modules.get("scripts") or ModuleType("scripts")
        monkeypatch.setitem(sys.modules, "scripts", scripts_pkg)
        monkeypatch.setattr(
            scripts_pkg, "compute_coordinator", coordinator_stub, raising=False,
        )
        monkeypatch.setattr(
            assimilator, "parse_response", lambda *a, **k: dict(PARSED),
        )
        return posted

    return _install


def _boom(*_args, **_kwargs):
    raise ImportError("cannot import name 'SnapshotStore' from 'store.snapshots'")


def test_a_failed_store_does_not_mark_the_job_assimilated(
    assimilator, coordinator, monkeypatch,
) -> None:
    """A job whose insight was dropped must stay COMPLETED for a retry."""
    posted = coordinator([dict(JOB)])
    monkeypatch.setattr(assimilator, "store_insight", _boom)

    processed, failed = assimilator.process_completed_jobs()

    assert (processed, failed) == (0, 1)
    assert posted == [], (
        "the job was marked validated/assimilated even though nothing "
        "reached analytical_snapshots"
    )


def test_a_failed_store_does_not_abort_the_rest_of_the_batch(
    assimilator, coordinator, monkeypatch,
) -> None:
    """The ImportError used to propagate out, so job #2 was never attempted."""
    jobs = [{**JOB, "id": 1}, {**JOB, "id": 2}]
    posted = coordinator(jobs)

    def _fail_first(job, parsed):
        if job["id"] == 1:
            _boom()
        return 77

    monkeypatch.setattr(assimilator, "store_insight", _fail_first)

    processed, failed = assimilator.process_completed_jobs()

    assert (processed, failed) == (1, 1)
    assert [url for url in posted if "/jobs/1/" in url] == []
    assert [url for url in posted if "/jobs/2/" in url] == [
        "http://localhost:8100/jobs/2/validate",
        "http://localhost:8100/jobs/2/assimilate",
    ]


def test_a_failed_store_is_logged_at_error_with_a_traceback(
    assimilator, coordinator, monkeypatch,
) -> None:
    """CLAUDE.md log levels: a name the module does not export is a bug.

    It must not be downgraded to a warning among the transient network
    failures, and the traceback has to survive so errors.jsonl names a root
    cause rather than a bare message.
    """
    from loguru import logger

    coordinator([dict(JOB)])
    monkeypatch.setattr(assimilator, "store_insight", _boom)

    records = []
    sink_id = logger.add(lambda m: records.append(m.record), level="DEBUG")
    try:
        assimilator.process_completed_jobs()
    finally:
        logger.remove(sink_id)

    store_failures = [
        r for r in records if "Failed to store insight" in r["message"]
    ]
    assert len(store_failures) == 1, "the store failure was not logged at all"
    record = store_failures[0]
    assert record["level"].name == "ERROR", (
        f"logged at {record['level'].name}; an import of a name that does not "
        f"exist is an application bug, not a transient warning"
    )
    assert record["exception"] is not None, (
        "logged without opt(exception=True), so the GitSink records the "
        "message but not the traceback"
    )


def test_once_exits_nonzero_when_an_insight_was_dropped(
    assimilator, monkeypatch,
) -> None:
    """A single pass that persisted nothing must not look clean to a scheduler."""
    monkeypatch.setattr(sys, "argv", ["assimilator.py", "--once"])

    monkeypatch.setattr(assimilator, "process_completed_jobs", lambda: (0, 1))
    assert assimilator.main() == 1

    monkeypatch.setattr(assimilator, "process_completed_jobs", lambda: (3, 0))
    assert assimilator.main() == 0


def test_once_routes_a_raise_through_the_error_sink(assimilator, monkeypatch) -> None:
    """A raise escaping `--once` would bypass errors.jsonl entirely.

    The daemon loop has this guard; `--once` did not. `config.py` attaches the
    GitSink with `log.add(_git_sink.write, level="ERROR")` — it only ever sees
    *logged* ERROR records, and nothing installs a `sys.excepthook`, so an
    uncaught traceback reaches stderr and the operational health signal never
    records that the pass failed.
    """
    from loguru import logger

    monkeypatch.setattr(sys, "argv", ["assimilator.py", "--once"])

    def _raising_pass():
        raise RuntimeError("coordinator returned a malformed job")

    monkeypatch.setattr(assimilator, "process_completed_jobs", _raising_pass)

    records = []
    sink_id = logger.add(lambda m: records.append(m.record), level="DEBUG")
    try:
        assert assimilator.main() == 1, "a crashed pass must not exit 0"
    finally:
        logger.remove(sink_id)

    errors = [r for r in records if r["level"].name == "ERROR"]
    assert len(errors) == 1, "the failed pass was not logged at ERROR"
    assert errors[0]["exception"] is not None, (
        "logged without opt(exception=True), so errors.jsonl gets the message "
        "but not the traceback"
    )
