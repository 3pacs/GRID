"""Acceptance-mode plumbing for tests/integration/test_research_vertical_slice.py.

The vertical slice's "required behaviours" (predict_fn genuinely driving
the result, a cheating predictor being rejected, the batched PIT-vintages
fetch, feature-order invariance, the leakage-safe chronological holdout,
and the real-Postgres PIT proof) are feature-detected against whatever
code is actually installed on this tree. By default (development mode)
a missing required behaviour SKIPS with an explicit reason — that is what
keeps this suite green on lanes that haven't been composed with the other
Fable branches yet.

Acceptance mode flips that: on the final composed tree, every required
behaviour must actually be present and proven, so a missing one must be a
loud, red FAILURE, never a quiet skip that could be mistaken for "nothing
to prove here." Turn it on with either:

    GRID_ACCEPTANCE_TREE=1 pytest tests/integration/...
    pytest --acceptance tests/integration/...

(the CLI flag just sets the env var for this process at collection time,
so every downstream check has exactly one source of truth:
``acceptance_mode()``).

``record_required_behaviour``/``required_behaviour_results`` are the
shared, in-process bookkeeping the summary test
(``TestAcceptanceSummary`` in test_research_vertical_slice.py, which runs
last by source position in that file) reads to write
``tests/integration/acceptance_summary.json`` — git-ignored via
``tests/integration/.gitignore``, since it is a run artifact, not
committed test code.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

ACCEPTANCE_ENV = "GRID_ACCEPTANCE_TREE"

# In-process only — reset per pytest session (module-level dict, not
# persisted between separate `pytest` invocations). Keyed by required
# behaviour name; see test_research_vertical_slice.py's module docstring
# for the canonical name list.
_RESULTS: dict[str, dict[str, Any]] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--acceptance",
        action="store_true",
        default=False,
        help=(
            "Run tests/integration in acceptance mode: every required "
            "behaviour hard-fails (instead of skipping) when missing on "
            "this tree. Equivalent to GRID_ACCEPTANCE_TREE=1."
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("--acceptance"):
        os.environ[ACCEPTANCE_ENV] = "1"


def acceptance_mode() -> bool:
    """True iff GRID_ACCEPTANCE_TREE is set truthy (env var or --acceptance,
    which sets the same env var in pytest_configure above)."""
    return os.environ.get(ACCEPTANCE_ENV, "") not in ("", "0", "false", "False")


def record_required_behaviour(name: str, *, executed: bool, passed: bool | None) -> None:
    """Record one required behaviour's outcome for this session.

    ``passed=None`` means "skipped, not evaluated" (development mode,
    capability absent) -- distinct from ``passed=False`` (acceptance mode,
    capability absent, hard-failed) and ``passed=True`` (ran and proved
    the behaviour, in either mode).
    """
    _RESULTS[name] = {"executed": executed, "passed": passed}


def required_behaviour_results() -> dict[str, dict[str, Any]]:
    """A shallow copy of every required behaviour recorded so far this
    session -- read by the summary test, never mutated by it."""
    return {k: dict(v) for k, v in _RESULTS.items()}


def require_capability(available: bool, name: str, reason: str) -> None:
    """The acceptance-mode gate every required-behaviour test calls FIRST,
    before any real assertion.

    - ``available=True``: no-op. The caller proceeds to its real
      assertions and records its own pass/fail afterwards.
    - ``available=False``, acceptance mode OFF (default/development):
      records ``executed=False, passed=None`` and skips with ``reason`` --
      unchanged pre-acceptance-mode behaviour.
    - ``available=False``, acceptance mode ON: records
      ``executed=True, passed=False`` and hard-FAILS with the mandated
      message ``"required behaviour missing on this tree: <name>"`` --
      feature detection must never turn missing required behaviour into a
      passing (or quietly-skipped) run.
    """
    if available:
        return
    if acceptance_mode():
        record_required_behaviour(name, executed=True, passed=False)
        pytest.fail(f"required behaviour missing on this tree: {name}")
    record_required_behaviour(name, executed=False, passed=None)
    pytest.skip(reason)


@pytest.fixture
def acceptance_pg_engine():
    """Postgres access for the one DB-gated required behaviour
    (``postgres_pit_vintage_proof``), with acceptance mode's own, stricter
    rule layered on top of the shared ``tests/conftest.py::pg_engine``
    pattern: acceptance mode requires the real-database proof, so an
    unset ``GRID_TEST_DB_URL`` or an unreachable Postgres is a hard FAIL
    here, not a skip. Outside acceptance mode this behaves exactly like
    the shared ``pg_engine`` fixture (skip with reason on any failure) --
    intentionally NOT the same fixture object, and this file never edits
    ``tests/conftest.py``, so the ~90 other pg_engine-gated tests
    elsewhere in the suite are completely unaffected by acceptance mode.
    """
    from sqlalchemy import create_engine, text

    behaviour = "postgres_pit_vintage_proof"

    def _db_url() -> str:
        # Mirrors tests/conftest.py::_db_url's default exactly, so
        # non-acceptance behaviour (GRID_TEST_DB_URL unset, no local
        # Postgres) is identical to the shared fixture's.
        return os.environ.get("GRID_TEST_DB_URL", "postgresql://grid_user:changeme@localhost:5432/grid")

    if acceptance_mode() and "GRID_TEST_DB_URL" not in os.environ:
        record_required_behaviour(behaviour, executed=True, passed=False)
        pytest.fail(
            f"required behaviour missing on this tree: {behaviour} "
            "(GRID_TEST_DB_URL is unset -- acceptance mode requires the "
            "real-database proof; the lead provides the URL)"
        )

    try:
        engine = create_engine(_db_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        if acceptance_mode():
            record_required_behaviour(behaviour, executed=True, passed=False)
            pytest.fail(f"required behaviour missing on this tree: {behaviour} ({exc})")
        record_required_behaviour(behaviour, executed=False, passed=None)
        pytest.skip("PostgreSQL not available")

    yield engine
    engine.dispose()
