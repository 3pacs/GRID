"""GRID W4f — DB-gated proof of research_leases against REAL PostgreSQL row
locking.

Every test above this file (tests/test_write_fencing_leases.py) uses fake
connections. This file is the one place the actual guarantee -- a real
``SELECT ... FOR UPDATE`` genuinely serializing two connections -- is
exercised against a real, disposable database.

Skipped with an explicit reason until GRID_TEST_DB_URL is exported to a
throwaway Postgres instance (the lead's disposable DB). Per this task's
constraints: no local Postgres is assumed, nothing here runs against a
shared/production database, and the scratch table + research_leases row
this test writes are cleaned up unconditionally (try/finally) whether the
test passes or fails.

Run with:
    GRID_TEST_DB_URL=postgresql://user:pass@host:5432/throwaway_db \\
    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_write_fencing_live_db.py -q
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import governance.leases as leases  # noqa: E402

_GRID_TEST_DB_URL = os.environ.get("GRID_TEST_DB_URL")

pytestmark = pytest.mark.skipif(
    not _GRID_TEST_DB_URL,
    reason=(
        "GRID_TEST_DB_URL not set — this test needs a real, disposable "
        "PostgreSQL to exercise actual SELECT ... FOR UPDATE row-locking "
        "semantics (fakes cannot demonstrate real blocking). Skipped until "
        "the lead provides one; export GRID_TEST_DB_URL to run it."
    ),
)


@pytest.fixture
def live_engine():
    engine = create_engine(_GRID_TEST_DB_URL, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"GRID_TEST_DB_URL is set but not reachable: {exc}")
    leases.ensure_schema(engine)
    yield engine
    engine.dispose()


def _scratch_table_name() -> str:
    return f"_test_write_fencing_scratch_{uuid.uuid4().hex[:8]}"


def test_second_acquire_after_expiry_fences_the_firsts_guarded_write(live_engine):
    """The scenario named in the task brief precisely:

      1. Owner A acquires the "autoresearch" lease with a very short TTL
         and does NOT heartbeat it.
      2. The TTL elapses -- the lease is now genuinely expired in
         Postgres, not just logically stale.
      3. Owner B acquires the SAME lease name -- succeeds (the row really
         was expired), bumping the generation.
      4. Owner A, still holding its now-stale generation, attempts a
         guarded_write. It must raise OwnershipLost, and the scratch row
         its write function tries to insert must NOT exist afterward --
         real FOR UPDATE + generation/expiry checking, not a fake.
    """
    lease_name = f"test_lease_{uuid.uuid4().hex[:8]}"
    scratch_table = _scratch_table_name()

    with live_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {scratch_table} (id SERIAL PRIMARY KEY, note TEXT)"))

    try:
        gen_a = leases.acquire(live_engine, lease_name, "owner-a", ttl_seconds=1)
        assert gen_a == 1

        time.sleep(1.5)  # let owner A's lease actually expire in Postgres

        gen_b = leases.acquire(live_engine, lease_name, "owner-b", ttl_seconds=30)
        assert gen_b == gen_a + 1

        def _write(conn):
            conn.execute(text(f"INSERT INTO {scratch_table} (note) VALUES ('owner-a should never land')"))

        with pytest.raises(leases.OwnershipLost) as excinfo:
            leases.run_guarded(live_engine, lease_name, gen_a, _write)

        assert excinfo.value.generation == gen_a
        assert excinfo.value.current_generation == gen_b

        with live_engine.connect() as conn:
            count = conn.execute(text(f"SELECT count(*) FROM {scratch_table}")).scalar()
        assert count == 0, "owner A's write must not have committed anything"

        # Owner B, holding the genuinely current generation, CAN write.
        def _write_b(conn):
            conn.execute(text(f"INSERT INTO {scratch_table} (note) VALUES ('owner-b')"))

        leases.run_guarded(live_engine, lease_name, gen_b, _write_b)
        with live_engine.connect() as conn:
            count = conn.execute(text(f"SELECT count(*) FROM {scratch_table}")).scalar()
        assert count == 1
    finally:
        with live_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {scratch_table}"))
            conn.execute(text("DELETE FROM research_leases WHERE name = :n"), {"n": lease_name})


def test_acquire_refuses_to_take_over_a_live_lease(live_engine):
    """acquire() must not silently steal a lease that is still within its
    TTL -- LeaseHeld, not a false generation bump."""
    lease_name = f"test_lease_{uuid.uuid4().hex[:8]}"
    try:
        leases.acquire(live_engine, lease_name, "owner-a", ttl_seconds=30)
        with pytest.raises(leases.LeaseHeld):
            leases.acquire(live_engine, lease_name, "owner-b", ttl_seconds=30)
    finally:
        with live_engine.begin() as conn:
            conn.execute(text("DELETE FROM research_leases WHERE name = :n"), {"n": lease_name})
