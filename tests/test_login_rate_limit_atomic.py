"""Login rate limiting must hold under concurrent requests.

``login``/``register`` used to be ``async def`` routes, so grid-api's event
loop ran them one at a time and a separate check-then-record was safe. As
plain ``def`` routes in the threadpool, a burst could all pass the check
before any of them recorded. The check and the record are now one atomic
step: a per-IP advisory-locked transaction in Postgres, or a locked
read-modify-write in the shelve fallback.
"""

from __future__ import annotations

import os
import threading

import pytest

os.environ.setdefault("DB_PASSWORD", "testpass")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-ci")

from fastapi import HTTPException  # noqa: E402

from api import auth  # noqa: E402


def _race(fn, n: int) -> list[object]:
    barrier = threading.Barrier(n)
    out: list[object] = []

    def run() -> None:
        barrier.wait()
        try:
            fn()
            out.append("ok")
        except HTTPException as exc:
            out.append(exc.status_code)

    threads = [threading.Thread(target=run) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    return out


def test_shelve_fallback_admits_exactly_the_limit(monkeypatch, tmp_path):
    def db_down(ip):  # noqa: ARG001
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(auth, "_consume_login_attempt_db", db_down)
    monkeypatch.setattr(auth, "_rate_limit_path", str(tmp_path / "rl"))

    out = _race(lambda: auth._consume_login_attempt("10.0.0.9"), n=20)

    assert out.count("ok") == auth._RATE_LIMIT_MAX
    assert out.count(429) == 20 - auth._RATE_LIMIT_MAX


class _FakeCursor:
    def __init__(self, log: list[str], total: int) -> None:
        self.log, self.total = log, total

    def execute(self, sql: str, params=None) -> None:  # noqa: ARG002
        self.log.append(" ".join(sql.split())[:40])

    def fetchone(self):
        return (self.total,)


class _FakeConn:
    def __init__(self, total: int) -> None:
        self.log: list[str] = []
        self.total = total

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return _FakeCursor(self.log, self.total)

    def close(self) -> None:
        pass


@pytest.mark.parametrize("total,allowed", [(0, True), (auth._RATE_LIMIT_MAX - 1, True),
                                           (auth._RATE_LIMIT_MAX, False)])
def test_db_path_locks_then_checks_then_records_in_one_transaction(monkeypatch, total, allowed):
    conn = _FakeConn(total)
    monkeypatch.setattr(auth, "_lazy_init", lambda: None)
    monkeypatch.setattr(auth, "_get_db_conn", lambda: conn)

    assert auth._consume_login_attempt_db("10.0.0.1") is allowed

    assert conn.log[0].startswith("SELECT pg_advisory_xact_lock")
    assert any(s.startswith("SELECT COALESCE(SUM(count)") for s in conn.log)
    inserted = any(s.startswith("INSERT INTO grid_rate_limits") for s in conn.log)
    assert inserted is allowed, "a blocked attempt is not recorded"


def test_login_and_register_use_the_atomic_helper():
    import inspect

    for route in (auth.login, auth.register):
        src = inspect.getsource(route)
        assert "_consume_login_attempt(client_ip)" in src
        assert "_check_rate_limit" not in src and "_record_login_attempt" not in src
