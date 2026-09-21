"""Regression: `main()`'s chunk-totals accumulator must never crash on a
counter key it was not seeded with.

Proof run 4 (coordinator): `test_preservation_legacy_rows_survive_a_real_
scorer_run` (tests/test_oracle_null_policy_pg.py) crashed
`sot.main(["--chunk-size", "500"])` with
``KeyError: 'unscorable_entry_price'`` at what is now
``totals[k] = totals.get(k, 0) + v``. ``score_one_chunk``'s own counters
dict has always carried ``unscorable_entry_price`` (its own
belt-and-braces counter -- rows its ``WHERE`` should already have
excluded), but ``main()``'s ``totals`` accumulator was seeded without it,
so the very first chunk that reported it -- with ANY value, since plain
``totals[k] += v`` raises on an unseen key regardless of whether it is
zero -- crashed the whole run. Invisible to ``score_one_chunk``'s own
fixture tests (e.g.
``tests/test_oracle_null_readers.py::TestScorerNullEntry``), which call
that function directly and never exercise ``main()``'s own accumulation
loop.

This test runs ``main()`` genuinely end-to-end. Real Postgres-flavoured SQL
(``created_at::date``, etc.) means SQLite cannot stand in here the way it
does in tests/test_oracle_null_readers.py, so the DB is a small fake that
inspects the SQL text and returns canned results -- the same pattern
tests/test_oracle_publish_entry_price.py's ``_Engine``/``_Conn``/``_Result``
already use for this exact module's Postgres-flavoured queries.
``score_one_chunk`` itself is replaced with a fake that returns every
counter key it can produce, including one ``main()`` does not pre-seed
``totals`` with, so a regression back to ``totals[k] += v`` fails loudly
without needing a specific DB row shape to happen to trigger the real code
path that produces that key.
"""

from __future__ import annotations

from datetime import date

import scripts.score_oracle_trades as sot


class _Result:
    def __init__(self, rows=(), scalar=None, rowcount=0):
        self._rows = list(rows)
        self._scalar = scalar
        self.rowcount = rowcount

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _Conn:
    def __init__(self, pending_rows):
        self._pending_rows = pending_rows

    def execute(self, stmt, params=None):  # noqa: ARG002 - params unused by the fake
        sql = " ".join(str(stmt).split())

        if "created_at::date, expiry" in sql:
            # Step 0: the pending-predictions pull.
            return _Result(rows=self._pending_rows)
        if sql.startswith("SELECT COUNT(*) FROM oracle_predictions"):
            # Step 4's pre-count.
            return _Result(scalar=len(self._pending_rows))
        if sql.startswith("UPDATE"):
            # Step 3's direction fixes and no_data sweep.
            return _Result(rowcount=0)
        # Step 5's scorecard reads -- empty is a valid, ordinary result.
        return _Result(rows=[])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def __init__(self, pending_rows):
        self._pending_rows = pending_rows

    def connect(self):
        return _Conn(self._pending_rows)

    def begin(self):
        return _Conn(self._pending_rows)


def test_main_accumulates_every_counter_key_score_one_chunk_can_report(monkeypatch):
    """main() must not crash regardless of which keys a chunk's counters
    dict carries -- including one `totals` was not pre-seeded with."""
    # One ordinary pending, expired row -- just needs to exist so Step 0's
    # `rows` list is non-empty (main() calls min()/max() on it before the
    # chunked loop even starts).
    pending_rows = [
        ("p-1", "AAA", "CALL", 100.0, date(2026, 9, 1), date(2026, 9, 20)),
    ]
    fake_engine = _Engine(pending_rows)

    monkeypatch.setattr(sot, "create_engine", lambda *a, **kw: fake_engine)
    monkeypatch.setattr(sot, "fetch_prices", lambda tickers, start, end: {})

    calls: list[int] = []

    def _fake_score_one_chunk(_conn, *, engine, prices, today, chunk_size):
        calls.append(1)
        if len(calls) == 1:
            # Every key score_one_chunk's real counters dict can carry,
            # including a hypothetical future one ("a_future_counter") to
            # prove the fix is genuinely generic, not just patched for
            # unscorable_entry_price specifically.
            return {
                "scored": 1, "hits": 1, "misses": 0, "partials": 0,
                "skipped": 0, "no_data": 1, "unscorable_entry_price": 1,
                "a_future_counter": 3,
                "fetched": 1,
            }
        # Second call: nothing left -- terminates main()'s while loop.
        return {
            "scored": 0, "hits": 0, "misses": 0, "partials": 0,
            "skipped": 0, "no_data": 0, "unscorable_entry_price": 0,
            "a_future_counter": 0,
            "fetched": 0,
        }

    monkeypatch.setattr(sot, "score_one_chunk", _fake_score_one_chunk)

    # Must not raise KeyError (or anything else).
    sot.main(["--chunk-size", "500"])

    assert len(calls) == 2, "expected exactly one scored chunk then one empty chunk"
