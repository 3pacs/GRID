"""The distinct-series_id scan must not hold one long-lived transaction.

``Resolver.resolve_pending`` opens with "which series have new data in this
window", and that used to be a single ``SELECT DISTINCT`` over the whole
window. Measured on griddb 2026-09-14 (``raw_series``: 511 GB, 1.93e9 rows),
sampling ``pg_stat_activity`` every 20s caught the rolling 2-day scan at 371s,
391s and 411s, waiting on ``IO:DataFileRead`` throughout; the identical window
re-run ~2 minutes later took 33.9s off warm cache.

The latency is secondary. While that statement runs its backend holds an xmin,
and ``CREATE``/``DROP INDEX CONCURRENTLY`` — on *any* table in the database —
wait for every transaction older than themselves. A 400s scan is a 400s window
in which no concurrent DDL can finish, which is what defeated the
``analytical_snapshots`` index work on 2026-09-13.

So the scan is sliced, and these tests pin the two properties that makes it
worth anything: the slices still cover exactly the requested window (so the
result is unchanged), and no single statement spans a wide window (so no long
transaction). Both drive the real methods with a recording double rather than
asserting on constants — a constant-pinning test would pass against the
unsliced version.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from normalization.resolver import DISTINCT_SCAN_SLICE_HOURS, Resolver

WIDTH = timedelta(hours=DISTINCT_SCAN_SLICE_HOURS)
T0 = datetime(2026, 9, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Recording doubles — no database
# ---------------------------------------------------------------------------

class _Result:
    def __init__(self, rows: list[tuple[str]]):
        self._rows = rows

    def fetchall(self) -> list[tuple[str]]:
        return self._rows


class _Conn:
    """Records every statement and its bound parameters."""

    def __init__(self, log: list[dict[str, Any]], rows_for: Any):
        self._log = log
        self._rows_for = rows_for

    def execute(self, statement: Any, params: dict[str, Any] | None = None):
        sql = " ".join(str(statement).split())
        if "set_config" in sql:
            return _Result([])
        self._log.append(dict(params or {}))
        return _Result([(s,) for s in self._rows_for(params or {})])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Engine:
    def __init__(self, rows_for=lambda params: []):
        self.calls: list[dict[str, Any]] = []
        self._rows_for = rows_for

    def begin(self):
        return _Conn(self.calls, self._rows_for)


def _resolver(engine: _Engine) -> Resolver:
    """A Resolver bound to the double, bypassing __init__'s engine setup."""
    resolver = Resolver.__new__(Resolver)
    resolver.engine = engine  # type: ignore[assignment]
    return resolver


# ---------------------------------------------------------------------------
# _scan_slices
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_slices_are_contiguous_and_cover_the_window():
    """No gap and no overlap, or the scan silently drops or repeats series."""
    until = T0 + timedelta(days=2)
    slices = Resolver._scan_slices(T0, until, WIDTH)

    assert slices[0][0] == T0, "first slice must start at `since`"
    assert slices[-1][1] == until, "last slice must end at `until`"
    for (_, prev_end), (next_start, _) in zip(slices, slices[1:]):
        assert prev_end == next_start, f"gap/overlap at {prev_end} -> {next_start}"


@pytest.mark.unit
def test_a_wide_window_is_actually_split():
    """The whole point. One slice for two days would change nothing."""
    slices = Resolver._scan_slices(T0, T0 + timedelta(days=2), WIDTH)
    assert len(slices) > 1, "a 2-day window must not be one statement"
    for start, end in slices[:-1]:
        assert end - start <= WIDTH


@pytest.mark.unit
def test_unbounded_window_keeps_its_open_end():
    """`until=None` must stay None on the final slice.

    Capping it at "now" would start dropping rows written while the scan runs
    — rows the single-statement version did see, because its upper bound was
    'infinity'.
    """
    slices = Resolver._scan_slices(T0, None, WIDTH)
    assert slices[-1][1] is None
    assert slices[0][0] == T0


@pytest.mark.unit
def test_window_narrower_than_a_slice_stays_one_statement():
    slices = Resolver._scan_slices(T0, T0 + timedelta(hours=1), WIDTH)
    assert slices == [(T0, T0 + timedelta(hours=1))]


@pytest.mark.unit
def test_incomparable_bounds_fall_back_to_one_pass():
    """A naive `since` against an aware `until` raises TypeError on compare.

    That combination was already broken in the SQL, so slicing is not the
    place to start raising on it — fall back to the single pass and let the
    behaviour stay exactly as it was.
    """
    naive = datetime(2026, 9, 12, 0, 0)
    aware = datetime(2026, 9, 14, 0, 0, tzinfo=timezone.utc)
    assert Resolver._scan_slices(naive, aware, WIDTH) == [(naive, aware)]


@pytest.mark.unit
def test_until_at_or_before_since_is_one_empty_pass():
    assert Resolver._scan_slices(T0, T0, WIDTH) == [(T0, T0)]
    earlier = T0 - timedelta(days=1)
    assert Resolver._scan_slices(T0, earlier, WIDTH) == [(T0, earlier)]


# ---------------------------------------------------------------------------
# _distinct_series_ids — the real method, against the double
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_each_slice_is_its_own_statement_with_its_own_bounds():
    engine = _Engine()
    until = T0 + timedelta(days=2)
    _resolver(engine)._distinct_series_ids({"since": T0, "until": until})

    expected = Resolver._scan_slices(T0, until, WIDTH)
    assert len(engine.calls) == len(expected), (
        f"expected one statement per slice, got {len(engine.calls)} for "
        f"{len(expected)} slices"
    )
    assert [(c["since"], c["until"]) for c in engine.calls] == expected


@pytest.mark.unit
def test_no_statement_spans_the_whole_wide_window():
    """The regression guard: this is what the unsliced version did.

    Fails against a single ``SELECT DISTINCT`` over ``[since, until)``.
    """
    engine = _Engine()
    until = T0 + timedelta(days=2)
    _resolver(engine)._distinct_series_ids({"since": T0, "until": until})

    for call in engine.calls:
        span = call["until"] - call["since"] if call["until"] else None
        assert span is None or span <= WIDTH, (
            f"a statement covered {span}, which is the long transaction this "
            "change exists to remove"
        )


@pytest.mark.unit
def test_result_is_the_deduplicated_union_across_slices():
    """Union of DISTINCT per slice == DISTINCT over the union."""
    per_slice = [["a", "b"], ["b", "c"], ["c"], ["d", "a"]]
    counter = {"n": 0}

    def rows_for(_params):
        got = per_slice[counter["n"] % len(per_slice)]
        counter["n"] += 1
        return got

    engine = _Engine(rows_for=rows_for)
    result = _resolver(engine)._distinct_series_ids(
        {"since": T0, "until": T0 + timedelta(days=2)}
    )
    assert result == sorted({"a", "b", "c", "d"})


@pytest.mark.unit
def test_missing_until_key_is_treated_as_unbounded():
    """`_window_bounds` always supplies `until`, but a caller need not."""
    engine = _Engine()
    _resolver(engine)._distinct_series_ids({"since": T0})
    assert engine.calls, "no statement was issued"
    assert engine.calls[-1]["until"] is None


@pytest.mark.unit
def test_statement_timeout_is_raised_inside_every_slice():
    """Each slice is its own transaction, so each needs its own SET LOCAL.

    db.py pins statement_timeout to 120s on every connection; a slice that
    did not lift it would be killed well before it finished.
    """
    seen: list[str] = []

    class _TimeoutConn(_Conn):
        def execute(self, statement, params=None):
            sql = " ".join(str(statement).split())
            if "set_config" in sql:
                seen.append(sql)
            return super().execute(statement, params)

    class _TimeoutEngine(_Engine):
        def begin(self):
            return _TimeoutConn(self.calls, self._rows_for)

    engine = _TimeoutEngine()
    until = T0 + timedelta(days=2)
    _resolver(engine)._distinct_series_ids({"since": T0, "until": until})

    assert len(seen) == len(engine.calls), (
        f"{len(seen)} statement_timeout lifts for {len(engine.calls)} slices"
    )
    assert all("statement_timeout" in s for s in seen)
