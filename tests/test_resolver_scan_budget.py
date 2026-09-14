"""A scan that runs out of budget must keep the work it did.

The Hermes cycle gives resolution a per-step budget smaller than a cold scan
of its own window: measured on griddb, the rolling 2-day distinct-series scan
took 371-411s cold against a 240s step budget. Before the budget below
existed, that cost the run *everything*:

  * ``_run_with_timeout`` abandoned the step;
  * ``scripts.hermes_operator`` advanced ``state.last_resolution`` only after a
    fully clean run, so the watermark stayed put;
  * the next cycle re-scanned the identical window and ran out the same way;
  * and ``blacklist_for_timeout`` held the step out for 24h in between.

``scan_budget_s`` makes a truncated scan an ordinary, successful, *resumable*
outcome: the scan stops between slices, reports the prefix it enumerated, and
resolution is run over exactly that prefix so the caller can move its
watermark there and resume rather than restart.

Every test here drives the real methods against a recording double. A test
that pinned ``RESOLUTION_SCAN_BUDGET_SECONDS == N`` would pass against the
unbudgeted resolver, which is the bug.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

import normalization.resolver as resolver_mod
from normalization.resolver import (
    DISTINCT_SCAN_SLICE_HOURS,
    MIN_SCAN_SLICE_TIMEOUT_S,
    Resolver,
)

WIDTH = timedelta(hours=DISTINCT_SCAN_SLICE_HOURS)
T0 = datetime(2026, 9, 12, 0, 0, tzinfo=timezone.utc)
TWO_DAYS = timedelta(days=2)


# ---------------------------------------------------------------------------
# Doubles — no database, and a clock the test drives
# ---------------------------------------------------------------------------

class _Clock:
    """Monotonic clock advanced explicitly, so slice cost is deterministic."""

    def __init__(self) -> None:
        self.t = 0.0

    def monotonic(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock(monkeypatch):
    c = _Clock()
    # The resolver reads wall cost only through time.monotonic().
    monkeypatch.setattr(resolver_mod, "time", SimpleNamespace(monotonic=c.monotonic))
    return c


class _Result:
    def __init__(self, rows: list[Any]):
        self._rows = rows

    def fetchall(self) -> list[Any]:
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Engine:
    """Records every statement; charges the clock for each distinct slice."""

    def __init__(
        self,
        clock: _Clock | None = None,
        slice_cost_s: float = 0.0,
        raw_rows: list[tuple] | None = None,
    ) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._clock = clock
        self._slice_cost = slice_cost_s
        self._raw_rows = raw_rows if raw_rows is not None else [
            ("VIXCLS", date(2026, 9, 12), 17.5, 3,
             datetime(2026, 9, 12, 6, 0, tzinfo=timezone.utc), 1, "FRED"),
        ]

    def _conn(self):
        engine = self

        class _Conn:
            def execute(self, statement, params=None):
                sql = " ".join(str(statement).split())
                engine.calls.append((sql, params))
                if "set_config" in sql:
                    return _Result([])
                if isinstance(params, list):            # executemany INSERT
                    return _Result([])
                if "SELECT DISTINCT" in sql:
                    if engine._clock is not None:
                        engine._clock.advance(engine._slice_cost)
                    return _Result(
                        [(sid,) for sid in sorted({r[0] for r in engine._raw_rows})]
                    )
                if "FROM raw_series rs" in sql:
                    sids = set(params.get("sids", [])) if params else set()
                    return _Result([r for r in engine._raw_rows if r[0] in sids])
                if "feature_registry" in sql:
                    return _Result([(1, "vol")])
                return _Result([])

        class _Ctx:
            def __enter__(self_inner):
                return _Conn()

            def __exit__(self_inner, *_exc):
                return False

        return _Ctx()

    def begin(self):
        return self._conn()

    def connect(self):
        return self._conn()

    # -- helpers ------------------------------------------------------------
    def distinct_windows(self) -> list[dict]:
        return [p for sql, p in self.calls
                if isinstance(p, dict) and "SELECT DISTINCT" in sql]

    def fetch_windows(self) -> list[dict]:
        """Params of the per-partition row fetch (never sliced)."""
        return [p for sql, p in self.calls
                if isinstance(p, dict)
                and "pull_timestamp >= :since" in sql
                and "SELECT DISTINCT" not in sql]


def _resolver(engine: _Engine) -> Resolver:
    """A Resolver bound to the double, bypassing __init__'s engine setup."""
    r = Resolver.__new__(Resolver)
    r.engine = engine  # type: ignore[assignment]
    return r


@pytest.fixture
def entity_map(monkeypatch):
    class _Stub:
        def __init__(self, _engine):
            pass

        def get_feature_id(self, series_id):
            return 1

        def missing_feature_report(self):
            return {"lookups_missed": 0, "series_ids": 0,
                    "unregistered_features": [], "top_series": []}

    monkeypatch.setattr(resolver_mod, "EntityMap", _Stub)
    return _Stub


# ---------------------------------------------------------------------------
# _scan_series_ids — the budget itself
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_scan_stops_at_its_budget_and_reports_how_far_it_got(clock):
    """The whole point: a truncated scan says where it stopped.

    Twelve 4h slices at 10s each against a 25s budget: slices 1 and 2 leave
    elapsed under budget, slice 3 puts it at 30s, so the scan stops there.
    """
    engine = _Engine(clock=clock, slice_cost_s=10.0)
    until = T0 + TWO_DAYS

    ids, through, complete = _resolver(engine)._scan_series_ids(
        {"since": T0, "until": until}, scan_budget_s=25.0,
    )

    assert complete is False, "a truncated scan must not report completeness"
    assert len(engine.distinct_windows()) == 3, "budget did not stop the scan"
    assert through == T0 + 3 * WIDTH, (
        "scanned_through must be the boundary of the last COMPLETED slice"
    )
    assert ids, "the prefix that was scanned still yields its series"


@pytest.mark.unit
def test_scanned_through_is_always_a_slice_boundary_inside_the_window(clock):
    """Never a partial slice, and never past the window it was given."""
    engine = _Engine(clock=clock, slice_cost_s=7.0)
    until = T0 + TWO_DAYS

    _, through, complete = _resolver(engine)._scan_series_ids(
        {"since": T0, "until": until}, scan_budget_s=20.0,
    )

    assert not complete
    assert T0 < through < until
    assert (through - T0) % WIDTH == timedelta(0)
    # And it matches the last slice actually executed.
    assert engine.distinct_windows()[-1]["until"] == through


@pytest.mark.unit
def test_no_budget_scans_the_whole_window(clock):
    """Manual and backfill callers pass no budget and must be unchanged."""
    engine = _Engine(clock=clock, slice_cost_s=10_000.0)
    until = T0 + TWO_DAYS

    _, through, complete = _resolver(engine)._scan_series_ids(
        {"since": T0, "until": until},
    )

    expected = Resolver._scan_slices(T0, until, WIDTH)
    assert complete is True
    assert through == until
    assert len(engine.distinct_windows()) == len(expected)


@pytest.mark.unit
def test_a_budget_of_zero_still_makes_one_slice_of_progress(clock):
    """The budget is checked BETWEEN slices, so a cycle is never a no-op.

    A budget checked before the first slice would let a permanently slow
    database make zero progress forever — the wedge this exists to break.
    """
    engine = _Engine(clock=clock, slice_cost_s=99.0)

    _, through, complete = _resolver(engine)._scan_series_ids(
        {"since": T0, "until": T0 + TWO_DAYS}, scan_budget_s=0.0,
    )

    assert len(engine.distinct_windows()) == 1
    assert complete is False
    assert through == T0 + WIDTH


@pytest.mark.unit
def test_overshoot_is_bounded_by_one_slice(clock):
    """A slice is never cut in half, so the worst case is budget + one slice."""
    engine = _Engine(clock=clock, slice_cost_s=30.0)

    _resolver(engine)._scan_series_ids(
        {"since": T0, "until": T0 + TWO_DAYS}, scan_budget_s=60.0,
    )

    # 30, 60 -> stop. Elapsed 60 <= budget(60) + one slice(30).
    assert clock.t <= 60.0 + 30.0


@pytest.mark.unit
def test_a_complete_open_ended_scan_reports_the_run_start(clock):
    """``until=None`` has no boundary to report, so the caller supplies one.

    Reporting None would hand the watermark holder something it cannot store;
    reporting "now" would claim rows written after the scan began.
    """
    engine = _Engine(clock=clock, slice_cost_s=0.0)
    open_end = datetime(2026, 9, 14, 5, 0, tzinfo=timezone.utc)

    _, through, complete = _resolver(engine)._scan_series_ids(
        {"since": T0, "until": None}, open_end=open_end,
    )

    assert complete is True
    assert through == open_end


@pytest.mark.unit
def test_distinct_series_ids_is_unbudgeted_and_still_returns_a_list(clock):
    """The plain helper keeps its old contract for its old callers."""
    engine = _Engine(clock=clock, slice_cost_s=500.0)
    result = _resolver(engine)._distinct_series_ids(
        {"since": T0, "until": T0 + TWO_DAYS}
    )
    assert isinstance(result, list)
    assert len(engine.distinct_windows()) == len(
        Resolver._scan_slices(T0, T0 + TWO_DAYS, WIDTH)
    )


# ---------------------------------------------------------------------------
# resolve_pending — the prefix is what gets resolved, and what gets reported
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_a_truncated_run_resolves_only_the_prefix_it_scanned(clock, entity_map):
    """The worker fetch must not reach past where the scan stopped.

    Otherwise the run resolves rows for a window it never enumerated series
    over — an arbitrary subset — while reporting the whole window done.
    """
    engine = _Engine(clock=clock, slice_cost_s=10.0)
    until = T0 + TWO_DAYS

    summary = _resolver(engine).resolve_pending(
        workers=1, since=T0, until=until, dry_run=True, scan_budget_s=25.0,
    )

    assert summary["scan_complete"] is False
    # ISO string in the summary (it is JSON-serialised by callers), datetime
    # in the SQL bind it produced.
    assert summary["scanned_through"] == (T0 + 3 * WIDTH).isoformat()
    through = datetime.fromisoformat(summary["scanned_through"])
    fetches = engine.fetch_windows()
    assert fetches, "the resolve phase did not run over the scanned prefix"
    for params in fetches:
        assert params["since"] == T0
        assert params["until"] == through, (
            "the worker fetch reached past the scan — it would resolve rows "
            "from a range no scan enumerated"
        )


@pytest.mark.unit
def test_a_complete_run_still_carries_the_callers_bounds(clock, entity_map):
    """No budget, no truncation: the fetch window is exactly as before."""
    engine = _Engine(clock=clock, slice_cost_s=1.0)
    until = T0 + TWO_DAYS

    summary = _resolver(engine).resolve_pending(
        workers=1, since=T0, until=until, dry_run=True,
    )

    assert summary["scan_complete"] is True
    assert summary["scanned_through"] == until.isoformat()
    for params in engine.fetch_windows():
        assert (params["since"], params["until"]) == (T0, until)


@pytest.mark.unit
def test_every_summary_carries_the_scan_keys(clock, entity_map, monkeypatch):
    """Callers key their watermark off these, so no return path may omit them."""
    engine = _Engine(clock=clock, slice_cost_s=0.0)
    r = _resolver(engine)

    complete = r.resolve_pending(workers=1, since=T0, until=T0 + WIDTH,
                                 dry_run=True)
    assert {"scanned_through", "scan_complete"} <= set(complete)

    # Nothing found — the early return.
    empty = _Engine(clock=clock, raw_rows=[])
    nothing = _resolver(empty).resolve_pending(
        workers=1, since=T0, until=T0 + WIDTH, dry_run=True,
    )
    assert {"scanned_through", "scan_complete"} <= set(nothing)

    # EntityMap blew up before the scan ever ran: report zero progress, not
    # the end of a window nothing looked at.
    class _Boom:
        def __init__(self, _engine):
            raise RuntimeError("entity map unavailable")

    monkeypatch.setattr(resolver_mod, "EntityMap", _Boom)
    failed = _resolver(_Engine(clock=clock)).resolve_pending(
        workers=1, since=T0, until=T0 + TWO_DAYS, dry_run=True,
    )
    assert failed["errors"] == 1
    assert failed["scan_complete"] is False
    assert failed["scanned_through"] == T0.isoformat(), (
        "a run that never scanned must report its own lower bound, or a "
        "watermark holder advances over time nothing looked at"
    )


@pytest.mark.unit
def test_the_summary_is_json_serialisable(clock, entity_map):
    """It is written to JSONB by the Hermes cycle and dumped verbatim into
    scripts/export_astrogrid_local_data.py's manifest, which does NOT pass
    ``default=str``. A raw datetime in the summary breaks that export."""
    import json

    engine = _Engine(clock=clock, slice_cost_s=10.0)
    summary = _resolver(engine).resolve_pending(
        workers=1, since=T0, until=T0 + TWO_DAYS, dry_run=True,
        scan_budget_s=25.0,
    )

    json.dumps(summary)  # raises TypeError on a datetime


# ---------------------------------------------------------------------------
# A slice must not be able to outlive the budget it is inside
# ---------------------------------------------------------------------------

class _TimeoutAfter(Exception):
    """Stands in for psycopg2's QueryCanceled — matched by _is_statement_timeout."""

    def __init__(self) -> None:
        super().__init__("canceling statement due to statement timeout")


class _FailingSliceEngine(_Engine):
    """Raises a statement timeout on the Nth distinct slice."""

    def __init__(self, fail_on: int, **kw) -> None:
        super().__init__(**kw)
        self.fail_on = fail_on
        self.slice_n = 0
        self.timeouts_ms: list[str] = []

    def _conn(self):
        engine = self
        outer = super()._conn()

        class _Conn:
            def execute(self, statement, params=None):
                sql = " ".join(str(statement).split())
                if "set_config" in sql and params:
                    engine.timeouts_ms.append(params.get("timeout_ms"))
                    return _Result([])
                if "SELECT DISTINCT" in sql:
                    engine.slice_n += 1
                    if engine.slice_n == engine.fail_on:
                        raise _TimeoutAfter()
                return outer.__enter__().execute(statement, params)

        class _Ctx:
            def __enter__(self_inner):
                return _Conn()

            def __exit__(self_inner, *_exc):
                return False

        return _Ctx()


@pytest.mark.unit
def test_a_budgeted_slice_is_capped_by_the_budget_it_has_left(clock):
    """Otherwise one slice can sit on the 10-minute bulk timeout and blow
    through the step timeout the budget exists to stay inside — straight back
    to the orphaned-thread-plus-blacklist path."""
    engine = _Engine(clock=clock, slice_cost_s=10.0)
    caps: list[int] = []
    original = Resolver._set_statement_timeout

    def _record(self, conn, timeout_ms=None):
        caps.append(timeout_ms)
        return original(self, conn, timeout_ms)

    r = _resolver(engine)
    r._set_statement_timeout = _record.__get__(r, Resolver)
    r._scan_series_ids({"since": T0, "until": T0 + TWO_DAYS}, scan_budget_s=60.0)

    assert caps, "no statement timeout was set"
    assert all(c is not None for c in caps), "a budgeted slice used the bulk cap"
    assert all(c <= 60_000 for c in caps), (
        "a slice was allowed more time than the whole budget"
    )
    assert caps[0] == 60_000, "the first slice may use the whole budget"
    assert caps == sorted(caps, reverse=True), (
        "each slice must be capped by what the budget has LEFT"
    )
    assert min(caps) == MIN_SCAN_SLICE_TIMEOUT_S * 1000, (
        "the floor must stop a nearly-spent budget handing out a useless cap"
    )
    assert all(c < resolver_mod.Resolver._RESOLVE_STATEMENT_TIMEOUT_MS
               for c in caps), (
        "a budgeted slice must never get the 10-minute bulk cap"
    )


@pytest.mark.unit
def test_an_unbudgeted_scan_keeps_the_bulk_statement_timeout(clock):
    """Backfills legitimately need the 10-minute cap; they pass no budget."""
    engine = _Engine(clock=clock, slice_cost_s=1.0)
    caps: list[int | None] = []
    original = Resolver._set_statement_timeout

    def _record(self, conn, timeout_ms=None):
        caps.append(timeout_ms)
        return original(self, conn, timeout_ms)

    r = _resolver(engine)
    r._set_statement_timeout = _record.__get__(r, Resolver)
    r._scan_series_ids({"since": T0, "until": T0 + TWO_DAYS})

    assert caps and all(c is None for c in caps)


@pytest.mark.unit
def test_a_slice_killed_by_its_timeout_keeps_the_slices_before_it(clock):
    """The budget firing from inside a statement must cost one slice, not all."""
    engine = _FailingSliceEngine(fail_on=4, clock=clock, slice_cost_s=10.0)

    ids, through, complete = _resolver(engine)._scan_series_ids(
        {"since": T0, "until": T0 + TWO_DAYS}, scan_budget_s=300.0,
    )

    assert complete is False
    assert through == T0 + 3 * WIDTH, "the three completed slices were thrown away"
    assert ids


@pytest.mark.unit
def test_a_timeout_on_the_very_first_slice_is_raised(clock):
    """Nothing completed, so there is no prefix to report.

    Swallowing it would turn a real (transient) database failure into a
    silent no-op cycle that looks successful.
    """
    engine = _FailingSliceEngine(fail_on=1, clock=clock, slice_cost_s=10.0)

    with pytest.raises(_TimeoutAfter):
        _resolver(engine)._scan_series_ids(
            {"since": T0, "until": T0 + TWO_DAYS}, scan_budget_s=300.0,
        )


@pytest.mark.unit
def test_a_non_timeout_failure_is_never_swallowed(clock):
    """A schema fault repeats at every width — it is a bug, not a budget."""
    class _Broken(_FailingSliceEngine):
        def _conn(self):
            engine = self

            class _Conn:
                def execute(self, statement, params=None):
                    sql = " ".join(str(statement).split())
                    if "set_config" in sql:
                        return _Result([])
                    engine.slice_n += 1
                    if engine.slice_n >= engine.fail_on:
                        raise RuntimeError(
                            'column "series_id" does not exist'
                        )
                    return _Result([("VIXCLS",)])

            class _Ctx:
                def __enter__(self_inner):
                    return _Conn()

                def __exit__(self_inner, *_exc):
                    return False

            return _Ctx()

    engine = _Broken(fail_on=3, clock=clock, slice_cost_s=1.0)

    with pytest.raises(RuntimeError, match="does not exist"):
        _resolver(engine)._scan_series_ids(
            {"since": T0, "until": T0 + TWO_DAYS}, scan_budget_s=300.0,
        )
