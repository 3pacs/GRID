"""A resolution cycle that runs out of budget must not lose its progress.

What was wrong, measured on griddb 2026-09-14 (ops-exec runs 292 and 312):

  (a) ``RESOLUTION_TIMEOUT_SECONDS`` was 240 while a COLD scan of the step's
      own 2-day window took 371-411s. Warm the same scan takes 0.2s, so this
      is a tail, not the norm — but the tail is the case that matters;
  (b) ``_run_with_timeout`` abandons a timed-out step and its worker thread
      keeps running with an open DB transaction, which blocks CREATE/DROP
      INDEX CONCURRENTLY database-wide;
  (c) ``state.last_resolution`` advanced only after a *fully* clean run, so a
      cycle that timed out held the watermark and the next cycle re-scanned
      the identical cold window with the identical budget. Nothing about the
      retry was more likely to succeed than the attempt before it.

(c) is the root. The fix: the resolver stops between scan slices at
``RESOLUTION_SCAN_BUDGET_SECONDS`` and reports ``scanned_through`` — how far
it actually got — and the step advances the watermark THERE. A truncated
cycle then costs one slice of progress instead of all of it, and returns
successfully rather than being abandoned.

Not in that list: the 24h ``blacklist_for_timeout`` the timeout also records.
Unlike oracle_cycle and the gemma steps, ``_run_resolution_step`` never
consults ``cooldowns.can_retry()``, so the entry has never skipped a
resolution step. Production on 2026-09-14 showed ``resolution`` blacklisted
until 10:15 UTC while cycles 6008-6015 all ran it and all succeeded. Nothing
here changes that; ``test_a_truncated_run_is_not_blacklisted`` only pins that
a budgeted stop does not *add* an entry.

These drive ``_run_resolution_step`` with a recording resolver double. The
watermark assertions below fail against the pre-fix step, which set
``state.last_resolution = run_started`` regardless of how far the run got.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from loguru import logger as log


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class _FakeCooldowns:
    def __init__(self) -> None:
        self.blacklisted: list[str] = []
        self.attempts: list[tuple] = []

    def blacklist_for_timeout(self, name: str) -> None:
        self.blacklisted.append(name)

    def record_attempt(self, name, success=True, error=None) -> None:
        self.attempts.append((name, success, error))


class _FakeState:
    """Minimal OperatorState stand-in carrying the resolution watermark."""

    def __init__(self, last_resolution: datetime | None = None) -> None:
        self.current_step: str | None = None
        self.last_resolution = last_resolution
        self.cooldowns = _FakeCooldowns()
        self.task_status: dict[str, dict] = {}

    def record_task(self, task_name, success, duration_s, error=None,
                    transient=False) -> None:
        self.task_status[task_name] = {
            "success": success, "duration_s": duration_s, "error": error,
            "transient": transient,
        }


@pytest.fixture
def warnings_captured():
    records: list[str] = []
    sink_id = log.add(lambda msg: records.append(msg), level="WARNING")
    yield records
    log.remove(sink_id)


def _summary(**over):
    """A resolver summary. ``scanned_through`` rides as an ISO string, as the
    real resolver emits it — the summary is JSON-serialised by its callers."""
    base = {
        "resolved": 7, "conflicts_found": 0, "errors": 0,
        "series_scanned": 3, "duration_s": 1.0, "dry_run": False,
        "scan_complete": True, "scanned_through": None,
    }
    base.update(over)
    through = base["scanned_through"]
    if hasattr(through, "isoformat"):
        base["scanned_through"] = through.isoformat()
    return base


def _install_resolver(monkeypatch, summary, calls=None):
    class _FakeResolver:
        def __init__(self, db_engine=None):
            self.engine = db_engine

        def resolve_pending(self, **kwargs):
            if calls is not None:
                calls.append(kwargs)
            return summary

    monkeypatch.setattr("normalization.resolver.Resolver", _FakeResolver)


# ---------------------------------------------------------------------------
# The budget reaches the resolver at all
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_the_step_hands_the_resolver_a_scan_budget(monkeypatch):
    """Without this the resolver scans the whole window and blows the step."""
    import scripts.hermes_operator as hermes

    calls: list[dict] = []
    _install_resolver(monkeypatch, _summary(), calls=calls)

    hermes._run_resolution_step(object(), _FakeState())

    assert calls[0]["scan_budget_s"] == hermes.RESOLUTION_SCAN_BUDGET_SECONDS


# ---------------------------------------------------------------------------
# The watermark follows the scan, not the clock
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_a_truncated_scan_advances_the_watermark_to_what_was_scanned(
    monkeypatch, warnings_captured,
):
    """The fix for (c). Pre-fix this set the watermark to `run_started`.

    The window opened 26h back and the scan reached 14h back. Advancing to
    "now" would skip the 14h the scan never enumerated — those rows would
    never be resolved. Holding the watermark at 26h back re-scans the same
    cold prefix next cycle, forever.
    """
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    watermark = now - timedelta(hours=24)
    reached = now - timedelta(hours=14)
    _install_resolver(monkeypatch, _summary(
        scan_complete=False, scanned_through=reached,
    ))
    state = _FakeState(last_resolution=watermark)

    result = hermes._run_resolution_step(object(), state)

    assert state.last_resolution == reached, (
        "the watermark must land exactly where the scan stopped"
    )
    assert state.last_resolution > watermark, "no progress was made at all"
    assert result["scan_complete"] is False
    assert any("scan budget" in str(r) for r in warnings_captured)


@pytest.mark.unit
def test_a_truncated_scan_never_advances_past_the_scan(monkeypatch):
    """The property, stated directly: the watermark may not outrun the scan."""
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    reached = now - timedelta(hours=30)
    _install_resolver(monkeypatch, _summary(
        scan_complete=False, scanned_through=reached,
    ))
    state = _FakeState(last_resolution=now - timedelta(days=2))

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution <= reached


@pytest.mark.unit
def test_successive_truncated_cycles_walk_the_watermark_forward(monkeypatch):
    """Resumption, not restart — the behaviour the wedge denied.

    Three cycles that each stop at their budget must each leave the window
    strictly shorter than the one before.
    """
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    state = _FakeState(last_resolution=now - timedelta(days=2))
    overlap = timedelta(hours=hermes.RESOLUTION_WATERMARK_OVERLAP_HOURS)

    opened: list[datetime] = []
    for step in (36, 24, 12):
        reached = now - timedelta(hours=step)
        calls: list[dict] = []
        _install_resolver(monkeypatch, _summary(
            scan_complete=False, scanned_through=reached,
        ), calls=calls)
        hermes._run_resolution_step(object(), state)
        opened.append(calls[0]["since"])
        assert state.last_resolution == reached

    assert opened == sorted(opened), "each cycle must open later than the last"
    assert opened[-1] == (now - timedelta(hours=24)) - overlap, (
        "the overlap margin must still be applied to the advanced watermark"
    )


@pytest.mark.unit
def test_a_truncated_run_is_not_blacklisted(monkeypatch):
    """Stopping inside the budget is a success, not a timeout.

    The blacklist is inert for this step today (see the module docstring),
    but a budgeted stop must not add an entry either way — it would be a
    false failure signal on the health surfaces.
    """
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    _install_resolver(monkeypatch, _summary(
        scan_complete=False, scanned_through=now - timedelta(hours=10),
    ))
    state = _FakeState(last_resolution=now - timedelta(days=2))

    hermes._run_resolution_step(object(), state)

    assert state.cooldowns.blacklisted == []
    assert state.task_status["resolution"]["success"] is True


@pytest.mark.unit
def test_the_watermark_never_moves_backwards(monkeypatch, warnings_captured):
    """A scan that stopped inside the overlap margin must not undo progress.

    The window opens at `watermark - overlap`, so the first slice boundary
    can legitimately fall behind the current watermark.
    """
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    watermark = now - timedelta(hours=2)
    _install_resolver(monkeypatch, _summary(
        scan_complete=False, scanned_through=now - timedelta(hours=3),
    ))
    state = _FakeState(last_resolution=watermark)

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution == watermark


@pytest.mark.unit
def test_a_complete_run_advances_to_the_run_start_as_before(monkeypatch):
    """Unchanged behaviour on the happy path.

    A complete open-ended scan reports the moment the resolver started, which
    is at or after the step's own `run_started`; the watermark must not land
    in this cycle's future either way.
    """
    import scripts.hermes_operator as hermes

    before = datetime.now(timezone.utc)
    _install_resolver(monkeypatch, _summary(
        scan_complete=True, scanned_through=before + timedelta(seconds=1),
    ))
    state = _FakeState()

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution is not None
    assert before <= state.last_resolution <= datetime.now(timezone.utc)


@pytest.mark.unit
def test_a_datetime_scanned_through_is_accepted_too(monkeypatch):
    """The string is the contract, but a datetime must not be mis-handled."""
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    reached = now - timedelta(hours=9)
    raw = _summary(scan_complete=False)
    raw["scanned_through"] = reached          # deliberately not serialised
    _install_resolver(monkeypatch, raw)
    state = _FakeState(last_resolution=now - timedelta(days=2))

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution == reached


@pytest.mark.unit
def test_a_naive_scanned_through_is_read_as_utc(monkeypatch):
    """Comparing naive to aware raises TypeError; the watermark must survive.

    The resolver emits naive ISO text whenever its caller supplied a naive
    ``since`` — an operator's ``--since 2026-04-04``, for one.
    """
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    reached_naive = (now - timedelta(hours=9)).replace(tzinfo=None)
    _install_resolver(monkeypatch, _summary(
        scan_complete=False, scanned_through=reached_naive,
    ))
    state = _FakeState(last_resolution=now - timedelta(days=2))

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution == reached_naive.replace(tzinfo=timezone.utc)


@pytest.mark.unit
def test_unparseable_text_holds_the_watermark_at_the_run_start(monkeypatch,
                                                               warnings_captured):
    """Garbage in the field must not crash the cycle or invent a watermark."""
    import scripts.hermes_operator as hermes

    raw = _summary(scan_complete=True)
    raw["scanned_through"] = "not a timestamp"
    _install_resolver(monkeypatch, raw)
    before = datetime.now(timezone.utc)
    state = _FakeState()

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution is not None
    assert state.last_resolution >= before
    assert any("unparseable" in str(r) for r in warnings_captured)


@pytest.mark.unit
def test_the_step_result_survives_the_snapshot_serialiser(monkeypatch):
    """cycle_result is written to JSONB and dumped by scripts without
    ``default=str``, so nothing in the summary may be a raw datetime."""
    import json

    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    _install_resolver(monkeypatch, _summary(
        scan_complete=False, scanned_through=now - timedelta(hours=8),
    ))

    result = hermes._run_resolution_step(object(), _FakeState())

    json.dumps(result)  # raises TypeError on a datetime


@pytest.mark.unit
def test_a_summary_without_the_key_falls_back_to_the_run_start(monkeypatch):
    """An older resolver must not stall the watermark at None."""
    import scripts.hermes_operator as hermes

    before = datetime.now(timezone.utc)
    legacy = {
        "resolved": 1, "conflicts_found": 0, "errors": 0,
        "series_scanned": 1, "duration_s": 0.1, "dry_run": False,
    }
    _install_resolver(monkeypatch, legacy)
    state = _FakeState()

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution is not None
    assert state.last_resolution >= before


@pytest.mark.unit
def test_a_failed_run_holds_the_watermark_even_with_a_scan_report(monkeypatch):
    """A run with worker errors cannot say which of its work landed."""
    import scripts.hermes_operator as hermes

    now = datetime.now(timezone.utc)
    watermark = now - timedelta(days=2)
    _install_resolver(monkeypatch, _summary(
        errors=2, scan_complete=False, scanned_through=now - timedelta(hours=6),
    ))
    state = _FakeState(last_resolution=watermark)

    hermes._run_resolution_step(object(), state)

    assert state.last_resolution == watermark


@pytest.mark.unit
def test_a_timeout_still_holds_the_watermark(monkeypatch):
    """An abandoned step returns nothing, so there is nothing to advance to."""
    import scripts.hermes_operator as hermes

    monkeypatch.setattr(
        hermes, "_run_with_timeout",
        lambda _name, _fn, _timeout, _state: (None, False),
    )
    watermark = datetime(2026, 9, 1, tzinfo=timezone.utc)
    state = _FakeState(last_resolution=watermark)

    result = hermes._run_resolution_step(object(), state)

    assert result["timeout"] is True
    assert state.last_resolution == watermark


# ---------------------------------------------------------------------------
# The budget must actually fit inside the step it is budgeting
# ---------------------------------------------------------------------------

# Worst resolve phase observed on a live cycle, griddb 2026-09-14: cycle 6014
# ran the step in 77.5s, of which the scan was ~1s and one resolver worker was
# ~76s. The resolve phase cannot be truncated the way the scan can — splitting
# it by pull_timestamp would split (series_id, obs_date) groups across cycles
# and break cross-source conflict detection — so it has to fit in headroom.
WORST_OBSERVED_RESOLVE_PHASE_S = 77.5


@pytest.mark.unit
def test_the_scan_budget_leaves_room_inside_the_step_timeout():
    """(a) restated as an invariant, from measured numbers.

    The budget is checked between slices and each budgeted slice is capped at
    MIN_SCAN_SLICE_TIMEOUT_S at worst, so a scan can overshoot by that much;
    the resolve phase then runs on top. If the sum can exceed the step
    timeout, the budget has not removed the cliff — it has only moved it,
    which is the band-aid this change exists instead of.
    """
    from normalization.resolver import MIN_SCAN_SLICE_TIMEOUT_S
    from scripts import hermes_operator as ho

    assert ho.RESOLUTION_SCAN_BUDGET_SECONDS < ho.RESOLUTION_TIMEOUT_SECONDS
    worst_case = (
        ho.RESOLUTION_SCAN_BUDGET_SECONDS
        + MIN_SCAN_SLICE_TIMEOUT_S
        + WORST_OBSERVED_RESOLVE_PHASE_S
    )
    assert worst_case < ho.RESOLUTION_TIMEOUT_SECONDS, (
        f"budget + one slice of overshoot + the observed worst resolve phase "
        f"is {worst_case}s, which does not fit inside "
        f"{ho.RESOLUTION_TIMEOUT_SECONDS}s — the step would be abandoned "
        f"before it could report where it got to"
    )


@pytest.mark.unit
def test_the_budget_covers_a_cold_slice_with_room_to_spare():
    """The steady-state window is one slice; the budget must not clip it.

    ops-exec run 292 measured the cold 2-day scan at 411s over
    48 / DISTINCT_SCAN_SLICE_HOURS slices. A budget that cannot clear one
    cold slice comfortably would truncate every cycle and never catch up.
    """
    from normalization.resolver import DISTINCT_SCAN_SLICE_HOURS
    from scripts import hermes_operator as ho

    slices_in_a_2_day_window = 48 / DISTINCT_SCAN_SLICE_HOURS
    cold_slice_s = 411.0 / slices_in_a_2_day_window   # ~34s

    assert ho.RESOLUTION_SCAN_BUDGET_SECONDS > 4 * cold_slice_s, (
        "the budget must clear several cold slices, or a cold catch-up "
        "crawls one slice per cycle"
    )


@pytest.mark.unit
def test_the_step_timeout_is_inside_the_cycle_budget():
    from scripts import hermes_operator as ho

    assert ho.RESOLUTION_TIMEOUT_SECONDS < ho.CYCLE_TIMEOUT_SECONDS
