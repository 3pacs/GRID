"""Tests for scripts/run_regime_state_vectors.py (Wave 3 W3.2's nightly job).

This is the only writer of ``regime_state_vectors``. Pins:

* ``resolve_target_date`` always resolves to a trading day strictly before
  "today", regardless of what day of the week "today" is.
* ``--as-of`` on or after today is refused (exit 2) — the job must never be
  talked into computing/persisting a same-day/current-session vector.
* A normal run calls ``get_or_compute_state_vector`` with ``persist=True``;
  ``--dry-run`` calls it with ``persist=False`` and forces a fresh compute
  instead of silently echoing back whatever happens to be cached.
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from scripts import run_regime_state_vectors as job


# ── resolve_target_date ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    "today",
    [
        date(2026, 9, 28),  # Monday
        date(2026, 9, 29),  # Tuesday
        date(2026, 9, 24),  # Thursday
        date(2026, 9, 21),  # Sunday (a manual/off-schedule run)
        date(2026, 1, 1),   # New Year's Day (a market holiday)
    ],
)
def test_resolve_target_date_is_always_strictly_before_today(today):
    target = job.resolve_target_date(today)
    assert target < today


def test_resolve_target_date_is_a_trading_day():
    from ingestion.market_calendar import is_market_open

    target = job.resolve_target_date(date(2026, 9, 28))
    assert is_market_open(target)


def test_resolve_target_date_skips_weekend():
    # Monday 2026-09-28's prior completed session is Friday 2026-09-25,
    # not Sunday.
    assert job.resolve_target_date(date(2026, 9, 28)) == date(2026, 9, 25)


# ── main(): refuses a same-day/current-session --as-of ───────────────────


def test_main_refuses_as_of_today():
    today = date.today().isoformat()
    rc = job.main(["--as-of", today], engine=MagicMock())
    assert rc == 2


def test_main_refuses_as_of_future():
    future = date(9999, 1, 1).isoformat()
    rc = job.main(["--as-of", future], engine=MagicMock())
    assert rc == 2


# ── main(): persist wiring ────────────────────────────────────────────────


def _fake_sv(as_of, completeness=0.9, price_basis="spy_full", cached=False):
    from intelligence.regime.state_vector import DIM_NAMES, StateVector

    return StateVector(
        as_of_date=as_of, values=tuple([0.1] * len(DIM_NAMES)),
        completeness=completeness, stale_dimensions=(), price_basis=price_basis,
        cached=cached,
    )


def test_main_default_run_persists():
    target = date(2026, 9, 24)
    sv = _fake_sv(target)
    with patch("intelligence.regime.state_vector.get_or_compute_state_vector", return_value=sv) as call:
        rc = job.main(["--as-of", target.isoformat()], engine=MagicMock())
    assert rc == 0
    assert call.call_args.kwargs["persist"] is True
    assert call.call_args.kwargs["force_recompute"] is False


def test_main_dry_run_never_persists_and_forces_recompute():
    target = date(2026, 9, 24)
    sv = _fake_sv(target)
    with patch("intelligence.regime.state_vector.get_or_compute_state_vector", return_value=sv) as call:
        rc = job.main(["--as-of", target.isoformat(), "--dry-run", "--json"], engine=MagicMock())
    assert rc == 0
    assert call.call_args.kwargs["persist"] is False
    assert call.call_args.kwargs["force_recompute"] is True


def test_main_json_summary_reports_persisted_true_when_above_floor():
    target = date(2026, 9, 24)
    sv = _fake_sv(target, completeness=0.9, cached=False)
    with patch("intelligence.regime.state_vector.get_or_compute_state_vector", return_value=sv), \
         capture_stdout() as out:
        rc = job.main(["--as-of", target.isoformat(), "--json"], engine=MagicMock())
    assert rc == 0
    summary = json.loads(out.getvalue())
    assert summary["as_of_date"] == target.isoformat()
    assert summary["persisted"] is True
    assert summary["would_persist"] is True
    assert summary["price_basis"] == "spy_full"
    assert summary["dry_run"] is False


def test_main_json_summary_reports_not_persisted_below_floor():
    target = date(2026, 9, 24)
    sv = _fake_sv(target, completeness=0.1, price_basis=None, cached=False)
    with patch("intelligence.regime.state_vector.get_or_compute_state_vector", return_value=sv), \
         capture_stdout() as out:
        rc = job.main(["--as-of", target.isoformat(), "--json"], engine=MagicMock())
    assert rc == 0
    summary = json.loads(out.getvalue())
    assert summary["would_persist"] is False
    assert summary["persisted"] is False
    assert summary["price_basis"] is None


def test_main_already_cached_reports_not_persisted_this_run():
    """When get_or_compute_state_vector serves an existing cache hit
    (cached=True), this run wrote nothing new."""
    target = date(2026, 9, 24)
    sv = _fake_sv(target, completeness=0.9, cached=True)
    with patch("intelligence.regime.state_vector.get_or_compute_state_vector", return_value=sv), \
         capture_stdout() as out:
        rc = job.main(["--as-of", target.isoformat(), "--json"], engine=MagicMock())
    assert rc == 0
    summary = json.loads(out.getvalue())
    assert summary["cached"] is True
    assert summary["persisted"] is False


# ── stdout capture helper (avoid a hard dependency on capsys inside a `with`) ──

import contextlib
import io


@contextlib.contextmanager
def capture_stdout():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        yield buf
