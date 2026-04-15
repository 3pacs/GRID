"""Integration test: convergence scanner sees the three newly-wired streams.

The task brief contract: after fixing smart_money / institutional_flows /
social-heat writes, the CAT-dots convergence scanner should go from
5/8 to 8/8 active streams by observing the new rows in
``signal_sources``.

This test seeds a FakeEngine with 3 rows per new stream on ticker
``TSLA`` and asserts the scanner sees all three.

UPSTREAM DEPENDENCY NOTE
------------------------

The scanner module ``intelligence/signal_convergence_scanner.py``
referenced by the task does NOT exist on this branch. Only
``intelligence/hypothesis_engine.AnomalyHunter.scan_convergence`` is
present, and that scanner reads from ``signal_data`` (not
``signal_sources``). The test therefore:

1. Attempts to import ``intelligence.signal_convergence_scanner``.
2. If present, runs the real scanner against the fake engine.
3. If absent, pytest.skip()s with a loud reason so the CI surface
   clearly shows the upstream blocker.

The write-path fixes themselves are fully covered by
``tests/test_signal_sources_write_path.py`` which passes on this branch.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# FakeEngine — minimal SQLAlchemy-ish engine that replays a fixture table
# ---------------------------------------------------------------------------


class _FakeRow(tuple):
    """Row subclass so ``row[0]`` indexing works like a real DB row."""
    pass


class _FakeConn:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        # Very small query simulator: we only need to return the rows
        # filtered by ``ticker`` if the scanner passes one in params.
        params = params or {}
        ticker = params.get("ticker")
        out = self._rows
        if ticker:
            out = [r for r in out if r.get("ticker") == ticker]
        # Represent as rows the scanner can read positionally + by name.
        result = MagicMock()
        result.fetchall.return_value = [
            _FakeRow((
                r["source_type"],
                r["signal_type"],
                r["ticker"],
                r["signal_date"],
                r.get("signal_value"),
            ))
            for r in out
        ]
        result.mappings.return_value.all.return_value = out
        # Iteration support
        result.__iter__ = lambda self: iter(result.fetchall.return_value)
        return result


class _FakeEngine:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def connect(self):
        return _FakeConn(self._rows)

    def begin(self):
        return _FakeConn(self._rows)


# ---------------------------------------------------------------------------
# Fixture — 3 rows per new stream on TSLA
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_engine() -> _FakeEngine:
    today = date.today()
    rows: list[dict[str, Any]] = []
    for i in range(3):
        sd = today - timedelta(days=i)
        rows.append({
            "source_type": "smart_money",
            "source_id": f"reddit:user_{i}",
            "ticker": "TSLA",
            "signal_date": sd,
            "signal_type": "NET_POSITION_DELTA",
            "signal_value": {"position_delta": 1.0,
                             "reddit_mentions_count": i + 1,
                             "sentiment_score": 0.8,
                             "window_days": 1},
        })
        rows.append({
            "source_type": "institutional",
            "source_id": f"0001067983:FUND_{i}",
            "ticker": "TSLA",
            "signal_date": sd,
            "signal_type": "NET_POSITION_DELTA",
            "signal_value": {"manager": f"FUND_{i}",
                             "action": "INCREASED",
                             "pct_change": 0.4},
        })
        rows.append({
            "source_type": "social",
            "source_id": f"reddit:hot_{i}",
            "ticker": "TSLA",
            "signal_date": sd,
            "signal_type": "HEAT_SPIKE",
            "signal_value": {"mentions_z": 2.1,
                             "sentiment": 0.6,
                             "ticker_rank": i + 1},
        })
    return _FakeEngine(rows)


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------


def _scanner_module():
    try:
        import importlib
        return importlib.import_module(
            "intelligence.signal_convergence_scanner"
        )
    except ModuleNotFoundError:
        return None


def test_convergence_scanner_sees_three_new_streams(seeded_engine):
    scanner = _scanner_module()
    if scanner is None:
        pytest.skip(
            "UPSTREAM DEPENDENCY MISSING: "
            "intelligence/signal_convergence_scanner.py does not exist "
            "on this branch. The puller write-path fixes are "
            "independently verified by "
            "tests/test_signal_sources_write_path.py. Once the "
            "convergence scanner module lands, this test will run "
            "end-to-end."
        )

    # Preferred call signature per the task brief:
    #   scan_convergence(engine, ticker='TSLA', as_of=today,
    #                    target_direction='bullish', window_days=7)
    result = scanner.scan_convergence(
        seeded_engine,
        ticker="TSLA",
        as_of=date.today(),
        target_direction="bullish",
        window_days=7,
    )

    # The scanner is expected to return either a dict with
    # ``n_active_streams`` + ``stream_signals`` OR a namedtuple-like
    # object with those fields. Handle both.
    n_active = (
        result.get("n_active_streams")
        if isinstance(result, dict)
        else getattr(result, "n_active_streams", None)
    )
    stream_signals = (
        result.get("stream_signals")
        if isinstance(result, dict)
        else getattr(result, "stream_signals", None)
    )

    assert n_active is not None, "scanner must return n_active_streams"
    assert n_active >= 3, (
        f"expected >=3 active streams from the 3 newly-wired source types, "
        f"got {n_active}"
    )

    # Each of the three new streams must appear by source_type.
    stream_types = {
        s.get("source_type") if isinstance(s, dict) else getattr(s, "source_type", None)
        for s in (stream_signals or [])
    }
    for expected in ("smart_money", "institutional", "social"):
        assert expected in stream_types, (
            f"stream '{expected}' missing from scanner output "
            f"(got {stream_types})"
        )
