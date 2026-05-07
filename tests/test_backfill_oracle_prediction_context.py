"""Unit tests for scripts.backfill_oracle_prediction_context.

Uses a ``FakeEngine`` that simulates oracle_predictions rows and records every
UPDATE issued. No real DB connection is ever opened.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


from scripts.backfill_oracle_prediction_context import backfill


# ── Fake engine ──────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def __init__(self, engine: "FakeEngine", mode: str):
        self._engine = engine
        self._mode = mode  # "read" or "write"

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, query, params=None):
        sql = str(query)
        params = params or {}
        # READ path for the backfill SELECT
        if "SELECT id, created_at, signals" in sql and "FROM oracle_predictions" in sql:
            applied = self._engine.target_rows
            if "LIMIT" in sql and "limit" in params:
                applied = applied[: int(params["limit"])]
            return _FakeResult(applied)
        # UPDATE path
        if sql.strip().startswith("UPDATE oracle_predictions"):
            self._engine.updates.append({"id": params.get("id"), "payload": params.get("payload")})
            return _FakeResult([])
        # Auxiliary reads from build_prediction_context
        if "regime_history" in sql:
            return _FakeResult([])
        if "resolved_series" in sql:
            return _FakeResult([])
        return _FakeResult([])


class FakeEngine:
    def __init__(self, target_rows, *, fail_on_row: Any = None):
        self.target_rows = target_rows
        self.updates: list[dict] = []
        self.fail_on_row = fail_on_row

    def connect(self):
        return _FakeConnection(self, mode="read")

    def begin(self):
        # Raise for a targeted row to simulate per-row failures.
        if self.fail_on_row is not None:
            trigger = self.fail_on_row

            class _FailingCtx:
                def __enter__(self):
                    raise RuntimeError(f"simulated failure for row {trigger}")

                def __exit__(self, *a):
                    return False

            # Only fail on next .begin() that follows a SELECT for the target id
            # — for simplicity we fail globally here; the caller can narrow.
            return _FailingCtx()
        return _FakeConnection(self, mode="write")


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _make_rows(n: int):
    base = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    return [
        (f"pred-{i}", base, [{"name": f"sig-{i}"}])
        for i in range(n)
    ]


# ── Tests ────────────────────────────────────────────────────────────────────

def test_backfill_updates_every_row():
    engine = FakeEngine(_make_rows(5))
    counters = backfill(engine, dry_run=False)
    assert counters["examined"] == 5
    assert counters["updated"] == 5
    assert counters["dry_run"] == 0
    assert len(engine.updates) == 5

    # Every written payload must contain the 4 keys.
    for upd in engine.updates:
        payload = json.loads(upd["payload"])
        assert payload["regime"] == "NEUTRAL"
        assert payload["fci_regime"] == "NEUTRAL"
        assert payload["vix_level"] is None
        assert payload["signal_contributions"] == {}
        # Existing list was wrapped under items
        assert isinstance(payload["items"], list)
        assert len(payload["items"]) == 1


def test_backfill_dry_run_writes_nothing():
    engine = FakeEngine(_make_rows(5))
    counters = backfill(engine, dry_run=True)
    assert counters["examined"] == 5
    assert counters["updated"] == 0
    assert counters["dry_run"] == 5
    assert engine.updates == []


def test_backfill_respects_limit():
    engine = FakeEngine(_make_rows(10))
    counters = backfill(engine, dry_run=False, limit=2)
    assert counters["examined"] == 2
    assert counters["updated"] == 2
    assert len(engine.updates) == 2


def test_backfill_skips_row_on_write_failure():
    # Engine where every .begin() raises — so every UPDATE attempt fails.
    engine = FakeEngine(_make_rows(3), fail_on_row="pred-0")
    counters = backfill(engine, dry_run=False)
    assert counters["examined"] == 3
    assert counters["updated"] == 0
    assert counters["skipped"] == 3
    # No updates were actually applied because every begin() context failed.
    assert engine.updates == []


def test_backfill_handles_existing_dict_signals_shape():
    """If the row already has a dict signals payload with some keys, those
    keys should be preserved and only missing keys filled in."""
    row = (
        "pred-x",
        datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
        {"items": [{"name": "keep"}], "regime": "EXPANSION_STRONG"},
    )
    engine = FakeEngine([row])
    counters = backfill(engine, dry_run=False)
    assert counters["updated"] == 1
    payload = json.loads(engine.updates[0]["payload"])
    # Pre-existing regime preserved
    assert payload["regime"] == "EXPANSION_STRONG"
    # Missing keys filled
    assert payload["fci_regime"] == "NEUTRAL"
    assert payload["vix_level"] is None
    assert payload["signal_contributions"] == {}
    # Items list preserved
    assert payload["items"] == [{"name": "keep"}]
