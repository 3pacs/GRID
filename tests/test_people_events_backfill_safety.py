"""Operational boundary and crash/resume checks; no database connections."""

from contextlib import contextmanager
from datetime import datetime, timezone
import json

import pandas as pd
import pytest

from scripts import people_events_backfill_safety as G
from intelligence.people_events_pipeline import writer as W


@pytest.mark.parametrize("hour,minute,expected", [
    (2, 29, True), (2, 30, False), (3, 30, False), (10, 29, False), (10, 30, True),
    (10, 57, True), (10, 58, False), (11, 11, False), (11, 12, True),
    (13, 24, True), (13, 25, False), (14, 19, False), (14, 20, True), (23, 59, True),
])
def test_all_operational_window_boundaries(hour, minute, expected):
    assert G.write_window_open(datetime(2026, 10, 2, hour, minute, tzinfo=timezone.utc)) is expected


def test_manifest_reuses_global_baseline_and_refuses_changed_scope_or_cap(tmp_path):
    identity = {"sha": "original"}
    manifest, resumed = G.load_manifest(tmp_path, identity, baseline_bytes=123,
                                       current_bytes=123, max_gb=8, stamp="20261002T180000Z")
    assert not resumed
    again, resumed = G.load_manifest(tmp_path, identity, baseline_bytes=None,
                                     current_bytes=1_000_000, max_gb=8, stamp="20261002T190000Z")
    assert resumed and again == manifest and again["baseline_bytes"] == 123
    with pytest.raises(RuntimeError, match="source/scope changed"):
        G.load_manifest(tmp_path, {"sha": "changed"}, baseline_bytes=None,
                        current_bytes=1_000_000, max_gb=8, stamp="20261002T190000Z")
    with pytest.raises(RuntimeError, match="baseline changed"):
        G.load_manifest(tmp_path, identity, baseline_bytes=999,
                        current_bytes=1_000_000, max_gb=8, stamp="20261002T190000Z")
    with pytest.raises(RuntimeError, match="raise the original"):
        G.load_manifest(tmp_path, identity, baseline_bytes=None,
                        current_bytes=1_000_000, max_gb=9, stamp="20261002T190000Z")


@pytest.mark.parametrize("bad", ["", "{", json.dumps({"run_id": "wrong"}) + "\n"])
def test_progress_malformed_or_torn_is_not_silently_truncated(tmp_path, bad):
    path = tmp_path / G.PROGRESS_NAME
    # Empty progress alone is valid before the first batch; corrupt a nonempty line.
    path.write_text(bad if bad else '{"run_id": "partial"}')
    with pytest.raises((RuntimeError, json.JSONDecodeError)):
        G.read_progress(path, "gd3-20261002T180000Z")


def test_append_only_progress_keeps_prefix_and_checks_each_batch(tmp_path):
    path = tmp_path / G.PROGRESS_NAME
    prefix = "gd3-20261002T180000Z"
    G.append_progress(path, {"run_id": prefix + "-b00000", "status": "SUCCESS", "batch_rows": 50,
                             "rows_written": 50})
    before = path.read_bytes()
    G.append_progress(path, {"run_id": prefix + "-b00001", "status": "SUCCESS", "batch_rows": 1,
                             "rows_written": 51})
    assert path.read_bytes().startswith(before)
    assert G.read_progress(path, prefix)[:2] == (2, 51)
    G.append_progress(path, {"run_id": prefix + "-b00002", "status": "SUCCESS", "batch_rows": 51,
                             "rows_written": 102})
    with pytest.raises(RuntimeError, match="sequence/counts"):
        G.read_progress(path, prefix)


def test_stale_execution_lock_requires_controller(tmp_path):
    with G.execution_lock(tmp_path):
        with pytest.raises(RuntimeError, match="controller inspection"):
            with G.execution_lock(tmp_path):
                pytest.fail("must not take existing lock")
    assert not (tmp_path / ".gd3_execute.lock").exists()


def test_writer_guard_runs_before_every_short_write_transaction(monkeypatch):
    transactions = []
    guards = []

    class Engine:
        @contextmanager
        def begin(self):
            transactions.append([])
            yield self

        def execute(self, statement, rows):
            transactions[-1].append(len(rows))

    monkeypatch.setattr(W, "_start_run", lambda *args: transactions[-1].append(1))
    monkeypatch.setattr(W, "_finish_run", lambda *args, **kwargs: transactions[-1].append(1) or "SUCCESS")
    monkeypatch.setattr(W, "event_row", lambda event, **kwargs: event)
    keys = [("form4", str(i)) for i in range(50)]
    plan = pd.DataFrame([{"channel": key[0], "dedup_key": key[1], "op": "insert"} for key in keys])
    W._apply_locked(Engine(), {key: {} for key in keys}, {"insert": 0}, plan,
                    run_id="test", mode="backfill", observed_at=datetime.now(timezone.utc), inputs={},
                    before_transaction=lambda: guards.append(len(transactions)))
    assert transactions == [[1], [50], [1]] and guards == [0, 1, 2]


def test_writer_never_opens_next_transaction_after_blackout(monkeypatch):
    transactions = []

    class Engine:
        @contextmanager
        def begin(self):
            transactions.append(1)
            yield self

    monkeypatch.setattr(W, "_start_run", lambda *args: None)

    def guard():
        if transactions:
            raise G.WriteWindowClosed("blackout")

    with pytest.raises(G.WriteWindowClosed):
        W._apply_locked(Engine(), {}, {"insert": 0}, pd.DataFrame([{"op": "insert"}]), run_id="test",
                        mode="backfill", observed_at=datetime.now(timezone.utc), inputs={}, before_transaction=guard)
    assert transactions == [1]
