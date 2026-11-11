from dataclasses import replace
from datetime import timedelta
import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from ingestion.realtime.native_volume_health import evaluate
from ingestion.realtime.native_volume_snapshot import canonical
from scripts.native_volume_health import inspect, main
from tests import test_native_volume_snapshot as fixture_module
from tests import test_native_volume_reconcile as recovery_module


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.f = fixture_module.SnapshotTests()
        self.f.setUp()
        v = self.f.fixture
        self.snapshot = self.f.snapshot
        self.now = v.now
        self.session = v.current
        self.checkpoint = dict(
            schema="grid.native-volume-worker.v1",
            symbol="SPY",
            session=v.current.key,
            status="complete",
            mode="bootstrap",
            started_at=v.now.isoformat(),
            window_end=self.snapshot["payload"]["end_of_completed_window"],
            received_at=v.now.isoformat(),
            receipt_sha256="a" * 64,
        )
        self.args = dict(
            enabled=True,
            now=self.now,
            session=self.session,
            calendar_version=v.current.calendar_version,
            baseline_sessions=[s.key for s in v.prior],
            checkpoint=self.checkpoint,
            snapshot=self.snapshot,
            disk_bytes=1_000_000,
            free_bytes=1_000_000_000,
        )

    def check(self, **kw):
        return evaluate(**dict(self.args, **kw))

    def rehash(self):
        self.snapshot["payload_sha256"] = hashlib.sha256(
            canonical(self.snapshot["payload"])
        ).hexdigest()

    def test_healthy_is_not_live_acceptance(self):
        result = self.check()
        self.assertEqual(result["status"], "healthy")
        self.assertTrue(result["ready"])
        self.assertFalse(result["requested"])
        self.assertFalse(result["live_acceptance"])
        self.assertFalse(result["signal_confirmation"])
        self.assertNotIn("cumulative_shares", json.dumps(result))

    def test_disabled_no_alert_for_staging(self):
        self.assertEqual(
            self.check(enabled=False, checkpoint=None, snapshot=None)["status"],
            "disabled",
        )

    def test_calendar_failure_is_not_closed(self):
        x = self.check(session=None, read_errors=("calendar_unavailable",))
        self.assertEqual(x["reasons"], ["calendar_unavailable"])
        self.assertTrue(x["alert"])

    def test_verified_closed_and_premarket_and_after_hours(self):
        for change in (
            dict(session=None),
            dict(now=self.session.opens),
            dict(now=self.session.closes + timedelta(minutes=6)),
        ):
            with self.subTest(change=change):
                x = self.check(**change)
                self.assertEqual(x["status"], "idle")
                self.assertFalse(x["alert"])

    def test_first_completed_boundary(self):
        self.assertEqual(
            self.check(now=self.session.opens + timedelta(seconds=64))["status"], "idle"
        )
        self.assertTrue(
            self.check(
                now=self.session.opens + timedelta(seconds=65),
                checkpoint=None,
                snapshot=None,
            )["alert"]
        )

    def test_missing_and_nonzero_exit(self):
        x = self.check(checkpoint=None, snapshot=None, last_exit_code=1)
        self.assertTrue(
            {"checkpoint_missing", "snapshot_missing", "invocation_failed"}
            <= set(x["reasons"])
        )

    def test_unknown_exit_not_success(self):
        self.assertIn("invalid_exit_status", self.check(last_exit_code=True)["reasons"])

    def test_missing_or_exhausted_disk(self):
        for value, code in [
            (None, "disk_usage_unknown"),
            (105_000_000, "disk_budget_exhausted"),
        ]:
            self.assertIn(code, self.check(disk_bytes=value)["reasons"])

    def test_checkpoint_failure_statuses(self):
        for status in ("failed", "coverage_failed", "other"):
            with self.subTest(status=status):
                self.checkpoint["status"] = status
                self.assertIn("checkpoint_incomplete", self.check()["reasons"])

    def test_active_grace_and_expired(self):
        self.checkpoint["status"] = "in_progress"
        x = self.check(snapshot=None)
        self.assertEqual(x["status"], "pending")
        self.assertFalse(x["alert"])
        self.assertFalse(x["ready"])
        self.assertEqual(
            self.check(now=self.now + timedelta(seconds=45), snapshot=None)["status"],
            "pending",
        )
        x = self.check(now=self.now + timedelta(seconds=46), snapshot=None)
        self.assertIn("checkpoint_incomplete", x["reasons"])
        self.assertIn("snapshot_missing", x["reasons"])

    def test_stale_all_clocks(self):
        x = self.check(now=self.now + timedelta(seconds=181))
        self.assertTrue(
            {"checkpoint_stale", "snapshot_stale", "source_stale"} <= set(x["reasons"])
        )

    def test_fresh_generation_does_not_hide_old_source(self):
        self.snapshot["payload"]["end_of_completed_window"] = (
            self.now - timedelta(seconds=121)
        ).isoformat()
        self.rehash()
        self.assertIn("snapshot_invalid", self.check()["reasons"])

    def test_changed_snapshot_hash(self):
        self.snapshot["payload"]["rvol"] = 12
        self.assertIn("snapshot_invalid", self.check()["reasons"])

    def test_wrong_calendar_source_symbol_and_session(self):
        for key, value in [
            ("calendar_version", "wrong"),
            ("source", "yahoo"),
            ("symbol", "QQQ"),
            ("session", "yesterday"),
        ]:
            with self.subTest(key=key):
                original = self.snapshot["payload"][key]
                self.snapshot["payload"][key] = value
                self.rehash()
                self.assertIn("snapshot_invalid", self.check()["reasons"])
                self.snapshot["payload"][key] = original
        self.rehash()

    def test_baseline_order_is_exact(self):
        self.snapshot["payload"]["baseline_sessions"].reverse()
        self.rehash()
        self.assertIn("snapshot_invalid", self.check()["reasons"])

    def test_coverage_count(self):
        self.snapshot["payload"]["completed_minutes"] = 0
        self.rehash()
        self.assertIn("snapshot_invalid", self.check()["reasons"])

    def test_checkpoint_snapshot_mismatch(self):
        self.checkpoint["window_end"] = (
            self.now.replace(second=0) - timedelta(minutes=1)
        ).isoformat()
        self.assertEqual(self.check()["status"], "pending")
        self.assertIn(
            "snapshot_checkpoint_mismatch",
            self.check(now=self.now + timedelta(seconds=46))["reasons"],
        )

    def test_complete_checkpoint_waits_for_bounded_publish(self):
        pending = self.check(snapshot=None)
        self.assertEqual(pending["status"], "pending")
        self.assertFalse(pending["ready"])
        self.assertFalse(pending["alert"])
        self.assertIn(
            "snapshot_missing",
            self.check(snapshot=None, now=self.now + timedelta(seconds=46))["reasons"],
        )
        self.assertIn(
            "invocation_failed", self.check(snapshot=None, last_exit_code=1)["reasons"]
        )

    def test_previous_session_snapshot_has_only_bounded_bootstrap_grace(self):
        from datetime import datetime

        self.snapshot["generated_at"] = (self.now - timedelta(days=1)).isoformat()
        payload = self.snapshot["payload"]
        payload["session"] = self.args["baseline_sessions"][-1]
        for key in (
            "as_of",
            "served_at",
            "latest_receipt_at",
            "end_of_completed_window",
        ):
            payload[key] = (
                datetime.fromisoformat(payload[key]) - timedelta(days=1)
            ).isoformat()
        self.rehash()
        for status in ("complete", "in_progress"):
            self.checkpoint["status"] = status
            pending = self.check()
            self.assertEqual(pending["status"], "pending")
            self.assertFalse(pending["alert"])
            self.assertFalse(pending["ready"])
            self.assertTrue(self.check(now=self.now + timedelta(seconds=46))["alert"])
        self.snapshot["provenance"] = None
        self.assertIn("snapshot_invalid", self.check()["reasons"])

    def test_malformed_provenance_cannot_be_healthy(self):
        good = copy.deepcopy(self.snapshot["provenance"])
        cases = [
            None,
            [],
            {},
            dict(good, retained_rows_consulted=True),
            dict(good, retained_rows_consulted=-1),
            dict(good, retained_rows_consulted=1_100_001),
            dict(good, retained_rows_sha256=None),
            dict(good, retained_rows_sha256="invalid"),
        ]
        for provenance in cases:
            with self.subTest(provenance=provenance):
                self.snapshot["provenance"] = provenance
                x = self.check()
                self.assertIn("snapshot_invalid", x["reasons"])
                self.assertTrue(x["alert"])
        self.snapshot["provenance"] = good

    def test_contradictory_ready_payload_is_invalid(self):
        for key, value in [
            ("reason", "current_session_gap"),
            ("baseline_errors", {"prior": "baseline_session_gap"}),
        ]:
            original = self.snapshot["payload"][key]
            self.snapshot["payload"][key] = value
            self.rehash()
            self.assertIn("snapshot_invalid", self.check()["reasons"])
            self.snapshot["payload"][key] = original
        self.rehash()

    def test_checkpoint_wrong_identity_or_future(self):
        for key, value in [
            ("symbol", "QQQ"),
            ("session", "yesterday"),
            ("received_at", (self.now + timedelta(seconds=1)).isoformat()),
            ("receipt_sha256", ""),
        ]:
            with self.subTest(key=key):
                original = self.checkpoint[key]
                self.checkpoint[key] = value
                self.assertIn("checkpoint_invalid", self.check()["reasons"])
                self.checkpoint[key] = original

    def test_unavailable_metric_is_not_successful_exit(self):
        self.snapshot["payload"].update(status="degraded", rvol=None)
        self.rehash()
        x = self.check(last_exit_code=0)
        self.assertIn("metrics_unavailable", x["reasons"])
        self.assertFalse(x["ready"])

    def test_race_is_unknown_not_healthy(self):
        x = self.check(read_errors=("observation_raced",))
        self.assertEqual(x["status"], "unknown")
        self.assertFalse(x["ready"])
        y = self.check(read_errors=("observation_raced",), last_exit_code=1)
        self.assertIn("invocation_failed", y["reasons"])
        self.assertTrue(y["alert"])

    def test_post_close_does_not_demand_another_fetch(self):
        self.assertEqual(
            self.check(now=self.session.closes + timedelta(seconds=181))["status"],
            "idle",
        )

    def test_wrong_claim_flags(self):
        self.snapshot["payload"]["live"] = True
        self.rehash()
        self.assertIn("snapshot_invalid", self.check()["reasons"])


class ReaderTests(unittest.TestCase):
    rehash = HealthTests.rehash

    def setUp(self):
        HealthTests.setUp(self)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.session = replace(
            self.session, key=f"XNYS:{self.now.date().isoformat()}:RTH"
        )
        self.checkpoint["session"] = self.session.key
        self.snapshot["payload"]["session"] = self.session.key
        self.args["session"] = self.session
        self.rehash()
        owner = self

        class Calendar:
            start = owner.now.date() - timedelta(days=31)
            end = owner.now.date() + timedelta(days=31)
            version = owner.session.calendar_version
            sessions = (owner.session,)

            def prior_sessions(self, key, n):
                return owner.f.fixture.prior

        self.cal = patch(
            "scripts.native_volume_health.ManifestCalendar", return_value=Calendar()
        )
        self.cal.start()
        self.addCleanup(self.cal.stop)
        self.write("checkpoint.json", self.checkpoint)
        self.write("native-volume.json", self.snapshot)
        self.write("minutes.sqlite", {})
        (self.root / "receipts").mkdir(mode=0o700)

    def write(self, name, value):
        p = self.root / name
        p.write_bytes(canonical(value))
        p.chmod(0o600)

    def inspect(self, **kw):
        return inspect(
            root=self.root, manifest=None, digest=None, enabled=True, now=self.now, **kw
        )

    def fingerprint(self):
        return {
            str(p.relative_to(self.root)): (p.stat().st_mode, p.read_bytes())
            for p in self.root.rglob("*")
            if p.is_file()
        }

    def test_read_only_no_provider(self):
        before = self.fingerprint()
        with patch.object(socket, "socket", side_effect=AssertionError("no network")):
            x = self.inspect()
        self.assertEqual(x["status"], "healthy")
        self.assertEqual(before, self.fingerprint())

    def test_disabled_does_not_open_paths(self):
        with patch(
            "scripts.native_volume_health.os.open",
            side_effect=AssertionError("no open"),
        ):
            self.assertEqual(
                inspect(root=None, manifest=None, digest=None)["status"], "disabled"
            )

    def test_default_cli_is_disabled(self):
        with patch("builtins.print"):
            self.assertEqual(main([]), 0)

    def test_reader_missing(self):
        (self.root / "native-volume.json").unlink()
        self.assertEqual(self.inspect()["status"], "pending")

    def test_unsafe_custody(self):
        (self.root / "native-volume.json").chmod(0o644)
        self.assertIn("snapshot_unreadable", self.inspect()["reasons"])

    def test_symlink_refused(self):
        (self.root / "native-volume.json").unlink()
        (self.root / "native-volume.json").symlink_to(self.root / "checkpoint.json")
        self.assertIn("snapshot_unreadable", self.inspect()["reasons"])

    def test_hardlink_refused(self):
        os.link(self.root / "native-volume.json", self.root / "extra.json")
        self.assertIn("snapshot_unreadable", self.inspect()["reasons"])

    def test_fifo_refused_without_block(self):
        (self.root / "native-volume.json").unlink()
        os.mkfifo(self.root / "native-volume.json", 0o600)
        self.assertIn("snapshot_unreadable", self.inspect()["reasons"])

    def test_duplicate_json_refused(self):
        (self.root / "native-volume.json").write_text('{"schema":"a","schema":"b"}')
        self.assertIn("snapshot_unreadable", self.inspect()["reasons"])

    def test_bad_disk(self):
        (self.root / "minutes.sqlite").chmod(0o644)
        self.assertIn("disk_unreadable", self.inspect()["reasons"])

    def test_bad_calendar(self):
        with patch(
            "scripts.native_volume_health.ManifestCalendar",
            side_effect=ValueError("private path secret"),
        ):
            x = self.inspect()
        self.assertEqual(x["reasons"], ["calendar_unavailable"])
        self.assertNotIn("secret", json.dumps(x))

    def test_race_unknown(self):
        import scripts.native_volume_health as module

        original = module._json
        calls = []

        def reader(fd, name, cap):
            value = original(fd, name, cap)
            if name == "checkpoint.json":
                calls.append(name)
                if len(calls) == 2:
                    value["status"] = "in_progress"
            return value

        with patch.object(module, "_json", side_effect=reader):
            x = self.inspect()
        self.assertEqual(x["status"], "unknown")
        self.assertFalse(x["ready"])


class RecoveryDrillTests(unittest.TestCase):
    def setUp(self):
        original = recovery_module.Calendar

        class FullCalendar(original):
            def prior_sessions(self, key, count):
                return [
                    replace(s, closes=s.opens + timedelta(minutes=390))
                    for s in super().prior_sessions(key, count)
                ]

        class FullClient(recovery_module.Client):
            def price_history(self, symbol, **kw):
                self.calls.append(kw)
                calendar = FullCalendar()
                candles = []
                for session in (
                    *calendar.prior_sessions("today", 10),
                    calendar("today"),
                ):
                    t = session.opens
                    while t < session.closes:
                        stamp = int(t.timestamp() * 1000)
                        if kw["startDate"] <= stamp <= kw["endDate"]:
                            candles.append(
                                dict(
                                    datetime=stamp,
                                    open=2,
                                    high=3,
                                    low=1,
                                    close=2,
                                    volume=10,
                                )
                            )
                        t += timedelta(minutes=1)
                return dict(symbol=symbol, candles=candles)

        calendar_patch = patch.object(recovery_module, "Calendar", FullCalendar)
        calendar_patch.start()
        self.addCleanup(calendar_patch.stop)
        self.r = recovery_module.ReconcileTests()
        self.r.setUp()
        self.r.client = FullClient()
        self.addCleanup(self.r.doCleanups)

    def health(self, **kw):
        r = self.r
        state = json.loads((r.root / "checkpoint.json").read_text())
        p = r.root / "native-volume.json"
        snapshot = json.loads(p.read_text()) if p.exists() else None
        cal = recovery_module.Calendar()
        return evaluate(
            enabled=True,
            now=r.now,
            session=cal("today"),
            calendar_version="fixture-v1",
            baseline_sessions=[s.key for s in cal.prior_sessions("today", 10)],
            checkpoint=state,
            snapshot=snapshot,
            disk_bytes=1000,
            free_bytes=1_000_000_000,
            **kw,
        )

    def test_capture_append_crash_recover_republish_no_second_fetch(self):
        r = self.r
        r.failed_append()
        before = self.health()
        self.assertIn("checkpoint_incomplete", before["reasons"])
        self.assertIn("snapshot_missing", before["reasons"])
        first = r.recover()
        self.assertEqual(first["status"], "reconciled")
        self.assertEqual(self.health()["status"], "healthy")
        again = r.recover(republish_only=True)
        self.assertEqual(again["inserted"], 0)
        self.assertEqual(len(r.client.calls), 1)
        self.assertEqual(
            len(list(r.root.glob("checkpoint.before-reconcile.*.json"))), 2
        )

    def test_snapshot_write_failure_is_visible_then_local_republish(self):
        r = self.r
        r.failed_append()
        with patch(
            "scripts.native_volume_reconcile.write_snapshot",
            side_effect=OSError("disk"),
        ):
            with self.assertRaises(OSError):
                r.recover()
        self.assertEqual(self.health()["status"], "pending")
        r.recover(republish_only=True)
        self.assertEqual(self.health()["status"], "healthy")
        self.assertEqual(len(r.client.calls), 1)
