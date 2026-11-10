import copy
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ingestion.realtime.native_volume_snapshot import (
    build_snapshot,
    write_snapshot,
    canonical,
)
from tests.test_native_volume import NativeVolumeTests


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.fixture = NativeVolumeTests()
        self.fixture.setUp()
        f = self.fixture

        class Calendar:
            def __call__(self, key):
                return f.current

            def prior_sessions(self, key, count):
                return f.prior

        self.calendar = Calendar()
        self.loader = lambda symbol, source, session, as_of: tuple(
            r for r in f.rows if session.opens <= r.ts < session.closes
        )
        self.snapshot = build_snapshot(
            self.loader, self.calendar, session_key="today", symbol="SPY", now=f.now
        )

    def write(self, path, snapshot=None):
        return write_snapshot(path, self.snapshot if snapshot is None else snapshot)

    def test_view_and_hash(self):
        d = self.snapshot
        self.assertEqual(d["payload"]["status"], "ready")
        self.assertEqual(d["payload"]["rvol"], 1)
        self.assertEqual(
            d["payload_sha256"], hashlib.sha256(canonical(d["payload"])).hexdigest()
        )
        self.assertEqual(d["provenance"]["retained_rows_consulted"], 3905)
        self.assertFalse(d["payload"]["historical_request"])
        self.assertFalse(d["payload"]["live"])

    def test_atomic_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "snapshot.json"
            receipt = self.write(p)
            self.assertEqual(json.loads(p.read_text()), self.snapshot)
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)
            self.assertEqual(receipt["bytes"], p.stat().st_size)
            self.write(p)
            self.assertEqual(len(list(Path(d).iterdir())), 1)

    def test_wrong_hash_preserves_existing(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "snapshot.json"
            self.write(p)
            before = p.read_bytes()
            bad = copy.deepcopy(self.snapshot)
            bad["payload"]["rvol"] = 2
            with self.assertRaises(ValueError):
                self.write(p, bad)
            self.assertEqual(before, p.read_bytes())

    def test_unsafe_parent(self):
        with tempfile.TemporaryDirectory() as d:
            os.chmod(d, 0o755)
            with self.assertRaises(ValueError):
                self.write(Path(d) / "snapshot.json")

    def test_unsafe_target(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "snapshot.json"
            p.write_text("old")
            os.chmod(p, 0o644)
            with self.assertRaises(ValueError):
                self.write(p)
            self.assertEqual(p.read_text(), "old")

    def test_target_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            other = Path(d) / "other"
            other.write_text("safe")
            p = Path(d) / "snapshot.json"
            p.symlink_to(other)
            with self.assertRaises(ValueError):
                self.write(p)
            self.assertEqual(other.read_text(), "safe")

    def test_parent_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            parent = Path(d) / "private"
            parent.mkdir(mode=0o700)
            link = Path(d) / "link"
            link.symlink_to(parent)
            with self.assertRaises((ValueError, OSError)):
                self.write(link / "snapshot.json")

    def test_failed_replace_keeps_old_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "snapshot.json"
            self.write(p)
            before = p.read_bytes()
            with patch(
                "ingestion.realtime.native_volume_snapshot.os.replace",
                side_effect=OSError("test"),
            ):
                with self.assertRaises(OSError):
                    self.write(p)
            self.assertEqual(p.read_bytes(), before)
            self.assertEqual(len(list(Path(d).iterdir())), 1)

    def test_bad_metrics_and_flags(self):
        for field, value in [
            ("rvol", float("nan")),
            ("rvol", True),
            ("cumulative_shares", 1.5),
            ("exact_trade_vwap", True),
            ("source", "yahoo"),
        ]:
            with self.subTest(field=field):
                bad = copy.deepcopy(self.snapshot)
                bad["payload"][field] = value
                with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
                    self.write(Path(d) / "snapshot.json", bad)

    def test_stale_event_rejected_even_valid_hash(self):
        bad = copy.deepcopy(self.snapshot)
        bad["payload"]["end_of_completed_window"] = (
            self.fixture.now - timedelta(minutes=10)
        ).isoformat()
        bad["payload_sha256"] = hashlib.sha256(canonical(bad["payload"])).hexdigest()
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            self.write(Path(d) / "snapshot.json", bad)

    def test_gaps_remain_unavailable(self):
        d = build_snapshot(
            lambda *args: (),
            self.calendar,
            session_key="today",
            symbol="SPY",
            now=self.fixture.now,
        )
        self.assertEqual(d["payload"]["status"], "unavailable")
        self.assertIsNone(d["payload"]["rvol"])

    def test_unknown_fields_refused(self):
        bad = copy.deepcopy(self.snapshot)
        bad["payload"]["unapproved_extra"] = "not exported"
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            self.write(Path(d) / "snapshot.json", bad)

    def test_nonfinite_nested_rejected(self):
        bad = copy.deepcopy(self.snapshot)
        bad["payload"]["baseline_errors"] = {"x": float("inf")}
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            self.write(Path(d) / "snapshot.json", bad)


if __name__ == "__main__":
    unittest.main()
