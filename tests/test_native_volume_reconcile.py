import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from scripts.native_volume_reconcile import reconcile
from ingestion.realtime.native_volume_worker import run_once, owner_lock
from ingestion.realtime.research_intervals import Session

T = datetime(2026, 11, 10, 15, tzinfo=timezone.utc)


class Calendar:
    def __call__(self, key):
        return Session(
            key, T - timedelta(minutes=30), T + timedelta(hours=6), "fixture-v1"
        )

    def prior_sessions(self, key, count):
        return [
            Session(
                str(i),
                T - timedelta(days=i, minutes=30),
                T - timedelta(days=i, minutes=29),
                "fixture-v1",
            )
            for i in range(10, 0, -1)
        ]


class Client:
    _max_retries = 0

    def __init__(self):
        self.calls = []

    def price_history(self, symbol, **kw):
        self.calls.append(kw)
        times = [T - timedelta(days=i, minutes=30) for i in range(10, 0, -1)]
        times += [T - timedelta(minutes=30) + timedelta(minutes=i) for i in range(40)]
        return {
            "symbol": symbol,
            "candles": [
                dict(
                    datetime=int(t.timestamp() * 1000),
                    open=2,
                    high=3,
                    low=1,
                    close=2,
                    volume=10,
                )
                for t in times
                if kw["startDate"] <= int(t.timestamp() * 1000) <= kw["endDate"]
            ],
        }


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.now = T + timedelta(seconds=5)
        self.client = Client()

    def run_worker(self):
        return run_once(
            self.client,
            root=self.root,
            calendar=Calendar(),
            session_key="today",
            initialize_store=True,
            clock=lambda: self.now,
        )

    def digest(self):
        return next((self.root / "receipts").glob("*.json")).stem

    def recover(self, **kw):
        return reconcile(
            root=self.root,
            calendar=Calendar(),
            receipt_sha256=self.digest(),
            clock=lambda: self.now,
            **kw,
        )

    def failed_append(self):
        with patch(
            "ingestion.realtime.native_volume_cache.append",
            side_effect=RuntimeError("simulated crash"),
        ):
            with self.assertRaises(RuntimeError):
                self.run_worker()
        self.assertEqual(
            json.loads((self.root / "checkpoint.json").read_text())["status"], "failed"
        )

    def test_append_failure_replay(self):
        self.failed_append()
        original = (self.root / "checkpoint.json").read_bytes()
        result = self.recover()
        self.assertEqual(result["status"], "reconciled")
        self.assertFalse(result["requested"])
        self.assertGreater(result["inserted"], 0)
        self.assertEqual(len(self.client.calls), 1)
        self.assertEqual(
            json.loads((self.root / "checkpoint.json").read_text())["status"],
            "complete",
        )
        archive = list(self.root.glob("checkpoint.before-reconcile.*.json"))
        self.assertEqual(len(archive), 1)
        self.assertEqual(json.loads(archive[0].read_bytes()), json.loads(original))
        self.assertEqual(self.recover()["inserted"], 0)

    def test_republish_completed_no_fetch(self):
        self.run_worker()
        before = (self.root / "minutes.sqlite").read_bytes()
        result = self.recover(republish_only=True)
        self.assertEqual(result["status"], "republished")
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(before, (self.root / "minutes.sqlite").read_bytes())
        self.assertEqual(len(self.client.calls), 1)

    def test_snapshot_failure_republish(self):
        self.failed_append()
        with patch(
            "scripts.native_volume_reconcile.write_snapshot",
            side_effect=OSError("disk"),
        ):
            with self.assertRaises(OSError):
                self.recover()
        self.assertEqual(
            json.loads((self.root / "checkpoint.json").read_text())["status"],
            "complete",
        )
        self.assertEqual(self.recover(republish_only=True)["status"], "republished")
        self.assertEqual(len(self.client.calls), 1)

    def test_republish_failed_refused(self):
        self.failed_append()
        with self.assertRaises(ValueError):
            self.recover(republish_only=True)
        self.assertEqual(
            json.loads((self.root / "checkpoint.json").read_text())["status"], "failed"
        )

    def test_incomplete_receipt_not_promoted(self):
        class Empty(Client):
            def price_history(self, symbol, **kw):
                self.calls.append(kw)
                return {"symbol": symbol, "candles": []}

        self.client = Empty()
        with self.assertRaises(RuntimeError):
            self.run_worker()
        before = (self.root / "checkpoint.json").read_bytes()
        with self.assertRaises(ValueError):
            self.recover()
        self.assertEqual((self.root / "checkpoint.json").read_bytes(), before)
        self.assertEqual(len(self.client.calls), 1)

    def test_no_receipt_refuses(self):
        self.run_worker()
        before = (self.root / "checkpoint.json").read_bytes()
        with self.assertRaises(FileNotFoundError):
            reconcile(
                root=self.root,
                calendar=Calendar(),
                receipt_sha256="0" * 64,
                clock=lambda: self.now,
            )
        self.assertEqual(before, (self.root / "checkpoint.json").read_bytes())

    def test_tampered_receipt_refuses(self):
        self.failed_append()
        p = next((self.root / "receipts").glob("*.json"))
        p.write_bytes(p.read_bytes() + b" ")
        with self.assertRaises(ValueError):
            self.recover()
        self.assertEqual(
            json.loads((self.root / "checkpoint.json").read_text())["status"], "failed"
        )

    def test_wrong_request_refuses(self):
        self.failed_append()
        p = self.root / "checkpoint.json"
        state = json.loads(p.read_text())
        state["mode"] = "incremental"
        p.write_text(json.dumps(state))
        with self.assertRaises(ValueError):
            self.recover()
        self.assertEqual(json.loads(p.read_text())["status"], "failed")

    def test_wrong_symbol_refuses(self):
        self.failed_append()
        p = self.root / "checkpoint.json"
        state = json.loads(p.read_text())
        state["symbol"] = "QQQ"
        p.write_text(json.dumps(state))
        with self.assertRaises(ValueError):
            self.recover()

    def test_locks_prevent_race(self):
        self.run_worker()
        with owner_lock(self.root):
            with self.assertRaises(RuntimeError):
                self.recover()

    def test_receipt_future_refused(self):
        self.run_worker()
        with self.assertRaises(ValueError):
            reconcile(
                root=self.root,
                calendar=Calendar(),
                receipt_sha256=self.digest(),
                clock=lambda: self.now - timedelta(seconds=1),
            )

    def test_path_traversal_refused(self):
        self.run_worker()
        with self.assertRaises(ValueError):
            reconcile(
                root=self.root,
                calendar=Calendar(),
                receipt_sha256="../secret",
                clock=lambda: self.now,
            )

    def test_receipt_symlink_refused(self):
        self.run_worker()
        p = next((self.root / "receipts").glob("*.json"))
        raw = p.read_bytes()
        other = self.root / "other"
        other.write_bytes(raw)
        other.chmod(0o600)
        p.unlink()
        p.symlink_to(other)
        with self.assertRaises(ValueError):
            self.recover()


if __name__ == "__main__":
    unittest.main()
