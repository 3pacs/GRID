from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from ingestion.realtime.minute_retention import initialize, append, sqlite_loader
from ingestion.realtime.native_minutes import select_as_of
from tests.test_research_intervals import T, S, bars


class TestMinuteRetention(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "minutes.db"
        initialize(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def load(self, when=S.closes):
        return sqlite_loader(self.path)("SPY", "fixture", S, when)

    def test_append_idempotent_reopen(self):
        self.assertEqual(append(self.path, bars(3)), 3)
        initialize(self.path)
        self.assertEqual(append(self.path, bars(3)), 0)
        self.assertEqual(self.load(), tuple(bars(3)))

    def test_retained_response_replay_and_later_receipt(self):
        from tests.test_native_minute_api import parse

        first = parse()
        later = parse(received_at=T + timedelta(minutes=4))
        self.assertEqual(append(self.path, first.bars), 3)
        self.assertEqual(append(self.path, first.bars), 0)
        self.assertEqual(append(self.path, later.bars), 3)
        rows = sqlite_loader(self.path)("SPY", "schwab_pricehistory", S, S.closes)
        self.assertEqual(len(rows), 6)
        selected = select_as_of(
            rows,
            symbol="SPY",
            source="schwab_pricehistory",
            price_basis="provider_unspecified",
            opens=S.opens,
            closes=S.closes,
            as_of=T + timedelta(minutes=3),
        )
        self.assertEqual(selected, first.bars)

    def test_revision_and_cutoff(self):
        old = bars(1)[0]
        new = replace(
            old, revision="new", available_at=T + timedelta(minutes=4), close=101.5
        )
        append(self.path, [old, new])
        self.assertEqual(len(self.load(T + timedelta(minutes=3))), 1)
        selected = select_as_of(
            self.load(),
            symbol="SPY",
            source="fixture",
            price_basis="unadjusted",
            opens=S.opens,
            closes=S.closes,
            as_of=S.closes,
        )
        self.assertEqual(selected, (new,))

    def test_conflict_rolls_back_entire_batch(self):
        append(self.path, bars(1))
        conflict = replace(bars(1)[0], close=101.5)
        with self.assertRaises(ValueError):
            append(self.path, [bars(2)[1], conflict])
        self.assertEqual(len(self.load()), 1)

    def test_guards(self):
        append(self.path, bars(1))
        for sql in (
            'UPDATE minute_revisions SET source="other"',
            "DELETE FROM minute_revisions",
        ):
            with sqlite3.connect(self.path) as conn:
                with self.assertRaises(sqlite3.DatabaseError):
                    conn.execute(sql)
        self.assertEqual(len(self.load()), 1)

    def test_corruption_detected(self):
        append(self.path, bars(1))
        with sqlite3.connect(self.path) as conn:
            conn.execute("DROP TRIGGER no_minute_update")
            conn.execute('UPDATE minute_revisions SET record_sha256="bad"')
        with self.assertRaises(ValueError):
            self.load()

    def test_unknown_database_untouched(self):
        p = Path(self.tmp.name) / "other.db"
        with sqlite3.connect(p) as conn:
            conn.execute("CREATE TABLE unrelated(x)")
        before = p.read_bytes()
        with self.assertRaises(ValueError):
            initialize(p)
        self.assertEqual(p.read_bytes(), before)

    def test_readonly_missing_does_not_create(self):
        p = Path(self.tmp.name) / "absent.db"
        with self.assertRaises(sqlite3.OperationalError):
            sqlite_loader(p)("SPY", "fixture", S, S.closes)
        self.assertFalse(p.exists())

    def test_concurrent_replay(self):
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: append(self.path, bars(3)), range(2)))
        self.assertEqual(sum(results), 3)
        self.assertEqual(len(self.load()), 3)

    def test_bounds(self):
        with self.assertRaises(ValueError):
            append(self.path, bars(3), max_rows=2)
        self.assertEqual(self.load(), ())
        append(self.path, bars(3))
        with self.assertRaises(ValueError):
            sqlite_loader(self.path, max_rows=2)("SPY", "fixture", S, S.closes)

    def test_invalid_source_not_retained(self):
        with self.assertRaises(ValueError):
            append(self.path, [replace(bars(1)[0], quality="sampled")])
        self.assertEqual(self.load(), ())


if __name__ == "__main__":
    unittest.main()
