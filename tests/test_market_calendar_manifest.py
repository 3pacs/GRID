"""Offline manifest parser regressions adapted from a3172b27; no exporter or provider."""

import copy
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from ingestion.market_calendar import (
    ManifestCalendar,
    canonical,
    SCHEMA,
)

VERSION = "exchange_calendars:4.11.2:pandas:2.3.3:numpy:2.4.0:tzdata:2025.2:XNYS:2026-01-01:2027-12-31"


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.doc = dict(
            schema=SCHEMA,
            exchange="XNYS",
            session_type="RTH",
            timezone="America/New_York",
            start="2026-11-25",
            end="2026-11-30",
            calendar_version=VERSION,
            sessions=[
                dict(
                    key="XNYS:2026-11-25:RTH",
                    opens="2026-11-25T14:30:00+00:00",
                    closes="2026-11-25T21:00:00+00:00",
                ),
                dict(
                    key="XNYS:2026-11-27:RTH",
                    opens="2026-11-27T14:30:00+00:00",
                    closes="2026-11-27T18:00:00+00:00",
                ),
                dict(
                    key="XNYS:2026-11-30:RTH",
                    opens="2026-11-30T14:30:00+00:00",
                    closes="2026-11-30T21:00:00+00:00",
                ),
            ],
        )

    def load(self, doc=None):
        raw = canonical(self.doc if doc is None else doc)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "calendar.json"
            p.write_bytes(raw)
            p.chmod(0o600)
            return ManifestCalendar(p, hashlib.sha256(raw).hexdigest())

    def test_fifo_refused_without_block(self):
        import signal

        def expired(signum, frame):
            raise AssertionError("manifest FIFO open blocked")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "calendar.json"
            os.mkfifo(path, 0o600)
            previous = signal.signal(signal.SIGALRM, expired)
            signal.alarm(2)
            try:
                with self.assertRaises(ValueError):
                    ManifestCalendar(path, "a" * 64)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, previous)

    def test_roundtrip(self):
        c = self.load()
        self.assertEqual(c.start, date(2026, 11, 25))
        self.assertEqual(len(c.prior_sessions("XNYS:2026-11-30:RTH", 2)), 2)
        self.assertEqual(
            (
                c("XNYS:2026-11-27:RTH").closes - c("XNYS:2026-11-27:RTH").opens
            ).total_seconds(),
            210 * 60,
        )
        self.assertIn("manifest:", c.version)

    def test_unknown_and_insufficient(self):
        c = self.load()
        for key in ("XNYS:2026-11-26:RTH", "XNYS:2027-01-04:RTH", "other"):
            with self.assertRaises(ValueError):
                c(key)
        with self.assertRaises(ValueError):
            c.prior_sessions("XNYS:2026-11-30:RTH", 10)
        with self.assertRaises(ValueError):
            c.prior_sessions("XNYS:2026-11-30:RTH", True)

    def test_mutations_refused(self):
        changes = [
            ("exchange", "CME"),
            ("timezone", "UTC"),
            ("calendar_version", "unverified"),
            ("end", "2026-11-26"),
            ("start", "2027-01-01"),
        ]
        for key, val in changes:
            doc = copy.deepcopy(self.doc)
            doc[key] = val
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.load(doc)

    def test_invalid_rows(self):
        for key, val in [
            ("opens", "2026-11-25T14:30:00"),
            ("opens", "2026-11-25T14:30:00-05:00"),
            ("closes", "2026-11-25T14:00:00+00:00"),
            ("key", "XNYS:2026-11-24:RTH"),
        ]:
            doc = copy.deepcopy(self.doc)
            doc["sessions"][0][key] = val
            with self.subTest(key=key, val=val), self.assertRaises(ValueError):
                self.load(doc)

    def test_order_duplicates(self):
        for rows in (
            list(reversed(self.doc["sessions"])),
            [self.doc["sessions"][0]] * 2,
        ):
            doc = copy.deepcopy(self.doc)
            doc["sessions"] = rows
            with self.assertRaises(ValueError):
                self.load(doc)

    def test_hash_required(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "c"
            p.write_bytes(canonical(self.doc))
            p.chmod(0o600)
            for digest in ("0" * 64, "", None):
                with self.assertRaises(ValueError):
                    ManifestCalendar(p, digest)

    def test_symlink_and_writable(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "c"
            raw = canonical(self.doc)
            p.write_bytes(raw)
            p.chmod(0o600)
            digest = hashlib.sha256(raw).hexdigest()
            link = Path(d) / "link"
            link.symlink_to(p)
            with self.assertRaises(ValueError):
                ManifestCalendar(link, digest)
            os.chmod(p, 0o666)
            with self.assertRaises(ValueError):
                ManifestCalendar(p, digest)

    def test_duplicate_json_keys(self):
        raw = canonical(self.doc)
        raw = raw[:-1] + b',"schema":"grid.xnys-session-manifest.v1"}'
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "c"
            p.write_bytes(raw)
            p.chmod(0o600)
            with self.assertRaises(ValueError):
                ManifestCalendar(p, hashlib.sha256(raw).hexdigest())

    def test_pretty_bytes_refused(self):
        raw = json.dumps(self.doc, indent=2).encode()
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "c"
            p.write_bytes(raw)
            p.chmod(0o600)
            with self.assertRaises(ValueError):
                ManifestCalendar(p, hashlib.sha256(raw).hexdigest())

    def test_closed_is_distinct_from_outside_verified_bounds(self):
        c = self.load()
        self.assertIsNone(c.for_date(date(2026, 11, 26)))
        self.assertIsNotNone(c.for_date(date(2026, 11, 27)))
        with self.assertRaises(ValueError):
            c.for_date(date(2026, 11, 24))

    def test_wrong_dst_open_is_refused(self):
        doc = copy.deepcopy(self.doc)
        doc["sessions"][0]["opens"] = "2026-11-25T13:30:00+00:00"
        doc["sessions"][0]["closes"] = "2026-11-25T20:00:00+00:00"
        with self.assertRaises(ValueError):
            self.load(doc)


if __name__ == "__main__":
    unittest.main()
