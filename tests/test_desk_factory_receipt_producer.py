"""Real producer-to-caller seam using generated temporary bars only."""
import importlib.util
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from analysis.desk_factory_receipt import receipt_bound_factory_cards
from analysis.factory_session_calendar import FactoryCalendar, PINS


class ProducerReceiptSeamTest(unittest.TestCase):
    def test_producer_report_and_anchored_result_join(self):
        # Reuse the established factory fixture rather than a second bar generator.
        path = Path(__file__).with_name("test_module_factory.py")
        spec = importlib.util.spec_from_file_location("desk_factory_fixture", path)
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        for plant, seed in ((0.9, 7), (0.0, 11)):
            with self.subTest(plant=plant), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                # The calendar reader refuses group-writable manifests. Copy
                # unchanged pinned bytes into PRIVATE test files, leaving the
                # checkout and real calendar permissions untouched.
                calendars = root / "calendars"
                calendars.mkdir()
                source = Path(mf.__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
                for name, _ in PINS:
                    fd = os.open(calendars / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, "wb") as stream:
                        stream.write((source / name).read_bytes())
                calendar = FactoryCalendar(calendars)
                data, days = fixture.staged(root, plant=plant, seed=seed)
                run_file, _ = fixture.run_file(root, days)
                ledger_path, anchor_path = fixture.file_ledger(root)
                ledger = lse.Ledger(ledger_path, anchor=anchor_path)
                with patch.object(mf, "FactoryCalendar", return_value=calendar):
                    report = mf.run(run_file, data, ledger, root / "out")
                verified = lse.Ledger(ledger_path, anchor=anchor_path)
                verified.verify(full=True)
                raw = (root / "out/report.json").read_bytes()
                result_line = ledger_path.read_bytes().splitlines()[-1]
                observed = datetime.fromisoformat(verified.records[-1]["recorded_at"])
                output = receipt_bound_factory_cards(
                    raw, result_line, expected_result_sha256=verified.head,
                    receipt_available_at=observed, as_of=observed,
                    max_age=timedelta(hours=1),
                )
                self.assertEqual(output["selected_count"], len(report["selected"]))
                self.assertEqual(output["tested_trials"], report["tested"])
                self.assertFalse(output["promotion_allowed"])
                self.assertEqual(output["integrity"], "report_canonical_object_matches_pinned_result_receipt")
                if plant:
                    self.assertGreater(output["selected_count"], 0)
                else:
                    self.assertEqual(output["outcome"], "no_selected_candidates")
                with self.assertRaisesRegex(ValueError, "canonical-object"):
                    receipt_bound_factory_cards(
                        raw.replace(b'"target_unit": "vol"', b'"target_unit": "bps"'),
                        result_line, expected_result_sha256=verified.head,
                        receipt_available_at=observed, as_of=observed,
                        max_age=timedelta(hours=1),
                    )


if __name__ == "__main__":
    unittest.main()
