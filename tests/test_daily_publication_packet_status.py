"""Offline operational controls; no diary, capture, DB or Gemini execution."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "daily_packet_status", ROOT / "scripts/daily_publication_packet_status.py")
job = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(job)


def observation() -> dict:
    return {
        "schema": "daily-publication-packet-observation-v1",
        "publication_day": "2026-11-05",
        "observed_at": "2026-11-06T07:00:00Z",
        "due_at": "2026-11-06T06:00:00Z",
        "capture_attempt": "unknown",
        "stages": {},
    }


def passed_through(data: dict, last: str) -> dict:
    previous = None
    for index, stage in enumerate(job.STAGES):
        digest = f"{index + 1:064x}"
        data["stages"][stage] = {
            "state": "passed", "receipt_sha256": digest,
            "publication_day": data["publication_day"],
            "upstream_receipt_sha256": previous,
        }
        previous = digest
        if stage == "capture":
            data["capture_attempt"] = "spent"
        if stage == last:
            break
    return data


class PacketStatusTests(unittest.TestCase):
    def test_missing_day_is_overdue_and_diary_unknown(self):
        result = job.evaluate(observation())
        self.assertEqual(result["state"], "OVERDUE")
        self.assertEqual(result["first_blocked_stage"], "diary_run")
        self.assertEqual(result["stage_states"]["diary_run"], "missing")
        self.assertEqual(result["accountable_owner"], "codex_daily_review")
        self.assertFalse(result["authorizes_execution"])

    def test_prepared_unadmitted_packet_names_reviewer(self):
        data = passed_through(observation(), "candidate")
        data["capture_attempt"] = "not_started"
        result = job.evaluate(data)
        self.assertEqual(result["first_blocked_stage"], "admission_review")
        self.assertEqual(result["action_owner"], "codex_daily_review")
        self.assertEqual(result["next_action"], "reconcile_admission_review_receipt")

    def test_before_due_is_pending_at_due_is_overdue(self):
        data = observation()
        data["observed_at"] = "2026-11-06T05:59:59Z"
        self.assertEqual(job.evaluate(data)["state"], "PENDING")
        data["observed_at"] = data["due_at"]
        self.assertEqual(job.evaluate(data)["state"], "OVERDUE")

    def test_utc_day_is_independent_of_session_dst_or_local_offset(self):
        for day in ("2026-11-01", "2026-11-06", "2026-11-07"):
            data = observation()
            data["publication_day"] = day
            data["observed_at"] = "2026-11-08T01:00:00-05:00"
            data["due_at"] = "2026-11-08T06:00:00Z"
            result = job.evaluate(data)
            self.assertEqual(result["window_start"], day + "T00:00:00+00:00")
            self.assertEqual(result["observed_at"], result["due_at"])
            self.assertEqual(result["state"], "OVERDUE")

    def test_cli_entry_runs_without_site_packages(self):
        process = subprocess.run(
            [sys.executable, "-I", "-S", "-B", str(ROOT / "scripts/daily_publication_packet_status.py"),
             str(ROOT / "no-such-packet-observation.json"), "--day", "2026-11-05"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        self.assertEqual(process.returncode, 2)
        self.assertEqual(json.loads(process.stdout)["state"], "INVALID_OBSERVATION")
        self.assertEqual(process.stderr, "")
        self.assertNotIn(str(ROOT), process.stdout)

    def test_failed_and_blocked_are_visible_before_due(self):
        for state in ("failed", "blocked", "unknown"):
            data = observation()
            data["observed_at"] = "2026-11-06T05:00:00Z"
            data["stages"]["diary_run"] = {"state": state}
            self.assertEqual(job.evaluate(data)["state"], "BLOCKED")

    def test_capture_retry_never_suggested_for_spent_or_uncertain(self):
        for attempt in ("started", "spent", "unknown"):
            data = passed_through(observation(), "admission_review")
            data["capture_attempt"] = attempt
            result = job.evaluate(data)
            self.assertEqual(result["next_action"], "reconcile_existing_capture_attempt")
            self.assertEqual(result["action_owner"], "claude_capture_producer")
            self.assertFalse(result["authorizes_execution"])

    def test_unstarted_capture_still_requires_existing_gate(self):
        data = passed_through(observation(), "admission_review")
        data["capture_attempt"] = "not_started"
        result = job.evaluate(data)
        self.assertEqual(result["next_action"], "producer_check_existing_a10_gate")
        self.assertFalse(result["authorizes_execution"])

    def test_complete_is_reported_only_not_acceptance(self):
        data = passed_through(observation(), "gemini_review")
        data["capture_attempt"] = "spent"
        before = copy.deepcopy(data)
        result = job.evaluate(data)
        self.assertEqual(result["state"], "COMPLETE_REPORTED")
        self.assertIsNone(result["first_blocked_stage"])
        self.assertFalse(result["authorizes_execution"])
        self.assertEqual(data, before)
        self.assertEqual(result, job.evaluate(data))

    def test_every_stage_can_block_even_with_later_success_claims(self):
        for stage in job.STAGES:
            data = passed_through(observation(), "gemini_review")
            data["stages"][stage] = {"state": "failed"}
            result = job.evaluate(data)
            self.assertEqual(result["first_blocked_stage"], stage)
            self.assertNotEqual(result["state"], "COMPLETE_REPORTED")

    def test_receipt_day_hash_and_chain_must_match(self):
        for field, value in (("publication_day", "2026-11-04"),
                             ("receipt_sha256", ""),
                             ("upstream_receipt_sha256", "f" * 64)):
            data = passed_through(observation(), "gemini_review")
            data["stages"]["capture"][field] = value
            with self.assertRaises(ValueError):
                job.evaluate(data)

    def test_missing_hash_or_previous_stage_cannot_be_skipped(self):
        data = passed_through(observation(), "capture")
        del data["stages"]["capture"]["receipt_sha256"]
        with self.assertRaises(ValueError):
            job.evaluate(data)
        data = passed_through(observation(), "capture")
        del data["stages"]["candidate"]
        self.assertEqual(job.evaluate(data)["first_blocked_stage"], "candidate")

    def test_closed_schema_rejects_payload_and_invalid_types(self):
        mutations = [
            ("schema", "wrong"), ("publication_day", "20261105"),
            ("due_at", "2026-11-05T23:59:59Z"),
            ("observed_at", "2026-11-06T07:00:00"),
            ("capture_attempt", True), ("stages", []),
            ("raw_prompt", "private body"), ("publication_day", None),
            ("observed_at", None),
        ]
        for key, value in mutations:
            data = observation()
            data[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                job.evaluate(data)
        data = observation()
        data["stages"]["candidate"] = {"state": "missing", "receipt_sha256": "a" * 64}
        with self.assertRaises(ValueError):
            job.evaluate(data)
        for value in ({"state": "success"}, {"state": "missing", "raw_text": "secret"}):
            data = observation()
            data["stages"]["candidate"] = value
            with self.assertRaises(ValueError):
                job.evaluate(data)

    def test_cli_nonzero_for_missing_invalid_or_overdue_input(self):
        from contextlib import redirect_stdout
        from io import StringIO

        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            path = Path(directory) / "observation.json"
            cases = [(None, 2), ("{broken", 2),
                     (json.dumps(observation()), 1),
                     (json.dumps(passed_through(observation(), "gemini_review")), 0)]
            for content, expected in cases:
                if content is not None:
                    path.write_text(content, encoding="utf-8")
                output = StringIO()
                with redirect_stdout(output):
                    code = job.main([str(path), "--day", "2026-11-05"])
                self.assertEqual(code, expected)
                result = json.loads(output.getvalue())
                self.assertFalse(result["authorizes_execution"])
                self.assertNotIn(str(path), output.getvalue())

    def test_cli_rejects_wrong_day_duplicate_keys_and_oversized_file(self):
        from contextlib import redirect_stdout
        from io import StringIO

        with tempfile.TemporaryDirectory(dir=ROOT) as directory:
            path = Path(directory) / "input.json"
            cases = [(json.dumps(observation()), "2026-11-04"),
                     ('{"schema":1,"schema":2}', "2026-11-05"),
                     (" " * 65537, "2026-11-05")]
            for body, day in cases:
                path.write_text(body, encoding="utf-8")
                with redirect_stdout(StringIO()):
                    self.assertEqual(job.main([str(path), "--day", day]), 2)

    def test_capture_success_cannot_hide_unknown_attempt_state(self):
        for state in ("not_started", "started", "unknown"):
            data = passed_through(observation(), "gemini_review")
            data["capture_attempt"] = state
            with self.assertRaises(ValueError):
                job.evaluate(data)


if __name__ == "__main__":
    unittest.main()
