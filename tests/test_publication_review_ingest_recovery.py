"""Offline receipt recovery tests: fake packet, engine and ingest; no DB or model."""
import contextlib
import importlib.util
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory

_spec = importlib.util.spec_from_file_location(
    "grader_receipt_recovery", Path(__file__).resolve().parents[1] / "scripts/grade_publication_review.py")
grader = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = grader
_spec.loader.exec_module(grader)
_real_run_ingest = grader.run_ingest


class Engine:
    @contextlib.contextmanager
    def connect(self):
        yield object()


def make_run(tmp_path, stack):
    day = date(2026, 11, 7)
    stated = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)
    now = datetime(2026, 11, 9, 12, tzinfo=timezone.utc)
    text = "AI narrative withheld: unsupported source interpretation. Data summary only."
    pub = grader.Publication("market_briefings:1", "market_briefings", stated, text,
                             grader.text_sha(text), {})
    packet = grader.Packet("a" * 64, [pub])
    stack.enter_context(patch.object(grader, "load_packet", lambda *args: packet))
    calls = []
    outcomes = []

    def ingest(*args):
        calls.append(args)
        if outcomes:
            return outcomes.pop(0)
        return {"state": "INGESTED", "run_id": len(calls), "rejected_rows": 0, "declared_mismatch": [],
                "review_sha256": grader.sha256((tmp_path / str(day) / "review.jsonl").read_bytes()),
                "packet_sha256": packet.sha256}

    stack.enter_context(patch.object(grader, "run_ingest", ingest))

    def invoke(**kwargs):
        return grader.grade_day(Engine(), day, review_root=tmp_path, now=now, **kwargs)

    return invoke, calls, outcomes, packet, tmp_path / str(day) / "grader-receipt.json"


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        folder = self.stack.enter_context(TemporaryDirectory())
        self.invoke_fixture = make_run(Path(folder), self.stack)

    def test_two_no_ingest_runs(self):
        self.check_no_ingest_never_certifies_uningested_bytes(False)

    def test_failed_ingest_recovery(self):
        self.check_no_ingest_never_certifies_uningested_bytes(True)

    def check_no_ingest_never_certifies_uningested_bytes(self, initial_failure):
        run = self.invoke_fixture
        invoke, calls, outcomes, _, receipt_path = run
        if initial_failure:
            outcomes.append({"state": "FAILED"})
            assert invoke()["ingest"]["state"] == "FAILED"
        for _ in range(2):
            result = invoke(ingest=False)
            assert "ingested_review_sha256" not in result
            assert "ingested_review_sha256" not in json.loads(receipt_path.read_text())
        result = invoke()
        assert result["ingest"]["state"] == "INGESTED"
        assert len(calls) == 1 + int(initial_failure)


    def test_no_ingest_preserves_genuine_confirmation(self):
        run = self.invoke_fixture
        invoke, calls, _, _, _ = run
        first = invoke()
        for _ in range(2):
            assert invoke(ingest=False)["ingested_review_sha256"] == first["review_sha256"]
        assert invoke()["state"] == "UNCHANGED"
        assert len(calls) == 1


    def test_confirmation_for_old_packet_does_not_cover_rewrite(self):
        run = self.invoke_fixture
        invoke, calls, _, packet, _ = run
        first = invoke()
        packet.sha256 = "b" * 64
        changed = invoke(ingest=False)
        assert changed["state"] == "WRITTEN" and changed["review_sha256"] != first["review_sha256"]
        assert "ingested_review_sha256" not in invoke(ingest=False)
        assert invoke()["ingest"]["state"] == "INGESTED"
        assert len(calls) == 2


    def test_missing_previous_receipt_requires_ingest(self):
        run = self.invoke_fixture
        invoke, calls, _, _, receipt_path = run
        invoke(ingest=False)
        receipt_path.unlink()
        assert "ingested_review_sha256" not in invoke(ingest=False)
        assert invoke()["ingest"]["state"] == "INGESTED"
        assert len(calls) == 1

    def test_legacy_fabricated_confirmation_does_not_skip_ingest(self):
        invoke, calls, _, _, receipt_path = self.invoke_fixture
        first = invoke(ingest=False)
        forged = dict(first, ingested_review_sha256=first["review_sha256"])
        receipt_path.write_text(json.dumps(forged))
        result = invoke()
        assert len(calls) == 1 and result["ingest"]["state"] == "INGESTED"
        assert result["ingest_confirmation"]["review_sha256"] == result["review_sha256"]

    def test_bad_ingest_binding_is_visible_and_retried(self):
        invoke, calls, outcomes, packet, _ = self.invoke_fixture
        first = invoke(ingest=False)
        good = {"state": "INGESTED", "run_id": 1, "rejected_rows": 0, "declared_mismatch": [],
                "review_sha256": first["review_sha256"], "packet_sha256": packet.sha256}
        for delta in ({"review_sha256": "b" * 64}, {"review_sha256": None},
                      {"packet_sha256": "b" * 64}, {"run_id": None}, {"run_id": True},
                      {"rejected_rows": 1}, {"declared_mismatch": ["count mismatch"]}):
            with self.subTest(delta=delta):
                outcomes.append(dict(good, **delta))
                result = invoke()
                assert "ingested_review_sha256" not in result
                assert "ingest_confirmation" not in result
                assert result["ingest_confirmation_error"]
                assert grader.exit_code([result]) == 3
        recovered = invoke()
        assert recovered["ingest_confirmation"]["review_sha256"] == first["review_sha256"]
        assert len(calls) == 8

    def test_file_replacement_at_handoff_does_not_certify_wrong_bytes(self):
        invoke, _, _, packet, receipt_path = self.invoke_fixture
        review_path = receipt_path.with_name("review.jsonl")
        first = invoke(ingest=False)

        def racing_ingest(*args):
            replacement = review_path.read_bytes().replace(b'"graded_at":', b'"raced_at":')
            review_path.write_bytes(replacement)
            return {"state": "INGESTED", "run_id": 42, "rejected_rows": 0, "declared_mismatch": [],
                    "review_sha256": grader.sha256(replacement), "packet_sha256": packet.sha256}

        with patch.object(grader, "run_ingest", racing_ingest):
            result = invoke()
        assert result["review_sha256"] == first["review_sha256"]
        assert result["ingest"]["review_sha256"] != result["review_sha256"]
        assert "ingested_review_sha256" not in result
        assert grader.exit_code([result]) == 3

    def test_present_result_binds_confirmation_and_reuses_it(self):
        invoke, calls, outcomes, packet, _ = self.invoke_fixture
        first = invoke(ingest=False)
        outcomes.append({"state": "PRESENT", "run_id": 7, "rejected_rows": 0, "declared_mismatch": [],
                         "review_sha256": first["review_sha256"], "packet_sha256": packet.sha256})
        result = invoke()
        assert result["ingest_confirmation"]["run_id"] == 7
        assert invoke()["ingested_review_sha256"] == first["review_sha256"]
        assert len(calls) == 1

    def test_ingest_wrapper_preserves_actual_hashes(self):
        class IngestModule:
            @staticmethod
            def ingest(*args, **kwargs):
                return {"state": "PRESENT", "review_sha256": "b" * 64, "packet_sha256": "c" * 64}
        with patch.object(grader, "_ingest_module", lambda: IngestModule):
            result = _real_run_ingest(None, date(2026, 11, 7), Path("unused"), Path("unused"))
        assert result["review_sha256"] == "b" * 64
        assert result["packet_sha256"] == "c" * 64


if __name__ == "__main__":
    unittest.main()
