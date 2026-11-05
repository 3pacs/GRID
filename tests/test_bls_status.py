"""Offline BLS request outcomes, committed-count contract and caller status.

HTTP uses real requests.Response objects; every database operation is a mock.
The tiny fixtures do not certify the existing unbounded observation writer.
"""

from __future__ import annotations

import ast
import json
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import requests

from ingestion.bls import BLSPuller
from ingestion.smart_scheduler import SmartScheduler, _classify_outcome


def response(payload: Any, status: int = 200) -> requests.Response:
    out = requests.Response()
    out.status_code = status
    out.url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    out._content = json.dumps(payload).encode()
    out.headers["Content-Type"] = "application/json"
    return out


def success(observations: list[dict] | None = None) -> requests.Response:
    series = [] if observations is None else [{"seriesID": "CES0000000001", "data": observations}]
    return response({"status": "REQUEST_SUCCEEDED", "responseTime": 12, "message": [],
                     "Results": {"series": series}})


OBS = {"year": "2024", "period": "M01", "periodName": "January", "value": "1,234.5",
       "footnotes": [{"code": "P", "text": "Preliminary"}]}


class MockEngine:
    """Record simulated DATA attempts across each whole mock transaction."""

    def __init__(self, existing=(), fail_insert=False):
        self.existing = set(existing)
        self.fail_insert = fail_insert
        self.transactions = []
        self.active = None
        self.statements = []

    @contextmanager
    def connect(self):
        yield self

    @contextmanager
    def begin(self):
        assert self.active is None, "nested mock transaction"
        transaction = {"data_attempts": 0, "committed": False}
        self.transactions.append(transaction)
        self.active = transaction
        try:
            yield self
            transaction["committed"] = True
        finally:
            self.active = None

    def execute(self, statement, params=None):
        sql = str(statement)
        assert "SAVEPOINT" not in sql.upper()
        params = dict(params or {})
        self.statements.append((sql, params))
        if sql.startswith(("INSERT", "UPDATE", "DELETE")):
            assert self.active is not None
            # Assert before the simulated write; never exercise >50.
            assert self.active["data_attempts"] < 50
            self.active["data_attempts"] += 1
            if self.fail_insert and "INSERT INTO raw_series" in sql:
                raise RuntimeError("fixture insert refusal")
        return SimpleNamespace(fetchone=lambda: (7,),
                               fetchall=lambda: [(d,) for d in self.existing])


class BLSStatusTests(unittest.TestCase):
    def run_pull(self, outcomes, *, engine=None, max_queries=None, query_count=0, **kwargs):
        engine = engine or MockEngine()
        puller = BLSPuller(engine)
        if max_queries is not None:
            puller._max_queries = max_queries
        puller.query_count = query_count
        with patch("ingestion.bls.requests.post", side_effect=outcomes) as post:
            result = puller.pull_series(start_year=kwargs.pop("start_year", 2024),
                                        end_year=kwargs.pop("end_year", 2024), **kwargs)
        self.assertEqual(set(result), {"series_count", "rows_inserted", "status", "errors"})
        self.assertTrue(all(t["data_attempts"] <= 50 for t in engine.transactions))
        return result, puller, engine, post

    def assert_failed(self, outcome):
        result, puller, engine, post = self.run_pull([outcome])
        self.assertEqual((result["status"], result["rows_inserted"]), ("FAILED", 0))
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(puller.query_count, 1)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(engine.transactions, [])

    def test_timeout(self):
        self.assert_failed(requests.Timeout("mock read timeout"))

    def test_connection_failure(self):
        self.assert_failed(requests.ConnectionError("mock connection refused"))

    def test_http_failure(self):
        self.assert_failed(response({"message": "Unavailable"}, 503))

    def test_api_failure(self):
        self.assert_failed(response({"status": "REQUEST_FAILED", "message": ["Invalid series"]}))

    def test_missing_status_is_failure(self):
        self.assert_failed(response({"message": ["No status supplied"]}))

    def test_invalid_json(self):
        out = success()
        out._content = b"<html>upstream gateway</html>"
        self.assert_failed(out)

    def test_all_requests_failed_across_year_windows(self):
        result, puller, _, post = self.run_pull(
            [requests.Timeout("window timeout"), response({"status": "REQUEST_FAILED", "message": ["API refusal"]})],
            start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("FAILED", 0))
        self.assertEqual(len(result["errors"]), 2)
        self.assertEqual(puller.query_count, 2)
        self.assertEqual([c.kwargs["json"]["startyear"] for c in post.call_args_list], ["2000", "2020"])

    def test_mixed_stored_success_then_failure(self):
        result, _, engine, _ = self.run_pull([success([OBS]), requests.Timeout("second window")],
                                            start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 1))
        self.assertEqual(engine.transactions, [{"data_attempts": 1, "committed": True}])
        row = next(p for s, p in engine.statements if s.startswith("INSERT INTO raw_series"))
        self.assertEqual((row["val"], row["od"]), (1234.5, date(2024, 1, 1)))
        self.assertEqual(json.loads(row["payload"]), OBS)

    def test_mixed_failure_then_stored_success(self):
        result, _, _, _ = self.run_pull([requests.Timeout("first window"), success([OBS])],
                                        start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 1))

    def test_empty_success(self):
        result, _, engine, _ = self.run_pull([success()])
        self.assertEqual((result["status"], result["rows_inserted"], result["errors"]), ("SUCCESS", 0, []))
        self.assertEqual(engine.transactions[0]["data_attempts"], 0)

    def test_duplicate_only_success(self):
        result, _, _, _ = self.run_pull([success([OBS])], engine=MockEngine([date(2024, 1, 1)]))
        self.assertEqual((result["status"], result["rows_inserted"]), ("SUCCESS", 0))

    def test_empty_success_then_failure(self):
        result, _, _, _ = self.run_pull([success(), requests.Timeout("second window")],
                                        start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 0))

    def test_failure_then_empty_success(self):
        result, _, _, _ = self.run_pull([requests.Timeout("first window"), success()],
                                        start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 0))

    def test_duplicate_success_then_failure(self):
        result, _, _, _ = self.run_pull([success([OBS]), requests.Timeout("second window")],
                                        engine=MockEngine([date(2024, 1, 1)]), start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 0))

    def test_budget_reached_without_attempt(self):
        result, _, _, post = self.run_pull([], max_queries=1, query_count=1)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 0))
        post.assert_not_called()

    def test_budget_after_only_failure(self):
        result, _, _, post = self.run_pull([requests.Timeout("only attempt")], max_queries=1,
                                          start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("FAILED", 0))
        self.assertEqual(len(result["errors"]), 2)
        self.assertEqual(post.call_count, 1)

    def test_budget_after_empty_success(self):
        result, _, _, _ = self.run_pull([success()], max_queries=1, start_year=2000, end_year=2021)
        self.assertEqual((result["status"], result["rows_inserted"]), ("PARTIAL", 0))

    def test_no_series_no_attempt(self):
        result, _, _, post = self.run_pull([], series_ids=[])
        self.assertEqual((result["status"], result["rows_inserted"]), ("SUCCESS", 0))
        post.assert_not_called()

    def test_reversed_years_no_attempt(self):
        result, _, _, post = self.run_pull([], start_year=2025, end_year=2024)
        self.assertEqual((result["status"], result["rows_inserted"]), ("SUCCESS", 0))
        post.assert_not_called()

    def test_backfill_and_series_chunk_policy_unchanged(self):
        result, puller, _, post = self.run_pull([success() for _ in range(6)],
                                              series_ids=[f"MOCK{i}" for i in range(51)],
                                              start_year=1980, end_year=2025)
        self.assertEqual((result["status"], result["rows_inserted"], puller.query_count), ("SUCCESS", 0, 6))
        self.assertEqual([len(c.kwargs["json"]["seriesid"]) for c in post.call_args_list], [50, 50, 50, 1, 1, 1])
        self.assertEqual([(c.kwargs["json"]["startyear"], c.kwargs["json"]["endyear"])
                          for c in post.call_args_list[:3]], [("1980", "1999"), ("2000", "2019"), ("2020", "2025")])
        self.assertTrue(all(c.kwargs["timeout"] == 60 for c in post.call_args_list))

    def test_pull_all_propagates_failure(self):
        puller = BLSPuller(MockEngine())
        with patch("ingestion.bls.requests.post", side_effect=requests.Timeout("scheduled mock")) as post:
            result = puller.pull_all()
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(post.call_args.kwargs["json"]["startyear"], str(date.today().year - 2))

    def test_database_failure_still_propagates_and_rolls_back(self):
        engine = MockEngine(fail_insert=True)
        puller = BLSPuller(engine)
        with patch("ingestion.bls.requests.post", return_value=success([OBS])):
            with self.assertRaisesRegex(RuntimeError, "fixture insert refusal"):
                puller.pull_series(start_year=2024, end_year=2024)
        self.assertEqual(engine.transactions, [{"data_attempts": 1, "committed": False}])

    def test_caller_classification_and_pull_log(self):
        scenarios = [([requests.Timeout("caller timeout")], "FAILED", "FAILED", 0),
                     ([success([OBS]), requests.Timeout("caller mixed")], "PARTIAL", "PARTIAL", 1),
                     ([success(), requests.Timeout("caller empty mixed")], "PARTIAL", "PARTIAL", 0),
                     ([success()], "NO_NEW_DATA", "SUCCESS", 0),
                     ([success([OBS])], "SUCCESS", "SUCCESS", 1)]
        for outcomes, expected_outcome, expected_log, count in scenarios:
            with self.subTest(expected_outcome=expected_outcome, count=count):
                result, _, _, _ = self.run_pull(outcomes, start_year=2000 if len(outcomes) == 2 else 2024,
                                              end_year=2021 if len(outcomes) == 2 else 2024)
                outcome, rows, note = _classify_outcome(result)
                self.assertEqual((outcome, rows), (expected_outcome, count))
                scheduler = object.__new__(SmartScheduler)
                scheduler.engine = MockEngine()
                scheduler._catalog_ids = {"bls": 7}
                scheduler._log_run("bls", datetime.now(timezone.utc),
                                   {"status": outcome, "rows_inserted": rows, "error": note, "reason": note})
                inserts = [p for s, p in scheduler.engine.statements if s.startswith("INSERT INTO pull_log")]
                self.assertEqual(len(inserts), 1)
                self.assertEqual((inserts[0]["status"], inserts[0]["rows"]), (expected_log, count))
                self.assertEqual(scheduler.engine.transactions[0]["data_attempts"], 1)
                self.assertFalse(any("UPDATE source_catalog" in s for s, _ in scheduler.engine.statements))

    def test_hermes_recovery_rejects_nonfresh_outcomes(self):
        # Compile only the actual pure recovery predicate and its constant;
        # importing the full operator would load production config/runtime.
        source = Path(__file__).resolve().parents[1] / "scripts/hermes_fixers.py"
        parsed = ast.parse(source.read_text())
        selected = [n for n in parsed.body if
                    (isinstance(n, ast.FunctionDef) and n.name == "retry_not_fresh_reason") or
                    (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "_NOT_FRESH_OUTCOMES"
                                                      for t in n.targets))]
        self.assertEqual(len(selected), 2)
        namespace = {"Any": Any}
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"), namespace)
        predicate = namespace["retry_not_fresh_reason"]
        for outcome in ("FAILED", "PARTIAL", "NO_NEW_DATA", "SKIPPED"):
            self.assertEqual(predicate({"outcome": outcome}), f"puller reported {outcome}")
        self.assertIsNone(predicate({"outcome": "SUCCESS"}))


if __name__ == "__main__":
    unittest.main(failfast=True)
