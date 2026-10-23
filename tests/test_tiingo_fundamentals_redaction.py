"""Offline source projections with fake transport, pandas, logger and DB.

These regressions execute the actual producer/classifier/PullContext definitions
read from repository paths. They do not qualify imports, requests/pandas/
SQLAlchemy behavior, a native database, the full scheduler, or provider access.
Run with: python -m unittest discover -s tests -p test_tiingo_fundamentals_redaction.py
"""
from __future__ import annotations

import ast
import json
import math
import types
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote, quote_plus, urlencode

ROOT = Path(__file__).resolve().parents[1]
DAY = date(2026, 10, 14)
TOKENS = (
    "SYNTHETIC_SENTINEL_7f2c9a",
    "SYNTHETIC_+/percent%?x=1&y=#colon:\"'END",
    "SYNTHETIC_line\nsecond\rline_END",
)


def project(relative_path, namespace, names=None):
    """Compile real definitions; omit imports and nonliteral initialization."""
    path = ROOT / relative_path
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    body = [ast.ImportFrom(module="__future__",
                           names=[ast.alias(name="annotations")], level=0)]
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            if names is None or node.name in names:
                body.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            target_names = {target.id for target in targets if isinstance(target, ast.Name)}
            if names is not None and not target_names.intersection(names):
                continue
            try:
                ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            body.append(node)
    module = ast.fix_missing_locations(ast.Module(body=body, type_ignores=[]))
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


class FakeLog:
    def __init__(self):
        self.messages = []

    def __getattr__(self, level):
        def record(message, **kwargs):
            self.messages.append((level, message.format(**kwargs)))
        return record


class FakeHTTPResponse:
    def __init__(self, status):
        self.status_code = status

    def __bool__(self):
        return False  # requests.Response is false for error status codes.

    @property
    def text(self):
        raise AssertionError("response bodies must not be read for diagnostics")


class UnprintableError(Exception):
    def __str__(self):
        raise AssertionError("raw exception text must not be formatted")


class HTTPError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.response = FakeHTTPResponse(status)


class FakeEngine:
    def __init__(self, rowcount=1, execute_error=None, commit_error=None):
        self.rowcount = rowcount
        self.execute_error = execute_error
        self.commit_error = commit_error
        self.executed = []
        self.attempts = 0
        self.commits = 0
        self.rollbacks = 0

    @contextmanager
    def begin(self):
        self.attempts += 1
        try:
            yield self
            if self.commit_error is not None:
                raise self.commit_error
        except Exception:
            self.rollbacks += 1
            raise
        else:
            self.commits += 1

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, params):
        self.executed.append((str(sql), dict(params)))
        if self.execute_error is not None and "INSERT INTO raw_series" in str(sql):
            raise self.execute_error
        return types.SimpleNamespace(
            rowcount=self.rowcount, fetchone=lambda: (101,), fetchall=lambda: [],
        )


class FakeTransport:
    def __init__(self, data=None, error=None, stage="get"):
        self.data = [] if data is None else data
        self.error = error
        self.stage = stage
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error is not None and self.stage == "get":
            raise self.error
        return self

    def raise_for_status(self):
        if self.error is not None and self.stage == "status":
            raise self.error

    def json(self):
        if self.error is not None and self.stage == "json":
            raise self.error
        return self.data


def environment(log):
    return dict(
        Any=object, date=date, datetime=datetime, timezone=timezone,
        math=math, json=json, log=log, text=lambda sql: sql, Engine=object,
    )


def make_puller(token=TOKENS[0], transport=None, engine=None):
    log = FakeLog()
    transport = transport or FakeTransport()
    namespace = environment(log)
    namespace.update(
        BasePuller=type("BasePuller", (), {}),
        _TIINGO_API_KEY=token,
        requests=types.SimpleNamespace(
            get=transport.get, exceptions=types.SimpleNamespace(HTTPError=HTTPError),
        ),
        pd=types.SimpleNamespace(to_datetime=lambda raw: datetime.fromisoformat(raw)),
        time=types.SimpleNamespace(sleep=lambda delay: None),
    )
    project("ingestion/tiingo_fundamentals_pull.py", namespace)
    cls = namespace["TiingoFundamentalsPuller"]
    puller = cls.__new__(cls)
    puller.engine = engine if engine is not None else FakeEngine()
    puller.source_id = 7
    return puller, namespace, transport, log


def classifier():
    names = {
        "_ROW_COUNT_KEYS", "_RESULT_KEYS", "_result_children",
        "_materialization_accounting", "_extract_rows", "_aggregate_coverage",
        "_classify_outcome", "OUTCOME_SUCCESS", "OUTCOME_NO_NEW_DATA",
        "OUTCOME_SKIPPED", "OUTCOME_FAILED", "OUTCOME_PARTIAL",
    }
    namespace = project("ingestion/smart_scheduler.py", environment(FakeLog()), names)
    return namespace["_classify_outcome"]


def propagate(results, log):
    """Real classifier and persistence boundary, with an inert caller/DB.

    Mirror ingestion/scheduler.py's failed-outcome branch: record rows, then
    raise its actual PullerReportedFailure so PullContext records the failure.
    This deliberately does not invoke a scheduler or construct any puller.
    """
    classified = classifier()(results)
    persistence = FakeEngine()
    namespace = project("ingestion/pull_context.py", environment(log))
    failure_namespace = project(
        "ingestion/scheduler.py", environment(log), {"PullerReportedFailure"},
    )
    failure = failure_namespace["PullerReportedFailure"]
    try:
        with namespace["PullContext"](persistence, "Tiingo_Fundamentals") as ctx:
            ctx.record_rows(classified[1])
            if classified[0] == "FAILED":
                raise failure(classified[2] or "puller reported FAILED")
            if classified[0] == "PARTIAL":
                ctx.set_status("PARTIAL", classified[2])
            if classified[0] in {"SKIPPED", "NO_NEW_DATA"}:
                ctx.set_note(f"{classified[0]}: {classified[2]}")
    except failure:
        pass
    updates = [
        params for sql, params in persistence.executed if "UPDATE pull_log SET" in sql
    ]
    if len(updates) != 1:
        raise AssertionError("expected exactly one fake pull_log update")
    return classified, updates[0], persistence


def secret_forms(token):
    return {
        token, quote(token, safe=""), quote_plus(token), repr(token)[1:-1],
        token[:len(token) // 2],
    }


class TiingoFundamentalsRedactionTests(unittest.TestCase):
    def assert_no_secret(self, token, result, classified, update, persistence, log):
        exported = repr((result, classified, update, persistence.executed, log.messages))
        for form in secret_forms(token):
            self.assertNotIn(form, exported)
            self.assertNotIn(repr(form)[1:-1], exported)
        self.assertNotIn("https://api.tiingo.com", exported)
        self.assertNotIn("Authorization", exported)
        self.assertNotIn("token=", exported)

    def test_header_auth_date_shape_and_literal_empty(self):
        for token in TOKENS:
            with self.subTest(token=repr(token)):
                puller, _, transport, log = make_puller(token)
                result = puller.pull_ticker("AAPL", DAY, DAY)
                self.assertEqual(result, dict(
                    ticker="AAPL", rows_inserted=0, status="NO_NEW_DATA", errors=[],
                ))
                self.assertEqual(transport.calls, [(
                    "https://api.tiingo.com/tiingo/fundamentals/AAPL/daily",
                    dict(params={"startDate": str(DAY), "endDate": str(DAY)},
                         headers={"Authorization": f"Token {token}"}, timeout=30),
                )])
                url, kwargs = transport.calls[0]
                query_url = url + "?" + urlencode(kwargs["params"])
                for form in secret_forms(token):
                    self.assertNotIn(form, query_url)
                self.assertNotIn("token=", query_url)
                self.assertEqual(puller.engine.attempts, 0)
                classified, update, persistence = propagate([result], log)
                self.assertEqual(classified[:2], ("NO_NEW_DATA", 0))
                self.assertEqual((update["status"], update["rows"]), ("SUCCESS", 0))
                self.assert_no_secret(token, result, classified, update, persistence, log)

    def test_http_transport_json_and_nested_failures_do_not_propagate(self):
        for token in TOKENS:
            url = "https://api.tiingo.com/daily?token=" + quote(token, safe="")
            inner = HTTPError("503 unavailable at " + url, 503)
            nested = RuntimeError(f"outer({inner!r}) Authorization: Token {token}")
            nested.__cause__ = inner
            nested.__context__ = inner
            cases = [
                (HTTPError("503 unavailable at " + url, 503), "status",
                 "Tiingo HTTP request failed (status 503)"),
                (HTTPError("truncated token=" + token[:len(token) // 2] + "..."), "status",
                 "Tiingo HTTP request failed"),
                (ConnectionError("nested URL: " + url), "get",
                 "Tiingo fundamentals pull failed"),
                (ValueError(f"bad JSON {token!r}"), "json",
                 "Tiingo fundamentals pull failed"),
                (nested, "get", "Tiingo fundamentals pull failed"),
                (UnprintableError(token), "get", "Tiingo fundamentals pull failed"),
            ]
            for error, stage, diagnostic in cases:
                with self.subTest(token=repr(token), stage=stage, kind=type(error).__name__):
                    transport = FakeTransport(error=error, stage=stage)
                    puller, _, _, log = make_puller(token, transport)
                    result = puller.pull_all(["AAPL"], DAY)
                    self.assertEqual(result[0]["status"], "FAILED")
                    self.assertEqual(result[0]["errors"], [diagnostic])
                    self.assertEqual(result[0]["rows_inserted"], 0)
                    self.assertEqual(puller.engine.attempts, 0)
                    classified, update, persistence = propagate(result, log)
                    self.assertEqual(classified[:2], ("FAILED", 0))
                    self.assertEqual((update["status"], update["rows"]), ("FAILED", 0))
                    self.assert_no_secret(token, result, classified, update, persistence, log)

    def test_paywall_predicate_and_untrusted_status_are_bounded(self):
        cases = [
            (HTTPError("400 Free tier token=" + TOKENS[0], 400),
             "PAYWALLED", "Free tier: DOW 30 only"),
            (HTTPError("401 denied token=" + TOKENS[0], 401),
             "FAILED", "Tiingo HTTP request failed (status 401)"),
            (HTTPError("opaque error token=" + TOKENS[0], TOKENS[0]),
             "FAILED", "Tiingo HTTP request failed"),
            (HTTPError("opaque error", True), "FAILED", "Tiingo HTTP request failed"),
            (HTTPError("opaque error", 999), "FAILED", "Tiingo HTTP request failed"),
        ]
        for error, status, diagnostic in cases:
            with self.subTest(status=status, diagnostic=diagnostic):
                puller, _, _, log = make_puller(
                    transport=FakeTransport(error=error, stage="status"),
                )
                result = puller.pull_ticker("AAPL", DAY, DAY)
                self.assertEqual((result["status"], result["errors"]), (status, [diagnostic]))
                classified, update, persistence = propagate([result], log)
                self.assertEqual((classified[0], update["status"]), ("FAILED", "FAILED"))
                self.assert_no_secret(TOKENS[0], result, classified, update, persistence, log)

    def test_missing_key_performs_no_request(self):
        puller, _, transport, _ = make_puller(token="")
        result = puller.pull_ticker("AAPL", DAY, DAY)
        self.assertEqual(result["status"], "FAILED")
        self.assertEqual(result["errors"], ["No TIINGO_API_KEY"])
        self.assertEqual(result["rows_inserted"], 0)
        self.assertEqual((transport.calls, puller.engine.executed), ([], []))

    def test_committed_values_sql_rowcounts_and_empty_distinction(self):
        data = [{"date": str(DAY), "marketCap": 0, "peRatio": 12.5}]
        for count in (0, 2):
            with self.subTest(count=count):
                engine = FakeEngine(rowcount=count)
                puller, _, _, _ = make_puller(
                    transport=FakeTransport(data=data), engine=engine,
                )
                result = puller.pull_ticker("AAPL", DAY, DAY)
                self.assertEqual((result["status"], result["rows_inserted"], result["errors"]),
                                 ("SUCCESS", 2 * count, []))
                self.assertEqual((engine.commits, engine.rollbacks), (1, 0))
                params = [params for _, params in engine.executed]
                self.assertEqual(params, [
                    dict(sid=7, series="TIINGO_FUND:AAPL:market_cap", obs=DAY, val=0.0),
                    dict(sid=7, series="TIINGO_FUND:AAPL:pe_ratio", obs=DAY, val=12.5),
                ])
                for sql, _ in engine.executed:
                    self.assertIn("WHERE NOT EXISTS", sql)
                    self.assertIn("AND pull_status = 'SUCCESS'", sql)

    def test_validation_and_unknown_transaction_results_remain_failures(self):
        for data, message in [
            ({}, "Tiingo daily response must be a list"),
            ([{}], "Malformed Tiingo daily record"),
            ([{"date": str(DAY), "marketCap": True}], "Invalid Tiingo fundamental value"),
            ([{"date": str(DAY), "marketCap": float("inf")}], "Invalid Tiingo fundamental value"),
        ]:
            puller, _, _, _ = make_puller(transport=FakeTransport(data=data))
            result = puller.pull_ticker("AAPL", DAY, DAY)
            self.assertEqual((result["status"], result["errors"]), ("FAILED", [message]))
            self.assertEqual(puller.engine.commits, 0)
        data = [{"date": str(DAY), "marketCap": 123.5}]
        for count in (None, -1, True, 1.0):
            puller, _, _, _ = make_puller(
                transport=FakeTransport(data=data), engine=FakeEngine(rowcount=count),
            )
            result = puller.pull_ticker("AAPL", DAY, DAY)
            self.assertEqual(result["status"], "FAILED")
            self.assertEqual(result["errors"], ["Unknown affected row count"])
            self.assertIsNone(result["rows_inserted"])
            self.assertEqual((puller.engine.commits, puller.engine.rollbacks), (0, 1))

    def test_db_execute_and_commit_failure_text_is_not_exported(self):
        for token in TOKENS:
            for phase in ("execute_error", "commit_error"):
                with self.subTest(token=repr(token), phase=phase):
                    error = RuntimeError("database nested token=" + quote_plus(token))
                    engine = FakeEngine(**{phase: error})
                    transport = FakeTransport(
                        data=[{"date": str(DAY), "marketCap": 123.5}],
                    )
                    puller, _, _, log = make_puller(token, transport, engine)
                    result = puller.pull_ticker("AAPL", DAY, DAY)
                    self.assertEqual(result["status"], "FAILED")
                    self.assertEqual(result["errors"], ["Tiingo fundamentals pull failed"])
                    self.assertIsNone(result["rows_inserted"])
                    self.assertEqual((engine.commits, engine.rollbacks), (0, 1))
                    classified, update, persistence = propagate([result], log)
                    self.assertEqual(classified[0], "FAILED")
                    self.assertIsNone(classified[1])
                    self.assertEqual(update["status"], "FAILED")
                    self.assertIsNone(update["rows"])
                    self.assert_no_secret(token, result, classified, update, persistence, log)


if __name__ == "__main__":
    unittest.main()
