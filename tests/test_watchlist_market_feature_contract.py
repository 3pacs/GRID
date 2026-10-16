"""Actual pure API helper and cached endpoint prefix; no backend imports or SQL."""

import ast
from copy import deepcopy
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "api/routers/watchlist_analysis.py"


def load_helper():
    tree = ast.parse(SOURCE.read_text())
    function = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "_attach_market_evidence"
    )
    namespace = {"Any": object}
    exec(
        compile(ast.Module(body=[function], type_ignores=[]), str(SOURCE), "exec"),
        namespace,
    )
    return namespace["_attach_market_evidence"]


class WatchlistMarketFeatureTests(unittest.TestCase):
    def setUp(self):
        self.attach = load_helper()
        self.original = {
            "ticker": "SPY",
            "price_source": "grid",
            "availability": {"price": "available"},
            "price_history": [
                {"date": "2026-01-05", "value": 0.0},
                {"date": "2026-01-06", "value": 104.125},
            ],
            "related_features": [{"name": "spy_close", "value": None}],
        }

    def test_actual_existing_values_dates_source_and_unknown_freshness(self):
        out = self.attach(self.original)
        daily = out["market_features"]["daily"]
        self.assertEqual(
            daily["rows"],
            [
                {"as_of": "2026-01-05", "value": 0.0},
                {"as_of": "2026-01-06", "value": 104.125},
            ],
        )
        self.assertIs(type(daily["rows"][0]["value"]), float)
        self.assertEqual(daily["source"], "grid")
        self.assertEqual(daily["as_of"], "2026-01-06")
        self.assertEqual(daily["status"], "observed")
        self.assertEqual(
            daily["freshness"],
            {"status": "unknown", "reason": "freshness_policy_not_supplied"},
        )
        self.assertEqual(out["related_features"], self.original["related_features"])

    def test_no_qualified_intraday_and_null_is_not_zero(self):
        original = {**self.original, "price_history": [{"date": None, "value": None}]}
        out = self.attach(original)["market_features"]
        self.assertIsNone(out["daily"]["rows"][0]["value"])
        self.assertIsNone(out["daily"]["as_of"])
        self.assertEqual(
            out["intraday"],
            {
                "status": "unavailable",
                "reason": "qualified_intraday_result_absent",
                "opening_range": None,
                "trade_vwap": None,
                "hlc3_volume_proxy": None,
            },
        )
        self.assertNotIn("eligible", out)
        self.assertNotIn("current", out["daily"])

    def test_declared_negative_states_retained_without_freshness_invention(self):
        for state in ("unavailable", "failed", "stale", "unknown"):
            with self.subTest(state=state):
                daily = self.attach(
                    {**self.original, "availability": {"price": state}}
                )["market_features"]["daily"]
                self.assertEqual(daily["status"], state)
                self.assertEqual(daily["reason"], "price_section_" + state)
                self.assertEqual(
                    daily["freshness"]["status"],
                    "stale" if state == "stale" else "unknown",
                )
                self.assertEqual(daily["rows"][1]["value"], 104.125)

    def test_missing_source_rows_and_partial_preload_cache(self):
        for payload in (
            {"ticker": "QQQ"},
            {"ticker": "QQQ", "price_history": [], "price_source": "grid"},
            {"ticker": "QQQ", "price_history": None},
        ):
            with self.subTest(payload_keys=tuple(payload)):
                daily = self.attach(payload)["market_features"]["daily"]
                self.assertEqual(daily["status"], "unknown")
                self.assertEqual(daily["returned_row_count"], 0)
                self.assertIsNone(daily["as_of"])
        # Existing preload cache omits section availability; present rows are evidence,
        # not policy-derived freshness.
        daily = self.attach(
            {
                "ticker": "QQQ",
                "price_history": [{"date": "2020-01-01", "value": 1}],
                "price_source": "grid",
                "_cached": True,
            }
        )["market_features"]["daily"]
        self.assertEqual(daily["as_of"], "2020-01-01")
        self.assertTrue(daily["served_from_cache"])
        self.assertEqual(daily["freshness"]["status"], "unknown")

    def test_existing_live_scalar_metadata_no_date_fabrication(self):
        for stamp in (None, "2026-01-05"):
            with self.subTest(stamp=stamp):
                daily = self.attach(
                    {
                        "ticker": "SPY",
                        "price_history": [],
                        "price_source": "live",
                        "live_price": {"price": 101.25, "as_of": stamp},
                    }
                )["market_features"]["daily"]
                self.assertEqual(daily["rows"], [{"as_of": stamp, "value": 101.25}])
                self.assertEqual(daily["source"], "live")
                self.assertEqual(daily["freshness"]["status"], "unknown")

    def test_existing_briefing_reason_only_and_no_payload_mutation(self):
        original = deepcopy(self.original)
        original["briefing"] = {
            "status": "withheld",
            "reason": "unsupported_money_transfer",
            "as_of": "2026-01-06",
        }
        before = deepcopy(original)
        out = self.attach(original)
        self.assertEqual(
            out["market_features"]["briefing_receipt"], original["briefing"]
        )
        out["market_features"]["daily"]["rows"][0]["value"] = 999
        self.assertEqual(original, before)
        self.assertIsNone(
            self.attach(self.original)["market_features"]["briefing_receipt"]
        )

    def test_actual_cached_endpoint_prefix_keeps_original_asof_without_backend(self):
        tree = ast.parse(SOURCE.read_text())
        fn = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "get_ticker_analysis"
        )
        # Select the unchanged normal cache statements after the new stored-only branch.
        cached = deepcopy(self.original)
        fn = ast.FunctionDef(
            name="cached_prefix",
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg="ticker"), ast.arg(arg="period")],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=[fn.body[1], *fn.body[3:5]],
            decorator_list=[],
        )

        def forbidden(*args):
            self.fail("cached path reached new backend or provider work")

        namespace = {
            "_get_analysis_cached": lambda *args: cached,
            "_attach_market_evidence": self.attach,
            "get_db_engine": forbidden,
        }
        code = ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[]))
        exec(compile(code, str(SOURCE), "exec"), namespace)
        out = namespace["cached_prefix"]("SPY", "3M")
        self.assertTrue(out["_cached"])
        self.assertEqual(out["market_features"]["daily"]["as_of"], "2026-01-06")
        self.assertEqual(cached, self.original)

    def test_endpoint_sql_provider_and_cache_logic_unchanged(self):
        source = ast.parse(SOURCE.read_text())
        fn = next(
            n
            for n in source.body
            if isinstance(n, ast.FunctionDef) and n.name == "get_ticker_analysis"
        )
        self.assertIsInstance(fn.body[-1], ast.Return)
        self.assertEqual(ast.unparse(fn.body[-1].value.func), "_attach_market_evidence")
        # The transport decoration must not add another query/provider/call loop.
        calls = [ast.unparse(n.func) for n in ast.walk(fn) if isinstance(n, ast.Call)]
        self.assertEqual(calls.count("_attach_market_evidence"), 2)
        self.assertNotIn("project_price_feature", calls)
        self.assertNotIn("trade_vwap", calls)



class StoredSourceEndpointTests(unittest.TestCase):
    """Actual endpoint/helper AST with inert store and rejecting legacy paths."""

    def setUp(self):
        import json
        from datetime import date, datetime, timezone
        self.json = json
        self.date = date
        self.datetime = datetime
        self.timezone = timezone
        self.calls = []
        self.selection = {
            "source_key": "baltic_exchange", "scope": "index", "ticker_or_cusip": "BDI",
            "series_id": "baltic.bdi", "expected_source_name": "Baltic_Exchange",
            "feature_name": "baltic_dry_index", "feature_id": 23,
        }
        self.request = {
            "selections": [self.selection], "as_of": "2026-10-15T12:00:00+00:00",
            "start_date": "2026-10-01", "vintage_policy": "LATEST_AS_OF",
        }

    def validate(self, selections, *, as_of, start_date, vintage_policy):
        if as_of.tzinfo is None or vintage_policy not in ("LATEST_AS_OF", "FIRST_RELEASE"):
            raise ValueError("invalid")
        return {"selections": selections or [], "as_of": as_of.astimezone(self.timezone.utc),
                "start_date": start_date, "vintage_policy": vintage_policy}

    def snapshot(self, **request):
        self.calls.append(request)
        return {
            "schema": "stored-source-snapshot-v1", "as_of": request["as_of"].isoformat(),
            "start_date": request["start_date"].isoformat(), "vintage_policy": request["vintage_policy"],
            "selections": [{**item, "status": "available", "reason": "resolved_observation",
                "value": 0 if item.get("feature_id") is not None else None, "actual_source": {"id": 7, "name": item.get("expected_source_name")},
                "obs_date": "2026-10-14", "release_date": "2026-10-15", "vintage_date": "2026-10-15",
                "freshness": {"status": "unknown", "reason": "freshness_policy_absent"},
                "storage_clock": "pull_date_not_provider_publication", "lineage": "resolved_source_not_raw_row",
                "conflict_flag": False, "conflict_detail": None} for item in request["selections"]],
        }

    def endpoint(self, snapshot=None, validate=None, cached=None):
        import json
        from datetime import date, datetime
        from types import SimpleNamespace
        class HTTPError(Exception):
            def __init__(self, status_code, detail):
                self.status_code = status_code
                self.detail = detail
        def forbidden(*args, **kwargs):
            self.fail("stored-only endpoint reached cache/legacy/backend/provider")
        def real_store():
            self.store_gets += 1
            return SimpleNamespace(get_source_snapshot=snapshot or self.snapshot)
        self.store_gets = 0
        self.HTTPError = HTTPError
        tree = ast.parse(SOURCE.read_text())
        names = {"_attach_market_evidence", "_stored_source_analysis", "get_ticker_analysis"}
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        for function in functions:
            function.decorator_list = []
        namespace = {
            "Any": object, "date": date, "datetime": datetime, "json": json,
            "Depends": lambda *args: None, "require_auth": object(),
            "Query": lambda default=None, **kwargs: default, "HTTPException": HTTPError,
            "validate_source_snapshot_request": validate or self.validate,
            "PITStore": lambda engine: SimpleNamespace(get_source_snapshot=snapshot or self.snapshot),
            "get_pit_store": real_store, "get_db_engine": forbidden,
            "_get_analysis_cached": (lambda *args: cached) if cached is not None else forbidden,
            "_fetch_live_price": forbidden,
        }
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), "exec"), namespace)
        return namespace["get_ticker_analysis"]

    def test_actual_endpoint_serializes_zero_and_original_storage_clocks(self):
        endpoint = self.endpoint()
        output = endpoint(" spy ", "1M", stored_only=True, stored_request=self.json.dumps(self.request))
        copied = self.json.loads(self.json.dumps(output, allow_nan=False))
        stored = copied["market_features"]["stored_source_evidence"]
        self.assertEqual(stored["ticker"], "SPY")
        self.assertEqual(stored["request_binding"]["as_of"], self.request["as_of"])
        self.assertEqual(stored["selections"][0]["value"], 0)
        self.assertEqual(stored["selections"][0]["obs_date"], "2026-10-14")
        self.assertEqual(stored["selections"][0]["freshness"]["status"], "unknown")
        self.assertEqual(copied["market_features"]["daily"]["status"], "unknown")
        self.assertIsNone(copied["market_features"]["intraday"]["trade_vwap"])
        self.assertEqual(self.store_gets, 1)

    def test_missing_request_has_no_store_or_cutoff_invention(self):
        output = self.endpoint()("SPY", stored_only=True)
        snapshot = output["market_features"]["stored_source_evidence"]
        self.assertEqual(snapshot["reason"], "stored_request_missing")
        self.assertIsNone(snapshot["as_of"])
        self.assertEqual(self.store_gets, 0)
        self.assertEqual(self.calls, [])

    def test_missing_bindings_use_no_engine_store(self):
        request = {**self.request, "selections": [{"source_key": "sec_ftd"}]}
        output = self.endpoint()("SPY", stored_only=True, stored_request=self.json.dumps(request))
        self.assertEqual(self.store_gets, 0)
        self.assertIsNone(output["market_features"]["stored_source_evidence"]["selections"][0]["value"])

    def test_invalid_json_types_duplicates_and_utf8_size_reject_before_store(self):
        for request in ('{', 'null', '[]', '{"as_of":"a","as_of":"b"}',
                        self.json.dumps({**self.request, "unexpected": True}),
                        self.json.dumps({**self.request, "as_of": None}),
                        self.json.dumps({**self.request, "as_of": "2026-10-15T12:00:00"}),
                        '{"unknown":"' + 'é' * 9000 + '"}'):
            with self.subTest(request_type=type(request).__name__, length=len(request)):
                endpoint = self.endpoint()
                with self.assertRaises(self.HTTPError) as error:
                    endpoint("SPY", stored_only=True, stored_request=request)
                self.assertEqual(error.exception.status_code, 400)
                self.assertEqual(self.store_gets, 0)

    def test_validator_failure_and_cross_ticker_refuse_before_store(self):
        def invalid(*args, **kwargs):
            raise ValueError("closed selector validation")
        endpoint = self.endpoint(validate=invalid)
        with self.assertRaises(self.HTTPError):
            endpoint("SPY", stored_only=True, stored_request=self.json.dumps(self.request))
        self.assertEqual(self.store_gets, 0)
        request = {**self.request, "selections": [{**self.selection, "scope": "ticker", "ticker_or_cusip": "QQQ"}]}
        endpoint = self.endpoint()
        with self.assertRaises(self.HTTPError):
            endpoint("SPY", stored_only=True, stored_request=self.json.dumps(request))
        self.assertEqual(self.store_gets, 0)

    def test_bad_cutoff_or_schema_and_store_failure_do_not_fallback(self):
        for field, value in (("as_of", "2026-10-16T12:00:00+00:00"), ("schema", "wrong")):
            with self.subTest(field=field):
                def wrong(**request):
                    return {**self.snapshot(**request), field: value}
                output = self.endpoint(snapshot=wrong)("SPY", stored_only=True, stored_request=self.json.dumps(self.request))
                snapshot = output["market_features"]["stored_source_evidence"]
                self.assertEqual(snapshot.get("reason"), "read_failed")
                self.assertEqual(snapshot["selections"], [])
        def broken(**request):
            raise RuntimeError("not forwarded to client")
        output = self.endpoint(snapshot=broken)("SPY", stored_only=True, stored_request=self.json.dumps(self.request))
        self.assertEqual(output["market_features"]["stored_source_evidence"]["reason"], "read_failed")

    def test_normal_cache_response_unchanged_and_request_not_interpreted(self):
        original = {"ticker": "SPY", "price_history": [{"date": "2020-01-02", "value": 0}]}
        before = deepcopy(original)
        output = self.endpoint(cached=original)("SPY", stored_request="invalid JSON ignored in normal mode")
        self.assertEqual(output, load_helper()({**original, "_cached": True}))
        self.assertNotIn("stored_source_evidence", output["market_features"])
        self.assertEqual(original, before)
        self.assertEqual(self.store_gets, 0)


if __name__ == "__main__":
    unittest.main()
