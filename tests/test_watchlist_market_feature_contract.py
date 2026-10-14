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
        # Exact first three statements return cached response before engine/provider work.
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
            body=fn.body[1:4],
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


if __name__ == "__main__":
    unittest.main()
