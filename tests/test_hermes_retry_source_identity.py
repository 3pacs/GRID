"""Actual resolver/retry/classifier AST definitions with inert adapters only."""
from __future__ import annotations

import argparse
import ast
import builtins
from datetime import date, datetime, timezone
import inspect
from pathlib import Path
import threading
import types
from typing import Any, Callable
import unittest

ROOT = Path(__file__).resolve().parents[1]
FIXERS = ROOT / "scripts/hermes_fixers.py"
SCHEDULER = ROOT / "ingestion/smart_scheduler.py"


class Log:
    def __init__(self):
        self.entries = []

    def __getattr__(self, name):
        return lambda text, **kwargs: self.entries.append((name, text, kwargs))


class Transaction:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def execute(self, sql, params):
        self.engine.formed_updates.append({"sql": sql, "params": dict(params)})

    def __exit__(self, *args):
        return False


class Engine:
    def __init__(self):
        self.formed_updates = []

    def begin(self):
        return Transaction(self)


def project(fixers: Path, scheduler: Path, importer):
    env = {"Any": Any, "Callable": Callable, "OperatorState": Any,
           "date": date, "datetime": datetime, "timezone": timezone,
           "inspect": inspect, "threading": threading, "log": Log(),
           "time": types.SimpleNamespace(monotonic=lambda: 10.0),
           "MAX_PULL_RETRIES": 3, "REPAIR_LOOKBACK_DAYS": 7,
           "_REPAIRS_LOCK": threading.RLock(), "_REPAIRS_IN_FLIGHT": {},
           "_repair_token_seq": 0,
           "__builtins__": dict(vars(builtins), __import__=importer)}
    scheduler_names = {"_puller_owns_catalog", "_result_children", "_materialization_accounting",
                       "_extract_rows", "_aggregate_coverage", "_classify_outcome"}
    constant_names = {"_ROW_COUNT_KEYS", "_RESULT_KEYS", "OUTCOME_SUCCESS", "OUTCOME_NO_NEW_DATA",
                      "OUTCOME_SKIPPED", "OUTCOME_FAILED", "OUTCOME_PARTIAL"}
    selected = []
    for node in ast.parse(scheduler.read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in scheduler_names:
            selected.append(node)
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(n, ast.Name) and n.id in constant_names for n in targets):
                selected.append(node)
    exec(compile(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])), "<actual-classifier>", "exec"), env)
    names = {"_FunctionPuller", "_resolve_puller", "_retry_source", "_next_repair_token",
             "_thread_is_alive", "retry_not_fresh_reason", "_retry_target_matches"}
    selected = []
    for node in ast.parse(fixers.read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names:
            selected.append(node)
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(n, ast.Name) and n.id in {"_CATALOG_TO_REGISTRY", "_NOT_FRESH_OUTCOMES"} for n in targets):
                selected.append(node)
    exec(compile(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])), "<actual-fixers>", "exec"), env)
    return env


class Harness:
    def __init__(self):
        self.engine = Engine()
        self.registry = {}
        self.modules = {}
        self.imports = []
        self.constructed = []
        self.calls = []
        self.callback = lambda puller, kwargs: {"status": "SUCCESS", "rows_inserted": 2}
        self.options_guard = None
        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "scripts.hermes_operator":
                return types.SimpleNamespace(_SOURCE_REGISTRY=self.registry)
            if name == "ingestion.smart_scheduler":
                return types.SimpleNamespace(**self.env)
            if name == "sqlalchemy":
                return types.SimpleNamespace(text=lambda sql: sql)
            if name == "importlib":
                return types.SimpleNamespace(import_module=self.import_module)
            if name in {"datetime", "inspect"}:
                return real_import(name, globals, locals, fromlist, level)
            raise AssertionError("Unexpected production import: " + name)
        # Functions capture their builtins at definition time: install the
        # rejecting importer before compiling either production projection.
        self.env = project(FIXERS, SCHEDULER, fake_import)
        self.env["_scheduler_options_retry_guard"] = lambda engine: self.options_guard

    def import_module(self, name):
        self.imports.append(name)
        if name not in self.modules:
            raise AssertionError("Unexpected adapter import: " + name)
        return self.modules[name]

    def add(self, key, declared, *, mod=None, class_name="Puller", entry=None):
        module_name = mod or "synthetic." + key
        harness = self

        class Puller:
            SOURCE_NAME = declared

            def __init__(self, db_engine):
                harness.constructed.append(key)

            def pull_all(self, **kwargs):
                harness.calls.append({"key": key, "kwargs": kwargs})
                return harness.callback(self, kwargs)
        self.modules[module_name] = types.SimpleNamespace(**{class_name: Puller})
        self.registry[key] = {"mod": module_name, "cls": class_name, **(entry or {})}
        return Puller

    def retry(self, name, **kwargs):
        return self.env["_retry_source"](name, self.engine, **kwargs)


class SourceIdentityTests(unittest.TestCase):
    def setUp(self):
        self.h = Harness()

    def test_01_actual_catalog_substitutions_refuse_before_constructor_provider(self):
        for requested, key, declared in [("polygon", "yfinance", "yfinance"),
                                          ("USDA_NASS", "fred", "FRED"),
                                          ("world_bank", "fred", "FRED"),
                                          ("fmp", "earnings_calendar", "yfinance_earnings")]:
            with self.subTest(requested=requested):
                self.h = Harness()
                self.h.add(key, declared)
                with self.assertRaises(ValueError):
                    self.h.retry(requested)
                self.assertEqual(self.h.constructed, [])
                self.assertEqual(self.h.calls, [])
                self.assertEqual(self.h.engine.formed_updates, [])

    def test_02_ecb_facility_prefix_is_not_facility_contract(self):
        self.h.add("ecb", "ECB")
        for name in ("ecb_tltro", "ecb_ilm_ltro"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.h.retry(name)
        self.assertEqual(self.h.imports, [])

    def test_03_ambiguous_prefix_order_does_not_dispatch(self):
        self.h.add("synthetic", "synthetic")
        self.h.add("synthetic_aux", "synthetic_aux")
        with self.assertRaises(ValueError):
            self.h.retry("synthetic_missing")
        self.assertEqual(self.h.constructed, [])

    def test_04_direct_entry_wins_over_bad_legacy_map(self):
        self.h.add("polygon", "polygon")
        self.h.add("yfinance", "yfinance")
        result = self.h.retry("PoLyGoN")
        self.assertEqual(result["outcome"], "SUCCESS")
        self.assertEqual(self.h.constructed, ["polygon"])
        self.assertEqual(self.h.engine.formed_updates[0]["params"], {"name": "PoLyGoN"})

    def test_05_same_declared_target_positive_report_preserves_existing_outcome(self):
        self.h.add("fred", "FRED")
        result = self.h.retry("FRED")
        self.assertEqual(result["outcome"], "SUCCESS")
        self.assertEqual(result["rows_inserted"], 2)
        self.assertEqual(len(self.h.calls), 1)
        self.assertEqual(self.h.engine.formed_updates[0]["params"], {"name": "FRED"})

    def test_06_existing_direct_registered_alias_with_exact_catalog_is_preserved(self):
        self.h.add("canonical_route", "Actual_Catalog")
        # Mirrors _SOURCE_ALIASES' shared entry references. The requested
        # catalog matches the class declaration, not just the route key.
        self.h.registry["actual_catalog"] = self.h.registry["canonical_route"]
        result = self.h.retry("Actual_Catalog")
        self.assertEqual(result["outcome"], "SUCCESS")
        self.assertEqual(len(self.h.calls), 1)

    def test_07_direct_entry_declaring_other_target_refuses_before_constructor(self):
        self.h.add("requested", "unrelated")
        with self.assertRaises(ValueError):
            self.h.retry("requested")
        self.assertEqual(self.h.constructed, [])
        self.assertEqual(self.h.calls, [])

    def test_08_unknown_or_nonstring_declared_target_refuses(self):
        for declared in (None, 5, ""):
            with self.subTest(declared=declared):
                self.h = Harness()
                self.h.add("requested", declared)
                with self.assertRaises(ValueError):
                    self.h.retry("requested")
                self.assertEqual(self.h.constructed, [])

    def test_09_callback_target_rebinding_cannot_stamp_or_report_recovery(self):
        self.h.add("requested", "requested")
        def callback(puller, kwargs):
            puller.SOURCE_NAME = "other_target"
            return {"status": "SUCCESS", "rows_inserted": 500}
        self.h.callback = callback
        result = self.h.retry("requested")
        self.assertEqual(len(self.h.calls), 1)
        self.assertEqual(result["outcome"], "FAILED")
        self.assertEqual(result["status"], "FAILED")
        self.assertIsNone(result["rows_inserted"])
        self.assertIsNotNone(self.h.env["retry_not_fresh_reason"](result))
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_10_all_nonfresh_unknown_or_partial_results_preserve_no_stamp(self):
        for payload in [None, {"status": "SUCCESS"}, {"status": "SUCCESS", "rows_inserted": None},
                        {"status": "PARTIAL", "rows_inserted": None},
                        {"status": "FAILED", "rows_inserted": 0},
                        {"status": "SKIPPED", "rows_inserted": 0},
                        {"status": "NO_NEW_DATA", "rows_inserted": 0},
                        {"status": "NO_DATA", "rows_inserted": 0},
                        {"status": "SUCCESS", "rows_inserted": 3, "failed": 1}]:
            with self.subTest(payload=payload):
                self.h = Harness()
                self.h.add("requested", "requested")
                self.h.callback = lambda puller, kwargs: payload
                result = self.h.retry("requested")
                self.assertNotEqual(result["outcome"], "SUCCESS")
                self.assertEqual(self.h.engine.formed_updates, [])
                if payload is None or (isinstance(payload, dict) and payload.get("rows_inserted", "missing") is None):
                    self.assertIsNone(result["rows_inserted"])

    def test_11_owner_managed_options_catalog_remains_single_writer(self):
        self.h.add("options", "YFINANCE_OPTIONS")
        self.h.registry["yfinance_options"] = self.h.registry["options"]
        for name in ("options", "yfinance_options"):
            with self.subTest(name=name):
                result = self.h.retry(name)
                self.assertEqual(result["outcome"], "SUCCESS")
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_12_options_observation_skip_precedes_constructor_and_preserves_backlog(self):
        self.h.add("options", "YFINANCE_OPTIONS")
        self.h.options_guard = {"status": "SKIPPED", "outcome": "SKIPPED", "rows_inserted": 0}
        state = types.SimpleNamespace(repair_backlog={"options": ["SYNTH"]})
        result = self.h.retry("options", state=state)
        self.assertEqual(result["outcome"], "SKIPPED")
        self.assertEqual(self.h.constructed, [])
        self.assertEqual(state.repair_backlog, {"options": ["SYNTH"]})

    def test_13_hold_never_bypassed_by_direct_or_prefix_mapping(self):
        self.h.add("offshore_leaks", "ICIJ_OFFSHORE", entry={"skip_runtime": "match-quality hold"})
        for name in ("offshore_leaks", "ICIJ_OFFSHORE", "offshore"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.h.retry(name)
        self.assertEqual(self.h.imports, [])
        self.assertEqual(self.h.constructed, [])

    def test_14_superseded_actual_token_cannot_join_other_attempt_result(self):
        self.h.add("requested", "requested")
        def callback(puller, kwargs):
            self.h.env["_REPAIRS_IN_FLIGHT"]["requested"]["token"] += 1
            return {"status": "SUCCESS", "rows_inserted": 3}
        self.h.callback = callback
        result = self.h.retry("requested")
        self.assertEqual(result["status"], "abandoned")
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_15_deadline_late_result_does_not_publish_state_or_freshness(self):
        self.h.add("requested", "requested")
        result = self.h.retry("requested", should_continue=lambda: False)
        self.assertEqual(result["status"], "abandoned")
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_16_dynamic_kwargs_evaluated_at_call_time_and_registry_not_mutated(self):
        values = []
        def dynamic():
            values.append("evaluated")
            return "2026-01-02"
        self.h.add("requested", "requested", entry={"pull_kwargs": {"anchor_date": dynamic}})
        self.assertEqual(values, [])
        self.h.retry("requested")
        self.assertEqual(values, ["evaluated"])
        self.assertEqual(self.h.calls[0]["kwargs"]["anchor_date"], "2026-01-02")
        self.assertIs(self.h.registry["requested"]["pull_kwargs"]["anchor_date"], dynamic)

    def test_17_budget_stop_preserves_unattempted_backlog_without_stamp(self):
        self.h.add("requested", "requested")
        self.h.callback = lambda puller, kwargs: {"status": "PARTIAL", "rows_inserted": 1,
                                                   "stopped_by_budget": True,
                                                   "tickers_not_attempted": ["SYNTH"]}
        state = types.SimpleNamespace(repair_backlog={})
        result = self.h.retry("requested", state=state)
        self.assertEqual(result["outcome"], "PARTIAL")
        self.assertEqual(state.repair_backlog, {"requested": ["SYNTH"]})
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_18_adapter_exception_not_logged_with_sensitive_value_here(self):
        self.h.add("requested", "requested")
        def callback(puller, kwargs):
            raise RuntimeError("SYNTHETIC_PRIVATE_VALUE")
        self.h.callback = callback
        with self.assertRaises(RuntimeError):
            self.h.retry("requested")
        self.assertNotIn("SYNTHETIC_PRIVATE_VALUE", repr(self.h.env["log"].entries))
        self.assertEqual(self.h.engine.formed_updates, [])
        self.assertEqual(self.h.env["_REPAIRS_IN_FLIGHT"], {})

    def test_19_function_route_without_catalog_contract_refuses_before_invocation(self):
        self.h.registry["function_route"] = {"mod": "synthetic.functions", "fn": "run"}
        self.h.modules["synthetic.functions"] = types.SimpleNamespace(run=lambda: self.fail("provider called"))
        with self.assertRaises(ValueError):
            self.h.retry("function_route")
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_20_dynamic_kwarg_rebinding_refuses_before_provider(self):
        cls = self.h.add("requested", "requested")
        def rebind():
            cls.SOURCE_NAME = "unrelated"
            return "2026-01-02"
        self.h.registry["requested"]["pull_kwargs"] = {"anchor_date": rebind}
        with self.assertRaises(ValueError):
            self.h.retry("requested")
        self.assertEqual(self.h.calls, [])
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_21_wrong_target_cannot_clear_existing_backlog_or_publish_check(self):
        self.h.add("requested", "requested")
        state = types.SimpleNamespace(repair_backlog={"requested": ["SYNTH"]},
                                      repair_last_check={}, repair_uncovered={})
        def callback(puller, kwargs):
            puller.SOURCE_NAME = "other_target"
            return {"status": "SUCCESS", "rows_inserted": 5,
                    "results": [], "counts": {"inserted": 1}}
        self.h.callback = callback
        result = self.h.retry("requested", state=state)
        self.assertEqual(result["outcome"], "FAILED")
        self.assertIsNone(result["rows_inserted"])
        self.assertEqual(state.repair_backlog, {"requested": ["SYNTH"]})
        self.assertEqual(state.repair_last_check, {})
        self.assertEqual(state.repair_uncovered, {})
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_22_explicit_alias_does_not_imply_different_catalog_equivalence(self):
        self.h.add("canonical", "actual_catalog")
        self.h.registry["different_catalog"] = self.h.registry["canonical"]
        with self.assertRaises(ValueError):
            self.h.retry("different_catalog")
        self.assertEqual(self.h.constructed, [])
        self.assertEqual(self.h.calls, [])

    def test_23_unsupported_provider_identity_field_is_not_trusted_schema(self):
        self.h.add("requested", None)
        self.h.callback = lambda puller, kwargs: {"status": "SUCCESS", "rows_inserted": 5,
                                                  "dataset_identity": "requested"}
        with self.assertRaises(ValueError):
            self.h.retry("requested")
        self.assertEqual(self.h.calls, [])
        self.assertEqual(self.h.engine.formed_updates, [])

    def test_24_route_normalization_does_not_merge_distinct_catalog_names(self):
        self.h.add("actual_catalog", "Actual_Catalog")
        for name in ("Actual Catalog", "Actual-Catalog"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.h.retry(name)
        self.assertEqual(self.h.constructed, [])
        self.assertEqual(self.h.calls, [])

    def test_25_explicit_mapped_alias_requires_existing_exact_catalog_declaration(self):
        self.h.add("earnings_calendar", "yfinance_earnings")
        result = self.h.retry("yfinance_earnings")
        self.assertEqual(result["outcome"], "SUCCESS")
        self.assertEqual(self.h.constructed, ["earnings_calendar"])
        self.assertEqual(len(self.h.calls), 1)
        self.assertEqual(self.h.engine.formed_updates[0]["params"], {"name": "yfinance_earnings"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixers", type=Path)
    parser.add_argument("--scheduler", type=Path)
    args, remaining = parser.parse_known_args()
    if args.fixers:
        FIXERS = args.fixers
    if args.scheduler:
        SCHEDULER = args.scheduler
    unittest.main(argv=[__file__, *remaining], verbosity=2)
