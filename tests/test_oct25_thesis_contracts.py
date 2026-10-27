"""Invented, AST-isolated contract tests; no application imports or real I/O.

Only the named source functions/classes are loaded. All effectful collaborators
are inert fixtures. These checks do not run outcomes, evaluation or live writes.
"""
from __future__ import annotations

import ast
import copy
import json
import math
import unittest
from decimal import Decimal
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PATHS = (
    "analysis/flow_thesis_scoring.py", "analysis/thesis_scorer.py",
    "discovery/changepoint_detector.py", "intelligence/thesis_tracker.py",
)
SOURCES = {name: (ROOT / name).read_text(encoding="utf-8") for name in PATHS}
NOW = datetime(2026, 10, 26, 14, 0, tzinfo=timezone.utc)


class Clock:
    @staticmethod
    def now(tz=None):
        return NOW


class Log:
    def __init__(self):
        self.messages = []

    def info(self, message, **kwargs):
        self.messages.append(message.format(**kwargs))

    warning = info
    debug = info


class Query:
    def __init__(self, sql):
        self.sql = sql
        self.params = {}

    def bindparams(self, **kwargs):
        self.params = kwargs
        return self


class MemoryBoundary:
    """A finite statement recorder, not a SQL interpreter or database."""
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements = []
        self.acquisitions = 0
        self.schema_calls = 0
        self.connection_checks = 0
        self.namespace = "synthetic_thesis"

    def begin(self):
        self.acquisitions += 1
        return self

    connect = begin

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.statements.append((query.sql, params if params is not None else query.params))
        return self

    def fetchone(self):
        return (17,)

    def fetchall(self):
        return self.rows


def schema_stub(engine):
    engine.schema_calls += 1
    return engine.namespace


def connection_contract(connection, *, expected_schema):
    """Explicit isolated seam: bind the operation connection to its preflight."""
    assert isinstance(connection, MemoryBoundary)
    assert expected_schema == connection.namespace == "synthetic_thesis"
    assert connection.acquisitions > 0
    connection.connection_checks += 1
    return expected_schema


def load(path, names, **overrides):
    tree = ast.parse(SOURCES[path], filename=path)
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
    if {n.name for n in nodes} != set(names):
        raise AssertionError("EXACT_AST_SELECTION_REQUIRED")
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)] + nodes, type_ignores=[])
    namespace = dict(__name__=__name__, Any=Any, datetime=Clock, timezone=timezone,
                     timedelta=timedelta, json=json, dataclass=dataclass, asdict=asdict,
                     log=Log(), text=Query, DEFAULT_ACCURACY=0.5,
                     BULLISH="bullish", BEARISH="bearish", NEUTRAL="neutral",
                     _ensure_tables=schema_stub, require_thesis_connection=connection_contract)
    namespace.update(overrides)
    exec(compile(ast.fix_missing_locations(module), path, "exec"), namespace)
    return namespace


def flow(knowledge, learned=None):
    return load(PATHS[0], ["generate_unified_thesis", "_build_narrative"],
                update_current_states=lambda engine: copy.deepcopy(knowledge),
                _load_learned_weights=lambda engine: dict(learned or {}))


def state(direction, confidence="low", value=1):
    return dict(confidence=confidence, current_state=dict(direction=direction, value=value, detail="SYNTH"))


def tracker():
    return load(PATHS[3], ["_snapshot_conviction_fraction", "snapshot_thesis",
                          "get_thesis_history", "ThesisSnapshot", "_parse_json"])


def categories(directions, confidences=None):
    ns = load(PATHS[1], ["_verdict", "_score_regime_changepoints"])
    values = confidences if confidences is not None else [0.5] * len(directions)
    boundary = MemoryBoundary([(d, c, 2.0, NOW) for d, c in zip(directions, values)])
    return ns["_score_regime_changepoints"](boundary, 0.5), boundary


class ThesisContracts(unittest.TestCase):
    def test_T1_weighted_minority_wins(self):
        knowledge = {"bull": state("bullish", "high"), "bear_a": state("bearish"), "bear_b": state("bearish")}
        out = flow(knowledge)["generate_unified_thesis"](None)
        self.assertEqual(out["overall_direction"], "BULLISH", "WEIGHTS_NOT_VOTE_SHARE")
        self.assertEqual((out["bullish_score"], out["bearish_score"], out["conviction"]), (3, 2, 20))
        self.assertIn("Weighted scores: 3 bull vs 2 bear", out["narrative"], "WEIGHT_LABEL_REQUIRED")
        self.assertIn("not vote counts", out["narrative"])
        self.assertIn("not calibrated probability", out["narrative"])

    def test_T1_learned_weights_not_counts(self):
        k = {"b": state("bullish", "high"), "s": state("bearish", "high")}
        out = flow(k, {"b": 0.5, "s": 2})["generate_unified_thesis"](None)
        self.assertEqual((out["overall_direction"], out["conviction"]), ("BEARISH", 60))
        self.assertIn("1.5 bull vs 6 bear", out["narrative"], "FRACTIONAL_WEIGHT_MUST_SURVIVE_RENDER")

    def test_T1_neutral_weight_stays_in_denominator(self):
        k = {"b": state("bullish", "high"), "s": state("bearish"), "n": state("neutral", "moderate")}
        out = flow(k)["generate_unified_thesis"](None)
        self.assertEqual(out["conviction"], 33, "NEUTRAL_WEIGHT_DENOMINATOR_PRESERVED")
        self.assertEqual(out["active_theses"], 3)

    def test_T1_threshold_boundary_preserved(self):
        k = {"b": state("bullish"), "s": state("bearish")}
        out = flow(k, {"b": 1.5})["generate_unified_thesis"](None)
        self.assertEqual(out["overall_direction"], "NEUTRAL")
        self.assertEqual(out["conviction"], 20)

    def test_T1_missing_state_is_not_a_zero_observation(self):
        out = flow({"missing": state("bullish", value=None), "zero": state("neutral", value=0)})["generate_unified_thesis"](None)
        self.assertEqual(out["active_theses"], 1)
        self.assertIsNone(out["theses"][0]["current_value"])

    def test_T1_empty_contract_preserved(self):
        out = flow({})["generate_unified_thesis"](None)
        self.assertEqual((out["overall_direction"], out["conviction"], out["active_theses"]), ("NEUTRAL", 0, 0))
        self.assertEqual(out["conviction_unit"], "percent", "PRODUCER_UNIT_REQUIRED")

    def test_T2_explicit_neutral_is_valid(self):
        out, _ = categories(["bullish"] * 4 + ["bearish"] * 4 + ["neutral"] * 2)
        self.assertEqual(out["category_counts"], dict(total=10, bullish=4, bearish=4, neutral=2, other_unknown=0, known_categories_exhaustive=True))
        self.assertEqual(out["score"], 0)

    def test_T2_other_unknown_is_not_neutral(self):
        out, _ = categories(["bullish", "bearish", "neutral", "other", None])
        c = out["category_counts"]
        self.assertEqual((c["neutral"], c["other_unknown"], c["known_categories_exhaustive"]), (1, 2, False), "UNKNOWN_IS_NOT_NEUTRAL")
        self.assertIn("1 neutral, 2 other/unknown", out["data_point"])
        self.assertIn("2 other/unknown", out["reasoning"])

    def test_T2_unknown_retains_original_denominator_and_confidence(self):
        out, boundary = categories(["bullish", "bullish", "bearish", "neutral", "unclassified"], [1, 1, 1, 0, 0])
        self.assertEqual(out["score"], 7.2, "UNKNOWN_DENOMINATOR_MUST_REMAIN")
        self.assertEqual(out["confidence"], 72.5)
        self.assertEqual(out["category_counts"]["total"], 5)
        self.assertIn("48 hours", boundary.statements[-1][0])

    def test_T2_all_unknown_is_not_exhaustive(self):
        out, _ = categories([None, "BULLISH", "unmapped"])
        self.assertEqual(out["category_counts"]["other_unknown"], 3)
        self.assertFalse(out["category_counts"]["known_categories_exhaustive"])
        self.assertEqual(out["category_counts"]["neutral"], 0)

    def test_T2_empty_retains_no_data(self):
        out, _ = categories([])
        self.assertEqual(out["status"], "no_data")
        self.assertNotIn("category_counts", out)

    def test_T2_publisher_unchanged_declared_partition(self):
        ns = load(PATHS[2], ["publish_regime_signals"])
        boundary = MemoryBoundary()
        report = SimpleNamespace(regime_changes=[SimpleNamespace(series_id=f"SYNTH-{i}", change_index=i, magnitude=1, confidence=0.5, pre_regime="stable", post_regime=r) for i, r in enumerate(["rising", "falling", "stable", "other"])])
        self.assertEqual(ns["publish_regime_signals"](boundary, report), 4)
        params = [p for q, p in boundary.statements if "INSERT" in q]
        self.assertEqual([p["direction"] for p in params], ["bullish", "bearish", "neutral", "neutral"])
        self.assertTrue(all(p["vfrom"] == NOW for p in params))
        self.assertIn('"post_regime": "other"', params[-1]["meta"])

    def test_T3_declared_units_disambiguate_one(self):
        f = tracker()["_snapshot_conviction_fraction"]
        self.assertEqual(f({"conviction": 1, "conviction_unit": "percent"}), 0.01, "UNIT_NOT_MAGNITUDE")
        self.assertEqual(f({"conviction": 1, "conviction_unit": "fraction"}), 1)
        self.assertEqual(f({"conviction": 1}), 1)

    def test_T3_percent_fraction_equivalence(self):
        f = tracker()["_snapshot_conviction_fraction"]
        self.assertEqual(f({"conviction": 75, "conviction_unit": "percent"}), f({"conviction": 0.75, "conviction_unit": "fraction"}))
        self.assertEqual(f({"conviction": 0.75}), 0.75)

    def test_T3_missing_and_null_unknown(self):
        f = tracker()["_snapshot_conviction_fraction"]
        self.assertIsNone(f({}), "MISSING_CONVICTION_IS_UNKNOWN")
        self.assertIsNone(f({"conviction": None}), "NULL_CONVICTION_IS_UNKNOWN")
        self.assertIsNone(f({"conviction": None, "conviction_unit": None}))

    def test_T3_zero_is_valid(self):
        f = tracker()["_snapshot_conviction_fraction"]
        self.assertEqual(f({"conviction": 0}), 0)
        self.assertEqual(f({"conviction": 0, "conviction_unit": "percent"}), 0)

    def test_T3_unknown_unit_refuses_numeric_conversion(self):
        f = tracker()["_snapshot_conviction_fraction"]
        for unit in [None, "unknown", "probability", "pct"]:
            with self.subTest(unit=unit), self.assertRaises(ValueError):
                f({"conviction": 1, "conviction_unit": unit})

    def test_T3_no_unit_magnitude_guess(self):
        with self.assertRaises(ValueError, msg="NO_MAGNITUDE_UNIT_INFERENCE"):
            tracker()["_snapshot_conviction_fraction"]({"conviction": 75})

    def test_T3_invalid_values_refuse_before_effect_boundary(self):
        ns = tracker()
        for value in [True, False, float("nan"), float("inf"), -1, 1.01, "not-a-number"]:
            with self.subTest(value=str(value)):
                boundary = MemoryBoundary()
                with self.assertRaises((ValueError, TypeError)):
                    ns["snapshot_thesis"](boundary, {"conviction": value})
                self.assertEqual((boundary.acquisitions, boundary.schema_calls, boundary.statements), (0, 0, []))

    def test_T3_percent_upper_bound(self):
        f = tracker()["_snapshot_conviction_fraction"]
        self.assertEqual(f({"conviction": 100, "conviction_unit": "percent"}), 1)
        with self.assertRaises(ValueError):
            f({"conviction": 100.01, "conviction_unit": "percent"})

    def test_T3_actual_generator_to_snapshot_seam(self):
        out = flow({"b": state("bullish", "high"), "s": state("bearish")})["generate_unified_thesis"](None)
        self.assertEqual(out["conviction"], 50)
        boundary = MemoryBoundary()
        ns = tracker()
        self.assertEqual(ns["snapshot_thesis"](boundary, out), 17)
        self.assertEqual(boundary.connection_checks, 1)
        params = boundary.statements[0][1]
        self.assertEqual(params["conviction"], 0.5, "GENERATOR_PERCENT_TO_STORAGE_FRACTION")
        self.assertEqual(params["narrative"], out["narrative"])
        self.assertEqual(out["conviction"], 50)
        self.assertIn("50%", ns["log"].messages[0])

    def test_T3_operation_namespace_drift_refuses_before_insert(self):
        ns = tracker(); boundary = MemoryBoundary()
        ns["_ensure_tables"] = lambda engine: "wrong_namespace"
        with self.assertRaises(AssertionError):
            ns["snapshot_thesis"](boundary, {"conviction": 0.5})
        self.assertEqual(boundary.statements, [])

    def test_T3_nullable_snapshot_seam(self):
        for data in [{}, {"conviction": None, "narrative": "unverified separate textual conviction"}]:
            with self.subTest(data=data):
                ns = tracker(); boundary = MemoryBoundary()
                ns["snapshot_thesis"](boundary, data)
                self.assertIsNone(boundary.statements[0][1]["conviction"], "SNAPSHOT_NULL_NOT_IMPUTED")
                self.assertIn("unknown", ns["log"].messages[0])
                self.assertEqual(boundary.statements[0][1]["narrative"], data.get("narrative", ""))

    def test_T3_history_preserves_raw_values_nulls_and_unknown_units(self):
        rows = [(i, NOW, "bullish", value, [], [], {}, "Original narrative", None, None, None) for i, value in enumerate([None, 0, 1, 0.75, 75])]
        original = copy.deepcopy(rows)
        result = tracker()["get_thesis_history"](MemoryBoundary(rows))
        self.assertEqual([s.conviction for s in result], [None, 0, 1, 0.75, 75], "HISTORICAL_NULL_AND_RAW_VALUES_PRESERVED")
        self.assertTrue(all(s.to_dict()["conviction_unit"] is None for s in result), "HISTORICAL_UNITS_UNKNOWN")
        self.assertEqual(rows, original)
        self.assertTrue(all(s.narrative == "Original narrative" for s in result))

    def test_T3_dataclass_null_serializes_as_null(self):
        cls = tracker()["ThesisSnapshot"]
        record = cls(1, NOW.isoformat(), "neutral", None, [], [], {}, "SYNTH")
        self.assertIsNone(json.loads(json.dumps(record.to_dict()))["conviction"])
        self.assertIsNone(record.to_dict()["conviction_unit"])


    def test_T3_exact_outside_bounds_refuses_before_effects(self):
        for value, unit in [("1.00000000000000001", "fraction"),
                            (Decimal("100.00000000000000001"), "percent"),
                            ("-1e-999", "fraction")]:
            boundary = MemoryBoundary()
            refused = False
            try:
                tracker()["snapshot_thesis"](boundary, {"conviction": value, "conviction_unit": unit})
            except ValueError:
                refused = True
            self.assertTrue(refused and boundary.schema_calls == boundary.acquisitions == 0
                            and not boundary.statements, "EXACT_BOUND_BEFORE_EFFECTS")

    def test_T3_exact_valid_near_bounds_and_units(self):
        for value, unit in [("0.99999999999999999", "fraction"),
                            (Decimal("99.99999999999999999"), "percent"),
                            (Decimal("0.00000000000000001"), "fraction"),
                            ("0", "fraction"), (Decimal("100"), "percent"),
                            ("1", "percent"), (Decimal("1"), "fraction")]:
            boundary = MemoryBoundary()
            tracker()["snapshot_thesis"](boundary, {"conviction": value, "conviction_unit": unit})
            expected = float(value) / (100 if unit == "percent" else 1)
            self.assertEqual(boundary.statements[0][1]["conviction"], expected)
            self.assertEqual((boundary.schema_calls, boundary.acquisitions), (1, 1))

    def test_T3_exact_representation_controls(self):
        class FloatLike:
            def __float__(self):
                raise AssertionError("ARBITRARY_COERCION_EXECUTED")
        for value in [Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"),
                      "-Infinity", "NaN", "bad", True, b"0.5", FloatLike()]:
            boundary = MemoryBoundary()
            with self.assertRaises(ValueError):
                tracker()["snapshot_thesis"](boundary, {"conviction": value})
            self.assertEqual((boundary.schema_calls, boundary.acquisitions, boundary.statements), (0, 0, []))
        for data in [{}, {"conviction": None, "conviction_unit": None}]:
            self.assertIsNone(tracker()["_snapshot_conviction_fraction"](data))
        with self.assertRaises(ValueError):
            tracker()["_snapshot_conviction_fraction"]({"conviction": Decimal("1"), "conviction_unit": None})


if __name__ == "__main__":
    unittest.main()
