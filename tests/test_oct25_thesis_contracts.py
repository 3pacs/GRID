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


def _node_name(node):
    if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
        return node.name
    if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
        return node.targets[0].id
    return None


def load(path, names, **overrides):
    """Exec exactly the named top-level functions, classes and constant assignments."""
    tree = ast.parse(SOURCES[path], filename=path)
    nodes = [n for n in tree.body if _node_name(n) in names]
    if sorted(_node_name(n) for n in nodes) != sorted(set(names)):
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


CHANGEPOINT_READER = ["DEFAULT_ACCURACY", "CHANGEPOINT_DETECTOR_VERSION", "CHANGEPOINT_FRESH_DAYS",
                      "CHANGEPOINT_POINTS_PER_FEATURE", "CHANGEPOINT_SCORE_CAP",
                      "CHANGEPOINT_CONFIDENCE_CAP", "_verdict", "_score_regime_changepoints"]


def categories(directions, confidences=None, tickers=None, statuses=None, sids=None, valid_froms=None,
               break_dates=None, unpublished=None, as_of=None):
    """Reader over invented rows (signal_id, ticker, direction, confidence, valid_from, status,
    break_date, effective_known_at, unpublished) as its query returns them (already fenced).
    Each row is its own break unless ``sids`` says otherwise."""
    ns = load(PATHS[1], CHANGEPOINT_READER)
    n = len(directions)
    values = confidences if confidences is not None else [0.5] * n
    names = tickers if tickers is not None else [f"SYNTH-{i}" for i in range(n)]
    status = statuses if statuses is not None else [None] * n
    ids = sids if sids is not None else [f"break-{i}" for i in range(n)]
    times = valid_froms if valid_froms is not None else [NOW - timedelta(seconds=i) for i in range(n)]
    dates = break_dates if break_dates is not None else ["2026-10-20"] * n
    flags = unpublished if unpublished is not None else [False] * n
    boundary = MemoryBoundary([(sid, t, d, c, vf, st, bd, vf, u) for sid, t, d, c, vf, st, bd, u
                               in zip(ids, names, directions, values, times, status, dates, flags)])
    return ns["_score_regime_changepoints"](boundary, 0.5, as_of=as_of), boundary


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
        self.assertEqual(out["category_counts"], dict(unit="features", rows_read=10, breaks_read=10, total=10, bullish=4, bearish=4, neutral=2, other_unknown=0, withdrawn=0, aged_out=0, unpublished=0, known_categories_exhaustive=True))
        self.assertEqual(out["score"], 0)

    def test_T2_other_unknown_is_not_neutral(self):
        out, _ = categories(["bullish", "bearish", "neutral", "other", None])
        c = out["category_counts"]
        self.assertEqual((c["neutral"], c["other_unknown"], c["known_categories_exhaustive"]), (1, 2, False), "UNKNOWN_IS_NOT_NEUTRAL")
        self.assertIn("1 neutral, 2 other/unknown", out["data_point"])
        self.assertIn("2 other/unknown", out["reasoning"])

    def test_T2_unknown_retains_original_denominator_and_confidence(self):
        out, boundary = categories(["bullish", "bullish", "bearish", "neutral", "unclassified"], [1, 1, 1, 0, 0])
        # net 1 feature x 10 points x avg conf 0.6 (unknown in the denominator)
        self.assertEqual(out["score"], 6.0, "UNKNOWN_DENOMINATOR_MUST_REMAIN")
        self.assertEqual(out["confidence"], 57.0)
        self.assertEqual(out["category_counts"]["total"], 5)
        sql, params = boundary.statements[-1]
        self.assertIn("valid_from > :window_start", sql)
        self.assertIn("valid_from <= :as_of", sql)
        self.assertEqual(params["window_start"], params["as_of"] - timedelta(days=5))

    def test_T2_counts_features_not_rows(self):
        # One feature re-published many times counts once, by its latest break.
        out, _ = categories(["bearish"] + ["bullish"] * 50 + ["bullish"],
                            [0.5] * 52, ["vix_spot"] * 51 + ["sp500_full"])
        c = out["category_counts"]
        self.assertEqual((c["total"], c["bullish"], c["bearish"], c["rows_read"]), (2, 1, 1, 52), "FEATURES_NOT_ROWS")
        self.assertEqual(out["score"], 0)
        self.assertIn("2 features with a fresh break", out["data_point"])
        self.assertIn("risk-off (vix_spot)", out["reasoning"])
        self.assertIn("risk-on (sp500_full)", out["reasoning"])

    def test_T2_contribution_is_capped(self):
        out, _ = categories(["bearish"] * 12, [1.0] * 12)
        self.assertEqual(out["score"], -30.0, "SCORE_CAP")
        self.assertLessEqual(out["confidence"], 60.0, "CONFIDENCE_CAP")
        self.assertEqual(out["direction"], "bearish")

    def test_T2_withdrawn_break_is_not_counted(self):
        # Break A on vix_spot: live, then withdrawn (its latest row). Neither row counts.
        out, _ = categories(["neutral", "bullish", "bearish"], [0, 1, 1], ["vix_spot", "sp500_full", "vix_spot"],
                            ["withdrawn", "live", "live"], sids=["A", "S", "A"])
        c = out["category_counts"]
        self.assertEqual((c["total"], c["bullish"], c["bearish"], c["withdrawn"]), (1, 1, 0, 1), "WITHDRAWN_NOT_COUNTED")
        self.assertIn("1 break withdrawn on later data", out["reasoning"])
        only, _ = categories(["neutral"], [0], ["vix_spot"], ["withdrawn"])
        self.assertEqual(only["status"], "no_data")
        self.assertNotIn("category_counts", only)

    def test_T2_withdrawal_does_not_hide_another_break_on_the_feature(self):
        # vix_spot: A live then withdrawn, B live. B counts, in either row order and either break order.
        hour = timedelta(hours=1)
        for withdrawal_newest in (True, False):
            for b_later in (True, False):
                with self.subTest(withdrawal_newest=withdrawal_newest, b_later=b_later):
                    a_date, b_date = ("2026-10-01", "2026-10-20") if b_later else ("2026-10-20", "2026-10-01")
                    rows = [("A", "bullish", 0.9, NOW - 48 * hour, "live", a_date),
                            ("A", "neutral", 0, NOW - (1 if withdrawal_newest else 3) * hour, "withdrawn", a_date),
                            ("B", "bearish", 0.8, NOW - (3 if withdrawal_newest else 1) * hour, "live", b_date)]
                    for order in (rows, rows[::-1]):
                        sid, d, c, vf, st, bd = zip(*order)
                        out, _ = categories(list(d), list(c), ["vix_spot"] * 3, list(st), list(sid), list(vf), list(bd))
                        counts = out["category_counts"]
                        self.assertEqual((counts["total"], counts["bearish"], counts["withdrawn"]), (1, 1, 1),
                                         "WITHDRAWAL_RECONCILED_BY_SIGNAL_ID_FIRST")

    def test_T2_both_fences_bound_at_an_explicit_as_of(self):
        replay = NOW - timedelta(days=1)
        _, boundary = categories(["bullish"], valid_froms=[replay - timedelta(hours=1)], as_of=replay)
        sql, params = boundary.statements[-1]
        self.assertIn("valid_from <= :as_of", sql, "AVAILABILITY_FENCE")
        self.assertIn("CAST(metadata->>'published_at' AS timestamptz) <= :as_of", sql, "PUBLICATION_FENCE")
        self.assertNotIn("NOW()", sql)
        self.assertEqual((params["as_of"], params["window_start"]), (replay, replay - timedelta(days=5)))
        _, live = categories(["bullish"])
        self.assertEqual(live.statements[-1][1]["as_of"], NOW, "LIVE_CALL_IS_AS_OF_NOW")

    def test_T2_unpublished_rows_drop_before_reconciliation(self):
        # A withdrawal with no published_at must not hide the break it names.
        out, _ = categories(["neutral", "bearish"], [0, 0.8], ["vix_spot", "vix_spot"], ["withdrawn", "live"],
                            sids=["A", "A"], unpublished=[True, False])
        c = out["category_counts"]
        self.assertEqual((c["total"], c["bearish"], c["withdrawn"], c["unpublished"]), (1, 1, 0, 1))

    def test_T2_only_the_current_detector_version_counts(self):
        _, boundary = categories(["bullish"])
        sql, params = boundary.statements[-1]
        self.assertIn("provenance = :version", sql, "LEGACY_ROWS_IGNORED_BY_VERSION")
        self.assertEqual(params["version"], "changepoint_detector/2")
        self.assertEqual((params["src"], params["stype"]), ("discovery.changepoint_detector", "regime_change"))
        self.assertNotIn("48 hours", sql)

    def test_T2_all_unknown_is_not_exhaustive(self):
        out, _ = categories([None, "BULLISH", "unmapped"])
        self.assertEqual(out["category_counts"]["other_unknown"], 3)
        self.assertFalse(out["category_counts"]["known_categories_exhaustive"])
        self.assertEqual(out["category_counts"]["neutral"], 0)

    def test_T2_empty_retains_no_data(self):
        out, _ = categories([])
        self.assertEqual(out["status"], "no_data")
        self.assertNotIn("category_counts", out)

    PUBLISHER = ["CHANGEPOINT_DETECTOR_VERSION", "SOURCE_MODULE", "SIGNAL_TYPE", "FRESH_BREAK_DAYS",
                 "STATUS_LIVE", "STATUS_WITHDRAWN", "_STAT_CLIP", "_EXISTING_SQL", "_INSERT_SQL", "_Written",
                 "_latest_written", "_base_meta", "_signal_params", "_withdrawal", "_plan_writes",
                 "publish_regime_signals"]

    def _publisher(self):
        import numpy
        from datetime import date
        return load(PATHS[2], self.PUBLISHER, np=numpy, date=date, datetime=datetime,
                    CAUSAL_PRE_WINDOW=60, CAUSAL_POST_WINDOW=10, CAUSAL_PEAK_WINDOW=3)

    @staticmethod
    def _report(*breaks):
        return SimpleNamespace(as_of=NOW.date(), fresh_breaks=list(breaks), breaks=list(breaks),
                               series={}, min_confidence=0.5)

    @staticmethod
    def _brk(i, direction, valid_from, stat=4.2, sha=None):
        spec = SimpleNamespace(key=f"k{i}", concept="vol", stress_sign=1)
        sig = SimpleNamespace(statistic=stat, confidence=0.7, magnitude=0.5, pre_regime="flat", post_regime="rising")
        return SimpleNamespace(spec=spec, feature=f"SYNTH-{i}", feature_id=i, signal=sig,
                               signal_id=f"changepoint_detector/2:SYNTH-{i}:2026-10-20",
                               break_date=NOW.date(), confirm_date=NOW.date(), known_at_date=NOW.date(),
                               support_start_date=NOW.date(), support_sha256=sha or f"sha{i}",
                               valid_from=valid_from, effective_known_at=valid_from,
                               direction=direction, fresh=True)

    def test_T2_publisher_declared_partition_by_stress_sign(self):
        ns = self._publisher()
        boundary = MemoryBoundary()
        boundary.rowcount = 1
        known = NOW - timedelta(hours=10)
        report = self._report(self._brk(0, "bearish", known), self._brk(1, "bullish", known, stat=-4.2),
                              self._brk(2, "neutral", known, stat=0.0),
                              self._brk(3, "bearish", NOW + timedelta(hours=1)))
        self.assertEqual(ns["publish_regime_signals"](boundary, report, now=NOW), 3, "FUTURE_VALID_FROM_DEFERRED")
        self.assertEqual((report.publish_counts["first"], report.publish_counts["waiting"]), (3, 1))
        params = [p for q, p in boundary.statements if "INSERT" in q]
        self.assertEqual([p["direction"] for p in params], ["bearish", "bullish", "neutral"])
        self.assertTrue(all(p["vfrom"] == known for p in params), "VALID_FROM_IS_KNOWN_AT_NOT_NOW")
        self.assertTrue(all(p["version"] == "changepoint_detector/2" for p in params))
        self.assertEqual(params[0]["sid"], "changepoint_detector/2:SYNTH-0:2026-10-20")
        self.assertIn('"risk": "risk_off"', params[0]["meta"])
        self.assertIn('"risk": "risk_on"', params[1]["meta"])
        self.assertIn('"break_id": "changepoint_detector/2:SYNTH-0:2026-10-20"', params[0]["meta"])
        self.assertIn("metadata->>'support_sha256'", boundary.statements[0][0])
        self.assertIn(f'"effective_known_at": "{known.isoformat()}"', params[0]["meta"])
        self.assertIn(f'"first_published_at": "{NOW.isoformat()}"', params[0]["meta"], "DETECTOR_CLOCK_SEPARATE")
        insert = next(q for q, _ in boundary.statements if "INSERT" in q)
        self.assertIn("valid_from >= CAST(:vfrom AS timestamptz)", insert, "NEVER_BEHIND_AN_EXISTING_ROW")

    def test_T2_publisher_revision_is_dated_no_earlier_than_the_scan(self):
        ns = self._publisher()
        earlier = NOW - timedelta(days=2)
        sid0, sid1 = (f"changepoint_detector/2:SYNTH-{i}:2026-10-20" for i in (0, 1))
        first = (earlier - timedelta(hours=1)).isoformat()
        boundary = MemoryBoundary([(sid0, "SYNTH-0", earlier, 0.7, "old-sha", "live", "2026-10-20", earlier.isoformat(), first),
                                   (sid1, "SYNTH-1", earlier, 0.7, "sha1", "live", "2026-10-20", earlier.isoformat(), first)])
        boundary.rowcount = 1
        # SYNTH-0's support changed; its new known_at is BEFORE its first row (a back-dated vintage).
        report = self._report(self._brk(0, "bearish", earlier - timedelta(days=1)),
                              self._brk(1, "bearish", earlier))
        self.assertEqual(ns["publish_regime_signals"](boundary, report, now=NOW), 1)
        (params,) = [p for q, p in boundary.statements if "INSERT" in q]
        self.assertEqual((params["sid"], params["vfrom"]), (sid0, NOW), "REVISION_NOT_BACKDATED")
        self.assertIn(f'"supersedes_valid_from": "{earlier.isoformat()}"', params["meta"])
        self.assertIn(f'"first_published_at": "{first}"', params["meta"], "FIRST_PUBLICATION_CARRIED")
        self.assertIn(f'"published_at": "{NOW.isoformat()}"', params["meta"])
        self.assertIn(f'"effective_known_at": "{(earlier - timedelta(days=1)).isoformat()}"', params["meta"])
        self.assertEqual((report.publish_counts["revision"], report.publish_counts["unchanged"]), (1, 1))

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
