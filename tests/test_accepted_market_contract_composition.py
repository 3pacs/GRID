"""Twelve synthetic contract checks in one isolated child per unittest class.

Import/pytest collection only defines ordinary wrappers. Canonical module shells,
effect fences and all accepted-module calls live in the isolated child process.
Run normally with unittest/pytest, or use --isolated-child/--isolated-example
through an inspected offline runner. No dependencies or services are installed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

_RECEIPT_MARKER = "COMPOSITION_CHILD_RECEIPT="
_EXPECTED_METHODS = ('test_duration_and_opening_window_are_exact_shared_boundaries', 'test_exact_joint_positive_and_original_metadata', 'test_fleet_default_and_explicit_primary_guard_with_joint_imports', 'test_identity_hash_session_and_quality_policy_are_bound', 'test_observation_and_ancillary_cutoff_microsecond', 'test_proxy_requires_distinct_basis_and_method', 'test_public_import_identity_and_effect_fence_controls', 'test_quality_fail_unknown_do_not_promote_numeric_structure', 'test_temporal_is_separate_preserves_bytes_and_typed_policy', 'test_temporal_selection_knowledge_and_future_lanes', 'test_temporal_support_clocks_do_not_use_later_current_leaf', 'test_time_roles_are_explicit_and_no_late_revision_repair')


def _isolated_payload(*, example: bool = False) -> dict:
    import importlib.util
    import os
    import socket
    import subprocess
    import sys
    import types
    import unittest
    from dataclasses import replace
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal
    from fractions import Fraction
    from pathlib import Path
    from unittest.mock import patch

    ROOT = Path(__file__).resolve().parents[1]


    def deny(*args: object, **kwargs: object) -> None:
        raise AssertionError("composition external effect fenced")


    def audit(event: str, args: tuple) -> None:
        if event.startswith(("socket.", "subprocess.", "os.exec", "os.spawn", "os.posix_spawn")) or event in {
            "os.system", "os.kill", "os.killpg", "os.fork", "os.forkpty",
        }:
            deny()


    # These fences precede imports, including fleet's module-level dependencies.
    sys.addaudithook(audit)
    subprocess.Popen = subprocess.run = subprocess.call = subprocess.check_call = subprocess.check_output = deny
    socket.socket = socket.create_connection = socket.getaddrinfo = deny
    os.system = os.popen = os.kill = deny
    logger = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None)
    sys.modules["loguru"] = types.SimpleNamespace(logger=logger)
    sys.modules["config"] = types.SimpleNamespace(settings=types.SimpleNamespace(), get_settings=deny)
    sys.modules["db"] = types.SimpleNamespace(get_engine=deny)
    sys.modules["sqlalchemy"] = types.SimpleNamespace(create_engine=deny)
    alerts = types.ModuleType("alerts")
    alerts.__path__ = []
    email = types.ModuleType("alerts.email")
    email.send_alert = deny
    alerts.email = email
    sys.modules.update({"alerts": alerts, "alerts.email": email})


    def load(name: str, relative: str) -> types.ModuleType:
        spec = importlib.util.spec_from_file_location(name, ROOT / relative)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


    for name in ("analysis", "intelligence"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    Q = load("analysis.feed_reliability_projection", "analysis/feed_reliability_projection.py")
    N = load("analysis.etf_price_features", "analysis/etf_price_features.py")
    T = load("analysis.temporal_feature_eligibility", "analysis/temporal_feature_eligibility.py")
    F = load("intelligence.fleet_heartbeat", "intelligence/fleet_heartbeat.py")

    START = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
    CUTOFF = START + timedelta(minutes=5)
    US = timedelta(microseconds=1)
    IDENTITY = Q.SourceIdentity("TEST:SPY", "synthetic-bars", "fixture", "ohlcv", "v1")


    def ref(name: str = "proof:1", at: datetime = START - timedelta(minutes=1)) -> Q.FactReference:
        return Q.FactReference(name, "a" * 64, at, at)


    def inputs() -> tuple:
        """Two complete, unequal intervals, exact trade notionals and explicit basis."""
        basis = N.PriceBasis("USD", "shares", "USD", N.Adjustment.RAW, "synthetic-raw", "v1", ref(), N.NotionalBasis.TRADE)
        session = N.SessionWindow("TEST:SPY", "TEST:X", "synthetic:day", "UTC", START, CUTOFF,
                                  "synthetic-calendar", "v1", ref(), N.SessionConvention.REGULAR,
                                  (N.BarInterval(START, CUTOFF),))
        bars, intervals = [], []
        cursor = START
        for i, (seconds, high, low, close, volume, notional) in enumerate(((120, "12", "8", "10", 2, "20"), (180, "15", "9", "12", 3, "39"))):
            end = cursor + timedelta(seconds=seconds)
            observation = Q.Observation(IDENTITY, f"bar:{i}", "b" * 64, cursor,
                                        end, cursor, end, end, Q.Presence.PRESENT,
                                        delay=Q.DelayEvidence(IDENTITY, 0, ref()),
                                        interval_seconds=seconds, finality=Q.Finality.FINAL,
                                        revision_id=f"revision:{i}", coverage_fraction=1,
                                        continuous=True, lifecycle=Q.LifecycleFacts(True, "generation:1", "generation:1", ref()), is_fallback=False)
            bars.append(N.NumericBar(observation, cursor, end, N.TimestampConvention.START, session, basis,
                                     Decimal(close), Decimal(high), Decimal(low), Decimal(close), volume, Decimal(notional), ref()))
            intervals.append(N.BarInterval(cursor, end))
            cursor = end
        bars = tuple(bars)
        grid = N.ExpectedBarGrid(START, CUTOFF, session, basis, tuple(intervals), ref("grid:1"))
        required = (Q.Check.FINALITY, Q.Check.INTERVAL, Q.Check.COVERAGE, Q.Check.CONTINUITY,
                    Q.Check.SESSION, Q.Check.DELAY, Q.Check.EVENT_AGE, Q.Check.RECEIPT_AGE)
        capabilities = tuple(Q.CapabilityPolicy(f"bar-quality:{i}", b.observation.identity, b.observation.evidence_id,
                                                Q.Mode.RESEARCH, required, expected_evidence_sha256=b.observation.evidence_sha256,
                                                max_event_age_seconds=300, max_receipt_age_seconds=300,
                                                max_future_skew_seconds=0, max_delay_seconds=0,
                                                required_interval_seconds=b.observation.interval_seconds,
                                                min_coverage_fraction=1, allowed_session_states=("open",))
                             for i, b in enumerate(bars))
        policy = Q.EvaluationPolicy("composition-quality-v1", {"code_sha256": "c" * 64, "config_sha256": "d" * 64}, capabilities)
        sessions = (Q.SessionFacts("TEST:SPY", "open", ref("session:1")),)
        return bars, grid, policy, sessions


    def feature(function: object, bars: tuple, grid: N.ExpectedBarGrid, policy: Q.EvaluationPolicy, sessions: tuple, **changes: object) -> N.PriceFeatureResult:
        options = dict(cutoff=CUTOFF, quality_policy=policy, evaluation_at=CUTOFF, decision_at=CUTOFF, session_facts=sessions)
        if function is N.opening_range:
            options["minutes"] = 5
        options.update(changes)
        return function(bars, grid, **options)


    def quality(bars: tuple, policy: Q.EvaluationPolicy, sessions: tuple) -> tuple:
        result = Q.evaluate_feed_quality(tuple(b.observation for b in bars), policy, now=CUTOFF, decision_at=CUTOFF, sessions=sessions)
        return tuple(cap for row in result.instruments for cap in row.capabilities)


    def temporal_row() -> tuple:
        key = T.FeatureKey("synthetic-diagnostic", "synthetic-bars", "observation:1")
        row = T.Candidate("row:1", key, "v1", T.SourceKind.OBSERVATION, START, CUTOFF, CUTOFF, CUTOFF,
                          None, b'{"true":true,"one":1,"missing":null}', b'{"source":"caller-declared"}')
        policy = T.TemporalPolicy("temporal-v1", key.feature_id, key.source_id, timedelta(minutes=5),
                                  ("observation", "release", "available_at", "vintage"), "knowledge_time", False, None)
        return row, policy


    class FakeEngine:
        """SQL-shaped in-memory receipt sink; no database API is called."""
        def __init__(self) -> None:
            self.records = []
            self.row = None

        def begin(self): return self
        def connect(self): return self
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def fetchone(self): return self.row

        def exec_driver_sql(self, sql, params=None):
            self.row = None
            if sql.lstrip().startswith("CREATE "): pass
            elif sql.startswith("INSERT INTO fleet_heartbeats"): self.records.append(params)
            elif sql.startswith("SELECT status,"): self.row = ("down", 1, None)
            elif sql.startswith("SELECT COUNT(*)"): self.row = (0,)
            elif sql.startswith(("SELECT EXTRACT", "SELECT heal_detail,", "SELECT 1 FROM")): pass
            else: raise AssertionError("unexpected fake SQL")
            return self


    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return CUTOFF if tz is None else CUTOFF.astimezone(tz)


    class CompositionTests(unittest.TestCase):
        def assert_withheld(self, result: N.PriceFeatureResult) -> None:
            self.assertIsNone(result.high)
            self.assertIsNone(result.low)
            self.assertIsNone(result.vwap)

        def test_public_import_identity_and_effect_fence_controls(self):
            self.assertIs(N.Observation, Q.Observation)
            self.assertIs(N.State, Q.State)
            for module in (Q, N, T, F):
                self.assertEqual(Path(module.__file__).resolve().parents[1], ROOT)
            for operation, args in ((subprocess.run, (["never-executed"],)), (socket.create_connection, (("example.invalid", 1),)),
                                    (sys.modules["db"].get_engine, ()), (sys.modules["alerts.email"].send_alert, ("x", "x"))):
                with self.assertRaisesRegex(AssertionError, "fenced"):
                    operation(*args)
            with self.assertRaisesRegex(AssertionError, "fenced"):
                sys.audit("socket.connect", None, ("example.invalid", 1))

        def test_exact_joint_positive_and_original_metadata(self):
            bars, grid, policy, sessions = inputs()
            self.assertEqual([c.state for c in quality(bars, policy, sessions)], [Q.State.PASS] * 2)
            self.assertTrue(all(N.validate_numeric_bar(b).state is Q.State.PASS for b in bars))
            self.assertIs(N.validate_bar_grid(bars, grid).state, Q.State.PASS)
            opening = feature(N.opening_range, bars, grid, policy, sessions)
            trade = feature(N.trade_vwap, bars, grid, policy, sessions)
            self.assertEqual((opening.high, opening.low), (Decimal("15"), Decimal("8")))
            self.assertEqual(trade.vwap, Fraction(59, 5))
            self.assertIs(trade.inputs, bars)
            self.assertIs(trade.inputs[0].observation, bars[0].observation)
            self.assertIs(trade.quality_policy, policy)
            self.assertIs(trade.grid, grid)
            self.assertIs(trade.session_facts, sessions)
            self.assertEqual(trade.method, N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
            self.assertEqual((trade.cutoff, trade.evaluation_at, trade.decision_at), (CUTOFF,) * 3)

        def test_proxy_requires_distinct_basis_and_method(self):
            bars, grid, policy, sessions = inputs()
            wrong = feature(N.ohlcv_hlc3_volume_proxy, bars, grid, policy, sessions)
            self.assertIs(wrong.basis.state, Q.State.FAIL)
            self.assert_withheld(wrong)
            proxy_basis = replace(grid.basis, notional_basis=N.NotionalBasis.OHLCV_PROXY)
            proxy_bars = tuple(replace(b, basis=proxy_basis) for b in bars)
            proxy_grid = replace(grid, basis=proxy_basis)
            result = feature(N.ohlcv_hlc3_volume_proxy, proxy_bars, proxy_grid, policy, sessions)
            self.assertEqual(result.vwap, Fraction(56, 5))
            self.assertIs(result.method, N.PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY)
            self.assertNotEqual(result.vwap, feature(N.trade_vwap, bars, grid, policy, sessions).vwap)
            self.assert_withheld(feature(N.trade_vwap, proxy_bars, proxy_grid, policy, sessions))

        def test_quality_fail_unknown_do_not_promote_numeric_structure(self):
            bars, grid, policy, sessions = inputs()
            for changes, expected in (({"presence": Q.Presence.MISSING}, Q.State.UNKNOWN),
                                      ({"presence": Q.Presence.UNAVAILABLE}, Q.State.FAIL),
                                      ({"finality": Q.Finality.PROVISIONAL}, Q.State.FAIL),
                                      ({"recorded_at": None}, Q.State.UNKNOWN)):
                altered = (replace(bars[0], observation=replace(bars[0].observation, **changes)), bars[1])
                self.assertIs(N.validate_numeric_bar(altered[0]).state, Q.State.PASS)
                self.assertIs(quality(altered, policy, sessions)[0].state, expected)
                result = feature(N.trade_vwap, altered, grid, policy, sessions)
                self.assertIs(result.temporal.state, expected)
                self.assert_withheld(result)

        def test_observation_and_ancillary_cutoff_microsecond(self):
            bars, grid, policy, sessions = inputs()
            for role in ("receipt_at", "available_at", "recorded_at"):
                altered = (replace(bars[0], observation=replace(bars[0].observation, **{role: CUTOFF + US})), bars[1])
                result = feature(N.trade_vwap, altered, grid, policy, sessions)
                self.assertIs(result.temporal.state, Q.State.FAIL)
                self.assert_withheld(result)
            for role in ("available_at", "recorded_at"):
                late_ref = replace(ref(), **{role: CUTOFF + US})
                altered = (replace(bars[0], notional_reference=late_ref), bars[1])
                result = feature(N.trade_vwap, altered, grid, policy, sessions)
                self.assertIs(result.temporal.state, Q.State.FAIL)
                self.assert_withheld(result)
            result = feature(N.opening_range, bars, grid, policy, sessions, cutoff=CUTOFF - US, decision_at=CUTOFF - US)
            self.assertIn("window_not_closed_at_cutoff", result.temporal.reasons)
            self.assert_withheld(result)

        def test_duration_and_opening_window_are_exact_shared_boundaries(self):
            bars, grid, policy, sessions = inputs()
            altered = (replace(bars[0], observation=replace(bars[0].observation, interval_seconds=120.000001)), bars[1])
            self.assertIs(N.validate_numeric_bar(altered[0]).state, Q.State.FAIL)
            result = feature(N.opening_range, altered, grid, policy, sessions)
            self.assertIs(result.coverage.state, Q.State.FAIL)
            self.assert_withheld(result)
            result = feature(N.opening_range, bars, grid, policy, sessions, minutes=15)
            self.assertIn("opening_window_duration_mismatch", result.coverage.reasons)
            self.assert_withheld(result)
            result = feature(N.trade_vwap, bars[:1], grid, policy, sessions)
            self.assertIs(result.coverage.state, Q.State.FAIL)
            self.assert_withheld(result)

        def test_identity_hash_session_and_quality_policy_are_bound(self):
            bars, grid, policy, sessions = inputs()
            for changes in ({"identity": replace(IDENTITY, source="foreign-source")}, {"evidence_sha256": "e" * 64}):
                observation = replace(bars[0].observation, **changes)
                if "identity" in changes:
                    # Keep delay evidence self-consistent so only the expected-source
                    # policy guard decides this foreign-identity counterexample.
                    observation = replace(observation, delay=replace(observation.delay, scope=observation.identity))
                altered = (replace(bars[0], observation=observation), bars[1])
                result = feature(N.trade_vwap, altered, grid, policy, sessions)
                self.assertIs(result.temporal.state, Q.State.FAIL)
                self.assert_withheld(result)
            result = feature(N.trade_vwap, bars, grid, policy, ())
            self.assertIs(result.temporal.state, Q.State.UNKNOWN)
            self.assert_withheld(result)
            missing_finality = replace(policy, capabilities=tuple(replace(c, required_checks=tuple(x for x in c.required_checks if x is not Q.Check.FINALITY)) for c in policy.capabilities))
            result = feature(N.trade_vwap, bars, grid, missing_finality, sessions)
            self.assertIn("explicit_finality_check_required", result.temporal.reasons)
            self.assert_withheld(result)

        def test_time_roles_are_explicit_and_no_late_revision_repair(self):
            bars, grid, policy, sessions = inputs()
            result = feature(N.trade_vwap, bars, grid, policy, sessions, decision_at=CUTOFF - US)
            self.assertIn("decision_cutoff_mismatch", result.temporal.reasons)
            self.assert_withheld(result)
            result = feature(N.trade_vwap, bars, grid, policy, sessions, evaluation_at=CUTOFF + US)
            self.assertIs(result.temporal.state, Q.State.PASS)  # research retains decision-time age
            self.assertEqual(result.vwap, Fraction(59, 5))
            current_policy = replace(policy, capabilities=tuple(replace(c, mode=Q.Mode.CURRENT) for c in policy.capabilities))
            self.assertEqual(feature(N.trade_vwap, bars, grid, current_policy, sessions).vwap, Fraction(59, 5))
            current = feature(N.trade_vwap, bars, grid, current_policy, sessions, evaluation_at=CUTOFF + US)
            self.assertIs(current.temporal.state, Q.State.FAIL)  # exact evaluation-time 300-second boundary
            self.assert_withheld(current)
            late = replace(bars[0].observation, revision_id="late:revision", revises_evidence_id="older:bar", recorded_at=CUTOFF + US)
            altered = (replace(bars[0], observation=late), bars[1])
            result = feature(N.trade_vwap, altered, grid, policy, sessions)
            self.assertIs(result.inputs[0].observation, late)
            self.assert_withheld(result)

        def test_temporal_is_separate_preserves_bytes_and_typed_policy(self):
            bars, grid, quality_policy, sessions = inputs()
            price = feature(N.trade_vwap, bars, grid, quality_policy, sessions)
            row, policy = temporal_row()
            diagnostic = T.classify(row, cutoff=CUTOFF, policy=policy)
            self.assertIs(diagnostic.status, T.Status.CURRENT)
            self.assertIs(diagnostic.row, row)
            self.assertIs(diagnostic.row.payload_json, row.payload_json)
            self.assertIs(T.classify(row, cutoff=CUTOFF + US, policy=policy).status, T.Status.STALE)
            for value in (None, 300, True, float("nan")):
                self.assertIs(T.classify(row, cutoff=CUTOFF, policy=replace(policy, max_age=value)).status, T.Status.UNKNOWN)
            self.assertIs(T.classify(row, cutoff=CUTOFF, policy=replace(policy, source_id="foreign")).status, T.Status.UNKNOWN)
            self.assertIs(T.classify(row, cutoff=CUTOFF, policy=None).status, T.Status.UNKNOWN)
            self.assertEqual(price.vwap, Fraction(59, 5))  # temporal diagnostic cannot authorize or rewrite price result

        def test_temporal_selection_knowledge_and_future_lanes(self):
            row, policy = temporal_row()
            late = replace(row, row_id="row:2", vintage=CUTOFF + US, payload_json=b'{"one":2}')
            selection = T.select_latest_as_of((late, row), key=row.key, cutoff=CUTOFF, policy=policy,
                                             selection_policy=T.SelectionPolicy("selection-v1", "LATEST_AS_OF"))
            self.assertIs(selection.current, row)
            self.assertEqual(selection.history, (late, row))
            self.assertIs(selection.diagnostics[1].status, T.Status.UNKNOWN)
            for role in ("release", "available_at", "vintage"):
                self.assertIs(T.classify(replace(row, **{role: CUTOFF + US}), cutoff=CUTOFF, policy=policy).status, T.Status.UNKNOWN)
            future = replace(row, kind=T.SourceKind.PRECOMPUTED_EPHEMERIS, observation=CUTOFF + timedelta(minutes=1), computed_at=START)
            permitted = replace(policy, permit_precomputed_ephemeris=True, future_horizon=timedelta(minutes=1))
            self.assertIs(T.classify(future, cutoff=CUTOFF, policy=permitted).status, T.Status.KNOWN_FUTURE)
            self.assertIs(T.classify(replace(future, observation=future.observation + US), cutoff=CUTOFF, policy=permitted).status, T.Status.UNKNOWN)
            selected = T.select_latest_as_of((future,), key=row.key, cutoff=CUTOFF, policy=permitted,
                                            selection_policy=T.SelectionPolicy("selection-v1", "LATEST_AS_OF"))
            self.assertIsNone(selected.current)
            self.assertEqual(selected.known_future, (future,))

        def test_temporal_support_clocks_do_not_use_later_current_leaf(self):
            row, policy = temporal_row()
            key = T.FeatureKey("synthetic-score-diagnostic", "synthetic-model", "score:1")
            score_policy = replace(policy, feature_id=key.feature_id, source_id=key.source_id)
            lineage = T.Lineage("synthetic-model", "v1", ("leaf:1",), (T.Component("leaf:1", row, policy),))
            score = replace(row, row_id="score:1", key=key, kind=T.SourceKind.DERIVED_SCORE, computed_at=CUTOFF, lineage=lineage)
            self.assertIs(T.classify(score, cutoff=CUTOFF, policy=score_policy).status, T.Status.CURRENT)
            for role in ("release", "available_at", "vintage"):
                leaf = replace(row, release=START + timedelta(minutes=1), available_at=START + timedelta(minutes=1), vintage=START + timedelta(minutes=1))
                leaf = replace(leaf, **{role: CUTOFF})
                altered = replace(score, computed_at=CUTOFF - US, lineage=replace(lineage, components=(T.Component("leaf:1", leaf, policy),)))
                result = T.classify(altered, cutoff=CUTOFF, policy=score_policy)
                self.assertIs(result.status, T.Status.UNKNOWN)
                self.assertEqual(result.reason, "support_unknown_at_computation")
                self.assertIs(result.component_diagnostics[0].status, T.Status.CURRENT)

        def test_fleet_default_and_explicit_primary_guard_with_joint_imports(self):
            commands, emails = [], []
            def command(*args, **kwargs):
                commands.append((args, kwargs))
                return 0, "synthetic only"
            with patch.multiple(F, datetime=FixedDatetime, probe_nodes=lambda: [], probe_units=lambda: [], probe_endpoints=lambda: [], probe_freshness=lambda: [],
                                probe_gpus=lambda: [("grid-svr:gpu", "gpu", "down", "synthetic GPU error", 17)],
                                _run=command, _ssh=command, _email=lambda *args: emails.append(args)):
                engine = FakeEngine()
                summary = F.run_fleet_heartbeat(engine)
                self.assertEqual((summary["checked"], summary["alerts"], summary["heals"]), (1, 1, 0))
                self.assertEqual(commands, [])
                self.assertEqual(engine.records[0][5:7], (2, False))
                explicit = FakeEngine()
                summary = F.run_fleet_heartbeat(explicit, do_heal=True)
                self.assertEqual(summary["heals"], 0)
                self.assertEqual(commands, [])
                self.assertIn("requires operator handling", explicit.records[0][7])
                self.assertEqual(len(emails), 2)
                # Positive control: the fake command collector detects a permitted remote heal.
                F.probe_gpus = lambda: [("redbox:gpu", "gpu", "down", "synthetic remote", 17)]
                default_remote = F.run_fleet_heartbeat(FakeEngine())
                self.assertEqual(default_remote["heals"], 0)
                self.assertEqual(commands, [])
                remote = F.run_fleet_heartbeat(FakeEngine(), do_heal=True)
                self.assertEqual(remote["heals"], 1)
                self.assertTrue(commands)
                bars, grid, policy, sessions = inputs()
                self.assertEqual(feature(N.trade_vwap, bars, grid, policy, sessions).vwap, Fraction(59, 5))

    if example:
        document = (ROOT / "docs/accepted_market_contract_composition.md").read_text()
        code = document.split("```python\n", 1)[1].split("```", 1)[0]
        exec(compile(code, str(ROOT / "docs/accepted_market_contract_composition.md"), "exec"), dict(locals()))
        return {"example_pass": True, "examples_run": 1}
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CompositionTests)
    method_names = tuple(test._testMethodName for test in suite)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return {
        "tests_run": result.testsRun,
        "method_names": method_names,
        "failures": {test._testMethodName: detail for test, detail in result.failures},
        "errors": {test._testMethodName: detail for test, detail in result.errors},
        "successful": result.wasSuccessful(),
    }


class CompositionTests(unittest.TestCase):
    """Collection-safe wrappers; the twelve original functional bodies run once."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.child = None
        cls.receipt = None
        cls.launch_error = None
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        try:
            cls.child = subprocess.run(
                [sys.executable, "-B", str(Path(__file__).resolve()), "--isolated-child"],
                capture_output=True, text=True, timeout=180, env=environment,
            )
            receipt = next(
                (line[len(_RECEIPT_MARKER):] for line in cls.child.stdout.splitlines()
                 if line.startswith(_RECEIPT_MARKER)),
                None,
            )
            cls.receipt = json.loads(receipt) if receipt is not None else None
        except (OSError, subprocess.SubprocessError, AssertionError, json.JSONDecodeError) as exc:
            cls.launch_error = str(exc)

    def _assert_child_method(self, name: str) -> None:
        detail = self.launch_error or (self.child.stdout + self.child.stderr if self.child is not None else "no child")
        self.assertIsNone(self.launch_error, detail)
        self.assertEqual(self.child.returncode, 0, detail)
        self.assertIsInstance(self.receipt, dict, detail)
        self.assertIs(self.receipt.get("successful"), True, detail)
        self.assertEqual(self.receipt.get("tests_run"), 12, detail)
        names = self.receipt.get("method_names")
        self.assertIsInstance(names, list, detail)
        self.assertTrue(all(type(item) is str for item in names), detail)
        self.assertEqual(len(names), 12, detail)
        self.assertEqual(len(set(names)), 12, detail)
        self.assertEqual(set(names), set(_EXPECTED_METHODS), detail)
        self.assertIn(name, names, detail)
        self.assertIsInstance(self.receipt.get("failures"), dict, detail)
        self.assertIsInstance(self.receipt.get("errors"), dict, detail)
        self.assertEqual(self.receipt["failures"], {}, detail)
        self.assertEqual(self.receipt["errors"], {}, detail)

    def test_public_import_identity_and_effect_fence_controls(self) -> None:
        self._assert_child_method("test_public_import_identity_and_effect_fence_controls")

    def test_exact_joint_positive_and_original_metadata(self) -> None:
        self._assert_child_method("test_exact_joint_positive_and_original_metadata")

    def test_proxy_requires_distinct_basis_and_method(self) -> None:
        self._assert_child_method("test_proxy_requires_distinct_basis_and_method")

    def test_quality_fail_unknown_do_not_promote_numeric_structure(self) -> None:
        self._assert_child_method("test_quality_fail_unknown_do_not_promote_numeric_structure")

    def test_observation_and_ancillary_cutoff_microsecond(self) -> None:
        self._assert_child_method("test_observation_and_ancillary_cutoff_microsecond")

    def test_duration_and_opening_window_are_exact_shared_boundaries(self) -> None:
        self._assert_child_method("test_duration_and_opening_window_are_exact_shared_boundaries")

    def test_identity_hash_session_and_quality_policy_are_bound(self) -> None:
        self._assert_child_method("test_identity_hash_session_and_quality_policy_are_bound")

    def test_time_roles_are_explicit_and_no_late_revision_repair(self) -> None:
        self._assert_child_method("test_time_roles_are_explicit_and_no_late_revision_repair")

    def test_temporal_is_separate_preserves_bytes_and_typed_policy(self) -> None:
        self._assert_child_method("test_temporal_is_separate_preserves_bytes_and_typed_policy")

    def test_temporal_selection_knowledge_and_future_lanes(self) -> None:
        self._assert_child_method("test_temporal_selection_knowledge_and_future_lanes")

    def test_temporal_support_clocks_do_not_use_later_current_leaf(self) -> None:
        self._assert_child_method("test_temporal_support_clocks_do_not_use_later_current_leaf")

    def test_fleet_default_and_explicit_primary_guard_with_joint_imports(self) -> None:
        self._assert_child_method("test_fleet_default_and_explicit_primary_guard_with_joint_imports")


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ("--isolated-child", "--isolated-example"):
        payload = _isolated_payload(example=sys.argv[1] == "--isolated-example")
        print(_RECEIPT_MARKER + json.dumps(payload, sort_keys=True))
        raise SystemExit(0 if payload.get("successful", payload.get("example_pass", False)) else 1)
    unittest.main(verbosity=2)
