"""Standalone literal synthetic oracles; no accepted ETF or GRID runtime dependency."""

import ast
import importlib.util
import json
import sys
import unittest
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


SOURCE = (
    Path(__file__).resolve().parents[1] / "analysis" / "feed_reliability_projection.py"
)
SPEC = importlib.util.spec_from_file_location("feed_quality_under_test", SOURCE)
F = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = F
SPEC.loader.exec_module(F)

NOW = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
PINS = {
    "code_sha256": "a" * 64,
    "config_sha256": "b" * 64,
    "calendar_sha256": "c" * 64,
    "outcome_definition_sha256": "d" * 64,
}
CONTROL_IDS = []


def identity(
    instrument="TEST:SPY", source="synthetic-price", tool="quote", field="price_context"
):
    return F.SourceIdentity(instrument, source, tool, field, "synthetic-v1")


def reference(at=NOW - timedelta(seconds=1)):
    return F.FactReference("proof:001", "e" * 64, at, at)


def observation(instrument="TEST:SPY"):
    ident = identity(instrument)
    return F.Observation(
        ident,
        "quote:001",
        "f" * 64,
        NOW - timedelta(seconds=10),
        NOW - timedelta(seconds=1),
        NOW - timedelta(seconds=5),
        NOW - timedelta(seconds=2),
        NOW - timedelta(seconds=1),
        F.Presence.PRESENT,
        F.DelayEvidence(ident, 0, reference()),
        60,
        F.Finality.FINAL,
        "revision:0",
        None,
        1,
        True,
        F.LifecycleFacts(True, "generation:1", "generation:1", reference()),
        None,
        False,
    )


def capability(obs=None, name="price_context", mode=F.Mode.RESEARCH, required=None):
    obs = observation() if obs is None else obs
    return F.CapabilityPolicy(
        name,
        obs.identity,
        obs.evidence_id,
        mode,
        (F.Check.EVENT_AGE, F.Check.RECEIPT_AGE, F.Check.DELAY)
        if required is None
        else required,
        expected_evidence_sha256=obs.evidence_sha256,
        max_event_age_seconds=60,
        max_receipt_age_seconds=5,
        max_future_skew_seconds=0,
        max_delay_seconds=0,
        required_interval_seconds=60,
        min_coverage_fraction=1,
        max_oi_age_seconds=86400,
        allowed_session_states=("open",),
    )


def policy(*caps):
    return F.EvaluationPolicy(
        "synthetic-policy-v1", PINS, caps or (capability(),), PINS
    )


def evaluate(observations=None, pol=None, now=NOW, decision=NOW, sessions=()):
    return F.evaluate_feed_quality(
        (observation(),) if observations is None else observations,
        policy() if pol is None else pol,
        now=now,
        decision_at=decision,
        sessions=sessions,
    )


def row(result, instrument="TEST:SPY", name="price_context"):
    return next(
        c
        for i in result.instruments
        if i.instrument == instrument
        for c in i.capabilities
        if c.name == name
    )


def check(result, which, instrument="TEST:SPY", name="price_context"):
    return next(c for c in row(result, instrument, name).checks if c.check is which)


class FeedQualityTests(unittest.TestCase):
    @contextmanager
    def control(self, label):
        CONTROL_IDS.append(label)
        with self.subTest(control=label):
            yield

    @classmethod
    def tearDownClass(cls):
        print(
            json.dumps(
                {
                    "synthetic_control_count": len(CONTROL_IDS),
                    "control_ids": CONTROL_IDS,
                }
            )
        )

    def test_F01_old_event_fresh_receipt(self):
        with self.control("F01-literal-960-event-1-receipt"):
            obs = replace(observation(), event_at=NOW - timedelta(seconds=960))
            result = evaluate((obs,))
            self.assertEqual(check(result, F.Check.EVENT_AGE).state, F.State.FAIL)
            self.assertEqual(check(result, F.Check.RECEIPT_AGE).state, F.State.PASS)
            self.assertFalse(row(result).eligible)
            context = result.instruments[0].context[0]
            self.assertTrue(context.visible)
            self.assertEqual(context.event_age_seconds, 960)
            self.assertEqual(context.receipt_age_seconds, 1)

    def test_F02_event_missing_is_unknown(self):
        with self.control("F02-missing-event-never-receipt-substitution"):
            result = evaluate((replace(observation(), event_at=None),))
            self.assertEqual(check(result, F.Check.EVENT_AGE).state, F.State.UNKNOWN)
            self.assertFalse(row(result).eligible)
            self.assertIsNone(result.instruments[0].context[0].event_age_seconds)
            self.assertEqual(result.instruments[0].context[0].receipt_age_seconds, 1)
        with self.control("F02-receipt-only-does-not-require-event"):
            obs = replace(observation(), event_at=None)
            result = evaluate(
                (obs,), policy(capability(obs, required=(F.Check.RECEIPT_AGE,)))
            )
            self.assertTrue(row(result).eligible)
            self.assertEqual(
                check(result, F.Check.EVENT_AGE).state, F.State.NOT_REQUIRED
            )

    def test_F03_tradingview_screener_delay_is_exactly_scoped(self):
        screener_id = identity(source="TradingView", tool="screener", field="close")
        screen = replace(
            observation(),
            identity=screener_id,
            event_at=NOW - timedelta(seconds=960),
            delay=F.DelayEvidence(screener_id, 900, reference()),
        )
        with self.control("F03-screen-900-labeled-delayed-visible"):
            result = evaluate((screen,), policy(capability(screen)))
            self.assertEqual(check(result, F.Check.DELAY).state, F.State.FAIL)
            context = result.instruments[0].context[0]
            self.assertTrue(context.visible)
            self.assertEqual(context.delay_label, "declared_delayed")
            self.assertEqual(context.observation.delay.seconds, 900)
        ohlcv_id = identity(source="TradingView", tool="ohlcv", field="bar")
        bar = replace(
            observation(), identity=ohlcv_id, delay=None, finality=F.Finality.UNKNOWN
        )
        with self.control("F03-ohlcv-delay-and-finality-unknown"):
            result = evaluate(
                (bar,),
                policy(capability(bar, required=(F.Check.DELAY, F.Check.FINALITY))),
            )
            self.assertEqual(check(result, F.Check.DELAY).state, F.State.UNKNOWN)
            self.assertEqual(check(result, F.Check.FINALITY).state, F.State.UNKNOWN)
            self.assertEqual(
                result.instruments[0].context[0].delay_label, "unknown_delay"
            )
        for changed in (
            ohlcv_id,
            replace(screener_id, field="volume"),
            replace(screener_id, source="ToS"),
            replace(screener_id, adapter_version="v2"),
            replace(screener_id, instrument="TEST:QQQ"),
        ):
            with self.control("F03-cross-scope-" + str(len(CONTROL_IDS))):
                obs = replace(screen, delay=replace(screen.delay, scope=changed))
                result = evaluate((obs,), policy(capability(screen)))
                self.assertEqual(check(result, F.Check.DELAY).state, F.State.FAIL)
                self.assertEqual(
                    check(result, F.Check.DELAY).reason, "delay_scope_mismatch"
                )
                self.assertEqual(
                    result.instruments[0].context[0].delay_label,
                    "unverified_delay_scope",
                )

    def test_F04_finality_only_blocks_dependent_capability(self):
        for finality, expected in (
            (F.Finality.PROVISIONAL, F.State.FAIL),
            (F.Finality.INCOMPLETE, F.State.FAIL),
            (F.Finality.UNKNOWN, F.State.UNKNOWN),
            (F.Finality.FINAL, F.State.PASS),
        ):
            with self.control("F04-finality-" + finality.value):
                obs = replace(observation(), finality=finality)
                pol = policy(
                    capability(obs),
                    capability(obs, "confirmation", required=(F.Check.FINALITY,)),
                )
                result = evaluate((obs,), pol)
                self.assertTrue(row(result).eligible)
                self.assertEqual(
                    check(result, F.Check.FINALITY, name="confirmation").state, expected
                )
                self.assertEqual(
                    row(result, name="confirmation").eligible, expected is F.State.PASS
                )
                self.assertEqual(
                    result.instruments[0].context[0].observation.finality, finality
                )

    def test_F05_instrument_absence_and_unavailability_are_independent(self):
        spy, qqq = observation(), observation("TEST:QQQ")
        pol = policy(capability(spy), capability(qqq))
        for supplied, expected in (
            ((), F.State.UNKNOWN),
            ((replace(qqq, presence=F.Presence.MISSING),), F.State.UNKNOWN),
            ((replace(qqq, presence=F.Presence.UNAVAILABLE),), F.State.FAIL),
            ((qqq,), F.State.PASS),
        ):
            with self.control(
                "F05-independent-" + expected.value + "-" + str(len(supplied))
            ):
                result = evaluate((spy,) + supplied, pol)
                self.assertTrue(row(result).eligible)
                self.assertEqual(row(result, "TEST:QQQ").state, expected)
                self.assertEqual(
                    row(result, "TEST:QQQ").eligible, expected is F.State.PASS
                )
                self.assertEqual(len(result.instruments), 2)

    def test_F06_options_unknowns_do_not_veto_underlying(self):
        req = (F.Check.OI_VINTAGE, F.Check.EXPIRY_SETTLEMENT, F.Check.GREEKS)
        for options in (
            None,
            F.OptionsFacts(None, None, None, reference()),
            F.OptionsFacts(NOW - timedelta(hours=1), True, None, reference()),
        ):
            with self.control("F06-options-dependent-unknown-" + str(len(CONTROL_IDS))):
                obs = replace(observation(), options=options)
                result = evaluate(
                    (obs,),
                    policy(capability(obs), capability(obs, "options", required=req)),
                )
                self.assertTrue(row(result).eligible)
                self.assertFalse(row(result, name="options").eligible)
                self.assertEqual(
                    check(result, F.Check.OI_VINTAGE).state, F.State.NOT_REQUIRED
                )
        with self.control("F06-options-explicit-proof-pass-0dte-held"):
            obs = replace(
                observation(),
                options=F.OptionsFacts(
                    NOW - timedelta(hours=1), True, True, reference()
                ),
            )
            result = evaluate(
                (obs,),
                policy(
                    capability(obs, "options", required=req),
                    capability(obs, "0dte", mode=F.Mode.ZERO_DTE, required=req),
                ),
            )
            self.assertTrue(row(result, name="options").eligible)
            self.assertFalse(row(result, name="0dte").eligible)
            self.assertEqual(
                check(result, F.Check.ZERO_DTE_HOLD, name="0dte").reason,
                "zero_dte_held",
            )
        with self.control("F06-gex-unverified-event-and-oi"):
            obs = replace(
                observation(),
                identity=identity(source="GEX", tool="snapshot"),
                event_at=None,
                options=F.OptionsFacts(None, None, None, reference()),
            )
            result = evaluate(
                (obs,),
                policy(
                    capability(obs, required=(F.Check.EVENT_AGE, F.Check.OI_VINTAGE))
                ),
            )
            self.assertEqual(check(result, F.Check.EVENT_AGE).state, F.State.UNKNOWN)
            self.assertEqual(check(result, F.Check.OI_VINTAGE).state, F.State.UNKNOWN)

    def test_F07_duplicates_revisions_and_out_of_order_are_retained(self):
        old = replace(observation(), event_at=NOW - timedelta(seconds=960))
        rereceived = replace(
            old, evidence_id="quote:retry", receipt_at=NOW, recorded_at=NOW
        )
        with self.control("F07-rereceipt-same-hash-economic-age-960"):
            result = evaluate((old, rereceived), policy(capability(rereceived)))
            self.assertFalse(row(result).eligible)
            self.assertEqual(
                [c.event_age_seconds for c in result.instruments[0].context], [960, 960]
            )
            self.assertEqual(
                [c.receipt_age_seconds for c in result.instruments[0].context], [1, 0]
            )
        late = replace(
            observation(),
            evidence_id="quote:revision",
            evidence_sha256="1" * 64,
            revision_id="revision:1",
            revises_evidence_id="quote:001",
            available_at=NOW + timedelta(seconds=1),
            receipt_at=NOW + timedelta(seconds=2),
            recorded_at=NOW + timedelta(seconds=2),
        )
        for sequence in ((observation(), late), (late, observation())):
            with self.control("F07-postcutoff-revision-" + str(len(CONTROL_IDS))):
                result = evaluate(
                    sequence,
                    policy(capability(), capability(late, "late")),
                    now=NOW + timedelta(minutes=10),
                )
                self.assertTrue(row(result).eligible)
                self.assertEqual(
                    check(result, F.Check.CUTOFF, name="late").state, F.State.FAIL
                )
                self.assertFalse(row(result, name="late").eligible)
                self.assertEqual(
                    tuple(c.observation for c in result.instruments[0].context),
                    sequence,
                )
        for sequence in (
            (observation(), observation()),
            (observation(), replace(observation(), evidence_sha256="2" * 64)),
        ):
            with self.control(
                "F07-duplicate-selected-id-ambiguous-" + str(len(CONTROL_IDS))
            ):
                result = evaluate(sequence)
                self.assertFalse(row(result).eligible)
                self.assertEqual(
                    check(result, F.Check.EVIDENCE).reason,
                    "ambiguous_evidence_identity",
                )
        with self.control("F07-newer-arrival-older-event-no-auto-selection"):
            obs = replace(
                old, evidence_id="quote:older-event", receipt_at=NOW, recorded_at=NOW
            )
            result = evaluate((observation(), obs))
            self.assertTrue(row(result).eligible)
            self.assertEqual(result.instruments[0].context[1].event_age_seconds, 960)

    def test_F08_literal_age_and_delay_boundaries(self):
        for age, expected in (
            (59.999999, F.State.PASS),
            (60, F.State.PASS),
            (60.000001, F.State.FAIL),
        ):
            with self.control("F08-event-endpoint-" + str(age)):
                obs = replace(
                    observation(), event_at=NOW - timedelta(seconds=age), as_of_at=None
                )
                self.assertEqual(
                    check(evaluate((obs,)), F.Check.EVENT_AGE).state, expected
                )
        for age, expected in (
            (4.999999, F.State.PASS),
            (5, F.State.PASS),
            (5.000001, F.State.FAIL),
        ):
            with self.control("F08-receipt-endpoint-" + str(age)):
                stamp = NOW - timedelta(seconds=age)
                obs = replace(
                    observation(),
                    available_at=stamp,
                    receipt_at=stamp,
                    recorded_at=stamp,
                    as_of_at=None,
                )
                self.assertEqual(
                    check(evaluate((obs,)), F.Check.RECEIPT_AGE).state, expected
                )
        for delay, expected in (
            (899.999999, F.State.PASS),
            (900, F.State.PASS),
            (900.000001, F.State.FAIL),
        ):
            with self.control("F08-delay-endpoint-" + str(delay)):
                obs = replace(
                    observation(), delay=F.DelayEvidence(identity(), delay, reference())
                )
                cap = replace(capability(obs), max_delay_seconds=900)
                self.assertEqual(
                    check(evaluate((obs,), policy(cap)), F.Check.DELAY).state, expected
                )

    def test_F08_future_skew_does_not_relax_decision_cutoff(self):
        for ahead, expected in (
            (0.999999, F.State.PASS),
            (1, F.State.PASS),
            (1.000001, F.State.FAIL),
        ):
            with self.control("F08-future-skew-" + str(ahead)):
                obs = replace(
                    observation(),
                    event_at=NOW + timedelta(seconds=ahead),
                    as_of_at=None,
                )
                cap = replace(capability(obs), max_future_skew_seconds=1)
                result = evaluate((obs,), policy(cap))
                self.assertEqual(check(result, F.Check.EVENT_AGE).state, expected)
                self.assertEqual(check(result, F.Check.CUTOFF).state, F.State.FAIL)
                self.assertFalse(row(result).eligible)
        for key in (
            "event_at",
            "as_of_at",
            "available_at",
            "receipt_at",
            "recorded_at",
        ):
            for delta, expected in ((0, F.State.PASS), (0.000001, F.State.FAIL)):
                with self.control("F08-cutoff-" + key + "-" + str(delta)):
                    obs = replace(
                        observation(), **{key: NOW + timedelta(seconds=delta)}
                    )
                    result = evaluate((obs,), now=NOW + timedelta(hours=1))
                    self.assertEqual(check(result, F.Check.CUTOFF).state, expected)

    def test_F09_equivalent_offsets_dst_and_supplied_sessions(self):
        original = evaluate()
        for offset in (-5, 0, 5.5, 14):
            with self.control("F09-equivalent-offset-" + str(offset)):
                tz = timezone(timedelta(hours=offset))
                obs = observation()
                obs = replace(
                    obs,
                    **{
                        k: getattr(obs, k).astimezone(tz)
                        for k in (
                            "event_at",
                            "receipt_at",
                            "as_of_at",
                            "available_at",
                            "recorded_at",
                        )
                    },
                )
                result = evaluate(
                    (obs,), now=NOW.astimezone(tz), decision=NOW.astimezone(tz)
                )
                self.assertEqual(row(result), row(original))
                self.assertEqual(result.instruments[0].context[0].event_age_seconds, 10)
        with self.control("F09-DST-fold-elapsed-instant-is-one-hour"):
            tz = ZoneInfo("America/New_York")
            earlier = datetime(2026, 11, 1, 1, 30, tzinfo=tz, fold=0)
            later = datetime(2026, 11, 1, 1, 30, tzinfo=tz, fold=1)
            obs = replace(
                observation(),
                event_at=earlier,
                as_of_at=earlier,
                available_at=earlier,
                receipt_at=earlier,
                recorded_at=earlier,
                delay=None,
            )
            cap = replace(
                capability(obs, required=(F.Check.EVENT_AGE,)),
                max_event_age_seconds=3600,
            )
            result = evaluate((obs,), policy(cap), now=later, decision=later)
            self.assertTrue(row(result).eligible)
            self.assertEqual(result.instruments[0].context[0].event_age_seconds, 3600)
        for state, expected in (
            ("open", F.State.PASS),
            ("holiday", F.State.FAIL),
            ("early_close", F.State.FAIL),
            ("dst_transition", F.State.FAIL),
            (None, F.State.UNKNOWN),
        ):
            with self.control("F09-injected-session-" + str(state)):
                obs = replace(observation(), finality=F.Finality.UNKNOWN)
                caps = policy(
                    capability(obs),
                    capability(
                        obs, "session", required=(F.Check.SESSION, F.Check.FINALITY)
                    ),
                )
                result = evaluate(
                    (obs,),
                    caps,
                    sessions=(F.SessionFacts("TEST:SPY", state, reference()),),
                )
                self.assertTrue(row(result).eligible)
                self.assertEqual(
                    check(result, F.Check.SESSION, name="session").state, expected
                )
                self.assertEqual(
                    check(result, F.Check.FINALITY, name="session").state,
                    F.State.UNKNOWN,
                )
                self.assertEqual(result.instruments[0].context[0].event_age_seconds, 10)

    def test_F10_missing_policy_and_fallback_are_explicit(self):
        for pol in (
            replace(policy(), version=None),
            replace(policy(), provenance=None),
            replace(policy(), provenance={}),
            replace(policy(), provenance={"code_sha256": "a" * 64}),
            policy(replace(capability(), required_checks=None)),
            policy(replace(capability(), required_checks=())),
            policy(replace(capability(), evidence_id=None)),
        ):
            with self.control("F10-unconfigured-policy-" + str(len(CONTROL_IDS))):
                result = evaluate(pol=pol)
                self.assertEqual(check(result, F.Check.POLICY).state, F.State.UNKNOWN)
                self.assertFalse(row(result).eligible)
        for field, which in (
            ("max_event_age_seconds", F.Check.EVENT_AGE),
            ("max_future_skew_seconds", F.Check.EVENT_AGE),
            ("max_receipt_age_seconds", F.Check.RECEIPT_AGE),
            ("max_delay_seconds", F.Check.DELAY),
        ):
            with self.control("F10-missing-" + field):
                result = evaluate(pol=policy(replace(capability(), **{field: None})))
                self.assertEqual(check(result, which).state, F.State.UNKNOWN)
                self.assertFalse(row(result).eligible)
        for flag, proof, expected in (
            (None, None, F.State.UNKNOWN),
            (True, None, F.State.UNKNOWN),
            (True, reference(), F.State.PASS),
            (False, None, F.State.PASS),
        ):
            with self.control(
                "F10-fallback-" + str(flag) + "-" + str(proof is not None)
            ):
                obs = replace(observation(), is_fallback=flag, fallback_reference=proof)
                self.assertEqual(
                    check(evaluate((obs,)), F.Check.FALLBACK).state, expected
                )

    def test_F11_current_mode_requires_lifecycle_even_if_not_requested(self):
        for life, expected in (
            (None, F.State.UNKNOWN),
            (F.LifecycleFacts(None, None, None, None), F.State.UNKNOWN),
            (F.LifecycleFacts(False, "g1", "g1", reference()), F.State.FAIL),
            (F.LifecycleFacts(True, "g1", "g2", reference()), F.State.FAIL),
            (F.LifecycleFacts(True, "g1", None, reference()), F.State.UNKNOWN),
            (F.LifecycleFacts(True, "g2", "g2", None), F.State.UNKNOWN),
            (F.LifecycleFacts(True, "g2", "g2", reference()), F.State.PASS),
        ):
            with self.control("F11-lifecycle-" + str(len(CONTROL_IDS))):
                obs = replace(observation(), lifecycle=life)
                cap = capability(
                    obs, mode=F.Mode.CURRENT, required=(F.Check.EVENT_AGE,)
                )
                result = evaluate((obs,), policy(cap))
                self.assertEqual(check(result, F.Check.LIFECYCLE).state, expected)
                self.assertEqual(row(result).eligible, expected is F.State.PASS)
        with self.control("F11-fresh-injected-now-reages-current-not-research"):
            obs = observation()
            pol = policy(
                capability(obs), capability(obs, "current", mode=F.Mode.CURRENT)
            )
            earlier = evaluate((obs,), pol)
            later = evaluate((obs,), pol, now=NOW + timedelta(minutes=2))
            self.assertTrue(row(earlier, name="current").eligible)
            self.assertFalse(row(later, name="current").eligible)
            self.assertTrue(row(later).eligible)
        with self.control("F11-ToS-health-unverified-despite-retrieval"):
            obs = replace(
                observation(),
                identity=identity(source="ToS", tool="snapshot"),
                lifecycle=None,
                delay=None,
            )
            result = evaluate((obs,), policy(capability(obs, mode=F.Mode.CURRENT)))
            self.assertEqual(check(result, F.Check.LIFECYCLE).state, F.State.UNKNOWN)
            self.assertFalse(row(result).eligible)

    def test_F12_inputs_results_and_policy_aliases_are_immutable(self):
        with self.control("F12-immutable-deterministic-alias-detachment"):
            pins = dict(PINS)
            obs = observation()
            pol = F.EvaluationPolicy("v1", pins, (capability(obs),), pins)
            result = evaluate((obs,), pol)
            pins.clear()
            self.assertEqual(result, evaluate((obs,), pol))
            self.assertEqual(obs, observation())
            with self.assertRaises(FrozenInstanceError):
                obs.event_at = NOW
            with self.assertRaises(TypeError):
                pol.provenance["code_sha256"] = "1" * 64
            with self.assertRaises(FrozenInstanceError):
                result.policy_version = "v2"
        with self.control("F12-stdlib-static-no-clock-or-effects"):
            tree = ast.parse(SOURCE.read_text())
            imports = {
                n.module.split(".")[0]
                for n in ast.walk(tree)
                if isinstance(n, ast.ImportFrom)
            }
            self.assertEqual(
                imports,
                {
                    "__future__",
                    "dataclasses",
                    "datetime",
                    "enum",
                    "math",
                    "types",
                    "typing",
                    "zoneinfo",
                },
            )
            self.assertFalse(any(isinstance(n, ast.Import) for n in ast.walk(tree)))
            forbidden = {
                "now",
                "today",
                "utcnow",
                "time",
                "getenv",
                "open",
                "print",
                "exec",
                "eval",
                "__import__",
            }
            calls = [n.func for n in ast.walk(tree) if isinstance(n, ast.Call)]
            self.assertFalse(
                any(
                    (isinstance(n, ast.Attribute) and n.attr in forbidden)
                    or (isinstance(n, ast.Name) and n.id in forbidden)
                    for n in calls
                )
            )

    def test_A01_identity_hash_and_missing_provenance(self):
        for field in ("source", "tool", "field", "adapter_version"):
            for value, expected in (
                (None, F.State.UNKNOWN),
                ("different", F.State.FAIL),
            ):
                with self.control("A01-identity-" + field + "-" + str(value)):
                    obs = replace(
                        observation(), identity=replace(identity(), **{field: value})
                    )
                    result = evaluate((obs,))
                    self.assertEqual(check(result, F.Check.IDENTITY).state, expected)
                    self.assertFalse(row(result).eligible)
        for value, expected in ((None, F.State.UNKNOWN), ("1" * 64, F.State.FAIL)):
            with self.control("A01-hash-pin-" + str(value is None)):
                obs = replace(observation(), evidence_sha256=value)
                self.assertEqual(
                    check(evaluate((obs,)), F.Check.EVIDENCE).state, expected
                )
        with self.control("A01-missing-evidence-hash-pin-policy"):
            result = evaluate(
                pol=policy(replace(capability(), expected_evidence_sha256=None))
            )
            self.assertEqual(check(result, F.Check.EVIDENCE).state, F.State.UNKNOWN)
            self.assertFalse(row(result).eligible)
        with self.control("A01-missing-evidence-id"):
            self.assertFalse(
                row(evaluate((replace(observation(), evidence_id=None),))).eligible
            )

    def test_A02_ancillary_fact_cutoffs_and_contradictory_clocks(self):
        for proof in (
            replace(reference(), available_at=None),
            replace(reference(), recorded_at=None),
            replace(reference(), evidence_id=None),
            replace(reference(), sha256=None),
            reference(NOW + timedelta(microseconds=1)),
            replace(
                reference(), available_at=NOW, recorded_at=NOW - timedelta(seconds=1)
            ),
        ):
            for fact in ("delay", "lifecycle", "options", "session", "fallback"):
                with self.control(
                    "A02-fact-cutoff-" + fact + "-" + str(len(CONTROL_IDS))
                ):
                    obs = observation()
                    sessions = ()
                    which = {
                        "delay": F.Check.DELAY,
                        "lifecycle": F.Check.LIFECYCLE,
                        "options": F.Check.OI_VINTAGE,
                        "session": F.Check.SESSION,
                        "fallback": F.Check.FALLBACK,
                    }[fact]
                    if fact == "delay":
                        obs = replace(obs, delay=replace(obs.delay, reference=proof))
                    elif fact == "lifecycle":
                        obs = replace(
                            obs, lifecycle=replace(obs.lifecycle, reference=proof)
                        )
                    elif fact == "options":
                        obs = replace(
                            obs,
                            options=F.OptionsFacts(
                                NOW - timedelta(hours=1), True, True, proof
                            ),
                        )
                    elif fact == "session":
                        sessions = (F.SessionFacts("TEST:SPY", "open", proof),)
                    else:
                        obs = replace(obs, is_fallback=True, fallback_reference=proof)
                    result = evaluate(
                        (obs,),
                        policy(capability(obs, required=(which,))),
                        now=NOW + timedelta(hours=1),
                        sessions=sessions,
                    )
                    self.assertIn(
                        check(result, which).state, (F.State.UNKNOWN, F.State.FAIL)
                    )
                    self.assertFalse(row(result).eligible)
        for change in (
            {"available_at": NOW},
            {"receipt_at": NOW - timedelta(seconds=3)},
            {"recorded_at": NOW - timedelta(seconds=3)},
            {"event_at": NOW - timedelta(seconds=4)},
        ):
            with self.control("A02-observation-clock-order-" + str(len(CONTROL_IDS))):
                result = evaluate((replace(observation(), **change),))
                self.assertEqual(check(result, F.Check.CLOCK_ORDER).state, F.State.FAIL)
                self.assertFalse(row(result).eligible)
        with self.control("A02-evaluation-before-decision"):
            with self.assertRaisesRegex(ValueError, "^decision_after_evaluation$"):
                evaluate(now=NOW - timedelta(microseconds=1))

    def test_A03_interval_coverage_continuity_options_and_unconfigured_thresholds(self):
        for field, values, which in (
            (
                "interval_seconds",
                ((None, F.State.UNKNOWN), (30, F.State.FAIL), (60, F.State.PASS)),
                F.Check.INTERVAL,
            ),
            (
                "coverage_fraction",
                ((None, F.State.UNKNOWN), (0.999999, F.State.FAIL), (1, F.State.PASS)),
                F.Check.COVERAGE,
            ),
            (
                "continuous",
                ((None, F.State.UNKNOWN), (False, F.State.FAIL), (True, F.State.PASS)),
                F.Check.CONTINUITY,
            ),
        ):
            for value, expected in values:
                with self.control("A03-" + field + "-" + str(value)):
                    obs = replace(observation(), **{field: value})
                    result = evaluate(
                        (obs,), policy(capability(obs, required=(which,)))
                    )
                    self.assertEqual(check(result, which).state, expected)
        for age, expected in (
            (86399.999999, F.State.PASS),
            (86400, F.State.PASS),
            (86400.000001, F.State.FAIL),
            (-1, F.State.FAIL),
        ):
            with self.control("A03-OI-vintage-endpoint-" + str(age)):
                obs = replace(
                    observation(),
                    options=F.OptionsFacts(
                        NOW - timedelta(seconds=age), True, True, reference()
                    ),
                )
                result = evaluate(
                    (obs,), policy(capability(obs, required=(F.Check.OI_VINTAGE,)))
                )
                self.assertEqual(check(result, F.Check.OI_VINTAGE).state, expected)
        for key, which in (
            ("required_interval_seconds", F.Check.INTERVAL),
            ("min_coverage_fraction", F.Check.COVERAGE),
            ("max_oi_age_seconds", F.Check.OI_VINTAGE),
            ("allowed_session_states", F.Check.SESSION),
        ):
            with self.control("A03-unconfigured-" + key):
                obs = replace(
                    observation(),
                    options=F.OptionsFacts(
                        NOW - timedelta(hours=1), True, True, reference()
                    ),
                )
                cap = replace(capability(obs, required=(which,)), **{key: None})
                result = evaluate(
                    (obs,),
                    policy(cap),
                    sessions=(F.SessionFacts("TEST:SPY", "open", reference()),),
                )
                self.assertEqual(check(result, which).state, F.State.UNKNOWN)
        for which, field in (
            (F.Check.GREEKS, "greeks_verified"),
            (F.Check.EXPIRY_SETTLEMENT, "expiry_settlement_verified"),
        ):
            for value, expected in (
                (False, F.State.FAIL),
                (None, F.State.UNKNOWN),
                (True, F.State.PASS),
            ):
                with self.control("A03-options-" + field + "-" + str(value)):
                    opt = replace(
                        F.OptionsFacts(
                            NOW - timedelta(hours=1), True, True, reference()
                        ),
                        **{field: value},
                    )
                    obs = replace(observation(), options=opt)
                    result = evaluate(
                        (obs,), policy(capability(obs, required=(which,)))
                    )
                    self.assertEqual(check(result, which).state, expected)

    def test_A05_literal_publication_pin_maps(self):
        # Literal keys independent of the source constant and supplied map contents.
        cap = capability(required=(F.Check.PUBLICATION_PINS,))
        literal_keys = (
            "code_sha256",
            "config_sha256",
            "calendar_sha256",
            "outcome_definition_sha256",
        )
        for side in ("provenance", "frozen_publication_provenance"):
            for key in literal_keys:
                for kind in ("absent", "null", "drift"):
                    with self.control("A05-pins-" + side + "-" + key + "-" + kind):
                        pins = dict(PINS)
                        if kind == "absent":
                            del pins[key]
                        else:
                            pins[key] = None if kind == "null" else "9" * 64
                        result = evaluate(pol=replace(policy(cap), **{side: pins}))
                        expected = F.State.FAIL if kind == "drift" else F.State.UNKNOWN
                        self.assertEqual(
                            check(result, F.Check.PUBLICATION_PINS).state, expected
                        )
                        self.assertFalse(row(result).eligible)
            for pins in (None, {}):
                with self.control("A05-pins-empty-" + side + "-" + str(pins is None)):
                    result = evaluate(pol=replace(policy(cap), **{side: pins}))
                    self.assertEqual(
                        check(result, F.Check.PUBLICATION_PINS).state, F.State.UNKNOWN
                    )
        with self.control("A05-pins-order-independent"):
            result = evaluate(
                pol=replace(policy(cap), provenance=dict(reversed(list(PINS.items()))))
            )
            self.assertTrue(row(result).eligible)

    def test_A05_malformed_type_confused_inputs_reject_boundedly(self):
        for value in (False, True, "0", [], {}, -1, float("nan"), float("inf")):
            for field in (
                "max_event_age_seconds",
                "max_receipt_age_seconds",
                "max_future_skew_seconds",
                "max_delay_seconds",
                "max_oi_age_seconds",
                "min_coverage_fraction",
                "required_interval_seconds",
            ):
                with self.control(
                    "A05-policy-number-" + field + "-" + str(len(CONTROL_IDS))
                ):
                    with self.assertRaisesRegex(ValueError, "^invalid_numeric_fact$"):
                        replace(capability(), **{field: value})
            for field in ("interval_seconds", "coverage_fraction"):
                with self.control(
                    "A05-observation-number-" + field + "-" + str(len(CONTROL_IDS))
                ):
                    with self.assertRaisesRegex(ValueError, "^invalid_numeric_fact$"):
                        replace(observation(), **{field: value})
            with self.control("A05-delay-number-" + str(len(CONTROL_IDS))):
                with self.assertRaisesRegex(ValueError, "^invalid_numeric_fact$"):
                    F.DelayEvidence(identity(), value, reference())
        for value in (0, 1, "true", [], {}):
            for field in ("continuous", "is_fallback"):
                with self.control("A05-boolean-" + field + "-" + str(len(CONTROL_IDS))):
                    with self.assertRaisesRegex(ValueError, "^invalid_boolean_fact$"):
                        replace(observation(), **{field: value})
            with self.control("A05-connected-boolean-" + str(len(CONTROL_IDS))):
                with self.assertRaisesRegex(ValueError, "^invalid_boolean_fact$"):
                    F.LifecycleFacts(value, "g1", "g1", reference())
        for value in (datetime(2026, 1, 5), "2026-01-05T14:30:00Z", 0, False, [], {}):
            for field in (
                "event_at",
                "receipt_at",
                "as_of_at",
                "available_at",
                "recorded_at",
            ):
                with self.control(
                    "A05-invalid-clock-" + field + "-" + str(len(CONTROL_IDS))
                ):
                    with self.assertRaisesRegex(ValueError, "^invalid_clock$"):
                        replace(observation(), **{field: value})
            with self.control("A05-invalid-injected-clock-" + str(len(CONTROL_IDS))):
                with self.assertRaisesRegex(ValueError, "^invalid_clock$"):
                    evaluate(now=value)
        for pins in (
            [],
            "RAW-SECRET-SENTINEL",
            False,
            0,
            {"code_sha256": ""},
            {"code_sha256": "g" * 64},
            {"code_sha256": True},
            {"extra": "a" * 64},
        ):
            with self.control("A05-malformed-map-" + str(len(CONTROL_IDS))):
                with self.assertRaises(ValueError) as caught:
                    replace(policy(), provenance=pins)
                self.assertIn(
                    str(caught.exception),
                    ("invalid_policy_provenance", "invalid_hash_reference"),
                )
                self.assertNotIn("RAW-SECRET-SENTINEL", str(caught.exception))
        for field, value in (
            ("presence", True),
            ("finality", "final"),
            ("delay", {}),
            ("lifecycle", {}),
            ("options", {}),
            ("identity", {}),
            ("evidence_sha256", False),
            ("evidence_sha256", "bad"),
        ):
            with self.control("A05-malformed-observation-" + field):
                with self.assertRaises(ValueError):
                    replace(observation(), **{field: value})
        for fn in (
            lambda: evaluate(observations=[]),
            lambda: evaluate(sessions=[]),
            lambda: F.evaluate_feed_quality(
                ({},), policy(), now=NOW, decision_at=NOW, sessions=()
            ),
            lambda: F.evaluate_feed_quality(
                (), None, now=NOW, decision_at=NOW, sessions=()
            ),
            lambda: replace(capability(), required_checks=("event_age",)),
            lambda: replace(
                capability(), required_checks=(F.Check.EVENT_AGE, F.Check.EVENT_AGE)
            ),
            lambda: replace(capability(), mode="current"),
            lambda: replace(policy(), capabilities=()),
            lambda: policy(capability(), capability()),
            lambda: identity(instrument="SPY"),
        ):
            with self.control(
                "A05-malformed-container-or-namespace-" + str(len(CONTROL_IDS))
            ):
                with self.assertRaises(ValueError):
                    fn()

    def test_A06_result_reasons_are_codes_and_hashes_do_not_authenticate(self):
        with self.control("A06-codes-only-no-payload-admission"):
            result = evaluate(
                (replace(observation(), event_at=None, delay=None, lifecycle=None),)
            )
            for chk in row(result).checks:
                self.assertRegex(chk.reason, "^[a-z_]+$")
                self.assertLess(len(chk.reason), 64)
            self.assertEqual(
                check(result, F.Check.EVIDENCE).reason, "opaque_evidence_reference"
            )
            self.assertFalse(hasattr(observation(), "raw_payload"))
            with self.assertRaises(TypeError):
                F.Observation(**{"raw_payload": "RAW-SECRET-SENTINEL"})

    def test_A04_retained_future_and_unknown_knowledge_are_not_visible(self):
        for key in ("available_at", "receipt_at", "recorded_at"):
            for stamp, expected in (
                (None, F.State.UNKNOWN),
                (NOW + timedelta(microseconds=1), F.State.FAIL),
            ):
                with self.control(
                    "A04-context-knowledge-" + key + "-" + expected.value
                ):
                    obs = replace(observation(), **{key: stamp})
                    result = evaluate((obs,))
                    context = result.instruments[0].context[0]
                    self.assertEqual(context.knowledge_state, expected)
                    self.assertFalse(context.visible)
                    self.assertIs(context.observation, obs)
                    self.assertFalse(row(result).eligible)
        with self.control("A04-future-context-retained-then-visible-after-retrieval"):
            obs = replace(
                observation(),
                available_at=NOW + timedelta(seconds=1),
                receipt_at=NOW + timedelta(seconds=2),
                recorded_at=NOW + timedelta(seconds=2),
            )
            earlier = evaluate((obs,))
            later = evaluate((obs,), now=NOW + timedelta(seconds=3))
            self.assertFalse(earlier.instruments[0].context[0].visible)
            self.assertTrue(later.instruments[0].context[0].visible)
            self.assertFalse(row(later).eligible)

    def test_A02_explicit_skew_and_missing_policy_for_source_clock_order(self):
        for skew, expected in (
            (None, F.State.UNKNOWN),
            (0, F.State.FAIL),
            (0.999999, F.State.FAIL),
            (1, F.State.PASS),
        ):
            with self.control("A02-source-clock-skew-" + str(skew)):
                obs = replace(
                    observation(), event_at=NOW - timedelta(seconds=1), as_of_at=None
                )
                cap = replace(
                    capability(obs, required=(F.Check.RECEIPT_AGE,)),
                    max_future_skew_seconds=skew,
                )
                result = evaluate((obs,), policy(cap))
                self.assertEqual(check(result, F.Check.CLOCK_ORDER).state, expected)
                self.assertEqual(row(result).eligible, expected is F.State.PASS)

    def test_A05_invalid_ancillary_types_and_safe_clock_range(self):
        for constructor in (
            lambda: F.OptionsFacts(NOW, 1, True, reference()),
            lambda: F.OptionsFacts(NOW, True, 0, reference()),
            lambda: F.OptionsFacts("2026-01-05", True, True, reference()),
            lambda: F.DelayEvidence({}, 0, reference()),
            lambda: F.DelayEvidence(identity(), 0, {}),
            lambda: F.FactReference(False, "a" * 64, NOW, NOW),
            lambda: F.FactReference("proof", "a" * 64, False, NOW),
            lambda: F.SessionFacts("SPY", "open", reference()),
            lambda: F.SessionFacts("TEST:SPY", 0, reference()),
            lambda: replace(observation(), interval_seconds=0),
            lambda: replace(observation(), coverage_fraction=1.000001),
            lambda: replace(capability(), required_interval_seconds=0),
            lambda: replace(capability(), min_coverage_fraction=1.000001),
            lambda: replace(capability(), allowed_session_states=[]),
            lambda: replace(capability(), expected_evidence_sha256=""),
            lambda: replace(policy(), version=""),
            lambda: replace(
                observation(),
                event_at=datetime.max.replace(tzinfo=timezone(timedelta(hours=-1))),
            ),
            lambda: evaluate(
                sessions=(
                    F.SessionFacts("TEST:SPY", "open", reference()),
                    F.SessionFacts("TEST:SPY", "holiday", reference()),
                )
            ),
        ):
            with self.control(
                "A05-ancillary-typed-or-clock-range-" + str(len(CONTROL_IDS))
            ):
                with self.assertRaises(ValueError):
                    constructor()

    def test_A06_zero_delay_and_disconnection_do_not_claim_live_authenticity(self):
        with self.control("A06-zero-delay-label-is-declaration-only"):
            obs = replace(
                observation(),
                lifecycle=F.LifecycleFacts(False, "g1", "g1", reference()),
            )
            result = evaluate(
                (obs,),
                policy(
                    capability(obs), capability(obs, "current", mode=F.Mode.CURRENT)
                ),
            )
            self.assertTrue(row(result).eligible)
            self.assertTrue(result.instruments[0].context[0].visible)
            self.assertEqual(
                result.instruments[0].context[0].delay_label, "declared_zero_delay"
            )
            self.assertFalse(row(result, name="current").eligible)

    def precision_case(
        self, which, duration, maximum, *, decision=None, skew=0, zone=None
    ):
        """Public-API facts for a literal exact-duration oracle, no source helper."""
        decision = (
            datetime(2026, 10, 12, 16, tzinfo=timezone.utc)
            if decision is None
            else decision
        )
        stamp = decision.astimezone(timezone.utc) - duration
        ident = identity()
        proof = F.FactReference("precision:proof", "a" * 64, decision, decision)
        obs = F.Observation(
            ident,
            "precision:obs",
            "b" * 64,
            decision,
            decision,
            None,
            decision,
            decision,
            F.Presence.PRESENT,
            is_fallback=False,
        )
        if which is F.Check.EVENT_AGE:
            obs = replace(obs, event_at=stamp)
        elif which is F.Check.RECEIPT_AGE:
            obs = replace(
                obs,
                event_at=stamp,
                available_at=stamp,
                receipt_at=stamp,
                recorded_at=stamp,
            )
        elif which is F.Check.OI_VINTAGE:
            obs = replace(obs, options=F.OptionsFacts(stamp, None, None, proof))
        else:
            obs = replace(obs, available_at=stamp, receipt_at=stamp, recorded_at=stamp)
        if zone is not None:
            obs = replace(
                obs,
                **{
                    k: getattr(obs, k).astimezone(zone)
                    for k in ("event_at", "receipt_at", "available_at", "recorded_at")
                },
            )
            if obs.options is not None:
                obs = replace(
                    obs,
                    options=replace(obs.options, oi_vintage_at=stamp.astimezone(zone)),
                )
            decision = decision.astimezone(zone)
        cap = F.CapabilityPolicy(
            "precision",
            ident,
            "precision:obs",
            F.Mode.RESEARCH,
            (which,),
            expected_evidence_sha256="b" * 64,
            max_event_age_seconds=maximum,
            max_receipt_age_seconds=maximum,
            max_oi_age_seconds=maximum,
            max_future_skew_seconds=maximum if which is F.Check.CLOCK_ORDER else skew,
        )
        return evaluate((obs,), policy(cap), now=decision, decision=decision)

    def test_F01_precision_wide_exact_integer_and_float_policy_boundaries(self):
        spans = (2147483648, 8589934592, 34359738368, 315537897598)
        for span in spans:
            decision = (
                datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
                if span == 315537897598
                else datetime(2026, 10, 12, 16, tzinfo=timezone.utc)
            )
            for maximum in (span, float(span)):
                for which in (
                    F.Check.EVENT_AGE,
                    F.Check.RECEIPT_AGE,
                    F.Check.OI_VINTAGE,
                    F.Check.CLOCK_ORDER,
                ):
                    for extra, expected in (
                        (-1, F.State.PASS),
                        (0, F.State.PASS),
                        (1, F.State.FAIL),
                    ):
                        with self.control(
                            "F01-wide-"
                            + which.value
                            + "-"
                            + str(span)
                            + "-"
                            + type(maximum).__name__
                            + "-"
                            + str(extra)
                        ):
                            duration = timedelta(seconds=span, microseconds=extra)
                            # The oracle is the literal integer endpoint; never float total_seconds().
                            self.assertEqual(
                                (duration.days * 86400 + duration.seconds) * 1000000
                                + duration.microseconds,
                                span * 1000000 + extra,
                            )
                            result = self.precision_case(
                                which, duration, maximum, decision=decision
                            )
                            self.assertEqual(
                                check(result, which, name="precision").state, expected
                            )
                            self.assertEqual(
                                row(result, name="precision").eligible,
                                expected is F.State.PASS,
                            )
                            if (
                                span == 34359738368
                                and extra == 1
                                and which is not F.Check.CLOCK_ORDER
                            ):
                                # Approximate reporting still rounds here; it cannot decide eligibility.
                                context = result.instruments[0].context[0]
                                report = (
                                    context.receipt_age_seconds
                                    if which is F.Check.RECEIPT_AGE
                                    else context.event_age_seconds
                                )
                                if which is not F.Check.OI_VINTAGE:
                                    self.assertEqual(report, 34359738368.0)

    def test_F01_precision_fractional_zero_and_huge_admitted_policies(self):
        cases = (
            (0, 0, F.State.PASS),
            (0, 1, F.State.FAIL),
            (0.5, 499999, F.State.PASS),
            (0.5, 500000, F.State.PASS),
            (0.5, 500001, F.State.FAIL),
            (0.25, 250000, F.State.PASS),
            (1.5, 1500000, F.State.PASS),
            (0.1, 100000, F.State.PASS),
            (0.3, 300000, F.State.FAIL),
            (0.000001, 1, F.State.FAIL),
        )
        for which in (
            F.Check.EVENT_AGE,
            F.Check.RECEIPT_AGE,
            F.Check.OI_VINTAGE,
            F.Check.CLOCK_ORDER,
        ):
            for maximum, exact_microseconds, expected in cases:
                with self.control(
                    "F01-fraction-"
                    + which.value
                    + "-"
                    + str(maximum)
                    + "-"
                    + str(exact_microseconds)
                ):
                    result = self.precision_case(
                        which, timedelta(microseconds=exact_microseconds), maximum
                    )
                    self.assertEqual(
                        check(result, which, name="precision").state, expected
                    )
                    self.assertEqual(
                        row(result, name="precision").eligible, expected is F.State.PASS
                    )
            for maximum in (10**1000, 1e308):
                with self.control(
                    "F01-huge-" + which.value + "-" + type(maximum).__name__
                ):
                    result = self.precision_case(
                        which, timedelta(seconds=34359738368, microseconds=1), maximum
                    )
                    self.assertTrue(row(result, name="precision").eligible)

    def test_F01_precision_negative_future_age_and_source_skew(self):
        for maximum in (0, 0.5, 34359738368):
            for extra, expected in (
                (-1, F.State.PASS),
                (0, F.State.PASS),
                (1, F.State.FAIL),
            ):
                if maximum == 0 and extra == -1:
                    continue
                duration = timedelta(seconds=maximum, microseconds=extra)
                for which in (F.Check.EVENT_AGE, F.Check.RECEIPT_AGE):
                    with self.control(
                        "F01-negative-age-"
                        + which.value
                        + "-"
                        + str(maximum)
                        + "-"
                        + str(extra)
                    ):
                        result = self.precision_case(
                            which, -duration, maximum, skew=maximum
                        )
                        self.assertEqual(
                            check(result, which, name="precision").state, expected
                        )
                        if duration > timedelta(0):
                            self.assertEqual(
                                check(result, F.Check.CUTOFF, name="precision").state,
                                F.State.FAIL,
                            )
                            self.assertFalse(row(result, name="precision").eligible)
                for which in (F.Check.OI_VINTAGE, F.Check.CLOCK_ORDER):
                    with self.control(
                        "F01-negative-PIT-"
                        + which.value
                        + "-"
                        + str(maximum)
                        + "-"
                        + str(extra)
                    ):
                        result = self.precision_case(
                            which, -duration, maximum, skew=maximum
                        )
                        if duration > timedelta(0):
                            # OI cannot be after decision; a negative source gap is ordered,
                            # but its future availability cannot enter the original decision.
                            self.assertEqual(
                                check(result, which, name="precision").state,
                                F.State.FAIL
                                if which is F.Check.OI_VINTAGE
                                else F.State.PASS,
                            )
                            self.assertFalse(row(result, name="precision").eligible)
                        else:
                            self.assertTrue(row(result, name="precision").eligible)

    def test_F01_precision_timezone_and_DST_microsecond_boundaries(self):
        for which in (
            F.Check.EVENT_AGE,
            F.Check.RECEIPT_AGE,
            F.Check.OI_VINTAGE,
            F.Check.CLOCK_ORDER,
        ):
            for extra, expected in ((0, F.State.PASS), (1, F.State.FAIL)):
                for offset in (-5, 5.5, 14):
                    with self.control(
                        "F01-wide-offset-"
                        + which.value
                        + "-"
                        + str(extra)
                        + "-"
                        + str(offset)
                    ):
                        result = self.precision_case(
                            which,
                            timedelta(seconds=34359738368, microseconds=extra),
                            34359738368,
                            zone=timezone(timedelta(hours=offset)),
                        )
                        self.assertEqual(
                            check(result, which, name="precision").state, expected
                        )
        zone = ZoneInfo("America/New_York")
        decision = datetime(2026, 11, 1, 1, 30, tzinfo=zone, fold=1)
        for which in (
            F.Check.EVENT_AGE,
            F.Check.RECEIPT_AGE,
            F.Check.OI_VINTAGE,
            F.Check.CLOCK_ORDER,
        ):
            for extra, expected in (
                (-1, F.State.PASS),
                (0, F.State.PASS),
                (1, F.State.FAIL),
            ):
                with self.control("F01-DST-exact-" + which.value + "-" + str(extra)):
                    result = self.precision_case(
                        which,
                        timedelta(seconds=3600, microseconds=extra),
                        3600,
                        decision=decision,
                        zone=zone,
                    )
                    self.assertEqual(
                        check(result, which, name="precision").state, expected
                    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
