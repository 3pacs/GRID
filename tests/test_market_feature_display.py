"""Synthetic display checks using normal imports; collection never alters packages."""

from dataclasses import fields, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
import sysconfig
import importlib
import json
import sys
import unittest
from collections.abc import Mapping
from zoneinfo import ZoneInfo

from analysis import etf_price_features as N
from analysis import feed_reliability_projection as Q
from analysis.market_feature_display import project_price_feature

START = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
END = START + timedelta(minutes=5)


def reference(name="proof:1", at=START):
    return Q.FactReference(name, "a" * 64, at, at)


def fixture(instrument="TEST:SPY"):
    identity = Q.SourceIdentity(instrument, "synthetic-bars", "fixture", "ohlcv", "v1")
    basis = N.PriceBasis(
        "USD",
        "shares",
        "USD",
        N.Adjustment.RAW,
        "raw",
        "v1",
        reference(),
        N.NotionalBasis.TRADE,
    )
    session = N.SessionWindow(
        instrument,
        "TEST:X",
        "session:1",
        "UTC",
        START,
        END,
        "calendar:1",
        "v1",
        reference(),
        N.SessionConvention.REGULAR,
        (N.BarInterval(START, END),),
    )
    bars = []
    cursor = START
    for i, (seconds, high, low, volume, notional) in enumerate(
        ((120, "104", "98", 1, "0.1"), (180, "103", "99", 2, "0.9"))
    ):
        end = cursor + timedelta(seconds=seconds)
        observation = Q.Observation(
            identity,
            f"bar:{i}",
            "b" * 64,
            cursor,
            end,
            cursor,
            end,
            end,
            Q.Presence.PRESENT,
            Q.DelayEvidence(identity, 0, reference()),
            seconds,
            Q.Finality.FINAL,
            f"revision:{i}",
            None,
            1,
            True,
            Q.LifecycleFacts(True, "gen:1", "gen:1", reference()),
            is_fallback=False,
        )
        bars.append(
            N.NumericBar(
                observation,
                cursor,
                end,
                N.TimestampConvention.START,
                session,
                basis,
                Decimal("100"),
                Decimal(high),
                Decimal(low),
                Decimal("100"),
                volume,
                Decimal(notional),
                reference(),
            )
        )
        cursor = end
    bars = tuple(bars)
    grid = N.ExpectedBarGrid(
        START,
        END,
        session,
        basis,
        tuple(N.BarInterval(b.interval_start, b.interval_end) for b in bars),
        reference("grid:1"),
    )
    capabilities = tuple(
        Q.CapabilityPolicy(
            f"quality:{i}",
            identity,
            b.observation.evidence_id,
            Q.Mode.RESEARCH,
            (Q.Check.FINALITY, Q.Check.INTERVAL),
            expected_evidence_sha256=b.observation.evidence_sha256,
            max_future_skew_seconds=0,
            required_interval_seconds=b.observation.interval_seconds,
        )
        for i, b in enumerate(bars)
    )
    policy = Q.EvaluationPolicy(
        "policy:1", {"code_sha256": "c" * 64, "config_sha256": "d" * 64}, capabilities
    )
    return (
        bars,
        grid,
        dict(
            cutoff=END,
            quality_policy=policy,
            evaluation_at=END,
            decision_at=END,
            session_facts=(Q.SessionFacts(instrument, "open", reference()),),
        ),
    )


def result(method=N.PriceFeatureMethod.OPENING_RANGE, instrument="TEST:SPY"):
    bars, grid, options = fixture(instrument)
    if method is N.PriceFeatureMethod.OPENING_RANGE:
        return N.opening_range(bars, grid, minutes=5, **options)
    if method is N.PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY:
        basis = replace(grid.basis, notional_basis=N.NotionalBasis.OHLCV_PROXY)
        return N.ohlcv_hlc3_volume_proxy(
            tuple(replace(b, basis=basis) for b in bars),
            replace(grid, basis=basis),
            **options,
        )
    return N.trade_vwap(bars, grid, **options)


def snapshot(value):
    """Type-sensitive whole-input fingerprint, including clocks, payload and order."""
    if type(value) is datetime:
        return ("datetime", value.isoformat(), str(value.tzinfo), value.fold)
    if is_dataclass(value):
        return (
            type(value).__name__,
            tuple((f.name, snapshot(getattr(value, f.name))) for f in fields(value)),
        )
    if isinstance(value, Mapping):
        return (type(value).__name__, tuple((k, snapshot(v)) for k, v in value.items()))
    if type(value) is tuple:
        return ("tuple", tuple(snapshot(v) for v in value))
    return (type(value).__name__, str(value))


class MarketFeatureDisplayTests(unittest.TestCase):
    def test_actual_opening_range_104_98(self):
        for instrument in ("TEST:SPY", "TEST:QQQ"):
            with self.subTest(instrument=instrument):
                supplied = result(instrument=instrument)
                self.assertEqual(
                    (supplied.high, supplied.low), (Decimal("104"), Decimal("98"))
                )
                display = project_price_feature(supplied)
                self.assertEqual(
                    display["values"], {"high": "104", "low": "98", "vwap": None}
                )
                self.assertEqual(display["method"], "opening_range")
                self.assertEqual(display["opening_minutes"], 5)
                self.assertEqual(
                    display["provenance"]["grid"]["session"]["instrument"], instrument
                )

    def test_actual_trade_ratio_one_third(self):
        supplied = result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
        self.assertEqual(supplied.vwap, Fraction(1, 3))
        display = project_price_feature(supplied)
        self.assertEqual(
            display["values"],
            {"high": None, "low": None, "vwap": {"numerator": "1", "denominator": "3"}},
        )
        self.assertEqual(display["method"], "trade_notional_volume_vwap")
        self.assertEqual(display["method_label"], "Trade-notional / volume VWAP")

    def test_zero_and_null_are_distinct(self):
        supplied = result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
        self.assertEqual(
            project_price_feature(replace(supplied, vwap=Fraction(0)))["values"][
                "vwap"
            ],
            {"numerator": "0", "denominator": "1"},
        )
        self.assertIsNone(
            project_price_feature(replace(supplied, vwap=None))["values"]["vwap"]
        )
        for seconds in (0, None):
            with self.subTest(seconds=seconds):
                bar = supplied.inputs[0]
                altered = replace(
                    bar,
                    observation=replace(
                        bar.observation,
                        delay=Q.DelayEvidence(
                            bar.observation.identity, seconds, reference()
                        ),
                    ),
                )
                display = project_price_feature(
                    replace(supplied, inputs=(altered,) + supplied.inputs[1:])
                )
                self.assertEqual(
                    display["provenance"]["input_metadata"][0]["observation"]["delay"][
                        "seconds"
                    ],
                    None if seconds is None else str(seconds),
                )

    def test_every_nonpass_state_withholds_even_populated_dto(self):
        for method in N.PriceFeatureMethod:
            base = result(method)
            for name in ("numeric", "coverage", "basis", "temporal"):
                for state in (Q.State.FAIL, Q.State.UNKNOWN, Q.State.NOT_REQUIRED):
                    with self.subTest(method=method, name=name, state=state):
                        populated = replace(
                            base,
                            high=Decimal("987654321.12345"),
                            low=Decimal("987654320.12345"),
                            vwap=Fraction(7654321, 17),
                            **{
                                name: N.StructuralResult(
                                    state, ("first", "second", "first")
                                )
                            },
                        )
                        display = project_price_feature(populated)
                        self.assertEqual(
                            display["values"], {"high": None, "low": None, "vwap": None}
                        )
                        self.assertEqual(
                            display["diagnostics"][name],
                            {
                                "state": state.value,
                                "reasons": ["first", "second", "first"],
                            },
                        )
                        encoded = json.dumps(display, allow_nan=False)
                        self.assertNotIn("987654321", encoded)
                        self.assertNotIn("7654321", encoded)
                        for other in ("numeric", "coverage", "basis", "temporal"):
                            if other != name:
                                self.assertEqual(
                                    display["diagnostics"][other]["state"], "pass"
                                )

    def test_proxy_label_and_ratio_are_distinct(self):
        supplied = result(N.PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY)
        self.assertEqual(supplied.vwap, Fraction(302, 3))
        display = project_price_feature(supplied)
        self.assertEqual(display["method"], "ohlcv_hlc3_volume_proxy")
        self.assertEqual(display["method_label"], "HLC3-volume proxy (not trade VWAP)")
        self.assertEqual(
            display["values"]["vwap"], {"numerator": "302", "denominator": "3"}
        )
        self.assertNotEqual(
            display["method_label"],
            project_price_feature(
                result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
            )["method_label"],
        )

    def test_exact_decimal_and_huge_fraction_no_context_rounding(self):
        supplied = result()
        hi = Decimal("104.00000000000000000000000000000000100")
        lo = Decimal("9.8E-900")
        with localcontext() as context:
            context.prec = 2
            display = project_price_feature(replace(supplied, high=hi, low=lo))
        self.assertEqual(display["values"]["high"], str(hi))
        self.assertEqual(display["values"]["low"], "9.8E-900")
        fraction = Fraction(10**100 + 1, 3**200)
        display = project_price_feature(
            replace(
                result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP), vwap=fraction
            )
        )
        self.assertEqual(
            display["values"]["vwap"],
            {
                "numerator": str(fraction.numerator),
                "denominator": str(fraction.denominator),
            },
        )
        self.assertEqual(json.loads(json.dumps(display, allow_nan=False)), display)

    def test_signed_fraction_declared_value_is_not_recomputed(self):
        supplied = result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
        for value in (Fraction(-1, 3), Fraction(0), Fraction(1, 10**150)):
            with self.subTest(value=str(value)):
                display = project_price_feature(replace(supplied, vwap=value))
                self.assertEqual(
                    display["values"]["vwap"],
                    {
                        "numerator": str(value.numerator),
                        "denominator": str(value.denominator),
                    },
                )

    def assert_json_projection(self, supplied):
        try:
            display = project_price_feature(supplied)
            encoded = json.dumps(display, allow_nan=False)
        except Exception as exc:
            self.fail(
                "admitted integer display failed: "
                + type(exc).__name__
                + ": "
                + str(exc)
            )
        self.assertEqual(json.loads(encoded), display)
        return display

    def test_integer_digit_boundaries_and_signed_fraction_default_limit(self):
        self.assertEqual(sys.get_int_max_str_digits(), 4300)
        self.assertEqual(sysconfig.get_python_version(), "3.11")
        supplied = result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
        for exponent in (0, 4298, 4299, 4300, 5000, 10000):
            digits = "1" + "0" * exponent
            for sign in (1, -1):
                with self.subTest(exponent=exponent, sign=sign, role="numerator"):
                    display = self.assert_json_projection(
                        replace(supplied, vwap=Fraction(sign * 10**exponent))
                    )
                    self.assertEqual(
                        display["values"]["vwap"],
                        {
                            "numerator": ("-" if sign < 0 else "") + digits,
                            "denominator": "1",
                        },
                    )
            with self.subTest(exponent=exponent, role="denominator"):
                display = self.assert_json_projection(
                    replace(supplied, vwap=Fraction(1, 10**exponent))
                )
                self.assertEqual(
                    display["values"]["vwap"], {"numerator": "1", "denominator": digits}
                )
        self.assertEqual(sys.get_int_max_str_digits(), 4300)

    def test_integer_metadata_schema_is_stable_across_sizes(self):
        supplied = result()
        for exponent in (0, 1, 4299, 4300, 5000):
            number, digits = 10**exponent, "1" + "0" * exponent
            bar = supplied.inputs[0]
            observed = replace(
                bar.observation,
                interval_seconds=number,
                delay=Q.DelayEvidence(bar.observation.identity, number, reference()),
            )
            cap = supplied.quality_policy.capabilities[0]
            integer_policy_names = (
                "max_event_age_seconds",
                "max_receipt_age_seconds",
                "max_future_skew_seconds",
                "max_delay_seconds",
                "required_interval_seconds",
                "max_oi_age_seconds",
            )
            integer_policy = replace(
                cap,
                **{name: number for name in integer_policy_names},
                min_coverage_fraction=1,
            )
            dto = replace(
                supplied,
                inputs=(replace(bar, observation=observed),) + supplied.inputs[1:],
                quality_policy=replace(
                    supplied.quality_policy, capabilities=(integer_policy,)
                ),
            )
            with self.subTest(exponent=exponent):
                display = self.assert_json_projection(dto)
                row = display["provenance"]["input_metadata"][0]["observation"]
                self.assertEqual(row["interval_seconds"], digits)
                self.assertEqual(row["delay"]["seconds"], digits)
                self.assertEqual(row["coverage_fraction"], "1")
                policy = display["provenance"]["quality_policy"]["capabilities"][0]
                for name in integer_policy_names:
                    self.assertEqual(policy[name], digits)
                    self.assertIs(type(policy[name]), str)
                self.assertEqual(policy["min_coverage_fraction"], "1")
                self.assertIs(type(display["opening_minutes"]), int)
                self.assertEqual(display["opening_minutes"], 5)
                self.assertIs(type(display["clocks"]["evaluation_at"]["fold"]), int)
                self.assertEqual(display["schema"], "market-feature-display-v2")
                self.assertIs(dto.inputs[0].observation, observed)
                self.assertEqual(observed.interval_seconds, number)

    def test_metadata_integer_zero_float_bool_and_null_remain_distinct(self):
        supplied = result()
        first = supplied.inputs[0]
        for number, expected in (
            (0, "0"),
            (0.0, 0.0),
            (1, "1"),
            (0.5, 0.5),
            (None, None),
        ):
            for boolean in (True, False, None):
                with self.subTest(number_type=type(number).__name__, boolean=boolean):
                    observed = replace(
                        first.observation,
                        coverage_fraction=number,
                        continuous=boolean,
                        delay=Q.DelayEvidence(
                            first.observation.identity, number, reference()
                        ),
                    )
                    display = self.assert_json_projection(
                        replace(
                            supplied, inputs=(replace(first, observation=observed),)
                        )
                    )
                    row = display["provenance"]["input_metadata"][0]["observation"]
                    self.assertEqual(row["coverage_fraction"], expected)
                    self.assertIs(type(row["coverage_fraction"]), type(expected))
                    self.assertEqual(row["delay"]["seconds"], expected)
                    self.assertIs(type(row["continuous"]), type(boolean))
                    self.assertEqual(row["continuous"], boolean)

    def test_large_integer_under_tiny_decimal_context_and_nonterminating_ratio(self):
        supplied = result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
        number = 10**5000 + 1
        cap = replace(
            supplied.quality_policy.capabilities[0], max_event_age_seconds=number
        )
        dto = replace(
            supplied,
            vwap=Fraction(number, 3),
            quality_policy=replace(supplied.quality_policy, capabilities=(cap,)),
        )
        with localcontext() as ctx:
            ctx.prec = 1
            ctx.Emax = 2
            ctx.Emin = -2
            display = self.assert_json_projection(dto)
            self.assertEqual(ctx.prec, 1)
            self.assertEqual((ctx.Emax, ctx.Emin), (2, -2))
        literal = "1" + "0" * 4999 + "1"
        self.assertEqual(
            display["values"]["vwap"], {"numerator": literal, "denominator": "3"}
        )
        self.assertEqual(
            display["provenance"]["quality_policy"]["capabilities"][0][
                "max_event_age_seconds"
            ],
            literal,
        )
        self.assertEqual(sys.get_int_max_str_digits(), 4300)

    def test_large_ratio_withholding_keeps_large_metadata_safe(self):
        supplied = result(N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP)
        number = 10**5000
        first = supplied.inputs[0]
        observed = replace(first.observation, interval_seconds=number)
        for role in ("numeric", "coverage", "basis", "temporal"):
            for state in (Q.State.FAIL, Q.State.UNKNOWN, Q.State.NOT_REQUIRED):
                with self.subTest(role=role, state=state.value):
                    dto = replace(
                        supplied,
                        vwap=Fraction(number),
                        inputs=(replace(first, observation=observed),),
                        **{role: N.StructuralResult(state, ("original", "original"))},
                    )
                    display = self.assert_json_projection(dto)
                    self.assertEqual(
                        display["values"], {"high": None, "low": None, "vwap": None}
                    )
                    self.assertEqual(
                        display["provenance"]["input_metadata"][0]["observation"][
                            "interval_seconds"
                        ],
                        "1" + "0" * 5000,
                    )
                    self.assertEqual(
                        display["diagnostics"][role],
                        {"state": state.value, "reasons": ["original", "original"]},
                    )
                    self.assertEqual(dto.vwap, Fraction(number))

    def test_method_specific_values_exclude_unrelated_populated_fields(self):
        opening = replace(result(), vwap=Fraction(77, 19))
        self.assertIsNone(project_price_feature(opening)["values"]["vwap"])
        for method in (
            N.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP,
            N.PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY,
        ):
            with self.subTest(method=method):
                supplied = replace(
                    result(method), high=Decimal("999"), low=Decimal("998")
                )
                self.assertIsNone(project_price_feature(supplied)["values"]["high"])
                self.assertIsNone(project_price_feature(supplied)["values"]["low"])

    def test_no_raw_numeric_alias_or_authority_flag(self):
        display = project_price_feature(result())
        self.assertEqual(
            set(display),
            {
                "schema",
                "method",
                "method_label",
                "opening_minutes",
                "values",
                "diagnostics",
                "clocks",
                "provenance",
            },
        )
        for bar in display["provenance"]["input_metadata"]:
            self.assertEqual(
                set(bar),
                {
                    "observation",
                    "interval_start",
                    "interval_end",
                    "timestamp_convention",
                    "session",
                    "basis",
                    "notional_reference",
                },
            )
            for name in ("open", "high", "low", "close", "volume", "trade_notional"):
                self.assertNotIn(name, bar)
        self.assertEqual(
            display["provenance"]["semantics"], "declared_not_authenticated"
        )
        self.assertNotIn('"current"', json.dumps(display))

    def test_later_clocks_are_metadata_without_re_evaluation(self):
        supplied = result()
        late = END + timedelta(days=2, microseconds=1)
        bar = supplied.inputs[0]
        later_reference = reference("later:1", late)
        altered = replace(
            supplied,
            evaluation_at=late,
            inputs=(
                replace(
                    bar,
                    notional_reference=later_reference,
                    observation=replace(bar.observation, receipt_at=late),
                ),
            )
            + supplied.inputs[1:],
        )
        display = project_price_feature(altered)
        self.assertEqual(display["clocks"]["evaluation_at"]["iso"], late.isoformat())
        self.assertEqual(display["clocks"]["decision_at"]["iso"], END.isoformat())
        self.assertEqual(display["clocks"]["cutoff"]["iso"], END.isoformat())
        self.assertEqual(
            display["provenance"]["input_metadata"][0]["observation"]["receipt_at"][
                "iso"
            ],
            late.isoformat(),
        )
        self.assertEqual(
            display["provenance"]["input_metadata"][0]["notional_reference"][
                "available_at"
            ]["iso"],
            late.isoformat(),
        )
        self.assertEqual(display["values"]["high"], "104")
        self.assertEqual(display["diagnostics"]["temporal"]["state"], "pass")

    def test_clock_offset_and_fold_representation_preserved(self):
        supplied = result()
        eastern = ZoneInfo("America/New_York")
        for fold in (0, 1):
            for zone in (eastern, timezone(timedelta(hours=9)), timezone.utc):
                with self.subTest(fold=fold, zone=str(zone)):
                    clock = datetime(
                        2026, 11, 1, 1, 30, 0, 1, tzinfo=eastern, fold=fold
                    )
                    stamp = clock if zone is eastern else clock.astimezone(zone)
                    display = project_price_feature(
                        replace(supplied, evaluation_at=stamp)
                    )
                    encoded = display["clocks"]["evaluation_at"]
                    self.assertEqual(encoded["iso"], stamp.isoformat())
                    self.assertEqual(
                        encoded["utc"], stamp.astimezone(timezone.utc).isoformat()
                    )
                    self.assertEqual(encoded["fold"], stamp.fold)
                    self.assertEqual(
                        encoded["zone"], getattr(stamp.tzinfo, "key", str(stamp.tzinfo))
                    )

    def test_all_observation_and_reference_clocks_hashes_lineage_preserved(self):
        supplied = result()
        first, second = supplied.inputs
        revisions = (
            replace(
                first,
                observation=replace(
                    first.observation, revision_id="rev:old", evidence_id="old:1"
                ),
            ),
            replace(
                second,
                observation=replace(
                    second.observation,
                    revision_id="rev:new",
                    evidence_id="new:1",
                    revises_evidence_id="old:1",
                    evidence_sha256="e" * 64,
                ),
            ),
        )
        display = project_price_feature(replace(supplied, inputs=revisions))
        rows = display["provenance"]["input_metadata"]
        self.assertEqual(
            [r["observation"]["revision_id"] for r in rows], ["rev:old", "rev:new"]
        )
        self.assertEqual(
            [r["observation"]["evidence_id"] for r in rows], ["old:1", "new:1"]
        )
        self.assertEqual(rows[1]["observation"]["revises_evidence_id"], "old:1")
        self.assertEqual(rows[1]["observation"]["evidence_sha256"], "e" * 64)
        for source, row in zip(revisions, rows):
            for name in (
                "event_at",
                "receipt_at",
                "as_of_at",
                "available_at",
                "recorded_at",
            ):
                self.assertEqual(
                    row["observation"][name]["iso"],
                    getattr(source.observation, name).isoformat(),
                )
            self.assertEqual(row["notional_reference"]["sha256"], "a" * 64)
        self.assertEqual(
            display["provenance"]["grid"]["reference"]["evidence_id"], "grid:1"
        )
        self.assertEqual(
            display["provenance"]["quality_policy"]["provenance"],
            {"code_sha256": "c" * 64, "config_sha256": "d" * 64},
        )
        self.assertEqual(
            display["provenance"]["session_facts"][0]["reference"]["sha256"], "a" * 64
        )

    def test_unknown_metadata_remains_null(self):
        supplied = result()
        bar = supplied.inputs[0]
        altered = replace(
            supplied,
            inputs=(
                replace(
                    bar,
                    notional_reference=None,
                    observation=replace(
                        bar.observation, revision_id=None, receipt_at=None
                    ),
                ),
            ),
            grid=replace(supplied.grid, reference=None, intervals=None),
        )
        display = project_price_feature(altered)
        self.assertIsNone(display["provenance"]["grid"]["reference"])
        self.assertIsNone(display["provenance"]["grid"]["intervals"])
        row = display["provenance"]["input_metadata"][0]
        self.assertIsNone(row["notional_reference"])
        self.assertIsNone(row["observation"]["revision_id"])
        self.assertIsNone(row["observation"]["receipt_at"])

    def test_deterministic_and_caller_mutation_isolation_and_input_immutability(self):
        supplied = result()
        before = snapshot(supplied)
        first = project_price_feature(supplied)
        second = project_price_feature(supplied)
        self.assertEqual(
            json.dumps(first, allow_nan=False), json.dumps(second, allow_nan=False)
        )
        first["values"]["high"] = "999"
        first["diagnostics"]["numeric"]["reasons"].append("caller")
        first["provenance"]["input_metadata"][0]["observation"]["evidence_id"] = (
            "caller"
        )
        first["provenance"]["grid"]["session"]["segments"].clear()
        first["provenance"]["quality_policy"]["provenance"]["code_sha256"] = "caller"
        self.assertEqual(project_price_feature(supplied), second)
        self.assertEqual(snapshot(supplied), before)

    def test_json_types_and_roundtrip(self):
        for method in N.PriceFeatureMethod:
            with self.subTest(method=method):
                display = project_price_feature(result(method))
                self.assertEqual(
                    json.loads(json.dumps(display, allow_nan=False)), display
                )

                def walk(value):
                    self.assertIn(
                        type(value), (dict, list, str, int, float, bool, type(None))
                    )
                    if type(value) is dict:
                        self.assertTrue(all(type(k) is str for k in value))
                        for item in value.values():
                            walk(item)
                    if type(value) is list:
                        for item in value:
                            walk(item)

                walk(display)

    def test_exact_input_type_required(self):
        for value in (None, {}, "PriceFeatureResult", object()):
            with self.subTest(value=type(value).__name__):
                with self.assertRaisesRegex(
                    TypeError, "exact_price_feature_result_required"
                ):
                    project_price_feature(value)

    def test_collection_preserves_package_and_unrelated_import_resolution(self):
        package = sys.modules["analysis"]
        path = tuple(package.__path__)
        numeric = sys.modules["analysis.etf_price_features"]
        quality = sys.modules["analysis.feed_reliability_projection"]
        unrelated = importlib.import_module("analysis.temporal_feature_eligibility")
        importlib.import_module(__name__)
        self.assertIs(sys.modules["analysis"], package)
        self.assertEqual(tuple(package.__path__), path)
        self.assertIs(sys.modules["analysis.etf_price_features"], numeric)
        self.assertIs(sys.modules["analysis.feed_reliability_projection"], quality)
        self.assertIs(
            importlib.import_module("analysis.temporal_feature_eligibility"), unrelated
        )


if __name__ == "__main__":
    unittest.main()
