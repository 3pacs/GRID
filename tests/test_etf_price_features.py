"""Standalone synthetic contract oracles on unchanged accepted quality metadata."""

import ast
import importlib.util
import json
import sys
import types
import unittest
from dataclasses import FrozenInstanceError, fields, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext, localcontext
from fractions import Fraction
import hashlib
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "analysis/etf_price_features.py"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# A dormant package shell avoids repository/package/conftest entrypoints. The
# accepted module is loaded with its exact canonical identity before candidate
# import, so candidate uses only its public API, with no quality monkeypatch.
package = types.ModuleType("analysis")
package.__path__ = []
sys.modules["analysis"] = package
Q = load(
    "analysis.feed_reliability_projection",
    ROOT / "analysis/feed_reliability_projection.py",
)
M = load("numeric_contract_under_test", SOURCE)
FIXTURE = json.loads((ROOT / "tests/fixtures/etf_price_features_v1.json").read_text())
CONTROL_IDS = []
T = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)


def stamp(value):
    return datetime.fromisoformat(value)


def ref(name="fact:1", at=T - timedelta(hours=1), recorded=None):
    return Q.FactReference(name, "a" * 64, at, at if recorded is None else recorded)


def basis(**changes):
    values = dict(
        price_currency="USD",
        volume_unit="shares",
        notional_unit="USD",
        adjustment=M.Adjustment.RAW,
        basis_id="test-trades",
        basis_version="v1",
        adjustment_reference=ref(),
        notional_basis=M.NotionalBasis.TRADE,
    )
    values.update(changes)
    return M.PriceBasis(**values)


def session(start=T, end=T + timedelta(minutes=2), **changes):
    values = dict(
        instrument="TEST:SPY",
        exchange="TEST:X",
        session_id="test:day",
        timezone_id="UTC",
        open_at=start,
        close_at=end,
        calendar_id="test-calendar",
        calendar_version="v1",
        calendar_reference=ref(),
        convention=M.SessionConvention.REGULAR,
        segments=(M.BarInterval(start, end),),
    )
    values.update(changes)
    return M.SessionWindow(**values)


def observation(start=T, index=1, **changes):
    ident = Q.SourceIdentity(
        "TEST:SPY", "synthetic-price", "fixture", "ohlcv", "fixture-v1"
    )
    values = dict(
        identity=ident,
        evidence_id=f"bar:{index}",
        evidence_sha256="e" * 64,
        event_at=start,
        receipt_at=start + timedelta(seconds=1),
        as_of_at=start,
        available_at=start + timedelta(seconds=1),
        recorded_at=start + timedelta(seconds=1),
        presence=Q.Presence.PRESENT,
        delay=Q.DelayEvidence(ident, 0, ref()),
        interval_seconds=60,
        finality=Q.Finality.FINAL,
        revision_id=f"revision:{index}",
        revises_evidence_id=None,
        coverage_fraction=1,
        continuous=True,
        lifecycle=Q.LifecycleFacts(True, "generation:1", "generation:1", ref()),
        options=None,
        is_fallback=False,
        fallback_reference=None,
    )
    values.update(changes)
    return Q.Observation(**values)


def bar(start=T, end=T + timedelta(minutes=1), index=1, **changes):
    values = dict(
        observation=observation(start, index),
        interval_start=start,
        interval_end=end,
        timestamp_convention=M.TimestampConvention.START,
        session=session(),
        basis=basis(),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10"),
        volume=100,
        trade_notional=Decimal("1000"),
        notional_reference=ref(),
    )
    values.update(changes)
    return M.NumericBar(**values)


def sample():
    s, b = session(), basis()
    bars = (
        bar(session=s, basis=b),
        bar(
            T + timedelta(minutes=1),
            T + timedelta(minutes=2),
            index=2,
            session=s,
            basis=b,
        ),
    )
    g = M.ExpectedBarGrid(
        T,
        T + timedelta(minutes=2),
        s,
        b,
        (
            M.BarInterval(T, T + timedelta(minutes=1)),
            M.BarInterval(T + timedelta(minutes=1), T + timedelta(minutes=2)),
        ),
        ref(),
    )
    return bars, g


def fingerprint(value):
    """Independent, recursive, type-sensitive preservation oracle (no dict equality)."""
    tag = (type(value).__module__, type(value).__qualname__)
    if type(value) is datetime:
        return tag, (
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            value.microsecond,
            value.fold,
            str(value.tzinfo),
            value.utcoffset().days,
            value.utcoffset().seconds,
            value.utcoffset().microseconds,
        )
    if type(value) is Decimal:
        return tag, value.as_tuple()
    if is_dataclass(value):
        return tag, tuple(
            (f.name, fingerprint(getattr(value, f.name))) for f in fields(value)
        )
    if type(value) in (tuple, list):
        return tag, tuple(fingerprint(v) for v in value)
    if type(value) is dict:
        return tag, tuple((fingerprint(k), fingerprint(v)) for k, v in value.items())
    return tag, value


def quality(obs, checks=(Q.Check.FINALITY,), **changes):
    values = dict(
        name="structural-composition",
        expected_identity=obs.identity,
        evidence_id=obs.evidence_id,
        mode=Q.Mode.RESEARCH,
        required_checks=checks,
        expected_evidence_sha256=obs.evidence_sha256,
        max_future_skew_seconds=0,
        max_event_age_seconds=120,
        max_receipt_age_seconds=120,
        max_delay_seconds=0,
    )
    values.update(changes)
    cap = Q.CapabilityPolicy(**values)
    p = Q.EvaluationPolicy(
        "synthetic-v1", {"code_sha256": "a" * 64, "config_sha256": "b" * 64}, (cap,)
    )
    result = Q.evaluate_feed_quality(
        (obs,),
        p,
        now=T + timedelta(minutes=5),
        decision_at=T + timedelta(minutes=3),
        sessions=(),
    )
    return result.instruments[0].capabilities[0].state


class NumericSessionTests(unittest.TestCase):
    def check(self, label, actual, expected):
        CONTROL_IDS.append(label)
        self.assertEqual(actual, expected, label)

    def test_N01_valid_and_partial(self):
        self.check("N01-valid", M.validate_numeric_bar(bar()).state, Q.State.PASS)
        for field in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_notional",
            "notional_reference",
        ):
            with self.subTest(field=field):
                x = bar(**{field: None})
                self.assertIsNone(getattr(x, field))
                self.check(
                    "N01-partial-" + field,
                    M.validate_numeric_bar(x).state,
                    Q.State.UNKNOWN,
                )

    def test_N01_strict_prices(self):
        for value in (True, False, 10.0, 10):
            with self.subTest(all_prices=repr(value)):
                CONTROL_IDS.append("N01-all-prices-" + repr(value))
                with self.assertRaises(ValueError):
                    bar(open=value, high=value, low=value, close=value)
        for field in ("open", "high", "low", "close"):
            for value in (
                True,
                False,
                10,
                10.0,
                "10",
                Decimal("0"),
                Decimal("-0"),
                Decimal("-1"),
                Decimal("NaN"),
                Decimal("-NaN"),
                Decimal("sNaN"),
                Decimal("-sNaN"),
                Decimal("Infinity"),
                Decimal("-Infinity"),
            ):
                with self.subTest(field=field, value=repr(value)):
                    CONTROL_IDS.append("N01-price-" + field + "-" + repr(value))
                    with self.assertRaises(ValueError):
                        bar(**{field: value})

    def test_N01_volume_and_notional(self):
        for value in (True, False, -1, 100.0, Decimal("100"), "100"):
            with self.subTest(value=repr(value)):
                CONTROL_IDS.append("N01-volume-" + repr(value))
                with self.assertRaises(ValueError):
                    bar(volume=value)
        for value in (
            True,
            False,
            1000,
            1000.0,
            "1000",
            Decimal("NaN"),
            Decimal("sNaN"),
            Decimal("-NaN"),
            Decimal("-sNaN"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
        ):
            with self.subTest(value=repr(value)):
                CONTROL_IDS.append("N01-notional-" + repr(value))
                with self.assertRaises(ValueError):
                    bar(trade_notional=value)
        for value in (0, 10**2000):
            self.check(
                "N01-volume-exact-" + str(value.bit_length()),
                bar(volume=value).volume,
                value,
            )

    def test_N01_price_relations(self):
        for changes in (
            {"low": Decimal("12")},
            {"high": Decimal("8")},
            {"open": Decimal("12")},
            {"close": Decimal("8")},
            {"low": None, "open": Decimal("12")},
            {"high": None, "close": Decimal("8")},
        ):
            with self.subTest(changes=changes):
                CONTROL_IDS.append("N01-range-" + repr(changes))
                with self.assertRaises(ValueError):
                    bar(**changes)
        self.check(
            "N01-flat-range",
            M.validate_numeric_bar(
                bar(
                    open=Decimal("9"),
                    high=Decimal("9"),
                    low=Decimal("9"),
                    close=Decimal("9"),
                )
            ).state,
            Q.State.PASS,
        )

    def test_N02_fixture_values_no_calculations(self):
        raw_before = fingerprint(FIXTURE)
        refs = {
            name: Q.FactReference(
                v["evidence_id"],
                v["sha256"],
                stamp(v["available_at"]),
                stamp(v["recorded_at"]),
            )
            for name, v in FIXTURE["references"].items()
        }
        sv, bv = FIXTURE["session"], FIXTURE["basis"]
        s = M.SessionWindow(
            FIXTURE["instrument"],
            sv["exchange"],
            sv["session_id"],
            sv["timezone_id"],
            stamp(sv["open_at"]),
            stamp(sv["close_at"]),
            sv["calendar_id"],
            sv["calendar_version"],
            refs["calendar"],
            M.SessionConvention(sv["convention"]),
            (M.BarInterval(stamp(sv["open_at"]), stamp(sv["close_at"])),),
        )
        b = M.PriceBasis(
            bv["price_currency"],
            bv["volume_unit"],
            bv["notional_unit"],
            M.Adjustment(bv["adjustment"]),
            bv["basis_id"],
            bv["basis_version"],
            refs["adjustment"],
            M.NotionalBasis(bv["notional_basis"]),
        )
        bars = []
        for index, row in enumerate(FIXTURE["bars"]):
            obs = observation(
                stamp(row["start"]),
                index + 1,
                evidence_id=row["evidence_id"],
                evidence_sha256=row["evidence_sha256"],
                revision_id=row["revision_id"],
                interval_seconds=row["interval_seconds"],
            )
            x = bar(
                stamp(row["start"]),
                stamp(row["end"]),
                observation=obs,
                session=s,
                basis=b,
                **{
                    f: Decimal(row[f])
                    for f in ("open", "high", "low", "close", "trade_notional")
                },
                volume=row["volume"],
                notional_reference=refs["notional"],
            )
            bars.append(x)
            for field in ("open", "high", "low", "close", "trade_notional"):
                self.check(
                    "N02-exact-" + str(index) + "-" + field,
                    getattr(x, field).as_tuple(),
                    Decimal(row[field]).as_tuple(),
                )
            self.assertIs(x.observation, obs)
            self.assertIs(x.session, s)
            self.assertIs(x.basis, b)
        self.check("N02-volume-literal", tuple(x.volume for x in bars), (100, 300))
        self.check(
            "N02-notional-literal",
            tuple(str(x.trade_notional) for x in bars),
            ("1000", "3600"),
        )
        self.check("N02-input-preserved", fingerprint(FIXTURE), raw_before)
        self.assertIsNone(FIXTURE["held_original_P01"]["computed_outputs"])
        self.assertIsNone(FIXTURE["held_original_P02"]["computed_outputs"])

    def test_N02_unknown_zero_and_basis(self):
        x = bar(volume=0, trade_notional=Decimal("0"))
        self.check(
            "N02-reported-zero",
            (x.volume, x.trade_notional.as_tuple()),
            (0, Decimal("0").as_tuple()),
        )
        missing = bar(volume=None, trade_notional=None)
        self.assertIsNone(missing.volume)
        self.assertIsNone(missing.trade_notional)
        self.check(
            "N02-no-fallback", M.validate_numeric_bar(missing).state, Q.State.UNKNOWN
        )
        b = basis()
        for field, value in (
            ("price_currency", "EUR"),
            ("volume_unit", "contracts"),
            ("notional_unit", "EUR"),
            ("adjustment", M.Adjustment.SPLIT),
            ("basis_id", "other"),
            ("basis_version", "v2"),
            ("notional_basis", M.NotionalBasis.OHLCV_PROXY),
        ):
            self.check(
                "N02-incompatible-" + field,
                M.compare_price_basis(b, replace(b, **{field: value})).state,
                Q.State.FAIL,
            )
        for field, value in (
            ("price_currency", None),
            ("volume_unit", None),
            ("notional_unit", None),
            ("basis_id", None),
            ("basis_version", None),
            ("adjustment_reference", None),
            ("adjustment", M.Adjustment.UNKNOWN),
            ("notional_basis", M.NotionalBasis.UNKNOWN),
        ):
            u = replace(b, **{field: value})
            self.check(
                "N02-unknown-" + field,
                M.compare_price_basis(u, u).state,
                Q.State.UNKNOWN,
            )
        self.check(
            "N02-explicit-basis-match",
            M.compare_price_basis(b, replace(b)).state,
            Q.State.PASS,
        )

    def test_N02_whole_adjustment_reference(self):
        b = basis()
        r = b.adjustment_reference
        for field, value in (
            ("evidence_id", "other:fact"),
            ("sha256", "b" * 64),
            ("available_at", r.available_at + timedelta(microseconds=1)),
            ("recorded_at", r.recorded_at + timedelta(microseconds=1)),
        ):
            self.check(
                "N02-reference-" + field,
                M.compare_price_basis(
                    b, replace(b, adjustment_reference=replace(r, **{field: value}))
                ).state,
                Q.State.FAIL,
            )
        ny = ZoneInfo("America/New_York")
        a = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=0)
        z = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=1)
        self.check(
            "N02-reference-fold",
            M.compare_price_basis(
                basis(adjustment_reference=ref(at=a)),
                basis(adjustment_reference=ref(at=z)),
            ).state,
            Q.State.FAIL,
        )

    def test_N02_partial_reference_components_stay_unknown(self):
        bars, g = sample()
        for field in ("evidence_id", "sha256", "available_at", "recorded_at"):
            with self.subTest(field=field):
                partial = replace(ref(), **{field: None})
                b = replace(g.basis, adjustment_reference=partial)
                self.check(
                    "N02-partial-adjustment-" + field,
                    M.compare_price_basis(b, b).state,
                    Q.State.UNKNOWN,
                )
                self.check(
                    "N02-partial-notional-" + field,
                    M.validate_numeric_bar(
                        replace(bars[0], notional_reference=partial)
                    ).state,
                    Q.State.UNKNOWN,
                )
                self.check(
                    "N02-partial-grid-" + field,
                    M.validate_bar_grid(bars, replace(g, reference=partial)).state,
                    Q.State.UNKNOWN,
                )
                s = replace(g.session, calendar_reference=partial)
                self.check(
                    "N02-partial-calendar-" + field,
                    M.validate_bar_grid(
                        tuple(replace(x, session=s) for x in bars),
                        replace(g, session=s),
                    ).state,
                    Q.State.UNKNOWN,
                )

    def test_N03_timezone_overnight_earlyclose(self):
        offset = timezone(timedelta(hours=5, minutes=30))
        a = datetime(2026, 1, 5, 23, 59, tzinfo=offset)
        z = datetime(2026, 1, 6, 0, 1, tzinfo=offset)
        s = session(a, z, timezone_id="supplied/overnight")
        obs = observation(a.astimezone(timezone.utc), interval_seconds=120)
        x = bar(a, z, observation=obs, session=s)
        self.check(
            "N03-overnight-equivalent", M.validate_numeric_bar(x).state, Q.State.PASS
        )
        self.assertIs(x.interval_start, a)
        self.assertIs(s.close_at, z)
        early = session(T, T + timedelta(minutes=1), calendar_version="early-close-v1")
        self.check(
            "N03-close-exact",
            M.validate_numeric_bar(bar(session=early)).state,
            Q.State.PASS,
        )
        self.check(
            "N03-close-plus-microsecond",
            M.validate_numeric_bar(
                bar(end=T + timedelta(minutes=1, microseconds=1), session=early)
            ).state,
            Q.State.FAIL,
        )

    def test_N03_fold_and_clock_boundaries(self):
        ny = ZoneInfo("America/New_York")
        a = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=0)
        z = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=1)
        s = session(a, z, timezone_id="America/New_York")
        x = bar(a, z, observation=observation(a, interval_seconds=3600), session=s)
        self.check(
            "N03-fold-distinct-positive", M.validate_numeric_bar(x).state, Q.State.PASS
        )
        with self.assertRaises(ValueError):
            M.BarInterval(z, a)
        end_obs = replace(x.observation, event_at=z)
        self.check(
            "N03-fold-start-mismatch",
            M.validate_numeric_bar(replace(x, observation=end_obs)).state,
            Q.State.FAIL,
        )
        self.check(
            "N03-fold-end-match",
            M.validate_numeric_bar(
                replace(
                    x,
                    observation=end_obs,
                    timestamp_convention=M.TimestampConvention.END,
                )
            ).state,
            Q.State.PASS,
        )
        for a, z in (
            (T, T),
            (T + timedelta(microseconds=1), T),
            (T.replace(tzinfo=None), T),
            (True, T),
            (datetime.min.replace(tzinfo=timezone(timedelta(hours=1))), T),
        ):
            with self.subTest(start=repr(a), end=repr(z)):
                CONTROL_IDS.append("N03-malformed-clock")
                with self.assertRaises(ValueError):
                    M.BarInterval(a, z)

    def test_N03_unknown_session_never_complete(self):
        bars, g = sample()
        for field, value in (
            ("exchange", None),
            ("session_id", None),
            ("timezone_id", None),
            ("calendar_id", None),
            ("calendar_version", None),
            ("calendar_reference", None),
            ("convention", M.SessionConvention.UNKNOWN),
            ("segments", None),
        ):
            with self.subTest(field=field):
                s = replace(g.session, **{field: value})
                x = tuple(replace(b, session=s) for b in bars)
                self.check(
                    "N03-unknown-session-" + field,
                    M.validate_bar_grid(x, replace(g, session=s)).state,
                    Q.State.UNKNOWN,
                )

    def test_N03_invalid_segments_and_identifiers(self):
        for segments in (
            [M.BarInterval(T, T + timedelta(minutes=1))],
            (M.BarInterval(T - timedelta(microseconds=1), T),),
            (M.BarInterval(T, T + timedelta(minutes=3)),),
            (
                M.BarInterval(T + timedelta(minutes=1), T + timedelta(minutes=2)),
                M.BarInterval(T, T + timedelta(minutes=1)),
            ),
            (
                M.BarInterval(T, T + timedelta(minutes=2)),
                M.BarInterval(T + timedelta(minutes=1), T + timedelta(minutes=2)),
            ),
        ):
            with self.subTest(segments=segments):
                CONTROL_IDS.append("N03-invalid-segments")
                with self.assertRaises(ValueError):
                    session(segments=segments)
        for value in (True, 1, "", "contains space", "x" * 129):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    basis(price_currency=value)
        with self.assertRaises(ValueError):
            session(instrument="SPY")

    def test_N04_grid_complete_missing_and_unknown(self):
        bars, g = sample()
        self.check(
            "N04-complete",
            M.validate_bar_grid(bars, g),
            M.StructuralResult(Q.State.PASS, ("structural_match",)),
        )
        self.check(
            "N04-missing",
            M.validate_bar_grid(bars[:1], g).reasons,
            ("missing_expected_interval",),
        )
        self.check(
            "N04-unknown-grid",
            M.validate_bar_grid(bars, replace(g, intervals=None)).state,
            Q.State.UNKNOWN,
        )
        self.assertIn(
            "unknown_expected_grid",
            M.validate_bar_grid((), replace(g, intervals=None)).reasons,
        )
        self.check(
            "N04-empty-required-grid",
            M.validate_bar_grid((), replace(g, intervals=())).state,
            Q.State.FAIL,
        )
        no_trading = replace(g.session, segments=())
        self.check(
            "N04-explicit-no-trading",
            M.validate_bar_grid((), replace(g, session=no_trading, intervals=())).state,
            Q.State.PASS,
        )
        self.check(
            "N04-unknown-grid-source",
            M.validate_bar_grid(bars, replace(g, reference=None)).state,
            Q.State.UNKNOWN,
        )

    def test_N04_duplicate_overlap_order_revision(self):
        bars, g = sample()
        cases = (
            (
                bars + (bars[0],),
                (
                    "observed_duplicate_interval",
                    "duplicate_evidence_id",
                    "duplicate_revision_id",
                ),
            ),
            (bars[::-1], ("observed_out_of_order",)),
            (
                (
                    bars[0],
                    replace(
                        bars[1],
                        interval_start=T + timedelta(seconds=30),
                        observation=replace(
                            bars[1].observation, event_at=T + timedelta(seconds=30)
                        ),
                    ),
                ),
                ("observed_overlap",),
            ),
            (
                (
                    bars[0],
                    replace(
                        bars[0],
                        observation=replace(
                            bars[0].observation,
                            evidence_id="late:correction",
                            revision_id="revision:late",
                            evidence_sha256="b" * 64,
                        ),
                    ),
                ),
                ("conflicting_interval_revision",),
            ),
        )
        for seq, reasons in cases:
            before = fingerprint(seq)
            result = M.validate_bar_grid(seq, g)
            self.check("N04-sequence-failure", result.state, Q.State.FAIL)
            for reason in reasons:
                self.assertIn(reason, result.reasons)
            self.check("N04-no-repair", fingerprint(seq), before)

    def test_N04_expected_grid_adversaries(self):
        bars, g = sample()
        cases = (
            (g.intervals[:1], "expected_grid_gap"),
            (g.intervals[::-1], "expected_out_of_order"),
            (g.intervals + (g.intervals[0],), "expected_duplicate_interval"),
            (
                (
                    M.BarInterval(T - timedelta(seconds=1), T + timedelta(minutes=1)),
                    g.intervals[1],
                ),
                "expected_outside_analysis",
            ),
            (
                (M.BarInterval(T, T + timedelta(minutes=2)), g.intervals[1]),
                "expected_overlap",
            ),
        )
        for seq, reason in cases:
            with self.subTest(reason=reason):
                result = M.validate_bar_grid(bars, replace(g, intervals=seq))
                self.check(
                    "N04-expected-adversary-" + reason, result.state, Q.State.FAIL
                )
                self.assertIn(reason, result.reasons)
        with self.assertRaises(ValueError):
            replace(g, intervals=list(g.intervals))
        with self.assertRaises(ValueError):
            M.validate_bar_grid(list(bars), g)

    def test_N04_break_auction_and_analysis_edges(self):
        s = session(
            T,
            T + timedelta(minutes=3),
            segments=(
                M.BarInterval(T, T + timedelta(minutes=1)),
                M.BarInterval(T + timedelta(minutes=2), T + timedelta(minutes=3)),
            ),
        )
        b = basis()
        seq = (
            bar(session=s, basis=b),
            bar(
                T + timedelta(minutes=2),
                T + timedelta(minutes=3),
                index=2,
                session=s,
                basis=b,
            ),
        )
        g = M.ExpectedBarGrid(T, T + timedelta(minutes=3), s, b, s.segments, ref())
        self.check(
            "N04-explicit-break-skipped",
            M.validate_bar_grid(seq, g).state,
            Q.State.PASS,
        )
        cross = bar(T, T + timedelta(minutes=3), session=s, basis=b)
        self.check(
            "N04-break-crossing",
            M.validate_bar_grid(
                (cross,),
                replace(g, intervals=(M.BarInterval(T, T + timedelta(minutes=3)),)),
            ).state,
            Q.State.FAIL,
        )
        auction = replace(s, convention=M.SessionConvention.AUCTION)
        self.check(
            "N04-auction-not-inferred",
            M.validate_bar_grid(
                tuple(replace(x, session=auction) for x in seq), g
            ).state,
            Q.State.FAIL,
        )
        self.check(
            "N04-explicit-auction",
            M.validate_bar_grid(
                tuple(replace(x, session=auction) for x in seq),
                replace(g, session=auction),
            ).state,
            Q.State.PASS,
        )
        self.check(
            "N04-analysis-outside",
            M.validate_bar_grid(
                seq, replace(g, interval_start=T - timedelta(seconds=1))
            ).state,
            Q.State.FAIL,
        )
        self.check(
            "N04-analysis-start-in-break",
            M.validate_bar_grid(
                (),
                replace(
                    g,
                    interval_start=T + timedelta(minutes=1, seconds=1),
                    interval_end=T + timedelta(minutes=1, seconds=2),
                    intervals=(),
                ),
            ).state,
            Q.State.FAIL,
        )

    def test_N04_basis_instrument_and_timestamp_unknowns(self):
        bars, g = sample()
        different = replace(bars[0], basis=replace(g.basis, price_currency="EUR"))
        self.check(
            "N04-basis-no-coercion",
            M.validate_bar_grid((different, bars[1]), g).state,
            Q.State.FAIL,
        )
        wrong = replace(
            bars[0],
            observation=replace(
                bars[0].observation,
                identity=replace(bars[0].observation.identity, instrument="TEST:QQQ"),
            ),
        )
        self.check(
            "N04-instrument-no-mixing",
            M.validate_bar_grid((wrong, bars[1]), g).state,
            Q.State.FAIL,
        )
        for x in (
            replace(bars[0], timestamp_convention=M.TimestampConvention.UNKNOWN),
            replace(bars[0], observation=replace(bars[0].observation, event_at=None)),
            replace(
                bars[0], observation=replace(bars[0].observation, revision_id=None)
            ),
        ):
            self.check(
                "N04-missing-declaration",
                M.validate_bar_grid((x, bars[1]), g).state,
                Q.State.UNKNOWN,
            )
        wrong_stamp = replace(
            bars[0],
            observation=replace(bars[0].observation, event_at=T + timedelta(seconds=1)),
        )
        self.check(
            "N04-wrong-event",
            M.validate_bar_grid((wrong_stamp, bars[1]), g).state,
            Q.State.FAIL,
        )

    def test_N05_structure_does_not_promote_quality(self):
        original = observation()
        cases = (
            (
                replace(original, delay=Q.DelayEvidence(original.identity, 900, ref())),
                (Q.Check.DELAY,),
                Q.State.FAIL,
            ),
            (
                replace(original, finality=Q.Finality.PROVISIONAL),
                (Q.Check.FINALITY,),
                Q.State.FAIL,
            ),
            (
                replace(original, finality=Q.Finality.UNKNOWN),
                (Q.Check.FINALITY,),
                Q.State.UNKNOWN,
            ),
            (
                replace(
                    original,
                    lifecycle=Q.LifecycleFacts(
                        False, "generation:1", "generation:1", ref()
                    ),
                ),
                (Q.Check.LIFECYCLE,),
                Q.State.FAIL,
            ),
            (
                replace(
                    original,
                    available_at=T + timedelta(minutes=4),
                    receipt_at=T + timedelta(minutes=4),
                    recorded_at=T + timedelta(minutes=4),
                ),
                (Q.Check.FINALITY,),
                Q.State.FAIL,
            ),
        )
        for obs, checks, expected in cases:
            x = bar(observation=obs)
            self.assertIs(x.observation, obs)
            self.check(
                "N05-numeric-valid-" + str(expected),
                M.validate_numeric_bar(x).state,
                Q.State.PASS,
            )
            self.check(
                "N05-independent-quality-" + str(expected),
                quality(obs, checks),
                expected,
            )
            self.assertFalse(hasattr(x, "eligible"))
            self.assertFalse(hasattr(M.validate_numeric_bar(x), "eligible"))

    def test_N06_all_metadata_identity_and_late_correction(self):
        o = observation(
            finality=Q.Finality.PROVISIONAL,
            options=Q.OptionsFacts(T - timedelta(days=1), None, None, ref("options:1")),
            fallback_reference=ref("fallback:1"),
        )
        before = fingerprint(o)
        refs = tuple(
            ref(n, T + timedelta(days=10), T + timedelta(days=11))
            for n in (
                "adjustment:future",
                "calendar:future",
                "grid:future",
                "notional:future",
            )
        )
        b, s = basis(adjustment_reference=refs[0]), session(calendar_reference=refs[1])
        x = bar(observation=o, basis=b, session=s, notional_reference=refs[3])
        g = M.ExpectedBarGrid(
            T,
            T + timedelta(minutes=1),
            s,
            b,
            (M.BarInterval(T, T + timedelta(minutes=1)),),
            refs[2],
        )
        self.check(
            "N06-nested-references-not-adjudicated",
            M.validate_bar_grid((x,), g).state,
            Q.State.PASS,
        )
        self.assertIs(x.observation, o)
        self.assertIs(b.adjustment_reference, refs[0])
        self.assertIs(s.calendar_reference, refs[1])
        self.assertIs(g.reference, refs[2])
        self.assertIs(x.notional_reference, refs[3])
        self.check("N06-all-observation-fields-preserved", fingerprint(o), before)
        self.assertIs(x.observation.finality, Q.Finality.PROVISIONAL)
        correction = replace(
            o,
            evidence_id="correction:late",
            revision_id="revision:late",
            revises_evidence_id=o.evidence_id,
            available_at=T + timedelta(minutes=4),
            receipt_at=T + timedelta(minutes=4),
            recorded_at=T + timedelta(minutes=4),
        )
        late = replace(x, observation=correction, high=Decimal("12"))
        both_before = fingerprint((x, late))
        self.check(
            "N06-conflict-retained",
            M.validate_bar_grid((x, late), g).state,
            Q.State.FAIL,
        )
        self.check(
            "N06-original-and-correction-retained", fingerprint((x, late)), both_before
        )
        self.check("N06-late-quality-fails", quality(correction), Q.State.FAIL)

    def test_N07_immutability_types_and_determinism(self):
        bars, g = sample()
        before = fingerprint((bars, g))
        first = M.validate_bar_grid(bars, g)
        for _ in range(5):
            self.check("N07-repeat", M.validate_bar_grid(bars, g), first)
        self.check("N07-whole-input-preserved", fingerprint((bars, g)), before)
        for value, field, replacement in (
            (bars[0], "volume", 0),
            (g, "intervals", ()),
            (g.session, "session_id", "other"),
            (g.basis, "price_currency", "EUR"),
            (g.intervals[0], "start", T),
            (first, "state", Q.State.FAIL),
        ):
            with self.subTest(type=type(value).__name__):
                with self.assertRaises(FrozenInstanceError):
                    setattr(value, field, replacement)
        for a, b in (
            (True, 1),
            (False, 0),
            (None, 0),
            (Decimal("1.0"), Decimal("1.00")),
            ([1, 2], [2, 1]),
            ([1, 2], (1, 2)),
            (-1, 1),
        ):
            self.assertNotEqual(fingerprint(a), fingerprint(b))

    def test_N07_constructor_preserves_every_original_value(self):
        original = observation(
            finality=Q.Finality.UNKNOWN,
            continuous=None,
            revision_id="revision:original",
            revises_evidence_id="older:bar",
        )
        refs = tuple(
            ref(n, T + timedelta(days=i), T + timedelta(days=i, seconds=1))
            for i, n in enumerate(
                (
                    "adjustment:original",
                    "calendar:original",
                    "grid:original",
                    "notional:original",
                )
            )
        )
        b = basis(adjustment_reference=refs[0])
        s = session(calendar_reference=refs[1])
        inputs = dict(
            observation=original,
            interval_start=T,
            interval_end=T + timedelta(minutes=1),
            timestamp_convention=M.TimestampConvention.START,
            session=s,
            basis=b,
            open=Decimal("10.0000000000000000000000000000000000000000001"),
            high=Decimal("11.00"),
            low=Decimal("9.000"),
            close=Decimal("10.0"),
            volume=100,
            trade_notional=None,
            notional_reference=refs[3],
        )
        before = fingerprint(inputs)
        x = M.NumericBar(**inputs)
        actual = {f.name: getattr(x, f.name) for f in fields(x)}
        self.check("N07-complete-constructor-preservation", fingerprint(actual), before)
        self.check("N07-original-values-unmodified", fingerprint(inputs), before)
        for name in inputs:
            self.assertIs(getattr(x, name), inputs[name])
        grid_inputs = dict(
            interval_start=T,
            interval_end=T + timedelta(minutes=1),
            session=s,
            basis=b,
            intervals=(M.BarInterval(T, T + timedelta(minutes=1)),),
            reference=refs[2],
        )
        before_grid = fingerprint(grid_inputs)
        g = M.ExpectedBarGrid(**grid_inputs)
        M.validate_bar_grid((x,), g)
        self.check(
            "N07-grid-complete-preservation",
            fingerprint({f.name: getattr(g, f.name) for f in fields(g)}),
            before_grid,
        )

    def test_N07_wrong_public_types_rejected(self):
        bars, g = sample()
        for construct in (
            lambda: replace(bars[0], observation=None),
            lambda: replace(bars[0], basis=None),
            lambda: replace(bars[0], session=None),
            lambda: replace(bars[0], timestamp_convention="start"),
            lambda: replace(g.basis, adjustment="raw"),
            lambda: replace(g.basis, notional_basis="trade"),
            lambda: replace(g.session, convention="regular"),
            lambda: replace(g, reference="hash"),
            lambda: replace(g, intervals=(True,)),
            lambda: M.validate_numeric_bar(None),
            lambda: M.compare_price_basis(True, g.basis),
            lambda: M.validate_bar_grid((True,), g),
        ):
            with self.subTest(construct=construct):
                CONTROL_IDS.append("N07-wrong-public-type")
                with self.assertRaises(ValueError):
                    construct()

    def test_N09_public_import_surface_and_no_effect_operations(self):
        tree = ast.parse(SOURCE.read_text())
        duration_function = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "_observation_duration"
        )
        duration_nodes = {id(n) for n in ast.walk(duration_function)}
        math_function = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "_feature_result"
        )
        math_nodes = {id(n) for n in ast.walk(math_function)}
        allowed_duration_products = {
            "elapsed.days * 86400",
            "(elapsed.days * 86400 + elapsed.seconds) * 1000000",
            "seconds * 1000000",
            "microseconds * denominator",
            "numerator * 1000000",
        }
        imports = [
            (n.module, tuple(a.name for a in n.names))
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom)
        ]
        allowed = {
            "__future__",
            "dataclasses",
            "datetime",
            "decimal",
            "enum",
            "fractions",
            "zoneinfo",
            "analysis.feed_reliability_projection",
        }
        for module, names in imports:
            self.assertIn(module, allowed)
            if module == "analysis.feed_reliability_projection":
                self.assertIn(
                    names,
                    (
                        ("FactReference", "Observation", "State"),
                        (
                            "Check",
                            "EvaluationPolicy",
                            "SessionFacts",
                            "evaluate_feed_quality",
                        ),
                    ),
                )
        self.assertFalse(any(isinstance(n, ast.Import) for n in ast.walk(tree)))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    self.assertNotIn(
                        node.func.attr,
                        (
                            "now",
                            "today",
                            "utcnow",
                            "timestamp",
                            "total_seconds",
                            "getenv",
                            "setcontext",
                            "fromisoformat",
                            "fromtimestamp",
                        ),
                    )
                elif isinstance(node.func, ast.Name):
                    self.assertNotIn(
                        node.func.id,
                        ("open", "eval", "exec", "__import__", "float", "int", "round"),
                    )
            if isinstance(node, ast.BinOp):
                self.assertFalse(isinstance(node.op, ast.Pow))
                if isinstance(node.op, ast.Div):
                    self.assertIn(id(node), math_nodes)
                if isinstance(node.op, ast.Mult) and id(node) not in math_nodes:
                    self.assertIn(id(node), duration_nodes)
                    self.assertIn(ast.unparse(node), allowed_duration_products)
        self.assertIs(M.FactReference, Q.FactReference)
        self.assertIs(M.Observation, Q.Observation)
        self.assertIs(M.State, Q.State)
        self.assertFalse(
            any(
                x in M.__all__
                for x in ("eligible", "current", "signal", "evaluate_feed_quality")
            )
        )

    def test_N10_exact_decimal_context_boundaries(self):
        before = getcontext().copy()
        values = (
            Decimal("1E-999999"),
            Decimal("1E+999999"),
            Decimal("9.0000000000000000000000000000000000000000000000000000001"),
        )
        for prec in (1, 3, 28, 100):
            with localcontext() as ctx:
                ctx.prec = prec
                for value in values:
                    x = bar(
                        open=value,
                        high=value,
                        low=value,
                        close=value,
                        trade_notional=value,
                    )
                    self.check(
                        "N10-exact-decimal-" + str(prec),
                        x.open.as_tuple(),
                        value.as_tuple(),
                    )
                    self.check(
                        "N10-context-independent-" + str(prec),
                        M.validate_numeric_bar(x).state,
                        Q.State.PASS,
                    )
                self.assertEqual(ctx.prec, prec)
        self.check("N10-global-context", str(getcontext()), str(before))

    def test_N10_wide_datetime_and_microsecond_grids(self):
        a = datetime.min.replace(tzinfo=timezone.utc)
        z = datetime.max.replace(tzinfo=timezone.utc, microsecond=0)
        s = session(a, z)
        obs = observation(event_at=a, interval_seconds=315537897599)
        x = bar(a, z, observation=obs, session=s)
        g = M.ExpectedBarGrid(a, z, s, x.basis, (M.BarInterval(a, z),), ref())
        self.check(
            "N10-wide-datetime", M.validate_bar_grid((x,), g).state, Q.State.PASS
        )
        s = session(T, T + timedelta(microseconds=2))
        seq = (
            bar(
                T,
                T + timedelta(microseconds=1),
                observation=observation(T, 1, interval_seconds=0.000001),
                session=s,
            ),
            bar(
                T + timedelta(microseconds=1),
                T + timedelta(microseconds=2),
                observation=observation(
                    T + timedelta(microseconds=1), 2, interval_seconds=0.000001
                ),
                session=s,
            ),
        )
        g = M.ExpectedBarGrid(
            T,
            T + timedelta(microseconds=2),
            s,
            seq[0].basis,
            tuple(M.BarInterval(x.interval_start, x.interval_end) for x in seq),
            ref(),
        )
        self.check(
            "N10-microsecond-geometric-cover-declaration-inexact",
            M.validate_bar_grid(seq, g).state,
            Q.State.FAIL,
        )
        self.check(
            "N10-microsecond-gap", M.validate_bar_grid(seq[:1], g).state, Q.State.FAIL
        )

    def test_F01_original_eight_cases_and_equality(self):
        for declared, expected in (
            (60, Q.State.PASS),
            (60.0, Q.State.PASS),
            (30, Q.State.FAIL),
            (60.000001, Q.State.FAIL),
            (61, Q.State.FAIL),
            (None, Q.State.UNKNOWN),
        ):
            with self.subTest(declared=declared):
                original = observation(interval_seconds=declared)
                before = fingerprint(original)
                x = bar(observation=original)
                g = M.ExpectedBarGrid(
                    T,
                    T + timedelta(minutes=1),
                    x.session,
                    x.basis,
                    (M.BarInterval(T, T + timedelta(minutes=1)),),
                    ref(),
                )
                for name, result in (
                    ("numeric", M.validate_numeric_bar(x)),
                    ("grid", M.validate_bar_grid((x,), g)),
                ):
                    self.check(
                        "F01-original-" + name + "-" + repr(declared),
                        result.state,
                        expected,
                    )
                    reason = (
                        "observation_duration_mismatch"
                        if expected is Q.State.FAIL
                        else "unknown_observation_duration"
                        if expected is Q.State.UNKNOWN
                        else None
                    )
                    if reason:
                        self.assertIn(reason, result.reasons)
                self.assertIs(x.observation, original)
                self.check(
                    "F01-declaration-and-clocks-preserved",
                    fingerprint(original),
                    before,
                )

    def test_F01_binary_rational_fraction_boundaries(self):
        cases = (
            (500000, 0.5, Q.State.PASS),
            (250000, 0.25, Q.State.PASS),
            (15625, 0.015625, Q.State.PASS),
            (125000, 0.125, Q.State.PASS),
            (499999, 0.5, Q.State.FAIL),
            (500001, 0.5, Q.State.FAIL),
            (1, 0.000001, Q.State.FAIL),
            (100000, 0.1, Q.State.FAIL),
            (300000, 0.3, Q.State.FAIL),
            (1000001, 1.000001, Q.State.FAIL),
            (1, float.fromhex("0x0.0000000000001p-1022"), Q.State.FAIL),
            (60000000, float.fromhex("0x1.fffffffffffffp+1023"), Q.State.FAIL),
            (60000000, 10**2000, Q.State.FAIL),
        )
        for us, seconds, expected in cases:
            with self.subTest(microseconds=us, seconds=seconds):
                end = T + timedelta(microseconds=us)
                x = bar(
                    T,
                    end,
                    observation=observation(interval_seconds=seconds),
                    session=session(T, end),
                )
                g = M.ExpectedBarGrid(
                    T, end, x.session, x.basis, (M.BarInterval(T, end),), ref()
                )
                self.check(
                    "F01-rational-numeric-" + str(us),
                    M.validate_numeric_bar(x).state,
                    expected,
                )
                self.check(
                    "F01-rational-grid-" + str(us),
                    M.validate_bar_grid((x,), g).state,
                    expected,
                )

    def test_F01_wide_span_and_fold_duration_exactness(self):
        a = datetime.min.replace(tzinfo=timezone.utc)
        z = datetime.max.replace(tzinfo=timezone.utc, microsecond=0)
        s = session(a, z)
        for declared, expected in (
            (315537897599, Q.State.PASS),
            (315537897599.0, Q.State.PASS),
            (315537897598, Q.State.FAIL),
            (None, Q.State.UNKNOWN),
        ):
            x = bar(
                a,
                z,
                observation=observation(event_at=a, interval_seconds=declared),
                session=s,
            )
            g = M.ExpectedBarGrid(a, z, s, x.basis, (M.BarInterval(a, z),), ref())
            self.check("F01-wide-numeric", M.validate_numeric_bar(x).state, expected)
            self.check("F01-wide-grid", M.validate_bar_grid((x,), g).state, expected)
        # A one-microsecond overrun at this span disappears in floating
        # total_seconds(). The independent expected result still must FAIL.
        end = z + timedelta(microseconds=1)
        x = bar(
            a,
            end,
            observation=observation(event_at=a, interval_seconds=315537897599),
            session=session(a, end),
        )
        self.check(
            "F01-wide-one-microsecond-overrun",
            M.validate_numeric_bar(x).state,
            Q.State.FAIL,
        )
        ny = ZoneInfo("America/New_York")
        a = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=0)
        z = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=1)
        for seconds, expected in (
            (3600, Q.State.PASS),
            (60, Q.State.FAIL),
            (None, Q.State.UNKNOWN),
        ):
            x = bar(
                a,
                z,
                observation=observation(a, interval_seconds=seconds),
                session=session(a, z, timezone_id="America/New_York"),
            )
            g = M.ExpectedBarGrid(
                a, z, x.session, x.basis, (M.BarInterval(a, z),), ref()
            )
            self.check(
                "F01-fold-UTC-numeric", M.validate_numeric_bar(x).state, expected
            )
            self.check(
                "F01-fold-UTC-grid", M.validate_bar_grid((x,), g).state, expected
            )
        # Distinct offsets express equal instants without altering originals.
        end = T.astimezone(timezone(timedelta(hours=9))) + timedelta(seconds=60)
        x = bar(T, end)
        self.check(
            "F01-offset-equivalent-duration",
            M.validate_numeric_bar(x).state,
            Q.State.PASS,
        )

    def test_F01_grid_duration_and_geometry_are_separate(self):
        bars, g = sample()
        without_numeric = tuple(
            replace(
                x, open=None, volume=None, trade_notional=None, notional_reference=None
            )
            for x in bars
        )
        self.check(
            "F01-no-new-numeric-gate",
            M.validate_bar_grid(without_numeric, g).state,
            Q.State.PASS,
        )
        for declaration, expected in ((30, Q.State.FAIL), (None, Q.State.UNKNOWN)):
            wrong = replace(
                without_numeric[0],
                observation=replace(
                    without_numeric[0].observation, interval_seconds=declaration
                ),
            )
            complete = M.validate_bar_grid((wrong, without_numeric[1]), g)
            self.check("F01-whole-grid-bound", complete.state, expected)
            self.assertNotIn("missing_expected_interval", complete.reasons)
            missing = M.validate_bar_grid((wrong,), g)
            self.check(
                "F01-known-geometric-gap-still-fails", missing.state, Q.State.FAIL
            )
            self.assertIn("missing_expected_interval", missing.reasons)
        x = replace(
            bars[0], observation=replace(bars[0].observation, interval_seconds=30)
        )
        self.check(
            "F01-unknown-grid-does-not-hide-contradiction",
            M.validate_bar_grid((x, bars[1]), replace(g, intervals=None)).state,
            Q.State.FAIL,
        )
        self.assertIn(
            "unknown_expected_grid",
            M.validate_bar_grid((x, bars[1]), replace(g, intervals=None)).reasons,
        )


ACCEPTED_DEFINITION_AST_HASHES = {
    "Adjustment": "c80b95fa0f63d2c0e62c474123b60d4cfbefd00ef0d2b80eb3106ac00447009e",
    "NotionalBasis": "6f711867dd946be1f7443785f130314050adbabd7a50628568733b538c75e7b8",
    "TimestampConvention": "aa6f566017f0a4f25e1b188c74af49c4340db1a3f2e00de84f57cf686bdf5f01",
    "SessionConvention": "3d7f718b6cdbb43e778fa01e733c7c476ec93f43faaf140edfe824a02d09a970",
    "_text": "229c58e455d9548918cda3b9db087a53eb873f33819f1ab362d6ece5d3bd4c51",
    "_typed": "ed75934d0d949690390f892f39e0f85629aa8f485574001a8ac7a3e53748af39",
    "_utc": "4ef72e555086bb52fa6b06013d0ff529aeffa65747a00bb09eddfbd06abd13f9",
    "_clock": "fa815085f5799bafcc59a9c5f20dc77952275bc46a1eecc2dba0a9f7a2f7b55b",
    "_interval": "a739a46aef2e0592e157a49009075d1a30eefea18ed87ac3cf6c16be9ceced96",
    "_decimal": "3f74b3325192878fb4c9e62af86e616abc18dc035a45b1aca65acaaa120057ef",
    "BarInterval": "ab72815bfbb29faa8bf4b9a2b985dce95d6ae6d1dcbf90cc98887c6c4d9f58f9",
    "PriceBasis": "052d4f146dcc2626e5ebea67f11b66409ce478627a51d625c415249f46ced706",
    "SessionWindow": "add399e470743d9e6dcdb471594a38479fae162b0653b20fa21dabd284a970da",
    "NumericBar": "050767ab56dec660aea47804f056aa6563e4825ff43e1de2866e5e28f1a7eda5",
    "ExpectedBarGrid": "d1e1af030f6351e9e9a5463679fa30e6701d93f6966cfec2a8aa9013ca36fe6a",
    "StructuralResult": "1586a12a5a8a54b36078cae827a0c7d0cf7464be187624c903ae8f11b42f3fb3",
    "_result": "9a5370e4780da8defc8f764f9120fd16e0cb1917fd0a1e85689090d2b580bbf3",
    "_reference_known": "d73a0fb64702ec3f7a721cfe2d457f86483469c2faf520188112d723850fe6f3",
    "_clock_key": "7e7338d4f3710d2c0ac98c663cbe73c1a17dcca023e869051dda0328e244366c",
    "_reference_key": "eb440b53516ff01465760ba648fd70fdde1a2534182af6293cf2684f1c70e997",
    "_basis_unknown": "904822250e9e6ca58a3cac424d46881e27dfd7e709aadf4789677044bc3f301e",
    "compare_price_basis": "a15909dc3315926e05e216b8ec7bc3122864d2043c0b67679e7ea22c4da7bbd9",
    "_session_key": "a032c504d5b8d928e4bc31ff0603c9625b88c5ce385966d613b40370befe102d",
    "_session_unknown": "64d187feb53cdc822f04bfdd913238f283af62454815d836a2d2708bdade72f7",
    "_inside": "4f79d6379490fb5ac60b23569f64b73ed98781d75bde8a5682a4c0fcc3f7ea62",
    "_observation_duration": "c5ae615b1508bbac61c790a7f21725bba5941710946da2a793e5f11ae20341f7",
    "validate_numeric_bar": "cce962c80c0808f80a3ee57d216438c61e3a5168ed0eeb1b596e70f57889ccaa",
    "_interval_key": "cfdcb634e15e3a68834ffc8110c1b83bf5d69270b46ea463469fdf7df66b8be7",
    "_sequence_checks": "0404cc32459f6b881e5e2602ff037ea98d017dbd311330fc72dcf660a4b304b8",
    "validate_bar_grid": "2a03ec00e1de9d8218ac14cd720706762c17a8ad37b4da4931bc81c794769018",
}


def math_inputs(durations=(60, 120, 120), method=M.NotionalBasis.TRADE, values=None):
    end = T + timedelta(seconds=sum(durations))
    s = session(T, end)
    b = basis(notional_basis=method)
    rows = values or (
        ("100", "102", "99", "101", None, None),
        ("102", "104", "100", "103", None, None),
        ("100", "103", "98", "102", None, None),
    )
    seq, intervals = [], []
    start = T
    for i, (seconds, row) in enumerate(zip(durations, rows), 1):
        stop = start + timedelta(seconds=seconds)
        obs = observation(start, i, interval_seconds=seconds)
        prices = [None if value is None else Decimal(value) for value in row[:4]]
        x = bar(
            start,
            stop,
            i,
            observation=obs,
            session=s,
            basis=b,
            open=prices[0],
            high=prices[1],
            low=prices[2],
            close=prices[3],
            volume=row[4],
            trade_notional=None if row[5] is None else Decimal(row[5]),
            notional_reference=None if row[5] is None else ref("trade:proof"),
        )
        seq.append(x)
        intervals.append(M.BarInterval(start, stop))
        start = stop
    g = M.ExpectedBarGrid(T, end, s, b, tuple(intervals), ref("expected:math"))
    seq = tuple(seq)
    return seq, g, math_policy(seq), end


def math_policy(
    bars,
    required=(Q.Check.FINALITY, Q.Check.INTERVAL),
    mode=Q.Mode.RESEARCH,
    max_delay=0,
    **changes,
):
    caps = tuple(
        Q.CapabilityPolicy(
            name="math:" + str(i),
            expected_identity=x.observation.identity,
            evidence_id=x.observation.evidence_id,
            mode=mode,
            required_checks=required,
            expected_evidence_sha256=x.observation.evidence_sha256,
            required_interval_seconds=x.observation.interval_seconds,
            max_future_skew_seconds=0,
            max_event_age_seconds=600,
            max_receipt_age_seconds=600,
            max_delay_seconds=max_delay,
            allowed_session_states=("open",),
        )
        for i, x in enumerate(bars)
    )
    values = dict(
        version="math-policy-v1",
        provenance={"code_sha256": "a" * 64, "config_sha256": "b" * 64},
        capabilities=caps,
    )
    values.update(changes)
    return Q.EvaluationPolicy(**values)


def feature_call(function, bars, grid, policy=None, cutoff=None, **changes):
    at = grid.interval_end if cutoff is None else cutoff
    values = dict(
        cutoff=at,
        quality_policy=math_policy(bars) if policy is None else policy,
        evaluation_at=at,
        decision_at=at,
        session_facts=(),
    )
    if function is M.opening_range:
        values["minutes"] = 5
    values.update(changes)
    return function(bars, grid, **values)


class FeatureMathTests(unittest.TestCase):
    def check(self, label, actual, expected):
        CONTROL_IDS.append(label)
        self.assertEqual(actual, expected, label)

    def states(self, result):
        return tuple(
            getattr(result, f).state
            for f in ("numeric", "coverage", "basis", "temporal")
        )

    def test_OR_literal_high_low_and_unneeded_fields(self):
        bars, g, policy, end = math_inputs()
        result = feature_call(M.opening_range, bars, g, policy)
        self.check(
            "M-OR-104-98", (result.high, result.low), (Decimal("104"), Decimal("98"))
        )
        self.check("M-OR-four-states", self.states(result), (Q.State.PASS,) * 4)
        self.check("M-OR-label", result.method, M.PriceFeatureMethod.OPENING_RANGE)
        self.assertIsNone(result.vwap)
        self.assertEqual(result.opening_minutes, 5)
        self.assertIs(result.inputs, bars)
        self.assertIs(result.grid, g)
        self.assertIs(result.quality_policy, policy)
        self.assertIsNone(bars[0].volume)
        self.assertIsNone(bars[0].trade_notional)
        self.assertIsNone(bars[0].notional_reference)
        no_open_close = tuple(replace(x, open=None, close=None) for x in bars)
        self.check(
            "M-OR-only-high-low",
            feature_call(M.opening_range, no_open_close, g).high,
            Decimal("104"),
        )

    def test_OR_5_15_30_and_anchor_boundaries(self):
        for minutes, durations in (
            (5, (60, 120, 120)),
            (15, (300, 300, 300)),
            (30, (600, 600, 600)),
        ):
            bars, g, p, end = math_inputs(durations)
            r = feature_call(M.opening_range, bars, g, p, minutes=minutes)
            self.check(
                "M-OR-explicit-minutes-" + str(minutes),
                (r.high, r.low),
                (Decimal("104"), Decimal("98")),
            )
            wrong = feature_call(
                M.opening_range, bars, g, p, minutes=15 if minutes != 15 else 5
            )
            self.check("M-OR-window-duration", wrong.coverage.state, Q.State.FAIL)
            self.assertIsNone(wrong.high)
        bars, g, _, _ = math_inputs()
        s = replace(
            g.session,
            open_at=T - timedelta(minutes=1),
            segments=(M.BarInterval(T - timedelta(minutes=1), g.interval_end),),
        )
        seq = tuple(replace(x, session=s) for x in bars)
        r = feature_call(M.opening_range, seq, replace(g, session=s))
        self.check("M-OR-anchor", r.coverage.state, Q.State.FAIL)
        self.assertIn("opening_window_not_anchored", r.coverage.reasons)
        for minutes in (True, False, 1, 0, 60, 5.0, "5", None):
            with self.subTest(minutes=minutes):
                CONTROL_IDS.append("M-OR-invalid-minutes")
                with self.assertRaises(ValueError):
                    feature_call(M.opening_range, bars, g, minutes=minutes)

    def test_trade_vwap_literal_ratio_and_nonterminating(self):
        cases = (
            (100, 300, "1000", "3600", Fraction(23, 2)),
            (1, 2, "1", "0", Fraction(1, 3)),
            (1, 6, "0.1", "0.2", Fraction(3, 70)),
            (
                1,
                2,
                "1.00000000000000000000000000000000000000000000000001",
                "0",
                Fraction(
                    100000000000000000000000000000000000000000000000001,
                    300000000000000000000000000000000000000000000000000,
                ),
            ),
        )
        for v1, v2, n1, n2, expected in cases:
            rows = (("10", "11", "9", "10", v1, n1), ("12", "13", "11", "12", v2, n2))
            bars, g, _, _ = math_inputs((60, 60), values=rows)
            result = feature_call(M.trade_vwap, bars, g)
            self.check("M-VWAP-exact-ratio", result.vwap, expected)
            self.check("M-VWAP-states", self.states(result), (Q.State.PASS,) * 4)
            self.check(
                "M-VWAP-label",
                result.method,
                M.PriceFeatureMethod.TRADE_NOTIONAL_VOLUME_VWAP,
            )
            self.assertIsNone(result.high)
            self.assertIsNone(result.low)
        bars = tuple(
            replace(x, open=None, high=None, low=None, close=None) for x in bars
        )
        self.check(
            "M-VWAP-no-OHLC-dependency",
            feature_call(M.trade_vwap, bars, g).vwap,
            expected,
        )

    def test_proxy_explicit_label_and_independent_ratio(self):
        rows = (("10", "11", "9", "10", 100, None), ("12", "14", "10", "12", 300, None))
        bars, g, _, _ = math_inputs((60, 60), M.NotionalBasis.OHLCV_PROXY, rows)
        r = feature_call(M.ohlcv_hlc3_volume_proxy, bars, g)
        self.check("M-proxy-literal-23/2", r.vwap, Fraction(23, 2))
        self.check(
            "M-proxy-method", r.method, M.PriceFeatureMethod.OHLCV_HLC3_VOLUME_PROXY
        )
        self.check("M-proxy-four-states", self.states(r), (Q.State.PASS,) * 4)
        altered = tuple(
            replace(x, trade_notional=Decimal("999999"), notional_reference=None)
            for x in bars
        )
        self.check(
            "M-proxy-ignore-declared-trade-notional",
            feature_call(M.ohlcv_hlc3_volume_proxy, altered, g).vwap,
            Fraction(23, 2),
        )
        exact = feature_call(M.trade_vwap, bars, g)
        self.check("M-proxy-not-trade", exact.basis.state, Q.State.FAIL)
        self.assertIsNone(exact.vwap)
        one = (
            replace(
                bars[0],
                interval_end=g.interval_end,
                observation=replace(bars[0].observation, interval_seconds=120),
                open=Decimal("2"),
                high=Decimal("4"),
                low=Decimal("1"),
                close=Decimal("2"),
                volume=3,
            ),
        )
        gg = replace(g, intervals=(M.BarInterval(g.interval_start, g.interval_end),))
        self.check(
            "M-proxy-nonterminating",
            feature_call(M.ohlcv_hlc3_volume_proxy, one, gg).vwap,
            Fraction(7, 3),
        )

    def test_method_currency_adjustment_and_volume_basis(self):
        rows = (
            ("10", "11", "9", "10", 100, "1000"),
            ("12", "13", "11", "12", 300, "3600"),
        )
        bars, g, _, _ = math_inputs((60, 60), values=rows)
        for field, value in (
            ("price_currency", "EUR"),
            ("volume_unit", "contracts"),
            ("adjustment", M.Adjustment.SPLIT),
            ("basis_version", "v2"),
            ("notional_basis", M.NotionalBasis.OHLCV_PROXY),
        ):
            x = replace(bars[0], basis=replace(bars[0].basis, **{field: value}))
            r = feature_call(M.trade_vwap, (x, bars[1]), g)
            self.check("M-incompatible-basis-" + field, r.basis.state, Q.State.FAIL)
            self.assertIsNone(r.vwap)
        for field, value in (("volume_unit", "contracts"), ("notional_unit", "EUR")):
            b = replace(g.basis, **{field: value})
            seq = tuple(replace(x, basis=b) for x in bars)
            r = feature_call(M.trade_vwap, seq, replace(g, basis=b))
            self.check("M-economic-basis-" + field, r.basis.state, Q.State.FAIL)
        for field in ("price_currency", "notional_unit", "volume_unit", "basis_id"):
            b = replace(g.basis, **{field: None})
            seq = tuple(replace(x, basis=b) for x in bars)
            self.check(
                "M-unknown-basis-" + field,
                feature_call(M.trade_vwap, seq, replace(g, basis=b)).basis.state,
                Q.State.UNKNOWN,
            )

    def test_volume_notional_missing_zero_and_negative(self):
        rows = (
            ("10", "11", "9", "10", 100, "1000"),
            ("12", "13", "11", "12", 300, "3600"),
        )
        bars, g, _, _ = math_inputs((60, 60), values=rows)
        for change, state, reason in (
            (dict(volume=None), Q.State.UNKNOWN, "missing_feature_volume"),
            (dict(trade_notional=None), Q.State.UNKNOWN, "missing_trade_notional"),
            (
                dict(trade_notional=Decimal("-1")),
                Q.State.FAIL,
                "negative_economic_notional",
            ),
            (dict(volume=0), Q.State.FAIL, "zero_volume_nonzero_notional"),
        ):
            seq = (replace(bars[0], **change), bars[1])
            before = fingerprint(seq)
            r = feature_call(M.trade_vwap, seq, g)
            self.check("M-economic-quantity-state", r.numeric.state, state)
            self.assertIn(reason, r.numeric.reasons)
            self.assertIsNone(r.vwap)
            self.check("M-economic-input-preserved", fingerprint(seq), before)
        zeros = tuple(replace(x, volume=0, trade_notional=Decimal("0")) for x in bars)
        r = feature_call(M.trade_vwap, zeros, g)
        self.check("M-zero-denominator", r.numeric.state, Q.State.FAIL)
        self.assertIn("zero_total_volume", r.numeric.reasons)
        seq = (replace(bars[0], volume=0, trade_notional=Decimal("0")), bars[1])
        self.check(
            "M-reported-zero-valid",
            feature_call(M.trade_vwap, seq, g).vwap,
            Fraction(12),
        )
        partial = (replace(bars[0], trade_notional=None, volume=0), bars[1])
        self.assertIsNone(feature_call(M.trade_vwap, partial, g).vwap)

    def test_finality_policy_and_source_cutoff(self):
        bars, g, _, end = math_inputs()
        for finality, expected in (
            (Q.Finality.PROVISIONAL, Q.State.FAIL),
            (Q.Finality.INCOMPLETE, Q.State.FAIL),
            (Q.Finality.UNKNOWN, Q.State.UNKNOWN),
        ):
            seq = (
                replace(
                    bars[0], observation=replace(bars[0].observation, finality=finality)
                ),
            ) + bars[1:]
            r = feature_call(M.opening_range, seq, g)
            self.check("M-finality-does-not-confirm", r.temporal.state, expected)
            self.check("M-finality-numeric-independent", r.numeric.state, Q.State.PASS)
            self.assertIsNone(r.high)
        for field in ("available_at", "recorded_at", "receipt_at", "as_of_at"):
            seq = (
                replace(
                    bars[0],
                    observation=replace(
                        bars[0].observation, **{field: end + timedelta(microseconds=1)}
                    ),
                ),
            ) + bars[1:]
            self.check(
                "M-source-future-" + field,
                feature_call(M.opening_range, seq, g).temporal.state,
                Q.State.FAIL,
            )
        missing = (
            replace(
                bars[0], observation=replace(bars[0].observation, available_at=None)
            ),
        ) + bars[1:]
        self.check(
            "M-source-known-at-missing",
            feature_call(M.opening_range, missing, g).temporal.state,
            Q.State.UNKNOWN,
        )
        omitted = feature_call(
            M.opening_range, bars, g, math_policy(bars, required=(Q.Check.INTERVAL,))
        )
        self.check("M-no-finality-policy", omitted.temporal.state, Q.State.UNKNOWN)
        self.assertIn("explicit_finality_check_required", omitted.temporal.reasons)
        caps = math_policy(bars).capabilities[:1]
        self.check(
            "M-unconfigured-input",
            feature_call(
                M.opening_range, bars, g, math_policy(bars, capabilities=caps)
            ).temporal.state,
            Q.State.UNKNOWN,
        )

    def test_ancillary_each_clock_cutoff_and_missing(self):
        rows = (
            ("10", "11", "9", "10", 100, "1000"),
            ("12", "13", "11", "12", 300, "3600"),
        )
        bars, g, _, end = math_inputs((60, 60), values=rows)
        for name in ("calendar", "adjustment", "grid", "notional"):
            for field in ("available_at", "recorded_at"):
                for offset, expected in ((0, Q.State.PASS), (1, Q.State.FAIL)):
                    at = end + timedelta(microseconds=offset)
                    evidence = (
                        ref("known:" + name, end, at)
                        if field == "recorded_at"
                        else ref("known:" + name, at, at)
                    )
                    seq, gg = bars, g
                    if name == "calendar":
                        s = replace(g.session, calendar_reference=evidence)
                        seq = tuple(replace(x, session=s) for x in bars)
                        gg = replace(g, session=s)
                    elif name == "adjustment":
                        b = replace(g.basis, adjustment_reference=evidence)
                        seq = tuple(replace(x, basis=b) for x in bars)
                        gg = replace(g, basis=b)
                    elif name == "grid":
                        gg = replace(g, reference=evidence)
                    else:
                        seq = tuple(
                            replace(x, notional_reference=evidence) for x in bars
                        )
                    r = feature_call(M.trade_vwap, seq, gg)
                    self.check(
                        "M-ancillary-" + name + "-" + field + "-" + str(offset),
                        r.temporal.state,
                        expected,
                    )
                    self.check(
                        "M-ancillary-qualified-value",
                        r.vwap,
                        Fraction(23, 2) if expected is Q.State.PASS else None,
                    )
            seq, gg = bars, g
            if name == "calendar":
                s = replace(g.session, calendar_reference=None)
                seq = tuple(replace(x, session=s) for x in bars)
                gg = replace(g, session=s)
            elif name == "adjustment":
                b = replace(g.basis, adjustment_reference=None)
                seq = tuple(replace(x, basis=b) for x in bars)
                gg = replace(g, basis=b)
            elif name == "grid":
                gg = replace(g, reference=None)
            else:
                seq = tuple(replace(x, notional_reference=None) for x in bars)
            self.check(
                "M-ancillary-unknown-" + name,
                feature_call(M.trade_vwap, seq, gg).temporal.state,
                Q.State.UNKNOWN,
            )
        future = ref("future:notional", end + timedelta(days=1))
        self.check(
            "M-notional-no-cutoff-substitution",
            feature_call(
                M.trade_vwap,
                tuple(replace(x, notional_reference=future) for x in bars),
                g,
            ).temporal.state,
            Q.State.FAIL,
        )

    def test_late_selected_revision_not_repaired(self):
        bars, g, _, end = math_inputs()
        late = replace(
            bars[0],
            high=Decimal("105"),
            observation=replace(
                bars[0].observation,
                evidence_id="late:selection",
                revision_id="late:revision",
                revises_evidence_id=bars[0].observation.evidence_id,
                available_at=end + timedelta(seconds=1),
                receipt_at=end + timedelta(seconds=1),
                recorded_at=end + timedelta(seconds=1),
            ),
        )
        seq = (late,) + bars[1:]
        before = fingerprint(seq)
        r = feature_call(M.opening_range, seq, g)
        self.check("M-late-value-withheld", r.high, None)
        self.check("M-late-temporal", r.temporal.state, Q.State.FAIL)
        later_evaluation = feature_call(
            M.opening_range, seq, g, evaluation_at=end + timedelta(seconds=5)
        )
        self.check(
            "M-later-now-does-not-admit-original-cutoff",
            later_evaluation.temporal.state,
            Q.State.FAIL,
        )
        self.assertIsNone(later_evaluation.high)
        self.check("M-no-automatic-revision-repair", fingerprint(seq), before)
        conflicting = (bars[0], late) + bars[1:]
        self.check(
            "M-conflicting-selection",
            feature_call(M.opening_range, conflicting, g).coverage.state,
            Q.State.FAIL,
        )

    def test_coverage_anomalies_cannot_doublecount(self):
        bars, g, _, _ = math_inputs()
        for seq in (bars[:2], bars[::-1], bars + (bars[0],)):
            r = feature_call(M.opening_range, seq, g)
            self.check("M-coverage-refusal", r.coverage.state, Q.State.FAIL)
            self.assertIsNone(r.high)
        self.check(
            "M-unknown-grid",
            feature_call(
                M.opening_range, bars, replace(g, intervals=None)
            ).coverage.state,
            Q.State.UNKNOWN,
        )
        missing_price = (replace(bars[0], high=None),) + bars[1:]
        r = feature_call(M.opening_range, missing_price, g)
        self.check("M-price-unknown", r.numeric.state, Q.State.UNKNOWN)
        self.assertIsNone(r.high)
        wrong_duration = (
            replace(
                bars[0], observation=replace(bars[0].observation, interval_seconds=30)
            ),
        ) + bars[1:]
        self.check(
            "M-F01-held",
            feature_call(M.opening_range, wrong_duration, g).coverage.state,
            Q.State.FAIL,
        )
        missing_duration = (
            replace(
                bars[0], observation=replace(bars[0].observation, interval_seconds=None)
            ),
        ) + bars[1:]
        self.check(
            "M-F01-unknown",
            feature_call(M.opening_range, missing_duration, g).coverage.state,
            Q.State.UNKNOWN,
        )

    def test_delay_research_and_current_quality_are_separate(self):
        bars, g, _, _ = math_inputs()
        seq = tuple(
            replace(
                x,
                observation=replace(
                    x.observation,
                    delay=Q.DelayEvidence(x.observation.identity, 900, ref()),
                ),
            )
            for x in bars
        )
        research = math_policy(
            seq,
            required=(Q.Check.FINALITY, Q.Check.INTERVAL, Q.Check.DELAY),
            max_delay=900,
        )
        r = feature_call(M.opening_range, seq, g, research)
        self.check("M-delayed-research-range", r.high, Decimal("104"))
        current = math_policy(seq, mode=Q.Mode.CURRENT)
        r = feature_call(M.opening_range, seq, g, current)
        self.check("M-current-delay-refused", r.temporal.state, Q.State.FAIL)
        self.check("M-current-numeric-valid", r.numeric.state, Q.State.PASS)
        self.assertIsNone(r.high)
        for name in ("eligible", "current", "signal", "order"):
            self.assertFalse(hasattr(r, name))

    def test_policy_source_binding_and_cutoff_clock_arguments(self):
        bars, g, p, end = math_inputs()
        cap = replace(p.capabilities[0], expected_evidence_sha256="b" * 64)
        wrong = replace(p, capabilities=(cap,) + p.capabilities[1:])
        self.check(
            "M-policy-hash-binding",
            feature_call(M.opening_range, bars, g, wrong).temporal.state,
            Q.State.FAIL,
        )
        cap = replace(
            p.capabilities[0],
            expected_identity=replace(
                bars[0].observation.identity, source="wrong:source"
            ),
        )
        wrong = replace(p, capabilities=(cap,) + p.capabilities[1:])
        self.check(
            "M-policy-identity-binding",
            feature_call(M.opening_range, bars, g, wrong).temporal.state,
            Q.State.FAIL,
        )
        self.check(
            "M-open-at-cutoff",
            feature_call(
                M.opening_range, bars, g, cutoff=end - timedelta(microseconds=1)
            ).temporal.state,
            Q.State.FAIL,
        )
        self.check(
            "M-decision-cutoff-mismatch",
            feature_call(
                M.opening_range, bars, g, decision_at=end - timedelta(microseconds=1)
            ).temporal.state,
            Q.State.FAIL,
        )
        for args in (
            dict(cutoff=end.replace(tzinfo=None)),
            dict(evaluation_at=True),
            dict(session_facts=[]),
        ):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    feature_call(M.opening_range, bars, g, **args)
        with self.assertRaises(ValueError):
            feature_call(M.opening_range, list(bars), g)

    def test_decimal_traps_exact_ratios_and_immutability(self):
        rows = (("10", "11", "9", "10", 1, "0.1"), ("12", "13", "11", "12", 6, "0.2"))
        bars, g, p, _ = math_inputs((60, 60), values=rows)
        before = fingerprint((bars, g, p))
        for precision in (1, 2, 28):
            with localcontext() as ctx:
                ctx.prec = precision
                ctx.Emin = -2
                ctx.Emax = 2
                for signal in ctx.traps:
                    ctx.traps[signal] = True
                ctx.clear_flags()
                context_before = (str(ctx), dict(ctx.flags), dict(ctx.traps))
                result = feature_call(M.trade_vwap, bars, g, p)
                self.check("M-trap-independent-Fraction", result.vwap, Fraction(3, 70))
                self.check(
                    "M-no-context-change",
                    (str(ctx), dict(ctx.flags), dict(ctx.traps)),
                    context_before,
                )
        for _ in range(3):
            self.check(
                "M-repeat-identical", feature_call(M.trade_vwap, bars, g, p), result
            )
        self.check("M-whole-input-preserved", fingerprint((bars, g, p)), before)
        with self.assertRaises(FrozenInstanceError):
            result.vwap = Fraction(1)

    def test_math_fixture_and_accepted_public_AST_preserved(self):
        cases = FIXTURE["math_cases"]
        self.check(
            "M-fixture-OR",
            (
                cases["opening_range_5"]["expected_high"],
                cases["opening_range_5"]["expected_low"],
            ),
            ("104", "98"),
        )
        self.check("M-fixture-VWAP", cases["trade_vwap"]["expected_ratio"], [23, 2])
        self.check(
            "M-fixture-original-fields",
            FIXTURE["held_original_P01"]["computed_outputs"],
            None,
        )
        tree = ast.parse(SOURCE.read_text())
        expected = ACCEPTED_DEFINITION_AST_HASHES
        actual = {
            n.name: hashlib.sha256(
                ast.dump(n, include_attributes=False).encode()
            ).hexdigest()
            for n in tree.body
            if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in expected
        }
        self.check("M-all-accepted-definitions-exact", actual, expected)
        self.check(
            "M-new-public-exports",
            M.__all__[-5:],
            (
                "PriceFeatureMethod",
                "PriceFeatureResult",
                "opening_range",
                "trade_vwap",
                "ohlcv_hlc3_volume_proxy",
            ),
        )

    def test_supplied_DST_break_auction_and_earlyclose(self):
        bars, g, _, _ = math_inputs()
        zone = ZoneInfo("America/New_York")
        points = (
            datetime(2026, 11, 1, 1, 58, tzinfo=zone, fold=0),
            datetime(2026, 11, 1, 1, 59, tzinfo=zone, fold=0),
            datetime(2026, 11, 1, 1, 1, tzinfo=zone, fold=1),
            datetime(2026, 11, 1, 1, 3, tzinfo=zone, fold=1),
        )
        s = session(points[0], points[-1], timezone_id="America/New_York")
        seq = []
        for i, x in enumerate(bars):
            a, z = points[i : i + 2]
            known = a.astimezone(timezone.utc) + timedelta(seconds=1)
            obs = replace(
                x.observation,
                event_at=a,
                as_of_at=a,
                receipt_at=known,
                available_at=known,
                recorded_at=known,
            )
            seq.append(
                replace(x, observation=obs, interval_start=a, interval_end=z, session=s)
            )
        seq = tuple(seq)
        gg = replace(
            g,
            session=s,
            interval_start=points[0],
            interval_end=points[-1],
            intervals=tuple(M.BarInterval(a, z) for a, z in zip(points, points[1:])),
        )
        r = feature_call(M.opening_range, seq, gg)
        self.check(
            "M-DST-UTC-five-minutes", (r.high, r.low), (Decimal("104"), Decimal("98"))
        )
        self.assertIs(r.grid.interval_start, points[0])
        self.assertIs(r.inputs[0].observation.event_at, points[0])
        # Explicit break omits the middle tile; there is no automatic filling.
        segments = (g.intervals[0], g.intervals[-1])
        s = replace(g.session, segments=segments)
        seq = (replace(bars[0], session=s), replace(bars[2], session=s))
        gg = replace(g, session=s, intervals=segments)
        r = feature_call(M.opening_range, seq, gg)
        self.check(
            "M-explicit-break-OR", (r.high, r.low), (Decimal("103"), Decimal("98"))
        )
        self.check(
            "M-no-break-bar",
            feature_call(
                M.opening_range, (seq[0], replace(bars[1], session=s), seq[1]), gg
            ).coverage.state,
            Q.State.FAIL,
        )
        unknown = replace(g.session, convention=M.SessionConvention.UNKNOWN)
        self.check(
            "M-unknown-auction-policy",
            feature_call(
                M.opening_range,
                tuple(replace(x, session=unknown) for x in bars),
                replace(g, session=unknown),
            ).coverage.state,
            Q.State.UNKNOWN,
        )
        auction = replace(g.session, convention=M.SessionConvention.AUCTION)
        self.check(
            "M-explicit-auction-policy",
            feature_call(
                M.opening_range,
                tuple(replace(x, session=auction) for x in bars),
                replace(g, session=auction),
            ).high,
            Decimal("104"),
        )
        early = replace(
            g.session,
            close_at=T + timedelta(minutes=3),
            segments=(M.BarInterval(T, T + timedelta(minutes=3)),),
            calendar_version="early:v2",
        )
        self.check(
            "M-no-guessed-earlyclose",
            feature_call(
                M.opening_range,
                tuple(replace(x, session=early) for x in bars),
                replace(g, session=early),
            ).coverage.state,
            Q.State.FAIL,
        )

    def test_partial_reference_clock_order_and_unneeded_quality(self):
        bars, g, p, _ = math_inputs()
        for field in ("evidence_id", "sha256", "available_at", "recorded_at"):
            partial = replace(g.reference, **{field: None})
            self.check(
                "M-partial-grid-reference-" + field,
                feature_call(
                    M.opening_range, bars, replace(g, reference=partial)
                ).temporal.state,
                Q.State.UNKNOWN,
            )
        bad = ref("inverted:reference", T, T - timedelta(microseconds=1))
        self.check(
            "M-ancillary-known-clock-order",
            feature_call(
                M.opening_range, bars, replace(g, reference=bad)
            ).temporal.state,
            Q.State.FAIL,
        )
        # An unused instrument/capability casts no vote about selected inputs.
        cap = replace(
            p.capabilities[0],
            name="unused:QQQ",
            evidence_id="unselected:evidence",
            expected_identity=replace(
                bars[0].observation.identity, instrument="TEST:QQQ"
            ),
        )
        self.check(
            "M-no-global-unused-quality-veto",
            feature_call(
                M.opening_range,
                bars,
                g,
                replace(p, capabilities=p.capabilities + (cap,)),
            ).high,
            Decimal("104"),
        )

    def test_large_exact_quantities_under_restricted_context(self):
        # Literal scaled ratios have the same independent 23/2 oracle. No
        # floating coercion or Decimal multiplication forms these quantities.
        volume_scale = 10**120
        rows = (
            ("10", "11", "9", "10", 100 * volume_scale, "1000" + "0" * 120),
            ("12", "13", "11", "12", 300 * volume_scale, "3600" + "0" * 120),
        )
        bars, g, _, _ = math_inputs((60, 60), values=rows)
        with localcontext() as ctx:
            ctx.prec = 1
            ctx.Emax = 2
            ctx.Emin = -2
            for signal in ctx.traps:
                ctx.traps[signal] = True
            ctx.clear_flags()
            before = (str(ctx), dict(ctx.flags))
            self.check(
                "M-large-exact-rational",
                feature_call(M.trade_vwap, bars, g).vwap,
                Fraction(23, 2),
            )
            self.check("M-large-context-unchanged", (str(ctx), dict(ctx.flags)), before)


if __name__ == "__main__":
    suite = unittest.TestSuite(
        (
            unittest.defaultTestLoader.loadTestsFromTestCase(NumericSessionTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(FeatureMathTests),
        )
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(
        json.dumps(
            {
                "methods": result.testsRun,
                "assertion_failures": len(result.failures),
                "errors": len(result.errors),
                "control_invocations": len(CONTROL_IDS),
                "control_ids": CONTROL_IDS,
            }
        )
    )
    sys.exit(0 if result.wasSuccessful() else 1)
