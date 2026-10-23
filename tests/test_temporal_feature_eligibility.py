"""Literal synthetic controller requirements C01-C34 and extra adversarial tests.

Standalone: python -B tests/test_temporal_feature_eligibility.py.
Verification also executes preloaded source under an audited effect fence.
"""

from __future__ import annotations

import copy
import dataclasses
import importlib.util
import itertools
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest
from zoneinfo import ZoneInfo

# The external fence preloads the module without package initializers or reads.
if "temporal_candidate" in sys.modules:
    m = sys.modules["temporal_candidate"]
else:
    spec = importlib.util.spec_from_file_location(
        "temporal_candidate",
        Path(__file__).resolve().parents[1]
        / "analysis/temporal_feature_eligibility.py",
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = m
    spec.loader.exec_module(m)

CUTOFF = datetime(2026, 10, 12, 13, tzinfo=timezone.utc)
OBS = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)
RELEASE = datetime(2026, 10, 12, 12, 5, tzinfo=timezone.utc)
ARRIVAL = datetime(2026, 10, 12, 12, 6, tzinfo=timezone.utc)
US = timedelta(microseconds=1)
KEY = m.FeatureKey("synthetic_feature", "synthetic_source", "observation-01")
POLICY = m.TemporalPolicy(
    "source-policy-v1",
    KEY.feature_id,
    KEY.source_id,
    timedelta(hours=1),
    ("observation", "release", "available_at", "vintage"),
    "knowledge_time",
    False,
    None,
)
SELECT = m.SelectionPolicy("selection-v1", "LATEST_AS_OF")
PAYLOAD = b'{"true":true,"one":1,"false":false,"zero":0,"null":null,"rows":[3,1,2]}'
PROVENANCE = b'{"source":"synthetic_source","version":"v1","basis":"literal"}'


def row(**changes: object) -> m.Candidate:
    return dataclasses.replace(
        m.Candidate(
            "row-01",
            KEY,
            "v1",
            m.SourceKind.OBSERVATION,
            OBS,
            RELEASE,
            ARRIVAL,
            ARRIVAL,
            None,
            PAYLOAD,
            PROVENANCE,
        ),
        **changes,
    )


def policy(**changes: object) -> m.TemporalPolicy:
    return dataclasses.replace(POLICY, **changes)


def future(**changes: object) -> m.Candidate:
    return dataclasses.replace(
        row(
            kind=m.SourceKind.PRECOMPUTED_EPHEMERIS,
            observation=CUTOFF + timedelta(hours=2),
            computed_at=OBS,
        ),
        **changes,
    )


def future_policy(**changes: object) -> m.TemporalPolicy:
    return policy(
        permit_precomputed_ephemeris=True, future_horizon=timedelta(hours=2), **changes
    )


def score(
    component_row: m.Candidate | None = None,
    component_policy: m.TemporalPolicy | None = POLICY,
    **changes: object,
) -> m.Candidate:
    support = row() if component_row is None else component_row
    lineage = m.Lineage(
        "model-01",
        "model-v1",
        ("part-01",),
        (m.Component("part-01", support, component_policy),),
    )
    return row(
        row_id="score-01",
        kind=m.SourceKind.DERIVED_SCORE,
        computed_at=ARRIVAL,
        lineage=lineage,
        **changes,
    )


def selection(rows: tuple[m.Candidate, ...], **changes: object) -> m.SelectionResult:
    args = dict(key=KEY, cutoff=CUTOFF, policy=POLICY, selection_policy=SELECT)
    args.update(changes)
    return m.select_latest_as_of(rows, **args)


def r1_fixture() -> tuple:
    """Literal independent-review counterexample, source/model policies explicit."""
    decision = datetime(2040, 2, 29, 6, 15, 0, 123456, tzinfo=timezone.utc)
    computation = datetime(2040, 2, 29, 6, 0, 0, 123456, tzinfo=timezone.utc)
    leaf_key = m.FeatureKey("r1-event", "r1-ephemeris", "r1-observation")
    score_key = m.FeatureKey("r1-score", "r1-model", "r1-score-observation")
    leaf_policy = m.TemporalPolicy(
        "r1-source-v1",
        leaf_key.feature_id,
        leaf_key.source_id,
        timedelta(hours=3),
        POLICY.clock_roles,
        "knowledge_time",
        True,
        timedelta(hours=4),
    )
    score_policy = dataclasses.replace(
        leaf_policy,
        feature_id=score_key.feature_id,
        source_id=score_key.source_id,
        version="r1-model-policy-v1",
        permit_precomputed_ephemeris=False,
        future_horizon=None,
    )
    leaf = m.Candidate(
        "r1-leaf",
        leaf_key,
        "declared-source-v2",
        m.SourceKind.PRECOMPUTED_EPHEMERIS,
        datetime(2040, 2, 29, 4, 15, 0, 123456, tzinfo=timezone.utc),
        datetime(2040, 2, 29, 5, 15, 0, 123456, tzinfo=timezone.utc),
        datetime(2040, 2, 29, 5, 30, 0, 123456, tzinfo=timezone.utc),
        datetime(2040, 2, 29, 5, 45, 0, 123456, tzinfo=timezone.utc),
        None,
        PAYLOAD,
        PROVENANCE,
        computation,
    )
    consumer = m.Candidate(
        "r1-score",
        score_key,
        "declared-model-v8",
        m.SourceKind.DERIVED_SCORE,
        datetime(2040, 2, 29, 4, 15, 0, 123456, tzinfo=timezone.utc),
        computation,
        computation,
        computation,
        None,
        PAYLOAD,
        PROVENANCE,
        computation,
        m.Lineage(
            "model-r1",
            "model-v8",
            ("leaf-A",),
            (m.Component("leaf-A", leaf, leaf_policy),),
        ),
    )
    return decision, consumer, leaf, leaf_policy, score_policy


def r1_consumer(
    consumer: m.Candidate, leaf: m.Candidate, leaf_policy: m.TemporalPolicy
) -> m.Candidate:
    return dataclasses.replace(
        consumer,
        lineage=m.Lineage(
            "model-r1",
            "model-v8",
            ("leaf-A",),
            (m.Component("leaf-A", leaf, leaf_policy),),
        ),
    )


class TemporalTests(unittest.TestCase):
    def check(
        self,
        candidate: m.Candidate,
        expected: str,
        supplied: m.TemporalPolicy | None = POLICY,
        cutoff: datetime = CUTOFF,
    ) -> m.Classification:
        result = m.classify(candidate, cutoff=cutoff, policy=supplied)
        self.assertEqual(result.status.value, expected)
        self.assertIs(result.row, candidate)
        self.assertIs(result.cutoff, cutoff)
        self.assertIs(result.row.payload_json, candidate.payload_json)
        return result

    def test_C01_baseline(self):
        self.check(row(), "current")

    def test_C02_exact_age_boundary(self):
        self.check(row(observation=CUTOFF - timedelta(hours=1)), "current")

    def test_C03_one_microsecond_stale(self):
        self.check(row(observation=OBS - US), "stale")

    def test_C04_release_equality(self):
        self.check(row(release=CUTOFF), "current")

    def test_C05_future_release(self):
        self.check(row(release=CUTOFF + US), "unknown")

    def test_C06_future_arrival(self):
        self.check(row(available_at=CUTOFF + US), "unknown")

    def test_C07_future_vintage(self):
        self.check(row(vintage=CUTOFF + US), "unknown")

    def test_C08_arrival_equality(self):
        self.check(row(available_at=CUTOFF), "current")

    def test_C09_vintage_equality(self):
        self.check(row(vintage=CUTOFF), "current")

    def test_C10_retracted_before(self):
        self.check(row(retracted_at=CUTOFF - US), "unknown")

    def test_C11_retracted_at_equality(self):
        self.check(row(retracted_at=CUTOFF), "unknown")

    def test_C12_later_retraction_is_historical(self):
        candidate = row(retracted_at=CUTOFF + US)
        self.check(candidate, "current")
        self.check(candidate, "unknown", cutoff=CUTOFF + US)

    def test_C13_absent_retraction_knowledge(self):
        self.check(row(retracted_at=m.Missing.RETRACTION_KNOWLEDGE), "unknown")

    def test_C14_missing_observation(self):
        self.check(row(observation=None), "unknown")

    def test_C15_missing_release(self):
        self.check(row(release=None), "unknown")

    def test_C16_missing_arrival(self):
        self.check(row(available_at=None), "unknown")

    def test_C17_missing_vintage(self):
        self.check(row(vintage=None), "unknown")

    def test_C18_naive_dateonly_each_role(self):
        for field in (
            "observation",
            "release",
            "available_at",
            "vintage",
            "retracted_at",
        ):
            for value in (datetime(2026, 10, 12), date(2026, 10, 12), "2026-10-12"):
                with self.subTest(field=field, value=str(value)):
                    self.check(row(**{field: value}), "unknown")
        for value in (None, date(2026, 10, 12), datetime(2026, 10, 12)):
            with self.subTest(cutoff=str(value)):
                self.check(row(), "unknown", cutoff=value)

    def test_C19_equivalent_offsets(self):
        for offset in (-7, 0, 9):
            with self.subTest(offset=offset):
                tz = timezone(timedelta(hours=offset))
                candidate = row(
                    **{
                        name: instant.astimezone(tz)
                        for name, instant in (
                            ("observation", OBS),
                            ("release", RELEASE),
                            ("available_at", ARRIVAL),
                            ("vintage", ARRIVAL),
                        )
                    }
                )
                self.check(candidate, "current", cutoff=CUTOFF.astimezone(tz))

    def test_C20_fold_and_overnight_UTC_elapsed(self):
        ny = ZoneInfo("America/New_York")
        start = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=0)
        end = datetime(2026, 11, 1, 1, 30, tzinfo=ny, fold=1)
        candidate = row(
            observation=start, release=start, available_at=start, vintage=start
        )
        self.check(candidate, "current", cutoff=end)
        self.check(candidate, "stale", policy(max_age=timedelta(minutes=59)), end)
        self.check(candidate, "stale", cutoff=end.astimezone(timezone.utc) + US)
        tokyo = ZoneInfo("Asia/Tokyo")
        midnight = datetime(2026, 10, 13, 0, tzinfo=tokyo)
        previous = datetime(2026, 10, 12, 23, tzinfo=tokyo)
        self.check(
            row(
                observation=previous,
                release=previous,
                available_at=previous,
                vintage=previous,
            ),
            "current",
            cutoff=midnight,
        )

    def test_C21_missing_policy_or_version(self):
        for supplied in (
            None,
            policy(version=""),
            policy(version=None),
            policy(version=True),
        ):
            with self.subTest(policy=repr(supplied)):
                self.check(row(), "unknown", supplied)

    def test_C22_invalid_duration_and_boolean(self):
        for value in (
            True,
            False,
            -1,
            3600,
            3600.0,
            float("nan"),
            float("inf"),
            "3600",
            None,
            timedelta(microseconds=-1),
        ):
            with self.subTest(duration=repr(value)):
                self.check(row(), "unknown", policy(max_age=value))

    def test_C23_future_observed(self):
        self.check(row(observation=CUTOFF + US), "unknown", future_policy())

    def test_C24_known_future_separate_lane(self):
        candidate = future()
        self.check(candidate, "known_future", future_policy())
        selected = selection((candidate,), policy=future_policy())
        self.assertIsNone(selected.current)
        self.assertEqual(selected.status.value, "unknown")
        self.assertEqual(selected.known_future, (candidate,))
        self.assertNotIn("leak", selected.diagnostics[0].reason)

    def test_C25_ephemeris_arrives_late(self):
        self.check(future(available_at=CUTOFF + US), "unknown", future_policy())

    def test_C26_ephemeris_permission_horizon_provenance(self):
        for candidate, supplied in (
            (future(), POLICY),
            (future(), policy(permit_precomputed_ephemeris=True)),
            (future(computed_at=None), future_policy()),
            (future(computed_at=CUTOFF + US), future_policy()),
            (future(provenance_json=b""), future_policy()),
            (
                future(),
                policy(
                    permit_precomputed_ephemeris=True,
                    future_horizon=timedelta(hours=2) - US,
                ),
            ),
        ):
            with self.subTest(candidate=repr(candidate), policy=repr(supplied)):
                self.check(candidate, "unknown", supplied)

    def test_C27_earlier_eligible_fallback_each_clock(self):
        earlier = row(row_id="earlier")
        for clock in ("release", "available_at", "vintage"):
            with self.subTest(clock=clock):
                later = row(
                    row_id="later",
                    vintage=CUTOFF,
                    **({clock: CUTOFF + US} if clock != "vintage" else {}),
                )
                if clock == "vintage":
                    later = dataclasses.replace(later, vintage=CUTOFF + US)
                selected = selection((later, earlier))
                self.assertIs(selected.current, earlier)
                self.assertEqual(
                    [v.status.value for v in selected.diagnostics],
                    ["current", "unknown"],
                )

    def test_C28_retracted_latest_fallback(self):
        earlier = row(row_id="earlier")
        later = row(row_id="later", vintage=CUTOFF, retracted_at=CUTOFF)
        self.assertIs(selection((earlier, later)).current, earlier)

    def test_C29_all_permutations_diagnostics(self):
        first = row(row_id="a")
        second = row(row_id="b", vintage=CUTOFF)
        late = row(row_id="c", vintage=CUTOFF + US)
        baseline = selection((first, second, late))
        for permutation in itertools.permutations((first, second, late)):
            with self.subTest(order=tuple(v.row_id for v in permutation)):
                result = selection(permutation)
                self.assertIs(result.current, second)
                self.assertEqual(result.diagnostics, baseline.diagnostics)
                self.assertEqual(result.known_future, baseline.known_future)
                self.assertEqual(result.reason, baseline.reason)
                self.assertIs(result.history, permutation)

    def test_C30_conflicting_duplicate_and_tie(self):
        for pair in (
            (row(), row(payload_json=b'{"true":1}')),
            (row(), row(row_id="other", payload_json=b"0")),
            (row(), row()),
        ):
            with self.subTest(ids=tuple(v.row_id for v in pair)):
                selected = selection(pair)
                self.assertIsNone(selected.current)
                self.assertEqual(selected.status.value, "unknown")
                self.assertEqual(len(selected.history), 2)

    def test_C31_score_lineage_missing(self):
        for changes in (
            {"lineage": None},
            {"computed_at": None},
            {"provenance_json": b""},
        ):
            with self.subTest(changes=repr(changes)):
                candidate = dataclasses.replace(score(), **changes)
                self.check(candidate, "unknown")
        for field in (
            "model_id",
            "model_version",
            "declared_component_ids",
            "components",
        ):
            with self.subTest(field=field):
                candidate = score()
                bad = dataclasses.replace(
                    candidate.lineage,
                    **{field: "" if field.startswith("model") else ()},
                )
                self.check(dataclasses.replace(candidate, lineage=bad), "unknown")

    def test_C32_score_support_future_known(self):
        for field in ("release", "available_at", "vintage"):
            with self.subTest(field=field):
                self.check(score(row(**{field: CUTOFF + US})), "unknown")
        self.check(score(future(), future_policy()), "unknown")

    def test_C33_score_stale_missing_policy(self):
        self.check(score(row(observation=OBS - US)), "unknown")
        self.check(score(component_policy=None), "unknown")

    def test_C34_original_bytes_types_order_history(self):
        rows = (
            row(row_id="a"),
            row(row_id="b", vintage=CUTOFF),
            row(row_id="c", retracted_at=CUTOFF),
        )
        original = copy.deepcopy(rows)
        result = selection(rows)
        self.assertIs(result.current, rows[1])
        self.assertIs(result.history, rows)
        for before, after in zip(original, rows):
            self.assertEqual(before, after)
            self.assertEqual(after.payload_json, PAYLOAD)
            self.assertEqual(after.provenance_json, PROVENANCE)
        self.assertEqual(tuple(v.row_id for v in result.history), ("a", "b", "c"))
        payload = json.loads(result.current.payload_json)
        for name, expected in (
            ("true", bool),
            ("one", int),
            ("false", bool),
            ("zero", int),
        ):
            self.assertIs(type(payload[name]), expected)
        self.assertEqual(payload["rows"], [3, 1, 2])
        self.assertEqual(
            list(payload), ["true", "one", "false", "zero", "null", "rows"]
        )

    def test_extra_zero_and_large_exact_duration(self):
        self.check(row(observation=CUTOFF), "current", policy(max_age=timedelta(0)))
        self.check(row(observation=CUTOFF - US), "stale", policy(max_age=timedelta(0)))
        # At this age, float total_seconds loses a microsecond. Literal duration
        # oracle compares exact datetime/timedelta components, never source math.
        ancient = datetime(1000, 1, 1, tzinfo=timezone.utc)
        end = datetime(9000, 1, 1, tzinfo=timezone.utc)
        age = end - ancient
        candidate = row(
            observation=ancient, release=ancient, available_at=ancient, vintage=ancient
        )
        self.check(candidate, "current", policy(max_age=age), end)
        self.check(candidate, "stale", policy(max_age=age - US), end)

    def test_extra_selection_policy_and_foreign_keys(self):
        for supplied in (
            None,
            m.SelectionPolicy("", "LATEST_AS_OF"),
            m.SelectionPolicy("v1", "FIRST_RELEASE"),
        ):
            with self.subTest(policy=repr(supplied)):
                self.assertIsNone(
                    selection((row(),), selection_policy=supplied).current
                )
        for foreign in (
            m.FeatureKey("other", KEY.source_id, KEY.observation_id),
            m.FeatureKey(KEY.feature_id, "other", KEY.observation_id),
            m.FeatureKey(KEY.feature_id, KEY.source_id, "other"),
        ):
            with self.subTest(key=repr(foreign)):
                self.assertIsNone(selection((row(key=foreign),)).current)
        self.assertIsNone(selection(()).current)
        self.assertIsNone(selection((row(),), policy=None).current)

    def test_extra_policy_roles_identity_and_types(self):
        for changes in (
            {"feature_id": "other"},
            {"source_id": "other"},
            {"clock_roles": ("observation", "release", "vintage")},
            {"clock_roles": list(POLICY.clock_roles)},
            {"retraction_semantics": "event_time"},
            {"permit_precomputed_ephemeris": 1},
            {"future_horizon": True},
        ):
            with self.subTest(policy=repr(changes)):
                self.check(row(), "unknown", policy(**changes))
        for changes in (
            {"row_id": ""},
            {"source_version": ""},
            {"kind": "observation"},
            {"key": None},
            {"payload_json": {}},
            {"provenance_json": []},
        ):
            with self.subTest(row=repr(changes)):
                self.check(row(**changes), "unknown")

    def test_extra_score_complete_and_computation_bound(self):
        candidate = score()
        result = self.check(candidate, "current")
        self.assertEqual(len(result.component_diagnostics), 1)
        self.check(dataclasses.replace(candidate, computed_at=ARRIVAL - US), "unknown")
        self.check(dataclasses.replace(candidate, computed_at=CUTOFF + US), "unknown")
        self.check(score(row(retracted_at=CUTOFF)), "unknown")
        for lineage in (
            m.Lineage(
                "model", "v1", ("part-01", "part-02"), candidate.lineage.components
            ),
            m.Lineage(
                "model", "v1", ("part-01", "part-01"), candidate.lineage.components
            ),
            m.Lineage(
                "model", "v1", ("part-01",), (m.Component("part-01", score(), POLICY),)
            ),
            m.Lineage("model", "v1", ("part-01",), candidate.lineage.components * 2),
        ):
            with self.subTest(lineage=repr(lineage)):
                self.check(dataclasses.replace(candidate, lineage=lineage), "unknown")

    def test_extra_future_order_and_current_orthogonal(self):
        observed = row(row_id="ordinary")
        first = dataclasses.replace(future(), row_id="future-a")
        second = dataclasses.replace(first, row_id="future-b", vintage=ARRIVAL + US)
        for permutation in itertools.permutations((first, second)):
            with self.subTest(order=tuple(v.row_id for v in permutation)):
                result = selection(permutation, policy=future_policy())
                self.assertIsNone(result.current)
                self.assertEqual(result.known_future, (first, second))
        result = selection((first, observed), policy=future_policy())
        self.assertIsNone(result.current)
        self.assertEqual(result.reason, "conflicting_observation_identity")
        self.assertEqual(len(result.history), 2)
        self.assertIs(selection((observed,), policy=future_policy()).current, observed)

    def test_extra_multiple_component_policies(self):
        other_key = m.FeatureKey("other_feature", "other_source", "other_observation")
        other = row(row_id="other", key=other_key)
        other_policy = policy(
            feature_id=other_key.feature_id, source_id=other_key.source_id
        )
        parts = (m.Component("b", other, other_policy), m.Component("a", row(), POLICY))
        candidate = dataclasses.replace(
            score(), lineage=m.Lineage("model", "v1", ("b", "a"), parts)
        )
        classified = self.check(candidate, "current")
        self.assertEqual(
            tuple(v.row.row_id for v in classified.component_diagnostics),
            ("row-01", "other"),
        )
        self.assertIs(candidate.lineage.components, parts)
        for malformed in (row(row_id=[]), row(source_version="")):
            with self.subTest(row=repr(malformed)):
                self.check(score(malformed), "unknown")

    def test_R1_literal_component_computation_boundary(self):
        decision, consumer, leaf, leaf_policy, score_policy = r1_fixture()
        for delta, expected in (
            (-US, "current"),
            (timedelta(0), "current"),
            (US, "unknown"),
            (timedelta(minutes=5), "unknown"),
        ):
            with self.subTest(delta=delta):
                component = dataclasses.replace(
                    leaf, computed_at=consumer.computed_at + delta
                )
                self.check(component, "current", leaf_policy, decision)
                result = self.check(
                    r1_consumer(consumer, component, leaf_policy),
                    expected,
                    score_policy,
                    decision,
                )
                self.assertEqual(len(result.component_diagnostics), 1)
                self.assertIs(result.component_diagnostics[0].row, component)
                if expected == "unknown":
                    self.assertEqual(result.reason, "support_unknown_at_computation")

    def test_R1_all_required_support_clocks_offsets(self):
        decision, consumer, leaf, leaf_policy, score_policy = r1_fixture()
        for role in ("release", "available_at", "vintage", "computed_at"):
            for delta, expected in (
                (-US, "current"),
                (timedelta(0), "current"),
                (US, "unknown"),
            ):
                for offset in (-5, 0, 9):
                    with self.subTest(role=role, delta=delta, offset=offset):
                        instant = (consumer.computed_at + delta).astimezone(
                            timezone(timedelta(hours=offset))
                        )
                        component = dataclasses.replace(leaf, **{role: instant})
                        self.check(component, "current", leaf_policy, decision)
                        self.check(
                            r1_consumer(consumer, component, leaf_policy),
                            expected,
                            score_policy,
                            decision,
                        )

    def test_R1_no_guessed_cross_clock_order(self):
        decision, consumer, leaf, leaf_policy, score_policy = r1_fixture()
        for delta in (-US, timedelta(0), US):
            with self.subTest(computation_relative_release=delta):
                component = dataclasses.replace(leaf, computed_at=leaf.release + delta)
                self.check(component, "current", leaf_policy, decision)
                self.check(
                    r1_consumer(consumer, component, leaf_policy),
                    "current",
                    score_policy,
                    decision,
                )

    def test_R1_future_ephemeris_and_optional_observed_clock(self):
        decision, consumer, leaf, leaf_policy, score_policy = r1_fixture()
        for delta in (timedelta(0), US, timedelta(minutes=5)):
            with self.subTest(future_computation_delta=delta):
                component = dataclasses.replace(
                    leaf,
                    observation=decision + timedelta(hours=1),
                    computed_at=consumer.computed_at + delta,
                )
                self.check(component, "known_future", leaf_policy, decision)
                result = self.check(
                    r1_consumer(consumer, component, leaf_policy),
                    "unknown",
                    score_policy,
                    decision,
                )
                self.assertEqual(result.reason, "noncurrent_score_support")
                self.assertNotIn("leak", result.reason)
        # An ordinary observation has no required computation clock in this
        # policy. Do not invent a new required role while fixing ephemeris.
        component = dataclasses.replace(
            leaf,
            kind=m.SourceKind.OBSERVATION,
            computed_at=decision + timedelta(days=1),
        )
        self.check(
            r1_consumer(consumer, component, leaf_policy),
            "current",
            score_policy,
            decision,
        )

    def test_extra_frozen_DTO_and_wrong_container(self):
        for obj, field in (
            (row(), "row_id"),
            (POLICY, "version"),
            (m.classify(row(), cutoff=CUTOFF, policy=POLICY), "reason"),
            (selection((row(),)), "history"),
        ):
            with self.subTest(type=type(obj).__name__):
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(obj, field, None)
        with self.assertRaises(TypeError):
            m.classify({}, cutoff=CUTOFF, policy=POLICY)
        with self.assertRaises(TypeError):
            selection([row()])


if __name__ == "__main__":
    unittest.main(verbosity=2)
