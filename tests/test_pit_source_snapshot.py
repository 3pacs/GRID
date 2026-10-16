"""Offline actual PIT method controls over Resolver-produced fixture rows."""
from __future__ import annotations

import unittest
from datetime import date, timedelta, timezone

if __package__:
    from .test_selected_source_resolution import (
        CUTOFF, START, FakeResult, StoredFixture, actual_pipeline, fixture_inputs, load_core,
    )
else:
    from test_selected_source_resolution import (
        CUTOFF, START, FakeResult, StoredFixture, actual_pipeline, fixture_inputs, load_core,
    )


class SnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_core()

    def make(self, mutate=None, *, policy="LATEST_AS_OF", selections=None):
        fixture = actual_pipeline(self.core)
        # actual_pipeline already exercises one snapshot. Capture only the
        # deliberate second call when checking this test's query boundaries.
        fixture["engine"].calls.clear()
        if mutate:
            mutate(fixture)
        selected = fixture["selections"] if selections is None else selections
        result = self.core.PITStore(fixture["engine"]).get_source_snapshot(
            selected, as_of=CUTOFF, start_date=START, vintage_policy=policy)
        return result, fixture

    def test_missing_bindings_and_unsupported_event_need_no_engine(self):
        snapshot = self.core.PITStore(None).get_source_snapshot(None, as_of=CUTOFF, start_date=START)
        self.assertEqual(len(snapshot["selections"]), 7)
        self.assertEqual([r["reason"] for r in snapshot["selections"][:6]], ["selection_missing"] * 6)
        self.assertTrue(all(r["value"] is None for r in snapshot["selections"]))
        selections = [dict(source_key="fx_rates", series_id="FX:EUR:close", expected_source_name="yfinance")]
        snapshot = self.core.PITStore(None).get_source_snapshot(selections, as_of=CUTOFF, start_date=START)
        self.assertEqual(snapshot["selections"][0]["reason"], "mapping_missing")

    def test_structural_rejection_precedes_engine_access(self):
        class NeverEngine:
            calls = 0
            def begin(self):
                self.calls += 1
                raise AssertionError("engine must not be touched")
        engine = NeverEngine()
        good = fixture_inputs()[0][0]
        invalid = [[good, dict(good)], [dict(good, feature_id=True)], [dict(good, extra="sql")],
                   [dict(good, feature_name=None)], [dict(good, expected_source_name="other")],
                   [dict(good, freshness_policy={"max_age_days": True})],
                   [dict(good, series_id="YF:SPY:close")], [dict(good, scope="unbounded")]]
        other = dict(fixture_inputs()[0][1], feature_id=good["feature_id"], feature_name=good["feature_name"])
        invalid.append([good, other])
        for selections in invalid:
            with self.subTest(selection=str(selections)):
                with self.assertRaises(ValueError):
                    self.core.PITStore(engine).get_source_snapshot(selections, as_of=CUTOFF, start_date=START)
        for cutoff, start in ((CUTOFF.replace(tzinfo=None), START),
                              (CUTOFF, CUTOFF.date() - timedelta(days=252)),
                              (CUTOFF, CUTOFF.date() + timedelta(days=1))):
            with self.assertRaises(ValueError):
                self.core.PITStore(engine).get_source_snapshot([good], as_of=cutoff, start_date=start)
        self.assertEqual(engine.calls, 0)

    def test_unknown_freshness_and_declared_fresh_stale_are_distinct(self):
        unknown, _ = self.make()
        self.assertEqual(unknown["selections"][0]["freshness"],
                         {"status": "unknown", "reason": "freshness_policy_absent"})
        for age, expected in ((2, "fresh"), (1, "stale")):
            snapshot, _ = self.make(lambda f: f["selections"][0].update(freshness_policy={"max_age_days": age}))
            self.assertEqual(snapshot["selections"][0]["freshness"]["status"], expected)
            self.assertEqual(snapshot["selections"][0]["obs_date"], "2026-10-13")

    def test_independent_registry_name_id_mismatch_refuses_without_snapshot_query(self):
        snapshot, fixture = self.make(lambda f: f["registry"][0].update(name="wrong_registry_name"))
        self.assertEqual(snapshot["selections"][0]["reason"], "mapping_missing")
        queries = [p for sql, p in fixture["engine"].calls if sql.startswith("WITH eligible AS")]
        self.assertNotIn(8101, [p["feature_id"] for p in queries])

    def test_actual_source_mismatch_masks_value_without_older_source_fallback(self):
        def change(f):
            newer = dict(f["produced"]["resolved_rows"][0], obs_date=date(2026, 10, 14),
                         source_priority_used=9999, value=999)
            f["engine"].rows.append(newer)
            f["catalog"].append(dict(id=9999, name="wrong_source", priority_rank=0))
        snapshot, _ = self.make(change)
        row = snapshot["selections"][0]
        self.assertEqual(row["reason"], "source_mismatch")
        self.assertIsNone(row["value"])
        self.assertEqual(row["actual_source"], {"id": 9999, "name": "wrong_source"})
        self.assertEqual(row["obs_date"], "2026-10-14")

    def test_zero_is_available_while_null_is_unavailable(self):
        available, _ = self.make()
        self.assertEqual(available["selections"][2]["value"], 0.0)
        self.assertEqual(available["selections"][2]["status"], "available")
        null, _ = self.make(lambda f: f["produced"]["resolved_rows"][2].update(value=None))
        self.assertEqual(null["selections"][2]["reason"], "invalid_value")
        self.assertIsNone(null["selections"][2]["value"])

    def test_two_resolver_produced_vintages_exercise_first_and_latest(self):
        selections, raw, registry, catalog = fixture_inputs()
        raw.append(dict(raw[0], value=1.2, pull_timestamp=CUTOFF - timedelta(hours=1)))
        produced = self.core.Resolver(None).resolve_selected_rows(
            selections, raw, registry, catalog, as_of=CUTOFF, start_date=START)
        for policy, expected in (("FIRST_RELEASE", 1.1), ("LATEST_AS_OF", 1.2)):
            engine = StoredFixture(produced["resolved_rows"], registry, catalog)
            result = self.core.PITStore(engine).get_source_snapshot(
                selections, as_of=CUTOFF, start_date=START, vintage_policy=policy)
            self.assertEqual(result["selections"][0]["value"], expected)

    def test_retractions_precede_vintage_choice_and_use_exact_aware_cutoff(self):
        selections, raw, registry, catalog = fixture_inputs()
        raw.append(dict(raw[0], value=1.2, pull_timestamp=CUTOFF - timedelta(hours=1)))
        produced = self.core.Resolver(None).resolve_selected_rows(
            selections, raw, registry, catalog, as_of=CUTOFF, start_date=START)
        retracted = dict(feature_id=8101, obs_date=date(2026, 10, 13), vintage_date=date(2026, 10, 15))
        for when, expected in ((CUTOFF, 1.1), (CUTOFF + timedelta(microseconds=1), 1.2)):
            engine = StoredFixture(produced["resolved_rows"], registry, catalog,
                                   [dict(retracted, retracted_at=when)])
            result = self.core.PITStore(engine).get_source_snapshot(selections, as_of=CUTOFF, start_date=START)
            self.assertEqual(result["selections"][0]["value"], expected)
        engine = StoredFixture(produced["resolved_rows"], registry, catalog,
                               [dict(retracted, vintage_date=r["vintage_date"], retracted_at=CUTOFF)
                                for r in produced["resolved_rows"] if r["feature_id"] == 8101])
        result = self.core.PITStore(engine).get_source_snapshot(selections, as_of=CUTOFF, start_date=START)
        self.assertEqual(result["selections"][0]["reason"], "no_resolved_row")

    def test_future_and_out_of_range_storage_rows_cannot_displace_valid_value(self):
        for changes in ({"obs_date": date(2026, 10, 16)},
                        {"obs_date": date(2026, 10, 14), "release_date": date(2026, 10, 16)},
                        {"obs_date": date(2026, 10, 14), "vintage_date": date(2026, 10, 16)}):
            def change(f):
                f["engine"].rows.append(dict(f["produced"]["resolved_rows"][0], value=999, **changes))
            snapshot, _ = self.make(change)
            self.assertEqual(snapshot["selections"][0]["value"], 1.1)
        snapshot, _ = self.make(lambda f: f["produced"]["resolved_rows"][0].update(obs_date=date(2026, 9, 30)))
        self.assertEqual(snapshot["selections"][0]["reason"], "no_resolved_row")

    def test_queries_are_bound_readonly_finite_and_provenance_bearing(self):
        result, fixture = self.make()
        queries = fixture["engine"].calls
        self.assertEqual(sum(sql == "SET TRANSACTION READ ONLY" for sql, _ in queries), 6)
        self.assertEqual(sum(sql.startswith("WITH eligible AS") for sql, _ in queries), 6)
        self.assertTrue(all(0 < int(p["timeout_ms"]) <= 5000 for sql, p in queries if "timeout_ms" in p))
        for sql, params in queries:
            if sql.startswith("WITH eligible AS"):
                for guard in ("rs.obs_date >= :start_date", "rs.obs_date <= :as_of_date",
                              "rs.release_date <= :as_of_date", "rs.vintage_date <= :as_of_date",
                              "rr.retracted_at <= :as_of", "WHERE vintage_rank = 1",
                              "ORDER BY obs_date DESC LIMIT 1", "LEFT JOIN source_catalog"):
                    self.assertIn(guard, sql)
                self.assertEqual(params["as_of"], CUTOFF)
        self.assertEqual(result["selections"][0]["actual_source"], {"id": 9101, "name": "yfinance"})
        self.assertEqual(result["selections"][0]["storage_clock"], "pull_date_not_provider_publication")
        self.assertEqual(result["selections"][0]["lineage"], "resolved_source_not_raw_row")

    def test_missing_engine_or_read_failure_returns_unavailable(self):
        result = self.core.PITStore(None).get_source_snapshot(fixture_inputs()[0], as_of=CUTOFF, start_date=START)
        self.assertTrue(all(r["reason"] == "read_failed" and r["value"] is None
                            for r in result["selections"][:6]))

    def test_utc_normalization_and_inclusive_252_date_boundary(self):
        offset = CUTOFF.astimezone(timezone(timedelta(hours=5, minutes=30)))
        normalized = self.core.validate(None, as_of=offset, start_date=CUTOFF.date() - timedelta(days=251))
        self.assertEqual(normalized["as_of"], CUTOFF)
        self.assertEqual(normalized["as_of"].utcoffset(), timedelta(0))
        with self.assertRaises(ValueError):
            self.core.validate(None, as_of=CUTOFF, start_date=CUTOFF.date() - timedelta(days=252))


class BudgetClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now


class TimedStoredFixture(StoredFixture):
    """A virtual clock and finite inert connection honoring requested timeouts."""
    def __init__(self, rows, registry, catalog, *, clock, acquire=0.0, registry_cost=0.0,
                 snapshot_cost=0.0, setup_cost=0.0, config_cost=0.0, exit_cost=0.0,
                 honor_timeout=True):
        super().__init__(rows, registry, catalog)
        self.clock = clock
        self.costs = dict(acquire=acquire, registry=registry_cost, snapshot=snapshot_cost,
                          setup=setup_cost, config=config_cost, exit=exit_cost)
        self.honor_timeout = honor_timeout
        self.timeout_ms = None
        self.trace = []
        self.acquisitions = 0

    def begin(self):
        return self

    def __enter__(self):
        self.acquisitions += 1
        self.clock.now += self.costs["acquire"]
        self.timeout_ms = None
        return self

    def __exit__(self, *args):
        self.clock.now += self.costs["exit"]
        return False

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        params = dict(params or {})
        self.trace.append(dict(sql=sql, params=params, started=self.clock.now,
                               timeout_ms=self.timeout_ms))
        if sql == "SET TRANSACTION READ ONLY":
            self.clock.now += self.costs["setup"]
            return FakeResult([])
        if "set_config('statement_timeout'" in sql:
            self.timeout_ms = int(params["timeout_ms"])
            self.clock.now += self.costs["config"]
            return FakeResult([])
        phase = "registry" if sql.startswith("SELECT id, name") else "snapshot"
        cost = self.costs[phase]
        if self.honor_timeout and cost * 1000 > self.timeout_ms:
            self.clock.now += self.timeout_ms / 1000
            raise RuntimeError("inert boundary honored statement_timeout")
        self.clock.now += cost
        return super().execute(statement, params)


class SharedBudgetTests(unittest.TestCase):
    def run_timed(self, *, all_selections=False, **costs):
        core = load_core()
        fixture = actual_pipeline(core)
        clock = BudgetClock()
        core.pit_globals["monotonic_time"] = clock
        engine = TimedStoredFixture(fixture["produced"]["resolved_rows"], fixture["registry"],
                                   fixture["catalog"], clock=clock, **costs)
        # FINRA fixture's zero proves the positive path is not missing-data credit.
        selections = fixture["selections"] if all_selections else fixture["selections"][2:3]
        output = core.PITStore(engine).get_source_snapshot(selections, as_of=CUTOFF, start_date=START)
        return clock, engine, output["selections"][2], output

    def test_budget_registry3_snapshot3_refreshes_remaining_timeout(self):
        clock, engine, row, _ = self.run_timed(registry_cost=3.0, snapshot_cost=3.0)
        self.assertLessEqual(clock.now, 5.0)
        self.assertEqual(row["reason"], "read_failed")
        self.assertIsNone(row["value"])
        snapshot = [r for r in engine.trace if r["sql"].startswith("WITH eligible AS")]
        self.assertEqual(len(snapshot), 1)
        self.assertLessEqual(snapshot[0]["timeout_ms"], 2000)
        self.assertEqual(snapshot[0]["started"], 3.0)

    def test_budget_acquisition_expiry_issues_no_setup_or_query(self):
        for acquire in (5.1, 5.0, 4.9995):
            with self.subTest(acquire=acquire):
                clock, engine, row, _ = self.run_timed(acquire=acquire)
                self.assertEqual(engine.trace, [])
                self.assertEqual(engine.acquisitions, 1)
                self.assertEqual(row["reason"], "read_failed")
                self.assertIsNone(row["value"])

    def test_budget_registry2_snapshot2_keeps_zero_available(self):
        clock, engine, row, _ = self.run_timed(registry_cost=2.0, snapshot_cost=2.0)
        self.assertEqual(clock.now, 4.0)
        self.assertEqual(row["status"], "available")
        self.assertEqual(row["value"], 0.0)
        data_queries = [r for r in engine.trace if r["sql"].startswith(("SELECT id, name", "WITH eligible AS"))]
        self.assertEqual([r["timeout_ms"] for r in data_queries], [5000, 3000])

    def test_budget_exhausted_phase_stops_later_work_and_never_sets_zero(self):
        for costs, phase in ((dict(registry_cost=5.0), "registry"),
                             (dict(setup_cost=5.0), "setup"),
                             (dict(config_cost=5.1), "config")):
            with self.subTest(costs=costs):
                clock, engine, row, _ = self.run_timed(**costs)
                self.assertFalse(any(r["sql"].startswith("WITH eligible AS") for r in engine.trace))
                if phase != "registry":
                    self.assertFalse(any(r["sql"].startswith("SELECT id, name") for r in engine.trace))
                self.assertEqual(row["reason"], "read_failed")
                self.assertTrue(all(int(r["params"]["timeout_ms"]) > 0 for r in engine.trace
                                    if "timeout_ms" in r["params"]))

    def test_budget_post_query_and_exit_expiry_refuse_availability(self):
        for costs in (dict(registry_cost=3.0, snapshot_cost=3.0, honor_timeout=False),
                      dict(registry_cost=2.0, snapshot_cost=2.0, exit_cost=1.0)):
            with self.subTest(costs=costs):
                clock, engine, row, _ = self.run_timed(**costs)
                self.assertEqual(row["reason"], "read_failed")
                self.assertIsNone(row["value"])

    def test_budget_is_shared_across_sources_without_reset(self):
        clock, engine, _, output = self.run_timed(all_selections=True, registry_cost=2.0, snapshot_cost=2.0)
        self.assertEqual(clock.now, 5.0)
        self.assertEqual(engine.acquisitions, 2)
        self.assertEqual(output["selections"][0]["status"], "available")
        self.assertEqual(output["selections"][1]["reason"], "read_failed")
        self.assertTrue(all(r["reason"] == "read_failed" for r in output["selections"][2:6]))


if __name__ == "__main__":
    unittest.main()
