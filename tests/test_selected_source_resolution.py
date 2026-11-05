"""Finite source pipeline fixtures. No DB/provider imports, calls or global hooks.

The loader executes the actual three module bodies with inert dependency names.
It does not replace EntityMap, Resolver, PITStore or their production methods.
PostgreSQL execution/planner/durability qualification is deliberately separate.
"""
from __future__ import annotations

import __future__
import argparse
import ast
import contextlib
import difflib
import json
import math
import os
import re
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
CUTOFF = datetime(2026, 10, 15, 12, tzinfo=timezone.utc)
START = date(2026, 10, 1)


def load_core(root=ROOT, source_overrides=None):
    """Compile real module bodies; external imports are inert test dependencies."""
    class Logger:
        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    common = dict(argparse=argparse, json=json, math=math, time=time, date=date,
                  datetime=datetime, timedelta=timedelta, timezone=timezone,
                  Decimal=Decimal, difflib=difflib, re=re, os=os,
                  contextmanager=contextlib.contextmanager, monotonic_time=time,
                  text=str, log=Logger(), pd=SimpleNamespace(), Engine=object,
                  Connection=object, CANONICAL_CLOSE_SERIES=frozenset(),
                  SPY_CLOSE_SERIES="YF:SPY:close", SPY_CLOSE_CONTRACT="spy_close_v1")
    loaded = {}
    for name, relative in (("entity", "normalization/entity_map.py"),
                           ("resolver", "normalization/resolver.py"),
                           ("pit", "store/pit.py")):
        source = ((source_overrides or {}).get(relative)
                  if relative in (source_overrides or {}) else (Path(root) / relative).read_text())
        tree = ast.parse(source, filename=relative)
        tree.body = [n for n in tree.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
        namespace = dict(common, __name__="offline_selected_" + name)
        if "entity" in loaded:
            namespace.update({k: v for k, v in loaded["entity"].items() if not k.startswith("__")})
            namespace["__name__"] = "offline_selected_" + name
            # entity's imports are removed, so it cannot replace stdlib bindings.
            namespace.update(common)
        exec(compile(tree, relative, "exec", flags=__future__.annotations.compiler_flag), namespace)
        loaded[name] = namespace
    return SimpleNamespace(EntityMap=loaded["entity"]["EntityMap"],
                           Resolver=loaded["resolver"]["Resolver"],
                           PITStore=loaded["pit"]["PITStore"],
                           validate=loaded["pit"]["validate_source_snapshot_request"],
                           entity_globals=loaded["entity"], resolver_globals=loaded["resolver"],
                           pit_globals=loaded["pit"])


def fixture_inputs():
    definitions = [
        ("fx_rates", "currency", "EUR", "FX:EUR:close", "yfinance", "synthetic_fx_eur", 1.1),
        ("ag_commodity_futures", "contract", "CC=F", "YF:CC=F:close", "YFINANCE_COMMODITY_FUTURES", "synthetic_cc", 7000),
        ("finra_short_volume", "ticker", "AAPL", "finra:short_volume:AAPL", "FINRA_SHORT_VOLUME", "synthetic_short_aapl", 0),
        ("sec_ftd", "cusip", "037833100", "sec:ftd_balance:037833100", "SEC_FTD", "synthetic_ftd_037833100", 25),
        ("nyfed_gscpi", "global", None, "NYFED:gscpi", "NYFED_GSCPI", "synthetic_gscpi", -0.25),
        ("baltic_exchange", "index", "BDI", "baltic.bdi", "Baltic_Exchange", "synthetic_bdi", 2000),
    ]
    selections, registry, catalog, raw = [], [], [], []
    for index, (key, scope, identity, series, source, feature, value) in enumerate(definitions):
        fid, source_id = 8101 + index, 9101 + index
        selections.append(dict(source_key=key, scope=scope, ticker_or_cusip=identity,
                               series_id=series, expected_source_name=source,
                               feature_name=feature, feature_id=fid))
        registry.append(dict(id=fid, name=feature, family="synthetic"))
        catalog.append(dict(id=source_id, name=source, priority_rank=1))
        raw.append(dict(series_id=series, source_id=source_id, obs_date=date(2026, 10, 13),
                        pull_timestamp=datetime(2026, 10, 14, 8, tzinfo=timezone.utc),
                        pull_status="SUCCESS", value=value))
    return selections, raw, registry, catalog


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class StoredFixture:
    """Inert relational fixture, not a SQL implementation or PG qualification.

    Boundary rows react to the predicates actually sent by PITStore. Tests also
    assert its query caps and read-only transaction/timeout requests directly.
    """
    def __init__(self, rows, registry, catalog, retractions=()):
        self.rows, self.registry, self.catalog = rows, registry, catalog
        self.retractions = list(retractions)
        self.calls = []

    def begin(self):
        return contextlib.nullcontext(self)

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        params = params or {}
        self.calls.append((sql, dict(params)))
        if sql == "SET TRANSACTION READ ONLY" or "set_config('statement_timeout'" in sql:
            return FakeResult([])
        if sql.startswith("SELECT id, name FROM feature_registry"):
            return FakeResult([(r["id"], r["name"]) for r in self.registry
                               if r["id"] == params["feature_id"] and r["name"] == params["feature_name"]])
        if not sql.startswith("WITH eligible AS"):
            raise AssertionError("unexpected SQL at inert boundary")
        candidates = [dict(r) for r in self.rows if r["feature_id"] == params["feature_id"]]
        guards = [
            ("rs.obs_date >= :start_date", lambda r: r["obs_date"] >= params["start_date"]),
            ("rs.obs_date <= :as_of_date", lambda r: r["obs_date"] <= params["as_of_date"]),
            ("rs.release_date <= :as_of_date", lambda r: r["release_date"] <= params["as_of_date"]),
            ("rs.vintage_date <= :as_of_date", lambda r: r["vintage_date"] <= params["as_of_date"]),
        ]
        for clause, predicate in guards:
            if clause in sql:
                candidates = [r for r in candidates if predicate(r)]
        if "rr.retracted_at <= :as_of" in sql:
            candidates = [r for r in candidates if not any(
                all(r[k] == rr[k] for k in ("feature_id", "obs_date", "vintage_date")) and
                rr["retracted_at"] <= params["as_of"] for rr in self.retractions)]
        first = "ORDER BY rs.vintage_date ASC" in sql
        by_date = {}
        for row in candidates:
            observed = row["obs_date"]
            previous = by_date.get(observed)
            if previous is None or ((row["vintage_date"] < previous["vintage_date"]) if first else
                                    (row["vintage_date"] > previous["vintage_date"])):
                by_date[observed] = row
        chosen = sorted(by_date.values(), key=lambda r: r["obs_date"], reverse=True)
        if "LIMIT 1" in sql:
            chosen = chosen[:1]
        catalog = {r["id"]: r["name"] for r in self.catalog}
        for row in chosen:
            row["source_name"] = catalog.get(row["source_priority_used"])
            detail = row.get("conflict_detail")
            row["detail_truncated"] = bool(detail and len(detail) > 2048)
            row["conflict_detail"] = detail[:2048] if detail else detail
        return FakeResult(chosen)


def actual_pipeline(core=None):
    """Public fixture used by the original API/Card author; real core end to end."""
    core = core or load_core()
    selections, raw, registry, catalog = fixture_inputs()
    produced = core.Resolver(None).resolve_selected_rows(
        selections, raw, registry, catalog, as_of=CUTOFF, start_date=START)
    engine = StoredFixture(produced["resolved_rows"], registry, catalog)
    snapshot = core.PITStore(engine).get_source_snapshot(selections, as_of=CUTOFF, start_date=START)
    return dict(core=core, selections=selections, raw=raw, registry=registry, catalog=catalog,
                produced=produced, engine=engine, snapshot=snapshot)


class SelectedResolutionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_core()

    def run_batch(self, mutate=None, **kwargs):
        selections, raw, registry, catalog = fixture_inputs()
        if mutate:
            mutate(selections, raw, registry, catalog)
        return self.core.Resolver(None).resolve_selected_rows(
            selections, raw, registry, catalog, as_of=CUTOFF, start_date=START, **kwargs)

    def test_actual_raw_mapper_resolver_pipeline_preserves_six_values_and_zero(self):
        result = actual_pipeline(self.core)
        self.assertEqual(len(result["produced"]["resolved_rows"]), 6)
        self.assertEqual([r["value"] for r in result["snapshot"]["selections"][:6]],
                         [1.1, 7000.0, 0.0, 25.0, -0.25, 2000.0])
        self.assertFalse(result["produced"]["persisted"])
        self.assertEqual(result["snapshot"]["selections"][6]["reason"], "resolver_unsupported")

    def test_missing_independent_registry_binding_never_resolves(self):
        out = self.run_batch(lambda s, r, f, c: f.pop(0))
        self.assertEqual(out["selections"][0]["reason"], "mapping_missing")
        self.assertNotIn(8101, [r["feature_id"] for r in out["resolved_rows"]])

    def test_wrong_name_with_matching_numeric_id_never_resolves(self):
        out = self.run_batch(lambda s, r, f, c: f[0].update(name="different_feature"))
        self.assertEqual(out["selections"][0]["reason"], "mapping_missing")

    def test_source_catalog_mismatch_never_promotes_raw(self):
        out = self.run_batch(lambda s, r, f, c: c[0].update(name="different_source"))
        self.assertEqual(out["selections"][0]["reason"], "source_mismatch")
        self.assertNotIn(8101, [r["feature_id"] for r in out["resolved_rows"]])

    def test_failed_null_bool_nan_and_infinite_raw_are_not_zero(self):
        for value in (None, True, float("nan"), float("inf")):
            with self.subTest(value=str(value)):
                out = self.run_batch(lambda s, r, f, c: r[0].update(value=value))
                self.assertEqual(out["selections"][0]["reason"], "invalid_value")
        out = self.run_batch(lambda s, r, f, c: r[0].update(pull_status="FAILED"))
        self.assertEqual(out["selections"][0]["reason"], "no_resolved_row")

    def test_future_pull_future_observation_and_before_range_are_excluded(self):
        for change in ({"pull_timestamp": CUTOFF + timedelta(seconds=1)},
                       {"obs_date": date(2026, 10, 16)}, {"obs_date": date(2026, 9, 30)}):
            with self.subTest(change=change):
                out = self.run_batch(lambda s, r, f, c: r[0].update(change))
                self.assertEqual(out["selections"][0]["reason"], "no_resolved_row")

    def test_two_storage_vintages_are_produced_without_inventing_intraday_clock(self):
        def change(s, raw, f, c):
            raw.append(dict(raw[0], value=1.2, pull_timestamp=CUTOFF - timedelta(hours=1)))
        out = self.run_batch(change)
        self.assertEqual([r["value"] for r in out["resolved_rows"] if r["feature_id"] == 8101], [1.1, 1.2])
        self.assertEqual(out["storage_clock"], "pull_date_not_provider_publication")

    def test_ambiguous_same_day_revision_refuses_entire_selector(self):
        out = self.run_batch(lambda s, r, f, c: r.append(dict(r[0], value=99)))
        self.assertEqual(out["selections"][0]["reason"], "invalid_value")
        self.assertNotIn(8101, [r["feature_id"] for r in out["resolved_rows"]])

    def test_raw_cap_and_unselected_series_are_rejected(self):
        for change in (lambda s, r, f, c: r.extend([dict(r[0])] * 4096),
                       lambda s, r, f, c: r[0].update(series_id="not_selected")):
            with self.assertRaises(ValueError):
                self.run_batch(change)

    def test_selected_mapper_is_inert_and_does_not_mutate_default_maps(self):
        selections, _, registry, _ = fixture_inputs()
        seed = dict(self.core.entity_globals["SEED_MAPPINGS"])
        mapper = self.core.EntityMap.for_selected_bindings(selections, registry)
        self.assertEqual(mapper.get_feature_id("FX:EUR:close"), 8101)
        self.assertIsNone(mapper.get_feature_id("YF:SPY:close"))
        self.assertEqual(seed, self.core.entity_globals["SEED_MAPPINGS"])

    def test_shared_bulk_priority_and_conflict_behavior(self):
        def source(value, rank, name):
            return dict(value=value, priority_rank=rank, source_name=name, source_id=rank)
        choose = self.core.Resolver._select_source_winner
        winner, conflict, detail = choose([source(101, 2, "second"), source(100, 1, "first")], "")
        self.assertEqual(winner["value"], 100)
        self.assertTrue(conflict)
        self.assertEqual(json.loads(detail)["sources"][0]["source_name"], "first")
        self.assertFalse(choose([source(0, 1, "first"), source(0, 2, "second")], "")[1])
        self.assertTrue(choose([source(0, 1, "first"), source(1, 2, "second")], "")[1])


if __name__ == "__main__":
    unittest.main()
