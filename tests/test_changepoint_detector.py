"""
Tests for the regime changepoint detector (discovery/changepoint_detector.py)
and its thesis reader (analysis/thesis_scorer.py::_score_regime_changepoints).

Point-in-time reads go through ``store.pit.PITWindowCache``, the in-memory
implementation of ``PITStore.get_pit`` (same obs_date / release_date /
retraction filters and the same vintage pick), over invented vintages. The
database is a small fake: feature_registry lookups and a signal_registry that
applies the publisher's ``WHERE NOT EXISTS (signal_id)`` rule.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import discovery.changepoint_detector as cd
from discovery.changepoint_detector import (
    CHANGEPOINT_DETECTOR_VERSION,
    FRESH_BREAK_DAYS,
    REGIME_CHANGEPOINT_FEATURES,
    REQUIRED_CONCEPTS,
    ChangeReport,
    RegimeFeature,
    check_declared_features,
    run_changepoint_cycle,
    scan_for_changepoints,
)
from store.pit import PIT_WINDOW_COLUMNS, PITWindowCache

AS_OF = date(2026, 11, 20)            # a Friday
NOW = datetime(2026, 11, 21, 12, tzinfo=timezone.utc)
N_OBS = 300


# ---------------------------------------------------------------------------
# Fixtures: invented vintages, a fake registry and signal store
# ---------------------------------------------------------------------------


def business_days(n: int = N_OBS, end: date = AS_OF) -> list[date]:
    return list(pd.bdate_range(end=end, periods=n).date)


def drift_shift_series(n: int = N_OBS, shift_at: int | None = None, drift: float = 3.0,
                       seed: int = 7) -> np.ndarray:
    """Random walk whose drift jumps from 0 to ``drift`` (noise units) at ``shift_at``."""
    rng = np.random.default_rng(seed)
    shift_at = n - 14 if shift_at is None else shift_at
    inc = np.concatenate([rng.normal(0, 1, shift_at), rng.normal(drift, 1, n - shift_at)])
    return 20 + np.cumsum(inc) * 0.1


def vintages(fid: int, days: list[date], values, *, release_lag: int = 0,
             release: date | None = None) -> list[tuple]:
    """One vintage per observation; vintage_date = release_date."""
    rows = []
    for d, v in zip(days, values):
        rel = release or d + timedelta(days=release_lag)
        rows.append((fid, d, float(v), rel, rel, None))
    return rows


def window(rows: list[tuple], fids: list[int]) -> PITWindowCache:
    return PITWindowCache(
        pd.DataFrame(rows, columns=list(PIT_WINDOW_COLUMNS)),
        feature_ids=fids, start_date=None, end_date=date(2101, 1, 1), max_as_of=date(2101, 1, 1),
    )


class _Result:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows


class FakeEngine:
    """feature_registry and retraction lookups, and an insert-only signal_registry.

    The signal store applies the publisher's SQL rules: the read of rows
    already written (by signal_id, or by ticker inside the window), and the
    insert guard (no row at or before an existing availability of the same
    signal_id; (signal_id, valid_from) unique).
    """

    def __init__(self, registry: list[tuple], retractions: list[tuple] = ()):
        self.registry = registry                 # (id, name, family, deprecated_at)
        self.retractions = list(retractions)     # (feature_id, obs_date, retracted_at)
        self.rows: list[dict] = []               # insert params, in write order
        self.statements: list[tuple[str, dict]] = []

    @property
    def signals(self) -> dict[str, dict]:
        """signal_id -> its latest row."""
        out: dict[str, dict] = {}
        for row in self.rows:
            if row["sid"] not in out or row["vfrom"] > out[row["sid"]]["vfrom"]:
                out[row["sid"]] = row
        return out

    def rows_for(self, sid: str) -> list[dict]:
        return sorted((r for r in self.rows if r["sid"] == sid), key=lambda r: r["vfrom"])

    def connect(self):
        return _Conn(self)

    begin = connect


class _Conn:
    def __init__(self, engine: FakeEngine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        params = dict(params or {})
        self.engine.statements.append((sql, params))
        if "FROM feature_registry" in sql:
            names = set(params["names"])
            return _Result([r for r in self.engine.registry if r[1] in names])
        if "FROM resolved_series_retractions" in sql:
            return _Result([
                r for r in self.engine.retractions
                if r[0] in params["fids"] and params["start"] <= r[1] <= params["end"]
                and r[2] <= params["cutoff"]
            ])
        if "metadata->>'support_sha256'" in sql and sql.lstrip().startswith("SELECT"):
            out = []
            for r in self.engine.rows:
                if (r["src"], r["stype"], r["version"]) != (params["src"], params["stype"], params["version"]):
                    continue
                if r["sid"] in params["ids"] or (r["ticker"] in params["tickers"] and r["vfrom"] > params["since"]):
                    meta = json.loads(r["meta"])
                    out.append((r["sid"], r["ticker"], r["vfrom"], r["conf"],
                                meta.get("support_sha256"), meta.get("status"), meta.get("break_date"),
                                meta.get("effective_known_at"), meta.get("first_published_at")))
            return _Result(out)
        if "INSERT INTO signal_registry" in sql:
            assert "valid_from >= CAST(:vfrom AS timestamptz)" in sql
            if any(r["sid"] == params["sid"] and r["vfrom"] >= params["vfrom"] for r in self.engine.rows):
                return _Result(rowcount=0)
            self.engine.rows.append(params)
            return _Result(rowcount=1)
        raise AssertionError(f"unexpected statement: {sql}")


def one_feature(key: str = "vix", stress_sign: int = 1, name: str = "vix_spot",
                concept: str = "vol", max_age_days: int = 5) -> RegimeFeature:
    return RegimeFeature(key, concept, (name,), stress_sign, max_age_days, "test")


def scan(engine, pit, features, as_of=AS_OF, **kw) -> ChangeReport:
    return scan_for_changepoints(engine, as_of=as_of, features=features, pit=pit, **kw)


VIX = [(11, "vix_spot", "vol", None)]


# ---------------------------------------------------------------------------
# ChangeReport
# ---------------------------------------------------------------------------


class TestChangeReport:
    def test_create_report(self) -> None:
        report = ChangeReport(
            timestamp=datetime.now(timezone.utc),
            features_scanned=10,
            changepoints_found=3,
            regime_changes=[],
            elapsed_seconds=1.5,
        )
        assert report.features_scanned == 10
        assert report.changepoints_found == 3
        assert report.breaks == [] and report.skipped == {}
        assert report.detector_version == CHANGEPOINT_DETECTOR_VERSION


# ---------------------------------------------------------------------------
# Declared feature set
# ---------------------------------------------------------------------------


class TestDeclaredFeatureSet:
    def test_covers_every_concept(self) -> None:
        concepts = {f.concept for f in REGIME_CHANGEPOINT_FEATURES}
        assert REQUIRED_CONCEPTS == {"vol", "credit", "rates", "equity", "liquidity", "macro"}
        assert REQUIRED_CONCEPTS <= concepts

    def test_no_ephemeris_or_astrological_names(self) -> None:
        names = [c for f in REGIME_CHANGEPOINT_FEATURES for c in f.candidates]
        for token in ("ephemeris", "lunar", "planet", "astro", "vedic", "solar", "moon"):
            assert not any(token in n for n in names), token

    def test_stress_signs(self) -> None:
        sign = {f.key: f.stress_sign for f in REGIME_CHANGEPOINT_FEATURES}
        assert sign == {
            "vix": +1,                 # VIX up = risk-off
            "hy_spread": +1,           # HY spread up = risk-off
            "ig_spread": +1,
            "yield_curve_10y2y": -1,   # flattening / inverting = stress
            "treasury_10y": +1,
            "sp500": -1,               # SPX up = risk-on
            "fed_net_liquidity": -1,   # draining liquidity = risk-off
            "dollar_index": +1,
            "initial_claims": +1,
            "breakeven_10y": -1,       # falling breakevens = deflation fear
        }

    def test_reuses_auto_regime_bindings(self) -> None:
        from scripts.auto_regime import DEFAULT_FEATURE_WEIGHTS, REGIME_FEATURE_SOURCES

        for f in REGIME_CHANGEPOINT_FEATURES:
            if f.key in DEFAULT_FEATURE_WEIGHTS:
                assert f.stress_sign == (1 if DEFAULT_FEATURE_WEIGHTS[f.key] > 0 else -1)
                head = tuple(REGIME_FEATURE_SOURCES[f.key])
                assert f.candidates[:len(head)] == head
        treasury = next(f for f in REGIME_CHANGEPOINT_FEATURES if f.key == "treasury_10y")
        assert "treasury_10y" in treasury.candidates   # DGS10's live feature

    def test_check_rejects_missing_concept_and_ephemeris(self) -> None:
        without_macro = tuple(f for f in REGIME_CHANGEPOINT_FEATURES if f.concept != "macro")
        with pytest.raises(RuntimeError, match="miss concepts"):
            check_declared_features(without_macro)
        astro = REGIME_CHANGEPOINT_FEATURES + (one_feature("moon", name="lunar_phase"),)
        with pytest.raises(RuntimeError, match="ephemeris"):
            check_declared_features(astro)

    def test_selection_is_by_name_not_model_eligible(self) -> None:
        engine = FakeEngine([])
        scan(engine, window([], [1]), REGIME_CHANGEPOINT_FEATURES)
        sql = [(s, p) for s, p in engine.statements if "feature_registry" in s]
        assert len(sql) == 1
        assert "name = ANY(:names)" in sql[0][0]
        assert "model_eligible" not in sql[0][0] and "LIMIT" not in sql[0][0]
        assert set(sql[0][1]["names"]) == {c for f in REGIME_CHANGEPOINT_FEATURES for c in f.candidates}

    def test_alternative_family_refused_at_runtime(self) -> None:
        engine = FakeEngine([(11, "vix_spot", "alternative", None)])
        pit = window(vintages(11, business_days(), drift_shift_series()), [11])
        report = scan(engine, pit, (one_feature(),))
        assert report.features_scanned == 0
        assert "never scanned" in report.skipped["vix"]


# ---------------------------------------------------------------------------
# Skips are reported
# ---------------------------------------------------------------------------


class TestSkips:
    def test_short_stale_unregistered_and_deprecated_are_reported(self) -> None:
        days = business_days()
        stale_days = business_days(end=AS_OF - timedelta(days=20))
        engine = FakeEngine([
            (1, "short", "vol", None), (2, "stale", "credit", None),
            (3, "old_name", "rates", datetime(2026, 1, 1, tzinfo=timezone.utc)),
        ])
        rows = vintages(1, days[-50:], np.arange(50.0)) + vintages(2, stale_days, drift_shift_series())
        report = scan(engine, window(rows, [1, 2, 3]), (
            one_feature("a", name="short"), one_feature("b", name="stale", concept="credit"),
            one_feature("c", name="old_name", concept="rates"), one_feature("d", name="missing"),
        ))
        assert report.features_scanned == 0
        assert "50 observations" in report.skipped["a"]
        assert "stale" in report.skipped["b"] and "limit 5" in report.skipped["b"]
        assert "deprecated" in report.skipped["c"]
        assert "not registered" in report.skipped["d"]

    def test_falls_through_to_the_next_candidate(self) -> None:
        days = business_days()
        engine = FakeEngine([(1, "first", "credit", None), (2, "second", "credit", None)])
        rows = vintages(1, days[-40:], np.arange(40.0)) + vintages(2, days, drift_shift_series())
        spec = RegimeFeature("hy", "credit", ("first", "second"), 1, 5, "test")
        report = scan(engine, window(rows, [1, 2]), (spec,))
        assert report.features_scanned == 1
        assert {b.feature for b in report.breaks} == {"second"}


# ---------------------------------------------------------------------------
# Point-in-time read
# ---------------------------------------------------------------------------


class TestPointInTime:
    def test_reads_latest_as_of_bounded_to_as_of(self) -> None:
        calls = []

        class Recorder:
            def get_pit(self, fids, as_of_date, policy, *, start_date=None, end_date=None):
                calls.append((fids, as_of_date, policy, start_date, end_date))
                return pd.DataFrame(columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"])

        scan(FakeEngine(VIX), Recorder(), (one_feature(),))
        assert calls == [([11], AS_OF, "LATEST_AS_OF", AS_OF - timedelta(days=cd.LOOKBACK_DAYS), AS_OF)]

    def test_future_rows_have_no_effect(self) -> None:
        days = business_days()
        base = vintages(11, days, drift_shift_series())
        future_obs = vintages(11, [AS_OF + timedelta(days=i) for i in range(1, 40)] + [date(2100, 12, 31)],
                              np.full(40, 1e6))
        future_vintage = vintages(11, days[-30:], np.full(30, -1e6), release=AS_OF + timedelta(days=1))
        engine = FakeEngine(VIX)
        clean = scan(engine, window(base, [11]), (one_feature(),))
        poisoned = scan(engine, window(base + future_obs + future_vintage, [11]), (one_feature(),))
        assert clean.fresh_breaks
        assert [b.signal for b in clean.breaks] == [b.signal for b in poisoned.breaks]
        assert [b.valid_from for b in clean.breaks] == [b.valid_from for b in poisoned.breaks]

    def test_vintage_pick_is_latest_known_at_as_of(self) -> None:
        # First print: a quiet random walk ending 11-17. A revision released on
        # 11-18 rewrites the last 20 observations with a strong up-drift; a
        # later revision, released after as_of, flattens them again.
        days = business_days(end=date(2026, 11, 17))
        quiet = drift_shift_series(drift=0.0, seed=3)
        revised = drift_shift_series(drift=3.0, seed=3)
        revision_day = date(2026, 11, 18)
        rows = (vintages(11, days, quiet)
                + vintages(11, days[-20:], revised[-20:], release=revision_day)
                + vintages(11, days[-20:], quiet[-20:], release=AS_OF + timedelta(days=3)))
        engine = FakeEngine(VIX)
        first_print_only = scan(engine, window(vintages(11, days, quiet), [11]), (one_feature(),))
        report = scan(engine, window(rows, [11]), (one_feature(),))
        assert not first_print_only.fresh_breaks
        (fresh,) = report.fresh_breaks
        assert fresh.direction == "bearish"
        # Known when the revision was released, not when the observations were.
        assert fresh.known_at_date == revision_day
        assert fresh.valid_from == datetime.combine(revision_day, time.max, tzinfo=timezone.utc)
        # One value per obs_date: the duplicate vintages do not add observations.
        assert fresh.signal.confirm_index <= N_OBS - 1

    def test_non_finite_values_are_dropped_not_filled(self) -> None:
        values = drift_shift_series()
        values[100] = np.nan
        report = scan(FakeEngine(VIX), window(vintages(11, business_days(), values), [11]), (one_feature(),))
        assert report.features_scanned == 1
        assert report.fresh_breaks      # the recent break survives a hole far before it


# ---------------------------------------------------------------------------
# Direction per feature
# ---------------------------------------------------------------------------


class TestDirection:
    def _directions(self, drift: float) -> dict[str, str]:
        days = business_days()
        values = drift_shift_series(drift=drift)
        engine = FakeEngine([(11, "vix_spot", "vol", None), (686, "sp500_full", "equity", None),
                             (12, "hy_oas_spread", "credit", None)])
        rows = vintages(11, days, values) + vintages(686, days, values) + vintages(12, days, values)
        report = scan(engine, window(rows, [11, 12, 686]), (
            one_feature(),
            one_feature("hy_spread", 1, "hy_oas_spread", "credit"),
            one_feature("sp500", -1, "sp500_full", "equity"),
        ))
        return {b.feature: b.direction for b in report.fresh_breaks}

    def test_same_up_drift_reads_by_stress_sign(self) -> None:
        assert self._directions(3.0) == {
            "vix_spot": "bearish", "hy_oas_spread": "bearish", "sp500_full": "bullish"}

    def test_down_drift_flips_it(self) -> None:
        assert self._directions(-3.0) == {
            "vix_spot": "bullish", "hy_oas_spread": "bullish", "sp500_full": "bearish"}

    def test_published_row_carries_the_risk_reading(self) -> None:
        engine = FakeEngine(VIX)
        run_changepoint_cycle(engine, as_of=AS_OF, now=NOW,
                              pit=window(vintages(11, business_days(), drift_shift_series()), [11]))
        (row,) = engine.signals.values()
        meta = json.loads(row["meta"])
        assert row["direction"] == "bearish" and meta["risk"] == "risk_off"
        assert meta["stress_sign"] == 1 and meta["concept"] == "vol"
        assert meta["detector_version"] == CHANGEPOINT_DETECTOR_VERSION
        assert row["version"] == CHANGEPOINT_DETECTOR_VERSION
        assert (row["src"], row["stype"], row["ticker"]) == (
            "discovery.changepoint_detector", "regime_change", "vix_spot")
        assert 0 < row["val"] == abs(row["zscore"]) <= 99


# ---------------------------------------------------------------------------
# Identity, valid_from and recency
# ---------------------------------------------------------------------------


class TestIdentityAndRecency:
    def test_signal_id_and_valid_from(self) -> None:
        # Released the day after each observation; the newest obs is 11-19.
        days = business_days(end=date(2026, 11, 19))
        pit = window(vintages(11, days, drift_shift_series(), release_lag=1), [11])
        (b,) = scan(FakeEngine(VIX), pit, (one_feature(),)).fresh_breaks
        assert b.signal_id == f"{CHANGEPOINT_DETECTOR_VERSION}:vix_spot:{b.break_date.isoformat()}"
        # Known when the confirming observation was released: the end of that UTC day.
        assert b.known_at_date == b.confirm_date + timedelta(days=1)
        assert b.valid_from == datetime.combine(b.known_at_date, time.max, tzinfo=timezone.utc)
        assert b.break_date < b.confirm_date <= AS_OF

    def test_rescan_writes_nothing_new(self) -> None:
        engine = FakeEngine(VIX)
        pit = window(vintages(11, business_days(), drift_shift_series()), [11])
        first = run_changepoint_cycle(engine, as_of=AS_OF, now=NOW, pit=pit)
        second = run_changepoint_cycle(engine, as_of=AS_OF, now=NOW + timedelta(hours=12), pit=pit)
        assert first["signals_published"] == 1 and second["signals_published"] == 0
        assert len(engine.signals) == 1

    def test_next_day_rescan_keeps_the_id_and_writes_nothing_new(self) -> None:
        engine = FakeEngine(VIX)
        days, values = business_days(), drift_shift_series()
        run_changepoint_cycle(engine, as_of=AS_OF, now=NOW, pit=window(vintages(11, days, values), [11]))
        (sid,) = engine.signals
        # One more observation, and a window that starts a day later.
        later_days = days[1:] + [date(2026, 11, 23)]
        later_values = np.append(values[1:], values[-1] + 0.3)
        nxt = scan(engine, window(vintages(11, later_days, later_values), [11]), (one_feature(),),
                   as_of=date(2026, 11, 23))
        assert sid in {b.signal_id for b in nxt.fresh_breaks}
        assert cd.publish_regime_signals(engine, nxt, now=NOW + timedelta(days=2)) == 0

    def test_old_breaks_are_not_emitted(self) -> None:
        # The same break scanned 15 sessions later (the drift goes on): it is
        # in the lookback, it is not fresh, nothing is written.
        later = AS_OF + timedelta(days=21)
        days = business_days(N_OBS + 15, end=later)
        values = drift_shift_series(N_OBS + 15, shift_at=N_OBS - 14)
        engine = FakeEngine(VIX)
        out = run_changepoint_cycle(engine, as_of=later, now=NOW + timedelta(days=21),
                                    pit=window(vintages(11, days, values), [11]))
        assert out["breaks_in_lookback"] >= 1
        assert out["changepoints_found"] == 0 and out["signals_published"] == 0
        assert engine.signals == {}

    def test_break_known_too_long_ago_is_not_fresh(self) -> None:
        # Every vintage released FRESH_BREAK_DAYS + 1 days before as_of.
        rows = vintages(11, business_days(end=AS_OF - timedelta(days=FRESH_BREAK_DAYS + 1)),
                        drift_shift_series())
        report = scan(FakeEngine(VIX), window(rows, [11]), (one_feature(max_age_days=10),))
        assert report.breaks and not report.fresh_breaks

    def test_future_valid_from_is_deferred(self) -> None:
        engine = FakeEngine(VIX)
        report = scan(engine, window(vintages(11, business_days(), drift_shift_series()), [11]), (one_feature(),))
        (b,) = report.fresh_breaks
        assert cd.publish_regime_signals(engine, report, now=b.valid_from - timedelta(microseconds=1)) == 0
        assert engine.signals == {}
        assert cd.publish_regime_signals(engine, report, now=b.valid_from) == 1

    def test_cycle_summary_is_json_safe(self) -> None:
        engine = FakeEngine(VIX)
        out = run_changepoint_cycle(engine, as_of=AS_OF, now=NOW,
                                    pit=window(vintages(11, business_days(), drift_shift_series()), [11]))
        json.dumps(out)
        for key in ("features_scanned", "changepoints_found", "signals_published", "elapsed_seconds",
                    "features_skipped", "breaks_in_lookback", "detector_version", "top_changes"):
            assert key in out
        assert out["top_changes"][0]["direction"] == "bearish"

    def test_breaks_do_not_depend_on_the_window_start(self) -> None:
        days = business_days(400)
        values = drift_shift_series(400, shift_at=330)
        engine = FakeEngine(VIX)
        full = scan(engine, window(vintages(11, days, values), [11]), (one_feature(),))
        later_start = scan(engine, window(vintages(11, days[80:], values[80:]), [11]), (one_feature(),))

        # A break needs 2 * pre_window observations of context before it; every
        # break whose context lies in both windows is found identically.
        from timeseries.autobnn import CAUSAL_PRE_WINDOW as W

        first_decidable = days[80 + 2 * W]

        def key(report):
            return [(b.signal_id, b.valid_from, b.direction, round(b.signal.statistic, 9))
                    for b in report.breaks if b.break_date >= first_decidable]

        assert key(full) and key(full) == key(later_start)
        assert len(later_start.breaks) == len(key(later_start))
        assert replace(full.breaks[0], fresh=False).signal_id == full.breaks[0].signal_id


# ---------------------------------------------------------------------------
# Late vintages: known_at, waiting, revisions and withdrawals (#1367 review)
# ---------------------------------------------------------------------------
#
# Fixture shape: a quiet first print ending 11-17 (each value released and
# fetched on its own obs_date), then a re-fetch of the last 20 observations
# that adds a strong up-drift. The re-fetch keeps release_date = obs_date
# (the bridge's back-dated stamp) and carries the fetch day as vintage_date.

LAST_OBS = date(2026, 11, 17)


def late_vintage_rows(fetched: date, *, seed: int = 3) -> list[tuple]:
    days = business_days(end=LAST_OBS)
    quiet = drift_shift_series(drift=0.0, seed=seed)
    drifted = drift_shift_series(drift=3.0, seed=seed)
    refetch = [(11, d, float(v), d, fetched, None) for d, v in zip(days[-20:], drifted[-20:])]
    return vintages(11, days, quiet) + refetch


class TestLateVintages:
    def test_backdated_refetch_takes_its_vintage_day(self) -> None:
        # Fetched 11-19 with release_date = obs_date: known on 11-19, not on
        # the release dates (9ff1bf4a used max(release_date) = 11-17).
        pit = window(late_vintage_rows(date(2026, 11, 19)), [11])
        (b,) = scan(FakeEngine(VIX), pit, (one_feature(),)).fresh_breaks
        assert b.known_at_date == date(2026, 11, 19)
        assert b.valid_from == datetime.combine(date(2026, 11, 19), time.max, tzinfo=timezone.utc)
        assert b.support_start_date is not None and b.support_start_date < b.break_date

    def test_refetched_old_observations_are_not_discarded_as_old(self) -> None:
        # Weekly-style lag: newest obs 8 days before as_of, fetched yesterday.
        # On release dates alone the break reads 8 days old and is dropped.
        as_of = LAST_OBS + timedelta(days=8)
        pit = window(late_vintage_rows(as_of - timedelta(days=1)), [11])
        report = scan(FakeEngine(VIX), pit, (one_feature(max_age_days=12),), as_of=as_of)
        (b,) = report.fresh_breaks
        assert b.known_at_date == as_of - timedelta(days=1)

    def test_vintage_after_as_of_waits_then_publishes_on_its_day(self) -> None:
        # release_date <= as_of < vintage_date: a value the read at as_of must
        # not use. Whether or not get_pit fences it (#1365), nothing dated
        # before the fetch may be written.
        fetched = date(2026, 11, 22)
        engine = FakeEngine(VIX)
        pit = window(late_vintage_rows(fetched), [11])
        spec = (one_feature(max_age_days=10),)
        report = scan(engine, pit, spec)
        assert not report.fresh_breaks
        assert all(b.known_at_date > AS_OF for b in report.breaks if b.break_date > date(2026, 10, 20))
        assert cd.publish_regime_signals(engine, report, now=NOW) == 0 and engine.rows == []

        # Once the fetch day is past, the break is written, valid from that day.
        report = scan(engine, pit, spec, as_of=date(2026, 11, 23))
        assert cd.publish_regime_signals(engine, report, now=datetime(2026, 11, 24, 12, tzinfo=timezone.utc)) == 1
        (row,) = engine.rows
        assert row["vfrom"] == datetime.combine(fetched, time.max, tzinfo=timezone.utc)
        assert json.loads(row["meta"])["known_at_date"] == fetched.isoformat()

    def test_rescan_after_a_backdated_revision_never_moves_valid_from_earlier(self) -> None:
        days = business_days(end=date(2026, 11, 19))
        values = drift_shift_series()
        engine = FakeEngine(VIX)
        first = run_changepoint_cycle(engine, as_of=AS_OF, now=NOW, pit=window(vintages(11, days, values), [11]))
        assert first["publish"]["first"] == 1
        (sid,) = engine.signals
        v1 = engine.signals[sid]["vfrom"]
        # A revision of one support value lands later, stamped with a vintage
        # day before the first row's valid_from.
        revised = vintages(11, days, values) + [(11, days[-5], float(values[-5]) + 0.05, days[-5],
                                                 date(2026, 11, 18), None)]
        later = NOW + timedelta(hours=12)
        second = run_changepoint_cycle(engine, as_of=AS_OF, now=later, pit=window(revised, [11]))
        rows = engine.rows_for(sid)
        assert second["publish"]["revision"] == 1 and len(rows) == 2
        assert rows[0]["vfrom"] == v1 and rows[1]["vfrom"] == later > v1
        meta, first_meta = json.loads(rows[1]["meta"]), json.loads(rows[0]["meta"])
        assert meta["supersedes_valid_from"] == v1.isoformat() and meta["status"] == "live"
        assert meta["support_sha256"] != first_meta["support_sha256"]
        # Two clocks, two fields: the inputs' effective known-at, and the detector's scans.
        assert first_meta["effective_known_at"] == v1.isoformat()
        assert first_meta["first_published_at"] == first_meta["published_at"] == NOW.isoformat()
        assert meta["first_published_at"] == NOW.isoformat() and meta["published_at"] == later.isoformat()
        assert datetime.fromisoformat(meta["effective_known_at"]) < rows[1]["vfrom"]
        # The same revised data again: nothing new.
        third = run_changepoint_cycle(engine, as_of=AS_OF, now=later + timedelta(hours=12), pit=window(revised, [11]))
        assert third["signals_published"] == 0 and third["publish"]["unchanged"] == 1

    def test_revision_that_removes_the_break_writes_a_withdrawal(self) -> None:
        days = business_days(end=date(2026, 11, 19))
        values = drift_shift_series()
        quiet = drift_shift_series(drift=0.0)
        engine = FakeEngine(VIX)
        run_changepoint_cycle(engine, as_of=AS_OF, now=NOW, pit=window(vintages(11, days, values), [11]))
        (sid,) = engine.signals
        refetch = [(11, d, float(v), d, AS_OF, None) for d, v in zip(days[-20:], quiet[-20:])]
        later = NOW + timedelta(hours=12)
        out = run_changepoint_cycle(engine, as_of=AS_OF, now=later,
                                    pit=window(vintages(11, days, values) + refetch, [11]))
        assert out["publish"]["withdrawal"] == 1
        latest = engine.signals[sid]
        meta = json.loads(latest["meta"])
        assert (latest["direction"], latest["conf"], meta["status"]) == ("neutral", 0.0, "withdrawn")
        assert latest["vfrom"] == later
        assert (meta["first_published_at"], meta["published_at"]) == (NOW.isoformat(), later.isoformat())
        assert meta["effective_known_at"] == datetime.combine(AS_OF, time.max, tzinfo=timezone.utc).isoformat()
        again = run_changepoint_cycle(engine, as_of=AS_OF, now=later + timedelta(hours=12),
                                      pit=window(vintages(11, days, values) + refetch, [11]))
        assert again["signals_published"] == 0

    def test_retraction_inside_the_support_moves_known_at(self) -> None:
        days = business_days(end=date(2026, 11, 19))
        values = drift_shift_series()
        wrong_day = days[-3]
        retracted_at = datetime(2026, 11, 20, 10, tzinfo=timezone.utc)
        rows = vintages(11, days, values) + [(11, wrong_day, 99.0, wrong_day, wrong_day + timedelta(days=1),
                                              retracted_at)]
        engine = FakeEngine(VIX, retractions=[(11, wrong_day, retracted_at)])
        (b,) = scan(engine, window(rows, [11]), (one_feature(),)).fresh_breaks
        # The retracted value is not read, and the break is only known once it was retracted.
        assert b.known_at_date == date(2026, 11, 20)
        clean = scan(FakeEngine(VIX), window(vintages(11, days, values), [11]), (one_feature(),)).fresh_breaks
        assert [c.support_sha256 for c in clean] == [b.support_sha256]
        assert clean[0].known_at_date == clean[0].confirm_date == date(2026, 11, 18)


# ---------------------------------------------------------------------------
# No edge artifacts
# ---------------------------------------------------------------------------


class TestNoEdgeArtifacts:
    def test_linear_trend_has_no_breaks(self) -> None:
        pit = window(vintages(11, business_days(), np.linspace(10, 40, N_OBS)), [11])
        report = scan(FakeEngine(VIX), pit, (one_feature(),))
        assert report.features_scanned == 1 and report.breaks == []

    def test_random_walks_have_no_breaks_at_the_window_edges(self) -> None:
        # The old fallback (centred rolling mean, NaN ends filled with the
        # series mean) put a full-confidence break near index 14 and n-15 of
        # every window. The causal test cannot decide a break without its full
        # context, so the first and last breaks it can report are at 2w and
        # n-1-(m+k).
        from timeseries.autobnn import CAUSAL_PEAK_WINDOW as M
        from timeseries.autobnn import CAUSAL_POST_WINDOW as K
        from timeseries.autobnn import CAUSAL_PRE_WINDOW as W

        days = business_days()
        engine = FakeEngine(VIX)
        fresh = total = 0
        for seed in range(40):
            values = 20 + np.cumsum(np.random.default_rng(seed).normal(0, 1, N_OBS))
            report = scan(engine, window(vintages(11, days, values), [11]), (one_feature(),))
            for b in report.breaks:
                assert 2 * W <= b.signal.change_index <= N_OBS - 1 - (M + K)
            total += len(report.breaks)
            fresh += len(report.fresh_breaks)
        assert total <= 40            # about one break per three years of a random walk
        assert fresh <= 2             # and almost never a fresh one


# ---------------------------------------------------------------------------
# Old-style mocks still work
# ---------------------------------------------------------------------------


class TestScanForChangepoints:
    def test_no_features_returns_empty(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = []

        report = scan_for_changepoints(engine)

        assert report.features_scanned == 0
        assert report.changepoints_found == 0
        assert set(report.skipped) == {f.key for f in REGIME_CHANGEPOINT_FEATURES}

    def test_max_features_caps_the_declared_set(self) -> None:
        report = scan(FakeEngine([]), window([], [1]), REGIME_CHANGEPOINT_FEATURES, max_features=2)
        assert list(report.skipped) == [f.key for f in REGIME_CHANGEPOINT_FEATURES[:2]]


class TestRunChangepointCycle:
    def test_returns_summary(self) -> None:
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = []

        result = run_changepoint_cycle(engine)

        assert "features_scanned" in result
        assert "changepoints_found" in result
        assert "signals_published" in result
        assert "elapsed_seconds" in result


# ---------------------------------------------------------------------------
# thesis_scorer: the reader of these rows, and the FRAGILE regime context
# ---------------------------------------------------------------------------


class TestThesisReader:
    def test_reader_counts_this_detector_version_and_window(self) -> None:
        from analysis import thesis_scorer

        assert thesis_scorer.CHANGEPOINT_DETECTOR_VERSION == CHANGEPOINT_DETECTOR_VERSION
        assert thesis_scorer.CHANGEPOINT_FRESH_DAYS == FRESH_BREAK_DAYS
        assert (cd.SOURCE_MODULE, cd.SIGNAL_TYPE) == ("discovery.changepoint_detector", "regime_change")

    @staticmethod
    def _read(rows: list[tuple]) -> tuple[dict, object]:
        from analysis import thesis_scorer

        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = rows
        return thesis_scorer._score_regime_changepoints(engine, 0.5), conn

    @staticmethod
    def _row(sid, ticker, direction, conf, valid_from, status="live", break_date="2026-11-10", effective=None):
        """(signal_id, ticker, direction, confidence, valid_from, status, break_date, effective_known_at)"""
        return (sid, ticker, direction, conf, valid_from, status, break_date, effective or valid_from)

    def test_reader_over_published_rows(self) -> None:
        from analysis import thesis_scorer

        now = datetime.now(timezone.utc)
        r = self._row
        rows = [r("vix-b", "vix_spot", "bearish", 1.0, now - timedelta(hours=1), break_date="2026-11-12"),
                r("hy-a", "hy_oas_spread", "bearish", 1.0, now - timedelta(hours=1), status=None),
                r("spx-a", "sp500_full", "bullish", 0.5, now - timedelta(hours=1)),
                r("ig-a", "ig_oas_spread", "neutral", 0.0, now - timedelta(hours=2), status="withdrawn"),
                r("vix-a", "vix_spot", "bullish", 1.0, now - timedelta(days=2), break_date="2026-11-03"),
                r("ig-a", "ig_oas_spread", "bearish", 1.0, now - timedelta(days=1))]
        verdict, conn = self._read(rows)
        statement = conn.execute.call_args_list[-1].args[0]
        assert "provenance = :version" in str(statement)
        assert "metadata->>'effective_known_at'" in str(statement)
        assert statement.compile().params["version"] == CHANGEPOINT_DETECTOR_VERSION
        counts = verdict["category_counts"]
        # ig-a's latest row withdraws it; vix_spot counts once, by its later break (vix-b).
        assert (counts["total"], counts["bearish"], counts["bullish"], counts["withdrawn"],
                counts["breaks_read"], counts["rows_read"]) == (3, 2, 1, 1, 5, 6)
        assert "1 break withdrawn on later data" in verdict["reasoning"]
        # net -1 feature x 10 points x average confidence 2.5 / 3
        assert verdict["score"] == round(-10 * 2.5 / 3, 1)
        assert verdict["confidence"] <= thesis_scorer.CHANGEPOINT_CONFIDENCE_CAP
        assert verdict["data_point"].startswith("3 features with a fresh break")

    @pytest.mark.parametrize("survivor_is_later_break", [True, False])
    @pytest.mark.parametrize("withdrawal_is_newest_row", [True, False])
    @pytest.mark.parametrize("rows_newest_first", [True, False])
    def test_withdrawing_one_break_does_not_hide_another_on_the_same_feature(
        self, survivor_is_later_break: bool, withdrawal_is_newest_row: bool, rows_newest_first: bool,
    ) -> None:
        # Feature vix_spot: break A (live, then withdrawn) and break B (live).
        now = datetime.now(timezone.utc)
        a_date, b_date = ("2026-11-03", "2026-11-12") if survivor_is_later_break else ("2026-11-12", "2026-11-03")
        withdrawn_at, b_at = ((now - timedelta(hours=1), now - timedelta(hours=3)) if withdrawal_is_newest_row
                              else (now - timedelta(hours=3), now - timedelta(hours=1)))
        rows = [self._row("A", "vix_spot", "bullish", 0.9, now - timedelta(days=2), break_date=a_date),
                self._row("A", "vix_spot", "neutral", 0.0, withdrawn_at, status="withdrawn", break_date=a_date),
                self._row("B", "vix_spot", "bearish", 0.8, b_at, break_date=b_date)]
        rows.sort(key=lambda row: row[4], reverse=rows_newest_first)
        verdict, _ = self._read(rows)
        counts = verdict["category_counts"]
        assert verdict["status"] == "active"
        assert (counts["total"], counts["bearish"], counts["bullish"], counts["withdrawn"]) == (1, 1, 0, 1)
        assert "risk-off (vix_spot)" in verdict["reasoning"]

    def test_freshness_is_measured_on_effective_known_at(self) -> None:
        # A revision written an hour ago does not make a break known six days ago fresh again.
        now = datetime.now(timezone.utc)
        old_break = self._row("A", "vix_spot", "bearish", 1.0, now - timedelta(hours=1),
                              effective=now - timedelta(days=6))
        verdict, _ = self._read([old_break])
        assert verdict["status"] == "no_data" and "1 older than the window" in verdict["reasoning"]
        fresh = self._row("B", "sp500_full", "bullish", 1.0, now - timedelta(hours=1),
                          effective=now - timedelta(days=4))
        verdict, _ = self._read([old_break, fresh])
        counts = verdict["category_counts"]
        assert (counts["total"], counts["bullish"], counts["aged_out"]) == (1, 1, 1)

    @pytest.mark.parametrize("regime", ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"])
    def test_every_auto_regime_label_has_adjustments(self, regime: str) -> None:
        from analysis import thesis_scorer
        from scripts.auto_regime import REGIME_HISTORY_LABELS

        assert regime in REGIME_HISTORY_LABELS
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (regime, 0.7, "S=0.8, dS=0.2")
        ctx = thesis_scorer._get_regime_context(engine)
        assert ctx["regime"] == regime and ctx["stress_index"] == 0.8
        assert ctx["adjustments"], f"{regime} has no model adjustments"

    def test_fragile_is_crisis_at_half_strength(self) -> None:
        from analysis import thesis_scorer

        def adjustments(regime: str) -> dict:
            engine = MagicMock()
            conn = engine.connect.return_value.__enter__.return_value
            conn.execute.return_value.fetchone.return_value = (regime, 0.7, "")
            return thesis_scorer._get_regime_context(engine)["adjustments"]

        crisis, fragile = adjustments("CRISIS"), adjustments("FRAGILE")
        assert set(fragile) == set(crisis)
        for key, value in crisis.items():
            assert fragile[key] == pytest.approx(1 + (value - 1) / 2), key
