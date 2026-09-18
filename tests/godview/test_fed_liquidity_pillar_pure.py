"""Pure-Python tests for godview/fed_liquidity_pillar.py — no database, no network."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

import godview.fed_liquidity_pillar as fed_liquidity_pillar
from godview.fed_liquidity_pillar import (
    RELEASE_RULE_ID,
    RRP_BILLIONS_TO_MILLIONS,
    classify_liquidity_regime,
    compute_net_liquidity_millions,
    compute_release_date,
    compute_rrp_pct_of_peak,
    coverage_fraction_for_window,
    find_nearest_prior,
    materialize_fed_liquidity_pillar,
)


def test_compute_release_date_sets_thursday_for_a_wednesday_obs():
    wednesday = date(2026, 9, 16)
    assert wednesday.weekday() == 2
    release_date, source_ref = compute_release_date(wednesday)
    assert release_date == date(2026, 9, 17)  # the following Thursday
    assert release_date.weekday() == 3
    assert RELEASE_RULE_ID in source_ref
    assert "Wednesday" in source_ref


def test_compute_release_date_withholds_for_a_non_wednesday_obs():
    thursday = date(2026, 9, 17)
    release_date, source_ref = compute_release_date(thursday)
    assert release_date is None
    assert "not Wednesday" in source_ref
    assert "withheld" in source_ref


def test_compute_net_liquidity_converts_rrp_billions_to_millions():
    """WALCL/WTREGEN are millions; RRPONTSYD is billions — confirmed via FRED (module docstring)."""
    assert RRP_BILLIONS_TO_MILLIONS == 1000.0
    # 7,500,000M WALCL - 700,000M WTREGEN - 300B RRP (=300,000M) = 6,500,000M
    result = compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    assert result == pytest.approx(6_500_000.0)


def test_compute_net_liquidity_without_conversion_would_be_wrong():
    """Sanity check: skipping the *1000 scale materially changes the result
    (this is exactly the bug flagged in ingestion/altdata/fed_liquidity.py)."""
    converted = compute_net_liquidity_millions(7_500_000.0, 700_000.0, 300.0)
    unconverted = 7_500_000.0 - 700_000.0 - 300.0  # what the buggy formula would give
    assert abs(converted - unconverted) == pytest.approx(300.0 * (RRP_BILLIONS_TO_MILLIONS - 1))


def test_compute_rrp_pct_of_peak_is_100_at_a_new_high():
    history = [100.0, 200.0, 150.0, 300.0]  # last value is the peak
    pct = compute_rrp_pct_of_peak(history, window=156)
    assert pct == pytest.approx(100.0)


def test_compute_rrp_pct_of_peak_none_below_min_history():
    assert compute_rrp_pct_of_peak([100.0, 200.0], window=156) is None


def test_coverage_fraction_for_window_caps_at_one():
    assert coverage_fraction_for_window(10, window=156) == pytest.approx(10 / 156)
    assert coverage_fraction_for_window(500, window=156) == 1.0


def test_find_nearest_prior_within_tolerance():
    history = [(date(2026, 9, 2), 100.0), (date(2026, 9, 9), 110.0)]
    # target 5 days before 2026-09-16 (2026-09-11); 2026-09-09 is 7 days
    # before, 2 days off target — within tolerance (3).
    value = find_nearest_prior(history, date(2026, 9, 16), target_days=5, tolerance_days=3)
    assert value == 110.0


def test_find_nearest_prior_none_when_nothing_within_tolerance():
    history = [(date(2026, 1, 1), 100.0)]
    value = find_nearest_prior(history, date(2026, 9, 16), target_days=5, tolerance_days=3)
    assert value is None


@pytest.mark.parametrize(
    "delta_30d,expected",
    [
        (None, "insufficient_history"),
        (60_000.0, "expanding"),
        (-60_000.0, "contracting"),
        (0.0, "neutral"),
    ],
)
def test_classify_liquidity_regime(delta_30d, expected):
    assert classify_liquidity_regime(delta_30d) == expected


# ---------------------------------------------------------------------------
# Pure fake: reproduces the SUCCESS_NOOP report from RESULTS.md, and proves
# the unit-normalisation fix (a828f4bf) is NOT the cause.
#
# Root cause (2026-09-18, real-Postgres run, composition 42df4362):
# fed_net_liquidity_daily is keyed by obs_date alone -- unlike
# cftc_positioning_daily (random contract_code) and
# commodity_warehouse_inventories (random metal), the DB-gated Fed tests
# used FIXED obs_dates. Composition 783ff735's first run of those tests
# passed and wrote real rows for those exact dates into the shared,
# persistent scratch DB; the SAME test file re-run at composition 42df4362
# against the SAME DB found those obs_dates already present and correctly
# reported SUCCESS_NOOP (idempotent no-op, not a bug -- see the module
# docstring). It was misattributed to a828f4bf only because of when it was
# first observed. tests/godview/test_fed_liquidity_pillar_db.py now uses a
# random Wednesday per test to make this collision astronomically
# unlikely; these two tests lock the underlying behaviour in with a fake
# so nobody re-diagnoses it as a materializer bug again.
# ---------------------------------------------------------------------------


class _FakeResult:
    def mappings(self):
        return self

    def all(self):
        return []

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def scalar(self):
        return 1


class _FakeConn:
    def execute(self, *args, **kwargs):
        return _FakeResult()


class _FakeEngineCtx:
    def __enter__(self):
        return _FakeConn()

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def begin(self):
        return _FakeEngineCtx()


def _patched_materialize(monkeypatch, obs_date, *, existing_obs_dates):
    """Run materialize_fed_liquidity_pillar against fully faked DB helpers.

    All three raw components are present and well-formed for ``obs_date``
    (real FRED series ids, real millions/billions values) -- the only
    variable is whether ``obs_date`` is already in
    ``fed_net_liquidity_daily`` (``existing_obs_dates``).
    """
    on_schedule = datetime.combine(obs_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1, hours=20)
    histories = {
        "WALCL": {obs_date: {"value": 7_500_000.0, "pull_timestamp": on_schedule}},
        "WTREGEN": {obs_date: {"value": 700_000.0, "pull_timestamp": on_schedule}},
        "RRPONTSYD": {obs_date: {"value": 300.0, "pull_timestamp": on_schedule}},
    }

    monkeypatch.setattr(
        fed_liquidity_pillar,
        "_read_component_history",
        lambda conn, series_id, as_of: histories[series_id],
    )
    monkeypatch.setattr(fed_liquidity_pillar, "_existing_obs_dates", lambda conn: set(existing_obs_dates))
    monkeypatch.setattr(fed_liquidity_pillar, "_existing_net_liquidity_history", lambda conn, before: ([], []))
    monkeypatch.setattr(fed_liquidity_pillar, "_distinct_pull_count", lambda conn, series_id, obs_date: 1)
    monkeypatch.setattr(fed_liquidity_pillar, "record_generation", lambda *a, **k: None)

    return materialize_fed_liquidity_pillar(_FakeEngine(), as_of=obs_date)


def test_materialize_succeeds_on_a_genuinely_fresh_obs_date(monkeypatch):
    """Confirms the unit-normalisation path itself is fine: with a real FRED
    id/unit and NO pre-existing row, the materializer writes the row."""
    obs_date = date(2026, 9, 16)  # a Wednesday
    result = _patched_materialize(monkeypatch, obs_date, existing_obs_dates=set())
    assert result.status == "SUCCESS"
    assert result.rows_written == 1


def test_materialize_is_success_noop_when_the_obs_date_already_exists(monkeypatch):
    """Reproduces the exact SUCCESS_NOOP from RESULTS.md with a fake that
    mirrors a persistent scratch DB already holding a prior run's row --
    correct idempotent behaviour, not a regression from a828f4bf."""
    obs_date = date(2026, 9, 16)
    result = _patched_materialize(monkeypatch, obs_date, existing_obs_dates={obs_date})
    assert result.status == "SUCCESS_NOOP"
    assert result.rows_written == 0
