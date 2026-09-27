"""Pure tests for godview/cftc_positioning.py (G4) -- no database, no network.

The DB-gated behaviour (upsert, legacy-row protection, ledger, PIT gate on a
real schema) lives in test_cftc_positioning_writer_pg.py.
"""

from __future__ import annotations

import inspect
import re
import statistics
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import godview.cftc_positioning as cp
from ingestion.altdata.cftc_markets import MARKETS, compute_release, series_id
from intelligence.cot_extremes import classify_extreme

_ET_PUBLISHED_2026 = {
    1: [5, 9, 16, 23, 30],
    2: [6, 13, 20, 27],
    3: [6, 13, 20, 27],
    4: [3, 10, 17, 24],
    5: [1, 8, 15, 22, 29],
    6: [5, 12, 22, 26],
    7: [6, 10, 17, 24, 31],
    8: [7, 14, 21, 28],
    9: [4, 11, 18, 25],
    10: [2, 9, 16, 23, 30],
    11: [6, 16, 20, 30],
    12: [4, 11, 18, 28],
}

PULL = datetime(2026, 9, 27, 3, 0, tzinfo=timezone.utc)


def _leg(code: str, metric: str, value: float, pull: datetime | None = PULL) -> cp.LegObservation:
    return cp.LegObservation(series_id(code, metric), value, pull, cp.SOURCE_NAME)


def _week(
    code: str,
    d: date,
    *,
    cl: float = 100,
    cs: float = 200,
    ncl: float = 300,
    ncs: float = 50,
    oi: float = 1000,
    net: float | None = None,
    pull: datetime | None = PULL,
) -> dict[str, cp.LegObservation]:
    legs = {
        "commercial_long": _leg(code, "commercial_long", cl, pull),
        "commercial_short": _leg(code, "commercial_short", cs, pull),
        "noncommercial_long": _leg(code, "noncommercial_long", ncl, pull),
        "noncommercial_short": _leg(code, "noncommercial_short", ncs, pull),
        "total_open_interest": _leg(code, "total_open_interest", oi, pull),
    }
    legs["net_speculative"] = _leg(code, "net_speculative", ncl - ncs if net is None else net, pull)
    return legs


def _by_metric(weeks: dict[date, dict[str, cp.LegObservation]]) -> dict[str, dict[date, cp.LegObservation]]:
    out: dict[str, dict[date, cp.LegObservation]] = {m: {} for m in cp.ALL_METRICS}
    for d, legs in weeks.items():
        for m, leg in legs.items():
            out[m][d] = leg
    return out


ES = "13874A"
T0 = date(2023, 1, 3)  # a Tuesday


# ── G1 release rule vs the CFTC's published 2026 schedule (Good Friday) ──


def test_g1_release_rule_matches_every_2026_published_release_including_good_friday():
    """cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule, read 2026-09-27.

    Good Friday 2026 is April 3: a regular release (no holiday asterisk), so
    G1's federal-holiday rule must NOT shift it -- and it does not.
    """
    published = sorted(date(2026, m, d) for m, ds in _ET_PUBLISHED_2026.items() for d in ds)
    tuesday = date(2025, 12, 30)
    got = []
    for _ in range(52):
        r = compute_release(tuesday)
        got.append(r.release_at.astimezone(ZoneInfo("America/New_York")).date())
        tuesday += timedelta(days=7)
    assert got == published
    good_friday = compute_release(date(2026, 3, 31))
    assert good_friday.holiday_shifted is False
    assert good_friday.release_at == datetime(2026, 4, 3, 19, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize("monday", [date(2018, 12, 24), date(2018, 12, 31), date(2020, 12, 21), date(2008, 12, 22)])
def test_known_monday_report_dates_have_no_computable_release(monday):
    """The CFTC published these Monday-dated reports on one-off dates; no rule gives a release."""
    assert monday.weekday() == 0
    assert compute_release(monday).release_at is None


# ── assemble_reports: fail closed ─────────────────────────────────────────


def test_complete_week_assembles_with_integer_legs_and_derived_nets():
    reports, skips = cp.assemble_reports(_by_metric({T0: _week(ES, T0)}))
    assert skips == []
    (r,) = reports
    assert (r.commercial_net, r.noncommercial_net) == (-100, 250)
    assert r.spec_net_pct_oi == pytest.approx(25.0)
    assert r.max_pull_timestamp == PULL
    assert [m for m, _ in r.legs] == list(cp.ALL_METRICS)


@pytest.mark.parametrize("missing", cp.RAW_LEGS)
def test_missing_any_raw_leg_skips_the_week(missing):
    week = _week(ES, T0)
    del week[missing]
    reports, skips = cp.assemble_reports(_by_metric({T0: week}))
    assert reports == []
    assert skips == [cp.RowSkip(T0, cp.SKIP_MISSING_LEG, missing)]


def test_missing_stored_net_speculative_is_not_required():
    week = _week(ES, T0)
    del week["net_speculative"]
    reports, skips = cp.assemble_reports(_by_metric({T0: week}))
    assert skips == [] and reports[0].noncommercial_net == 250


def test_net_speculative_disagreeing_with_legs_is_ambiguous_and_skipped():
    reports, skips = cp.assemble_reports(_by_metric({T0: _week(ES, T0, net=999)}))
    assert reports == [] and skips[0].reason == cp.SKIP_NET_SPECULATIVE_MISMATCH


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"cl": -1}, cp.SKIP_INVALID_LEG),
        ({"cl": 10.5}, cp.SKIP_INVALID_LEG),
        ({"ncs": float("nan")}, cp.SKIP_INVALID_LEG),
        ({"oi": 0}, cp.SKIP_NON_POSITIVE_OPEN_INTEREST),
        ({"ncl": 1001}, cp.SKIP_LEG_EXCEEDS_OPEN_INTEREST),
    ],
)
def test_invalid_legs_are_skipped_never_rounded_or_zero_filled(kwargs, reason):
    reports, skips = cp.assemble_reports(_by_metric({T0: _week(ES, T0, **kwargs)}))
    assert reports == [] and skips[0].reason == reason


def test_missing_pull_timestamp_skips():
    reports, skips = cp.assemble_reports(_by_metric({T0: _week(ES, T0, pull=None)}))
    assert reports == [] and skips[0].reason == cp.SKIP_MISSING_PULL_TIMESTAMP


def test_max_pull_is_the_latest_leg():
    week = _week(ES, T0)
    late = PULL + timedelta(hours=5)
    week["total_open_interest"] = _leg(ES, "total_open_interest", 1000, late)
    reports, _ = cp.assemble_reports(_by_metric({T0: week}))
    assert reports[0].max_pull_timestamp == late


# ── windows and statistics ────────────────────────────────────────────────


def _series(nets: list[int], start: date = T0, pulls: list[datetime] | None = None) -> list[cp.WeeklyReport]:
    weeks = {}
    for i, n in enumerate(nets):
        d = start + timedelta(days=7 * i)
        pull = pulls[i] if pulls else PULL
        weeks[d] = _week(ES, d, ncl=500 + n, ncs=500, oi=100_000, pull=pull)
    reports, skips = cp.assemble_reports(_by_metric(weeks))
    assert skips == []
    return reports


def test_window_never_contains_a_later_report():
    reports = _series(list(range(60)))
    for idx in (0, 10, 59):
        members = cp.window_members(reports, idx, cp.Z_1Y_WEEKS)
        assert max(m.report_date for m in members) == reports[idx].report_date


def test_window_is_calendar_bounded_not_last_n_observations():
    """A gap must not stretch the '1y' window back past 52 weeks."""
    nets = list(range(60))
    reports = _series(nets)
    gapped = reports[:5] + reports[40:]  # 35-week hole
    idx = len(gapped) - 1
    members = cp.window_members(gapped, idx, cp.Z_1Y_WEEKS)
    floor = gapped[idx].report_date - timedelta(days=7 * 52)
    assert all(m.report_date > floor for m in members)
    assert len(members) == 20


def test_zscore_and_percentile_match_cot_extremes_on_a_full_window():
    nets = [((i * 37) % 101) - 50 for i in range(160)]
    reports = _series(nets)
    m = cp.compute_row_metrics(reports, len(reports) - 1)
    ref = classify_extreme(contract="ES", metric="net_speculative", history=[float(v) for v in nets])
    assert m.z_score_1y == pytest.approx(ref.z_score)
    assert m.percentile_3y == pytest.approx(ref.percentile_rank)
    w3 = [float(v) for v in nets[-156:]]
    assert m.z_score_3y == pytest.approx((w3[-1] - statistics.mean(w3)) / statistics.stdev(w3))
    assert m.n_obs_1y == 52 and m.n_obs_3y == 156 and m.coverage_fraction == 1.0


def test_short_history_gives_null_statistics_and_null_regime():
    reports = _series(list(range(cp.MIN_OBS_1Y - 1)))
    m = cp.compute_row_metrics(reports, len(reports) - 1)
    assert (m.z_score_1y, m.z_score_3y, m.percentile_3y, m.crowding_regime) == (None, None, None, None)


def test_1y_available_before_3y():
    reports = _series(list(range(cp.MIN_OBS_1Y + 5)))
    m = cp.compute_row_metrics(reports, len(reports) - 1)
    assert m.z_score_1y is not None
    assert m.z_score_3y is None and m.percentile_3y is None and m.crowding_regime is None


def test_flat_window_gives_null_z_not_zero():
    reports = _series([7] * 160)
    m = cp.compute_row_metrics(reports, len(reports) - 1)
    assert m.z_score_1y is None and m.z_score_3y is None
    assert m.percentile_3y == pytest.approx(50.0)


def test_window_max_pull_covers_history_not_only_the_row():
    pulls = [PULL] * 60
    pulls[10] = PULL + timedelta(days=2)  # a history week re-pulled later
    reports = _series(list(range(60)), pulls=pulls)
    m = cp.compute_row_metrics(reports, 59)
    assert m.window_max_pull_timestamp == PULL + timedelta(days=2)


@pytest.mark.parametrize(
    "pct,regime",
    [
        (None, None),
        (99.0, cp.REGIME_EXTREME_LONG),
        (95.0, cp.REGIME_EXTREME_LONG),
        (90.0, cp.REGIME_ELEVATED_LONG),
        (50.0, cp.REGIME_NEUTRAL),
        (10.0, cp.REGIME_ELEVATED_SHORT),
        (5.0, cp.REGIME_EXTREME_SHORT),
    ],
)
def test_crowding_regime_thresholds(pct, regime):
    assert cp.classify_crowding(pct) == regime


def test_every_tracked_market_has_an_asset_class():
    assert {m.root for m in MARKETS.values()} == set(cp.ASSET_CLASS_BY_ROOT)


def test_source_ref_is_deterministic_and_names_every_input():
    reports = _series(list(range(60)))
    m = cp.compute_row_metrics(reports, 59)
    a = cp.build_source_ref(MARKETS[ES], reports[59], m, False)
    b = cp.build_source_ref(MARKETS[ES], reports[59], m, False)
    assert a == b
    assert {i["series_id"] for i in a["inputs"]} == {series_id(ES, x) for x in cp.ALL_METRICS}
    assert a["market_code"] == ES and a["window"]["n_obs_1y"] == 52


def test_module_has_no_zero_fill_or_default_regime():
    # the incident materializer's `.get("commercial_long", 0.0)` pattern
    code = "".join(
        inspect.getsource(f) for f in (cp.assemble_reports, cp.compute_row_metrics, cp.materialize_cftc_positioning)
    )
    assert not re.search(r"\.get\([^)]*,\s*-?\d", code)
    assert cp.classify_crowding(None) is None


def test_reads_only_code_keyed_ids():
    src = inspect.getsource(cp)
    assert "series_id(code, metric)" in src
    assert 'f"cftc.' not in src and "f'cftc." not in src
