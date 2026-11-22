"""obs_date guard on raw_series -> resolved_series (normalization/resolver.py).

A raw row is either an observation, dated the day it was observed, or an
event-dated row, dated by a scheduled event. Each row gets one verdict
against the UTC date of its pull (``obs_date_verdict``):

* an observation may not be dated after that day (tolerance 0);
* a declared local-date series (``LOCAL_DATE_SERIES``) may be one day ahead;
* an event-dated series (``EVENT_DATED_SERIES``) passes at any distance;
* an undeclared row exactly one day ahead is quarantined, never passed;
* anything further ahead is skipped.

Skipped and quarantined rows are counted per series family under
``future_obs_skipped`` and ``ambiguous_next_day``. Both resolver paths call
the one helper, ``skip_future_observation``.

The path tests drive the real ``resolve_pending`` / ``resolve_range`` workers
against the recording double from tests/test_resolver_scan_budget.py, and
the real ``resolve_selected_rows`` against the fixture batch from
tests/test_selected_source_resolution.py. No database.
"""

from __future__ import annotations

import ast
import inspect
import re
import textwrap
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import normalization.resolver as resolver_mod
from normalization.entity_map import EntityMap, NEW_MAPPINGS_V2, SEED_MAPPINGS
from normalization.resolver import (
    EVENT_DATED_SERIES,
    FUTURE_OBS_TOLERANCE_DAYS,
    LOCAL_DATE_SERIES,
    LOCAL_DATE_TOLERANCE_DAYS,
    SKIP_COUNT_KEYS,
    Resolver,
    is_event_dated_series,
    is_local_date_series,
    new_skip_counts,
    obs_date_verdict,
    skip_future_observation,
)
from tests.test_resolver_scan_budget import _Engine, _resolver
from tests.test_resolver_scan_budget import entity_map  # noqa: F401  (pytest fixture)
from tests.test_selected_source_resolution import CUTOFF, START, fixture_inputs

ROOT = Path(__file__).resolve().parents[1]
PULL = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
DAY = PULL.date()

OBSERVATION = "VIXCLS"
LOCAL = "pla_activity:aircraft_sorties"
EVENT = "EARNINGS:AAPL:estimate"


def _days(n: int) -> date:
    return DAY + timedelta(days=n)


# ---------------------------------------------------------------------------
# Tolerances and verdicts
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_tolerances_are_zero_for_observations_and_one_for_declared_local_dates():
    assert FUTURE_OBS_TOLERANCE_DAYS == 0
    assert LOCAL_DATE_TOLERANCE_DAYS == 1
    assert set(SKIP_COUNT_KEYS.values()) == {"future_obs_skipped", "ambiguous_next_day"}


@pytest.mark.unit
@pytest.mark.parametrize("offset", [-3650, -30, -1, 0])
@pytest.mark.parametrize("series_id", [OBSERVATION, LOCAL, EVENT])
def test_on_or_before_the_pull_date_passes(series_id, offset):
    assert obs_date_verdict(series_id, _days(offset), PULL) == "on_or_before_pull"


@pytest.mark.unit
def test_plus_one_day_observation_is_quarantined_not_passed():
    assert obs_date_verdict(OBSERVATION, _days(1), PULL) == "ambiguous_next_day"


@pytest.mark.unit
def test_plus_one_day_declared_local_date_passes():
    assert obs_date_verdict(LOCAL, _days(1), PULL) == "declared_local_date"
    assert obs_date_verdict("sge:usd_per_oz", _days(1), PULL) == "declared_local_date"


@pytest.mark.unit
@pytest.mark.parametrize("offset", [2, 3, 7, 300])
@pytest.mark.parametrize("series_id", [OBSERVATION, LOCAL])
def test_two_or_more_days_ahead_is_future(series_id, offset):
    assert obs_date_verdict(series_id, _days(offset), PULL) == "future"


@pytest.mark.unit
@pytest.mark.parametrize("obs", [date(3018, 4, 9), date(2100, 12, 31)])
def test_far_future_glitches_are_future(obs):
    assert obs_date_verdict("finbert.hf_news.reuters", obs, PULL) == "future"
    assert obs_date_verdict("ephemeris.lunar_phase", obs, PULL) == "future"


@pytest.mark.unit
@pytest.mark.parametrize("offset", [1, 2, 14, 300])
def test_event_dated_series_pass_at_any_distance(offset):
    assert obs_date_verdict(EVENT, _days(offset), PULL) == "event_dated"


# ---------------------------------------------------------------------------
# Pull clocks: aware timestamps compare on their UTC date, naive as UTC
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_aware_pull_west_of_utc_uses_its_utc_date():
    # 20:00 at UTC-5 on the 21st is 01:00 UTC on the 22nd.
    pull = datetime(2026, 11, 21, 20, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 22), pull) == "on_or_before_pull"
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 23), pull) == "ambiguous_next_day"


@pytest.mark.unit
def test_aware_pull_east_of_utc_uses_its_utc_date():
    # 08:00 at UTC+9 on the 22nd is 23:00 UTC on the 21st: the Tokyo date is
    # already the 22nd, the UTC date is not.
    pull = datetime(2026, 11, 22, 8, 0, tzinfo=timezone(timedelta(hours=9)))
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 21), pull) == "on_or_before_pull"
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 22), pull) == "ambiguous_next_day"
    assert obs_date_verdict(LOCAL, date(2026, 11, 22), pull) == "declared_local_date"


@pytest.mark.unit
def test_naive_pull_is_taken_as_utc():
    pull = datetime(2026, 11, 21, 23, 59)
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 21), pull) == "on_or_before_pull"
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 22), pull) == "ambiguous_next_day"
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 23), pull) == "future"


@pytest.mark.unit
def test_pull_given_as_a_date():
    pull = date(2026, 11, 21)
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 21), pull) == "on_or_before_pull"
    assert obs_date_verdict(OBSERVATION, date(2026, 11, 22), pull) == "ambiguous_next_day"


# ---------------------------------------------------------------------------
# obs_date shapes
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("same_day, next_day, plus_two", [
    (date(2026, 11, 21), date(2026, 11, 22), date(2026, 11, 23)),
    (datetime(2026, 11, 21, 23, 0), datetime(2026, 11, 22, 0, 0), datetime(2026, 11, 23, 0, 0)),
    (datetime(2026, 11, 21, 23, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 22, 0, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 23, 0, 0, tzinfo=timezone.utc)),
    ("2026-11-21", "2026-11-22", "2026-11-23"),
    ("2026-11-21T23:59:59+09:00", "2026-11-22T00:00:00+00:00", "2026-11-23T08:00:00-05:00"),
], ids=["date", "naive-datetime", "aware-datetime", "iso-str", "iso-str-with-time"])
def test_obs_date_types(same_day, next_day, plus_two):
    assert obs_date_verdict(OBSERVATION, same_day, PULL) == "on_or_before_pull"
    assert obs_date_verdict(OBSERVATION, next_day, PULL) == "ambiguous_next_day"
    assert obs_date_verdict(OBSERVATION, plus_two, PULL) == "future"


@pytest.mark.unit
@pytest.mark.parametrize("obs, pull", [
    ("not-a-date", PULL), ("", PULL), (None, PULL), (20261130, PULL),
    (date(2100, 1, 1), None), (date(2100, 1, 1), "2026-11-21"),
])
def test_unreadable_clocks_are_not_judged(obs, pull):
    assert obs_date_verdict(OBSERVATION, obs, pull) == "unjudged"
    assert skip_future_observation(OBSERVATION, obs, pull, new_skip_counts()) is False


# ---------------------------------------------------------------------------
# The two declared tables
# ---------------------------------------------------------------------------

EVENT_DATED = [
    "EARNINGS:AAPL:estimate",
    "EARNINGS:BRK-B:estimate",
    "earnings:MSFT:eps_estimate",
    "LEGISLATION:119-hearing-House-1234:hearing",
]
LOCAL_DATED = [
    "pla_activity:aircraft_sorties",
    "pla_activity:adiz_entries",
    "sge:usd_per_oz",
    "sge:premium_usd_per_oz",
]
OBSERVATIONS = [
    "EARNINGS:AAPL:actual",          # actuals exist only once reported
    "earnings:MSFT:eps_actual",
    "earnings:MSFT:surprise_pct",
    "earnings:calendar:MSFT",        # FMP stores these observed today
    "LEGISLATION:hr-1234-119:introduced",
    "earnings:MSFT:eps_estimate_v2",  # full match, not prefix match
    "XEARNINGS:AAPL:estimate",
    "Earnings:AAPL:estimate",        # series ids are case-sensitive
    "EARNINGS:AAPL:x:estimate",      # one ticker segment only
    "pla_activity:aircraft_sorties:v2",
    "taiwan_strait:aircraft_count",  # dead scraper with placeholder rows
    "japan_topix",                   # TSE session is inside one UTC day
    "pboc:omo_net_cny_bn",           # announced ~09:20 Beijing, 01:20 UTC
    "ephemeris.lunar_phase",
    "open_meteo.nyc_temp_max",
    "finbert.hf_news.reuters",
    "GOV_CONTRACT:dod:LMT:large",
    "YF:EURUSD=X:close",
]


@pytest.mark.unit
@pytest.mark.parametrize("series_id", EVENT_DATED)
def test_event_dated_families_pass_at_plus_300_days(series_id):
    skipped = new_skip_counts()
    assert is_event_dated_series(series_id) is True
    assert is_local_date_series(series_id) is False
    assert skip_future_observation(series_id, _days(300), PULL, skipped) is False
    assert skipped == new_skip_counts()


@pytest.mark.unit
@pytest.mark.parametrize("series_id", LOCAL_DATED)
def test_declared_local_dates_pass_at_plus_one_only(series_id):
    skipped = new_skip_counts()
    assert is_local_date_series(series_id) is True
    assert is_event_dated_series(series_id) is False
    assert skip_future_observation(series_id, _days(1), PULL, skipped) is False
    assert skip_future_observation(series_id, _days(2), PULL, skipped) is True
    assert sum(skipped["future_obs_skipped"].values()) == 1
    assert skipped["ambiguous_next_day"] == {}


@pytest.mark.unit
@pytest.mark.parametrize("series_id", OBSERVATIONS)
def test_observations_are_quarantined_at_plus_one_and_skipped_beyond(series_id):
    skipped = new_skip_counts()
    assert is_event_dated_series(series_id) is False
    assert is_local_date_series(series_id) is False
    assert skip_future_observation(series_id, _days(0), PULL, skipped) is False
    assert skip_future_observation(series_id, _days(1), PULL, skipped) is True
    assert skip_future_observation(series_id, _days(300), PULL, skipped) is True
    assert sum(skipped["ambiguous_next_day"].values()) == 1
    assert sum(skipped["future_obs_skipped"].values()) == 1


@pytest.mark.unit
def test_skips_and_quarantines_are_counted_per_series_family():
    skipped = new_skip_counts()
    for sid, obs in [
        ("ephemeris.lunar_phase", date(2100, 12, 31)),
        ("ephemeris.mars.longitude", date(2100, 12, 30)),
        ("open_meteo.nyc_temp_max", _days(5)),
        ("open_meteo.nyc_temp_max", _days(1)),       # quarantined
        ("finbert.hf_news.reuters", date(3018, 4, 9)),
        ("VIXCLS", _days(2)),
        ("VIXCLS", _days(1)),                        # quarantined
        ("VIXCLS", _days(0)),                        # passes
        ("pla_activity:plan_ships", _days(1)),       # declared: passes
        ("EARNINGS:AAPL:estimate", _days(90)),       # event-dated: passes
    ]:
        skip_future_observation(sid, obs, PULL, skipped)
    assert skipped == {
        "future_obs_skipped": {"ephemeris": 2, "open_meteo": 1, "finbert": 1, "bare": 1},
        "ambiguous_next_day": {"open_meteo": 1, "bare": 1},
    }


@pytest.mark.unit
@pytest.mark.parametrize("table, width", [(LOCAL_DATE_SERIES, 5), (EVENT_DATED_SERIES, 4)],
                         ids=["local-date", "event-dated"])
def test_every_declared_entry_names_its_writer_and_reason(table, width):
    names = [entry[0] for entry in table]
    assert len(names) == len(set(names))
    for entry in table:
        assert len(entry) == width
        name, pattern, writer, *rest = entry
        why = rest[-1]
        re.compile(pattern)
        assert len(why) > 40, f"{name}: say why"
        source = (ROOT / writer).read_text()
        prefix = pattern.split("[", 1)[0]          # e.g. "EARNINGS:"
        assert prefix in source, f"{name}: {writer} does not write {prefix!r}"
        if width == 5:
            assert rest[0].startswith(("Asia/", "Australia/", "Pacific/")), name
        else:
            suffix = pattern.rsplit(":", 1)[1]     # e.g. "estimate"
            assert suffix in source, f"{name}: {writer} does not write {suffix!r}"


@pytest.mark.unit
def test_local_date_writers_take_obs_date_from_the_local_date():
    """Pin the writer lines the LOCAL_DATE_SERIES reasons cite."""
    pla = (ROOT / "ingestion/altdata/rocaf_pla_activity.py").read_text()
    assert "counts as of 06:00 UTC+8 that day" in pla
    assert "self._insert_raw(conn, sid, rep.report_date" in pla
    sge = (ROOT / "ingestion/altdata/sge_premium.py").read_text()
    assert "obs_date = _extract_date(row, date_col)" in sge
    assert "anchor_date: date = sge_date or london_date or fx_date" in sge


@pytest.mark.unit
def test_no_event_dated_family_is_mapped_to_a_feature():
    """Passing the guard is not model eligibility.

    The EVENT_DATED_SERIES note says none of these reaches resolved_series
    today. If one is ever mapped, decide its eligibility on purpose: its
    obs_date is an event date, not an observation date.
    """
    mapped = [sid for sid in {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
              if is_event_dated_series(sid)]
    assert mapped == []


# ---------------------------------------------------------------------------
# Path 1: resolve_pending workers (rolling cycle, watermark, filters, backfill)
# ---------------------------------------------------------------------------

T0 = datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc)

_ROWS = [
    ("VIXCLS", _days(-1), 17.0, 3, PULL, 1, "FRED"),
    ("VIXCLS", _days(0), 17.5, 3, PULL, 1, "FRED"),                    # boundary: passes
    ("VIXCLS", _days(1), 18.0, 3, PULL, 1, "FRED"),                    # quarantined
    ("pla_activity:aircraft_sorties", _days(1), 12.0, 7, PULL, 1, "rocaf"),  # declared
    ("pla_activity:aircraft_sorties", _days(2), 13.0, 7, PULL, 1, "rocaf"),  # future
    ("ephemeris.lunar_phase", _days(2), 0.4, 4, PULL, 1, "astro"),
    ("ephemeris.lunar_phase", date(2100, 12, 31), 0.5, 4, PULL, 1, "astro"),
    ("finbert.hf_news.reuters", date(3018, 4, 9), 0.1, 5, PULL, 1, "finbert"),
    ("EARNINGS:AAPL:estimate", _days(300), 2.1, 6, PULL, 1, "yfinance_earnings"),
]
_KEPT = {
    ("VIXCLS", _days(-1)), ("VIXCLS", _days(0)),
    ("pla_activity:aircraft_sorties", _days(1)),
    ("EARNINGS:AAPL:estimate", _days(300)),
}
_EXPECTED_SKIPS = {"ephemeris": 2, "finbert": 1, "pla_activity": 1}
_EXPECTED_QUARANTINE = {"bare": 1}


def _inserted(engine: _Engine) -> set[tuple[float, date]]:
    return {(p["val"], p["od"]) for _sql, params in engine.calls
            if isinstance(params, list) for p in params}


def _expected_inserts() -> set[tuple[float, date]]:
    return {(r[2], r[1]) for r in _ROWS if (r[0], r[1]) in _KEPT}


class _Warnings:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def __call__(self, message) -> None:
        self.messages.append(message.record["message"])


@pytest.fixture
def warn_sink():
    sink = _Warnings()
    handler = resolver_mod.log.add(sink, level="WARNING")
    yield sink
    resolver_mod.log.remove(handler)


def _guard_warnings(sink: _Warnings) -> list[str]:
    return [m for m in sink.messages if "future-dated raw row" in m]


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
@pytest.mark.parametrize("kwargs", [
    {},                                                    # full lookback window
    {"since": T0, "until": T0 + timedelta(days=1)},        # watermark / incremental
    {"series_ids": [r[0] for r in _ROWS]},                 # operator series filter
], ids=["lookback", "watermark", "series-filter"])
def test_resolve_pending_skips_quarantines_and_counts(kwargs, warn_sink):
    engine = _Engine(raw_rows=list(_ROWS))

    summary = _resolver(engine).resolve_pending(workers=2, **kwargs)

    assert _inserted(engine) == _expected_inserts()
    assert summary["future_obs_skipped"] == _EXPECTED_SKIPS
    assert list(summary["future_obs_skipped"]) == ["ephemeris", "finbert", "pla_activity"], (
        "largest first, then by name"
    )
    assert summary["ambiguous_next_day"] == _EXPECTED_QUARANTINE
    warnings = _guard_warnings(warn_sink)
    assert len(warnings) == 1, "one warning per run, not per worker"
    assert "skipped 4 future-dated" in warnings[0]
    assert "quarantined 1 undeclared next-day" in warnings[0]


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_pending_dry_run_counts_the_same_skips():
    engine = _Engine(raw_rows=list(_ROWS))

    summary = _resolver(engine).resolve_pending(workers=1, dry_run=True)

    assert summary["resolved"] == len(_KEPT)
    assert summary["future_obs_skipped"] == _EXPECTED_SKIPS
    assert summary["ambiguous_next_day"] == _EXPECTED_QUARANTINE


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_pending_with_nothing_ahead_reports_empty_counts(warn_sink):
    engine = _Engine(raw_rows=[r for r in _ROWS if (r[0], r[1]) in _KEPT])

    summary = _resolver(engine).resolve_pending(workers=1, dry_run=True)

    assert summary["future_obs_skipped"] == {}
    assert summary["ambiguous_next_day"] == {}
    assert not _guard_warnings(warn_sink)


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_pending_early_return_carries_both_keys():
    engine = _Engine(raw_rows=[])

    summary = _resolver(engine).resolve_pending(workers=1)

    assert summary["future_obs_skipped"] == {}
    assert summary["ambiguous_next_day"] == {}


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_range_sums_skips_over_chunks():
    engine = _Engine(raw_rows=list(_ROWS))

    totals = _resolver(engine).resolve_range(
        since=T0, until=T0 + timedelta(days=2), chunk_days=1, workers=1, dry_run=True,
    )

    assert totals["chunks"] == 2
    # The double serves every row to every chunk, so each chunk skips them.
    assert totals["future_obs_skipped"] == {"ephemeris": 4, "finbert": 2, "pla_activity": 2}
    assert totals["ambiguous_next_day"] == {"bare": 2}


# ---------------------------------------------------------------------------
# Path 2: resolve_selected_rows (finite supplied batch, PIT replay)
# ---------------------------------------------------------------------------

SELECTED_PULL = datetime(2026, 10, 10, 8, 0, tzinfo=timezone.utc)


def _selected(mutate=None) -> dict:
    selections, raw, registry, catalog = fixture_inputs()
    if mutate:
        mutate(raw, catalog)
    return Resolver(None).resolve_selected_rows(
        selections, raw, registry, catalog, as_of=CUTOFF, start_date=START)


def _fx(raw: list[dict], **change) -> None:
    raw[0].update(change)   # FX:EUR:close, the fx_rates selection


@pytest.mark.unit
def test_selected_rows_skip_and_count_future_rows(warn_sink):
    out = _selected(lambda raw, _c: _fx(raw, pull_timestamp=SELECTED_PULL,
                                       obs_date=date(2026, 10, 12)))

    assert out["selections"][0]["reason"] == "no_resolved_row"
    assert 8101 not in [r["feature_id"] for r in out["resolved_rows"]]
    assert out["future_obs_skipped"] == {"FX": 1}
    assert out["ambiguous_next_day"] == {}
    assert any(m.startswith("Selected resolution: skipped 1 future-dated")
               for m in _guard_warnings(warn_sink))


@pytest.mark.unit
def test_selected_rows_quarantine_an_undeclared_next_day_row():
    out = _selected(lambda raw, _c: _fx(raw, pull_timestamp=SELECTED_PULL,
                                       obs_date=date(2026, 10, 11)))

    assert out["selections"][0]["reason"] == "no_resolved_row"
    assert out["future_obs_skipped"] == {}
    assert out["ambiguous_next_day"] == {"FX": 1}


@pytest.mark.unit
def test_selected_rows_keep_the_same_day():
    out = _selected(lambda raw, _c: _fx(raw, pull_timestamp=SELECTED_PULL,
                                       obs_date=date(2026, 10, 10)))

    assert out["selections"][0]["reason"] == "resolved_observation"
    assert out["future_obs_skipped"] == {}
    assert out["ambiguous_next_day"] == {}


@pytest.mark.unit
def test_selected_rows_report_empty_counts_by_default():
    out = _selected()
    assert out["future_obs_skipped"] == {}
    assert out["ambiguous_next_day"] == {}
    assert len(out["resolved_rows"]) == 6


@pytest.mark.unit
def test_selected_replay_cutoff_still_applies_first():
    """obs_date past the replay cutoff is the replay window's exclusion.

    Pulled 10-14, observed 10-16: two days past its pull, but also past the
    10-15 cutoff. The replay window drops it before the obs_date guard sees
    it, so it is not counted as a future skip.
    """
    out = _selected(lambda raw, _c: _fx(raw, obs_date=date(2026, 10, 16)))

    assert out["selections"][0]["reason"] == "no_resolved_row"
    assert out["future_obs_skipped"] == {}
    assert out["ambiguous_next_day"] == {}


@pytest.mark.unit
def test_selected_guard_never_hides_a_source_mismatch():
    def change(raw, catalog):
        _fx(raw, pull_timestamp=SELECTED_PULL, obs_date=date(2026, 10, 14))
        catalog[0].update(name="different_source")

    out = _selected(change)

    assert out["selections"][0]["reason"] == "source_mismatch"
    assert out["future_obs_skipped"] == {}


@pytest.mark.unit
def test_selected_guard_never_hides_an_invalid_value():
    out = _selected(lambda raw, _c: _fx(raw, pull_timestamp=SELECTED_PULL,
                                       obs_date=date(2026, 10, 14), value=float("nan")))

    assert out["selections"][0]["reason"] == "invalid_value"
    assert out["future_obs_skipped"] == {}


# ---------------------------------------------------------------------------
# Both paths: one helper, identical decisions
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
@pytest.mark.parametrize("offset", [-1, 0, 1, 2, 3, 5])
def test_both_paths_make_the_same_decision(offset, monkeypatch):
    obs = SELECTED_PULL.date() + timedelta(days=offset)

    # The selected path binds features itself; only the bulk path needs the stub.
    stub = resolver_mod.EntityMap
    monkeypatch.setattr(resolver_mod, "EntityMap", EntityMap)
    selected = _selected(lambda raw, _c: _fx(raw, pull_timestamp=SELECTED_PULL, obs_date=obs))
    selected_kept = 8101 in [r["feature_id"] for r in selected["resolved_rows"]]
    monkeypatch.setattr(resolver_mod, "EntityMap", stub)

    engine = _Engine(raw_rows=[("FX:EUR:close", obs, 1.1, 9101, SELECTED_PULL, 1, "yfinance")])
    pending = _resolver(engine).resolve_pending(workers=1, dry_run=True)
    pending_kept = pending["resolved"] == 1

    assert selected_kept is pending_kept is (offset <= FUTURE_OBS_TOLERANCE_DAYS)
    for key in SKIP_COUNT_KEYS.values():
        assert selected[key] == pending[key]


@pytest.mark.unit
@pytest.mark.parametrize("method", ["resolve_pending", "resolve_selected_rows"])
def test_every_grouping_loop_uses_the_shared_helper(method):
    tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(Resolver, method))))
    called = [node.func.id for node in ast.walk(tree)
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)]
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert called.count("skip_future_observation") == 1
    assert "obs_date_verdict" not in called
    for constant in ("FUTURE_OBS_TOLERANCE_DAYS", "LOCAL_DATE_TOLERANCE_DAYS"):
        assert constant not in names, "the tolerances live in the helper only"
