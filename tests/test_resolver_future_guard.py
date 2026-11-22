"""Future-date guard on raw_series -> resolved_series (normalization/resolver.py).

A raw row whose obs_date runs days or centuries past its own pull is not an
observation: FinBERT scores dated 3018, Open-Meteo forecast days stored as
daily weather, ephemeris rows computed to 2100. Every resolve path drops
those rows through one helper, ``skip_future_observation``, with one
tolerance, ``FUTURE_OBS_TOLERANCE_DAYS``, and counts them per series family.
Declared future-by-design families (scheduled event dates) pass.

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
    FUTURE_BY_DESIGN_SERIES,
    FUTURE_OBS_TOLERANCE_DAYS,
    Resolver,
    is_future_by_design,
    is_future_observation,
    skip_future_observation,
)
from tests.test_resolver_scan_budget import _Engine, _resolver
from tests.test_resolver_scan_budget import entity_map  # noqa: F401  (pytest fixture)
from tests.test_selected_source_resolution import CUTOFF, START, fixture_inputs

ROOT = Path(__file__).resolve().parents[1]
PULL = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
DAY = PULL.date()


def _days(n: int) -> date:
    return DAY + timedelta(days=n)


# ---------------------------------------------------------------------------
# The tolerance
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_tolerance_is_one_named_day():
    assert FUTURE_OBS_TOLERANCE_DAYS == 1
    default = inspect.signature(is_future_observation).parameters["tolerance_days"].default
    assert default == FUTURE_OBS_TOLERANCE_DAYS


@pytest.mark.unit
@pytest.mark.parametrize("offset", [-3650, -30, -1, 0, 1])
def test_past_today_and_plus_one_day_pass(offset):
    assert is_future_observation(_days(offset), PULL) is False


@pytest.mark.unit
@pytest.mark.parametrize("offset", [2, 3, 7, 300])
def test_two_or_more_days_ahead_are_future(offset):
    assert is_future_observation(_days(offset), PULL) is True


@pytest.mark.unit
@pytest.mark.parametrize("obs", [date(3018, 4, 9), date(2100, 12, 31)])
def test_far_future_glitches_are_future(obs):
    assert is_future_observation(obs, PULL) is True


@pytest.mark.unit
def test_tolerance_argument_moves_the_boundary():
    assert is_future_observation(_days(2), PULL, tolerance_days=2) is False
    assert is_future_observation(_days(3), PULL, tolerance_days=2) is True


# ---------------------------------------------------------------------------
# Pull clocks: aware timestamps compare on their UTC date, naive as UTC
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_aware_pull_west_of_utc_uses_its_utc_date():
    # 20:00 at UTC-5 on the 21st is 01:00 UTC on the 22nd.
    pull = datetime(2026, 11, 21, 20, 0, tzinfo=timezone(timedelta(hours=-5)))
    assert is_future_observation(date(2026, 11, 23), pull) is False
    assert is_future_observation(date(2026, 11, 24), pull) is True


@pytest.mark.unit
def test_aware_pull_east_of_utc_uses_its_utc_date():
    # 08:00 at UTC+9 on the 22nd is 23:00 UTC on the 21st.
    pull = datetime(2026, 11, 22, 8, 0, tzinfo=timezone(timedelta(hours=9)))
    assert is_future_observation(date(2026, 11, 22), pull) is False
    assert is_future_observation(date(2026, 11, 23), pull) is True


@pytest.mark.unit
def test_naive_pull_is_taken_as_utc():
    pull = datetime(2026, 11, 21, 23, 59)
    assert is_future_observation(date(2026, 11, 22), pull) is False
    assert is_future_observation(date(2026, 11, 23), pull) is True


@pytest.mark.unit
def test_pull_given_as_a_date():
    assert is_future_observation(date(2026, 11, 22), date(2026, 11, 21)) is False
    assert is_future_observation(date(2026, 11, 23), date(2026, 11, 21)) is True


# ---------------------------------------------------------------------------
# obs_date shapes
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.parametrize("plus_one, plus_two", [
    (date(2026, 11, 22), date(2026, 11, 23)),
    (datetime(2026, 11, 22, 0, 0), datetime(2026, 11, 23, 0, 0)),
    (datetime(2026, 11, 22, 23, 0, tzinfo=timezone.utc),
     datetime(2026, 11, 23, 0, 0, tzinfo=timezone.utc)),
    ("2026-11-22", "2026-11-23"),
    ("2026-11-22T23:59:59+09:00", "2026-11-23T00:00:00+00:00"),
], ids=["date", "naive-datetime", "aware-datetime", "iso-str", "iso-str-with-time"])
def test_obs_date_types(plus_one, plus_two):
    assert is_future_observation(plus_one, PULL) is False
    assert is_future_observation(plus_two, PULL) is True


@pytest.mark.unit
@pytest.mark.parametrize("obs, pull", [
    ("not-a-date", PULL), ("", PULL), (None, PULL), (20261130, PULL),
    (date(2100, 1, 1), None), (date(2100, 1, 1), "2026-11-21"),
])
def test_unreadable_clocks_are_not_judged(obs, pull):
    assert is_future_observation(obs, pull) is False


# ---------------------------------------------------------------------------
# The allowlist
# ---------------------------------------------------------------------------

ALLOWLISTED = [
    "EARNINGS:AAPL:estimate",
    "EARNINGS:BRK-B:estimate",
    "earnings:MSFT:eps_estimate",
    "LEGISLATION:119-hearing-House-1234:hearing",
]
NOT_ALLOWLISTED = [
    "EARNINGS:AAPL:actual",          # actuals exist only once reported
    "earnings:MSFT:eps_actual",
    "earnings:MSFT:surprise_pct",
    "earnings:calendar:MSFT",        # FMP stores these observed today
    "LEGISLATION:hr-1234-119:introduced",
    "earnings:MSFT:eps_estimate_v2",  # full match, not prefix match
    "XEARNINGS:AAPL:estimate",
    "Earnings:AAPL:estimate",        # series ids are case-sensitive
    "EARNINGS:AAPL:x:estimate",      # one ticker segment only
    "ephemeris.lunar_phase",
    "open_meteo.nyc_temp_max",
    "finbert.hf_news.reuters",
    "GOV_CONTRACT:dod:LMT:large",
]


@pytest.mark.unit
@pytest.mark.parametrize("series_id", ALLOWLISTED)
def test_allowlisted_families_pass_at_plus_300_days(series_id):
    skipped: dict[str, int] = {}
    assert is_future_by_design(series_id) is True
    assert skip_future_observation(series_id, _days(300), PULL, skipped) is False
    assert skipped == {}


@pytest.mark.unit
@pytest.mark.parametrize("series_id", NOT_ALLOWLISTED)
def test_everything_else_is_skipped_and_counted(series_id):
    skipped: dict[str, int] = {}
    assert is_future_by_design(series_id) is False
    assert skip_future_observation(series_id, _days(300), PULL, skipped) is True
    assert sum(skipped.values()) == 1


@pytest.mark.unit
def test_allowlisted_rows_inside_the_tolerance_are_not_special():
    skipped: dict[str, int] = {}
    for sid in ALLOWLISTED + NOT_ALLOWLISTED:
        assert skip_future_observation(sid, _days(1), PULL, skipped) is False
    assert skipped == {}


@pytest.mark.unit
def test_skips_are_counted_per_series_family():
    skipped: dict[str, int] = {}
    for sid, obs in [
        ("ephemeris.lunar_phase", date(2100, 12, 31)),
        ("ephemeris.mars.longitude", date(2100, 12, 30)),
        ("open_meteo.nyc_temp_max", _days(5)),
        ("finbert.hf_news.reuters", date(3018, 4, 9)),
        ("VIXCLS", _days(2)),
        ("VIXCLS", _days(1)),            # inside the tolerance: not counted
    ]:
        skip_future_observation(sid, obs, PULL, skipped)
    assert skipped == {"ephemeris": 2, "open_meteo": 1, "finbert": 1, "bare": 1}


@pytest.mark.unit
def test_every_allowlist_entry_names_its_writer_and_reason():
    names = [entry[0] for entry in FUTURE_BY_DESIGN_SERIES]
    assert len(names) == len(set(names))
    for name, pattern, writer, why in FUTURE_BY_DESIGN_SERIES:
        re.compile(pattern)
        assert len(why) > 40, f"{name}: say why it is future-by-design"
        source = (ROOT / writer).read_text()
        prefix = pattern.split("[", 1)[0]          # e.g. "EARNINGS:"
        suffix = pattern.rsplit(":", 1)[1]         # e.g. "estimate"
        assert prefix in source, f"{name}: {writer} does not write {prefix!r}"
        assert suffix in source, f"{name}: {writer} does not write {suffix!r}"


@pytest.mark.unit
def test_no_allowlisted_family_is_mapped_to_a_feature():
    """Passing the guard is not model eligibility.

    The allowlist note says none of these families reaches resolved_series
    today. If one is ever mapped, decide its eligibility on purpose: its
    obs_date is an event date, not an observation date.
    """
    mapped = [sid for sid in {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
              if is_future_by_design(sid)]
    assert mapped == []


# ---------------------------------------------------------------------------
# Path 1: resolve_pending workers (rolling cycle, watermark, filters, backfill)
# ---------------------------------------------------------------------------

T0 = datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc)

_ROWS = [
    ("VIXCLS", _days(-1), 17.0, 3, PULL, 1, "FRED"),
    ("VIXCLS", _days(0), 17.5, 3, PULL, 1, "FRED"),
    ("VIXCLS", _days(1), 18.0, 3, PULL, 1, "FRED"),           # boundary: passes
    ("ephemeris.lunar_phase", _days(2), 0.4, 4, PULL, 1, "astro"),
    ("ephemeris.lunar_phase", date(2100, 12, 31), 0.5, 4, PULL, 1, "astro"),
    ("finbert.hf_news.reuters", date(3018, 4, 9), 0.1, 5, PULL, 1, "finbert"),
    ("EARNINGS:AAPL:estimate", _days(300), 2.1, 6, PULL, 1, "yfinance_earnings"),
]
_KEPT = {
    ("VIXCLS", _days(-1)), ("VIXCLS", _days(0)), ("VIXCLS", _days(1)),
    ("EARNINGS:AAPL:estimate", _days(300)),
}
_EXPECTED_SKIPS = {"ephemeris": 2, "finbert": 1}


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


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
@pytest.mark.parametrize("kwargs", [
    {},                                                    # full lookback window
    {"since": T0, "until": T0 + timedelta(days=1)},        # watermark / incremental
    {"series_ids": [r[0] for r in _ROWS]},                 # operator series filter
], ids=["lookback", "watermark", "series-filter"])
def test_resolve_pending_skips_and_counts_future_rows(kwargs, warn_sink):
    engine = _Engine(raw_rows=list(_ROWS))

    summary = _resolver(engine).resolve_pending(workers=2, **kwargs)

    assert _inserted(engine) == _expected_inserts()
    assert summary["future_obs_skipped"] == _EXPECTED_SKIPS
    assert list(summary["future_obs_skipped"]) == ["ephemeris", "finbert"], "largest first"
    future_warnings = [m for m in warn_sink.messages if "future-dated raw row" in m]
    assert len(future_warnings) == 1, "one warning per run, not per worker"
    assert "3 future-dated" in future_warnings[0]
    assert "'ephemeris': 2" in future_warnings[0]


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_pending_dry_run_counts_the_same_skips():
    engine = _Engine(raw_rows=list(_ROWS))

    summary = _resolver(engine).resolve_pending(workers=1, dry_run=True)

    assert summary["resolved"] == len(_KEPT)
    assert summary["future_obs_skipped"] == _EXPECTED_SKIPS


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_pending_with_nothing_future_reports_an_empty_count(warn_sink):
    engine = _Engine(raw_rows=[r for r in _ROWS if (r[0], r[1]) in _KEPT])

    summary = _resolver(engine).resolve_pending(workers=1, dry_run=True)

    assert summary["future_obs_skipped"] == {}
    assert not [m for m in warn_sink.messages if "future-dated raw row" in m]


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_pending_early_return_carries_the_key():
    engine = _Engine(raw_rows=[])

    summary = _resolver(engine).resolve_pending(workers=1)

    assert summary["future_obs_skipped"] == {}


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_range_sums_skips_over_chunks():
    engine = _Engine(raw_rows=list(_ROWS))

    totals = _resolver(engine).resolve_range(
        since=T0, until=T0 + timedelta(days=2), chunk_days=1, workers=1, dry_run=True,
    )

    assert totals["chunks"] == 2
    # The double serves every row to every chunk, so each chunk skips them.
    assert totals["future_obs_skipped"] == {"ephemeris": 4, "finbert": 2}


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
    assert any("Selected resolution: skipped 1 future-dated" in m for m in warn_sink.messages)


@pytest.mark.unit
def test_selected_rows_keep_the_plus_one_day_boundary():
    out = _selected(lambda raw, _c: _fx(raw, pull_timestamp=SELECTED_PULL,
                                       obs_date=date(2026, 10, 11)))

    assert out["selections"][0]["reason"] == "resolved_observation"
    assert out["future_obs_skipped"] == {}


@pytest.mark.unit
def test_selected_rows_report_an_empty_count_by_default():
    out = _selected()
    assert out["future_obs_skipped"] == {}
    assert len(out["resolved_rows"]) == 6


@pytest.mark.unit
def test_selected_replay_cutoff_still_applies_first():
    """obs_date past the replay cutoff is the replay window's exclusion.

    Pulled 10-14, observed 10-16: two days past its pull, but also past the
    10-15 cutoff. The replay window drops it before the future guard sees
    it, so it is not counted as a future skip.
    """
    out = _selected(lambda raw, _c: _fx(raw, obs_date=date(2026, 10, 16)))

    assert out["selections"][0]["reason"] == "no_resolved_row"
    assert out["future_obs_skipped"] == {}


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
@pytest.mark.parametrize("offset", [0, 1, 2, 3, 5])
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
    assert selected["future_obs_skipped"] == pending["future_obs_skipped"]


@pytest.mark.unit
@pytest.mark.parametrize("method", ["resolve_pending", "resolve_selected_rows"])
def test_every_grouping_loop_uses_the_shared_helper(method):
    tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(Resolver, method))))
    called = [node.func.id for node in ast.walk(tree)
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)]
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert called.count("skip_future_observation") == 1
    assert "is_future_observation" not in called
    assert "FUTURE_OBS_TOLERANCE_DAYS" not in names, "the tolerance lives in the helper only"
