"""Operator series filter for the resolver (``--series-id``).

A feed whose mapping landed late (``skew_index``: unmapped from 2026-03-26
until #1146) needs its backlog resolved. An unfiltered ``--since`` walk
re-scans every series in every chunk (measured on griddb 2026-11-13: one day
took 223 s over 903 series). The filter resolves only the named series,
skips the distinct-series scan, and keeps the same per-row path.

These tests drive the real methods against the recording double from
``test_resolver_scan_budget``.
"""

from __future__ import annotations

import sys
import types
from datetime import date, datetime, timedelta, timezone

import pytest

from normalization.resolver import Resolver, _normalise_series_ids, main
from tests.test_resolver_scan_budget import _Engine, _resolver
from tests.test_resolver_scan_budget import entity_map  # noqa: F401  (pytest fixture)

T0 = datetime(2026, 3, 26, 0, 0, tzinfo=timezone.utc)

_ROWS = [
    ("skew_index", date(2026, 3, 26), 145.2, 3,
     datetime(2026, 3, 27, 4, 0, tzinfo=timezone.utc), 1, "CBOE"),
    ("VIXCLS", date(2026, 3, 26), 17.5, 4,
     datetime(2026, 3, 27, 6, 0, tzinfo=timezone.utc), 1, "FRED"),
]


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_filter_skips_the_distinct_scan_and_fetches_only_named_series():
    engine = _Engine(raw_rows=_ROWS)

    summary = _resolver(engine).resolve_pending(
        since=T0, until=T0 + timedelta(days=1), dry_run=True,
        series_ids=["skew_index"],
    )

    assert engine.distinct_windows() == [], "filtered run must not scan the window"
    fetched = engine.fetch_windows()
    assert fetched and all(p["sids"] == ["skew_index"] for p in fetched)
    assert summary["series_scanned"] == 1
    assert summary["scan_complete"] is True
    assert summary["dry_run"] is True
    assert summary["errors"] == 0


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_filter_reports_the_window_end_as_scanned_through():
    engine = _Engine(raw_rows=_ROWS)
    until = T0 + timedelta(days=1)

    summary = _resolver(engine).resolve_pending(
        since=T0, until=until, dry_run=True, series_ids=["skew_index"],
    )

    assert summary["scanned_through"] == until.isoformat()


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_unfiltered_run_still_scans():
    engine = _Engine(raw_rows=_ROWS)

    _resolver(engine).resolve_pending(
        since=T0, until=T0 + timedelta(days=1), dry_run=True,
    )

    assert engine.distinct_windows(), "the default path must keep its scan"


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_range_passes_the_filter_to_every_chunk():
    engine = _Engine(raw_rows=_ROWS)

    summary = _resolver(engine).resolve_range(
        since=T0, until=T0 + timedelta(days=3), chunk_days=1, dry_run=True,
        series_ids=["skew_index"],
    )

    assert summary["chunks"] == 3
    assert engine.distinct_windows() == []
    fetched = engine.fetch_windows()
    assert len(fetched) == 3
    assert all(p["sids"] == ["skew_index"] for p in fetched)


@pytest.mark.unit
@pytest.mark.parametrize("bad", [[], [""], ["  "]])
def test_empty_filter_is_refused(bad):
    with pytest.raises(ValueError):
        _normalise_series_ids(bad)


@pytest.mark.unit
def test_single_string_is_refused():
    # "skew_index" iterated as characters would silently resolve nothing.
    with pytest.raises(TypeError):
        _normalise_series_ids("skew_index")


@pytest.mark.unit
def test_filter_is_deduplicated_and_stripped():
    assert _normalise_series_ids(["b", " a ", "b"]) == ["a", "b"]
    assert _normalise_series_ids(None) is None


@pytest.mark.unit
def test_cli_passes_repeatable_series_id(monkeypatch):
    seen: dict = {}

    def fake_range(self, **kw):
        seen.update(kw)
        return {"errors": 0, "failed_ranges": []}

    monkeypatch.setattr(Resolver, "__init__", lambda self, db_engine=None: None)
    monkeypatch.setattr(Resolver, "resolve_range", fake_range)
    # main() does ``from db import get_engine``; stub the module so the test
    # never loads settings or touches a database.
    monkeypatch.setitem(sys.modules, "db", types.SimpleNamespace(get_engine=lambda: object()))

    rc = main(["--since", "2026-03-26", "--until", "2026-03-27", "--dry-run",
               "--series-id", "skew_index", "--series-id", "CBOE:SKEW"])

    assert rc == 0
    assert seen["series_ids"] == ["skew_index", "CBOE:SKEW"]
    assert seen["dry_run"] is True
