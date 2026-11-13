"""Observation-date floor for resolver backfills (``--obs-since``).

skew_index's pull window 2026-03-26..11-12 also holds two stretches where
every pull re-wrote the whole 1990-onward history: 736,635 dry-run
candidates on griddb (2026-11-13), all identical to values already
resolved, against about 160 dates actually missing. The floor keeps a
backfill to the missing dates instead of adding a duplicate vintage per
pull for every past date.
"""

from __future__ import annotations

import sys
import types
from datetime import date, datetime, timedelta, timezone

import pytest

import inspect
import re

import normalization.resolver as resolver_mod
from normalization.resolver import Resolver, _normalise_obs_since, main
from tests.test_resolver_scan_budget import _Engine, _Result, _resolver
from tests.test_resolver_scan_budget import entity_map  # noqa: F401  (pytest fixture)

T0 = datetime(2026, 5, 20, 0, 0, tzinfo=timezone.utc)
PULL = datetime(2026, 5, 20, 4, 0, tzinfo=timezone.utc)

# One pull that re-wrote history (1990, 2020) and added one new date.
_ROWS = [
    ("skew_index", date(1990, 1, 2), 110.0, 3, PULL, 1, "CBOE"),
    ("skew_index", date(2020, 6, 1), 130.0, 3, PULL, 1, "CBOE"),
    ("skew_index", date(2026, 5, 19), 145.0, 3, PULL, 1, "CBOE"),
]


class _FloorEngine(_Engine):
    """The double, honouring the ``obs_since`` bind like PostgreSQL would."""

    def _conn(self):
        inner = super()._conn()
        engine = self

        class _Ctx:
            def __enter__(self_inner):
                conn = inner.__enter__()
                original = conn.execute

                def execute(statement, params=None):
                    sql = " ".join(str(statement).split())
                    if (isinstance(params, dict) and "FROM raw_series rs" in sql
                            and "obs_since" in params):
                        engine.calls.append((sql, params))
                        sids = set(params["sids"])
                        return _Result([r for r in engine._raw_rows
                                        if r[0] in sids and r[1] >= params["obs_since"]])
                    return original(statement, params)

                conn.execute = execute
                return conn

            def __exit__(self_inner, *exc):
                return inner.__exit__(*exc)

        return _Ctx()


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_floor_resolves_only_dates_on_or_after_it():
    engine = _FloorEngine(raw_rows=_ROWS)

    summary = _resolver(engine).resolve_pending(
        since=T0, until=T0 + timedelta(days=1), dry_run=True,
        series_ids=["skew_index"], obs_since=date(2026, 3, 26),
    )

    assert summary["resolved"] == 1, "history re-pulls must not become candidates"
    fetches = [p for _sql, p in engine.calls
               if isinstance(p, dict) and "obs_since" in p]
    assert fetches and all(p["obs_since"] == date(2026, 3, 26) for p in fetches)


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_without_floor_every_date_is_a_candidate():
    engine = _FloorEngine(raw_rows=_ROWS)

    summary = _resolver(engine).resolve_pending(
        since=T0, until=T0 + timedelta(days=1), dry_run=True,
        series_ids=["skew_index"],
    )

    assert summary["resolved"] == 3


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_resolve_range_passes_the_floor_to_every_chunk():
    engine = _FloorEngine(raw_rows=_ROWS)

    summary = _resolver(engine).resolve_range(
        since=T0, until=T0 + timedelta(days=2), chunk_days=1, dry_run=True,
        series_ids=["skew_index"], obs_since=date(2026, 3, 26),
    )

    assert summary["chunks"] == 2
    fetches = [p for _sql, p in engine.calls
               if isinstance(p, dict) and "obs_since" in p]
    assert len(fetches) == 2


def _fetch_statements() -> list[str]:
    src = inspect.getsource(resolver_mod.Resolver.resolve_pending)
    return [" ".join(m.split()) for m in re.findall(
        r'text\("""(\s*SELECT rs\.series_id.*?)"""\)', src, re.S)]


@pytest.mark.unit
def test_floor_statement_differs_only_by_the_predicate():
    plain, floored = _fetch_statements()
    assert "obs_date >=" not in plain, "the rolling cycle's statement must not change"
    assert "rs.obs_date >= :obs_since" in floored
    assert floored.replace(" AND rs.obs_date >= :obs_since", "") == plain


@pytest.mark.unit
@pytest.mark.usefixtures("entity_map")
def test_rolling_cycle_never_binds_obs_since():
    engine = _FloorEngine(raw_rows=_ROWS)
    _resolver(engine).resolve_pending(since=T0, until=T0 + timedelta(days=1), dry_run=True)
    assert not [p for _s, p in engine.calls if isinstance(p, dict) and "obs_since" in p]


@pytest.mark.unit
def test_obs_since_validation():
    assert _normalise_obs_since(None) is None
    assert _normalise_obs_since(date(2026, 3, 26)) == date(2026, 3, 26)
    assert _normalise_obs_since(datetime(2026, 3, 26, 5)) == date(2026, 3, 26)
    with pytest.raises(TypeError):
        _normalise_obs_since("2026-03-26")  # type: ignore[arg-type]


@pytest.mark.unit
def test_cli_parses_obs_since(monkeypatch):
    seen: dict = {}

    def fake_range(self, **kw):
        seen.update(kw)
        return {"errors": 0, "failed_ranges": []}

    monkeypatch.setattr(Resolver, "__init__", lambda self, db_engine=None: None)
    monkeypatch.setattr(Resolver, "resolve_range", fake_range)
    monkeypatch.setitem(sys.modules, "db", types.SimpleNamespace(get_engine=lambda: object()))

    rc = main(["--since", "2026-03-26", "--series-id", "skew_index",
               "--obs-since", "2026-03-26", "--dry-run"])

    assert rc == 0
    assert seen["obs_since"] == date(2026, 3, 26)
    assert seen["series_ids"] == ["skew_index"]
