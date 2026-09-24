"""Unit tests for alpha_research/data/panel_builder.py.

Covers two remediated bugs (2026-09-24 rotation-inputs fix):

* VERIFIED FACT 1 — ``get_vix_series`` used to match
  ``fr.name ILIKE '%vix%' AND fr.family = 'vol'``, which also matched
  ``vvix`` (VIX-of-VIX, a ~80-100 scale instrument). It now reads exactly
  ``VIX_FEATURE_NAME`` ("vix_spot") by name.
* VERIFIED FACT 3 — ``build_price_panel`` deduplicated same-day multi-vintage
  rows with ``drop_duplicates(keep="first")`` over unordered DB rows, so
  which vintage won was arbitrary/non-reproducible. It now resolves the
  latest vintage deterministically via ``store/pit.py``.

A fake engine simulates the two query shapes panel_builder now issues
(a feature_registry name/id lookup, and store/pit.py's
``DISTINCT ON (feature_id, obs_date) ... ORDER BY vintage_date`` PIT query)
so these tests exercise the real dedup/PIT-filter logic without a live
Postgres (``DISTINCT ON`` is Postgres-only, see .claude/rules/data-integrity.md).
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from alpha_research.data.panel_builder import (
    VIX_FEATURE_NAME,
    build_price_panel,
    get_vix_series,
)

# ── Fake engine simulating feature_registry + resolved_series ──────────


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, stmt, params=None) -> _Result:
        params = params or {}
        sql = " ".join(str(stmt).split())
        self._engine.calls.append((sql, dict(params)))

        if "FROM feature_registry" in sql:
            if "ANY(:names)" in sql:
                names = set(params["names"])
                rows = [(fid, nm) for fid, nm in self._engine.registry if nm in names]
                return _Result(rows)
            if ":name" in sql:
                name = params["name"]
                rows = [(fid,) for fid, nm in self._engine.registry if nm == name]
                return _Result(rows)
            if "LIKE" in sql:
                rows = [(fid, nm) for fid, nm in self._engine.registry if nm.endswith("_full")]
                return _Result(rows)
            return _Result([])

        if "FROM resolved_series" in sql and "DISTINCT ON" in sql:
            fids = set(params["fids"])
            aod = params["aod"]
            order_tail = sql.rsplit("ORDER BY", 1)[-1]
            latest_wins = "DESC" in order_tail
            candidates = [
                r for r in self._engine.resolved_series
                if r["feature_id"] in fids and r["obs_date"] <= aod and r["release_date"] <= aod
            ]
            best: dict[tuple, dict] = {}
            for r in candidates:
                key = (r["feature_id"], r["obs_date"])
                cur = best.get(key)
                if cur is None:
                    best[key] = r
                elif latest_wins and r["vintage_date"] > cur["vintage_date"]:
                    best[key] = r
                elif not latest_wins and r["vintage_date"] < cur["vintage_date"]:
                    best[key] = r
            rows = [
                (r["feature_id"], r["obs_date"], r["value"], r["release_date"], r["vintage_date"])
                for r in best.values()
            ]
            return _Result(rows)

        return _Result([])


class _FakeEngine:
    """Simulates feature_registry (name/id) + resolved_series (PIT vintages)."""

    def __init__(self, registry: list[tuple[int, str]], resolved_series: list[dict]) -> None:
        self.registry = registry
        self.resolved_series = resolved_series
        self.calls: list[tuple[str, dict]] = []

    def connect(self) -> _FakeConn:
        return _FakeConn(self)


def _row(fid, obs, value, release, vintage) -> dict:
    return {
        "feature_id": fid,
        "obs_date": obs,
        "value": value,
        "release_date": release,
        "vintage_date": vintage,
    }


# ── get_vix_series: exact-name match, never vvix ────────────────────────


class TestGetVixSeriesNeverMixesVvix:
    def test_only_vix_spot_values_returned(self):
        """vix_spot and vvix both exist and both obs on the same dates; the
        old ILIKE '%vix%' match pulled in both. Only vix_spot's (low, ~15)
        values must appear — never vvix's (high, ~90) values."""
        registry = [(11, "vix_spot"), (18094, "vvix")]
        rows = []
        for i in range(25):
            d = date(2026, 5, 1 + i)
            rows.append(_row(11, d, 15.0, d, d))   # vix_spot: ~15
            rows.append(_row(18094, d, 90.0, d, d))  # vvix: ~90
        engine = _FakeEngine(registry, rows)

        s = get_vix_series(engine, start_date=date(2026, 5, 1), end_date=date(2026, 5, 25),
                            as_of_date=date(2026, 5, 25))

        assert not s.empty
        assert (s == 15.0).all(), f"vvix leaked into the VIX series: {s.unique()}"

    def test_resolves_feature_by_exact_name_not_ilike(self):
        """A query parameterized by :name must bind VIX_FEATURE_NAME exactly."""
        registry = [(11, "vix_spot")]
        engine = _FakeEngine(registry, [_row(11, date(2026, 5, 1), 15.0, date(2026, 5, 1), date(2026, 5, 1))])
        get_vix_series(engine, as_of_date=date(2026, 5, 1))
        name_calls = [p for sql, p in engine.calls if "name" in p]
        assert name_calls, "expected a feature_registry lookup bound by :name"
        assert name_calls[0]["name"] == VIX_FEATURE_NAME == "vix_spot"

    def test_missing_feature_returns_empty_series_not_error(self):
        engine = _FakeEngine([], [])
        s = get_vix_series(engine, as_of_date=date(2026, 5, 1))
        assert s.empty


class TestGetVixSeriesPitCorrectness:
    def test_latest_vintage_wins_deterministically(self):
        """Two vintages of the same obs_date — VERIFIED FACT 1/3's dedup bug.
        The later vintage_date must win, and repeated calls must agree."""
        registry = [(11, "vix_spot")]
        obs = date(2026, 5, 10)
        rows = [
            _row(11, obs, 18.0, date(2026, 5, 10), date(2026, 5, 10)),   # first vintage
            _row(11, obs, 21.0, date(2026, 5, 11), date(2026, 5, 11)),   # later revision
        ]
        # Pad with enough other days so the series isn't trivially empty.
        for i in range(1, 10):
            d = date(2026, 5, 10) - pd.Timedelta(days=i)
            rows.append(_row(11, d, 20.0, d, d))
        engine = _FakeEngine(registry, rows)

        as_of = date(2026, 5, 20)
        s1 = get_vix_series(engine, start_date=date(2026, 5, 1), end_date=as_of, as_of_date=as_of)
        s2 = get_vix_series(engine, start_date=date(2026, 5, 1), end_date=as_of, as_of_date=as_of)

        assert s1[pd.Timestamp(obs)] == 21.0
        assert s1.equals(s2), "get_vix_series must be deterministic across calls"

    def test_release_after_as_of_is_excluded(self):
        registry = [(11, "vix_spot")]
        obs = date(2026, 5, 10)
        as_of = date(2026, 5, 10)
        rows = [_row(11, obs, 99.0, date(2026, 5, 15), date(2026, 5, 15))]  # released AFTER as_of
        engine = _FakeEngine(registry, rows)

        s = get_vix_series(engine, start_date=date(2026, 5, 1), end_date=as_of, as_of_date=as_of)
        assert s.empty, "a row released after as_of must never be returned (lookahead)"


# ── build_price_panel: deterministic latest-vintage dedup ──────────────


class TestBuildPricePanelDeterministicVintage:
    def test_latest_vintage_wins_for_duplicate_obs_date(self):
        """SPY 2025-11-28 carries 679.52 then a later 683.39 revision
        (VERIFIED FACT 3) — the panel must contain 683.39, deterministically."""
        registry = [(1, "spy_full")]
        obs = date(2025, 11, 28)
        rows = [
            _row(1, obs, 679.52, date(2025, 11, 28), date(2025, 11, 28)),
            _row(1, obs, 683.39, date(2025, 11, 29), date(2025, 11, 29)),
        ]
        for i in range(1, 15):
            d = obs - pd.Timedelta(days=i)
            rows.append(_row(1, d, 680.0, d, d))
        engine = _FakeEngine(registry, rows)

        as_of = date(2025, 12, 1)
        panel1 = build_price_panel(engine, tickers=["SPY"], start_date=date(2025, 11, 1),
                                    end_date=as_of, as_of_date=as_of)
        panel2 = build_price_panel(engine, tickers=["SPY"], start_date=date(2025, 11, 1),
                                    end_date=as_of, as_of_date=as_of)

        assert panel1.loc[pd.Timestamp(obs), "SPY"] == pytest.approx(683.39)
        pd.testing.assert_frame_equal(panel1, panel2)

    def test_ticker_filter_is_exact_name_match(self):
        registry = [(1, "spy_full"), (2, "qqq_full")]
        rows = [
            _row(1, date(2026, 1, 5), 500.0, date(2026, 1, 5), date(2026, 1, 5)),
            _row(2, date(2026, 1, 5), 400.0, date(2026, 1, 5), date(2026, 1, 5)),
        ]
        engine = _FakeEngine(registry, rows)

        panel = build_price_panel(engine, tickers=["SPY"], start_date=date(2026, 1, 1),
                                   end_date=date(2026, 1, 10), as_of_date=date(2026, 1, 10))
        assert list(panel.columns) == ["SPY"]

    def test_no_matching_features_returns_empty_frame(self):
        engine = _FakeEngine([], [])
        panel = build_price_panel(engine, tickers=["ZZZZ"], start_date=date(2026, 1, 1),
                                   end_date=date(2026, 1, 10), as_of_date=date(2026, 1, 10))
        assert panel.empty
