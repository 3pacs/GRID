"""Tests for intelligence.sector_health.

Uses a MagicMock Engine that dispatches SQL queries to a side-effect
function, so every test can tailor the rows returned per table.

Covers:
  * end-to-end ``compute_sector_health`` with full data
  * graceful fallback when every sub-score is missing
  * trend computation with and without prior snapshot rows
  * narrative template formatting
  * FastAPI endpoint shape + 404 for unknown sectors
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from intelligence import sector_health as sh


# ─────────────────────────────────────────────────────────────────
# Mock engine helpers
# ─────────────────────────────────────────────────────────────────

def _make_router_engine(side_effect):
    """Build a mock engine whose ``conn.execute`` delegates to the
    ``side_effect`` callable: ``side_effect(sql_text, params) -> MagicMock``
    where the returned mock must support ``.fetchall()`` / ``.fetchone()``.
    """
    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, *args, **kwargs):
        sql = str(getattr(stmt, "text", stmt))
        params = {}
        try:
            params = stmt.compile().params  # type: ignore[attr-defined]
        except Exception:
            pass
        return side_effect(sql, params)

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _res(rows=None, one=None):
    """Wrap rows into a MagicMock supporting fetchall/fetchone."""
    m = MagicMock()
    m.fetchall.return_value = rows if rows is not None else []
    if one is not None:
        m.fetchone.return_value = one
    elif rows:
        m.fetchone.return_value = rows[0]
    else:
        m.fetchone.return_value = None
    return m


def _regclass(value: str | None):
    """Return a fetchone-style row for to_regclass queries."""
    return (value,)


# ─────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────

def test_compute_sector_health_full_data_returns_0_to_100():
    """With full happy-path data for every sub-score, compute_sector_health
    must return a score in [0, 100], populate every component, and emit
    a narrative string."""

    def side_effect(sql: str, params: dict):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.dummy"))
        if "from capital_flows" in s and "flow_type in ('revenue', 'cogs')" in s:
            # margin_score path: expanding margins
            return _res(rows=[
                (date(2025, 12, 31), "revenue", 1000.0),
                (date(2025, 12, 31), "cogs", 400.0),
                (date(2024, 12, 31), "revenue", 900.0),
                (date(2024, 12, 31), "cogs", 380.0),
                (date(2023, 12, 31), "revenue", 800.0),
                (date(2023, 12, 31), "cogs", 360.0),
                (date(2022, 12, 31), "revenue", 700.0),
                (date(2022, 12, 31), "cogs", 350.0),
            ])
        if "from capital_flows" in s and "debt_issuance" in s:
            # capital_allocation_score path
            return _res(rows=[
                (date(2025, 12, 31), "revenue", 1000.0),
                (date(2025, 12, 31), "cogs", 400.0),
                (date(2025, 12, 31), "opex", 200.0),
                (date(2025, 12, 31), "capex", 100.0),
                (date(2025, 12, 31), "dividends", 30.0),
                (date(2025, 12, 31), "buybacks", 40.0),
                (date(2025, 12, 31), "debt_issuance", 20.0),
            ])
        if "from supply_chain_edges" in s:
            # low chokepoint avg → strong chokepoint sub-score
            return _res(one=(0.25,))
        if "from insider_trades" in s:
            return _res(one=(10.0, 2.0))  # strong net buys
        if "from congressional_trades" in s:
            return _res(one=(4.0, 1.0))
        if "from dark_pool_weekly" in s or "from dark_pool" in s:
            return _res(rows=[(0.40,), (0.42,), (0.38,)])  # accumulation
        if "from sector_health_snapshots" in s:
            return _res(one=None)
        return _res()

    engine, _ = _make_router_engine(side_effect)
    result = sh.compute_sector_health(engine, "Technology")

    assert 0.0 <= result["score"] <= 100.0
    assert set(result["components"].keys()) == set(sh.WEIGHTS.keys())
    for k, v in result["components"].items():
        assert 0.0 <= v <= 1.0, f"{k} out of range: {v}"
    assert isinstance(result["narrative"], str) and len(result["narrative"]) > 0
    # trend defaults to stable when there's no prior snapshot
    assert result["trend_30d"] == "stable"
    # Full-data scenario should be clearly above neutral (50)
    assert result["score"] > 55.0


def test_compute_sector_health_missing_data_is_neutral():
    """Empty tables → neutral 0.5 components → score == 50."""

    def side_effect(sql: str, params: dict):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass(None))  # every table absent
        return _res()

    engine, _ = _make_router_engine(side_effect)
    result = sh.compute_sector_health(engine, "Technology")

    assert result["score"] == 50.0
    for v in result["components"].values():
        assert v == 0.5
    assert result["trend_30d"] == "stable"


def test_trend_from_snapshots_improving_and_deteriorating():
    """Covers both branches of ``_trend_from_snapshots`` + the default."""
    # improving: latest score comfortably above prior
    def improving(sql, params):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.sector_health_snapshots"))
        if "from sector_health_snapshots" in s:
            return _res(one=(40.0,))
        return _res()

    engine, conn = _make_router_engine(improving)
    with engine.connect() as c:
        trend = sh._trend_from_snapshots(c, "Technology", latest_score=55.0)
    assert trend == "improving"

    # deteriorating: latest well below prior
    def deteriorating(sql, params):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.sector_health_snapshots"))
        if "from sector_health_snapshots" in s:
            return _res(one=(70.0,))
        return _res()

    engine, conn = _make_router_engine(deteriorating)
    with engine.connect() as c:
        trend = sh._trend_from_snapshots(c, "Technology", latest_score=55.0)
    assert trend == "deteriorating"

    # no prior snapshot → stable
    def no_prior(sql, params):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.sector_health_snapshots"))
        if "from sector_health_snapshots" in s:
            return _res(one=None)
        return _res()

    engine, conn = _make_router_engine(no_prior)
    with engine.connect() as c:
        trend = sh._trend_from_snapshots(c, "Technology", latest_score=55.0)
    assert trend == "stable"


def test_narrative_template_mentions_score_and_trend_and_levers():
    components = {
        "margin": 0.9, "chokepoints": 0.6, "capital_allocation": 0.5,
        "insider": 0.4, "congress": 0.3, "dark_pool": 0.2,
    }
    text = sh._build_narrative("Consumer Staples", 62.0, components, "improving")
    assert "Consumer Staples" in text
    assert "62" in text
    assert "trending up" in text
    # strongest = margin, weakest = dark_pool
    assert "margin trajectory" in text
    assert "dark-pool" in text


def test_endpoint_shape_and_unknown_sector(monkeypatch):
    """Endpoint returns the documented dict for known sectors and
    raises 404 for unknown ones."""
    import asyncio
    from fastapi import HTTPException

    from api.routers import sector_health as router_mod

    # Avoid the real DB engine and the TTL cache keeping stale values
    # across the two assertions below.
    router_mod._CACHE.clear() if hasattr(router_mod._CACHE, "clear") else None

    fake_result = {
        "sector": "Technology",
        "score": 67.5,
        "trend_30d": "stable",
        "components": {
            "margin": 0.7, "chokepoints": 0.6, "capital_allocation": 0.65,
            "insider": 0.55, "congress": 0.5, "dark_pool": 0.6,
        },
        "narrative": "Technology health 68/100 — stable. Strongest lever: ...",
        "as_of": datetime.now(timezone.utc).isoformat(),
    }

    def fake_compute(engine, sector_name):
        return fake_result

    monkeypatch.setattr(
        "intelligence.sector_health.compute_sector_health", fake_compute,
    )
    monkeypatch.setattr(
        "api.routers.sector_health.get_db_engine", lambda: MagicMock(),
    )

    result = asyncio.run(router_mod.get_sector_health("Technology", "test-token"))
    assert result["sector"] == "Technology"
    assert 0.0 <= result["score"] <= 100.0
    assert result["trend_30d"] in {"improving", "stable", "deteriorating"}
    assert set(result["components"].keys()) == set(sh.WEIGHTS.keys())
    assert isinstance(result["narrative"], str)
    assert "as_of" in result

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            router_mod.get_sector_health("Atlantis", "test-token"),
        )
    assert excinfo.value.status_code == 404
