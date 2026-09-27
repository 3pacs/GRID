"""Honesty test for ``GET /api/v1/intelligence/globe``.

Before this change a country with no GDP/FX/night-lights data at all still
got ``activity_score: 0.5`` — a fabricated "neutral" reading indistinguishable
from a country that genuinely scored neutral on real inputs. This test pins
the honest contract: when nothing could be measured for a country,
``activity_score`` is ``None`` and ``gdp_signal`` is ``"no_data"``, never a
placeholder number.

No database is touched: the engine is a ``MagicMock`` that returns empty
result sets for every query.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from api.routers import intelligence_risk as ir


def _empty_engine():
    """Mock engine whose every query returns an empty result set."""
    engine = MagicMock()
    conn = MagicMock()

    result = MagicMock()
    result.fetchall.return_value = []
    result.fetchone.return_value = None
    conn.execute.return_value = result

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def test_country_with_no_data_gets_null_activity_score_not_a_fake_neutral(monkeypatch):
    monkeypatch.setattr(ir, "get_db_engine", lambda: _empty_engine())

    result = ir._build_globe_data()

    assert result["countries"], "expected the fixed country list to be built"
    for country in result["countries"]:
        assert country["gdp_signal"] == "no_data", country
        assert country["activity_score"] is None, country
        assert country["fx_change_1m"] is None, country

    assert "generated_at" in result


def test_source_has_no_fabricated_neutral_default():
    import inspect

    src = inspect.getsource(ir._build_globe_data)
    # The old fallback: `... if score_parts else 0.5`
    assert "else 0.5" not in src
