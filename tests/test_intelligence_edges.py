from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from api.routers.intelligence_edges import get_market_edges, router


def test_edges_router_has_no_extra_prefix():
    assert router.prefix == ""


def test_get_market_edges_returns_scanner_payload():
    payload = {
        "as_of": "2026-04-18",
        "generated_at": "2026-04-18T12:00:00+00:00",
        "summary": {"count": 1},
        "opportunities": [{
            "id": "backlog-vs-model",
            "score": 77,
            "targets": ["NVDA"],
            "evidence": ["NVDA tied to a named rule change"],
            "source_tags": ["Export Controls"],
            "supporting_source_types": ["Export Controls", "Options Flow"],
            "data_mode": "live",
            "sector_focus": "Advanced semis and export-gated compute",
            "confidence_label": "high",
        }],
    }

    with patch("api.routers.intelligence_edges.get_db_engine", return_value=MagicMock()), patch(
        "api.routers.intelligence_edges.build_market_edge_snapshot",
        return_value=payload,
    ) as mock_build:
        result = asyncio.run(get_market_edges(limit=3, _token="token"))

    assert result == payload
    assert result["opportunities"][0]["targets"] == ["NVDA"]
    assert result["opportunities"][0]["data_mode"] == "live"
    mock_build.assert_called_once()
    _, kwargs = mock_build.call_args
    assert kwargs["limit"] == 3


def test_get_market_edges_falls_back_to_stable_shape_on_error():
    fallback = {
        "as_of": "2026-04-18",
        "generated_at": "2026-04-18T12:00:00+00:00",
        "summary": {"count": 0, "coverage_gap_count": 2},
        "opportunities": [],
        "coverage_gaps": [{"id": "gap-1"}, {"id": "gap-2"}],
    }

    with patch("api.routers.intelligence_edges.get_db_engine", side_effect=RuntimeError("db down")), patch(
        "api.routers.intelligence_edges.build_market_edge_snapshot",
        return_value=fallback,
    ):
        result = asyncio.run(get_market_edges(limit=2, _token="token"))

    assert result["as_of"] == fallback["as_of"]
    assert result["generated_at"] == fallback["generated_at"]
    assert result["summary"] == fallback["summary"]
    assert result["opportunities"] == []
    assert result["coverage_gaps"] == fallback["coverage_gaps"]
    assert result["error"] == "db down"
