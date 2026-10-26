"""GET /intelligence/patterns must be read-only (item #23, Wave 3 triage
report): it used to upsert into event_patterns on every call. Also pins the
honest 'actionable_note' the response now carries -- hit_rate/avg_return_after
are in-sample and confidence has no holdout, so 'actionable' must never read
as a validated finding.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from api.routers import intelligence_news as news


def test_get_patterns_calls_discover_patterns_with_persist_false():
    with patch.object(news, "get_db_engine", return_value=MagicMock()), \
         patch("intelligence.pattern_engine.discover_patterns", return_value=[]) as mock_discover:
        out = asyncio.run(news.get_discovered_patterns(
            min_occurrences=3, max_sequence_length=4, _token="t",
        ))

    assert mock_discover.call_args.kwargs["persist"] is False
    assert out["actionable_note"] == news._PATTERN_ACTIONABLE_NOTE


def test_get_patterns_response_carries_actionable_note_with_results():
    fake_pattern = MagicMock()
    fake_pattern.actionable = True
    fake_pattern.to_dict.return_value = {
        "id": "p1", "sequence": ["insider:bearish", "price_move:bearish"],
        "hit_rate": 0.72, "confidence": 0.81, "actionable": True,
    }
    with patch.object(news, "get_db_engine", return_value=MagicMock()), \
         patch("intelligence.pattern_engine.discover_patterns", return_value=[fake_pattern]):
        out = asyncio.run(news.get_discovered_patterns(
            min_occurrences=3, max_sequence_length=4, _token="t",
        ))

    assert out["count"] == 1
    assert out["actionable_count"] == 1
    assert "no out-of-sample holdout" in out["actionable_note"]
