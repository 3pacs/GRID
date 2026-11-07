"""GET /intelligence/patterns must be read-only (item #23, Wave 3 triage
report): it used to upsert into event_patterns on every call. Also pins the
honest 'actionable_note' the response now carries -- hit_rate/avg_return_after
are in-sample and confidence has no holdout, so 'actionable' must never read
as a validated finding.

The mining now runs in the precompute job (``patterns`` in
intelligence/api_precompute.py) through ``_compute_discovered_patterns``; the
GET only reads the stored body (tests/test_api_precompute.py).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from api.routers import intelligence_news as news


def test_patterns_mining_calls_discover_patterns_with_persist_false():
    with patch("intelligence.pattern_engine.discover_patterns", return_value=[]) as mock_discover:
        out = news._compute_discovered_patterns(MagicMock(), 3, 4)

    assert mock_discover.call_args.kwargs["persist"] is False
    assert out["actionable_note"] == news._PATTERN_ACTIONABLE_NOTE


def test_patterns_body_carries_actionable_note_with_results():
    fake_pattern = MagicMock()
    fake_pattern.actionable = True
    fake_pattern.to_dict.return_value = {
        "id": "p1", "sequence": ["insider:bearish", "price_move:bearish"],
        "hit_rate": 0.72, "confidence": 0.81, "actionable": True,
    }
    with patch("intelligence.pattern_engine.discover_patterns", return_value=[fake_pattern]):
        out = news._compute_discovered_patterns(MagicMock(), 3, 4)

    assert out["count"] == 1
    assert out["actionable_count"] == 1
    assert "no out-of-sample holdout" in out["actionable_note"]
