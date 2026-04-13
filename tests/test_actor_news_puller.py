"""Tests for actor news puller parsing helpers and enumeration."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingestion.altdata.actor_news_puller import (
    NewsRow,
    extract_loyalty,
    extract_stance,
    parse_rfc822,
    score_sentiment,
    slugify,
)


class TestSlugify:
    def test_basic(self):
        assert slugify("Nelson Peltz") == "nelson_peltz"

    def test_punctuation(self):
        assert slugify("Warren Buffett Jr.") == "warren_buffett_jr"

    def test_non_ascii(self):
        assert slugify("LVMH Moët Hennessy") == "lvmh_mo_t_hennessy"

    def test_collapsed_whitespace(self):
        assert slugify("Mars   Inc.") == "mars_inc"


class TestSentiment:
    def test_positive(self):
        s = score_sentiment("Strong growth, profits beat expectations")
        assert s > 0

    def test_negative(self):
        s = score_sentiment("Fraud lawsuit, losses mount, investigation")
        assert s < 0

    def test_neutral(self):
        assert score_sentiment("reports quarterly") == 0.0

    def test_empty(self):
        assert score_sentiment("") == 0.0


class TestStance:
    def test_pro(self):
        out = extract_stance("CEO supports new trade deal")
        assert "pro" in out

    def test_anti(self):
        out = extract_stance("Activist opposes merger terms")
        assert "anti" in out
        assert "activist" in out

    def test_call_for(self):
        out = extract_stance("Senator calls for investigation")
        assert "call_for" in out


class TestLoyalty:
    def test_former_employee(self):
        out = extract_loyalty("Peltz, former CEO of Wendy's, backs the plan")
        assert any(sig.startswith("former_employee:") for sig in out)

    def test_board_member(self):
        out = extract_loyalty("She is a board member at Goldman Sachs")
        assert any(sig.startswith("board_member:") for sig in out)

    def test_founded(self):
        out = extract_loyalty("Founded Berkshire Hathaway in 1965")
        assert any(sig.startswith("founded:") for sig in out)


class TestRFC822:
    def test_iso(self):
        dt = parse_rfc822("Sat, 11 Apr 2026 12:30:00 +0000")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_gmt(self):
        dt = parse_rfc822("Sat, 11 Apr 2026 12:30:00 GMT")
        assert dt is not None

    def test_invalid(self):
        assert parse_rfc822("not a date") is None

    def test_empty(self):
        assert parse_rfc822("") is None


class TestNewsRow:
    def test_defaults(self):
        r = NewsRow(
            actor_id="nelson_peltz",
            source="google_news",
            url="https://example.com/a",
            title="Activist Peltz supports P&G board",
        )
        assert r.stance_markers == []
        assert r.loyalty_signals == []
        assert r.sentiment is None


class TestEnumerate:
    """Validates sector_map enumeration — skipped if sector_map import fails
    (some python versions can't handle late `dict | None` annotation)."""

    def test_enumerate(self):
        try:
            from ingestion.altdata.actor_news_puller import enumerate_sector_map_actors
            actors = enumerate_sector_map_actors()
        except TypeError:
            pytest.skip("sector_map import incompatible with this python version")
        assert len(actors) > 100
        assert all("actor_id" in a and "name" in a for a in actors)
        # Should be sorted by weight desc
        weights = [a["weight"] for a in actors]
        assert weights == sorted(weights, reverse=True)
