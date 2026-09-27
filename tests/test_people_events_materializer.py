"""Unit tests for intelligence/people_events_materializer.py's pure functions.

No database needed: these exercise the dedup-key, known_at and field
extraction logic in isolation, per plan section 2.1
(GRID-GRANULAR-DISCOVERY-PLAN-20260927.md). The end-to-end materialize path
against real signal_sources rows is covered by
tests/test_people_events_migration_pg.py's PostgreSQL contract (skippable).
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from intelligence.people_events_materializer import (
    MaterializeResult,
    _extract_qq_form4_fields,
    form4_dedup_key,
    form4_transaction_direction,
    materialize_congress,
    materialize_form4_edgar_native,
    materialize_gov_contract,
    materialize_gov_contract_qq_aggregate,
    materialize_lobbying,
    materialize_news,
    materialize_thirteen_f,
    normalize_actor_name,
    qq_form4_known_at,
    round_up_to_next_session_open,
)


@pytest.mark.unit
class TestRoundUpToNextSessionOpen:
    def test_a_weekday_rounds_to_the_following_trading_day(self):
        # Monday 2026-09-28 -> next open is Tuesday 2026-09-29, not the same day.
        result = round_up_to_next_session_open(date(2026, 9, 28))
        assert result.date() == date(2026, 9, 29)
        assert result.tzinfo == timezone.utc

    def test_a_friday_rounds_past_the_weekend(self):
        # Friday 2026-09-25 -> next open is Monday 2026-09-28.
        result = round_up_to_next_session_open(date(2026, 9, 25))
        assert result.date() == date(2026, 9, 28)

    def test_never_rounds_to_the_same_calendar_date(self):
        for offset in range(14):
            d = date(2026, 9, 21 + offset) if offset < 10 else date(2026, 10, offset - 9)
            assert round_up_to_next_session_open(d).date() > d


@pytest.mark.unit
class TestNormalizeActorName:
    def test_strips_punctuation_and_case(self):
        # Punctuation/case-insensitive, same word order -- normalize_actor_name
        # does not reorder "Last, First" name components (too error-prone to
        # do generically), only fold punctuation/whitespace/case.
        assert normalize_actor_name("Timothy D. Cook") == normalize_actor_name("TIMOTHY D COOK")

    def test_collapses_whitespace(self):
        assert normalize_actor_name("  Jane   Q  Public  ") == "JANE Q PUBLIC"


@pytest.mark.unit
class TestForm4TransactionDirection:
    def test_acquired_disposed_code_wins_over_transaction_code(self):
        assert form4_transaction_direction("A", "S") == "buy"
        assert form4_transaction_direction("D", "P") == "sell"

    def test_falls_back_to_transaction_code(self):
        assert form4_transaction_direction(None, "P") == "buy"
        assert form4_transaction_direction(None, "S") == "sell"
        assert form4_transaction_direction(None, "A") == "award"

    def test_unclassified_codes_return_none(self):
        # M (option exercise), F (tax withholding), G (gift) are deliberately
        # not forced into buy/sell/award.
        assert form4_transaction_direction(None, "M") is None
        assert form4_transaction_direction(None, "F") is None
        assert form4_transaction_direction(None, None) is None


@pytest.mark.unit
class TestForm4DedupKey:
    def test_same_act_from_two_sources_collides(self):
        # QuiverQuant and a hypothetical EDGAR-native materializer describing
        # the exact same act (same rounded share count) must produce the same
        # key, case-insensitively, so upsert merges them into one row.
        key_a = form4_dedup_key("AAPL", "TIMOTHY D COOK", date(2026, 9, 1), "P", 1000.0)
        key_b = form4_dedup_key("aapl", "timothy d cook", date(2026, 9, 1), "p", 1000.0)
        assert key_a == key_b

    def test_shares_within_rounding_noise_still_collide(self):
        key_a = form4_dedup_key("AAPL", "X", date(2026, 9, 1), "P", 1000.4)
        key_b = form4_dedup_key("AAPL", "X", date(2026, 9, 1), "P", 1000.49)
        assert key_a == key_b  # both round() to 1000

    def test_shares_far_enough_apart_do_not_collide(self):
        key_a = form4_dedup_key("AAPL", "X", date(2026, 9, 1), "P", 1000.0)
        key_b = form4_dedup_key("AAPL", "X", date(2026, 9, 1), "P", 1001.0)
        assert key_a != key_b

    def test_different_transaction_dates_do_not_collide(self):
        key_a = form4_dedup_key("AAPL", "X", date(2026, 9, 1), "P", 100.0)
        key_b = form4_dedup_key("AAPL", "X", date(2026, 9, 2), "P", 100.0)
        assert key_a != key_b

    def test_missing_shares_still_produces_a_stable_key(self):
        key = form4_dedup_key("AAPL", "X", date(2026, 9, 1), "P", None)
        assert "NA" in key


@pytest.mark.unit
class TestQqForm4KnownAt:
    def test_file_date_present_uses_filing_basis(self):
        known_at, basis = qq_form4_known_at(date(2026, 9, 28), None)
        assert basis == "filing"
        assert known_at.date() == date(2026, 9, 29)  # rounded to next session open

    def test_falls_back_to_uploaded_when_no_file_date(self):
        known_at, basis = qq_form4_known_at(None, "2026-09-28T15:04:00+00:00")
        assert basis == "first_seen"
        assert known_at == datetime(2026, 9, 28, 15, 4, tzinfo=timezone.utc)

    def test_neither_present_returns_none(self):
        assert qq_form4_known_at(None, None) is None

    def test_unparseable_uploaded_returns_none(self):
        assert qq_form4_known_at(None, "not-a-date") is None


@pytest.mark.unit
class TestExtractQqForm4Fields:
    def test_extracts_the_verified_and_documented_keys(self):
        fields = _extract_qq_form4_fields({
            "Name": "Jane Q Public",
            "TransactionCode": "P",
            "AcquiredDisposedCode": "A",
            "Shares": "1500",
            "Price": "42.5",
            "fileDate": "2026-09-20",
            "uploaded": "2026-09-20T12:00:00+00:00",
        })
        assert fields is not None
        assert fields.owner_name == "Jane Q Public"
        assert fields.shares == 1500.0
        assert fields.price == 42.5
        assert fields.file_date == date(2026, 9, 20)

    def test_missing_owner_name_returns_none(self):
        assert _extract_qq_form4_fields({"fileDate": "2026-09-20"}) is None

    def test_missing_file_date_returns_none(self):
        assert _extract_qq_form4_fields({"Name": "Jane Q Public"}) is None

    def test_alternate_owner_name_alias_is_accepted(self):
        fields = _extract_qq_form4_fields({"Insider": "Someone", "fileDate": "2026-09-20"})
        assert fields is not None
        assert fields.owner_name == "Someone"


@pytest.mark.unit
class TestUnimplementedChannelsFailLoudly:
    """Every channel GD2 does not materialize must say so, not return zero rows."""

    @pytest.mark.parametrize(
        "fn",
        [
            materialize_form4_edgar_native,
            materialize_congress,
            materialize_thirteen_f,
            materialize_gov_contract,
            materialize_gov_contract_qq_aggregate,
            materialize_lobbying,
            materialize_news,
        ],
    )
    def test_raises_not_implemented(self, fn):
        with pytest.raises(NotImplementedError):
            fn(engine=None)


@pytest.mark.unit
def test_materialize_result_is_a_frozen_summary():
    result = MaterializeResult(channel="form4", rows_read=3, rows_upserted=2, rows_skipped=1)
    assert result.channel == "form4"
    with pytest.raises(Exception):
        result.rows_read = 99  # frozen dataclass
