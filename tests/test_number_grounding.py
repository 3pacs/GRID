"""Unit tests for ollama/number_grounding.py.

Pure functions — no DB, no network, no LLM. Covers the extraction rules
(what counts as a numeric claim vs. prose/date/count noise), the
precision-tolerant matching, and the annotate/footer/banner behavior of
``check_numbers_grounded``.
"""
from __future__ import annotations

from ollama.number_grounding import (
    DEFAULT_BANNER_THRESHOLD,
    GroundingResult,
    NumericMention,
    check_numbers_grounded,
    extract_numeric_mentions,
)


# ── extract_numeric_mentions: what gets extracted ────────────────────────


def test_extracts_dollar_amount():
    [m] = extract_numeric_mentions("SPY notional near $1,234.56 today.")
    assert m.kind == "dollar"
    assert m.value == 1234.56
    assert m.raw == "$1,234.56"


def test_extracts_dollar_with_unit_suffix():
    [m] = extract_numeric_mentions("Market cap around $1.5B.")
    assert m.kind == "dollar"
    assert m.value == 1_500_000_000.0


def test_extracts_percentage_with_sign():
    [m] = extract_numeric_mentions("Sentiment score +3.2% today.")
    assert m.kind == "percent"
    assert m.value == 3.2


def test_extracts_negative_percentage():
    [m] = extract_numeric_mentions("Down -1.5% on the session.")
    assert m.value == -1.5


def test_extracts_bare_decimal_ratio():
    [m] = extract_numeric_mentions("P/C ratio of 8.96 signals hedging.")
    assert m.kind == "decimal"
    assert m.value == 8.96
    assert m.raw == "8.96"


def test_extracts_ratio_x_suffix():
    [m] = extract_numeric_mentions("Volume was 2.5x the average.")
    assert m.kind == "ratio"
    assert m.value == 2.5


def test_extracts_comma_grouped_integer():
    [m] = extract_numeric_mentions("DJI volume near 45,123 contracts.")
    assert m.kind == "integer"
    assert m.value == 45123.0


def test_extracts_large_bare_integer():
    [m] = extract_numeric_mentions("Open interest was 4523 contracts.")
    assert m.value == 4523.0


def test_dollar_amount_without_unit_suffix_does_not_swallow_trailing_space():
    [m] = extract_numeric_mentions("Notional near $1,234.56 today.")
    assert m.raw == "$1,234.56"
    assert m.end == len("Notional near $1,234.56")


def test_does_not_double_count_dollar_mantissa_as_decimal():
    mentions = extract_numeric_mentions("Priced at $45123.46 today.")
    assert len(mentions) == 1
    assert mentions[0].kind == "dollar"


# ── extract_numeric_mentions: what gets ignored ──────────────────────────


def test_ignores_iso_date():
    assert extract_numeric_mentions("As of 2026-09-24 markets were closed.") == []


def test_ignores_iso_timestamp_with_microseconds():
    # This is literally the footer format _save_briefing writes today:
    # "*Generated: 2026-09-24T06:09:53.123456*" — must not read as decimal 53.123456.
    text = "*Generated: 2026-09-24T06:09:53.123456*"
    assert extract_numeric_mentions(text) == []


def test_ignores_clock_time():
    assert extract_numeric_mentions("Reversal happened at 14:30 ET.") == []


def test_ignores_month_name_date():
    assert extract_numeric_mentions("Filed on September 24, 2026.") == []


def test_ignores_bare_year():
    assert extract_numeric_mentions("The regime shifted in 2026.") == []


def test_bare_four_digit_non_year_is_extracted():
    # 4523 is outside the [1990, 2039] "year" window and carries no
    # $/%/decimal/comma marker, but is still 3+ digits, so it is data.
    [m] = extract_numeric_mentions("Index level near 4523 today.")
    assert m.value == 4523.0


def test_ignores_small_bare_integer_count():
    assert extract_numeric_mentions("3 sources agreed on direction.") == []


def test_ignores_markdown_list_numbering():
    text = "1. First point about the market.\n2. Second point follows.\n"
    assert extract_numeric_mentions(text) == []


def test_ignores_section_count_in_features_header():
    text = "### GRID FEATURES (150 total, 20 selected by orthogonality)"
    mentions = extract_numeric_mentions(text)
    # 150 is 3+ digits -> extracted as data; 20 is <=2 digits -> a count, ignored.
    assert [m.value for m in mentions] == [150.0]


def test_empty_text_returns_no_mentions():
    assert extract_numeric_mentions("") == []
    assert extract_numeric_mentions(None) == []  # type: ignore[arg-type]


# ── check_numbers_grounded: grounded numbers pass ────────────────────────


def test_grounded_exact_match_not_flagged():
    context = "- SPY: 63.21 (as of 2026-09-24)"
    text = "SPY closed at 63.21, a quiet session."
    result = check_numbers_grounded(text, context)
    assert result.ungrounded == ()
    assert result.total_numbers == 1
    assert result.grounded_count == 1
    assert result.annotated_text == text
    assert "[unverified]" not in result.annotated_text
    assert result.banner_triggered is False


def test_grounded_allows_rounding_to_displayed_precision():
    context = "- SPY put/call: 8.9633 (as of 2026-09-24)"
    text = "Put/call sits at 8.96, elevated."
    result = check_numbers_grounded(text, context)
    assert result.ungrounded == ()


def test_grounded_allows_reverse_rounding():
    # Context is coarser (fewer decimals) than the claim; still grounded.
    context = "- Feature z-score: 1.2 (as of 2026-09-24)"
    text = "The z-score printed 1.20 exactly."
    result = check_numbers_grounded(text, context)
    assert result.ungrounded == ()


def test_multiple_grounded_numbers_all_pass():
    context = "- VIX: 25.5 (as of 2026-09-24)\n- SPY: 4523.10 (as of 2026-09-24)"
    text = "VIX at 25.5 with SPY holding 4523.10."
    result = check_numbers_grounded(text, context)
    assert result.total_numbers == 2
    assert result.ungrounded == ()


# ── check_numbers_grounded: the 8.96 incident ────────────────────────────


def test_ungrounded_number_is_flagged_and_listed():
    # Context has no put/call figure anywhere -- exactly the 2026-09-23 incident.
    context = "- VIX: 25.5 (as of 2026-09-24)\n- SPY: 4523.10 (as of 2026-09-24)"
    text = "P/C ratio of 8.96 signals maximum hedging demand."
    result = check_numbers_grounded(text, context)

    assert len(result.ungrounded) == 1
    assert result.ungrounded[0].raw == "8.96"
    assert "8.96 [unverified]" in result.annotated_text
    assert "Unverified figures:" in result.annotated_text
    assert "8.96" in result.annotated_text.rsplit("Unverified figures:", 1)[1]


def test_ungrounded_number_not_mistaken_for_year_or_date():
    context = ""
    text = "As of 2026-09-24, put/call hit 8.96."
    result = check_numbers_grounded(text, context)
    # Only 8.96 should be a claim; the date must not appear as an ungrounded number.
    assert [m.raw for m in result.ungrounded] == ["8.96"]


def test_repeated_ungrounded_number_marked_at_every_occurrence_deduped_in_footer():
    context = ""
    text = "P/C at 8.96. Later, P/C still 8.96."
    result = check_numbers_grounded(text, context)
    assert result.annotated_text.count("8.96 [unverified]") == 2
    footer = result.annotated_text.rsplit("Unverified figures:", 1)[1]
    assert footer.count("8.96") == 1


def test_sign_mismatch_is_not_grounded():
    context = "- Sentiment: -3.2 (as of 2026-09-24)"
    text = "Sentiment printed +3.2 today."
    result = check_numbers_grounded(text, context)
    assert len(result.ungrounded) == 1
    assert result.ungrounded[0].raw == "+3.2"


# ── check_numbers_grounded: formatting variants normalize ───────────────


def test_formatting_variants_all_match_their_context_value():
    cases = [
        ("- Level: 1,234.5 (as of 2026-09-24)", "The level printed 1234.50 today."),
        ("- Level: 1234.50 (as of 2026-09-24)", "The level printed 1,234.5 today."),
        ("- Level: 1234 (as of 2026-09-24)", "Priced at $1,234 flat."),
        ("- Ratio: 12.3 (as of 2026-09-24)", "Ratio came in at 12.3%."),
    ]
    for context, text in cases:
        result = check_numbers_grounded(text, context)
        assert result.ungrounded == (), f"expected grounded for: {text!r} vs {context!r}"


# ── check_numbers_grounded: banner threshold ─────────────────────────────


def test_banner_not_triggered_below_threshold():
    # 1 of 4 ungrounded = 25% < default 30% threshold.
    context = "- A: 1.1 (x)\n- B: 2.2 (x)\n- C: 3.3 (x)"
    text = "Values were 1.1, 2.2, 3.3 and a made-up 9.99."
    result = check_numbers_grounded(text, context)
    assert result.total_numbers == 4
    assert len(result.ungrounded) == 1
    assert result.banner_triggered is False
    assert not result.annotated_text.startswith("> **GROUNDING WARNING")


def test_banner_triggered_above_threshold():
    context = "- A: 1.1 (x)"
    text = "Values: 1.1 grounded, but 9.9, 8.8, 7.7 are not."
    result = check_numbers_grounded(text, context)
    assert result.ungrounded_ratio > DEFAULT_BANNER_THRESHOLD
    assert result.banner_triggered is True
    assert result.annotated_text.startswith("> **GROUNDING WARNING")
    assert "3/4" in result.annotated_text


def test_banner_threshold_is_configurable():
    context = "- A: 1.1 (x)"
    text = "Values: 1.1 grounded, but 9.9 is not."
    # 1/2 = 50% ungrounded. Default threshold (30%) would trigger; a
    # generous custom threshold should not.
    result = check_numbers_grounded(text, context, threshold=0.9)
    assert result.banner_triggered is False


def test_no_claims_never_triggers_banner():
    result = check_numbers_grounded("No numbers here at all.", "also none here")
    assert result.total_numbers == 0
    assert result.banner_triggered is False
    assert result.ungrounded_ratio == 0.0


# ── GroundingResult.to_dict() ─────────────────────────────────────────────


def test_stats_dict_shape_and_values():
    context = ""
    text = "Fabricated figure: 8.96."
    result = check_numbers_grounded(text, context)
    stats = result.to_dict()
    assert stats["total_numbers"] == 1
    assert stats["grounded_count"] == 0
    assert stats["ungrounded_count"] == 1
    assert stats["ungrounded_values"] == ["8.96"]
    # 1/1 = 100% ungrounded, above the default 30% threshold -> banner fires.
    assert stats["banner_triggered"] is True
    assert stats["threshold"] == DEFAULT_BANNER_THRESHOLD
    assert "checked_at" in stats
    assert isinstance(stats["ungrounded_ratio"], float)


def test_empty_briefing_text_returns_empty_result():
    result = check_numbers_grounded("", "some context with 1.23")
    assert isinstance(result, GroundingResult)
    assert result.total_numbers == 0
    assert result.annotated_text == ""


def test_numeric_mention_is_frozen_and_hashable():
    m = NumericMention(raw="1.0", value=1.0, kind="decimal", decimals=1, start=0, end=3)
    assert hash(m) is not None
