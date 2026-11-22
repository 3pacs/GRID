"""Claim-guard precision: accepted caveat forms, GRID labels, posture and actor rules.

Origin: 169 of 222 briefings (76%) on 2026-11-11..20 were withheld by
``check_publication_claims``. The only retained rejected drafts (12, from the
2026-11-04 packet, same guard and prompt code) showed 9 withheld only for
grounded caveats the prompt asked for ("no specific institutional distribution
or accumulation is evidenced", "does not measure buyer initiation or hedging
intent", "the positioning proxy scores +1.00 (supportive)"). The three true
positives in that sample, and actor/posture claims that slipped through into
published narratives, must stay blocked. Matching is widened only for whole
caveat clauses; no new evidence counts as support.
"""

from __future__ import annotations

import pytest

from ollama.number_grounding import check_publication_claims

# Exact clauses or sentences from the retained 2026-11-04 rejected drafts that
# are grounded caveats or GRID-label descriptions. Each one alone used to withhold.
RETAINED_FALSE_POSITIVES = [
    # market_briefings:4945 / 4960 / 4963
    ("Actor actions and liquidity valves are unavailable; no specific institutional distribution or "
    "accumulation is evidenced by the supplied data."),
    # 4944
    ("Actor actions and liquidity valves are unavailable; no specific institutional distribution or "
    "accumulation is established by the data."),
    # 4947
    ("Actor actions and liquidity valves are unavailable; no source-bound evidence supports specific "
    "institutional distribution or accumulation."),
    # 4944
    ("The most significant observable condition is the SPY put/call ratio of 2.95, which measures relative "
    "contract open interest; this high ratio indicates a heavy concentration of put contracts relative to "
    "calls, though it does not measure buyer initiation or hedging intent."),
    # 4951
    ("Insider/congressional ratios (21%/25%) are low but do not establish institutional distribution; "
    "they are merely relative transaction counts."),
    # 4951
    ("The SPY put/call ratio (2.95) is high, but this measures relative open interest contracts, "
    "not buyer initiation or hedging intent."),
    # 4956
    ("The SPY put/call ratio is 2.95, a high level indicating relative put open interest, though this "
    "measures contract volume, not specific hedging intent or dealer positioning."),
    # 4959
    ("The SPY put/call ratio is 2.9527, indicating relative contract activity, but this does not measure "
    "buyer initiation or hedging intent."),
    "The SPY put/call ratio is elevated, but this does not confirm hedging intent.",
    # 4962
    ("The SPY put/call ratio of 2.9168 reflects relative contract volume, not specific buyer intent or "
    "hedging activity."),
    # 4952 (parenthesis no longer split at "and")
    "The insider/congressional ratios (21% and 25% buy ratios) do not establish institutional conviction.",
]

# Proxy labels with their signed score, after an actor caveat earlier in the text.
LABEL_CASES = [
    ("No specific institutional distribution or accumulation is evidenced. "
    "The positioning proxy scores +1.00 (supportive)."),
    ("No specific institutional distribution or accumulation is evidenced. "
    "The positioning proxy is supportive (score +1.00)."),
    ("No specific institutional distribution or accumulation is evidenced. "
    "Yet the computed sentiment positioning proxy is supportive (+1.00)."),
    ("No specific institutional distribution or accumulation is evidenced. "
    "The ETF activity proxy (+1.00) and dark pool proxy (-0.20) disagree."),
    ("No specific institutional distribution or accumulation is evidenced. "
    "Watch the SPY put/call ratio for changes in relative contract positioning."),
]

# Exact true-positive spans from the retained drafts: must stay withheld.
RETAINED_TRUE_POSITIVES = [
    # 4946
    ("The positioning proxy (+1.00) and ETF activity proxy (+1.00) are strongly supportive, "
    "suggesting institutional or large-player support."),
    # 4957 (missed by the old keyword list; caught only by luck on a later clause)
    ("Insider and congressional ratios (25% buys, 75% sells) further contradict the neutral-to-greedy "
    "sentiment score, pointing to potential institutional or informed caution despite stable equity prices."),
    # 4963
    ("The high put/call ratio (2.9168) conflicts with the greed sentiment reading, as high put ratios "
    "often indicate defensive positioning."),
]

# Leaked into published 2026-11-11..20 narratives under the old guard.
PUBLISHED_LEAKS = [
    ("This divergence implies that while aggregate positioning metrics look stable, informed participants "
    "are reducing exposure."),  # 5113
    ("These positioning proxies suggest caution among informed participants, even as the broader index "
    "holds levels."),  # 5142
    ("Insider activity is heavily bearish (287 buys vs 946 sells), creating a conflict between informed "
    "selling and broader market support."),  # 5121
    "The SPY put/call ratio (1.34) suggests defensive positioning relative to calls.",  # 5155
    "Credit ETF prices (HYG, LQD) reflect risk-off positioning.",  # 5339
    "The VIX (21.40) is elevated, which often signals caution.",  # 5132
]

# Adversarial probes: every one asserts an unsupported actor, intent or posture.
ADVERSARIAL = [
    "Not hedging intent, but dealers bought puts.",
    "The positioning proxy shows institutions are buying.",
    "The positioning proxy is supportive, indicating institutional accumulation.",
    "The positioning proxy is supportive as institutions accumulate shares.",
    "Supportive positioning proxies show smart money support.",
    "The insider ratio (institutions bought and sold) is low.",
    "It does not measure hedging intent today institutions bought puts.",
    "No specific institutional distribution or accumulation is evidenced, yet institutions are selling.",
    "No institutional distribution or accumulation is established; they are selling.",
    "Institutional selling is unknown. The positioning proxy is supportive.",
    "Not buyer initiation or hedging intent; traders purchased puts.",
    "The put/call ratio does not measure buyer initiation, so investors are hedging.",
    "Insider and congressional ratios point to informed caution.",
    "Institutions remain cautious.",
]


@pytest.mark.parametrize("text", RETAINED_FALSE_POSITIVES)
def test_retained_grounded_caveats_pass(text: str) -> None:
    result = check_publication_claims(text)
    assert result["passed"], result["reasons"]


@pytest.mark.parametrize("text", LABEL_CASES)
def test_grid_proxy_labels_with_signed_score_are_nouns(text: str) -> None:
    result = check_publication_claims(text)
    assert result["passed"], result["reasons"]


@pytest.mark.parametrize("text", RETAINED_TRUE_POSITIVES + PUBLISHED_LEAKS + ADVERSARIAL)
def test_unsupported_actor_intent_and_posture_stay_withheld(text: str) -> None:
    assert not check_publication_claims(text)["passed"]


def test_adversarial_probe_count_is_fourteen() -> None:
    blocked = [t for t in ADVERSARIAL if not check_publication_claims(t)["passed"]]
    assert len(ADVERSARIAL) == 14 and blocked == ADVERSARIAL


@pytest.mark.parametrize("denial", [
    "This does not measure buyer initiation or hedging intent",
    "Not specific hedging intent or dealer positioning",
    "No specific institutional distribution or accumulation is evidenced",
    "They do not establish institutional distribution",
    "The insider/congressional ratios (21% and 25% buy ratios) do not establish institutional conviction",
])
@pytest.mark.parametrize("separator", [" but ", ", ", "; ", ". ", "\n"])
@pytest.mark.parametrize("tail,reason", [
    ("institutions are selling", "unsupported_actor_interpretation"),
    ("dealers bought puts", "unsupported_options_intent"),
    ("it is occurring beneath the rally", None),
])
def test_widened_denial_never_licenses_a_tail(denial: str, separator: str, tail: str, reason: str | None) -> None:
    result = check_publication_claims(denial + separator + tail + ".")
    if reason is None:  # elided subject: whichever denied measure it refers back to
        assert not result["passed"]
    else:
        assert reason in result["reasons"]


@pytest.mark.parametrize("text", [
    "It does not measure hedging intent that institutions show.",
    "Not specific hedging intent among dealers who bought puts.",
    "No specific institutional distribution or accumulation is evidenced except heavy selling.",
    "Retail traders do not measure hedging intent.",
])
def test_denial_grammar_is_whole_clause_only(text: str) -> None:
    assert not check_publication_claims(text)["passed"]


@pytest.mark.parametrize("text,reason", [
    ("The put/call ratio suggests defensive positioning.", "unsupported_positioning_posture"),
    ("Credit proxies show risk-off positioning.", "unsupported_positioning_posture"),
    ("Positioning is defensive across sectors.", "unsupported_positioning_posture"),
    ("Cautious positioning dominates.", "unsupported_positioning_posture"),
    ("Elevated VIX signals caution.", "unsupported_positioning_posture"),
    ("The insider ratio is consistent with caution.", "unsupported_positioning_posture"),
    ("Informed participants are reducing exposure.", "unsupported_actor_interpretation"),
    ("Large players are buying the dip.", "unsupported_actor_interpretation"),
    ("Informed selling dominates the tape.", "unsupported_actor_interpretation"),
])
def test_posture_and_named_actor_rules(text: str, reason: str) -> None:
    assert reason in check_publication_claims(text)["reasons"]


@pytest.mark.parametrize("text", [
    "Not a measure of defensive positioning.",
    "This proxy cannot confirm defensive positioning.",
    "Not a direct measure of informed trading.",
    "Informed participant exits are unknown.",
    "Hypothesis: informed participants may be exiting (unverified).",
])
def test_posture_and_actor_denials_pass(text: str) -> None:
    result = check_publication_claims(text)
    assert result["passed"], result["reasons"]


def test_label_direction_needs_the_score_and_no_actor() -> None:
    # Without its signed score the direction word may still describe an actor.
    assert not check_publication_claims(
        "Institutional selling is unknown. The positioning proxy is supportive.")["passed"]
    assert not check_publication_claims(
        "Institutional selling is unknown. The positioning proxy (+1.00) shows institutional support.")["passed"]
    assert check_publication_claims(
        "Institutional selling is unknown. The positioning proxy score (+1.00) is supportive.")["passed"]


def test_parenthesis_split_keeps_actor_words_in_clause() -> None:
    assert not check_publication_claims("The ratio (institutions bought, retail sold) is low.")["passed"]
    assert not check_publication_claims("Counts (and institutions sold) are low.")["passed"]


def test_reason_counts_report_clause_hits_without_text() -> None:
    result = check_publication_claims(
        "Institutions are selling. They are buying puts. Retail is buying. Defensive positioning dominates.")
    # The posture clause also carries the earlier named actor.
    assert result["reason_counts"]["unsupported_actor_interpretation"] == 4
    assert result["reason_counts"]["unsupported_options_intent"] == 1
    assert result["reason_counts"]["unsupported_positioning_posture"] == 1
    assert set(result["reason_counts"]) == set(result["reasons"])
    assert all(isinstance(v, int) for v in result["reason_counts"].values())
    assert check_publication_claims("VIX is 18.2.")["reason_counts"] == {}


def test_prompt_caveat_sentences_pass_the_guard() -> None:
    from ollama.market_briefing import MarketBriefingEngine

    rule = MarketBriefingEngine._CAVEAT_WORDING_RULE
    sentence = MarketBriefingEngine._ACTOR_UNAVAILABLE_SENTENCE
    assert sentence == "Actor actions and liquidity valves are unavailable or unknown."
    assert sentence in rule and "the positioning proxy score (+1.00)" in rule
    for text in [sentence, "Not a measure of buyer initiation or intent.",
                 "Institutional distribution is unknown. The positioning proxy score (+1.00) is supportive.",
                 sentence + " The positioning proxy score (+1.00) is supportive."]:
        result = check_publication_claims(text)
        assert result["passed"], (text, result["reasons"])
