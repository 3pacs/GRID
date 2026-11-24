"""The Drewry WCI parser reads the live page's own shape and nothing looser.

2026-11-22/23 on grid-svr: every ``smart:supply_chain`` run reported "No data
parsed from Drewry page". The proof rules added on 2026-11-22 refused the page
itself (read 2026-11-23), whose index sentence sits in a list under an
``<h4>`` that carries the assessment date, and names a trade lane as the
reason for the move:

    <h4>Our detailed assessment for Thursday, 19 Nov 2026</h4>
    <ul><li>The Drewry World Container Index (WCI), ..., decreased 3% to
    $4,176 per 40ft container this week due to declining rates on the
    Transpacific trade route. ...</li> ...</ul>

The fixtures below copy that structure (no table, no chart, no <article> or
<section>). Each refusal changes one thing about it.
"""

from __future__ import annotations

from datetime import date

import pytest
from loguru import logger as log

from ingestion.altdata import supply_chain as sc

PULL_DATE = date(2026, 11, 23)
HEADING = "<h4>Our detailed assessment for Thursday, 19 Nov 2026</h4>"
WCI_LI = (
    "<li>The Drewry World Container Index (WCI), the benchmark widely referenced by "
    "procurement teams, decreased 3% to $4,176 per 40ft container this week due to "
    "declining rates on the Transpacific trade route. Despite the decline, the WCI "
    "remains 126% higher YoY.</li>"
)
LANE_LIS = (
    "<li>On the Transpacific trade route, rates from Shanghai to New York decreased 9% to "
    "$8,194 per 40ft container, and those from Shanghai to Los Angeles fell 5% to $7,026 "
    "per 40ft container.</li>"
    "<li>On the Asia-Europe/Med trade route, rates from Shanghai to Genoa increased 6% to "
    "$4,436 per 40ft container, while Shanghai to Rotterdam rates remained unchanged at "
    "$3,496 per 40ft container.</li>"
)
PLAIN_WCI_LI = "<li>The Drewry World Container Index decreased 3% to $4,176 per 40ft container.</li>"


@pytest.fixture
def captured_warnings():
    messages: list[str] = []
    sink_id = log.add(lambda m: messages.append(str(m)), level="WARNING")
    yield messages
    log.remove(sink_id)


def _page(body: str) -> str:
    return (
        "<html><head><title>World Container Index - 19 Nov</title></head><body>"
        '<div class="ao-Article"><div class="aos-WYSEdit">' + body + "</div></div>"
        "</body></html>"
    )


def _parse(body: str, pull_date: date = PULL_DATE) -> list[dict]:
    return sc._parse_drewry_wci_html(_page(body), pull_date=pull_date)


def test_live_page_shape_reads_the_composite_and_its_assessment_date():
    assert _parse(HEADING + "<ul>" + WCI_LI + LANE_LIS + "</ul>") == [
        {"date": "2026-11-19", "value": 4176.0}
    ]


def test_lane_items_listed_first_do_not_win():
    assert _parse(HEADING + "<ul>" + LANE_LIS + WCI_LI + "</ul>") == [
        {"date": "2026-11-19", "value": 4176.0}
    ]


def test_a_paragraph_directly_under_the_assessment_heading_is_read():
    body = HEADING + "<p>The Drewry World Container Index decreased 3% to $4,176 per 40ft container.</p>"
    assert _parse(body) == [{"date": "2026-11-19", "value": 4176.0}]


@pytest.mark.parametrize(
    "item",
    [
        # index as subject, lane after the value, no stated cause: a lane rate
        "<li>The World Container Index rose 3% to $2,680 per 40ft container on Shanghai - Rotterdam.</li>",
        # lane between the index name and the value
        "<li>The World Container Index for Shanghai to Genoa increased 6% to $4,436 per 40ft "
        "container due to tight capacity.</li>",
        # a causal phrase, but the index is not the subject of the sentence
        "<li>Rates under the World Container Index rose to $2,680 per 40ft container due to "
        "demand on Shanghai - Rotterdam.</li>",
        # lane-only items never carry the composite
        LANE_LIS,
        # another index
        "<li>The SCFI and the World Container Index fell to $4,176 per 40ft container.</li>",
    ],
)
def test_lane_and_foreign_index_sentences_are_still_refused(item, captured_warnings):
    assert _parse(HEADING + "<ul>" + item + "</ul>") == []
    assert captured_warnings


@pytest.mark.parametrize(
    "body",
    [
        # the heading does not say it is the assessment date
        "<h4>Published 19 Nov 2026</h4><ul>" + PLAIN_WCI_LI + "</ul>",
        "<h4>Weekly update for 19 Nov 2026</h4><ul>" + PLAIN_WCI_LI + "</ul>",
        # no heading at all
        "<ul>" + PLAIN_WCI_LI + "</ul>",
        # a different heading stands between the dated one and the list
        HEADING + "<p>Summary.</p><h4>Market outlook</h4><ul>" + PLAIN_WCI_LI + "</ul>",
        # the dated heading is not a sibling of the list
        "<div>" + HEADING + "</div><div><ul>" + PLAIN_WCI_LI + "</ul></div>",
        "<div>" + HEADING + "</div><ul>" + PLAIN_WCI_LI + "</ul>",
        # the dated heading comes after the list
        "<ul>" + PLAIN_WCI_LI + "</ul>" + HEADING,
        # the heading names a lane or another index
        "<h4>Shanghai - Rotterdam assessment for Thursday, 19 Nov 2026</h4><ul>" + PLAIN_WCI_LI + "</ul>",
        "<h4>SCFI assessment for Thursday, 19 Nov 2026</h4><ul>" + PLAIN_WCI_LI + "</ul>",
        # an undated assessment heading
        "<h4>Our detailed assessment</h4><ul>" + PLAIN_WCI_LI + "</ul>",
    ],
)
def test_the_date_comes_only_from_the_heading_that_introduces_the_sentence(body, captured_warnings):
    assert _parse(body) == []
    assert captured_warnings


def test_an_assessment_heading_dated_after_the_pull_is_refused(captured_warnings):
    assert _parse(HEADING + "<ul>" + PLAIN_WCI_LI + "</ul>", pull_date=date(2026, 11, 18)) == []
    assert captured_warnings


@pytest.mark.parametrize(
    ("sentence", "value_text", "ok"),
    [
        ("The Drewry World Container Index (WCI) decreased 3% to $4,176 per 40ft container this "
         "week due to declining rates on the Transpacific trade route", "$4,176", True),
        ("WCI fell to $4,176 per 40ft container following weaker Asia-Europe demand", "$4,176", True),
        ("The World Container Index decreased 3% to $4,176 per 40ft container", "$4,176", True),
        ("The World Container Index rose to $2,680 per 40ft container on Shanghai - Rotterdam",
         "$2,680", False),
        ("On the Transpacific, the World Container Index fell to $4,176 due to weak demand",
         "$4,176", False),
        ("Carriers said the World Container Index fell to $4,176 due to the Transpacific",
         "$4,176", False),
    ],
)
def test_trailing_cause_rule(sentence, value_text, ok):
    value_end = sentence.index(value_text) + len(value_text)
    assert sc._drewry_lane_is_trailing_cause(sentence, value_end) is ok


# ── reported, not predicted (Codex HOLD 6032263784) ─────────────────────────

ACTUAL = ("The Drewry World Container Index decreased 3% to $4,176 per 40ft container this week "
          "due to declining rates on the Transpacific trade route.")
FORECAST = ("The Drewry World Container Index is expected to fall to $4,176 per 40ft container "
            "due to weak Transpacific rates.")


def test_the_same_number_is_read_when_reported_and_refused_when_forecast(captured_warnings):
    """The pair Codex asked for: same heading, same value, same lane clause."""
    assert _parse(HEADING + "<ul><li>" + ACTUAL + "</li></ul>") == [{"date": "2026-11-19", "value": 4176.0}]
    assert _parse(HEADING + "<ul><li>" + FORECAST + "</li></ul>") == []
    assert captured_warnings


@pytest.mark.parametrize("sentence", [
    FORECAST,
    "The Drewry World Container Index will fall to $4,176 per 40ft container next week.",
    "The Drewry World Container Index could reach $4,176 per 40ft container by December.",
    "The Drewry World Container Index is likely to drop to $4,176 per 40ft container.",
    "The Drewry World Container Index is set to decline to $4,176 per 40ft container.",
    "Drewry forecasts the World Container Index at $4,176 per 40ft container for next week.",
    "The World Container Index may ease to $4,176 per 40ft container.",
    "The World Container Index target of $4,176 per 40ft container was repeated.",
    # no assessment verb at all before the value
    "The World Container Index, $4,176 per 40ft container.",
])
def test_predictive_or_verbless_prose_is_refused(sentence, captured_warnings):
    assert _parse(HEADING + "<ul><li>" + sentence + "</li></ul>") == []
    assert captured_warnings


def test_a_forecast_item_does_not_stop_the_reported_item_from_being_read():
    body = HEADING + "<ul><li>" + FORECAST + "</li><li>" + ACTUAL + "</li></ul>"
    assert _parse(body) == [{"date": "2026-11-19", "value": 4176.0}]


def test_a_forecast_after_the_reported_value_does_not_undo_it():
    sentence = ("The Drewry World Container Index decreased 3% to $4,176 per 40ft container this week, "
                "and Drewry expects rates to fall further.")
    assert _parse(HEADING + "<ul><li>" + sentence + "</li></ul>") == [{"date": "2026-11-19", "value": 4176.0}]


# The boundary pair from Codex HOLD 6032642033: an assessment verb in one
# clause, the amount in a forecast clause after it.
OTHER_CLAUSE_FORECAST = ("The Drewry World Container Index declined this week, and for next week Drewry "
                         "sees $4,176 per 40ft container due to weak Transpacific rates.")
ACTUAL_THEN_FORECAST = ("The Drewry World Container Index declined 3% to $4,176 per 40ft container this week, "
                        "and for next week Drewry sees $4,050 per 40ft container.")


def test_a_verb_in_another_clause_does_not_report_the_amount(captured_warnings):
    assert _parse(HEADING + "<ul><li>" + OTHER_CLAUSE_FORECAST + "</li></ul>") == []
    assert captured_warnings


def test_an_actual_price_followed_by_a_forecast_clause_still_reads_the_actual_price():
    assert _parse(HEADING + "<ul><li>" + ACTUAL_THEN_FORECAST + "</li></ul>") == [
        {"date": "2026-11-19", "value": 4176.0}]


@pytest.mark.parametrize("sentence", [
    "The Drewry World Container Index declined this week and Drewry puts the new level at about $4,176 per 40ft container.",
    "The Drewry World Container Index fell; analysts pencil in $4,176 per 40ft container.",
    "The Drewry World Container Index rose this week (consensus: $4,176 per 40ft container).",
    "The Drewry World Container Index fell to an estimated $4,176 per 40ft container.",
    "The Drewry World Container Index declined, with $4,176 per 40ft container the level to watch.",
])
def test_the_amount_must_sit_in_the_reporting_clause(sentence, captured_warnings):
    assert _parse(HEADING + "<ul><li>" + sentence + "</li></ul>") == []


@pytest.mark.parametrize("sentence", [
    "The Drewry World Container Index decreased 3% this week to $4,176 per 40ft container.",
    "The Drewry World Container Index fell by 3% to $4,176 per 40ft container.",
    "The Drewry World Container Index remained unchanged at $4,176 per 40ft container.",
    "The Drewry World Container Index stood at $4,176 per 40ft container.",
    "The Drewry World Container Index was $4,176 per 40ft container.",
])
def test_reporting_clauses_that_are_read(sentence):
    assert _parse(HEADING + "<ul><li>" + sentence + "</li></ul>") == [{"date": "2026-11-19", "value": 4176.0}]


# The pair from Codex HOLD 6033027424: the index is in the sentence, the
# dollar figure belongs to another subject.
FOREIGN_SUBJECT = ("The Drewry World Container Index declined this week, and the peak season surcharge "
                   "stood at $4,176 per 40ft container.")
WCI_SUBJECT_CONTROL = ("The Drewry World Container Index stood at $4,176 per 40ft container, and the peak "
                       "season surcharge rose.")


def test_another_subjects_price_is_refused_and_the_indexs_own_is_read(captured_warnings):
    assert _parse(HEADING + "<ul><li>" + FOREIGN_SUBJECT + "</li></ul>") == []
    assert captured_warnings
    assert _parse(HEADING + "<ul><li>" + WCI_SUBJECT_CONTROL + "</li></ul>") == [
        {"date": "2026-11-19", "value": 4176.0}]


@pytest.mark.parametrize("sentence", [
    FOREIGN_SUBJECT,
    "The Drewry World Container Index declined this week, and the fuel surcharge was $4,176 per 40ft container.",
    "The Drewry World Container Index fell this week while the bunker adjustment factor rose to $4,176 per 40ft container.",
    "The Drewry World Container Index declined this week and spot rates stood at $4,176 per 40ft container.",
    "The Drewry World Container Index rose, but the carrier's general rate increase was $4,176 per 40ft container.",
    "Against the Drewry World Container Index, the contract rate was $4,176 per 40ft container.",
    "Carriers tracking the Drewry World Container Index said their average rate fell to $4,176 per 40ft container.",
    "This week the peak season surcharge and the Drewry World Container Index both stood at $4,176 per 40ft container.",
    # No comma to split on: a second subject between the index's verb and "to".
    "The Drewry World Container Index declined as surcharges rose to $4,176 per 40ft container.",
    "The Drewry World Container Index fell and rates rose to $4,176 per 40ft container.",
    "The Drewry World Container Index eased while surcharges climbed to $4,176 per 40ft container.",
    "The Drewry World Container Index held but surcharges jumped to $4,176 per 40ft container.",
])
def test_a_price_whose_subject_is_not_the_index_is_refused(sentence, captured_warnings):
    assert _parse(HEADING + "<ul><li>" + sentence + "</li></ul>") == []


@pytest.mark.parametrize("sentence", [
    WCI_SUBJECT_CONTROL,
    ("The Drewry World Container Index (WCI), the benchmark widely referenced by procurement teams, "
     "decreased 3% to $4,176 per 40ft container this week."),
    "The WCI composite fell 3% to $4,176 per 40ft container, while the peak season surcharge stood at $600.",
    "Drewry's World Container Index remained at $4,176 per 40ft container; the fuel surcharge was $350.",
    "The Drewry World Container Index decreased 3% this week to $4,176 per 40ft container.",
    "The Drewry World Container Index fell sharply week-on-week to $4,176 per 40ft container.",
    "The Drewry World Container Index remained steady at $4,176 per 40ft container.",
    "The Drewry World Container Index was assessed at $4,176 per 40ft container.",
])
def test_the_indexs_own_reporting_clause_is_read_whatever_follows(sentence):
    assert _parse(HEADING + "<ul><li>" + sentence + "</li></ul>") == [{"date": "2026-11-19", "value": 4176.0}]


@pytest.mark.parametrize(("lead", "ok"), [
    ("The Drewry World Container Index decreased 3% to $", True),
    ("The World Container Index composite assessed on 19 Nov 2026 stood at $", True),
    ("WCI remained at $", True),
    ("The Drewry World Container Index (WCI), the benchmark widely referenced by procurement teams, decreased 3% to $", True),
    ("The Drewry World Container Index is expected to fall to $", False),
    ("The World Container Index would have been $", False),
    ("The World Container Index $", False),
    ("The Drewry World Container Index declined this week, and for next week Drewry sees $", False),
    ("The Drewry World Container Index declined, and Drewry puts it at $", False),
    ("The Drewry World Container Index declined this week, and the peak season surcharge stood at $", False),
    ("The Drewry World Container Index declined this week, and the fuel surcharge was $", False),
    ("The peak season surcharge stood at $", False),
    ("The Drewry World Container Index declined as surcharges rose to $", False),
    ("The Drewry World Container Index fell and rates rose to $", False),
    ("The Drewry World Container Index edged lower this week to $", True),
    ("The Drewry World Container Index, the fuel surcharge was $", False),
])
def test_value_is_reported_rule(lead, ok):
    assert sc._drewry_value_is_reported(lead + "4,176", len(lead)) is ok
