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
