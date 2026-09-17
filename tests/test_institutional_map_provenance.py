"""The institutional map must admit it is a hand-curated table.

Covers audit A-H1 / A-H2 / A-H3. ``build_institutional_graph(engine)`` takes
an engine and never opens a connection; the other entry points take none at
all. Every AUM, net-worth and allocation figure in
``intelligence/institutional_map.py`` is a literal about a named real person
or organization, and used to ship beside ``"confidence": "confirmed"`` with
fee projections silently built on an assumed 8% gross return.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from intelligence import institutional_map as im

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPO_ROOT / "intelligence" / "institutional_map.py"


def _src() -> str:
    return SOURCE.read_text(encoding="utf-8", errors="replace")


# ── Criterion 4: data_source + as_of on every record, no "confirmed" ──────


@pytest.mark.unit
def test_module_asserts_no_confirmed_confidence() -> None:
    assert '"confidence": "confirmed"' not in _src()


@pytest.mark.unit
def test_every_graph_record_carries_data_source_and_as_of() -> None:
    graph = im.build_institutional_graph(None)

    records = (
        graph["nodes"]
        + graph["links"]
        + graph["conflicts"]
        + [graph["metadata"], graph["fee_summary"]]
    )
    assert records
    for rec in records:
        assert rec.get("data_source") == im.DATA_SOURCE, rec
        assert rec.get("as_of") == im.CURATION_AS_OF, rec


@pytest.mark.unit
def test_trace_and_fees_records_carry_data_source_and_as_of() -> None:
    trace = im.trace_pension_dollars("calpers")
    assert trace["data_source"] == im.DATA_SOURCE
    assert trace["as_of"] == im.CURATION_AS_OF
    assert trace["allocations"]
    for alloc in trace["allocations"]:
        assert alloc["data_source"] == im.DATA_SOURCE
        assert alloc["as_of"] == im.CURATION_AS_OF

    fees = im.get_fee_extraction_estimate("apollo")
    assert fees["data_source"] == im.DATA_SOURCE
    assert fees["as_of"] == im.CURATION_AS_OF

    summary = im.get_institutional_summary()
    assert summary["data_source"] == im.DATA_SOURCE
    assert summary["as_of"] == im.CURATION_AS_OF


@pytest.mark.unit
def test_invented_2026_headlines_live_only_under_curated_notes() -> None:
    summary = im.get_institutional_summary()

    assert "private_credit_crisis_2026" not in summary
    notes = summary["curated_notes"]
    assert notes["label"] == "hand-entered, unverified"
    assert notes["data_source"] == im.DATA_SOURCE
    assert notes["as_of"] == im.CURATION_AS_OF
    assert "fortune_headline" in notes["private_credit_crisis_2026"]


# ── Criterion 5: null projections + echoed assumptions ────────────────────


@pytest.mark.unit
def test_the_eight_percent_multiplier_is_gone_from_the_arithmetic() -> None:
    """The assumed return may only appear as a declared assumption."""
    assert "* 0.08" not in _src()
    assert "ASSUMED_GROSS_RETURN_PCT = 8.0" in _src()


@pytest.mark.unit
def test_trace_returns_null_return_dependent_projections() -> None:
    trace = im.trace_pension_dollars("calpers")

    assert trace["total_est_annual_fees"] is None
    assert trace["assumptions"]["gross_return_pct"] == 8.0
    assert trace["assumptions"]["source"] == "assumed"

    for alloc in trace["allocations"]:
        fee = alloc["fee_structure"]
        assert fee["est_annual_perf_fee"] is None
        assert fee["est_annual_total_fees"] is None
        assert fee["assumptions"]["gross_return_pct"] == 8.0
        assert fee["assumptions"]["source"] == "assumed"
        # The management leg needs no return assumption and survives.
        assert fee["est_annual_mgmt_fee"] > 0


@pytest.mark.unit
def test_fees_returns_null_projections_with_assumptions() -> None:
    fees = im.get_fee_extraction_estimate("apollo")

    assert fees["annual_estimates"]["performance_fees"] is None
    assert fees["annual_estimates"]["passthrough_fees"] is None
    assert fees["annual_estimates"]["total_annual_extraction"] is None
    assert fees["ten_year_estimates"]["performance_fees"] is None
    assert fees["ten_year_estimates"]["total_extraction"] is None
    assert fees["assumptions"]["gross_return_pct"] == 8.0
    assert fees["assumptions"]["source"] == "assumed"


@pytest.mark.unit
def test_graph_fee_summary_nulls_the_performance_leg() -> None:
    summary = im.build_institutional_graph(None)["fee_summary"]

    assert summary["est_annual_performance_fees"] is None
    assert summary["est_annual_total_fee_extraction"] is None
    assert summary["assumptions"]["source"] == "assumed"


@pytest.mark.unit
def test_fund_manager_actors_are_labelled_curated() -> None:
    actors = im.get_all_fund_managers()
    assert actors
    for a in actors:
        assert a["credibility"] == "curated_estimate"
        assert a["data_source"] == im.DATA_SOURCE
        assert a["as_of"] == im.CURATION_AS_OF


@pytest.mark.unit
def test_unknown_pension_and_fund_still_answer_honestly() -> None:
    assert "error" in im.get_fee_extraction_estimate("no_such_fund")
    missing = im.trace_pension_dollars("no_such_pension")
    assert isinstance(missing, list) and "error" in missing[0]
