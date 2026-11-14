"""Offline SEC period regressions. No provider or database access."""
from __future__ import annotations

from datetime import date, timedelta
from itertools import permutations
from unittest.mock import MagicMock

import pytest

from ingestion.altdata import sec_xbrl_financials as sec

REVENUE = "RevenueFromContractWithCustomerExcludingAssessedTax"
ACCESSION = "0001045810-26-000075"


def fact(value=96_221_000_000, **overrides):
    # Hand-transcribed regression shape from NVDA's July 26, 2026 10-Q.
    # Not an archived Company Facts response; no provider fetch in this suite.
    result = dict(start="2026-04-27", end="2026-07-26", val=value,
                  accn=ACCESSION, filed="2026-08-26", form="10-Q", fp="Q2")
    result.update(overrides)
    return result


def payload(tags, taxonomy="us-gaap", currency="USD"):
    return {"facts": {taxonomy: {tag: {"units": {currency: entries}}
                                  for tag, entries in tags.items()}}}


def revenue(entries):
    return sec._collect_simple_flow(payload({REVENUE: entries}), "revenue", "in",
                                    [REVENUE], [], {"USD": 1.0})


def test_nvda_quarter_beats_six_month_total_in_either_order():
    quarter = fact()
    ytd = fact(177_837_000_000, start="2026-01-26")
    for entries in permutations([quarter, ytd]):
        rows = revenue(list(entries))
        assert len(rows) == 1
        assert rows[0]["amount_usd"] == 96_221_000_000
        assert rows[0]["period_type"] == "quarter"


@pytest.mark.parametrize("start", ["2026-01-26", "2025-10-27"])
def test_ytd_only_does_not_become_a_quarter(start):
    assert revenue([fact(start=start)]) == []


def test_annual_filing_does_not_turn_quarter_comparative_into_annual():
    rows = revenue([fact(form="10-K", fp="FY")])
    assert len(rows) == 1
    assert rows[0]["period_type"] == "quarter"


def test_annual_context_in_interim_filing_is_not_a_quarter():
    rows = revenue([fact(start="2025-07-28", form="6-K", fp="")])
    assert rows[0]["period_type"] == "annual"


@pytest.mark.parametrize("days,expected", [(80, "quarter"), (91, "quarter"),
    (98, "quarter"), (100, "quarter"), (350, "annual"), (364, "annual"),
    (366, "annual"), (371, "annual"), (380, "annual"),
    (79, None), (101, None), (181, None), (273, None), (349, None), (381, None)])
def test_duration_boundaries(days, expected):
    end = date(2026, 7, 26)
    rows = revenue([fact(start=(end - timedelta(days=days-1)).isoformat())])
    assert [r["period_type"] for r in rows] == ([expected] if expected else [])


@pytest.mark.parametrize("overrides", [
    {"start": None}, {"start": "bad"}, {"start": "2026-08-01"},
    {"start": "2026-07-26"}, {"end": "bad"}, {"end": None},
    {"filed": None}, {"filed": "bad"}, {"filed": "2026-07-25"},
    {"accn": None}, {"accn": "bad"}, {"form": "8-K"},
    {"val": float("nan")}, {"val": float("inf")}, {"val": True},
])
def test_missing_or_invalid_context_is_not_guessed(overrides):
    assert revenue([fact(**overrides)]) == []


def test_source_contains_exact_period_and_filing_provenance():
    row = revenue([fact()])[0]
    assert row["period_start"] == date(2026, 4, 27)
    assert row["fiscal_period"] == date(2026, 7, 26)
    assert row["filed_date"] == date(2026, 8, 26)
    assert row["accession"] == ACCESSION
    assert row["taxonomy"] == "us-gaap"
    source = row["source_filing"]
    assert source.startswith("10-Q [SEC-XBRL-PERIOD-V1] filing_fp=Q2 ")
    for token in ["period=2026-04-27/2026-07-26", "filed=2026-08-26",
                  f"accn={ACCESSION}", f"tag=us-gaap:{REVENUE}"]:
        assert token in source


def test_same_rank_conflicts_are_omitted_independent_of_input_order():
    for entries in permutations([fact(10), fact(20)]):
        assert revenue(list(entries)) == []


def test_exact_duplicate_contexts_collapse():
    assert len(revenue([fact(), fact()])) == 1


def test_latest_filing_wins_and_keeps_its_provenance():
    revised = fact(100_000_000_000, filed="2026-09-01", form="10-Q/A",
                   accn="0001045810-26-000076")
    for entries in permutations([fact(), revised]):
        rows = revenue(list(entries))
        assert len(rows) == 1
        assert rows[0]["amount_usd"] == 100_000_000_000
        assert rows[0]["accession"] == "0001045810-26-000076"


def test_ifrs_semiannual_is_omitted_but_actual_quarter_is_kept():
    data = payload({"Revenue": [fact(100, start="2026-01-26", form="6-K"),
                                fact(55, form="6-K")]}, "ifrs-full", "EUR")
    rows = sec._collect_simple_flow(data, "revenue", "in", [], ["Revenue"], {"EUR": 1.1})
    assert len(rows) == 1
    assert rows[0]["amount_usd"] == pytest.approx(60.5)
    assert rows[0]["taxonomy"] == "ifrs-full"


def test_all_simple_flows_apply_the_same_period_gate():
    tags = {spec["tags"][-1]: [fact(100, start="2026-01-26"), fact(40)]
            for spec in sec._FLOW_MAPPINGS}
    rows = sec._collect_all_flows(payload(tags), {"USD": 1})
    assert len(rows) == len(sec._FLOW_MAPPINGS)
    assert all(row["amount_usd"] == 40 for row in rows)


def debt(proceeds, repayments):
    return sec._collect_net_debt_issuance(payload({
        sec._DEBT_PROCEEDS_TAG: proceeds, sec._DEBT_REPAY_TAG: repayments}), {"USD": 1})


def test_debt_pairs_only_exact_period_and_accession():
    rows = debt([fact(100)], [fact(10, start="2026-01-26"), fact(30)])
    assert len(rows) == 1
    assert rows[0]["amount_usd"] == 70
    assert rows[0]["period_start"] == date(2026, 4, 27)
    assert ACCESSION in rows[0]["source_filing"]


@pytest.mark.parametrize("repayment", [None, fact(30, start="2026-01-26"),
    fact(30, accn="0001045810-26-000076"), fact(30, start="2026-04-26"),
    fact(30, filed="2026-08-27")])
def test_debt_missing_or_mismatched_repayment_is_unknown_not_zero(repayment):
    assert debt([fact(100)], [] if repayment is None else [repayment]) == []


def test_debt_explicit_zero_repayment_is_valid():
    assert debt([fact(100)], [fact(0)])[0]["amount_usd"] == 100


def test_conflicting_repayments_do_not_depend_on_input_order():
    for repayments in permutations([fact(20), fact(30)]):
        assert debt([fact(100)], list(repayments)) == []


def test_debt_does_not_subtract_different_currencies():
    data = payload({sec._DEBT_PROCEEDS_TAG: [fact(100)]})
    data["facts"]["us-gaap"][sec._DEBT_REPAY_TAG] = {"units": {"EUR": [fact(20)]}}
    assert sec._collect_net_debt_issuance(data, {"USD": 1, "EUR": 1.1}) == []


def test_writer_keeps_period_provenance_in_existing_source_column():
    engine = MagicMock()
    row = revenue([fact()])[0]
    assert sec._write_rows(engine, "nvda", [row]) == 1
    calls = engine.begin.return_value.__enter__.return_value.execute.call_args_list
    inserted = next(call.args[1] for call in calls if "INSERT INTO" in str(call.args[0]))
    assert "period=2026-04-27/2026-07-26" in inserted["source_filing"]
    assert f"accn={ACCESSION}" in inserted["source_filing"]
    assert inserted["amount_usd"] == 96_221_000_000


def test_debt_conflicting_proceeds_cannot_hide_a_nonpositive_net():
    for proceeds in permutations([fact(100), fact(10)]):
        assert debt(list(proceeds), [fact(30)]) == []


@pytest.mark.parametrize('net', [0, -20])
def test_newer_nonpositive_debt_restatement_supersedes_positive_net(net):
    revision = dict(filed='2026-09-01', form='10-Q/A', accn='0001045810-26-000076')
    proceeds = [fact(100), fact(30 + net, **revision)]
    repayments = [fact(30), fact(30, **revision)]
    for ordered in permutations(proceeds):
        assert debt(list(ordered), repayments) == []


@pytest.fixture
def writer_engine():
    # Execute the real writer SQL in isolated SQLite, registering the two
    # PostgreSQL scalar functions it needs. This is not PG integration proof.
    from sqlalchemy import create_engine, event, text
    engine = create_engine('sqlite://')

    @event.listens_for(engine, 'connect')
    def functions(connection, record):
        connection.create_function('NOW', 0, lambda: '2026-11-07T00:00:00')
        connection.create_function('split_part', 3,
            lambda value, sep, n: value.split(sep)[n-1] if len(value.split(sep)) >= n else '')

    with engine.begin() as conn:
        conn.execute(text('''CREATE TABLE capital_flows (
            actor_id TEXT, fiscal_period DATE, period_type TEXT, flow_type TEXT,
            direction TEXT, amount_usd NUMERIC, counterparty_id TEXT,
            source_filing TEXT, confidence TEXT, currency TEXT, as_of TEXT)'''))
    yield engine
    engine.dispose()


def stored(writer_engine):
    from sqlalchemy import text
    with writer_engine.connect() as conn:
        return list(conn.execute(text('SELECT actor_id, amount_usd, source_filing FROM capital_flows ORDER BY actor_id, amount_usd')))


def test_writer_replaces_amended_validated_source_without_adding_a_period(writer_engine):
    original = revenue([fact(100)])
    revision = revenue([fact(90, form='10-Q/A', filed='2026-09-01',
                             accn='0001045810-26-000076')])
    sec._write_rows(writer_engine, 'nvda', original)
    sec._write_rows(writer_engine, 'nvda', revision)
    sec._write_rows(writer_engine, 'nvda', revision)
    rows = stored(writer_engine)
    assert len(rows) == 1
    assert rows[0][1] == 90
    assert '0001045810-26-000076' in rows[0][2]


def test_writer_preserves_other_keys_and_unmarked_legacy_rows(writer_engine):
    row = revenue([fact(100)])[0]
    sec._write_rows(writer_engine, 'other', [row])
    for override in [dict(source_filing='10-Q Q2'),
                     dict(source_filing='10-Q unrelated [SEC-XBRL-PERIOD-V1]'),
                     dict(source_filing='seed [SEC-XBRL-PERIOD-V1]'),
                     dict(period_type='annual'), dict(flow_type='tax'),
                     dict(fiscal_period=date(2026, 4, 26))]:
        sec._write_rows(writer_engine, 'nvda', [{**row, **override}])
    sec._write_rows(writer_engine, 'nvda', [row])
    sec._write_rows(writer_engine, 'nvda', [{**row, 'amount_usd': 90}])
    rows = stored(writer_engine)
    assert len(rows) == 8
    assert sum(r[1] == 90 for r in rows) == 1


def test_empty_snapshot_does_not_claim_to_reconcile_previously_stored_rows(writer_engine):
    # Document the rollout gate: filtering prevents new bad rows; it does not
    # withdraw old facts absent from today's normalized output or rebuild TTM.
    sec._write_rows(writer_engine, 'nvda', revenue([fact(100)]))
    assert sec._write_rows(writer_engine, 'nvda', []) == 0
    assert len(stored(writer_engine)) == 1


@pytest.mark.parametrize('repayments', [[], [20, 40]])
def test_latest_unknown_or_conflicting_debt_blocks_older_positive(repayments):
    revision = dict(filed='2026-09-01', form='10-Q/A', accn='0001045810-26-000076')
    proceeds = [fact(100), fact(100, **revision)]
    repaid = [fact(30)] + [fact(amount, **revision) for amount in repayments]
    assert debt(proceeds, repaid) == []


def test_nonpositive_gaap_debt_is_not_replaced_with_ifrs_fallback():
    data = payload({sec._DEBT_PROCEEDS_TAG: [fact(10)],
                    sec._DEBT_REPAY_TAG: [fact(30)]})
    data['facts'].update(payload({sec._IFRS_DEBT_PROCEEDS_TAG: [fact(100)],
                                 sec._IFRS_DEBT_REPAY_TAG: [fact(30)]}, 'ifrs-full')['facts'])
    assert sec._collect_net_debt_issuance(data, {'USD': 1}) == []


@pytest.mark.parametrize('value', [None, float('nan'), float('inf'), True])
def test_invalid_latest_amount_cannot_resurrect_an_older_fact(value):
    revision = dict(filed='2026-09-01', form='10-Q/A', accn='0001045810-26-000076')
    assert revenue([fact(100), fact(value, **revision)]) == []
    assert debt([fact(100), fact(value, **revision)], [fact(30), fact(30, **revision)]) == []


def test_repayment_only_amendment_cannot_resurrect_old_net():
    revision = dict(filed='2026-09-01', form='10-Q/A', accn='0001045810-26-000076')
    assert debt([fact(100)], [fact(30), fact(120, **revision)]) == []


@pytest.mark.parametrize('value', [None, float('nan'), float('inf'), True])
def test_invalid_repayment_duplicate_blocks_same_context(value):
    for repayments in permutations([fact(30), fact(value)]):
        assert debt([fact(100)], list(repayments)) == []
