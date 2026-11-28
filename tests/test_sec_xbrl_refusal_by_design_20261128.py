"""SEC_XBRL refusals the duration gate makes on purpose are not run failures (2026-11-28).

After #1494 the scheduler's SEC_XBRL_Financials runs were PARTIAL only because of
issuers the gate refuses by design: a start-less point at a reporting-period end
(WOLF, EE, MT, IBKR, POR) or a flow filed before its end (NTR, ASC). The 11-27
20:16Z run: 1 of 200 items, IBKR. The owner (11-28): "count refusals separately";
a run with only by-design refusals is not partial, real failures stay visible.

The gate names every refused entry with its shape; only the two by-design shapes,
with the evidence in hand, become a NO_DATA item with reason REFUSED_BY_DESIGN.
Everything else (no EDGAR evidence, an implausible report date, a missing current
period, a start-less forward entry, any other admission failure) stays FAILED.
"""
from datetime import date, datetime, timedelta, timezone
import json
from unittest.mock import MagicMock

import pytest

from ingestion import smart_scheduler as sched
from ingestion.altdata import sec_xbrl_financials as sec
from ingestion.altdata.sec_financial_snapshots import (
    DURATION_REFUSAL_MESSAGE, FlowDurationRefusal, prepare_snapshot,
)
from store import sec_financial_snapshots as store

AT = datetime(2026, 11, 7, 10, tzinfo=timezone.utc)
OWN = '0001045810-26-000075'
RD = {OWN: date(2026, 7, 26)}


def payload():
    return dict(cik=1045810, entityName='NVIDIA CORP', facts={'us-gaap': {'Revenues': {'units': {'USD': [
        dict(start='2026-04-27', end='2026-07-26', val=96_221_000_000, accn=OWN, filed='2026-08-26',
             form='10-Q', fp='Q2')]}}}})


def entry(end, *, start=None, filed='2026-08-26', accn=OWN):
    fact = dict(end=end, val=1_000, accn=accn, filed=filed, form='10-Q', fp='Q2')
    if start:
        fact['start'] = start
    return {'units': {'USD': [fact]}}


def refusal(data, report_dates=None) -> FlowDurationRefusal:
    with pytest.raises(FlowDurationRefusal) as info:
        prepare_snapshot(source_body=json.dumps(data).encode(), actor_id='NVDA', cik='0001045810',
                         request_started_at=AT, received_at=AT + timedelta(seconds=2),
                         fx_rates={'USD': 1.0}, report_dates=report_dates)
    assert str(info.value) == DURATION_REFUSAL_MESSAGE      # callers matching the text are unchanged
    assert isinstance(info.value, ValueError)
    return info.value


def with_tags(**tags):
    data = payload()
    data['facts']['us-gaap'].update(tags)
    return data


# ── the gate names each refused entry's shape ────────────────────────────────

def test_a_point_at_the_report_date_with_evidence_is_a_by_design_refusal():
    r = refusal(with_tags(PaymentsOfDividends=entry('2026-07-26')), RD)
    assert r.entries == (('period_end_point', 'us-gaap', 'PaymentsOfDividends', '2026-07-26', OWN),)
    assert r.by_design


def test_a_point_at_another_qualifying_period_end_is_a_by_design_refusal():
    data = with_tags(PaymentsOfDividends=entry('2025-07-27'))
    data['facts']['us-gaap']['Revenues']['units']['USD'].append(dict(
        start='2025-04-28', end='2025-07-27', val=1, accn='0001045810-25-000070', filed='2025-08-27',
        form='10-Q', fp='Q2'))
    r = refusal(data, RD)
    assert r.shapes == {'period_end_point'} and r.by_design


def test_a_forward_dated_flow_with_a_start_is_a_by_design_refusal_even_without_evidence():
    data = with_tags(PaymentsForRepurchaseOfCommonStock=entry('2026-12-31', start='2026-01-01'))
    assert refusal(data).shapes == {'forward_dated'} and refusal(data).by_design
    assert refusal(data, RD).by_design


@pytest.mark.parametrize('name, data, report_dates, shape', [
    ('a point with no EDGAR reportDate', with_tags(PaymentsOfDividends=entry('2026-07-26')), None,
     'point_without_evidence'),
    ('evidence for another accession only', with_tags(PaymentsOfDividends=entry('2026-07-26')),
     {'0001045810-26-000099': date(2026, 7, 26)}, 'point_without_evidence'),
    ('an implausible reportDate', with_tags(PaymentsOfDividends=entry('2026-07-26')),
     {OWN: date(2026, 3, 1)}, 'unexplained_point'),
    ('a start-less entry filed before its end', with_tags(PaymentsOfDividends=entry('2026-12-31')), RD,
     'forward_dated_point'),
])
def test_refusals_the_gate_cannot_explain_are_not_by_design(name, data, report_dates, shape):
    r = refusal(data, report_dates)
    assert r.shapes == {shape} and not r.by_design


def test_a_missing_current_period_is_not_by_design():
    """The report date is established, but the accession has no qualifying flow ending on it."""
    data = {'cik': 1045810, 'entityName': 'NVIDIA CORP', 'facts': {'us-gaap': {'Revenues': {'units': {'USD': [
        dict(start='2026-01-27', end='2026-04-26', val=1, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2'),
        dict(end='2026-07-26', val=96_221_000_000, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2')]}}}}}
    r = refusal(data, RD)
    assert r.shapes == {'unexplained_point'} and not r.by_design


def test_every_refused_entry_is_named_and_one_unexplained_entry_makes_the_issuer_a_failure():
    data = with_tags(PaymentsOfDividends=entry('2026-07-26'),
                     PaymentsForRepurchaseOfCommonStock=entry('2026-12-31', start='2026-01-01'))
    assert refusal(data, RD).shapes == {'period_end_point', 'forward_dated'} and refusal(data, RD).by_design
    r = refusal(data)                                       # without evidence the point is unexplained
    assert r.shapes == {'point_without_evidence', 'forward_dated'} and not r.by_design


def test_tolerated_points_and_legacy_entries_are_unchanged():
    s = prepare_snapshot(source_body=json.dumps(with_tags(PaymentsOfDividends=entry('2026-06-15'))).encode(),
                         actor_id='NVDA', cik='0001045810', request_started_at=AT,
                         received_at=AT + timedelta(seconds=2), fx_rates={'USD': 1.0}, report_dates=RD)
    assert s.tolerated_stray_points == (('PaymentsOfDividends', '2026-06-15', OWN),)


# ── the puller reports a by-design refusal as its own checked item ───────────

def body(data):
    now = datetime.now(timezone.utc)
    return json.dumps(data).encode(), now, now


def good():
    return dict(cik=320193, entityName='APPLE INC', facts={'us-gaap': {'Revenues': {'units': {'USD': [
        dict(start='2026-06-29', end='2026-09-27', val=5, accn='0000320193-26-000100', filed='2026-10-30',
             form='10-K', fp='FY')]}}}})


def refused_by_design():
    # A flow with a start, filed before its end: refused whatever EDGAR says.
    return with_tags(PaymentsForRepurchaseOfCommonStock=entry('2027-12-31', start='2027-01-01'))


def refused_unexplained():
    return with_tags(PaymentsOfDividends=entry('2026-07-26'))


@pytest.fixture
def run(monkeypatch):
    monkeypatch.setattr(sec, '_sector_map_tickers', lambda: ['NVDA', 'AAPL'])
    monkeypatch.setattr(sec, '_load_fx_rates', lambda _: {'USD': 1})
    monkeypatch.setattr(sec, '_fetch_ticker_to_cik_map', lambda: {'NVDA': '0001045810', 'AAPL': '0000320193'})
    monkeypatch.setattr(sec, '_fetch_report_dates', lambda cik: None)
    monkeypatch.setattr(sec, '_load_checkpoint', lambda: {})
    monkeypatch.setattr(sec.time, 'sleep', lambda _: None)
    checkpoints = []
    monkeypatch.setattr(sec, '_save_checkpoint', lambda *a, **kw: checkpoints.append(a))
    write = MagicMock(return_value=dict(status='applied', rows_written=1, snapshot_id='synthetic', rows_removed=0))
    monkeypatch.setattr(store, 'write_snapshot', write)

    def go(nvda):
        monkeypatch.setattr(sec, '_fetch_company_facts_response',
                            lambda cik: body(nvda() if cik == '0001045810' else good()))
        return sec.SECXBRLFinancialsPuller(MagicMock()).pull_all()
    return go, write, checkpoints


def test_a_by_design_refusal_is_reported_separately_and_the_run_is_not_partial(run):
    go, write, checkpoints = run
    out = go(refused_by_design)
    nvda = next(r for r in out if r.get('ticker') == 'NVDA')
    assert (nvda['status'], nvda['reason'], nvda['rows']) == ('NO_DATA', 'REFUSED_BY_DESIGN', 0)
    assert nvda['refusal_shapes'] == ['forward_dated'] and 'error' not in nvda
    assert nvda['refused_entries'][0][:3] == ['forward_dated', 'us-gaap', 'PaymentsForRepurchaseOfCommonStock']
    assert write.call_count == 1                            # only AAPL was written; NVDA's projection retained
    assert checkpoints[-1][0] == ['AAPL']                   # never checkpointed complete
    assert 'NVDA' in checkpoints[-1][3]
    summary = out[-1]
    assert summary['refused_by_design'] == 1 and summary['refused_by_design_tickers'] == ['NVDA']
    assert sched._classify_outcome(out) == (sched.OUTCOME_SUCCESS, 1, None)


def test_a_run_with_only_by_design_refusals_reads_as_checked_not_failed(run, monkeypatch):
    go, write, _ = run
    monkeypatch.setattr(sec, '_sector_map_tickers', lambda: ['NVDA'])
    out = go(refused_by_design)
    assert sched._classify_outcome(out)[0] == sched.OUTCOME_NO_NEW_DATA
    write.assert_not_called()


def test_an_unexplained_refusal_stays_a_failure_and_the_run_partial(run):
    go, write, _ = run
    out = go(refused_unexplained)
    nvda = next(r for r in out if r.get('ticker') == 'NVDA')
    assert nvda['status'] == 'FAILED' and nvda['error'] == DURATION_REFUSAL_MESSAGE
    assert nvda['refusal_shapes'] == ['point_without_evidence']
    assert out[-1]['refused_by_design'] == 0
    outcome, rows, note = sched._classify_outcome(out)
    assert outcome == sched.OUTCOME_PARTIAL and rows == 1 and DURATION_REFUSAL_MESSAGE in note


def test_other_admission_failures_are_unchanged(run, monkeypatch):
    go, write, _ = run
    write.side_effect = lambda engine, snap: (_ for _ in ()).throw(RuntimeError('synthetic'))
    out = go(refused_by_design)
    assert [r['status'] for r in out if r.get('ticker') == 'AAPL'] == ['FAILED']
