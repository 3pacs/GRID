"""Offline source-capture admission and reconciliation-plan tests."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json

import pytest

from ingestion.altdata.sec_financial_snapshots import (
    prepare_snapshot, validate_snapshot, build_reconciliation_manifest, _json,
)

AT = datetime(2026, 11, 7, 10, tzinfo=timezone.utc)


def payload(value=96_221_000_000, **extra):
    fact = dict(start='2026-04-27', end='2026-07-26', val=value,
                accn='0001045810-26-000075', filed='2026-08-26', form='10-Q', fp='Q2')
    fact.update(extra)
    return dict(cik=1045810, entityName='NVIDIA CORP',
                facts={'us-gaap': {'Revenues': {'units': {'USD': [fact]}}}})


def prepared(data=None, **overrides):
    args = dict(source_body=json.dumps(data if data is not None else payload()).encode(),
                actor_id='NVDA', cik='0001045810', request_started_at=AT,
                received_at=AT + timedelta(seconds=2), fx_rates={'USD': 1.0})
    args.update(overrides)
    return prepare_snapshot(**args)


def test_capture_identity_preserves_original_bytes_and_excludes_repeated_receipt_time():
    s = prepared()
    assert s.source_sha256 == hashlib.sha256(s.source_body).hexdigest()
    assert s.rows[0]['amount_usd'] == 96_221_000_000
    assert s.actor_id == 'nvda'
    assert prepared(request_started_at=AT+timedelta(hours=1),
                    received_at=AT+timedelta(hours=1, seconds=2)).snapshot_id == s.snapshot_id
    validate_snapshot(s)


def test_capture_id_binds_exact_source_bytes_and_normalized_values():
    s = prepared()
    assert prepared(source_body=json.dumps(payload(), indent=2).encode()).snapshot_id != s.snapshot_id
    assert prepared(payload(1)).snapshot_id != s.snapshot_id


def test_usd_only_capture_identity_ignores_moving_fx_rates():
    first = prepared(fx_rates={'USD': 1.0, 'EUR': 1.1, 'JPY': 0.007})
    later = prepared(fx_rates={'USD': 1.0, 'EUR': 1.2, 'JPY': 0.008},
                     request_started_at=AT+timedelta(days=1),
                     received_at=AT+timedelta(days=1, seconds=2))
    assert later.snapshot_id == first.snapshot_id == prepared().snapshot_id
    assert later.normalized_json == first.normalized_json
    assert first.fx_json == later.fx_json == '{}'
    validate_snapshot(later)


def test_foreign_capture_identity_binds_only_applied_fx_rates():
    data = payload(100)
    units = data['facts']['us-gaap']['Revenues']['units']
    units['EUR'] = units.pop('USD')
    first = prepared(data, fx_rates={'EUR': 1.1, 'JPY': 0.007})
    unused_changed = prepared(data, fx_rates={'EUR': 1.1, 'JPY': 0.008})
    applied_changed = prepared(data, fx_rates={'EUR': 1.2, 'JPY': 0.007})
    assert json.loads(first.fx_json) == {'EUR': 1.1}
    assert unused_changed.snapshot_id == first.snapshot_id
    assert applied_changed.snapshot_id != first.snapshot_id
    assert applied_changed.rows[0]['amount_usd'] == 120
    validate_snapshot(first)
    validate_snapshot(applied_changed)


def test_foreign_fallback_preserves_confidence_and_replays_without_unused_inputs():
    data = payload(100)
    units = data['facts']['us-gaap']['Revenues']['units']
    units['EUR'] = units.pop('USD')
    first = prepared(data, fx_rates={'EUR': 0, 'JPY': 0.007})
    assert first.fx_json == '{}'
    assert first.snapshot_id == prepared(data, fx_rates={}).snapshot_id
    assert first.rows[0]['confidence'] == 'estimated'
    validate_snapshot(first)


def test_applied_overflow_rate_is_retained_for_write_boundary_replay():
    data = payload(1e100)
    units = data['facts']['us-gaap']['Revenues']['units']
    units['EUR'] = units.pop('USD')
    capture = prepared(data, fx_rates={'EUR': 1e300, 'JPY': 0.007})
    assert capture.rows == []
    assert json.loads(capture.fx_json) == {'EUR': 1e300}
    validate_snapshot(capture)


@pytest.mark.parametrize('changes', [dict(snapshot_id='0'*64), dict(actor_id='aapl'),
    dict(normalized_json='[]'), dict(source_url='https://example.com'),
    dict(normalizer_version='wrong'), dict(source_sha256='0'*64)])
def test_write_boundary_rejects_tampered_artifact(changes):
    with pytest.raises(ValueError):
        validate_snapshot(replace(prepared(), **changes))


@pytest.mark.parametrize('data', [dict(cik=1045810, entityName='NVDA', facts={}),
    dict(cik=1045810, entityName='NVDA', facts={'us-gaap': {'Revenues': {'units': {}}}}),
    dict(cik=1045810, entityName='NVDA', facts={'us-gaap': {'Revenues': {'units': {'USD': []}}}}),
    dict(cik=1045810, entityName='NVDA', facts={'us-gaap': {'Revenues': {'units': {'USD': [{}]}}}}),
    dict(cik=320193, entityName='NVDA', facts=payload()['facts']),
    dict(cik=1045810, facts=payload()['facts']), {}, [], None])
def test_malformed_envelopes_are_not_qualified_empty_snapshots(data):
    with pytest.raises((ValueError, TypeError)):
        prepared(source_body=json.dumps(data).encode())


@pytest.mark.parametrize('field,value', [('val', None), ('val', True), ('val', '99'),
    ('val', float('nan')), ('val', float('inf')), ('end', 'bad'), ('filed', None),
    ('filed', '2026-01-01'), ('start', '2027-01-01'), ('accn', None), ('form', '')])
def test_bad_entries_never_admit_destructive_empty_projection(field, value):
    data=payload()
    data['facts']['us-gaap']['Revenues']['units']['USD'][0][field]=value
    with pytest.raises((ValueError, TypeError)):
        prepared(data)


def test_valid_ytd_only_is_a_qualified_empty_projection_not_zero():
    s=prepared(payload(177_837_000_000, start='2026-01-26'))
    assert s.rows == []
    assert json.loads(s.source_body)['facts']['us-gaap']['Revenues']['units']['USD'][0]['val'] == 177_837_000_000


def test_instant_fact_is_structurally_valid_but_not_a_flow():
    data=payload()
    del data['facts']['us-gaap']['Revenues']['units']['USD'][0]['start']
    data['facts']['us-gaap']['Assets'] = data['facts']['us-gaap'].pop('Revenues')
    assert prepared(data).rows == []


def test_duplicate_json_keys_are_rejected():
    with pytest.raises(ValueError, match='Duplicate'):
        prepared(source_body=b'{"cik":1045810,"cik":320193}')


@pytest.mark.parametrize('changes', [dict(request_started_at=AT.replace(tzinfo=None)),
    dict(received_at=AT-timedelta(seconds=1)), dict(cik='no'), dict(actor_id='../nvda'),
    dict(source_body=b'')])
def test_capture_envelope_bounds(changes):
    with pytest.raises(ValueError):
        prepared(**changes)


def legacy(**changes):
    row=dict(id=42, actor_id='nvda', fiscal_period='2026-07-26', period_type='quarter',
             flow_type='revenue', amount_usd=Decimal('177837000000.00'),
             counterparty_id=None, source_filing='10-Q Q2', currency='USD')
    row.update(changes)
    return row


def test_reconciliation_is_lossless_read_only_review_proposal():
    old=legacy()
    before=deepcopy(old)
    report=build_reconciliation_manifest(prepared(), [old])
    assert old == before
    assert report['mode'] == 'proposal_only_no_execution'
    row=report['entries'][0]
    assert row['disposition'] == 'review_value_difference'
    assert row['proposed_current']['amount_usd'] == 96_221_000_000
    assert 'accession' not in row['before']
    assert json.loads(_json(report))['entries'][0]['before']['amount_usd'] == '177837000000.00'
    assert row['expected_row_sha256'] == hashlib.sha256(_json(old).encode()).hexdigest()


@pytest.mark.parametrize('override,expected', [
    (dict(counterparty_id='supplier'), 'outside_scope_counterparty'),
    (dict(period_type='ttm'), 'outside_scope_period_type'),
    (dict(source_filing='seed'), 'outside_scope_source'),
    (dict(source_filing='10-Q [SEC-XBRL-PERIOD-V1] old'), 'already_versioned_review_separately'),
    (dict(flow_type='capex'), 'review_unavailable'),
    (dict(amount_usd=Decimal('96221000000')), 'review_provenance_only')])
def test_manifest_dispositions_do_not_repair_unrelated_rows(override, expected):
    assert build_reconciliation_manifest(prepared(),[legacy(**override)])['entries'][0]['disposition']==expected


@pytest.mark.parametrize('rows', [[legacy(id=None)], [legacy(id=True)], [legacy(actor_id='aapl')],
                                  [legacy(), legacy()]])
def test_manifest_requires_exact_unique_positive_row_identity(rows):
    with pytest.raises(ValueError):
        build_reconciliation_manifest(prepared(), rows)


def test_all_preserved_official_captures_pass_structural_admission():
    import gzip
    from pathlib import Path
    root = Path(__file__).parent / 'fixtures/sec_financial_periods'
    for symbol, cik in [('nvda', '1045810'), ('aapl', '320193'), ('tsm', '1046179')]:
        meta=json.loads((root / f'{symbol}.provenance.json').read_text())
        raw=gzip.decompress((root / meta['filename']).read_bytes())
        s=prepared(source_body=raw, actor_id=symbol, cik=cik)
        assert s.source_sha256 == meta['sha256']
        assert s.rows


def test_missing_start_in_mapped_flow_is_not_qualified_empty():
    data=payload()
    del data['facts']['us-gaap']['Revenues']['units']['USD'][0]['start']
    with pytest.raises(ValueError, match='duration'):
        prepared(data)


def test_forward_dated_unmapped_facts_do_not_disqualify_valid_reported_flows():
    data=payload()
    data['facts']['us-gaap']['CommonStockDividendsPerShareDeclared'] = {
        'units': {'USD/shares': [dict(start='2026-08-01', end='2026-10-31', val=0.1,
           filed='2026-08-26', form='10-Q', accn='0001045810-26-000075')]}}
    assert prepared(data).rows[0]['amount_usd'] == 96_221_000_000


def test_reconciliation_cli_preserves_decimal_export_and_refuses_overwrite(tmp_path):
    import gzip
    from pathlib import Path
    from scripts.plan_sec_financial_reconciliation import main
    s=prepared()
    (tmp_path/'facts.gz').write_bytes(gzip.compress(s.source_body,mtime=0))
    receipt=dict(actor_id='nvda',cik=s.cik,source_url=s.source_url,http_status=200,
                 body_file='facts.gz',body_storage='gzip',response_sha256=s.source_sha256,
                 request_started_at=s.request_started_at.isoformat(),received_at=s.received_at.isoformat())
    (tmp_path/'capture.json').write_text(json.dumps(receipt))
    # Genuine JSON numeric text is loaded with Decimal, not float.
    before=_json(dict(rows=[legacy()],row_count=1)).replace('"177837000000.00"','177837000000.00')
    (tmp_path/'before.json').write_text(before)
    args=['--capture',str(tmp_path/'capture.json'),'--before-images',str(tmp_path/'before.json'),
          '--output',str(tmp_path/'proposal.json')]
    assert main(args)==0
    output=json.loads((tmp_path/'proposal.json').read_text())
    assert output['disposition_counts']=={'review_value_difference':1}
    assert output['entries'][0]['before']['amount_usd']=='177837000000.00'
    with pytest.raises(FileExistsError):
        main(args)
    assert not any('sql' in str(p) for p in Path(tmp_path).iterdir())



def test_current_view_migration_covers_every_normalizer_flow():
    from pathlib import Path
    from ingestion.altdata.sec_xbrl_financials import _FLOW_MAPPINGS
    migration = (Path(__file__).resolve().parents[1] / 'migrations/versions/sec_financial_current_20261108.py').read_text(encoding='utf-8')
    for flow in {item['flow_type'] for item in _FLOW_MAPPINGS} | {'debt_issuance'}:
        assert migration.count("'" + flow + "'") == 2, flow


# ── legacy unusable entries do not refuse the whole issuer (2026-11-25) ───────
# 2026-11-23/24: 47 to 60 of each 200-issuer batch were refused "Invalid mapped
# financial-flow duration" because of 2014-2023 mapped-flow entries with no start
# (YUM: a 2020 acquisition payment; SIRI: 2016 dividends). The normalizer drops
# such entries anyway, so the issuer's valid periods must be admitted unchanged.

def _legacy(end='2020-03-18', filed='2021-02-22', **extra):
    fact = dict(end=end, val=1_000, accn='0001041061-21-000012', filed=filed, form='10-K', fp='FY')
    fact.update(extra)                               # no 'start': a point-in-time amount tagged as a flow
    return {'units': {'USD': [fact]}}


def test_legacy_mapped_entry_without_start_does_not_refuse_the_issuer():
    clean = prepared()
    data = payload()
    data['facts']['us-gaap']['PaymentsToAcquireBusinessesNetOfCashAcquired'] = _legacy()
    s = prepared(data)
    assert s.rows == clean.rows and s.rows[0]['amount_usd'] == 96_221_000_000
    assert s.normalized_sha256 == clean.normalized_sha256          # the entry changes nothing the normalizer emits
    assert s.source_sha256 != clean.source_sha256                  # the capture still holds the exact source bytes


def test_legacy_entry_filed_before_its_end_does_not_refuse_the_issuer():
    data = payload()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _legacy(
        end='2014-02-23', filed='2014-02-11', start='2013-11-24')
    assert prepared(data).rows[0]['amount_usd'] == 96_221_000_000


def test_a_current_period_entry_without_start_stays_fatal_even_beside_legacy_ones():
    data = payload()
    data['facts']['us-gaap']['PaymentsToAcquireBusinessesNetOfCashAcquired'] = _legacy()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _legacy(
        end='2026-07-26', filed='2026-08-26', accn='0001045810-26-000075', form='10-Q', fp='Q2')
    with pytest.raises(ValueError, match='duration'):
        prepared(data)


@pytest.mark.parametrize('days_before_capture, refused', [(-30, True), (0, True), (730, True), (731, False), (4000, False)])
def test_legacy_horizon_is_measured_from_the_capture_date(days_before_capture, refused):
    data = payload()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _legacy(
        end=(AT.date() - timedelta(days=days_before_capture)).isoformat(), filed='2026-08-26')
    if refused:
        with pytest.raises(ValueError, match='duration'):
            prepared(data)
    else:
        assert prepared(data).rows


def test_an_unrelated_future_filed_fact_does_not_widen_the_horizon():
    """Codex 6061699121: an Assets fact filed 2035-01-01 moved the old anchor past the
    capture and admitted a current-period dividend without a start."""
    data = payload()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _legacy(
        end='2026-07-26', filed='2026-08-26', accn='0001045810-26-000075', form='10-Q', fp='Q2')
    data['facts']['us-gaap']['Assets'] = {'units': {'USD': [dict(
        end='2035-01-01', val=5, accn='0001045810-35-000001', filed='2035-01-01', form='10-K')]}}
    with pytest.raises(ValueError, match='duration'):
        prepared(data)


def test_tolerated_legacy_entries_are_counted_but_not_part_of_the_identity():
    clean = prepared()
    data = payload()
    data['facts']['us-gaap']['PaymentsToAcquireBusinessesNetOfCashAcquired'] = _legacy()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _legacy(
        end='2014-02-23', filed='2014-02-11', start='2013-11-24')
    s = prepared(data)
    assert s.tolerated_legacy_entries == 2 and clean.tolerated_legacy_entries == 0
    validate_snapshot(s)                                    # replay at the write boundary still matches


# ── event-dated stray points do not refuse the issuer (2026-11-26) ────────────
# INTC, EOG, ON and WLK (and later DHI, RPD, NXE) were refused "Invalid mapped
# financial-flow duration" for ONE or TWO start-less entries each: a one-day debt
# issuance, share issuance or buyback in a notes disclosure, tagged with a flow
# concept but no start. Codex 6081201501 / 6081419580 / 6081574064: nothing inferred
# from Company Facts (other periods in the accession, a balance-sheet date, a
# filing-age window) establishes the filing's report period, so the tolerance needs
# EDGAR's reportDate for the accession, supplied as evidence, plus a qualifying flow
# at that date; without it the entry refuses (fail closed).

from datetime import date as _date                  # noqa: E402
from dataclasses import replace as _replace          # noqa: E402

OWN = '0001045810-26-000075'          # the accession of payload()'s usable Q2 entry (period end 2026-07-26)
RD = {OWN: _date(2026, 7, 26)}        # EDGAR submissions reportDate for that accession


def _stray(end, *, accn=OWN, filed='2026-08-26', **extra):
    return _legacy(end=end, filed=filed, accn=accn, form='10-Q', fp='Q2', **extra)


def _stray_payload(tag='PaymentsOfDividends', end='2026-06-15'):
    data = payload()
    data['facts']['us-gaap'][tag] = _stray(end)
    return data


def test_an_event_dated_stray_point_with_edgar_report_date_does_not_refuse_and_is_traceable():
    clean = prepared()
    s = prepared(_stray_payload(), report_dates=RD)
    assert s.rows == clean.rows and s.normalized_sha256 == clean.normalized_sha256
    assert s.tolerated_stray_points == (('PaymentsOfDividends', '2026-06-15', OWN),)
    assert s.report_dates == ((OWN, '2026-07-26'),)            # the evidence the decision rested on
    assert s.tolerated_legacy_entries == 1 and clean.tolerated_stray_points == () and clean.report_dates == ()
    assert s.snapshot_id != clean.snapshot_id                   # the evidence is part of the identity
    validate_snapshot(s)
    assert s.source_sha256 != clean.source_sha256


def test_without_edgar_report_date_evidence_nothing_is_tolerated():
    with pytest.raises(ValueError, match='duration'):
        prepared(_stray_payload())
    with pytest.raises(ValueError, match='duration'):
        prepared(_stray_payload(), report_dates={'0001045810-26-000099': _date(2026, 7, 26)})   # another accession


def test_ordinary_snapshots_keep_their_identity_when_evidence_is_offered_but_not_used():
    assert prepared(report_dates=RD).snapshot_id == prepared().snapshot_id


def test_a_stray_point_at_the_report_date_stays_fatal():
    data = payload()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _stray('2026-07-26')
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


def test_a_stray_point_at_a_period_end_reported_by_another_accession_stays_fatal():
    data = payload()
    prior = dict(start='2025-04-28', end='2025-07-27', val=1, accn='0001045810-25-000070',
                 filed='2025-08-27', form='10-Q', fp='Q2')
    data['facts']['us-gaap']['Revenues']['units']['USD'].append(prior)
    data['facts']['us-gaap']['PaymentsOfDividends'] = _stray('2025-07-27')       # a comparative period end
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


def test_a_nearer_comparative_with_the_real_report_date_elsewhere_stays_fatal():
    """Codex 6081574064: a 10-Q (reportDate 2026-07-26) whose only qualifying flow is an earlier
    comparative, with a start-less current Revenue. The report date is established by EDGAR, so the
    current period is seen to have no qualifying flow of its own."""
    data = {'cik': 1045810, 'entityName': 'NVIDIA CORP', 'facts': {'us-gaap': {'Revenues': {'units': {'USD': [
        dict(start='2026-01-27', end='2026-04-26', val=1, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2'),
        dict(end='2026-07-26', val=96_221_000_000, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2')]}}}}}
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)
    data['facts']['us-gaap']['Revenues']['units']['USD'].pop()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _stray('2026-06-15')
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


def test_comparative_only_evidence_cannot_hide_a_missing_current_period():
    data = {'cik': 1045810, 'entityName': 'NVIDIA CORP', 'facts': {'us-gaap': {'Revenues': {'units': {'USD': [
        dict(start='2025-04-28', end='2025-07-27', val=1, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2'),
        dict(end='2026-07-26', val=96_221_000_000, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2')]}}}}}
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


@pytest.mark.parametrize('name, form, start, end', [
    ('a two-day 8-K span', '8-K', '2026-07-24', '2026-07-26'),
    ('a nine-month year-to-date duration', '10-Q', '2025-10-27', '2026-07-26'),
    ('a quarter in a form the normalizer does not read', '10-K405', '2026-04-27', '2026-07-26'),
])
def test_a_nonqualifying_duration_is_not_evidence_of_the_current_period(name, form, start, end):
    data = {'cik': 1045810, 'entityName': 'NVIDIA CORP', 'facts': {'us-gaap': {'Revenues': {'units': {'USD': [
        dict(start=start, end=end, val=1, accn=OWN, filed='2026-08-26', form=form, fp='Q2')]}}}}}
    data['facts']['us-gaap']['PaymentsOfDividends'] = _stray('2026-06-15')
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


@pytest.mark.parametrize('age_days, refused', [(0, False), (31, False), (150, False), (151, True), (395, True), (-1, True)])
def test_the_report_date_must_also_be_plausible_for_the_filing(age_days, refused):
    filed = _date(2026, 8, 26)
    r = filed - timedelta(days=age_days)
    data = {'cik': 1045810, 'entityName': 'NVIDIA CORP', 'facts': {'us-gaap': {
        'Revenues': {'units': {'USD': [dict(start=(r - timedelta(days=89)).isoformat(), end=r.isoformat(),
                                            val=96_221_000_000, accn=OWN, filed=filed.isoformat(), form='10-Q', fp='Q2')]}},
        'PaymentsOfDividends': _stray((r - timedelta(days=40)).isoformat())}}}
    if refused:
        with pytest.raises(ValueError, match='duration'):
            prepared(data, report_dates={OWN: r})
    else:
        assert len(prepared(data, report_dates={OWN: r}).tolerated_stray_points) == 1


@pytest.mark.parametrize('bad', [{'not-an-accession': _date(2026, 7, 26)}, {OWN: '2026-07-26'},
                                 {OWN: datetime(2026, 7, 26, tzinfo=timezone.utc)}])
def test_malformed_report_date_evidence_is_rejected(bad):
    with pytest.raises(ValueError, match='report-date evidence'):
        prepared(_stray_payload(), report_dates=bad)


def test_tolerance_metadata_and_evidence_cannot_be_dropped_without_failing_the_replay():
    s = prepared(_stray_payload(), report_dates=RD)
    for tampered in (_replace(s, tolerated_stray_points=()), _replace(s, tolerated_legacy_entries=0),
                     _replace(s, tolerated_stray_points=(('PaymentsOfDividends', '2026-06-16', OWN),)),
                     _replace(s, report_dates=()),
                     _replace(s, report_dates=((OWN, '2026-07-27'),))):
        with pytest.raises(ValueError):
            validate_snapshot(tampered)
    validate_snapshot(s)


def test_a_recent_entry_filed_before_its_end_stays_fatal_even_with_evidence():
    data = payload()
    data['facts']['us-gaap']['PaymentsOfDividends'] = {'units': {'USD': [dict(
        start='2026-06-01', end='2026-09-30', val=5, accn=OWN, filed='2026-08-26', form='10-Q', fp='Q2')]}}
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


def test_one_tolerable_and_one_fatal_entry_still_refuse():
    data = _stray_payload()
    data['facts']['us-gaap']['PaymentsForRepurchaseOfCommonStock'] = _stray('2026-07-26')
    with pytest.raises(ValueError, match='duration'):
        prepared(data, report_dates=RD)


@pytest.mark.parametrize('days_before_capture, tolerated_as', [(10, 'stray'), (730, 'stray'), (731, 'legacy'), (4000, 'legacy')])
def test_the_horizon_still_decides_what_is_legacy(days_before_capture, tolerated_as):
    data = payload()
    end = (AT.date() - timedelta(days=days_before_capture)).isoformat()
    data['facts']['us-gaap']['PaymentsOfDividends'] = _stray(end, filed=AT.date().isoformat())
    # the accession of this entry is OWN, filed on the capture day; its report date is 2026-07-26 (104 days before)
    s = prepared(data, report_dates=RD)
    assert s.tolerated_legacy_entries == 1
    assert (len(s.tolerated_stray_points) == 1) == (tolerated_as == 'stray')


@pytest.mark.parametrize('name, taxonomy, tag, stray_end', [
    ('INTC', 'us-gaap', 'ProceedsFromIssuanceOfCommonStock', '2026-06-29'),
    ('EOG', 'us-gaap', 'ProceedsFromIssuanceOfLongTermDebt', '2026-07-01'),
    ('ON', 'us-gaap', 'PaymentsForRepurchaseOfCommonStock', '2026-05-11'),
    ('WLK', 'us-gaap', 'RepaymentsOfLongTermDebt', '2026-05-29'),
    ('DHI', 'us-gaap', 'PaymentsToAcquireBusinessesNetOfCashAcquired', '2026-07-27'),
    ('RPD', 'us-gaap', 'ProceedsFromIssuanceOfCommonStock', '2026-03-14'),
    ('NXE', 'ifrs-full', 'ProceedsFromIssuingShares', '2026-04-15'),
])
def test_the_refused_issuers_shapes_are_admitted_with_evidence(name, taxonomy, tag, stray_end):
    data = payload()
    data['facts'].setdefault(taxonomy, {})[tag] = _stray(stray_end)
    s = prepared(data, report_dates=RD)
    assert s.rows[0]['amount_usd'] == 96_221_000_000
    assert s.tolerated_stray_points == ((tag, stray_end, OWN),), name
    with pytest.raises(ValueError, match='duration'):
        prepared(data)                                          # and refused without the evidence


def test_the_wolf_and_ee_shapes_stay_refused():
    wolf = payload()
    for tag in ('OperatingExpenses', 'ResearchAndDevelopmentExpense', 'IncomeTaxExpenseBenefit'):
        wolf['facts']['us-gaap'][tag] = _stray('2026-07-26')
    with pytest.raises(ValueError, match='duration'):
        prepared(wolf, report_dates=RD)
    ee = payload()
    ee['facts']['us-gaap']['Revenues']['units']['USD'].append(dict(
        start='2025-04-28', end='2025-07-27', val=1, accn='0001045810-25-000070', filed='2025-08-27', form='10-Q', fp='Q2'))
    ee['facts']['us-gaap']['ProceedsFromIssuanceOfCommonStock'] = _stray('2025-07-27')
    with pytest.raises(ValueError, match='duration'):
        prepared(ee, report_dates=RD)
