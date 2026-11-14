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


def test_capture_id_binds_exact_source_bytes_fx_inputs_and_normalized_values():
    s = prepared()
    assert prepared(source_body=json.dumps(payload(), indent=2).encode()).snapshot_id != s.snapshot_id
    assert prepared(fx_rates={'USD': 1, 'EUR': 1.1}).snapshot_id != s.snapshot_id
    assert prepared(payload(1)).snapshot_id != s.snapshot_id


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
