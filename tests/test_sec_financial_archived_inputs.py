"""Offline regressions from preserved SEC bytes, not handwritten facts.

Companyfacts files are current snapshots retrieved on 2026-11-07, not
point-in-time archives. The NVDA accession HTML is a separate filing artifact.
"""
from copy import deepcopy
from datetime import date
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

import pytest
from ingestion.altdata import sec_xbrl_financials as sec

ROOT = Path(__file__).parent / 'fixtures' / 'sec_financial_periods'


def original_bytes(name):
    meta = json.loads((ROOT / f'{name}.provenance.json').read_text())
    assert meta['storage_encoding'] == 'gzip'
    return gzip.decompress((ROOT / meta['filename']).read_bytes())


def snapshot(symbol):
    return json.loads(original_bytes(symbol))


def rows(symbol):
    return sec._collect_all_flows(snapshot(symbol), {'USD': 1.0})


@pytest.mark.parametrize('name', ['nvda', 'aapl', 'tsm', 'nvda-filing'])
def test_original_response_bytes_match_provenance(name):
    meta = json.loads((ROOT / f'{name}.provenance.json').read_text())
    raw = original_bytes(name)
    assert meta['status'] == 200
    assert len(raw) == meta['bytes']
    assert hashlib.sha256(raw).hexdigest() == meta['sha256']
    assert meta['url'].startswith(('https://data.sec.gov/', 'https://www.sec.gov/'))


def test_nvda_actual_revenues_tag_rejects_six_month_fact():
    facts = snapshot('nvda')
    entries = facts['facts']['us-gaap']['Revenues']['units']['USD']
    current = [e for e in entries if e['end'] == '2026-07-26']
    assert {(e['start'], e['val']) for e in current} == {
        ('2026-01-26', 177_837_000_000), ('2026-04-27', 96_221_000_000)}
    for entry in current:
        result = sec._normalize_entry(entry)
        if entry['start'] == '2026-01-26':
            assert result is None
        else:
            assert result['period_type'] == 'quarter'
    selected = [r for r in rows('nvda') if r['flow_type'] == 'revenue'
                and r['fiscal_period'] == date(2026, 7, 26)]
    assert len(selected) == 1
    assert selected[0]['amount_local'] == 96_221_000_000
    assert selected[0]['xbrl_tag'] == 'Revenues'
    assert selected[0]['accession'] == '0001045810-26-000075'


def test_nvda_immutable_filing_independently_confirms_duration_values():
    root = ET.fromstring(original_bytes('nvda-filing'))
    ns = {'x': 'http://www.xbrl.org/2003/instance'}
    contexts = {e.get('id'): e for e in root.findall('.//x:context', ns)}
    observed = set()
    for e in root.iter():
        if not e.tag.endswith('nonFraction') or e.get('name') != 'us-gaap:Revenues':
            continue
        context = contexts[e.get('contextRef')]
        # Consolidated, dimensionless contexts only.
        if context.find('.//x:segment', ns) is not None:
            continue
        start = context.findtext('.//x:startDate', namespaces=ns)
        end = context.findtext('.//x:endDate', namespaces=ns)
        if end == '2026-07-26':
            value = Decimal(''.join(e.itertext()).replace(',', '')) * 10 ** int(e.get('scale', '0'))
            observed.add((start, int(value)))
    assert observed == {('2026-01-26', 177_837_000_000), ('2026-04-27', 96_221_000_000)}


def test_apple_real_53_week_year_and_14_week_quarter():
    revenue = {r['fiscal_period']: r for r in rows('aapl') if r['flow_type'] == 'revenue'}
    annual = revenue[date(2023, 9, 30)]
    quarter = revenue[date(2022, 12, 31)]
    assert (annual['fiscal_period'] - annual['period_start']).days + 1 == 371
    assert annual['period_type'] == 'annual'
    assert annual['amount_local'] == 383_285_000_000
    assert (quarter['fiscal_period'] - quarter['period_start']).days + 1 == 98
    assert quarter['period_type'] == 'quarter'
    assert quarter['amount_local'] == 117_154_000_000


def test_tsm_real_ifrs_annual_revenue_preserves_selected_twd_context():
    selected = [r for r in rows('tsm') if r['flow_type'] == 'revenue'
                and r['fiscal_period'] == date(2024, 12, 31)]
    assert len(selected) == 1
    row = selected[0]
    assert row['taxonomy'] == 'ifrs-full'
    assert row['period_type'] == 'annual'
    assert row['period_start'] == date(2024, 1, 1)
    assert row['currency'] == 'TWD'
    assert row['amount_local'] == 2_894_307_700_000
    # FX fallback is existing approximate conversion, not validated market FX.
    assert row['confidence'] == 'estimated'


@pytest.mark.parametrize('symbol', ['nvda', 'aapl', 'tsm'])
def test_full_snapshot_is_order_independent_and_only_supported_durations(symbol):
    facts = snapshot(symbol)
    original = sec._collect_all_flows(facts, {'USD': 1.0})
    reversed_facts = deepcopy(facts)
    for taxonomy in reversed_facts['facts'].values():
        for concept in taxonomy.values():
            for entries in concept.get('units', {}).values():
                entries.reverse()
    assert sec._collect_all_flows(reversed_facts, {'USD': 1.0}) == original
    assert original
    keys = [(r['flow_type'], r['fiscal_period'], r['period_type']) for r in original]
    assert len(keys) == len(set(keys))
    for r in original:
        days = (r['fiscal_period'] - r['period_start']).days + 1
        assert (80 <= days <= 100 if r['period_type'] == 'quarter' else 350 <= days <= 380)
        assert f"period={r['period_start']}/{r['fiscal_period']}" in r['source_filing']
