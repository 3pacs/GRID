"""Offline bridge tests using pinned public research artifacts and synthetic mutations."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import socket
from unittest.mock import patch

import pytest

from analysis import r3_nvda_ledger as L

ROOT = Path(__file__).resolve().parents[1]
LEDGER = (ROOT / 'docs/research/r3-nvda/ledger-v2.jsonl').read_bytes()
CASH = (ROOT / 'docs/research/r3-nvda/cash-reconciliation-companion-v1.json').read_bytes()
NOW = '2026-11-14T09:30:00Z'


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('network forbidden in offline tests')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket, 'create_connection', denied)


def report(**kwargs):
    return L.desk_report(LEDGER, CASH, as_of=kwargs.get('as_of', NOW),
                         max_age_days=kwargs.get('max_age_days', 120))


def test_reconciled_actual_artifact():
    result = report()
    assert result['ledger_records'] == 455
    assert [row['totals']['issuer_style_fcf'] for row in result['cash_periods']] == [48554, 69895, 21341]
    assert all(row['totals']['cfo_residual'] == 0 for row in result['cash_periods'])


def test_source_verification_is_inherited():
    result = report()
    assert 'inherited' in result['source_verification']
    assert all('inherited' in source['verification'] for row in result['cash_periods'] for source in row['sources'])


def test_q2_receipt_not_publication_alone():
    q2 = report()['cash_periods'][2]
    assert q2['evidence']['available_at'] == L.DESK_BUNDLE_OBSERVED_AT
    assert q2['evidence']['observed_at'].startswith('2026-08-26T20:36:00')
    assert len(q2['sources']) == 2
    assert all(fact['available_at'] == q2['evidence']['available_at'] for fact in q2['facts'].values())
    assert all(fact['inherited_publication_available_at'] == '2026-08-26T16:36:00-04:00' for fact in q2['facts'].values())
    assert q2['sources'][1]['retrieved_at'].startswith('2026-11-13T00:17:54')


def test_source_to_ledger_binding_present():
    for row in report()['cash_periods']:
        assert all(source['ledger_source_record_ids'] for source in row['sources'])


def test_exact_receipt_boundary():
    assert report(as_of=L.DESK_BUNDLE_OBSERVED_AT)['cash_periods'][2]['totals'] is not None


def test_before_bundle_has_no_economic_metadata():
    with pytest.raises(ValueError, match='input bundle unavailable'):
        report(as_of='2026-11-14T09:27:06Z')
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        status = L.main(['--ledger', str(ROOT / 'docs/research/r3-nvda/ledger-v2.jsonl'), '--desk-cash', str(ROOT / 'docs/research/r3-nvda/cash-reconciliation-companion-v1.json'), '--desk-as-of', '2026-01-01T00:00:00Z', '--desk-max-age-days', '120'])
    assert status == 1
    result = json.loads(out.getvalue())
    assert set(result) == {'schema', 'status', 'error', 'promotion_allowed'}
    assert '11.4b' not in out.getvalue() and 'no-revenue' not in out.getvalue()


def test_at_original_source_receipt_is_not_a_contemporaneous_bundle():
    with pytest.raises(ValueError, match='input bundle unavailable'):
        report(as_of='2026-11-13T00:17:54Z')


def test_stale_context_cannot_be_current_evidence():
    rows = report()['cash_periods']
    assert rows[0]['totals'] is not None  # retained historical accounting context only
    assert rows[0]['evidence']['quality'] == 'withheld'
    assert 'stale_observation' in rows[0]['evidence']['reasons']
    assert rows[1]['evidence']['quality'] == 'clock_eligible'
    assert all(not row['evidence']['promotion_allowed'] for row in rows)


def test_policy_does_not_promote():
    result = report(max_age_days=3660)
    assert all(row['evidence']['quality'] == 'clock_eligible' for row in result['cash_periods'])
    assert result['promotion_allowed'] is False
    assert result['forward_evidence'] is False
    assert all(row['evidence']['kind'] == 'development' for row in result['cash_periods'])


def test_no_recognition_from_units_or_percentages():
    records = {row['record_id']: row for row in report()['recognition']['candidate_records']}
    for key in ('orcl-gpus-delivered-q1fy27', 'crwv-nvda-share-of-purchases-2025'):
        assert 'not_delivery_dollar_consideration' in records[key]['reasons']
        assert 'nvidia_attribution_unqualified' in records[key]['reasons']
        assert not records[key]['usable_for_recognition']


def test_scoped_zero_is_not_consolidated_zero_revenue():
    result = report()
    rows = [row for row in result['recognition']['candidate_records'] if row['stage'] == 'recognized_revenue']
    assert len(rows) == 2
    assert all(row['amount']['value'] == 0 for row in rows)
    assert result['recognition']['result'] is None


def test_cash_obligations_or_balances_are_not_sales():
    rows = [row for row in report()['recognition']['candidate_records'] if row['stage'] == 'cash_receipt']
    assert len(rows) == 3
    assert all('cash_receipt_stage_is_not_recognized_nvidia_sales' in row['reasons'] for row in rows)


def test_all_seven_candidates_accounted_for_without_conversion():
    rows = report()['recognition']['candidate_records']
    assert len(rows) == 7
    assert all(row['known_at_cutoff'] and not row['usable_for_recognition'] for row in rows)
    assert all(row['reasons'] for row in rows)


def test_never_emits_forecast_valuation_or_signal():
    result = report()
    assert result['recognition']['status'] == 'UNAVAILABLE'
    assert result['valuation'] is result['probability'] is result['trade_signal'] is None
    assert 'scenarios' not in result and 'stack' not in result
    assert len(result['recognition']['blockers']) == 4


@pytest.mark.parametrize('field,value', [
    ('as_of', '2026-11-14'), ('as_of', '20261114T09:30:00Z'),
    ('as_of', '2026-11-14T09:30:00'), ('as_of', None),
    ('max_age_days', 0), ('max_age_days', True), ('max_age_days', 1.5), ('max_age_days', 3661),
])
def test_bad_policy_clock(field, value):
    with pytest.raises(ValueError):
        report(**{field: value})


def test_offset_clock_normalizes():
    assert report(as_of='2026-11-14T01:30:00-08:00') == report()


def test_date_only_publication_rounds_to_next_midnight():
    assert L._desk_source_clock('2026-11-14').isoformat() == '2026-11-15T00:00:00+00:00'


@pytest.mark.parametrize('which', ['ledger', 'cash'])
def test_mutation_rejected(which):
    with pytest.raises(ValueError, match='hash mismatch'):
        L.desk_report(LEDGER + (b' ' if which == 'ledger' else b''),
                      CASH + (b' ' if which == 'cash' else b''), as_of=NOW, max_age_days=120)


def test_artifact_bytes_required():
    with pytest.raises(ValueError, match='artifact bytes'):
        L.desk_report(LEDGER.decode(), CASH, as_of=NOW, max_age_days=120)


def test_derived_q2_checked_again():
    cash = json.loads(CASH)
    cash['facts']['Q2']['cfo']['value'] += 1
    with pytest.raises(ValueError, match='Q2 does not equal'):
        L._desk_cash(cash, [json.loads(line) for line in LEDGER.splitlines()])


def test_original_cash_receipt_mismatch():
    cash = json.loads(CASH)
    cash['facts']['H1']['cfo']['retrieved_at'] = NOW
    with pytest.raises(ValueError, match='provenance mismatch'):
        L._desk_cash(cash, [json.loads(line) for line in LEDGER.splitlines()])


def test_cash_totals_must_recalculate():
    cash = json.loads(CASH)
    cash['reconciliations']['Q2']['issuer_style_fcf'] = 1
    with pytest.raises(ValueError, match='recalculation'):
        L._desk_cash(cash, [json.loads(line) for line in LEDGER.splitlines()])


def test_missing_ledger_source_binding():
    cash = json.loads(CASH)
    records = [json.loads(line) for line in LEDGER.splitlines()]
    records = [row for row in records if row['source']['url'] != cash['facts']['Q1']['cfo']['source_url']]
    with pytest.raises(ValueError, match='source absent'):
        L._desk_cash(cash, records)


def test_deterministic_and_inputs_unchanged():
    before = hashlib.sha256(LEDGER + CASH).hexdigest()
    assert json.dumps(report(), sort_keys=True, allow_nan=False) == json.dumps(report(), sort_keys=True, allow_nan=False)
    assert hashlib.sha256(LEDGER + CASH).hexdigest() == before


def test_cli_uses_desk_branch_not_frozen_scenarios():
    out = io.StringIO()
    with patch.object(L, 'run_scenarios', side_effect=AssertionError('must not evaluate frozen scenarios')), contextlib.redirect_stdout(out):
        status = L.main(['--ledger', str(ROOT / 'docs/research/r3-nvda/ledger-v2.jsonl'), '--desk-cash', str(ROOT / 'docs/research/r3-nvda/cash-reconciliation-companion-v1.json'), '--desk-as-of', NOW, '--desk-max-age-days', '120'])
    assert status == 0
    assert json.loads(out.getvalue()) == report()


def test_cli_fail_closed(tmp_path):
    bad = tmp_path / 'wrong.json'
    bad.write_text('{}')
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        status = L.main(['--ledger', str(ROOT / 'docs/research/r3-nvda/ledger-v2.jsonl'), '--desk-cash', str(bad), '--desk-as-of', NOW, '--desk-max-age-days', '120'])
    assert status == 1
    assert json.loads(out.getvalue())['status'] == 'UNAVAILABLE'
    assert 'cash_periods' not in json.loads(out.getvalue())


@pytest.mark.parametrize('options', [['--desk-as-of', NOW], ['--desk-cash', str(ROOT / 'docs/research/r3-nvda/cash-reconciliation-companion-v1.json')]])
def test_cli_missing_options(options):
    with pytest.raises(SystemExit) as exc:
        L.main(options)
    assert exc.value.code == 2
