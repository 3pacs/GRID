"""New C/M/L synthetic contracts; source-isolated, no product imports/DB/provider.

Sealed source-span IDs/hashes are provenance anchors, not fixture truth or PIT.
No original publication body or earlier frozen test is consumed here.
"""
from __future__ import annotations
import ast
from copy import deepcopy
import dataclasses
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
import types
import unittest
import uuid
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
ANCHORS = {
 'A': ('market_briefings:4710/line104', '45a1776c5fd6e31b3d4bcdd95c9becbc2cadec4421133bbb4cc6592bbcc460c9'),
 'B': ('market_briefings:4708/line90', '7b4f2dc45316600cb2a1693d2ed61656313fe66a8cdf908f9b214e4bd6cab34e'),
 'C': ('market_briefings:4710/line101', '19ccbf30d46086f464288a437e12bfd681f3b6425b14bcd95319c30924e5b9dc'),
 'D': ('market_briefings:4705/line6', 'cca05e865a0fd783323336786cd0595fb08e6500f51ddfc0a3fabc2d07e02593'),
 'E': ('market_briefings:4710/line103', '0a24abf8cf8f43d6b3095f02267702ea32a5953f7cf3228d01c0823ac66dbf0f'),
 'F': ('market_briefings:4706/line62', 'cfbe7f2a265999bcaafebc45a3beab1d809fe4f21fc190ff09cb612149097185'),
 'G': ('market_briefings:4706/line63', 'e93977921560c4e3295eeaffb321afc7c962098ba23f60537f8c327c5c6e902c'),
 'H': ('market_briefings:4706/line45', '5973aeb87b578f10cdcc960d1c2967580949fb52de1825578c56ac4058c5a774'),
 'I': ('market_briefings:4711/line114', '1b696a74b15f713b5ef68c86597218a42ba7455dbff782040c834df8f7ae1270'),
}


def module(name, nodes, bindings):
    result = types.ModuleType(name)
    sys.modules[name] = result
    result.__dict__.update(bindings)
    future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    nodes = [n for n in nodes if not (isinstance(n, ast.ImportFrom) and n.module == '__future__')]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future] + nodes, type_ignores=[])), name, 'exec'), result.__dict__)
    return result


class DeclaredPublicationContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tree = ast.parse((ROOT / 'ollama/market_briefing.py').read_text())
        cls.market = module('_oct25_renderer', [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))], {
            'Any': object, 'dataclass': dataclasses.dataclass, 'asdict': dataclasses.asdict,
            'datetime': datetime, 'date': date, 'timedelta': timedelta, 'timezone': timezone,
            'json': json, 'math': math, 'hashlib': hashlib, 'uuid': uuid, 'Path': Path, 'log': Mock()})
        guard = ast.parse((ROOT / 'ollama/number_grounding.py').read_text())
        cls.guard = module('_oct25_existing_guard', [n for n in guard.body if not (
            isinstance(n, ast.ImportFrom) and n.module == 'loguru')], {'log': Mock()})

    def test_C1_same_civil_basis_and_unknown_intent(self):
        """C1/A: refuse only the declared same-civil mismatch; preserve input."""
        value = {'basis': 'Gregorian civil date', 'date': '2026-10-27', 'weekday': 'Tuesday'}
        self.assertEqual(self.market._publication_calendar_assessment(value)['status'], 'PASS')
        wrong = dict(value, weekday='Monday')
        old = deepcopy(wrong)
        result = self.market._publication_calendar_assessment(wrong)
        self.assertEqual(result['status'], 'REFUSE_MISMATCH')
        self.assertEqual(result['expected']['weekday'], 'Tuesday')
        self.assertEqual(wrong, old)
        unknown = self.market._publication_calendar_assessment(dict(wrong, basis=None))
        self.assertEqual(unknown['status'], 'ACCEPT_WITH_LIMITATION', 'ABSENT_BASIS_IS_NOT_HARD_MISMATCH')
        self.assertEqual(unknown['expected']['conditional_same_civil_basis']['weekday'], 'Tuesday')

    def test_C2_UTC_Pacific_dual_and_cross_basis_refusal(self):
        """C2/A: literal expected civil dates for the same explicit instant."""
        common = {'basis': 'gregorian_civil_date', 'intent': 'instant',
                  'reference_instant': '2026-10-27T00:30:00Z'}
        utc = {'timezone': 'UTC', 'date': '2026-10-27', 'weekday': 'Tuesday'}
        pacific = {'timezone': 'America/Los_Angeles', 'date': '2026-10-26', 'weekday': 'Monday'}
        for presentation in (utc, pacific):
            self.assertEqual(self.market._publication_calendar_assessment({**common, **presentation})['status'], 'PASS')
        self.assertEqual(self.market._publication_calendar_assessment({**common, 'presentations': [utc, pacific]})['status'], 'PASS')
        mismatch = self.market._publication_calendar_assessment({**common, **utc, 'weekday': 'Monday'})
        self.assertEqual(mismatch['status'], 'REFUSE_MISMATCH')
        self.assertEqual(mismatch['expected']['weekday'], 'Tuesday')
        self.assertEqual(self.market._publication_calendar_assessment({**common, **utc, 'intent': None})['status'], 'ACCEPT_WITH_LIMITATION')

    def test_C2_relative_needs_reference_and_absolute_future_is_allowed(self):
        value = {'basis': 'gregorian_civil_date', 'timezone': 'America/Los_Angeles',
                 'reference_instant': '2026-10-27T00:30:00Z', 'relative_day': 'tomorrow',
                 'date': '2026-10-27', 'weekday': 'Tuesday'}
        self.assertEqual(self.market._publication_calendar_assessment(value)['status'], 'PASS')
        self.assertEqual(self.market._publication_calendar_assessment(dict(value, reference_instant=None))['status'], 'ACCEPT_WITH_LIMITATION')
        future = dict(value, relative_day=None, intent='absolute_date', date='2026-10-28', weekday='Wednesday')
        self.assertEqual(self.market._publication_calendar_assessment(future)['status'], 'PASS')
        self.assertEqual(self.market._publication_calendar_assessment(dict(value, relative_day='next_trading_day'))['status'], 'ACCEPT_WITH_LIMITATION')

    def test_F1_instant_without_zone_is_not_a_civil_mapping(self):
        value = {'basis': 'gregorian_civil_date', 'intent': 'instant',
                 'reference_instant': '2026-10-27T00:30:00Z', 'date': '2026-10-27', 'weekday': 'Tuesday'}
        original = deepcopy(value)
        self.assertEqual(self.market._publication_calendar_assessment(value)['status'], 'ACCEPT_WITH_LIMITATION',
                         'INSTANT_WITHOUT_ZONE_IS_UNBOUND')
        self.assertEqual(value, original)
        civil = {'basis': 'gregorian_civil_date', 'date': '2026-10-27', 'weekday': 'Tuesday'}
        self.assertEqual(self.market._publication_calendar_assessment(civil)['status'], 'PASS')

    def metric(self, measure, value, *, unit='ratio', cohort=None):
        denominator = {'buy_share': 'buys_plus_sells', 'buy_to_sell': 'sells'}.get(measure, 'calls')
        contract = {'metric_id': 'synthetic', 'definition_version': 'v1', 'measure': measure,
                    'unit': unit, 'denominator': denominator, 'cohort_id': cohort,
                    'puts': 6, 'calls': 3, 'buys': 2, 'sells': 8}
        assertion = {k: contract[k] for k in ('metric_id', 'definition_version', 'measure', 'unit', 'denominator', 'cohort_id')}
        return {'value': value, 'metric_contract': contract, 'metric_assertion': dict(assertion, value=value)}

    def test_M1_OI_and_volume_are_separate_valid_contracts(self):
        """M1/B,C: same ratio2 does not make equal definitions."""
        for measure, swapped in [('open_interest', 'traded_volume'), ('traded_volume', 'open_interest')]:
            record = self.metric(measure, 2)
            self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'PASS')
            record['metric_assertion']['measure'] = swapped
            self.assertEqual(self.market._publication_metric_assessment(record)['reason'], 'metric_definition')
            self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'REFUSE_MISMATCH')

    def test_M2_denominators_and_qualified_cohorts(self):
        """M2/D,E: literal20% vs25%, not a universal cohort assumption."""
        for measure, value, wrong in [('buy_share', 20, 25), ('buy_to_sell', 25, 20)]:
            record = self.metric(measure, value, unit='percent', cohort='synthetic-cohort')
            self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'PASS')
            record['metric_assertion']['value'] = wrong
            self.assertEqual(self.market._publication_metric_assessment(record)['reason'], 'denominator')
            record['metric_assertion']['cohort_id'] = 'different-cohort'
            result = self.market._publication_metric_assessment(record)
            self.assertEqual(result['status'], 'ACCEPT_WITH_LIMITATION')
            self.assertEqual(result['reason'], 'cohort_comparison_unknown')
        wrong_denominator = self.metric('buy_share', 20, unit='percent', cohort='synthetic-cohort')
        wrong_denominator['metric_assertion']['denominator'] = 'sells'
        self.assertEqual(self.market._publication_metric_assessment(wrong_denominator)['status'], 'REFUSE_MISMATCH',
                         'DENOMINATOR_BINDING_REQUIRED')

    def test_M3_unknown_definition_and_ETF_not_spread(self):
        unknown = {'value': 2, 'metric_contract': None, 'unit': None}
        self.assertEqual(self.market._publication_metric_assessment(unknown)['status'], 'ACCEPT_WITH_LIMITATION')
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        self.assertIn('unit=unknown', engine._format_evidence_value('features', 'unknown', unknown))
        self.assertIsNone(unknown['metric_contract'])
        assertion = dict(unknown, measure='open_interest', unit='ratio', metric_assertion={'measure': 'open_interest'})
        original = deepcopy(assertion)
        rendered = engine._format_evidence_value('features', 'unknown', assertion)
        self.assertIn('measure=unknown', rendered)
        self.assertIn('unit=unknown', rendered)
        self.assertIn('REFUSE_MISMATCH', rendered)
        self.assertEqual(assertion, original)
        price = {'value': 77, 'metric_contract': {'metric_id': 'HYG', 'definition_version': 'v1',
                 'measure': 'ETF_price', 'unit': 'USD'}, 'metric_assertion': {
                 'metric_id': 'HYG', 'definition_version': 'v1', 'measure': 'spread', 'unit': 'basis_points'}}
        self.assertEqual(self.market._publication_metric_assessment(price)['status'], 'REFUSE_MISMATCH')

    def test_M_identity_version_uncertainty_is_not_same_input_proof(self):
        record = self.metric('open_interest', 2)
        record['metric_assertion']['definition_version'] = 'v2'
        self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'ACCEPT_WITH_LIMITATION')

    def _footer_record(self, record):
        diagnostics = {'roles': {'emitted': {'numeric_mentions': [{'start': 0, 'end': 2,
            'snapshot_matches': [{'path': 'features.synthetic.value'}]}]}}}
        return self.market._publication_evidence_footer({'features': {'synthetic': record}}, diagnostics,
                    narrative_span=(0, 2))['records'][0]

    def test_F2_displayed_value_cannot_borrow_assertion_pass(self):
        record = self.metric('open_interest', 2)
        record.update(value=99, measure='open_interest', unit='ratio', denominator='calls')
        original = deepcopy(record)
        assessment = self.market._publication_metric_assessment(record)
        self.assertNotEqual(assessment['status'], 'PASS', 'DISPLAYED_VALUE_MUST_BIND_ASSERTION')
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        rendered = engine._format_evidence_value('features', 'synthetic', record)
        self.assertIn('raw value=99', rendered)
        self.assertNotIn('metric_contract_status=PASS', rendered)
        footer = self._footer_record(record)
        self.assertEqual(footer['raw_value'], 99)
        self.assertNotEqual(footer['metric_binding']['status'], 'PASS')
        self.assertEqual(record, original)

    def test_F2_displayed_unit_cannot_contradict_contract(self):
        record = self.metric('open_interest', 2)
        record.update(measure='open_interest', unit='percent', denominator='calls')
        original = deepcopy(record)
        self.assertNotEqual(self.market._publication_metric_assessment(record)['status'], 'PASS',
                            'DISPLAYED_UNIT_MUST_BIND_CONTRACT')
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        rendered = engine._format_evidence_value('features', 'synthetic', record)
        self.assertFalse('unit=percent' in rendered and 'metric_contract_status=PASS' in rendered,
                         'DISPLAYED_UNIT_MUST_BIND_CONTRACT')
        self.assertNotEqual(self._footer_record(record)['metric_binding']['status'], 'PASS')
        self.assertEqual(record, original)

    def test_F2_explicit_percent_ratio_alternatives_and_transform_unknown(self):
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        for value, unit in [(25, 'percent'), (0.25, 'ratio')]:
            record = self.metric('buy_to_sell', value, unit=unit, cohort='synthetic-cohort')
            original = deepcopy(record)
            self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'PASS')
            self.assertIn('unit=' + unit, engine._format_evidence_value('features', 'synthetic', record))
            footer = self._footer_record(record)
            self.assertEqual((footer['raw_value'], footer['unit']), (value, unit))
            self.assertEqual(footer['metric_binding']['status'], 'PASS')
            self.assertEqual(record, original)
        transformed = self.metric('buy_to_sell', 25, unit='percent', cohort='synthetic-cohort')
        transformed.update(value=0.25, unit='ratio', metric_transformation={'declaration': 'ratio-to-percent'})
        self.assertEqual(self.market._publication_metric_assessment(transformed)['status'], 'ACCEPT_WITH_LIMITATION')
        self.assertEqual(transformed['value'], 0.25)

    def test_F2_display_identity_and_cohort_must_bind(self):
        for key, value in [('metric_id', 'different'), ('definition_version', 'v2'),
                           ('measure', 'traded_volume'), ('denominator', 'puts')]:
            record = self.metric('open_interest', 2)
            record[key] = value
            self.assertNotEqual(self.market._publication_metric_assessment(record)['status'], 'PASS')
        record = self.metric('buy_share', 20, unit='percent', cohort='one-cohort')
        record['cohort_id'] = 'other-cohort'
        self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'ACCEPT_WITH_LIMITATION')

    def _assert_put_call_cohort_binding(self, measure):
        record = self.metric(measure, 2, cohort='population-A')
        record.update(measure=measure, unit='ratio', denominator='calls', cohort_id='population-A')
        self.assertEqual(self.market._publication_metric_assessment(record)['status'], 'PASS',
                         'LEGITIMATE_SAME_COHORT_CONTROL')
        self.assertEqual(self._footer_record(record)['metric_binding']['status'], 'PASS')
        record['metric_assertion'] = dict(record['metric_assertion'], cohort_id='population-B')
        original = deepcopy(record)
        result = self.market._publication_metric_assessment(record)
        self.assertEqual(result['status'], 'ACCEPT_WITH_LIMITATION', 'ASSERTED_COHORT_MUST_BIND_DEFINITION')
        self.assertEqual(result['reason'], 'cohort_comparison_unknown')
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        rendered = engine._format_evidence_value('features', 'synthetic', record)
        self.assertIn('metric_contract_status=ACCEPT_WITH_LIMITATION', rendered)
        self.assertNotIn('metric_contract_status=PASS', rendered)
        footer = self._footer_record(record)
        self.assertEqual(footer['cohort_id'], 'population-A')
        self.assertEqual(footer['metric_binding']['status'], 'ACCEPT_WITH_LIMITATION')
        self.assertEqual(footer['source_truth'], 'unknown')
        self.assertEqual(record, original)

    def test_F2_OI_asserted_cohort_binds_definition(self):
        self._assert_put_call_cohort_binding('open_interest')

    def test_F2_volume_asserted_cohort_binds_definition(self):
        self._assert_put_call_cohort_binding('traded_volume')

    def test_F2_unbound_explicit_cohorts_qualify_without_new_legacy_fields(self):
        for measure in ('open_interest', 'traded_volume'):
            legacy = self.metric(measure, 2)
            self.assertEqual(self.market._publication_metric_assessment(legacy)['status'], 'PASS')
            for contract_cohort, assertion_cohort in [('population-A', None), (None, 'population-B'),
                                                      ('population-A', ''), (True, True)]:
                with self.subTest(measure=measure, contract=contract_cohort, assertion=assertion_cohort):
                    record = self.metric(measure, 2)
                    record['metric_contract']['cohort_id'] = contract_cohort
                    record['metric_assertion']['cohort_id'] = assertion_cohort
                    original = deepcopy(record)
                    self.assertEqual(self.market._publication_metric_assessment(record)['status'],
                                     'ACCEPT_WITH_LIMITATION')
                    self.assertEqual(record, original)

    def _options_context(self, record):
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        parent = types.ModuleType('ollama')
        parent.__path__ = []
        optional = types.ModuleType('intelligence.context_provider')
        with patch.dict(sys.modules, {'ollama': parent, 'ollama.number_grounding': self.guard,
                                      'intelligence.context_provider': optional}):
            return engine._build_data_context({'timestamp': 'SYNTHETIC', 'options': {'spy_put_call': record}})

    def test_F3_explicit_volume_context_is_not_legacy_OI(self):
        record = self.metric('traded_volume', 2)
        record.update(date='2030-01-01', source='synthetic', measure='traded_volume', unit='ratio', denominator='calls')
        original = deepcopy(record)
        context = self._options_context(record)
        self.assertNotIn('Definition: SPY put/call ratio, open interest, all expiries', context,
                         'DECLARED_VOLUME_CANNOT_BECOME_OI')
        self.assertIn('Declared definition: SPY put/call traded volume; expiry scope=unknown', context)
        self.assertIn('metric_contract_status=PASS', context)
        self.assertEqual(record, original)

    def test_F3_typed_OI_scope_is_explicit_and_legacy_OI_survives(self):
        legacy = {'value': 2, 'date': '2030-01-01', 'source': 'synthetic'}
        self.assertIn('Definition: SPY put/call ratio, open interest, all expiries', self._options_context(legacy))
        typed = self.metric('open_interest', 2)
        typed.update(date='2030-01-01', source='synthetic')
        typed['metric_contract']['expiry_scope'] = 'all'
        typed['metric_assertion']['expiry_scope'] = 'all'
        context = self._options_context(typed)
        self.assertIn('Declared definition: SPY put/call open interest; expiry scope=all', context)
        typed['metric_assertion']['expiry_scope'] = 'front_expiry'
        self.assertIn('interpretation held', self._options_context(typed))

    def test_F3_unknown_or_incompatible_options_remain_uninterpreted(self):
        unknown = {'value': 2, 'date': '2030-01-01', 'source': 'synthetic', 'metric_contract': None}
        context = self._options_context(unknown)
        self.assertIn('Declared put/call definition: unknown; interpretation held', context)
        self.assertNotIn('Definition: SPY put/call ratio, open interest, all expiries', context)
        bad = self.metric('traded_volume', 2)
        bad.update(date='2030-01-01', source='synthetic')
        bad['metric_assertion']['measure'] = 'open_interest'
        context = self._options_context(bad)
        self.assertIn('REFUSE_MISMATCH', context)
        self.assertNotIn('Declared definition: SPY put/call traded volume', context)

    def test_L1_separate_clocks_null_publication_and_PIT(self):
        """L1/F,G: all clocks retained; storage/capture never publication."""
        clocks = {'observed_at': '2030-01-01T00:00:00Z', 'release_at': '2030-01-01T01:00:00Z',
                  'ingested_at': '2030-01-02T00:00:00Z', 'stored_row_clock': '2030-01-02T00:00:00Z',
                  'captured_at': '2030-01-03T00:00:00Z', 'original_publication_time': None, 'pit_possession': None}
        info = {'lineage': {'clocks': clocks}}
        original = deepcopy(info)
        result = self.market._publication_lineage_assessment(info)
        for key, value in clocks.items():
            self.assertEqual(result['clocks'][key], value, 'CLOCKS_CANNOT_PROMOTE_PUBLICATION_OR_PIT')
        self.assertEqual(result['clock_status'], 'ACCEPT_WITH_LIMITATION')
        self.assertEqual(info, original)

    def test_L1_promotions_require_independent_declared_references(self):
        for clocks, reason in [({'original_publication_time': '2030-01-02T00:00:00Z'}, 'publication_time_receipt_missing'),
                               ({'release_at': '2030-01-01T00:00:00Z', 'pit_possession': True}, 'pit_receipt_missing')]:
            result = self.market._publication_lineage_assessment({'lineage': {'clocks': clocks}})
            self.assertEqual(result['clock_status'], 'REFUSE_MISMATCH')
            self.assertEqual(result['clock_reason'], reason)
        result = self.market._publication_lineage_assessment({'lineage': {
            'clocks': {'original_publication_time': '2030-01-02T00:00:00Z', 'pit_possession': True},
            'publication_receipt_id': 'synthetic-publication-reference', 'pit_receipt_id': 'synthetic-possession-reference'}})
        self.assertEqual(result['clock_status'], 'PASS')
        self.assertEqual(result['source_truth'], 'unknown')
        self.assertEqual(result['receipts'], 'declared_references_not_verified')

    def versions(self):
        return [{'source_id': 'synthetic-source', 'series_id': 'synthetic-series',
                 'observation_date': '2030-01-01', 'version_id': version, 'value': value,
                 'content_sha256': hashlib.sha256(json.dumps({'value': value}, sort_keys=True).encode()).hexdigest(),
                 'revision_clock': clock, 'contract_receipt_id': 'synthetic-version-reference'}
                for version, value, clock in [('v1', 10, '2030-01-02T00:00:00Z'), ('v2', 11, '2030-01-03T00:00:00Z')]]

    def test_L2_distinct_same_asof_versions_preserved(self):
        """L2/H,I: version identity is not observation date identity."""
        versions = self.versions()
        original = deepcopy(versions)
        result = self.market._publication_lineage_assessment({'lineage': {'versions': versions}})
        self.assertEqual(result['version_status'], 'PASS')
        self.assertEqual(result['versions'], original, 'LEGITIMATE_VERSIONS_MUST_ALL_SURVIVE')
        self.assertEqual([v['value'] for v in json.loads(json.dumps(result))['versions']], [10, 11])
        self.assertEqual(versions, original)

    def test_L2_unknown_receipts_and_explicit_history_mismatch(self):
        first, second = self.versions()
        unknown = self.market._publication_lineage_assessment({'lineage': {'versions': [{'value': 11}]}})
        self.assertEqual(unknown['version_status'], 'ACCEPT_WITH_LIMITATION')
        wrong = self.market._publication_lineage_assessment({'lineage': {
            'versions': [second], 'previous_versions': [first], 'assert_immutable_history': True}})
        self.assertEqual(wrong['version_status'], 'REFUSE_MISMATCH')
        right = self.market._publication_lineage_assessment({'lineage': {
            'versions': [first, second], 'previous_versions': [first], 'assert_immutable_history': True}})
        self.assertEqual(right['version_status'], 'PASS')

    def test_L_unknown_clock_zones_and_revision_clocks_stay_qualified(self):
        lineage = {'clocks': {'original_publication_time': '2030-01-02T00:00:00', 'pit_possession': True},
                   'publication_receipt_id': 'synthetic-reference', 'pit_receipt_id': 'synthetic-PIT-reference'}
        result = self.market._publication_lineage_assessment({'lineage': lineage})
        self.assertEqual(result['clock_status'], 'ACCEPT_WITH_LIMITATION')
        self.assertEqual(result['clocks']['original_publication_time'], '2030-01-02T00:00:00')
        versions = self.versions()
        versions[1]['revision_clock'] = '2030-01-03T00:00:00'
        result = self.market._publication_lineage_assessment({'lineage': {'versions': versions}})
        self.assertEqual(result['version_status'], 'ACCEPT_WITH_LIMITATION')
        self.assertEqual(result['versions'], versions)

    def test_existing_OI_all_expiries_and_insider_formulas_are_preserved(self):
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        snapshot = {'timestamp': 'SYNTHETIC', 'options': {'spy_put_call': {
            'value': 2, 'date': '2030-01-01', 'source': 'synthetic'}}}
        parent = types.ModuleType('ollama')
        parent.__path__ = []
        with patch.dict(sys.modules, {'ollama': parent, 'ollama.number_grounding': self.guard}):
            context = engine._build_data_context(snapshot)
        self.assertIn('open interest, all expiries', context)
        self.assertIn('put open interest / call open interest', context)
        for formula in ('buys/(buys+sells)', 'buys/sells', 'sells/buys'):
            self.assertIn(formula, context)
        self.assertIn('calendar_intent_or_basis_unknown', context)

    def test_prompt_clock_is_UTC_assembly_not_action_or_market_session(self):
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        prompt = engine._get_user_prompt('hourly', 'SYNTHETIC')
        self.assertIn('Prompt assembly time (UTC, not action/publication/PIT time)', prompt)
        self.assertNotIn('US markets are open', prompt)
        self.assertNotIn('US markets are closed', prompt)
        self.assertIn('Do not infer a market session from the host clock', prompt)

    def test_footer_serialization_carries_new_bindings_without_verifying(self):
        info = self.metric('buy_share', 20, unit='percent', cohort='synthetic-cohort')
        info.update(unverified=False, lineage={'clocks': {'captured_at': '2030-01-03T00:00:00Z',
                                                        'original_publication_time': None}})
        diagnostics = {'roles': {'emitted': {'numeric_mentions': [{'start': 0, 'end': 2,
            'snapshot_matches': [{'path': 'features.synthetic.value'}]}]}}}
        result = self.market._publication_evidence_footer({'features': {'synthetic': info}}, diagnostics,
                                                          narrative_span=(0, 2))
        record = json.loads(json.dumps(result))['records'][0]
        self.assertEqual(record['metric_binding']['status'], 'PASS')
        self.assertIsNone(record['lineage']['clocks']['original_publication_time'])
        self.assertEqual(record['source_truth'], 'unknown')
        self.assertEqual(record['verification_state'], 'unknown')
        self.assertIs(record['unverified_flag'], False)


if __name__ == '__main__':
    unittest.main(verbosity=2)
