"""New desired contracts, UNRUN. No native application imports or provider calls.

Literal expectations come from the proposal contract, not helper-derived values.
Run only under a fresh controller-admitted source-only scope.
"""
from __future__ import annotations
import ast
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


def selected_module(name, nodes, bindings):
    obj = types.ModuleType(name)
    sys.modules[name] = obj
    obj.__dict__.update(bindings)
    future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    nodes = [n for n in nodes if not (isinstance(n, ast.ImportFrom) and n.module == '__future__')]
    exec(compile(ast.fix_missing_locations(ast.Module(body=[future] + nodes, type_ignores=[])), name, 'exec'), obj.__dict__)
    return obj


class PublicationEvidenceFooterContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        guard_tree = ast.parse((ROOT / 'ollama/number_grounding.py').read_text())
        cls.guard = selected_module('_new_footer_guard', [n for n in guard_tree.body if not (
            isinstance(n, ast.ImportFrom) and n.module == 'loguru')], {'log': Mock()})
        tree = ast.parse((ROOT / 'ollama/market_briefing.py').read_text())
        nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef))]
        cls.market = selected_module('_new_footer_market', nodes, dict(
            dataclass=dataclasses.dataclass, asdict=dataclasses.asdict, hashlib=hashlib,
            math=math, json=json, uuid=uuid, date=date, datetime=datetime,
            timedelta=timedelta, timezone=timezone, Path=Path, Any=object, log=Mock()))

    def footer(self, features, paths=None):
        self.assertTrue(callable(getattr(self.market, '_publication_evidence_footer', None)),
                        'NEW_FOOTER_CONTRACT_NOT_IMPLEMENTED')
        paths = paths or ['features.alpha.value']
        diagnostics = {'roles': {'emitted': {'numeric_mentions': [{
            'start': 8, 'end': 13, 'generation_context_numeric_match': False,
            'snapshot_matches': [{'path': p} for p in paths]}]}}}
        return self.market._publication_evidence_footer(
            {'features': features}, diagnostics, narrative_span=(0, 20))

    def test_missing_false_and_malformed_flags_never_mean_verified(self):
        for extra, expected_flag in [({}, None), ({'unverified': False}, False),
                                     ({'unverified': 'false'}, None), ({'unverified': True}, True)]:
            with self.subTest(extra=extra):
                result = self.footer({'alpha': {'value': 0.125, 'date': '2026-10-23', **extra}})
                record = json.loads(json.dumps(result))['records'][0]
                self.assertEqual(record, {
                    'feature_key': 'alpha', 'path': 'features.alpha.value', 'raw_value': 0.125,
                    'observation_date': '2026-10-23', 'baseline_date': 'unknown',
                    'selection_policy': 'unknown', 'policy_status': 'unknown',
                    'source_id': 'unknown', 'receipt_id': 'unknown', 'unit': 'unknown',
                    'available_at': 'unknown', 'collected_at': 'unknown', 'vintage_date': 'unknown',
                    'basis': 'unknown', 'measure': 'unknown',
                    'verification_state': 'unknown', 'unverified_flag': expected_flag,
                    'flag_scope': 'upstream_declaration_only',
                    'source_truth': 'unknown', 'freshness': 'unknown',
                }, 'MISSING_FLAG_IS_NOT_VERIFIED')

    def test_declared_historical_baseline_and_current_observation_are_distinct(self):
        result = self.footer({'alpha': {
            'value': 0.125, 'date': '2026-10-23', 'baseline_date': '2020-01-02',
            'selection_policy': 'fixed_reference_date', 'source_id': 'synthetic',
            'receipt_id': 'synthetic-record', 'unit': 'ratio'}})
        record = result['records'][0]
        self.assertEqual(record['observation_date'], '2026-10-23')
        self.assertEqual(record['baseline_date'], '2020-01-02', 'BASELINE_DATE_IS_NOT_CURRENT_DATE')
        self.assertEqual(record['selection_policy'], 'fixed_reference_date')
        self.assertEqual(record['policy_status'], 'declared_only')
        self.assertEqual((record['source_id'], record['receipt_id'], record['unit']),
                         ('synthetic', 'synthetic-record', 'ratio'))
        self.assertEqual(record['freshness'], 'unknown')
        missing = self.footer({'alpha': {'value': 0.125, 'date': '2020-01-02'}})
        self.assertEqual(missing['records'][0]['selection_policy'], 'unknown')
        self.assertEqual(missing['records'][0]['freshness'], 'unknown')
        latest = self.footer({'alpha': {'value': 0.125, 'date': '2020-01-02',
                                        'selection_policy': 'latest_available'}})
        self.assertEqual(latest['records'][0]['freshness'], 'unknown')
        undeclared = self.footer({'alpha': {'value': 0.125, 'date': '2026-10-23',
                                            'baseline_date': '2020-01-02'}})
        self.assertEqual(undeclared['records'][0]['baseline_date'], '2020-01-02')
        self.assertEqual(undeclared['records'][0]['selection_policy'], 'unknown')

    def test_equal_values_preserve_both_keys_and_ambiguous_attribution(self):
        features = {'beta': {'value': 0.125}, 'alpha': {'value': 0.125}}
        paths = ['features.beta.value', 'features.alpha.value']
        result = self.footer(features, paths)
        self.assertEqual([r['feature_key'] for r in result['records']], ['alpha', 'beta'],
                         'EQUAL_VALUES_MUST_KEEP_BOTH_KEYS')
        self.assertEqual(result['mentions'], [{
            'start': 8, 'end': 13, 'feature_paths': ['features.alpha.value', 'features.beta.value'],
            'attribution': 'ambiguous_candidates', 'numeric_context_match': False}])
        reversed_input = self.footer(dict(reversed(list(features.items()))), list(reversed(paths)))
        self.assertEqual(result, reversed_input)
        self.assertFalse(result['snapshot_presence_is_grounding_permission'])

    def test_data_rendering_does_not_create_markdown_or_html_instructions(self):
        result = self.footer({'alpha': {'value': 0.125, 'source_id': '```<script>synthetic</script>'}})
        rendered = result['rendered_text']
        self.assertEqual(rendered.count('```'), 2)
        self.assertNotIn('<script>', rendered)
        encoded = rendered.split('```json\n', 1)[1].rsplit('\n```', 1)[0]
        decoded = json.loads(encoded)
        self.assertEqual(decoded['records'][0]['source_id'], '```<script>synthetic</script>')
        self.assertEqual(decoded['records'][0]['source_truth'], 'unknown')

    def test_candidate_only_matches_do_not_appear_in_displayed_footer(self):
        self.assertTrue(callable(getattr(self.market, '_publication_evidence_footer', None)),
                        'NEW_FOOTER_CONTRACT_NOT_IMPLEMENTED')
        diagnostics = {'roles': {
            'candidate': {'numeric_mentions': [{'start': 0, 'end': 5,
                'snapshot_matches': [{'path': 'features.alpha.value'}]}]},
            'emitted': {'numeric_mentions': []}}}
        result = self.market._publication_evidence_footer(
            {'features': {'alpha': {'value': 0.125}}}, diagnostics, narrative_span=(0, 20))
        self.assertEqual(result['records'], [])
        self.assertEqual(result['mentions'], [])
        self.assertEqual(result['rendered_text'], '')

    def _warning_result(self, narrative):
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        engine.engine = object()
        engine.ollama = types.SimpleNamespace(chat=Mock(return_value=narrative))
        engine._gather_market_snapshot = Mock(return_value={
            'timestamp': 'SYNTHETIC', 'features': {'alpha': {'value': 100, 'date': '2026-10-23'}}})
        engine._build_data_context = Mock(return_value='supplied 100.')
        engine._get_system_prompt = Mock(return_value='SYNTHETIC')
        engine._get_user_prompt = Mock(return_value='SYNTHETIC')
        parent = types.ModuleType('ollama')
        parent.__path__ = []
        optional = types.ModuleType('_unavailable_optional')
        with patch.dict(sys.modules, {'ollama': parent, 'ollama.number_grounding': self.guard,
                'ingestion.wiki_history': optional, 'ingestion.social_sentiment': optional,
                'intelligence.sentiment_scorer': optional}):
            result = engine.generate_briefing(save=False)
        engine.ollama.chat.assert_called_once()
        return result

    def test_warning_only_collision_does_not_select_feature(self):
        narrative = 'Observation 999.25.'
        result = self._warning_result(narrative)
        self.assertIn('100%', result['content'])  # Real generated warning collision.
        self.assertIn('999.25 [unverified]', result['content'])
        self.assertEqual(result['grounding']['ungrounded_values'], ['999.25'])
        footer = result['publication_evidence_footer']
        self.assertEqual(footer['records'], [], 'WARNING_ONLY_FEATURE_NOT_ELIGIBLE')
        self.assertEqual(footer['mentions'], [])
        self.assertEqual(footer['rendered_text'], '')
        self.assertNotIn('Feature evidence declarations', result['content'])
        span = footer['narrative_span']
        self.assertEqual(result['content'][span[0]:span[1]], 'Observation 999.25 [unverified].')
        roles = result['publication_stage_diagnostics']['roles']
        self.assertEqual(roles['candidate']['digest']['sha256'], hashlib.sha256(narrative.encode()).hexdigest())
        self.assertEqual(roles['emitted']['digest']['sha256'], hashlib.sha256(result['content'].encode()).hexdigest())

    def test_real_narrative_feature_under_warning_retains_display_offsets(self):
        narrative = 'Observation 100; unsupported 999.25.'
        result = self._warning_result(narrative)
        self.assertIn('50%', result['content'])
        self.assertEqual(result['grounding']['ungrounded_values'], ['999.25'])
        self.assertIn('999.25 [unverified]', result['content'])
        footer = result['publication_evidence_footer']
        self.assertEqual([record['feature_key'] for record in footer['records']], ['alpha'],
                         'REAL_NARRATIVE_FEATURE_REQUIRED')
        self.assertEqual(len(footer['mentions']), 1)
        mention = footer['mentions'][0]
        expected_start = result['content'].index('Observation 100') + len('Observation ')
        self.assertEqual((mention['start'], mention['end']), (expected_start, expected_start + 3))
        self.assertEqual(result['content'][mention['start']:mention['end']], '100')
        self.assertEqual(mention['feature_paths'], ['features.alpha.value'])
        self.assertTrue(mention['numeric_context_match'])
        self.assertEqual(mention['attribution'], 'candidate_only')
        self.assertLessEqual(footer['narrative_span'][0], mention['start'])
        self.assertLessEqual(mention['end'], footer['narrative_span'][1])
        self.assertTrue(result['content'].endswith(footer['rendered_text']))
        roles = result['publication_stage_diagnostics']['roles']
        self.assertEqual(roles['grounding_input']['digest']['sha256'], hashlib.sha256(narrative.encode()).hexdigest())
        self.assertEqual(roles['emitted']['digest']['sha256'], hashlib.sha256(result['content'].encode()).hexdigest())

    def test_final_display_digest_and_original_numeric_pool(self):
        engine = self.market.MarketBriefingEngine.__new__(self.market.MarketBriefingEngine)
        candidate = 'Observation 0.125; another figure 999.25.'
        engine.engine = object()
        engine.ollama = types.SimpleNamespace(chat=Mock(return_value=candidate))
        engine._gather_market_snapshot = Mock(return_value={'timestamp': 'SYNTHETIC', 'features': {
            'alpha': {'value': 0.125, 'date': '2026-10-23', 'source_id': 'fixture 999.25'}}})
        engine._build_data_context = Mock(return_value='Original supplied observation 0.125.')
        engine._get_system_prompt = Mock(return_value='SYNTHETIC')
        engine._get_user_prompt = Mock(return_value='SYNTHETIC')
        parent = types.ModuleType('ollama')
        parent.__path__ = []
        optional = types.ModuleType('_unavailable_optional')
        with patch.dict(sys.modules, {'ollama': parent, 'ollama.number_grounding': self.guard,
                'ingestion.wiki_history': optional, 'ingestion.social_sentiment': optional,
                'intelligence.sentiment_scorer': optional}):
            result = engine.generate_briefing(save=False)
        self.assertEqual(result['grounding']['ungrounded_values'], ['999.25'],
                         'FOOTER_CANNOT_EXPAND_GROUNDING_POOL')
        self.assertIn('999.25 [unverified]', result['content'])
        footer = result['publication_evidence_footer']
        self.assertTrue(result['content'].endswith(footer['rendered_text']))
        self.assertEqual(result['grounding']['checked_scope'], 'narrative_before_evidence_appendix')
        self.assertEqual(result['snapshot']['publication_evidence_footer'], footer)
        sidecar = json.loads(json.dumps(result['grounding']))
        self.assertEqual(sidecar['publication_evidence_footer'], footer)
        roles = result['publication_stage_diagnostics']['roles']
        self.assertEqual(roles['candidate']['digest']['sha256'], hashlib.sha256(candidate.encode()).hexdigest())
        self.assertEqual(roles['emitted']['digest']['sha256'], hashlib.sha256(result['content'].encode()).hexdigest())


if __name__ == '__main__':
    unittest.main(verbosity=2)
