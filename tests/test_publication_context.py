"""Frozen-audit counterexamples and native publication-boundary regressions."""
from __future__ import annotations

import copy
import json

import pytest

from ollama.number_grounding import annotate_publication_context, publication_context_facts
from tests.test_market_briefing_number_grounding import (
    _FakeConnection, _FakeEngine, _FakeOllamaClient, _empty_snapshot,
    _make_engine, _no_network,
)


def snapshot(*, aligned=False):
    data = _empty_snapshot()
    data['timestamp'] = '2026-10-05T22:00:00+00:00'
    data['latest_regime'] = {
        'state': 'FRAGILE', 'confidence': 0.5481, 'transition_prob': 0.1548,
        'timestamp': '2026-10-04T22:01:04.002177+00:00',
        'recommendation': 'DEFENSIVE',
    }
    data['volatility'] = {
        '^VIX': {'value': 16.3, 'date': '2026-10-05'},
        '^VIX3M': {'value': 18.03, 'date': '2026-10-05' if aligned else '2026-10-02'},
        '^VIX9D': {'value': 12.95, 'date': '2026-10-05' if aligned else '2026-10-02'},
    }
    data['options'] = {'spy_put_call': {'value': 1.97, 'date': '2026-10-05', 'source': 'spy_pcr'}}
    data['convergence'] = [{'ticker': 'DVN', 'direction': 'PUT', 'sources': 6,
                            'source_types': ['synthetic'], 'confidence': 0.85}]
    return data


@pytest.mark.parametrize('claim,reason', [
    ('FRAGILE | Confidence: 55% | Direction: Worsening.', 'missing_regime_comparator'),
    ('State: FRAGILE | Confidence: 55% | Trend: Deteriorating.', 'missing_regime_comparator'),
    ('FRAGILE | Confidence 55% | Worsening (Insider selling).', 'missing_regime_comparator'),
    ('State: STABLE | Confidence: 75% | Trend: Deteriorating.', 'missing_regime_comparator'),
    ('State: CRISIS | Confidence: 75% | Trend: Deteriorating.', 'missing_regime_comparator'),
    ('State: VOLATILE | Confidence: 75% | Trend: Deteriorating.', 'missing_regime_comparator'),
    ('State: TRANSITIONAL | Confidence: 75% | Trend: Deteriorating.', 'missing_regime_comparator'),
    ('State: FRAGILE | Confidence: 55% | Trend: Stabilizing.', 'missing_regime_comparator'),
    ('## Regime Check\nState: STABLE\nConfidence: 75%\nTrend: Deteriorating.', 'missing_regime_comparator'),
    ('State: STABLE\nConfidence: 75%\nTrend: Stabilizing.', 'missing_regime_comparator'),
    ('FRAGILE | Confidence: 55% | Trend: Transitioning.', 'missing_regime_comparator'),
    ('The regime is deteriorating compared to yesterday.', 'missing_regime_comparator'),
    ('The regime has remained FRAGILE since yesterday.', 'missing_regime_comparator'),
    ('The regime trend is unmeasured, but conditions are deteriorating.', 'missing_regime_comparator'),
    ('Direction of travel is rapidly worsening.', 'missing_regime_comparator'),
    ('Direction: <b>Worsening</b>.', 'missing_regime_comparator'),
    ('The regime is wor\u034fsening.', 'missing_regime_comparator'),
    ('Regime confidence drops below 40% (indicating stability).', 'confidence_is_not_stability'),
    ('FRAGILE confidence is low, indicating a transitional state.', 'confidence_is_not_stability'),
    ('The VIX term structure is in contango.', 'incompatible_vix_observation_dates'),
    ('VIX9D below VIX3M confirms a contemporaneous volatility slope.', 'incompatible_vix_observation_dates'),
    ('VIX9D < VIX3M.', 'incompatible_vix_observation_dates'),
    ('VIX exceeds VIX3M.', 'incompatible_vix_observation_dates'),
    ('The spread between VIX and VIX3M is positive.', 'incompatible_vix_observation_dates'),
    ('The bull case has a 30% probability next week.', 'uncalibrated_outcome_probability'),
    ('There is a high probability of a sharp reversal within 48 hours.', 'uncalibrated_outcome_probability'),
    ('The 85% confidence scenario predicts a crash.', 'uncalibrated_outcome_probability'),
    ('With 85% confidence, markets will rise next week.', 'uncalibrated_outcome_probability'),
    ('With 85% confidence, markets will rally next week.', 'uncalibrated_outcome_probability'),
    ('There is an 80 percent probability of a crash.', 'uncalibrated_outcome_probability'),
    ('We assign a 0.85 probability to the recession scenario.', 'uncalibrated_outcome_probability'),
    ('PCR proves dealers are short gamma.', 'aggregate_pcr_does_not_identify_dealer_side'),
    ('Market makers are selling vol because the PCR is high.', 'aggregate_pcr_does_not_identify_dealer_side'),
    ('If VIX9D rises and SPY drops, exit all long positions.', 'unsupported_positioning_instruction'),
    ('Do not initiate new long positions.', 'unsupported_positioning_instruction'),
    ('If you are long, tighten stops.', 'unsupported_positioning_instruction'),
    ('Watch for VIX above 18 as a buy trigger.', 'unsupported_positioning_instruction'),
    ('The convergence hypotheses for biotech names (DVN, MESO) point to PUT.', 'dvn_sector_misclassification'),
    ('The biotech convergence signals for DVN are significant.', 'dvn_sector_misclassification'),
    ('DVN is a technology stock.', 'dvn_sector_misclassification'),
    ('DVN, a leading biotech firm, announced earnings.', 'dvn_sector_misclassification'),
])
def test_known_defect_omits_only_its_sentence(claim, reason):
    good = 'SPY put/call open-interest ratio is 1.97.'
    text, receipt = annotate_publication_context(good + ' ' + claim, snapshot())
    assert good in text
    assert claim not in text
    assert reason in receipt['reasons']
    assert receipt['passed'] is False


@pytest.mark.parametrize('text', [
    'Reported regime state: FRAGILE. Reported classifier confidence: 54.81% (calibration unverified).',
    'The regime trend is unmeasured. A prior comparator is unavailable.',
    'Lower classifier confidence does not establish stability.',
    'Reported classifier confidence is 54.81%, not a calibrated outcome probability.',
    'No dealer inventory is supplied in this snapshot.',
    'Signed dealer gamma cannot be inferred from aggregate PCR.',
    'No regime improvement can be inferred without a comparator.',
    'A regime transition cannot be inferred from one record.',
    'A classifier confidence of 55% does not predict a crash.',
    'Treasury term structure is unavailable.',
    'VIX is 16.3 as of 2026-10-05. VIX3M is 18.03 as of 2026-10-02.',
    'Contemporaneous term structure is unavailable.',
    'Contemporaneous VIX term structure is unavailable.',
    'Dealer inventory is unmeasured. PCR does not establish dealer side.',
    'DVN is Energy / Exploration & Production. MESO is a biotech name.',
    'DVN is Energy, while MESO is biotech.',
    'Tech stocks rallied, while DVN fell 2%.',
    'Unlike tech leaders, DVN trades with crude oil.',
    'The latest regime is STABLE.',
    'In our base case, revenue grew 12% year-over-year.',
    'Short sellers covered their positions today.',
    'Hold ratings outnumber sell ratings among analysts.',
    'We cannot infer dealer gamma from aggregate PCR.',
    'Insiders reported selling shares. Watch observation dates.',
    'BULLISH sentiment and NEUTRAL thesis conviction describe distinct model outputs.',
    'FRAGILE | Confidence: 0.5481 | Sales are improving.',
    '## Regime Check\nState: FRAGILE\n## Company Results\nTrend: Improving.',
])
def test_supported_descriptions_and_disclosures_survive(text):
    output, receipt = annotate_publication_context(text, snapshot())
    assert output == text
    assert receipt['passed']


def test_aligned_curve_and_same_dated_weekend_readings_survive():
    data = snapshot(aligned=True)
    for value in data['volatility'].values():
        value['date'] = '2026-10-02'  # No invented weekend expiry rule.
    claim = 'VIX9D is below VIX3M; the term structure is in contango.'
    assert annotate_publication_context(claim, data)[0] == claim
    facts = publication_context_facts(data)
    assert facts['vix_dates_compatible'] is True
    assert facts['regime_comparator_available'] is False


@pytest.mark.parametrize('bad', [None, '', 'not-a-date', '2026-13-01'])
def test_unknown_dates_cannot_authorize_curve(bad):
    data = snapshot(aligned=True)
    data['volatility']['^VIX3M']['date'] = bad
    text, receipt = annotate_publication_context('The VIX curve is in contango.', data)
    assert 'contango' not in text
    assert 'incompatible_vix_observation_dates' in receipt['reasons']


@pytest.mark.parametrize('bad', [None, True, float('nan'), float('inf'), 10 ** 400, '16.3'])
def test_invalid_values_cannot_authorize_curve(bad):
    data = snapshot(aligned=True)
    data['volatility']['^VIX']['value'] = bad
    assert not publication_context_facts(data)['vix_dates_compatible']


def test_boolean_and_narrative_comparators_do_not_grant_permission():
    data = snapshot()
    data['has_regime_comparator'] = True
    data['prior_regime'] = 'Conditions are improving.'
    assert not annotate_publication_context('The regime is improving.', data)[1]['passed']


@pytest.mark.parametrize('state', ['STABLE', 'CRISIS', 'VOLATILE', 'UNKNOWN'])
def test_reported_state_survives_but_does_not_authorize_pipe_trend(state):
    data = snapshot()
    data['latest_regime']['state'] = state
    good = state + ' | Confidence: 0.5481'
    assert annotate_publication_context(good, data)[0] == good
    text, receipt = annotate_publication_context(good + ' | Trend: Unchanged.', data)
    assert good in text
    assert 'Trend: Unchanged' not in text
    assert 'missing_regime_comparator' in receipt['reasons']


def test_factual_stable_state_and_confidence_are_not_a_causal_claim():
    data = snapshot()
    data['latest_regime']['state'] = 'STABLE'
    text = 'Reported regime state: STABLE, classifier confidence: 54.81%.'
    assert annotate_publication_context(text, data)[0] == text


@pytest.mark.parametrize('claim', [
    'No dealer inventory is supplied in this snapshot, but dealers are short gamma.',
    'Signed dealer gamma cannot be inferred from aggregate PCR, but dealers are long gamma.',
    'No regime improvement can be inferred without a comparator, but the regime is improving.',
    'A classifier confidence of 55% does not predict a crash, but confidence means stability.',
    'A classifier confidence of 55% does not predict a crash, but markets will rally next week.',
])
def test_measurement_negation_cannot_launder_a_positive_assertion(claim):
    assert not annotate_publication_context(claim, snapshot())[1]['passed']


def test_future_dates_cannot_authorize_curve_or_enter_context(tmp_path):
    data = snapshot(aligned=True)
    for info in data['volatility'].values():
        info['date'] = '2030-01-01'
    claim = 'As of 2030-01-01, VIX9D is below VIX3M; the VIX term structure is in contango.'
    text, receipt = annotate_publication_context(claim, data)
    assert 'contango' not in text
    assert not receipt['passed']
    assert not publication_context_facts(data)['vix_dates_compatible']
    engine = _make_engine(_FakeOllamaClient(''), None, tmp_path)
    context = engine._build_data_context(data)
    summary = engine._generate_fallback_briefing(data)
    for forbidden in ['^VIX: 16.3', '^VIX3M: 18.03', '^VIX9D: 12.95',
                      '**^VIX**: 16.3', '**^VIX3M**: 18.03', '**^VIX9D**: 12.95']:
        assert forbidden not in context
        assert forbidden not in summary


@pytest.mark.parametrize('cutoff', [None, '', 'invalid', True])
def test_missing_or_invalid_snapshot_cutoff_cannot_authorize_observations(cutoff):
    data = snapshot(aligned=True)
    data['timestamp'] = cutoff
    assert not publication_context_facts(data)['vix_dates_compatible']


def test_treasury_limit_after_vix_level_is_not_a_vix_curve():
    text = 'VIX is 16.3; Treasury term structure is unavailable.'
    assert annotate_publication_context(text, snapshot())[0] == text


@pytest.mark.parametrize('data', [{'volatility': None}, {'latest_regime': None},
                                {'volatility': {'^VIX': None}}])
def test_null_context_reports_unknown_without_crashing(data):
    assert not publication_context_facts(data)['vix_dates_compatible']


def test_numeric_sentence_after_defect_is_preserved():
    good = '10 energy companies reported earnings.'
    assert good in annotate_publication_context('The regime is worsening. ' + good, snapshot())[0]


def test_aligned_pair_does_not_require_unrelated_third_tenor():
    data = snapshot(aligned=True)
    del data['volatility']['^VIX9D']
    claim = 'VIX3M is above VIX.'
    assert annotate_publication_context(claim, data)[0] == claim


def test_dated_older_pair_survives_mixed_full_snapshot():
    claim = 'As of 2026-10-02, VIX9D is below VIX3M.'
    assert annotate_publication_context(claim, snapshot())[0] == claim


@pytest.mark.parametrize('claim', [
    'The put/call ratio does not measure hedging intent.',
    'Hedging intent is unmeasured.',
    'We cannot infer signed dealer gamma from aggregate PCR.',
])
def test_measurement_disclosures_also_survive_legacy_gate(claim):
    from ollama.number_grounding import check_publication_claims
    assert annotate_publication_context(claim, snapshot())[0] == claim
    assert check_publication_claims(claim)['passed']


def test_measurement_disclosure_does_not_launder_positive_hedge_claim():
    from ollama.number_grounding import check_publication_claims
    claim = 'The put/call ratio does not measure hedging intent, but investors are buying puts.'
    assert not check_publication_claims(claim)['passed']


@pytest.mark.parametrize('claim', [
    'ＦＲＡＧＩＬＥ | Direction: **Worsening**.',
    'The regime is wor\u200bsening.',
    'Dealer inventory is unmeasured; nevertheless dealers are short gamma.',
    'Lower classifier confidence does not establish stability, but confidence falling means stability.',
])
def test_adversarial_markup_and_disclaimer_laundering(claim):
    assert not annotate_publication_context(claim, snapshot())[1]['passed']


@pytest.mark.parametrize('kind', ['hourly', 'daily', 'weekly'])
def test_native_generation_preserves_good_prose_before_file_and_db(tmp_path, monkeypatch, kind):
    monkeypatch.setattr('ingestion.wiki_history.WikiHistoryPuller', _no_network)
    monkeypatch.setattr('ingestion.social_sentiment.SocialSentimentPuller', _no_network)

    class RecordingConnection(_FakeConnection):
        def __init__(self):
            super().__init__()
            self.publications = []

        def execute(self, stmt, params=None):
            if 'INSERT INTO market_briefings' in str(stmt):
                self.publications.append(params)
            return super().execute(stmt, params)

    conn = RecordingConnection()
    good = 'SPY put/call open-interest ratio is 1.97.'
    bad = 'FRAGILE | Confidence: 55% | Direction: Worsening.'
    engine = _make_engine(_FakeOllamaClient(good + '\n\n' + bad), _FakeEngine(conn), tmp_path)
    monkeypatch.setattr(engine, '_gather_market_snapshot', snapshot)
    result = engine.generate_briefing(kind, save=True)
    assert good in result['content']
    assert bad not in result['content']
    assert 'AI narrative withheld' not in result['content']
    assert len(conn.publications) == 1
    assert conn.publications[0]['content'] == result['content']
    assert result['content'] in next(tmp_path.glob(f'{kind}_*.md')).read_text()
    saved = json.loads(conn.publications[0]['snap'])
    assert saved['publication_context_guard'] == result['publication_context_guard']
    context = engine._build_data_context(snapshot())
    for expected in ['2026-10-04T22:01:04.002177+00:00', 'comparator', 'observation dates',
                     'classifier', 'conviction', 'sentiment', 'DVN', 'Energy']:
        assert expected in context


@pytest.mark.parametrize('state', ['FRAGILE', 'STABLE', 'CRISIS', 'STABLE\nTrend: Deteriorating'])
def test_source_label_cannot_reintroduce_a_context_claim_via_fallback(tmp_path, monkeypatch, state):
    monkeypatch.setattr('ingestion.wiki_history.WikiHistoryPuller', _no_network)
    monkeypatch.setattr('ingestion.social_sentiment.SocialSentimentPuller', _no_network)
    data = snapshot()
    data['latest_regime']['state'] = state + ' | Trend: Deteriorating'
    engine = _make_engine(_FakeOllamaClient('PCR proves maximum hedging demand.'),
                          _FakeEngine(_FakeConnection()), tmp_path)
    monkeypatch.setattr(engine, '_gather_market_snapshot', lambda: data)
    result = engine.generate_briefing(save=True)
    assert 'Trend: Deteriorating' not in result['content']
    assert 'Trend: Deteriorating' not in next(tmp_path.glob('hourly_*.md')).read_text()
    assert not result['publication_context_guard']['fallback']['passed']


@pytest.mark.parametrize('date_value, eligible', [('2026-10-05', True), ('2030-01-01', False)])
def test_vix_alias_uses_same_date_boundary(date_value, eligible, tmp_path):
    data = snapshot()
    data['volatility'] = {'VIX': {'value': 16.31, 'date': date_value}}
    assert (publication_context_facts(data)['vix_observation_dates']['^VIX'] is not None) == eligible
    text, receipt = annotate_publication_context('VIX 16.31 is implied volatility.', data)
    assert receipt['passed'] == eligible
    engine = _make_engine(_FakeOllamaClient(''), _FakeEngine(_FakeConnection()), tmp_path)
    for rendered in [engine._build_data_context(data), engine._generate_fallback_briefing(data)]:
        assert ('16.31' in rendered) == eligible


@pytest.mark.parametrize('kind', ['hourly', 'daily', 'weekly'])
@pytest.mark.parametrize('case', ['future_curve', 'factual_stable_confidence', 'dealer_absence_disclosure', 'supported_baseline'])
def test_controller_regression_at_publication_boundary(tmp_path, monkeypatch, kind, case):
    monkeypatch.setattr('ingestion.wiki_history.WikiHistoryPuller', _no_network)
    monkeypatch.setattr('ingestion.social_sentiment.SocialSentimentPuller', _no_network)

    class Recording(_FakeConnection):

        def __init__(self):
            super().__init__()
            self.publications = []

        def execute(self, stmt, params=None):
            if 'INSERT INTO market_briefings' in str(stmt):
                self.publications.append(params)
            return super().execute(stmt, params)
    data = snapshot(aligned=True)
    if case == 'future_curve':
        for info in data['volatility'].values():
            info['date'] = '2030-01-01'
        claim = 'As of 2030-01-01, VIX9D is below VIX3M; the VIX term structure is in contango.'
    elif case == 'factual_stable_confidence':
        data['latest_regime']['state'] = 'STABLE'
        claim = 'Reported regime state: STABLE, classifier confidence: 54.81%.'
    elif case == 'dealer_absence_disclosure':
        claim = 'No dealer inventory is supplied in this snapshot.'
    else:
        claim = 'Reported regime state: FRAGILE. A prior comparator is unavailable.'
    conn = Recording()
    engine = _make_engine(_FakeOllamaClient(claim), _FakeEngine(conn), tmp_path)
    monkeypatch.setattr(engine, '_gather_market_snapshot', lambda: copy.deepcopy(data))
    result = engine.generate_briefing(kind, save=True)
    assert len(conn.publications) == 1
    assert result['content'] == conn.publications[0]['content']
    assert result['content'] in next(tmp_path.glob(kind + '_*.md')).read_text()
    assert json.loads(conn.publications[0]['snap'])['publication_context_guard'] == result['publication_context_guard']
    if case == 'future_curve':
        assert 'contango' not in result['content'], 'Future-dated curve persists to mocked DB and file'
        assert not result['publication_context_guard']['passed']
    else:
        assert result['publication_context_guard']['passed'], 'Legitimate source description/disclosure removed by context guard'
        if case == 'supported_baseline':
            assert claim in result['content']


@pytest.mark.parametrize('claim', ['VIX is 16.', 'VIX: 16', 'VIX reads 16.', 'VIX at 16.'])
def test_ineligible_bare_integer_vix_reading_is_withheld(claim):
    data = snapshot()
    data['volatility']['^VIX']['date'] = '2030-01-01'
    assert not annotate_publication_context(claim, data)[1]['passed']


@pytest.mark.parametrize('claim', [
    'Treasury term structure is inverted, while VIX is 16.3.',
    'State: STABLE, while inventory remained flat.',
])
def test_comma_joined_independent_subjects_survive(claim):
    data = snapshot()
    data['latest_regime']['state'] = 'STABLE'
    assert annotate_publication_context(claim, data)[0] == claim


def test_missing_third_tenor_does_not_authorize_full_curve():
    data = snapshot()
    del data['volatility']['^VIX']
    _, receipt = annotate_publication_context(
        'VIX9D is below VIX3M; the term structure is in contango.', data)
    assert not receipt['passed']
    assert 'incompatible_vix_observation_dates' in receipt['reasons']


@pytest.mark.parametrize('claim', ['$500', '$10B', 'Share price: $500.'])
def test_isolated_currency_amount_is_not_itself_a_transfer(claim):
    from ollama.number_grounding import check_publication_claims
    assert check_publication_claims(claim)['passed']


@pytest.mark.parametrize('claim', ['VIX was 16.', 'VIX closed at 16.', 'VIX stood at 16.', 'VIX of 16.'])
def test_ineligible_past_tense_vix_levels_are_withheld(claim):
    data = snapshot()
    data['volatility']['^VIX']['date'] = '2030-01-01'
    assert not annotate_publication_context(claim, data)[1]['passed']


@pytest.mark.parametrize('claim', ['Higher confidence establishes stability.', 'Confidence shows stability.',
    'VIX9D is below VIX3M, while the term structure is in contango.'])
def test_reviewed_causal_and_curve_continuations_are_rejected(claim):
    assert not annotate_publication_context(claim, snapshot())[1]['passed']


@pytest.mark.parametrize('claim', [
    'State: STABLE, and inventory remained flat.',
    'Treasury term structure is inverted, although VIX is 16.3.',
    'Primary dealers hold long positions in Treasury bills.',
    'Dealers reduced Treasury inventory.',
    'Credit conditions improved.',
    'Working capital conditions remained unchanged.',
    'VIX is 16.3 and VIX3M is unavailable.',
])
def test_explicit_unrelated_subjects_are_not_regime_or_pcr_inferences(claim):
    data = snapshot()
    del data['volatility']['^VIX3M']
    assert annotate_publication_context(claim, data)[0] == claim


def test_aligned_pair_continuation_still_needs_full_curve_evidence():
    data = snapshot()
    del data['volatility']['^VIX']
    text, receipt = annotate_publication_context(
        'VIX9D is below VIX3M, while the term structure is in contango.', data)
    assert not receipt['passed']
    assert 'contango' not in text


def test_treasury_topic_does_not_launder_pcr_gamma_claim():
    assert not annotate_publication_context(
        'Treasury dealers are short gamma because PCR is high.', snapshot())[1]['passed']


@pytest.mark.parametrize('conjunction', ['BUT', 'However'])
def test_case_variants_do_not_reset_quantitative_confidence_context(conjunction):
    claim = f'A classifier confidence of 55% does not predict a crash, {conjunction} markets will rally next week.'
    assert not annotate_publication_context(claim, snapshot())[1]['passed']


@pytest.mark.parametrize('claim', ['VIX surged to 30.', 'VIX reached 25.', 'VIX is currently 16.'])
def test_ineligible_integer_vix_reading_does_not_depend_on_reporting_verb(claim):
    data = snapshot()
    data['volatility']['^VIX']['date'] = '2030-01-01'
    assert not annotate_publication_context(claim, data)[1]['passed']


@pytest.mark.parametrize('claim', [
    'VIX is 16.3; overall term structure is in contango.',
    'VIX is 16.3; however, the term structure is in contango.',
    'Reported classifier confidence is 85%. Equities will plunge next week.',
    'Confidence is 85%; markets will crash.',
])
def test_adverbial_curve_continuations_and_categorical_predictions_are_withheld(claim):
    text, receipt = annotate_publication_context(claim, snapshot())
    assert not receipt['passed']
    assert 'contango' not in text
    assert 'will plunge' not in text
    assert 'will crash' not in text


@pytest.mark.parametrize('claim', [
    'Hedge funds increased equity allocations.',
    'Gold acted as an inflation hedge.',
    'Realized volatility is unavailable.',
    'Realized volatility is unmeasured.',
])
def test_non_options_nouns_and_explicit_realized_limits_survive(claim):
    from ollama.number_grounding import check_publication_claims
    assert check_publication_claims(claim)['passed']


@pytest.mark.parametrize('claim', [
    'Commodity dealers reduced inventory.',
    'Corporate bond dealers hold short positions.',
])
def test_explicit_non_options_dealer_subjects_survive(claim):
    assert annotate_publication_context(claim, snapshot())[0] == claim


@pytest.mark.parametrize('claim', [
    'Hedge funds are buying puts.',
    'Realized volatility is unavailable, but VIX measures realized volatility.',
])
def test_fixed_noun_and_limit_disclosures_do_not_launder_options_or_realized_claims(claim):
    from ollama.number_grounding import check_publication_claims
    assert not check_publication_claims(claim)['passed']


@pytest.mark.parametrize('claim', [
    'Traders should reduce equity exposure.',
    'Investors must close positions.',
    'We forecast a severe recession.',
    'Biotech stocks such as DVN rallied 3%.',
])
def test_latest_review_action_forecast_and_taxonomy_forms_are_rejected(claim):
    assert not annotate_publication_context(claim, snapshot())[1]['passed']


@pytest.mark.parametrize('claim', ['VIX 3-month is 18.03.', 'VIX three-month is 18.03.',
                                     'VIX 9-day is 12.95.', 'VIX nine-day is 12.95.'])
def test_inverted_tenor_phrase_cannot_bind_to_eligible_spot(claim):
    data = snapshot()
    for tenor in ['^VIX3M', '^VIX9D']:
        data['volatility'][tenor]['date'] = '2030-01-01'
    assert not annotate_publication_context(claim, data)[1]['passed']


def test_missing_short_tenor_never_authorizes_full_vix_curve():
    data = snapshot(aligned=True)
    del data['volatility']['^VIX9D']
    for info in data['volatility'].values():
        info['date'] = '2026-10-02'
    text, receipt = annotate_publication_context('The VIX term structure is in contango.', data)
    assert not receipt['passed']
    assert 'contango' not in text


@pytest.mark.parametrize('claim', ['Airlines are hedging fuel costs.',
                                     'Gold acted as a hedge against inflation.'])
def test_explicit_commercial_hedge_subjects_are_not_pcr_intent(claim):
    from ollama.number_grounding import check_publication_claims
    assert check_publication_claims(claim)['passed']


@pytest.mark.parametrize('claim', [
    'Airlines are hedging fuel costs because PCR is high.',
    'Airlines are buying puts to hedge fuel costs.',
    'Biotech stocks excluding DVN rallied 3%, but DVN is a biotech company.',
])
def test_unrelated_subject_does_not_launder_protected_assertion(claim):
    from ollama.number_grounding import check_publication_claims
    assert not (annotate_publication_context(claim, snapshot())[1]['passed']
                and check_publication_claims(claim)['passed'])


@pytest.mark.parametrize('claim', ['VIX 3-month is 18.03.', 'VIX nine-day is 12.95.',
                                     'Traders reduced equity exposure.',
                                     'We do not forecast a severe recession.'])
def test_tenor_aliases_and_descriptive_action_forecast_limits_survive(claim):
    assert annotate_publication_context(claim, snapshot())[0] == claim


def test_pair_in_same_clause_cannot_authorize_a_full_curve():
    data = snapshot()
    del data['volatility']['^VIX']
    text, receipt = annotate_publication_context(
        'VIX9D is below VIX3M and the term structure is in contango.', data)
    assert not receipt['passed']
    assert 'contango' not in text


@pytest.mark.parametrize('claim', ['3M VIX is 18.03.', '9D VIX is 12.95.'])
def test_inverted_acronym_cannot_borrow_spot_eligibility(claim):
    data = snapshot()
    for tenor in ['^VIX3M', '^VIX9D']:
        data['volatility'][tenor]['date'] = '2030-01-01'
    assert not annotate_publication_context(claim, data)[1]['passed']


@pytest.mark.parametrize('claim', ['VIX 3-month is unavailable at snapshot cutoff.',
                                     'VIX 9-day is unavailable at snapshot cutoff.'])
def test_maturity_number_is_not_an_unavailable_reading(claim):
    data = snapshot()
    for tenor in ['^VIX3M', '^VIX9D']:
        data['volatility'][tenor]['date'] = '2030-01-01'
    assert annotate_publication_context(claim, data)[0] == claim


@pytest.mark.parametrize('claim', ['Operating cash flow grew.', 'Free cash flow was positive.',
    'Airlines hedge fuel prices.', 'Exporters hedge currency fluctuations.',
    'Firms hedge interest rates.'])
def test_accounting_and_commercial_topics_are_not_cross_market_transfers_or_pcr_intent(claim):
    from ollama.number_grounding import check_publication_claims
    assert check_publication_claims(claim)['passed']


@pytest.mark.parametrize('claim', [
    'Operating cash flow grew, but $800B flowed into crypto.',
    'Firms hedge interest rates because PCR is high.',
])
def test_accounting_and_commercial_topics_do_not_license_protected_claims(claim):
    from ollama.number_grounding import check_publication_claims
    assert not check_publication_claims(claim)['passed']
