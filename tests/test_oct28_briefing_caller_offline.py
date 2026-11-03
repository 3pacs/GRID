"""Native full-module caller tests with invented input and no external effects.

Run only through the companion offline runner, --noconftest, without loading
the broad suite's DB fixtures. Gathered input/provider/sentiment boundaries
are doubles; caller, context renderer, fallback, guards and diagnostics are real.
"""
from copy import deepcopy
import socket
import sys
import types
from unittest.mock import Mock

import pytest
from ollama.market_briefing import MarketBriefingEngine
from ollama import number_grounding as guard


def invented_snapshot():
    return dict(timestamp='2026-10-28T20:00:00+00:00',date='2026-10-28',
        equities={},equity_volume={},rates={},credit={},commodities={},fx={},macro={},features={},
        volatility={'^VIX':dict(value=17.52,date='2026-10-28'),
                    '^VIX3M':dict(value=19.57,date='2026-10-28'),
                    '^VIX9D':dict(value=16.0,date='2026-10-28')})


@pytest.fixture(autouse=True)
def external_fences(monkeypatch):
    def refuse(*args,**kwargs):
        raise RuntimeError('synthetic offline boundary')
    for name,attributes in {
        'ingestion.wiki_history':{'WikiHistoryPuller':refuse},
        'ingestion.social_sentiment':{'SocialSentimentPuller':refuse},
        'intelligence.sentiment_scorer':{'compute_sentiment':refuse,'log_prediction':refuse},
        'intelligence.context_provider':{'build_full_context':lambda *a,**kw:''},
    }.items():
        module=types.ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules,name,module)
    monkeypatch.setattr(socket,'create_connection',refuse)


def generate(monkeypatch,text,snapshot=None):
    snapshot=invented_snapshot() if snapshot is None else snapshot
    before=deepcopy(snapshot)
    engine=MarketBriefingEngine.__new__(MarketBriefingEngine)
    engine.engine=object()  # Never get a default DB engine.
    engine.ollama=types.SimpleNamespace(chat=Mock(return_value=text))
    engine._gather_market_snapshot=lambda:deepcopy(snapshot)
    engine._save_briefing=Mock(side_effect=AssertionError('unexpected file publication'))
    engine._persist_to_db=Mock(side_effect=AssertionError('unexpected DB publication'))
    calls=[]
    claims,context=guard.check_publication_claims,guard.annotate_publication_context
    def check(value,*args,**kwargs):
        calls.append(('claim',value))
        return claims(value,*args,**kwargs)
    def annotate(value,*args,**kwargs):
        calls.append(('context',value))
        return context(value,*args,**kwargs)
    monkeypatch.setattr(guard,'check_publication_claims',check)
    monkeypatch.setattr(guard,'annotate_publication_context',annotate)
    result=engine.generate_briefing(save=False)
    assert calls[0][0]=='claim' and calls[1][0]=='context'
    if text is not None:
        assert calls[:2]==[('claim',text),('context',text)]
    assert snapshot==before
    engine._save_briefing.assert_not_called()
    engine._persist_to_db.assert_not_called()
    assert result['publication_status']['source_evidence_status']=='unknown'
    assert 'data_context' not in result
    return result,calls


@pytest.mark.parametrize('text',[
    'VIX 17.52 is below VIX3M 19.57.',
    'VIX3M 19.57 is above VIX 17.52.',
    'HYG/LQD clearly does not prove credit stress.',
    'HYG/LQD may reflect credit stress; independent evidence is unavailable.',
])
def test_native_acceptance_and_qualified_preservation(monkeypatch,text):
    result,_=generate(monkeypatch,text)
    assert result['publication_context_guard']['passed']
    assert result['publication_claim_guard']['passed']
    assert text in result['content']
    assert result['publication_status']['content_kind']=='narrative'


@pytest.mark.parametrize('text,reason',[
    ('VIX3M 19.56 is lower than VIX 17.52.','unbound_vix_comparison_reading'),
    ('VIX 17.52 versus VIX3M 19.57 declines over the horizon.','contradicted_vix_tenor_direction'),
    ('HYG/LQD clearly proves credit stress.','price_proxy_does_not_establish_credit_stress'),
    ('HYG/LQD confirms worsening credit stress.','price_proxy_does_not_establish_credit_stress'),
])
def test_native_context_omission(monkeypatch,text,reason):
    result,_=generate(monkeypatch,text)
    assert reason in result['publication_context_guard']['reasons']
    assert text not in result['content']
    assert 'Claim omitted' in result['content']
    assert result['publication_context_guard']['input_role']=='candidate'


def test_native_actor_rejection_uses_actual_fallback(monkeypatch):
    text='HYG/LQD confirms institutional positioning.'
    result,calls=generate(monkeypatch,text)
    assert result['publication_status']['content_kind']=='data_summary'
    assert result['publication_status']['reason']=='claim_guard_withheld'
    assert result['publication_claim_guard']['rejected_candidate']==text
    assert text not in result['content']
    assert 'Data summary only' in result['content']
    assert [name for name,_ in calls]==['claim','context','claim','context']
    assert result['publication_context_guard']['fallback']['input_role']=='data_summary'


def test_native_model_unavailable_uses_actual_fallback(monkeypatch):
    result,_=generate(monkeypatch,None)
    assert result['publication_status']['reason']=='model_unavailable'
    assert result['publication_status']['content_kind']=='data_summary'
    assert result['publication_context_guard']['input_role']=='data_summary'
    assert 'Model response unavailable' in result['content']


def test_native_missing_date_stays_refused(monkeypatch):
    snapshot=invented_snapshot(); snapshot['volatility']['^VIX']['date']=None
    result,_=generate(monkeypatch,'VIX 17.52 is below VIX3M 19.57.',snapshot)
    assert 'incompatible_vix_observation_dates' in result['publication_context_guard']['reasons']


def test_native_measured_oas_preserved(monkeypatch):
    snapshot=invented_snapshot()
    snapshot['credit']={'OAS_FIXTURE':dict(instrument='OAS_FIXTURE',measure='oas',unit='basis_points',
        value=323,date='2026-10-28',source_id='fixture_source',receipt_id='fixture_receipt',
        population='OAS_FIXTURE',expiry_scope='not_applicable')}
    text='Fixture source reports OAS of 323bp for October 28.'
    result,_=generate(monkeypatch,text,snapshot)
    assert result['publication_context_guard']['passed']
    assert text in result['content']


def oi_snapshot():
    snapshot=invented_snapshot()
    snapshot['options']={'spy_put_call':dict(value=2.0,date='2026-10-28',instrument='SPY',
        measure='put_open_interest/call_open_interest',unit='ratio',population='SPY',
        expiry_scope='all',source_id='synthetic_oi',receipt_id='invented_receipt')}
    return snapshot


@pytest.mark.parametrize('with_options',[False,True])
@pytest.mark.parametrize('text',[
    'Open interest, though this measures contract volume.',
    'The ratio reflects open interest, though this measures contract volume.',
    'Open interest measures trading volume.',
    'Open-interest represents contract volume.',
])
def test_native_oi_volume_conflation(monkeypatch,text,with_options):
    result,_=generate(monkeypatch,text,oi_snapshot() if with_options else invented_snapshot())
    assert 'open_interest_is_not_traded_volume' in result['publication_context_guard']['reasons']
    assert text not in result['content']


@pytest.mark.parametrize('text',[
    'Open interest is distinct from trading volume.',
    'Open interest does not measure contract volume.',
    'Open interest measures outstanding contracts.',
    'Open interest is not contract volume.',
])
def test_native_truthful_oi_distinctions(monkeypatch,text):
    result,_=generate(monkeypatch,text,oi_snapshot())
    assert result['publication_context_guard']['passed']
    assert text in result['content']


def test_native_declared_traded_volume_preserved(monkeypatch):
    snapshot=invented_snapshot()
    contract=dict(metric_id='invented_volume',definition_version='v1',measure='traded_volume',
                  unit='ratio',denominator='calls',cohort_id='synthetic-A',puts=5,calls=2)
    assertion={key:contract[key] for key in ('metric_id','definition_version','measure','unit','denominator','cohort_id')}
    snapshot['options']={'spy_put_call':dict(value=2.5,date='2026-10-28',measure='traded_volume',
        unit='ratio',denominator='calls',cohort_id='synthetic-A',metric_contract=contract,
        metric_assertion={**assertion,'value':2.5})}
    text='Declared traded volume ratio is 2.50.'
    result,_=generate(monkeypatch,text,snapshot)
    assert result['publication_context_guard']['passed']
    assert result['publication_claim_guard']['passed']
    assert text in result['content']
    # Real renderer must have selected the volume contract, not a legacy OI label.
    engine=MarketBriefingEngine.__new__(MarketBriefingEngine)
    rendered=engine._build_data_context(snapshot)
    assert 'Declared definition: SPY put/call traded volume' in rendered
    assert 'Definition: SPY put/call ratio, open interest' not in rendered


@pytest.mark.parametrize('with_credit',[False,True])
@pytest.mark.parametrize('text',[
    'HYG 80.00 versus LQD 100.00 implies a widening spread.',
    'HYG/LQD nominal prices show the spread is widening.',
])
def test_native_nominal_etf_widening_refused(monkeypatch,with_credit,text):
    snapshot=invented_snapshot()
    if with_credit:
        snapshot['credit']={'HYG':dict(value=80,date='2026-10-28',measure='ETF_price'),
                            'LQD':dict(value=100,date='2026-10-28',measure='ETF_price')}
    result,_=generate(monkeypatch,text,snapshot)
    if with_credit:
        assert {'unestablished_credit_spread_basis','missing_compatible_credit_comparator'} & set(result['publication_context_guard']['reasons'])
    else:
        assert 'unestablished_credit_spread_basis' in result['publication_context_guard']['reasons']
    assert text not in result['content']


def test_native_nominal_etf_description_preserved(monkeypatch):
    text='HYG 80.00 and LQD 100.00 are nominal ETF prices; basis interpretation remains unknown.'
    result,_=generate(monkeypatch,text)
    assert result['publication_context_guard']['passed']


def compatible_nominal_pair():
    snapshot=invented_snapshot()
    def leg(name,now,before):
        def reading(value,day,receipt):
            return dict(instrument=name,measure='ETF_price',unit='USD',value=value,date=day,
                        population=name,expiry_scope='not_applicable',source_id='invented_same_feed',
                        receipt_id=receipt,price_basis='unadjusted_close')
        return {**reading(now,'2026-10-28',name+'_current'),
                'previous':reading(before,'2026-10-27',name+'_previous')}
    snapshot['credit']={'HYG':leg('HYG',75,80),'LQD':leg('LQD',100,100)}
    return snapshot


@pytest.mark.parametrize('absolute',['','absolute '])
def test_native_supported_nominal_gap(monkeypatch,absolute):
    text=f'HYG and LQD {absolute}nominal price spread is widening.'
    result,_=generate(monkeypatch,text,compatible_nominal_pair())
    assert result['publication_context_guard']['passed']
    assert result['publication_claim_guard']['passed']
    assert text in result['content']


def test_native_supported_nominal_gap_tightening(monkeypatch):
    snapshot=compatible_nominal_pair(); snapshot['credit']['HYG']['value']=85
    text='HYG and LQD absolute nominal price spread is tightening.'
    result,_=generate(monkeypatch,text,snapshot)
    assert result['publication_context_guard']['passed']
    assert text in result['content']


@pytest.mark.parametrize('mutation',[
    'missing_receipt','missing_previous','different_dates','different_currency',
    'different_basis','missing_basis','different_source','wrong_direction','no_change',
    'different_window','one_sided_window','wrong_record_path',
])
def test_native_nominal_gap_requires_typed_compatible_arithmetic(monkeypatch,mutation):
    snapshot=compatible_nominal_pair(); lqd=snapshot['credit']['LQD']
    if mutation=='missing_receipt': lqd.pop('receipt_id')
    elif mutation=='missing_previous': lqd.pop('previous')
    elif mutation=='different_dates': lqd['date']='2026-10-27'
    elif mutation=='different_currency': lqd['unit']=lqd['previous']['unit']='EUR'
    elif mutation=='different_basis': lqd['price_basis']=lqd['previous']['price_basis']='adjusted_close'
    elif mutation=='missing_basis': lqd.pop('price_basis')
    elif mutation=='different_source': lqd['source_id']=lqd['previous']['source_id']='other_feed'
    elif mutation=='wrong_direction': snapshot['credit']['HYG']['value']=85
    elif mutation=='no_change': snapshot['credit']['HYG']['value']=80
    elif mutation=='different_window':
        for item in (lqd,lqd['previous']): item['window']='intraday'
        for item in (snapshot['credit']['HYG'],snapshot['credit']['HYG']['previous']): item['window']='session_close'
    elif mutation=='one_sided_window':
        for item in (lqd,lqd['previous']): item['window']='session_close'
    elif mutation=='wrong_record_path': snapshot['credit']['OTHER']=snapshot['credit'].pop('LQD')
    text='HYG and LQD nominal price spread is widening.'
    result,_=generate(monkeypatch,text,snapshot)
    assert not result['publication_context_guard']['passed']
    assert 'unestablished_credit_spread_basis' in result['publication_context_guard']['reasons']
    assert text not in result['content']


@pytest.mark.parametrize('text',[
    'HYG and LQD spread is widening.',
    'HYG and LQD credit spread is widening.',
    'HYG and LQD nominal price spread is widening and proves credit stress.',
    'HYG and LQD nominal price spread is widening and OAS is widening.',
])
def test_native_nominal_gap_cannot_lend_credit_interpretation(monkeypatch,text):
    result,_=generate(monkeypatch,text,compatible_nominal_pair())
    assert not result['publication_context_guard']['passed']
    assert text not in result['content']


@pytest.mark.parametrize('name',['S&P 500','S&amp;P 500','s&p 500'])
def test_native_index_name_token_is_not_quantity(name):
    text=f'{name}. Real quantities: 500 contracts, $500, 500%, 500.25.'
    mentions=guard.extract_numeric_mentions(text)
    assert [m.raw for m in mentions]==['500','$500','500%','500.25']
    for mention in mentions:
        assert text[mention.start:mention.end]==mention.raw
    assert [m.raw for m in guard.extract_numeric_mentions(text,include_small_integers=True)]==[m.raw for m in mentions]


@pytest.mark.parametrize('text',[
    'S&P 500%', 'S&P 500.25', 'S&P 500,000', 'S&P $500', '500 contracts',
])
def test_native_real_500_claims_are_still_extracted(text):
    mentions=guard.extract_numeric_mentions(text)
    assert len(mentions)==1
    assert mentions[0].value>=500
    assert text[mentions[0].start:mentions[0].end]==mentions[0].raw
    result=guard.check_numbers_grounded(text,'No numeric data supplied.')
    assert len(result.ungrounded)==1


def test_native_index_name_cannot_supply_grounding_for_500_quantity():
    result=guard.check_numbers_grounded('There are 500 contracts.','S&P 500 index name only.')
    assert len(result.ungrounded)==1
    assert result.ungrounded[0].raw=='500'


def test_native_sp500_caller_preserves_name_and_other_quantity(monkeypatch):
    text='S&P 500 index. Observation 17.52.'
    result,_=generate(monkeypatch,text)
    assert result['grounding']['ungrounded_count']==0
    assert text in result['content']
    assert result['publication_status']['source_evidence_status']=='unknown'


def eem_snapshot():
    snapshot=invented_snapshot()
    snapshot['fx']={'EEM':dict(value=42.5,date='2026-10-28',source='invented'),
                    'UUP':dict(value=28.5,date='2026-10-28',source='invented'),
                    'EURUSD=X':dict(value=1.125,date='2026-10-28',source='invented')}
    return snapshot


def test_native_eem_renderer_truthful_without_changing_input():
    snapshot=eem_snapshot(); before=deepcopy(snapshot)
    engine=MarketBriefingEngine.__new__(MarketBriefingEngine); engine.engine=object()
    rendered=engine._build_data_context(snapshot)
    assert snapshot==before
    assert '### EQUITY ETF PROXIES' in rendered
    assert 'equity_etf_proxies:EEM: raw value=42.5' in rendered
    assert 'emerging-market equity ETF; equity-price proxy, not FX' in rendered
    assert 'fx:EEM:' not in rendered
    assert 'fx:UUP: raw value=28.5' in rendered
    assert 'fx:EURUSD=X: raw value=1.125' in rendered
    assert set(snapshot['fx'])=={'EEM','UUP','EURUSD=X'}


def test_native_eem_fallback_truthful_preserves_legacy_record(monkeypatch):
    snapshot=eem_snapshot()
    result,_=generate(monkeypatch,None,snapshot)
    assert '## Equity ETF Proxies' in result['content']
    assert 'equity_etf_proxies:EEM:' in result['content']
    assert 'fx:EEM:' not in result['content']
    assert 'fx:UUP:' in result['content']
    assert 'fx:EURUSD=X:' in result['content']
    assert result['snapshot']['fx']==snapshot['fx']


def test_native_fx_only_display_is_unchanged():
    snapshot=eem_snapshot(); snapshot['fx'].pop('EEM')
    before=deepcopy(snapshot)
    rows=MarketBriefingEngine._publication_display_categories(snapshot)
    assert ('fx',snapshot['fx']) in rows
    assert not any(category=='equity_etf_proxies' for category,_ in rows)
    assert snapshot==before


@pytest.mark.parametrize('context',[
    'S&amp;P 500 XLE comparison',
    'S＆P 500 index',
    'S&P 500 XLE comparison',
    'S＆P\u00a0500 XLE comparison',
])
def test_native_index_variant_cannot_ground_quantity(context):
    assert guard.extract_numeric_mentions(context)==[]
    assert guard.extract_numeric_mentions(context,include_small_integers=True)==[]
    result=guard.check_numbers_grounded('There are 500 contracts.',context)
    assert len(result.ungrounded)==1
    assert result.ungrounded[0].raw=='500'


@pytest.mark.parametrize('text',[
    'S&amp;P 500 XLE comparison. Observation 17.52.',
    'S＆P 500 index. Observation 17.52.',
])
def test_native_index_variant_caller_has_no_name_claim(monkeypatch,text):
    result,_=generate(monkeypatch,text)
    assert result['grounding']['ungrounded_count']==0
    assert text in result['content']
    assert result['publication_status']['source_evidence_status']=='unknown'


@pytest.mark.parametrize('quantity',['500 contracts','500x','500 x','500%','500.25','500,000','$500'])
def test_native_unicode_name_preserves_quantity_and_original_offsets(quantity):
    text=f'Prefix Ω S＆P 500 XLE comparison; {quantity}.'
    mentions=guard.extract_numeric_mentions(text)
    assert len(mentions)==1
    expected=quantity.removesuffix(' contracts')
    assert mentions[0].raw==expected
    assert mentions[0].start==text.index(quantity)
    assert mentions[0].end==text.index(quantity)+len(expected)
    assert text[mentions[0].start:mentions[0].end]==expected
    assert len(guard.check_numbers_grounded(text,'No numeric evidence supplied.').ungrounded)==1


@pytest.mark.parametrize('text',['S&P 500x','S＆P 500 x','S&amp;P 500%','S＆P 500.25','S&P 500,000'])
def test_native_numeric_modifier_cannot_hide_in_name(text):
    mentions=guard.extract_numeric_mentions(text)
    assert len(mentions)==1
    assert mentions[0].value>=500
    assert text[mentions[0].start:mentions[0].end]==mentions[0].raw
