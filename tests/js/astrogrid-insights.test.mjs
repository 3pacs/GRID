import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import assert from 'node:assert/strict';
const source = readFileSync(new URL('../../astrogrid_web/lib/insights.js', import.meta.url), 'utf8');
const { insightsMarkup } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
test('missing or failed evidence never shows fabricated accuracy or a live call', () => {
    const text = insightsMarkup({ items: [], performance: { scored: 0, accuracy: null } });
    assert.match(text, /No validated forecast track record/);
    assert.doesNotMatch(text, /0\.00%|100\.00%/);
    assert.match(insightsMarkup(null, 'Service unavailable'), /Service unavailable/);
});
test('observations, stale source time, and unvalidated celestial context stay explicit', () => {
    const text = insightsMarkup({ items: [{ prediction_id: 'one', call: '<script>unsafe</script>',
        setup: 'No actor-backed trigger', stale: true, evidence: {
            symbol: 'BTC', as_of: '2026-11-06T01:00:00Z', coverage: .98,
            change_24h: .03, validation: 'unvalidated', entry: {instrument: 'BTCUSDT'},
        } }] });
    assert.match(text, /STALE/);
    assert.match(text, /Measured observation/);
    assert.match(text, /Binance BTCUSDT/);
    assert.match(text, /98\.00%/);
    assert.doesNotMatch(text, /<script>/);
    assert.match(text, /symbolic context/);
});
test('an observation does not claim that separate qualified forecasts are absent', () => {
    const text = insightsMarkup({items:[{setup:'No verified actor-driven liquidity trigger. No directional forecast issued.',
        evidence:{symbol:'BTC',kind:'observation',available:true}}]});
    assert.match(text,/Qualified directional forecasts appear as separate research contracts/);
    assert.doesNotMatch(text,/No verified actor|No directional forecast issued/);
    assert.match(insightsMarkup({items:[{setup:'No usable market evidence has been published for this asset.',
        evidence:{symbol:'BTC',available:false}}]}),/No usable market evidence/);
});
test('a scored forecast shows the declared outcome and signed return', () => {
    const text = insightsMarkup({ items: [{prediction_id: 'two', status: 'miss',
        evidence: {symbol: 'ETH', forecast: {target_price: 90, due_at: '2026-11-07T01:00:00Z'}},
        score: {signed_return: -.05}}], performance: {scored: 1, accuracy: 0}});
    assert.match(text, /Result: miss/);
    assert.match(text, /-5\.00%/);
    assert.match(text, /1 prospective forecasts scored/);
});
test('30m measured-flow rules and results remain separate from the 24h cohort', () => {
    const text = insightsMarkup({items:[{prediction_id:'flow', status:'miss', stale:true,
        evidence:{symbol:'BTC', entry:{source:'binance_spot_rest',instrument:'BTCUSDT'},
            forecast:{label:'30m spot taker-flow research', threshold:.001,target_price:100.1,
                due_at:'2026-11-06T06:00:10Z'}},score:{signed_return:-.0005}}],
        performance:{scored:0,accuracy:null}, performance_by_contract:{
            old:{label:'24h actor-flow research',scored:0,accuracy:null},
            short:{label:'30m spot taker-flow research',scored:1,accuracy:0}}});
    assert.match(text,/30m spot taker-flow research/);
    assert.match(text,/0\.10% directional move/);
    assert.match(text,/24h actor-flow research: 0 prospective forecasts scored/);
    assert.match(text,/30m spot taker-flow research: 1 prospective forecasts scored · 0\.00% hit rate/);
    assert.match(text,/global spot REST/);
    assert.match(text,/Result: miss/);
    assert.doesNotMatch(text,/STALE|24h candle coverage|1\.00% directional move/);
});
test('edge assessment distinguishes cost-aware historical replay from prospective forecasts', () => {
    const report = JSON.parse(readFileSync(new URL('../../astrogrid_web/research/spot-flow-edge-v1.json', import.meta.url)));
    const text = insightsMarkup({items:[], edge_assessment:report,
        prospective_cost_diagnostic:{scored:40,utc_dates:1,mean_gross_bps:5,mean_scenario_net_bps:-19}});
    assert.match(text,/No predictive edge after costs/);
    assert.match(text,/10,854 nonoverlapping events across 92 UTC days/);
    assert.match(text,/-23\.94 bp/);
    assert.match(text,/Always long/);
    assert.match(text,/Random direction \(expected\)/);
    assert.match(text,/40 scored forecasts across 1 UTC dates/);
    assert.match(text,/Insufficient independent prospective evidence/);
    assert.match(text,/Full results, per-asset comparisons and archive SHA256 checksums/);
    assert.doesNotMatch(text,/positive trading edge|validated alpha/);
});
