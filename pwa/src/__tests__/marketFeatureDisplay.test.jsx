import React from 'react';
import vm from 'node:vm';
import viewSource from '../views/WatchlistAnalysis.jsx?raw';
import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import Card, { presentScalar, marketEvidenceModel } from '../components/ExactMarketFeatureCard.jsx';

afterEach(cleanup);
const payload = (ticker = 'SPY') => ({
    ticker,
    market_features: {
        schema: 'watchlist-existing-evidence-v1', ticker,
        daily: {
            status: 'observed', source: 'grid', as_of: '2020-01-02',
            reason: 'existing_scalar_evidence', returned_row_count: 2,
            rows: [{ as_of: '2020-01-01', value: 0 }, { as_of: '2020-01-02', value: 104.125 }],
            freshness: { status: 'unknown', reason: 'freshness_policy_not_supplied' },
            served_from_cache: true,
        },
        briefing_receipt: null,
    },
});

describe('existing daily evidence card', () => {
    it('shows actual scalars, original as-of/source and explicit unknown freshness/intraday absence', () => {
        render(<Card analysis={payload()} ticker="SPY" />);
        expect(screen.getByText(/Source: grid/).textContent).toContain('2020-01-02');
        expect(screen.getByText(/Source: grid/).textContent).toContain('Cached response');
        expect(screen.getByText(/Freshness:/).textContent).toContain('unknown');
        expect(screen.getByText(/Opening range/).textContent).toContain('qualified_intraday_result_absent');
        expect(screen.getByText(/latest returned: 104.125/)).toBeTruthy();
        expect(screen.getByText('0')).toBeTruthy();
        expect(screen.getByText('104.125')).toBeTruthy();
        expect(screen.getByText(/No briefing receipt supplied/)).toBeTruthy();
    });

    it('preserves zero versus null, declared strings and has no fraction/float conversion', () => {
        expect(presentScalar(0)).toBe('0');
        expect(presentScalar(null)).toBe('Unavailable');
        expect(presentScalar('1/3')).toBe('1/3');
        expect(presentScalar('104.00000000000000000100')).toBe('104.00000000000000000100');
        expect(presentScalar(Infinity)).toBe('Unsupported scalar');
    });

    it('rejects a stale cross-ticker or unknown-schema payload without showing old values/reason', () => {
        const old = payload('SPY');
        old.market_features.briefing_receipt = { reason: 'old_spy_reason' };
        const { rerender } = render(<Card analysis={old} ticker="QQQ" overview={{ reason: 'old_overview_reason' }} />);
        expect(screen.queryByText('104.125')).toBeNull();
        expect(screen.queryByText(/old_spy_reason|old_overview_reason/)).toBeNull();
        expect(screen.getByText(/no matching response/)).toBeTruthy();
        rerender(<Card analysis={payload('QQQ')} ticker="QQQ" />);
        expect(screen.getByText('104.125')).toBeTruthy();
        const invalid = payload('QQQ');
        invalid.market_features.schema = 'unrecognized';
        expect(marketEvidenceModel(invalid, 'QQQ')).toBeNull();
    });

    it('shows supplied negative state and reason, never current or eligible', () => {
        const supplied = payload();
        supplied.market_features.daily.status = 'stale';
        supplied.market_features.daily.reason = 'price_section_stale';
        supplied.market_features.daily.freshness = { status: 'stale', reason: 'declared_price_section_stale' };
        render(<Card analysis={supplied} ticker="SPY" />);
        expect(screen.getByText(/Daily evidence: stale/)).toBeTruthy();
        expect(screen.getByText(/^price_section_stale · Returned rows: 2$/)).toBeTruthy();
        expect(screen.getByText(/^Freshness: stale — declared_price_section_stale$/)).toBeTruthy();
        expect(screen.queryByText(/eligible|Daily evidence: current/)).toBeNull();
    });

    it('keeps missing briefing explicit and labels a synthetic overview transport reason separately', () => {
        render(<Card analysis={payload()} ticker="SPY" overview={{ reason: 'synthetic_source_unavailable' }} />);
        expect(screen.getByText(/No briefing receipt supplied/)).toBeTruthy();
        expect(screen.getByText(/Overview reason:/).textContent).toContain('synthetic_source_unavailable');
        expect(screen.queryByText(/Briefing receipt status:/)).toBeNull();
    });

    it('retains original supplied narrative reason but does not claim latest briefing', () => {
        const supplied = payload();
        supplied.market_features.briefing_receipt = { status: 'withheld', reason: 'unsupported_money_transfer', as_of: '2020-01-02' };
        render(<Card analysis={supplied} ticker="SPY" />);
        expect(screen.getByText(/Briefing receipt status:/).textContent).toContain('unsupported_money_transfer');
        expect(screen.queryByText(/latest briefing/i)).toBeNull();
    });
});

// Execute the actual production callback bodies against deferred API promises.
// This avoids reproducing request-generation logic in a cosmetic mock.
function deferredAnalysisEnvironment() {
    const preamble = viewSource.slice(viewSource.indexOf('        let active = true;'), viewSource.indexOf('        // Reset state for new ticker'));
    const initial = viewSource.slice(viewSource.indexOf('        api.getTickerAnalysis(ticker, period).then'), viewSource.indexOf('        // Phase 2: Fetch AI overview'));
    const cleanup = viewSource.slice(viewSource.indexOf('        return () => {\n            active = false;'), viewSource.indexOf('    }, [ticker]);'));
    const marker = '    const handlePeriodChange = useCallback(async (newPeriod) => {';
    const start = viewSource.indexOf(marker) + marker.length;
    const periodBody = viewSource.slice(start, viewSource.indexOf('    }, [ticker, period]);', start));
    const pending = [], errors = [], priceLoading = [];
    let state = null;
    const context = vm.createContext({
        ticker: 'SPY', period: '3M', periodRequestRef: { current: 0 }, currentTickerRef: { current: 'SPY' }, analysisPeriodRef: { current: '3M' },
        api: { getTickerAnalysis: () => { let resolve, reject; const promise = new Promise((a,b) => { resolve=a; reject=b; }); pending.push({ promise, resolve, reject }); return promise; } },
        setData: next => { state = typeof next === 'function' ? next(state) : next; },
        setDataLoading: () => {}, setPriceLoading: value => priceLoading.push(value), setError: value => errors.push(value), setPeriod: () => {},
    });
    vm.runInContext(`function beginInitial(){${preamble}${initial}${cleanup}} async function changePeriod(newPeriod){${periodBody}}`, context);
    return { context, pending, errors, priceLoading, get state(){ return state; } };
}
const analysisResult = (period, value) => ({ ...payload(), period, options: ['same-ticker-option'],
    price_history: [{ date: '2020-01-02', value }], price_source: 'grid', availability: { price: 'available' },
    market_features: { ...payload().market_features, daily: { ...payload().market_features.daily, rows: [{ as_of: '2020-01-02', value }] } },
});
const settleCallbacks = async () => { await Promise.resolve(); await Promise.resolve(); };

describe('initial and period request ordering from actual source', () => {
    it.each([true, false])('preserves selected-period prices with initialFirst=%s and loads nonperiod metadata', async (initialFirst) => {
        const e = deferredAnalysisEnvironment(); e.context.beginInitial(); const newer = e.context.changePeriod('1M');
        if (initialFirst) {
            e.pending[0].resolve(analysisResult('3M', 33)); await settleCallbacks();
            expect(e.state.options).toEqual(['same-ticker-option']);
            expect(e.state.market_features).toBeNull();
        }
        e.pending[1].resolve(analysisResult('1M', 11)); await newer;
        if (!initialFirst) { e.pending[0].resolve(analysisResult('3M', 33)); await settleCallbacks(); }
        expect(e.state.period).toBe('1M');
        expect(e.state.market_features.daily.rows[0].value).toBe(11);
    });

    it.each([null, { price: 'unavailable' }, { price: 'unavailable', options: 'failed' },
             { price: 'available', options: 'unavailable', regime: 'available', related_features: 'available' }])(
        'preserves complete fresh availability and existing per-field declarations with previous=%s', async (previousMap) => {
            const e = deferredAnalysisEnvironment();
            if (previousMap != null) e.context.setData({ ticker: 'SPY', availability: previousMap, options: ['previous-option'] });
            const newer = e.context.changePeriod('1M');
            const refreshed = { ...analysisResult('1M', 11), availability: {
                price: 'available', options: 'unavailable', regime: 'available', related_features: 'available',
            } };
            e.pending[0].resolve(refreshed); await newer;
            expect(JSON.parse(JSON.stringify(e.state.availability))).toEqual({
                ...refreshed.availability, ...previousMap, price: refreshed.availability.price,
            });
            expect(e.state.market_features.daily.rows[0].value).toBe(11);
            expect(e.state.options).toEqual(previousMap == null ? ['same-ticker-option'] : ['previous-option']);
        });

    it('ignores stale initial errors and old period finally writes', async () => {
        const e = deferredAnalysisEnvironment(); e.context.beginInitial(); const first = e.context.changePeriod('1M'), second = e.context.changePeriod('1Y');
        e.pending[1].reject(new Error('old-period-error')); await first;
        expect(e.priceLoading[e.priceLoading.length - 1]).toBe(true);
        e.pending[2].resolve(analysisResult('1Y', 22)); await second;
        e.pending[0].reject(new Error('old-initial-error')); await settleCallbacks();
        expect(e.state.period).toBe('1Y');
        expect(e.errors).toEqual([null]);
    });

    it('cleanup invalidates both outstanding initial and period callbacks', async () => {
        const e = deferredAnalysisEnvironment(); const stop = e.context.beginInitial(); const later = e.context.changePeriod('1M');
        stop(); e.pending[0].resolve(analysisResult('3M', 33)); e.pending[1].resolve(analysisResult('1M', 11)); await later; await settleCallbacks();
        expect(e.state).toBeNull();
        expect(e.priceLoading).toEqual([true]);
    });
});

// Couple actual request callbacks to the real Card, not only the stored DTO.
describe('callback state consumed by the real evidence card', () => {
    it('renders the selected period in both response orders and rejects late results after cleanup', async () => {
        const coherentResult = (period, value, source, asOf, reason) => {
            const rows = [{ as_of: asOf, value }, { as_of: asOf, value: null }];
            return {
                ticker: 'SPY', period, options: ['same-ticker-option'],
                price_history: rows.map(row => ({ date: row.as_of, value: row.value })),
                price_source: source, availability: { price: 'available' },
                market_features: {
                    ...payload().market_features,
                    daily: {
                        ...payload().market_features.daily,
                        source, as_of: asOf, reason, rows, returned_row_count: rows.length,
                    },
                },
            };
        };
        const old = coherentResult('3M', 33, 'grid', '2020-01-02', 'old_3M_reason');
        const selected = coherentResult('1M', 0, 'yfinance', '2021-02-03', 'selected_1M_reason');
        for (const initialFirst of [true, false]) {
            const e = deferredAnalysisEnvironment();
            e.context.beginInitial();
            const newer = e.context.changePeriod('1M');
            const card = render(<Card analysis={e.state} ticker="SPY" />);
            const showState = () => card.rerender(<Card analysis={e.state} ticker="SPY" />);
            const assertNoOldEvidence = () => {
                expect(card.queryByText('33')).toBeNull();
                expect(card.queryByText(/old_3M_reason|Source: grid|2020-01-02/)).toBeNull();
            };
            const assertSelectedEvidence = () => {
                expect(card.getByText(/^Source: yfinance · As of: 2021-02-03 · Cached response$/)).toBeTruthy();
                expect(card.getByText(/^selected_1M_reason · Returned rows: 2$/)).toBeTruthy();
                expect(card.getByText(/^Freshness: unknown — freshness_policy_not_supplied$/)).toBeTruthy();
                expect(card.getByText('0')).toBeTruthy();
                expect(card.getByText('Unavailable')).toBeTruthy();
                expect(card.getByText(/latest returned: Unavailable/)).toBeTruthy();
                assertNoOldEvidence();
            };
            if (initialFirst) {
                e.pending[0].resolve(old); await settleCallbacks(); showState();
                assertNoOldEvidence();
                expect(card.getByText(/no matching response/)).toBeTruthy();
                expect(card.queryByText('0')).toBeNull();
            }
            e.pending[1].resolve(selected); await newer; showState();
            assertSelectedEvidence();
            if (!initialFirst) {
                e.pending[0].resolve(old); await settleCallbacks(); showState();
                assertSelectedEvidence();
            }
            card.unmount();
        }
        const e = deferredAnalysisEnvironment();
        const stop = e.context.beginInitial();
        const newer = e.context.changePeriod('1M');
        const card = render(<Card analysis={e.state} ticker="SPY" />);
        stop(); e.context.currentTickerRef.current = 'QQQ';
        e.pending[0].resolve(old); await settleCallbacks();
        card.rerender(<Card analysis={e.state} ticker="QQQ" />);
        expect(card.getByText(/no matching response/)).toBeTruthy();
        e.pending[1].resolve(selected); await newer;
        card.rerender(<Card analysis={e.state} ticker="QQQ" />);
        expect(card.getByText(/no matching response/)).toBeTruthy();
        expect(card.queryByText(/old_3M_reason|selected_1M_reason|Source: grid|Source: yfinance/)).toBeNull();
        expect(card.queryByText('33')).toBeNull();
        expect(card.queryByText('0')).toBeNull();
        expect(e.state).toBeNull();
    });
});
