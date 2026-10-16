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


const storedContextResult = (ticker = 'SPY', cutoff = '2026-10-15T12:00:00+00:00', value = 0) => {
    const selection = {
        source_key: 'baltic_exchange', scope: 'index', ticker_or_cusip: 'BDI',
        series_id: 'baltic.bdi', expected_source_name: 'Baltic_Exchange',
        feature_id: 23, feature_name: 'baltic_dry_index',
    };
    return {
        ticker, period: '1M', price_history: [],
        market_features: {
            schema: 'watchlist-existing-evidence-v1', ticker,
            daily: { status: 'unknown', source: null, as_of: null, reason: 'no_returned_price_rows',
                rows: [], returned_row_count: 0, freshness: { status: 'unknown', reason: 'freshness_policy_not_supplied' } },
            briefing_receipt: null,
            stored_source_evidence: {
                schema: 'stored-source-snapshot-v1', ticker, as_of: cutoff,
                start_date: '2026-10-01', vintage_policy: 'LATEST_AS_OF',
                request_binding: { ticker, as_of: cutoff, start_date: '2026-10-01',
                    vintage_policy: 'LATEST_AS_OF', selections: [selection] },
                selections: [{ ...selection, status: 'available', reason: 'resolved_observation', value,
                    actual_source: { id: 7, name: 'Baltic_Exchange' },
                    obs_date: '2026-10-14', release_date: '2026-10-15', vintage_date: '2026-10-15',
                    freshness: { status: 'unknown', reason: 'freshness_policy_absent' },
                    conflict_flag: false, storage_clock: 'pull_date_not_provider_publication', lineage: 'resolved_source_not_raw_row' }],
            },
        },
    };
};

describe('stored-only source context consumer', () => {
    it('renders stored zero, declared storage dates/cutoff and unknown freshness separately from absent prices', () => {
        render(<Card analysis={storedContextResult()} ticker="SPY" />);
        expect(screen.getByText(/^Stored value: 0$/)).toBeTruthy();
        expect(screen.getByText(/Requested cutoff:/).textContent).toContain('2026-10-15T12:00:00+00:00');
        expect(screen.getByText(/Observation date:/).textContent).toContain('2026-10-14');
        expect(screen.getByText(/Resolved source:/).textContent).toContain('Baltic_Exchange');
        expect(screen.getByText(/^Freshness: unknown — freshness_policy_absent$/)).toBeTruthy();
        expect(screen.getByText(/Daily evidence: unknown/)).toBeTruthy();
        expect(screen.getByText(/Opening range/).textContent).toContain('qualified_intraday_result_absent');
    });

    it('keeps null, missing, source mismatch and unsupported-event values unavailable', () => {
        const supplied = storedContextResult();
        const stored = supplied.market_features.stored_source_evidence;
        const template = stored.selections[0];
        const rows = [
            { ...template, value: null },
            { source_key: 'sec_ftd', status: 'unavailable', reason: 'selection_missing', value: 999 },
            { ...template, source_key: 'fx_rates', expected_source_name: 'yfinance',
                actual_source: { id: 9, name: 'Other' }, value: 777 },
            { source_key: 'quiverquant', status: 'unavailable', reason: 'resolver_unsupported', value: 888 },
        ];
        stored.selections = rows;
        stored.request_binding.selections = rows.map(({ source_key, scope, ticker_or_cusip, series_id, expected_source_name, feature_id, feature_name }) =>
            ({ source_key, scope, ticker_or_cusip, series_id, expected_source_name, feature_id, feature_name }));
        render(<Card analysis={supplied} ticker="SPY" />);
        expect(screen.getAllByText(/^Stored value: Unavailable$/)).toHaveLength(4);
        expect(screen.getByText(/selection_missing/)).toBeTruthy();
        expect(screen.getByText(/resolver_unsupported/)).toBeTruthy();
        expect(screen.getByText(/invalid_source_context/)).toBeTruthy();
        expect(screen.queryByText(/Stored value: (777|888|999)/)).toBeNull();
    });

    it('displays explicit stale policy without replacing original observation dates or inventing freshness', () => {
        const supplied = storedContextResult();
        supplied.market_features.stored_source_evidence.selections[0].freshness = {
            status: 'stale', reason: 'exceeds_declared_age',
        };
        render(<Card analysis={supplied} ticker="SPY" />);
        expect(screen.getByText(/^Freshness: stale — exceeds_declared_age$/)).toBeTruthy();
        expect(screen.getByText(/Observation date:/).textContent).toContain('2026-10-14');
        expect(screen.queryByText(/Daily evidence: current|eligible|qualified value/i)).toBeNull();
    });

    it.each(['ticker', 'as_of', 'start_date', 'vintage_policy', 'selection'])('refuses mismatched stored request binding %s', field => {
        const supplied = storedContextResult();
        const stored = supplied.market_features.stored_source_evidence;
        if (field === 'selection') stored.selections[0].series_id = 'different.series';
        else stored.request_binding[field] = 'mismatch';
        render(<Card analysis={supplied} ticker="SPY" />);
        expect(screen.queryByText(/^Stored value:/)).toBeNull();
        expect(screen.getByText(/response_context_mismatch/)).toBeTruthy();
    });

    it.each([true, false])('couples actual callback state/cutoff to stored Card with initialFirst=%s', async initialFirst => {
        const e = deferredAnalysisEnvironment(); e.context.beginInitial();
        const newer = e.context.changePeriod('1M');
        const old = { ...storedContextResult('SPY', '2026-10-14T12:00:00+00:00', 33), period: '3M' };
        const selected = storedContextResult();
        const card = render(<Card analysis={e.state} ticker="SPY" />);
        const show = () => card.rerender(<Card analysis={e.state} ticker="SPY" />);
        if (initialFirst) {
            e.pending[0].resolve(old); await settleCallbacks(); show();
            expect(card.queryByText(/Stored value:/)).toBeNull();
            expect(card.queryByText(/2026-10-14T12:00:00/)).toBeNull();
        }
        e.pending[1].resolve(selected); await newer; show();
        expect(card.getByText(/^Stored value: 0$/)).toBeTruthy();
        if (!initialFirst) { e.pending[0].resolve(old); await settleCallbacks(); show(); }
        expect(card.getByText(/^Stored value: 0$/)).toBeTruthy();
        expect(card.getByText(/Requested cutoff:/).textContent).toContain('2026-10-15T12:00:00+00:00');
        expect(card.queryByText(/^Stored value: 33$/)).toBeNull();
        card.unmount();
    });

    it('refuses old-ticker stored response and reports absent request explicitly', () => {
        const supplied = storedContextResult();
        const { rerender } = render(<Card analysis={supplied} ticker="QQQ" />);
        expect(screen.queryByText(/Stored value:/)).toBeNull();
        const missing = storedContextResult('QQQ');
        const stored = missing.market_features.stored_source_evidence;
        Object.assign(stored, { as_of: null, start_date: null, vintage_policy: null, selections: [],
            status: 'unavailable', reason: 'stored_request_missing',
            request_binding: { ticker: 'QQQ', as_of: null, start_date: null, vintage_policy: null, selections: [] } });
        rerender(<Card analysis={missing} ticker="QQQ" />);
        expect(screen.getByText(/^Stored read: unavailable — stored_request_missing$/)).toBeTruthy();
        expect(screen.queryByText(/Stored value:/)).toBeNull();
    });
});


it('refuses populated stored context without an aware cutoff and explicit range/policy', () => {
    for (const [field, value] of [['as_of', null], ['as_of', '2026-10-15T12:00:00'],
        ['start_date', null], ['vintage_policy', 'unrecognized']]) {
        const supplied = storedContextResult();
        const stored = supplied.market_features.stored_source_evidence;
        stored[field] = value; stored.request_binding[field] = value;
        const card = render(<Card analysis={supplied} ticker="SPY" />);
        expect(card.queryByText(/^Stored value:/)).toBeNull();
        expect(card.getByText(/response_context_mismatch/)).toBeTruthy();
        card.unmount();
    }
});


describe('stored row UTC-day proof before primary value display', () => {
    it('withholds each literal C1 contradiction and preserves the supplied date diagnostics', () => {
        for (const [field, replacement] of [['obs_date', '2026-09-30'], ['obs_date', '2026-10-16'],
            ['release_date', '2026-10-16'], ['vintage_date', '2026-10-16']]) {
            const supplied = storedContextResult();
            supplied.market_features.stored_source_evidence.selections[0][field] = replacement;
            const card = render(<Card analysis={supplied} ticker="SPY" />);
            expect(card.queryByText(/^Stored value: 0$/)).toBeNull();
            expect(card.getByText(/^Stored value: Unavailable$/)).toBeTruthy();
            expect(card.getByText(/invalid_date_context/)).toBeTruthy();
            expect(card.getByText(/Observation date:/).textContent).toContain(replacement);
            expect(card.getByText(/^Freshness: unknown — freshness_policy_absent$/)).toBeTruthy();
            card.unmount();
        }
    });

    it('requires every row date to be present, well typed and a real calendar date', () => {
        for (const field of ['obs_date', 'release_date', 'vintage_date']) {
            for (const value of [undefined, null, '', false, 20261014, {}, [],
                '0000-01-01', '2026-02-29', '2026-02-30', '2026-13-01', '2026-10-00',
                '2026-10-32', '2026-1-2', '2026-10-14T00:00:00Z']) {
                const supplied = storedContextResult();
                supplied.market_features.stored_source_evidence.selections[0][field] = value;
                const card = render(<Card analysis={supplied} ticker="SPY" />);
                expect(card.queryByText(/^Stored value: 0$/), `${field}:${String(value)}`).toBeNull();
                expect(card.getByText(/invalid_date_context/)).toBeTruthy();
                card.unmount();
            }
        }
    });

    it('preserves inclusive range/cutoff equality, zero/null and independent storage date columns', () => {
        for (const [observed, release, vintage, value] of [
            ['2026-10-01', '2026-10-15', '2026-10-15', 0],
            ['2026-10-15', '2026-10-15', '2026-10-15', 0],
            ['2026-10-15', '0001-01-01', '2026-01-01', 0],
            ['2026-10-15', '2026-10-15', '2026-10-15', null],
        ]) {
            const supplied = storedContextResult();
            Object.assign(supplied.market_features.stored_source_evidence.selections[0],
                { obs_date: observed, release_date: release, vintage_date: vintage, value });
            const card = render(<Card analysis={supplied} ticker="SPY" />);
            expect(card.getByText(value === null ? /^Stored value: Unavailable$/ : /^Stored value: 0$/)).toBeTruthy();
            expect(card.queryByText(/invalid_date_context/)).toBeNull();
            expect(card.getByText(/^Status: available — resolved_observation$/)).toBeTruthy();
            expect(card.getByText(/Observation date:/).textContent).toContain(observed);
            expect(card.getByText(/^Freshness: unknown — freshness_policy_absent$/)).toBeTruthy();
            card.unmount();
        }
    });

    it('uses the UTC cutoff day for equivalent instants across local midnight', () => {
        for (const cutoff of ['2026-10-15T23:00:00Z', '2026-10-16T01:00:00+02:00',
            '2026-10-15T18:00:00-05:00']) {
            for (const [observed, allowed] of [['2026-10-15', true], ['2026-10-16', false]]) {
                const supplied = storedContextResult('SPY', cutoff);
                Object.assign(supplied.market_features.stored_source_evidence.selections[0],
                    { obs_date: observed, release_date: '2026-10-15', vintage_date: '2026-10-15' });
                const card = render(<Card analysis={supplied} ticker="SPY" />);
                if (allowed) expect(card.getByText(/^Stored value: 0$/)).toBeTruthy();
                else expect(card.queryByText(/^Stored value: 0$/)).toBeNull();
                expect(card.getByText(/Requested cutoff:/).textContent).toContain(cutoff);
                card.unmount();
            }
        }
    });

    it('accepts real leap days and day-precision equality without inferring a publication instant', () => {
        for (const cutoff of ['2024-03-01T00:00:00Z', '2024-03-01T00:00:00.000001+00:00']) {
            const supplied = storedContextResult('SPY', cutoff);
            const stored = supplied.market_features.stored_source_evidence;
            stored.start_date = '2024-02-01'; stored.request_binding.start_date = '2024-02-01';
            Object.assign(stored.selections[0], { obs_date: '2024-02-29', release_date: '2024-03-01', vintage_date: '2024-03-01' });
            const card = render(<Card analysis={supplied} ticker="SPY" />);
            expect(card.getByText(/^Stored value: 0$/)).toBeTruthy();
            expect(card.getByText(/Observation date:/).textContent).toContain('2024-02-29');
            expect(card.getByText(/^Freshness: unknown — freshness_policy_absent$/)).toBeTruthy();
            card.unmount();
        }
    });

    it('refuses paired invalid request dates and never rolls nonexistent dates into a proof', () => {
        for (const [field, value] of [['start_date', '2026-02-30'], ['as_of', '2026-02-30T12:00:00Z'],
            ['as_of', '2026-10-15T24:00:00Z'], ['as_of', '2026-10-15T12:60:00Z']]) {
            const supplied = storedContextResult();
            const stored = supplied.market_features.stored_source_evidence;
            stored[field] = value; stored.request_binding[field] = value;
            const card = render(<Card analysis={supplied} ticker="SPY" />);
            expect(card.queryByText(/^Stored value:/)).toBeNull();
            expect(card.getByText(/response_context_mismatch/)).toBeTruthy();
            card.unmount();
        }
    });
});

// Current-view stored-only seam: real client, component, callbacks and Card;
// every external request is intercepted at the existing _fetch boundary.
import { act, fireEvent } from '@testing-library/react';
import { vi } from 'vitest';
import WatchlistAnalysis from '../views/WatchlistAnalysis.jsx';
import { api, snapshotStoredRequest, storedAnalysisMatchesRequest } from '../api.js';
const actualStoredRequest = {
  "selections": [
    {
      "source_key": "fx_rates",
      "scope": "currency",
      "ticker_or_cusip": "EUR",
      "series_id": "FX:EUR:close",
      "expected_source_name": "yfinance",
      "feature_name": "synthetic_fx_eur",
      "feature_id": 8101
    },
    {
      "source_key": "ag_commodity_futures",
      "scope": "contract",
      "ticker_or_cusip": "CC=F",
      "series_id": "YF:CC=F:close",
      "expected_source_name": "YFINANCE_COMMODITY_FUTURES",
      "feature_name": "synthetic_cc",
      "feature_id": 8102
    },
    {
      "source_key": "finra_short_volume",
      "scope": "ticker",
      "ticker_or_cusip": "AAPL",
      "series_id": "finra:short_volume:AAPL",
      "expected_source_name": "FINRA_SHORT_VOLUME",
      "feature_name": "synthetic_short_aapl",
      "feature_id": 8103
    },
    {
      "source_key": "sec_ftd",
      "scope": "cusip",
      "ticker_or_cusip": "037833100",
      "series_id": "sec:ftd_balance:037833100",
      "expected_source_name": "SEC_FTD",
      "feature_name": "synthetic_ftd_037833100",
      "feature_id": 8104
    },
    {
      "source_key": "nyfed_gscpi",
      "scope": "global",
      "ticker_or_cusip": null,
      "series_id": "NYFED:gscpi",
      "expected_source_name": "NYFED_GSCPI",
      "feature_name": "synthetic_gscpi",
      "feature_id": 8105
    },
    {
      "source_key": "baltic_exchange",
      "scope": "index",
      "ticker_or_cusip": "BDI",
      "series_id": "baltic.bdi",
      "expected_source_name": "Baltic_Exchange",
      "feature_name": "synthetic_bdi",
      "feature_id": 8106
    }
  ],
  "as_of": "2026-10-15T12:00:00+00:00",
  "start_date": "2026-10-01",
  "vintage_policy": "LATEST_AS_OF"
};
const actualStoredResponse = {
  "ticker": "AAPL",
  "period": "1M",
  "market_features": {
    "schema": "watchlist-existing-evidence-v1",
    "ticker": "AAPL",
    "daily": {
      "status": "unknown",
      "reason": "no_returned_price_rows",
      "source": null,
      "section_availability": null,
      "rows": [],
      "returned_row_count": 0,
      "as_of": null,
      "freshness": {
        "status": "unknown",
        "reason": "freshness_policy_not_supplied"
      },
      "served_from_cache": false,
      "representation": "existing_scalar_not_qualified_intraday"
    },
    "intraday": {
      "status": "unavailable",
      "reason": "qualified_intraday_result_absent",
      "opening_range": null,
      "trade_vwap": null,
      "hlc3_volume_proxy": null
    },
    "briefing_receipt": null,
    "stored_source_evidence": {
      "schema": "stored-source-snapshot-v1",
      "as_of": "2026-10-15T12:00:00+00:00",
      "start_date": "2026-10-01",
      "vintage_policy": "LATEST_AS_OF",
      "selections": [
        {
          "source_key": "fx_rates",
          "scope": "currency",
          "ticker_or_cusip": "EUR",
          "series_id": "FX:EUR:close",
          "feature_id": 8101,
          "feature_name": "synthetic_fx_eur",
          "expected_source_name": "yfinance",
          "status": "available",
          "reason": "resolved_observation",
          "value": 1.1,
          "obs_date": "2026-10-13",
          "release_date": "2026-10-14",
          "vintage_date": "2026-10-14",
          "actual_source": {
            "id": 9101,
            "name": "yfinance"
          },
          "conflict_flag": false,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "freshness_policy_absent"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        },
        {
          "source_key": "ag_commodity_futures",
          "scope": "contract",
          "ticker_or_cusip": "CC=F",
          "series_id": "YF:CC=F:close",
          "feature_id": 8102,
          "feature_name": "synthetic_cc",
          "expected_source_name": "YFINANCE_COMMODITY_FUTURES",
          "status": "available",
          "reason": "resolved_observation",
          "value": 7000.0,
          "obs_date": "2026-10-13",
          "release_date": "2026-10-14",
          "vintage_date": "2026-10-14",
          "actual_source": {
            "id": 9102,
            "name": "YFINANCE_COMMODITY_FUTURES"
          },
          "conflict_flag": false,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "freshness_policy_absent"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        },
        {
          "source_key": "finra_short_volume",
          "scope": "ticker",
          "ticker_or_cusip": "AAPL",
          "series_id": "finra:short_volume:AAPL",
          "feature_id": 8103,
          "feature_name": "synthetic_short_aapl",
          "expected_source_name": "FINRA_SHORT_VOLUME",
          "status": "available",
          "reason": "resolved_observation",
          "value": 0.0,
          "obs_date": "2026-10-13",
          "release_date": "2026-10-14",
          "vintage_date": "2026-10-14",
          "actual_source": {
            "id": 9103,
            "name": "FINRA_SHORT_VOLUME"
          },
          "conflict_flag": false,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "freshness_policy_absent"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        },
        {
          "source_key": "sec_ftd",
          "scope": "cusip",
          "ticker_or_cusip": "037833100",
          "series_id": "sec:ftd_balance:037833100",
          "feature_id": 8104,
          "feature_name": "synthetic_ftd_037833100",
          "expected_source_name": "SEC_FTD",
          "status": "available",
          "reason": "resolved_observation",
          "value": 25.0,
          "obs_date": "2026-10-13",
          "release_date": "2026-10-14",
          "vintage_date": "2026-10-14",
          "actual_source": {
            "id": 9104,
            "name": "SEC_FTD"
          },
          "conflict_flag": false,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "freshness_policy_absent"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        },
        {
          "source_key": "nyfed_gscpi",
          "scope": "global",
          "ticker_or_cusip": null,
          "series_id": "NYFED:gscpi",
          "feature_id": 8105,
          "feature_name": "synthetic_gscpi",
          "expected_source_name": "NYFED_GSCPI",
          "status": "available",
          "reason": "resolved_observation",
          "value": -0.25,
          "obs_date": "2026-10-13",
          "release_date": "2026-10-14",
          "vintage_date": "2026-10-14",
          "actual_source": {
            "id": 9105,
            "name": "NYFED_GSCPI"
          },
          "conflict_flag": false,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "freshness_policy_absent"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        },
        {
          "source_key": "baltic_exchange",
          "scope": "index",
          "ticker_or_cusip": "BDI",
          "series_id": "baltic.bdi",
          "feature_id": 8106,
          "feature_name": "synthetic_bdi",
          "expected_source_name": "Baltic_Exchange",
          "status": "available",
          "reason": "resolved_observation",
          "value": 2000.0,
          "obs_date": "2026-10-13",
          "release_date": "2026-10-14",
          "vintage_date": "2026-10-14",
          "actual_source": {
            "id": 9106,
            "name": "Baltic_Exchange"
          },
          "conflict_flag": false,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "freshness_policy_absent"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        },
        {
          "source_key": "quiverquant",
          "scope": null,
          "ticker_or_cusip": null,
          "series_id": null,
          "feature_id": null,
          "feature_name": null,
          "expected_source_name": null,
          "status": "unavailable",
          "reason": "resolver_unsupported",
          "value": null,
          "obs_date": null,
          "release_date": null,
          "vintage_date": null,
          "actual_source": null,
          "conflict_flag": null,
          "conflict_detail": null,
          "conflict_detail_truncated": false,
          "freshness": {
            "status": "unknown",
            "reason": "max_no_observation"
          },
          "storage_clock": "pull_date_not_provider_publication",
          "lineage": "resolved_source_not_raw_row"
        }
      ],
      "ticker": "AAPL",
      "request_binding": {
        "ticker": "AAPL",
        "as_of": "2026-10-15T12:00:00+00:00",
        "start_date": "2026-10-01",
        "vintage_policy": "LATEST_AS_OF",
        "selections": [
          {
            "source_key": "fx_rates",
            "scope": "currency",
            "ticker_or_cusip": "EUR",
            "series_id": "FX:EUR:close",
            "expected_source_name": "yfinance",
            "feature_name": "synthetic_fx_eur",
            "feature_id": 8101
          },
          {
            "source_key": "ag_commodity_futures",
            "scope": "contract",
            "ticker_or_cusip": "CC=F",
            "series_id": "YF:CC=F:close",
            "expected_source_name": "YFINANCE_COMMODITY_FUTURES",
            "feature_name": "synthetic_cc",
            "feature_id": 8102
          },
          {
            "source_key": "finra_short_volume",
            "scope": "ticker",
            "ticker_or_cusip": "AAPL",
            "series_id": "finra:short_volume:AAPL",
            "expected_source_name": "FINRA_SHORT_VOLUME",
            "feature_name": "synthetic_short_aapl",
            "feature_id": 8103
          },
          {
            "source_key": "sec_ftd",
            "scope": "cusip",
            "ticker_or_cusip": "037833100",
            "series_id": "sec:ftd_balance:037833100",
            "expected_source_name": "SEC_FTD",
            "feature_name": "synthetic_ftd_037833100",
            "feature_id": 8104
          },
          {
            "source_key": "nyfed_gscpi",
            "scope": "global",
            "ticker_or_cusip": null,
            "series_id": "NYFED:gscpi",
            "expected_source_name": "NYFED_GSCPI",
            "feature_name": "synthetic_gscpi",
            "feature_id": 8105
          },
          {
            "source_key": "baltic_exchange",
            "scope": "index",
            "ticker_or_cusip": "BDI",
            "series_id": "baltic.bdi",
            "expected_source_name": "Baltic_Exchange",
            "feature_name": "synthetic_bdi",
            "feature_id": 8106
          },
          {
            "source_key": "quiverquant"
          }
        ]
      }
    }
  }
};
vi.mock('../authSession.js', () => ({ clearAuthSession: vi.fn(), getStoredToken: () => null }));
vi.mock('../stores/realtimeStore.js', () => ({ default: { getState: () => ({}) } }));
vi.mock('../hooks/useDevice.js', () => ({ useDevice: () => ({ isMobile: false }) }));
vi.mock('../utils/formatTime.js', () => ({ formatTime: String, formatDate: String }));
vi.mock('../components/PriceChart.jsx', () => ({ default: () => { throw new Error('ordinary_price_mounted'); } }));
vi.mock('../components/GEXProfile.jsx', () => ({ default: () => { throw new Error('ordinary_gex_mounted'); } }));
vi.mock('../components/VannaCharmViz.jsx', () => ({ default: () => { throw new Error('ordinary_vanna_mounted'); } }));
vi.mock('../components/FlowTimeline.jsx', () => ({ default: () => { throw new Error('ordinary_flow_mounted'); } }));
vi.mock('../views/Options.jsx', () => ({ TickerRecommendations: () => null }));

function storedTransport() {
    const pending = [];
    const fetch = vi.spyOn(api, '_fetch').mockImplementation(path => {
        let resolve, reject;
        const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
        const url = new URL(path, 'https://fixture.invalid');
        pending.push({ path, url, request: url.searchParams.has('stored_request')
            ? JSON.parse(url.searchParams.get('stored_request')) : null, resolve, reject });
        return promise;
    });
    const forbidden = ['getTickerOverview', 'getTickerEdge', 'getGEXProfile', 'getVannaCharm', 'getFlowTimeline']
        .map(name => vi.spyOn(api, name).mockImplementation(() => { throw new Error(`forbidden:${name}`); }));
    return { pending, fetch, forbidden };
}
function matchingStoredResponse(call, value = 0) {
    const ticker = decodeURIComponent(call.url.pathname.split('/')[4]);
    const result = storedContextResult(ticker, call.request.as_of, value);
    result.period = call.url.searchParams.get('period');
    const context = result.market_features.stored_source_evidence;
    context.start_date = call.request.start_date;
    context.vintage_policy = call.request.vintage_policy;
    context.request_binding = { ticker, ...structuredClone(call.request) };
    const selector = call.request.selections[0];
    context.selections[0] = { ...context.selections[0], ...selector };
    return result;
}
function declaredRequest() {
    const context = storedContextResult().market_features.stored_source_evidence;
    const { ticker: _ticker, ...request } = context.request_binding;
    return structuredClone(request);
}
async function settleStored(call, value = 0) {
    await act(async () => { call.resolve(matchingStoredResponse(call, value)); });
}
afterEach(() => {
    expect(globalThis.__blockedExternalEffects ?? []).toEqual([]);
    vi.restoreAllMocks();
});

describe('explicit stored-only client and mounted view', () => {
    it('preserves default two-argument URLs and serializes a detached bounded whole stored request', async () => {
        const paths = [];
        vi.spyOn(api, '_fetch').mockImplementation(async path => { paths.push(path); return null; });
        await api.getTickerAnalysis('A/B');
        await api.getTickerAnalysis('SPY', '1M');
        expect(paths).toEqual(['/api/v1/watchlist/A%2FB/analysis', '/api/v1/watchlist/SPY/analysis?period=1M']);
        const request = declaredRequest();
        const before = structuredClone(request);
        await api.getTickerAnalysis('SPY', '3M', { storedOnly: true, storedRequest: request });
        request.selections[0].feature_id = 999;
        const query = new URL(paths[2], 'https://fixture.invalid').searchParams;
        expect(query.get('stored_only')).toBe('true');
        expect(query.get('period')).toBe('3M');
        expect(JSON.parse(query.get('stored_request'))).toEqual(before);
        expect(snapshotStoredRequest(before).value).not.toBe(before);
        expect(paths).toHaveLength(3);
    });
    it('refuses absent, non-JSON, extra-root and oversized contexts without any request or fallback', async () => {
        const t = storedTransport();
        const bad = [null, {}, { ...declaredRequest(), extra: 1 }, { ...declaredRequest(), selections: [NaN] },
            { ...declaredRequest(), as_of: 'x'.repeat(16384) }, { ...declaredRequest(), selections: [undefined] }];
        for (const request of bad) {
            await expect(api.getTickerAnalysis('SPY', '3M', { storedOnly: true, storedRequest: request })).rejects.toThrow();
        }
        await expect(api.getTickerAnalysis('SPY', '3M', { storedOnly: false })).rejects.toThrow('stored_mode_invalid');
        const rendered = render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={null} />);
        expect(screen.getByRole('alert').textContent).toContain('stored_request_missing');
        rendered.rerender(<WatchlistAnalysis ticker="SPY" storedOnly="true" storedRequest={declaredRequest()} />);
        expect(screen.getByRole('alert').textContent).toContain('stored_mode_invalid');
        expect(t.pending).toHaveLength(0);
        expect(t.forbidden.every(spy => spy.mock.calls.length === 0)).toBe(true);
    });
    it('keeps backend validation failure unavailable without a normal analysis or secondary retry', async () => {
        const t = storedTransport();
        render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={declaredRequest()} />);
        await act(async () => { t.pending[0].reject(new Error('400: invalid_selection')); });
        expect(screen.getByRole('alert').textContent).toContain('400: invalid_selection');
        expect(t.pending).toHaveLength(1);
        expect(t.pending[0].url.searchParams.get('stored_only')).toBe('true');
        expect(t.forbidden.every(spy => spy.mock.calls.length === 0)).toBe(true);
    });
    for (const first of ['initial', 'period']) it(`keeps selected period with ${first} response first through real callbacks/Card`, async () => {
        const t = storedTransport();
        render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={declaredRequest()} />);
        fireEvent.change(screen.getByLabelText('Stored display period'), { target: { value: '1M' } });
        expect(t.pending.map(item => item.url.searchParams.get('period'))).toEqual(['3M', '1M']);
        if (first === 'initial') { await settleStored(t.pending[0], 33); expect(screen.queryByText('Stored value: 33')).toBeNull(); }
        await settleStored(t.pending[1], 0);
        if (first === 'period') await settleStored(t.pending[0], 33);
        expect(screen.getByText('Stored value: 0')).toBeTruthy();
        expect(screen.queryByText('Stored value: 33')).toBeNull();
        expect(screen.getByLabelText('Stored display period').value).toBe('1M');
        expect(screen.queryByRole('alert')).toBeNull();
        expect(t.forbidden.every(spy => spy.mock.calls.length === 0)).toBe(true);
    });
    for (const oldFirst of [true, false]) it(`binds same ticker/period context-only changes and ignores stale errors with oldFirst=${oldFirst}`, async () => {
        const t = storedTransport(), request = declaredRequest();
        const rendered = render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={request} />);
        const changed = { ...request, as_of: '2026-10-15T13:00:00+00:00' };
        rendered.rerender(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={changed} />);
        expect(screen.queryByText('Stored value: 33')).toBeNull();
        if (oldFirst) await settleStored(t.pending[0], 33);
        await settleStored(t.pending[1], null);
        if (!oldFirst) await act(async () => { t.pending[0].reject(new Error('old_context_failed')); });
        expect(screen.getByText(/Requested cutoff: 2026-10-15T13:00:00/)).toBeTruthy();
        expect(screen.getByText('Stored value: Unavailable')).toBeTruthy();
        expect(screen.queryByRole('alert')).toBeNull();
    });
    it('masks an already rendered value immediately on context/ticker change and on missing context', async () => {
        const t = storedTransport(), request = declaredRequest();
        const rendered = render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={request} />);
        await settleStored(t.pending[0], 12345);
        expect(screen.getByText('Stored value: 12345')).toBeTruthy();
        rendered.rerender(<WatchlistAnalysis ticker="QQQ" storedOnly storedRequest={request} />);
        expect(screen.queryByText('Stored value: 12345')).toBeNull();
        await settleStored(t.pending[1], 0);
        expect(screen.getByText('Stored value: 0')).toBeTruthy();
        rendered.rerender(<WatchlistAnalysis ticker="QQQ" storedOnly storedRequest={null} />);
        expect(screen.queryByText('Stored value: 0')).toBeNull();
        expect(screen.getByRole('alert').textContent).toContain('stored_request_missing');
        expect(t.pending).toHaveLength(2);
    });
    for (const mismatch of ['ticker', 'period', 'as_of', 'start_date', 'vintage_policy', 'selection', 'freshness']) it(`refuses a foreign returned ${mismatch} binding`, async () => {
        const t = storedTransport();
        render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={declaredRequest()} />);
        const result = matchingStoredResponse(t.pending[0], 987654);
        const binding = result.market_features.stored_source_evidence.request_binding;
        if (mismatch === 'ticker') result.ticker = 'QQQ';
        else if (mismatch === 'period') result.period = '1M';
        else if (mismatch === 'as_of') binding.as_of = '2026-10-15T12:00:00.000001+00:00';
        else if (mismatch === 'start_date') binding.start_date = '2026-10-02';
        else if (mismatch === 'vintage_policy') binding.vintage_policy = 'FIRST_RELEASE';
        else if (mismatch === 'selection') binding.selections[0].feature_id = 999;
        else binding.selections[0].freshness_policy = { max_age_days: 1 };
        await act(async () => { t.pending[0].resolve(result); });
        expect(screen.getByRole('alert').textContent).toContain('response_context_mismatch');
        expect(screen.queryByText('Stored value: 987654')).toBeNull();
    });
    it('preserves equivalent offset instants without rounding a one-microsecond difference', () => {
        const t = { url: new URL('https://fixture.invalid/api/v1/watchlist/SPY/analysis?period=3M'), request: declaredRequest() };
        const result = matchingStoredResponse(t);
        result.market_features.stored_source_evidence.request_binding.as_of = '2026-10-15T08:00:00-04:00';
        expect(storedAnalysisMatchesRequest(result, 'SPY', '3M', t.request)).toBe(true);
        result.market_features.stored_source_evidence.request_binding.as_of = '2026-10-15T08:00:00.000001-04:00';
        expect(storedAnalysisMatchesRequest(result, 'SPY', '3M', t.request)).toBe(false);
    });
    it('invalidates cleanup callbacks and switches modes without inheriting stored or ordinary requests', async () => {
        const t = storedTransport();
        // This deliberate ordinary remount may render its existing chart; still no ticker/request.
        vi.spyOn(await import('../components/PriceChart.jsx'), 'default').mockImplementation(() => null);
        const rendered = render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={declaredRequest()} />);
        // No ticker keeps ordinary effects idle while proving the ordinary branch remount.
        rendered.rerender(<WatchlistAnalysis ticker={null} storedOnly={false} />);
        await settleStored(t.pending[0], 88);
        expect(screen.queryByText('Stored value: 88')).toBeNull();
        rendered.rerender(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={declaredRequest()} />);
        expect(t.pending).toHaveLength(2);
        rendered.unmount();
        await settleStored(t.pending[1], 99);
        expect(screen.queryByText('Stored value: 99')).toBeNull();
    });
    for (const first of ['initial', 'period']) it(`actual accepted raw Resolver PIT endpoint JSON reaches revised client/callback/Card with ${first} first`, async () => {
        const t = storedTransport();
        render(<WatchlistAnalysis ticker="AAPL" storedOnly storedRequest={actualStoredRequest} />);
        fireEvent.change(screen.getByLabelText('Stored display period'), { target: { value: '1M' } });
        expect(t.pending.every(call => call.url.searchParams.get('stored_only') === 'true')).toBe(true);
        expect(t.pending[1].request).toEqual(actualStoredRequest);
        const initial = { ...structuredClone(actualStoredResponse), period: '3M' };
        if (first === 'initial') await act(async () => { t.pending[0].resolve(initial); });
        await act(async () => { t.pending[1].resolve(structuredClone(actualStoredResponse)); });
        if (first === 'period') await act(async () => { t.pending[0].resolve(initial); });
        expect(screen.getByRole('region', { name: 'Stored source finra_short_volume' }).textContent).toContain('Stored value: 0');
        expect(screen.getByRole('region', { name: 'Stored source quiverquant' }).textContent).toContain('resolver_unsupported');
        expect(screen.getByRole('region', { name: 'Stored source fx_rates' }).textContent).toContain('freshness_policy_absent');
        expect(screen.queryByRole('alert')).toBeNull();
        expect(t.forbidden.every(spy => spy.mock.calls.length === 0)).toBe(true);
    });
});

 it('binds source-selection-only changes without mutating the original declared context', async () => {
    const t = storedTransport(), request = declaredRequest();
    const before = structuredClone(request);
    const rendered = render(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={request} />);
    const changed = structuredClone(request);
    changed.selections[0].feature_id = 24;
    changed.selections[0].feature_name = 'explicit_other_feature';
    rendered.rerender(<WatchlistAnalysis ticker="SPY" storedOnly storedRequest={changed} />);
    await settleStored(t.pending[1], 0);
    await settleStored(t.pending[0], 33);
    expect(screen.getByText('Stored value: 0')).toBeTruthy();
    expect(screen.getByText(/Feature: explicit_other_feature/)).toBeTruthy();
    expect(screen.queryByText('Stored value: 33')).toBeNull();
    expect(request).toEqual(before);
    expect(t.pending[0].request).toEqual(before);
 });

// Actual application's current ticker/onBack-only mount has no stored props.
import actualAppMountSource from '../app.jsx?raw';
function ActualWatchlistAppMount({ selectedTicker, navigateBack }) {
            return <WatchlistAnalysis ticker={selectedTicker} onBack={() => navigateBack('dashboard')} />;
}
async function applicationTransport() {
    // Ordinary mode is intentionally preserved, including its existing cold
    // requests. All are pending inert fakes, never network/provider execution.
    for (const path of ['PriceChart', 'GEXProfile', 'VannaCharmViz', 'FlowTimeline']) {
        const module = path === 'PriceChart' ? await import('../components/PriceChart.jsx')
            : path === 'GEXProfile' ? await import('../components/GEXProfile.jsx')
            : path === 'VannaCharmViz' ? await import('../components/VannaCharmViz.jsx')
            : await import('../components/FlowTimeline.jsx');
        vi.spyOn(module, 'default').mockImplementation(() => null);
    }
    const pending = [];
    vi.spyOn(api, '_fetch').mockImplementation(path => {
        let resolve, reject;
        const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
        const url = new URL(path, 'https://fixture.invalid');
        pending.push({ path, url, resolve, reject, request: url.searchParams.has('stored_request')
            ? JSON.parse(url.searchParams.get('stored_request')) : null });
        return promise;
    });
    return { pending, stored: () => pending.filter(item => item.url.searchParams.get('stored_only') === 'true') };
}
function enterStoredRequest(request) {
    fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: JSON.stringify(request) } });
    fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
}

describe('actual application Stored analysis entry', () => {
    it('reaches real control from exact ticker/onBack-only app mount and loads accepted materializer envelope through client/Card', async () => {
        expect(actualAppMountSource).toContain("return <WatchlistAnalysis ticker={selectedTicker} onBack={() => navigateBack('dashboard')} />;");
        const t = await applicationTransport();
        // Same prop surface as the actual source above, not optional stored props.
        render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
        expect(t.pending.some(call => !call.url.searchParams.has('stored_only'))).toBe(true);
        const ordinaryCount = t.pending.length;
        fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
        expect(screen.getByLabelText('Declared stored request').value).toBe('');
        expect(t.pending).toHaveLength(ordinaryCount);
        fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: JSON.stringify(actualStoredRequest) } });
        expect(screen.getByRole('region', { name: 'Stored request preview' }).textContent).toContain(actualStoredRequest.as_of);
        expect(t.stored()).toHaveLength(0);
        fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
        expect(t.stored()).toHaveLength(1);
        expect(t.stored()[0].request).toEqual(actualStoredRequest);
        expect(t.pending).toHaveLength(ordinaryCount + 1);
        // Select the fixture's actual period; its endpoint JSON stays byte-value
        // exact rather than replacing the echoed outer period in a mock.
        fireEvent.change(screen.getByLabelText('Stored display period'), { target: { value: '1M' } });
        expect(t.stored()).toHaveLength(2);
        expect(t.stored()[1].request).toEqual(actualStoredRequest);
        expect(t.stored()[1].url.searchParams.get('period')).toBe('1M');
        expect(t.pending).toHaveLength(ordinaryCount + 2);
        await act(async () => { t.stored()[1].resolve(structuredClone(actualStoredResponse)); });
        expect(screen.getByRole('region', { name: 'Stored source finra_short_volume' }).textContent).toContain('Stored value: 0');
        expect(screen.getByRole('region', { name: 'Stored source quiverquant' }).textContent).toContain('resolver_unsupported');
        expect(screen.getByRole('region', { name: 'Stored source fx_rates' }).textContent).toContain('freshness_policy_absent');
        expect(screen.getByText(/qualified_intraday_result_absent/)).toBeTruthy();
    });
    it('keeps empty/invalid declared inputs unavailable and never falls back while editing', async () => {
        const t = await applicationTransport();
        render(<WatchlistAnalysis ticker="SPY" onBack={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
        const count = t.pending.length;
        fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
        expect(screen.getByText(/Stored data unavailable: Add a declared request/)).toBeTruthy();
        for (const [text, expected] of [['{bad', 'must be valid JSON'], ['{}', 'Missing request fields:'], ['[]', 'one JSON object']]) {
            fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: text } });
            fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
            expect(screen.getByText(new RegExp(`Stored data unavailable: .*${expected}`))).toBeTruthy();
        }
        expect(t.pending).toHaveLength(count);
        expect(t.stored()).toHaveLength(0);
    });
    for (const oldFirst of [true, false]) it(`applies independent submit snapshots and resubmits without old data revival with oldFirst=${oldFirst}`, async () => {
        const t = await applicationTransport();
        render(<WatchlistAnalysis ticker="SPY" onBack={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
        const request = declaredRequest();
        enterStoredRequest(request);
        const changed = { ...request, as_of: '2026-10-15T13:00:00+00:00' };
        // A draft edit does not mutate dispatch or start another request.
        fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: JSON.stringify(changed) } });
        expect(t.stored()).toHaveLength(1);
        expect(t.stored()[0].request).toEqual(request);
        fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
        expect(t.stored()).toHaveLength(2);
        if (oldFirst) await settleStored(t.stored()[0], 33);
        await settleStored(t.stored()[1], 0);
        if (!oldFirst) await settleStored(t.stored()[0], 33);
        expect(screen.getByText('Stored value: 0')).toBeTruthy();
        expect(screen.queryByText('Stored value: 33')).toBeNull();
        expect(screen.getByText(/Requested cutoff: 2026-10-15T13:00:00/)).toBeTruthy();
        fireEvent.click(screen.getByRole('button', { name: 'Reset stored request' }));
        expect(screen.getByLabelText('Declared stored request').value).toBe('');
        expect(screen.queryByText('Stored value: 0')).toBeNull();
        expect(t.stored()).toHaveLength(2);
    });
    it('clears applied request and draft on ticker switch, preserves errors and leaves ordinary return explicit', async () => {
        const t = await applicationTransport();
        const rendered = render(<WatchlistAnalysis ticker="SPY" onBack={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
        enterStoredRequest(declaredRequest());
        rendered.rerender(<WatchlistAnalysis ticker="QQQ" onBack={() => {}} />);
        expect(screen.getByLabelText('Declared stored request').value).toBe('');
        await settleStored(t.stored()[0], 33);
        expect(screen.queryByText('Stored value: 33')).toBeNull();
        expect(t.stored()).toHaveLength(1);
        enterStoredRequest(declaredRequest());
        await act(async () => { t.stored()[1].reject(new Error('400: binding unavailable')); });
        expect(screen.getByText(/400: binding unavailable/)).toBeTruthy();
        const count = t.pending.length;
        expect(t.stored()).toHaveLength(2);
        fireEvent.click(screen.getByRole('button', { name: 'Return to ordinary analysis' }));
        expect(screen.queryByLabelText('Declared stored request')).toBeNull();
        expect(t.pending.length).toBeGreaterThan(count);
        expect(t.stored()).toHaveLength(2);
    });
});

// E1 raw declaration integrity. Semantic selector validation remains backend-owned.
function ambiguousEntryTexts() {
    const unique = JSON.stringify(actualStoredRequest);
    const cutoff = `"as_of":${JSON.stringify(actualStoredRequest.as_of)}`;
    const id = actualStoredRequest.selections.find(row => row.source_key === 'finra_short_volume').feature_id;
    return [
        ['root cutoff', unique.replace(cutoff, `"as_of":"2026-10-15T11:00:00+00:00",${cutoff}`)],
        ['escaped root key', unique.replace(cutoff, String.raw`"\u0061s_of":"2026-10-15T11:00:00+00:00",` + cutoff)],
        ['two escapes in decoded key', unique.replace(cutoff, String.raw`"\u0061s\u005fof":"2026-10-15T11:00:00+00:00",` + cutoff)],
        ['nested feature id', unique.replace(`"feature_id":${id}`, `"feature_id":${id + 1000},"feature_id":${id}`)],
        ['nested escaped feature key', unique.replace(`"feature_id":${id}`, `"feature_id":${id + 1000},` + String.raw`"feature_\u0069d":` + id)],
        ['root selections', unique.replace('"selections":', '"selections":null,"selections":')],
        ['nested policy member', unique.replace('"selections":[{', '"selections":[{"freshness_policy":{"max_age_days":1,"max_age_days":2},')],
        ['escaped quote member', unique.slice(0, -1) + String.raw`,"other":{"a\"b":1,"\u0061\"b":2}}`],
    ];
}

for (const [name, raw] of ambiguousEntryTexts()) {
    it(`rejects ambiguous raw ${name} before preview or request`, async () => {
        const t = await applicationTransport();
        render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
        const count = t.pending.length;
        fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: raw } });
        expect(screen.queryByRole('region', { name: 'Stored request preview' })).toBeNull();
        fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
        expect(screen.getByText(/Stored data unavailable: The request repeats an object field/)).toBeTruthy();
        expect(t.stored()).toHaveLength(0);
        expect(t.pending).toHaveLength(count);
    });
}

it('keeps decoded unique escaped keys and independent object scopes unambiguous', async () => {
    const t = await applicationTransport();
    render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
    // The six selector objects repeat normal member names across independent scopes.
    const unique = JSON.stringify(actualStoredRequest).replace('"as_of":', String.raw`"\u0061s_of":`);
    fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: ` \r\n\t${unique}\n ` } });
    expect(screen.getByRole('region', { name: 'Stored request preview' })).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
    expect(t.stored()).toHaveLength(1);
    expect(t.stored()[0].request).toEqual(actualStoredRequest);
});

it('does not mistake property-like quoted strings, escaped slashes or array contents for member names', async () => {
    const t = await applicationTransport();
    render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
    const request = structuredClone(actualStoredRequest);
    // Transport-only literal name; no registry availability/semantic acceptance claimed.
    request.selections[0].feature_name = 'name "as_of":1, "as_of":2 { [ \\ / unicode-é';
    request.selections[0].freshness_policy = { max_age_days: 0 };
    const raw = JSON.stringify(request).replace('unicode-é', String.raw`unicode-\u00e9`).replace(' / ', String.raw` \/ `);
    fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: raw } });
    fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
    expect(t.stored()).toHaveLength(1);
    expect(t.stored()[0].request).toEqual(request);
});

for (const selections of [[], null]) {
    it(`keeps unambiguous ${JSON.stringify(selections)} selections as declared without inventing defaults`, async () => {
        const t = await applicationTransport();
        render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
        const request = { ...actualStoredRequest, selections };
        enterStoredRequest(request);
        expect(t.stored()).toHaveLength(1);
        expect(t.stored()[0].request).toEqual(request);
    });
}

it('retains exact canonical 16KiB limit and syntax failure without a duplicate-regex shortcut', async () => {
    const t = await applicationTransport();
    render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
    const request = structuredClone(actualStoredRequest);
    request.selections[0].feature_name = '';
    const baseLength = new TextEncoder().encode(JSON.stringify(request)).length;
    request.selections[0].feature_name = 'x'.repeat(16384 - baseLength);
    expect(new TextEncoder().encode(snapshotStoredRequest(request).json).length).toBe(16384);
    enterStoredRequest(request);
    expect(t.stored()).toHaveLength(1);
    request.selections[0].feature_name += 'x';
    enterStoredRequest(request);
    expect(screen.getByText(/Stored data unavailable: The request exceeds the 16 KiB size limit/)).toBeTruthy();
    expect(t.stored()).toHaveLength(1);
    for (const invalid of ['{"selections": [,]}', '{"as_of":"unterminated}', '{"as_of":1,}']) {
        fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: invalid } });
        fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
        expect(screen.getByText(/Stored data unavailable: The request must be valid JSON/)).toBeTruthy();
        expect(t.stored()).toHaveLength(1);
    }
});

// Repo-local offline fence: no private setup/fixture file is needed for this suite.
import { beforeAll, afterAll } from 'vitest';
import net from 'node:net';
import http from 'node:http';
import https from 'node:https';
import tls from 'node:tls';
import dgram from 'node:dgram';
import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';
const restoreOfflineEffects = [];
beforeAll(() => {
    globalThis.__blockedExternalEffects = [];
    const replace = (target, name) => {
        const original = target[name];
        restoreOfflineEffects.push(() => { target[name] = original; });
        target[name] = () => {
            globalThis.__blockedExternalEffects.push(name);
            throw new Error(`offline_effect_denied:${name}`);
        };
    };
    for (const [target, names] of [[net, ['connect', 'createConnection', 'createServer']],
        [http, ['request', 'get', 'createServer']], [https, ['request', 'get', 'createServer']],
        [tls, ['connect', 'createServer']], [dgram, ['createSocket']],
        [childProcess, ['spawn', 'spawnSync', 'exec', 'execSync', 'execFile', 'execFileSync', 'fork']]]) {
        for (const name of names) replace(target, name);
    }
    replace(net.Socket.prototype, 'connect');
    replace(globalThis, 'fetch');
    syncBuiltinESMExports();
});
afterAll(() => {
    for (const restore of restoreOfflineEffects.reverse()) restore();
    syncBuiltinESMExports();
});

it('portable E1 guard refuses ambiguous root and nested declarations at the dispatch boundary', async () => {
    const t = await applicationTransport();
    render(<ActualWatchlistAppMount selectedTicker="AAPL" navigateBack={() => {}} />);
    fireEvent.click(screen.getByRole('button', { name: 'Stored analysis', exact: true }));
    const count = t.pending.length;
    for (const [name, raw] of ambiguousEntryTexts().filter(([name]) => ['root cutoff', 'nested feature id'].includes(name))) {
        fireEvent.change(screen.getByLabelText('Declared stored request'), { target: { value: raw } });
        fireEvent.click(screen.getByRole('button', { name: 'Load stored data' }));
        expect(t.stored(), name).toHaveLength(0);
        expect(t.pending, name).toHaveLength(count);
    }
});
