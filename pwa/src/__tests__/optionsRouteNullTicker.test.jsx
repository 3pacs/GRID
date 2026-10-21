// @vitest-environment jsdom
import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import { parseHashRoute } from '../routing.js';
import Options from '../views/Options.jsx';
import ViewErrorBoundary from '../components/ViewErrorBoundary.jsx';

vi.mock('../api.js', () => ({ api: {
    getOptionsSignals: vi.fn(), scanMispricing: vi.fn(), get100xOpportunities: vi.fn(),
    getGEXProfile: vi.fn(), getVannaCharm: vi.fn(), getFlowTimeline: vi.fn(), get: vi.fn(),
} }));
vi.mock('../components/GEXProfile.jsx', () => ({ default: ({ ticker }) => <div>Profile for {ticker}</div> }));
vi.mock('../components/VannaCharmViz.jsx', () => ({ default: () => null }));
vi.mock('../components/FlowTimeline.jsx', () => ({ default: () => null }));

// This is the actual app caller's route-to-prop contract, without its unrelated
// lazy-view/store warmup graph. The parser, Options, boundary and Gamma Watch are real.
function RouteOptions({ hash }) {
    const route = parseHashRoute(hash);
    return <ViewErrorBoundary viewName="options"><Options selectedTicker={route.selectedTicker ?? null} /></ViewErrorBoundary>;
}
const flush = async () => { await act(async () => {}); };
const pending = () => new Promise(() => {});
const assertNoDealerRequests = () => {
    expect(api.getGEXProfile).not.toHaveBeenCalled();
    expect(api.getVannaCharm).not.toHaveBeenCalled();
    expect(api.getFlowTimeline).not.toHaveBeenCalled();
};
function assertSignalsAvailable() {
    expect(screen.getByText('Options & Watchlist')).toBeTruthy();
    expect(screen.queryByText('options Error')).toBeNull();
    expect(screen.getByRole('button', { name: 'Gamma Watch', exact: true })).toBeTruthy();
    assertNoDealerRequests();
}

describe('Options route nullable selected ticker', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(1000000);
        vi.resetAllMocks();
        api.getOptionsSignals.mockImplementation(pending);
        api.scanMispricing.mockImplementation(pending);
        api.get100xOpportunities.mockImplementation(pending);
        api.getGEXProfile.mockResolvedValue({ spot: 600 });
        api.getVannaCharm.mockResolvedValue({ error: 'Unavailable' });
        api.getFlowTimeline.mockResolvedValue({ error: 'Unavailable' });
        vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('Unexpected network'))));
    });
    afterEach(() => {
        cleanup();
        expect(globalThis.fetch).not.toHaveBeenCalled();
        vi.clearAllTimers();
        vi.useRealTimers();
        vi.unstubAllGlobals();
    });

    it('keeps the default hash route mounted before any of its three data calls resolves', () => {
        const route = parseHashRoute('#/options');
        expect(route.view).toBe('options');
        expect(route.selectedTicker ?? null).toBeNull();
        render(<RouteOptions hash="#/options" />);
        assertSignalsAvailable();
        expect(screen.getByText('Loading options data...')).toBeTruthy();
        expect(api.getOptionsSignals).toHaveBeenCalledTimes(1);
        expect(api.scanMispricing).toHaveBeenCalledExactlyOnceWith(5.0);
        expect(api.get100xOpportunities).toHaveBeenCalledTimes(1);
        expect(api.get).not.toHaveBeenCalled();
    });

    it.each([
        ['missing', {}], ['undefined', { selectedTicker: undefined }],
        ['empty', { selectedTicker: '' }], ['whitespace', { selectedTicker: '   ' }],
        ['number', { selectedTicker: 0 }], ['boolean', { selectedTicker: false }],
        ['object', { selectedTicker: { toString: () => 'SPY' } }],
    ])('treats %s selection as absent without inventing a symbol or request', (_, props) => {
        render(<ViewErrorBoundary viewName="options"><Options {...props} /></ViewErrorBoundary>);
        assertSignalsAvailable();
        expect(screen.getByText('Loading options data...')).toBeTruthy();
        expect(api.get).not.toHaveBeenCalled();
    });

    it.each([
        ['#/options?ticker=%20spy%20', 'SPY'], ['#/options?symbol=qqq', 'QQQ'],
    ])('preserves valid ticker routing and normalized Dealer Flow selection for %s', async (hash, ticker) => {
        api.getOptionsSignals.mockResolvedValue({ signals: [] });
        api.scanMispricing.mockResolvedValue({ opportunities: [] });
        api.get100xOpportunities.mockResolvedValue({ opportunities: [] });
        render(<RouteOptions hash={hash} />);
        await flush();
        expect(screen.queryByText('options Error')).toBeNull();
        expect(screen.getByText(`Profile for ${ticker}`)).toBeTruthy();
        expect(api.getGEXProfile).toHaveBeenCalledExactlyOnceWith(ticker);
        expect(api.getVannaCharm).toHaveBeenCalledExactlyOnceWith(ticker);
        expect(api.getFlowTimeline).toHaveBeenCalledExactlyOnceWith(ticker, 90);
        expect(api.get).not.toHaveBeenCalled();
    });

    it('retains an unavailable-valued signal and opens the real Gamma Watch without a dealer request', async () => {
        api.getOptionsSignals.mockResolvedValue({ signals: [{
            ticker: 'SPY', spot_price: null, put_call_ratio: null,
            max_pain: null, iv_skew: null, iv_atm: null, total_oi: null,
        }] });
        api.scanMispricing.mockResolvedValue({ opportunities: [] });
        api.get100xOpportunities.mockResolvedValue({ opportunities: [] });
        api.get.mockResolvedValue({ status: 'available', data: {
            schema: 'gex-screen/2', symbol: 'SPY', served_at: 1000, evaluation_clock: 'ok',
            public_price: { status: 'unverified-public-latency', source: 'Yahoo chart · public, latency not guaranteed',
                source_time: 995, receipt_time: 999, values_available: true,
                current_interpretation: false, max_context_age_seconds: 120, price: 600 },
        } });
        render(<RouteOptions hash="#/options" />);
        await flush();
        assertSignalsAvailable();
        expect(screen.getByText('SPY', { exact: true })).toBeTruthy();
        expect(screen.getByText('Spot: $--')).toBeTruthy();
        fireEvent.click(screen.getByRole('button', { name: 'Gamma Watch', exact: true }));
        await flush();
        expect(screen.queryByText('options Error')).toBeNull();
        expect(screen.getByText('Gamma Watch · experimental public context')).toBeTruthy();
        expect(screen.getByText(/^SPY 600 ·/)).toBeTruthy();
        expect(api.get).toHaveBeenCalledExactlyOnceWith('/api/v1/gamma-watch/state');
        assertNoDealerRequests();
    });
});
