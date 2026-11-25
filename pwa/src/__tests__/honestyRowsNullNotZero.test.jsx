/**
 * #959 M1 honesty, second pass: a value the backend did not report is shown as
 * not reported, never as 0, and an older Regime read applies nothing after a
 * newer one has started.
 */
import React from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import TickerLookup from '../views/TickerLookup.jsx';
import Regime from '../views/Regime.jsx';
import CorrelationMatrix from '../views/CorrelationMatrix.jsx';
import PipelineHealth from '../views/PipelineHealth.jsx';
import SpiderStats from '../views/SpiderStats.jsx';
import Surfacer from '../views/Surfacer.jsx';
import EdgeScanner from '../views/EdgeScanner.jsx';

const storeState = { currentRegime: null, setCurrentRegime: vi.fn() };

vi.mock('../api.js', () => ({
    api: {
        getDadTickerGold: vi.fn(), streamDadTickerGold: vi.fn(),
        getCurrent: vi.fn(), getAllActiveRegimes: vi.fn(), getHistory: vi.fn(), getTransitions: vi.fn(),
        getStrategyForRegime: vi.fn(), getRegimeSynthesis: vi.fn(),
        getDiscoveryCorrelationMatrix: vi.fn(), getTimeseries: vi.fn(),
        getPipelineHealth: vi.fn(), getSpiderStats: vi.fn(), get: vi.fn(), getMarketEdges: vi.fn(),
    },
}));
vi.mock('../store.js', () => ({ default: vi.fn(() => storeState) }));
vi.mock('../components/ViewHelp.jsx', () => ({ default: () => null }));
vi.mock('../components/PriceChart.jsx', () => ({ default: () => <div /> }));

beforeAll(() => {
    if (typeof window.matchMedia !== 'function') {
        window.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} });
    }
    if (typeof globalThis.ResizeObserver === 'undefined') {
        globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    }
});

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    storeState.currentRegime = null;
    storeState.setCurrentRegime.mockReset();
    storeState.setCurrentRegime.mockImplementation((r) => { storeState.currentRegime = r; });
    api.streamDadTickerGold.mockReturnValue({ close: vi.fn() });
});

describe('TickerLookup', () => {
    it('shows no data, not 0.0 pts / 0 fields / 0.0 score, for sources that reported nothing', async () => {
        window.location.hash = '#/ticker-lookup?ticker=AAA';
        api.getDadTickerGold.mockResolvedValue({
            ticker: 'AAA', grid_data: {}, signals: {},
            decision_stack: {
                stance: 'Needs more evidence', score: 12, method: 'stack',
                cards: [{ source: 'Dad workbooks', state: 'missing', points: 0, detail: 'no file' }],
                reasons: [], blockers: [],
            },
            finviz: { status: 'unavailable' },
            workbook: { evidence: [{ file: 'f.xlsx', sheet: 'S1', cell: 'B2', evidence_text: 'AAA', context_score: null }] },
        });
        render(<TickerLookup />);
        await waitFor(() => expect(screen.getByText('no data')).toBeTruthy());
        expect(screen.queryByText('0.0 pts')).toBeNull();
        expect(screen.getByText('fields not reported')).toBeTruthy();
        expect(screen.queryByText('0 fields')).toBeNull();
        const cell = screen.getByText('AAA', { selector: '.tl-evidence-text' }).closest('tr').querySelectorAll('td');
        expect(cell[cell.length - 1].textContent).toBe('-');
    });

    it('a card with no points is no data, not 0.0 pts, whatever its state', async () => {
        window.location.hash = '#/ticker-lookup?ticker=AAA';
        api.getDadTickerGold.mockResolvedValue({
            ticker: 'AAA', grid_data: {}, signals: {},
            decision_stack: {
                stance: 'Needs more evidence', score: 12, method: 'stack',
                cards: [{ source: 'Chart', state: 'watch', detail: 'no points field' }],
                reasons: [], blockers: [],
            },
        });
        render(<TickerLookup />);
        await waitFor(() => expect(screen.getByText('no data')).toBeTruthy());
        expect(screen.queryByText('0.0 pts')).toBeNull();
    });
});

describe('Regime', () => {
    const GROWTH = { state: 'GROWTH', confidence: 0.7, transition_probability: 0.2 };

    it('draws a state with no reported confidence as "--", not 0%', async () => {
        api.getCurrent.mockResolvedValue(GROWTH);
        api.getAllActiveRegimes.mockResolvedValue({
            macro: [{ state: 'GROWTH', confidence: null, recommendation: 'HOLD' }], strategy: [],
        });
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({ transitions: [] });
        api.getStrategyForRegime.mockResolvedValue(null);
        render(<Regime />);
        await waitFor(() => expect(api.getAllActiveRegimes).toHaveBeenCalled());
        fireEvent.click(await screen.findByText(/^macro$/i));
        await waitFor(() => expect(screen.getByText('MACRO REGIME READINGS')).toBeTruthy());
        expect(screen.queryByText('0%')).toBeNull();
        expect(screen.getAllByText('--').length).toBeGreaterThan(0);
    });

    it.each([
        ['is null', null],
        ['is an error object', { error: true, status: 503 }],
    ])('does not keep the earlier regime strategy when the new strategy %s', async (_name, second) => {
        api.getCurrent.mockResolvedValue(GROWTH);
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({ transitions: [] });
        api.getStrategyForRegime
            .mockResolvedValueOnce({ name: 'Trend Rider', action: 'Add', allocation: '60%', risk_level: 'med' })
            .mockResolvedValueOnce(second);
        render(<Regime />);
        await screen.findByText('Trend Rider');
        fireEvent.click(screen.getByText('REFRESH'));
        await waitFor(() => expect(api.getStrategyForRegime).toHaveBeenCalledTimes(2));
        await waitFor(() => expect(screen.queryByText('Trend Rider')).toBeNull());
    });

    it('never shows the previous regime\'s strategy under a newly read regime while its own loads', async () => {
        const CONTRACTION = { state: 'CONTRACTION', confidence: 0.6, transition_probability: 0.3 };
        let release;
        const slow = new Promise((res) => { release = res; });
        api.getCurrent.mockResolvedValueOnce(GROWTH).mockResolvedValueOnce(CONTRACTION);
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({ transitions: [] });
        api.getStrategyForRegime
            .mockResolvedValueOnce({ name: 'Trend Rider', action: 'Add', allocation: '60%', risk_level: 'med' })
            .mockReturnValueOnce(slow);
        const view = render(<Regime />);
        await waitFor(() => expect(api.getStrategyForRegime).toHaveBeenCalledTimes(1));
        view.rerender(<Regime />);
        await screen.findByText('Trend Rider');
        fireEvent.click(screen.getByText('REFRESH'));
        await waitFor(() => expect(api.getStrategyForRegime).toHaveBeenCalledTimes(2));
        view.rerender(<Regime />);
        // New regime on screen, its strategy still loading: the old one must be gone.
        await waitFor(() => expect(screen.getAllByText(/CONTRACTION/).length).toBeGreaterThan(0));
        expect(screen.queryByText('Trend Rider')).toBeNull();
        await act(async () => { release({ name: 'Defensive Posture', action: 'Trim', allocation: '30%', risk_level: 'low' }); });
        view.rerender(<Regime />);
        await screen.findByText('Defensive Posture');
        expect(screen.queryByText('Trend Rider')).toBeNull();
    });

    it('an older read finishing its strategy load after a newer read applies none of its panels', async () => {
        let release;
        const slowStrategy = new Promise((res) => { release = res; });
        api.getCurrent.mockResolvedValue(GROWTH);
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory
            .mockResolvedValueOnce({ history: [{ date: '2026-01-01', state: 'GROWTH', confidence: 0.5 }] })
            .mockResolvedValueOnce({ history: [{ date: '2026-02-02', state: 'GROWTH', confidence: 0.5 }] });
        api.getTransitions.mockResolvedValue({ transitions: [] });
        api.getStrategyForRegime.mockReturnValueOnce(slowStrategy).mockResolvedValue(null);
        const view = render(<Regime />);
        await waitFor(() => expect(api.getStrategyForRegime).toHaveBeenCalledTimes(1));
        fireEvent.click(screen.getByText('REFRESH'));
        await waitFor(() => expect(api.getHistory).toHaveBeenCalledTimes(2));
        await act(async () => { await Promise.resolve(); });
        await act(async () => { release(null); });
        view.rerender(<Regime />);
        fireEvent.click(await screen.findByText(/^history$/i));
        await waitFor(() => expect(view.container.querySelector('[title^="2026-02-02"]')).toBeTruthy());
        expect(view.container.querySelector('[title^="2026-01-01"]')).toBeNull();
    });
});

describe('CorrelationMatrix', () => {
    it('labels a missing cell n/a and never prints 0.00 for it', async () => {
        api.getDiscoveryCorrelationMatrix.mockResolvedValue({
            features: ['SPY', 'QQQ'], matrix: [[1, null], [null, 1]], regime_matrices: {}, breakdowns: [],
            current_regime: 'GROWTH', pca: { components: [], total_variance: 0 }, n_observations: 120,
        });
        const { container } = render(<CorrelationMatrix />);
        await waitFor(() => expect(container.querySelectorAll('rect.corr-cell').length).toBe(4));
        const texts = Array.from(container.querySelectorAll('svg text')).map((t) => t.textContent);
        expect(texts).toContain('n/a');
        expect(texts).not.toContain('0.00');
    });
});

describe('PipelineHealth', () => {
    it('shows a dash, not 0, for summary tiles the backend did not send', async () => {
        api.getPipelineHealth.mockResolvedValue({
            summary: {}, sources: [], coverage: {}, recent_errors: [], resolver_status: {},
            availability: 'available', stale_reason: null,
        });
        render(<PipelineHealth />);
        await waitFor(() => expect(screen.getByText('Total Sources')).toBeTruthy());
        expect(screen.getByText('Total Sources').previousSibling.textContent).toBe('-');
    });
});

describe('SpiderStats', () => {
    it('an unavailable reply is not shown as zero stats with a fresh clock', async () => {
        api.getSpiderStats.mockResolvedValue({ status: 'unavailable', total_actors: 0, total_connections: 0 });
        render(<SpiderStats />);
        await waitFor(() => expect(screen.getByText('Spider stats unavailable')).toBeTruthy());
        expect(screen.queryByText(/Last updated/)).toBeNull();
    });
});

describe('Surfacer', () => {
    it('shows a dash for Ready and Avg Score when meta carries neither', async () => {
        api.get.mockResolvedValue({ meta: {}, candidates: [] });
        render(<Surfacer />);
        await waitFor(() => expect(screen.getByText('Avg Score')).toBeTruthy());
        expect(screen.getByText('Avg Score').nextSibling.textContent).toBe('-');
        screen.getAllByText('Ready').forEach((el) => expect(el.nextSibling.textContent).toBe('-'));
    });
});

describe('EdgeScanner', () => {
    it('shows a dash for Live, not 0, when the scan failed', async () => {
        api.getMarketEdges.mockResolvedValue({ error: 'down' });
        render(<EdgeScanner />);
        await waitFor(() => expect(screen.getByText('Live')).toBeTruthy());
        expect(screen.getByText('Live').nextSibling.textContent).toBe('-');
    });
});
