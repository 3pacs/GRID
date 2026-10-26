import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeAll, beforeEach } from 'vitest';
import SectorDive from '../views/SectorDive.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getRecentTradeTickets: vi.fn(),
        getSectorDetail: vi.fn(),
        getSectorHealth: vi.fn(),
    },
}));

beforeAll(() => {
    if (typeof globalThis.ResizeObserver === 'undefined') {
        globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    }
    if (typeof SVGElement !== 'undefined' && !('transform' in SVGElement.prototype)) {
        Object.defineProperty(SVGElement.prototype, 'transform', {
            configurable: true,
            get() { return { baseVal: { consolidate: () => null } }; },
        });
    }
});

const DETAIL_NO_SIGNALS = {
    name: 'Technology', sector: 'Technology', etf: 'XLK',
    latest_price: null, price: null,
    sector_metrics: { relative_strength_1m: null },
    subsectors: {}, nodes: [], edges: [], clusters: [], lineage: [],
};

const HEALTH_UNAVAILABLE = {
    sector: 'Technology', score: null, trend_30d: null, components: {},
    narrative: 'Technology health unavailable: no underlying data for any component.',
    as_of: null, status: 'unavailable', reason: 'no underlying data for any component',
    data_coverage: { with_data: [], missing_neutral_filled: ['margin', 'chokepoints', 'capital_allocation', 'insider', 'congress', 'dark_pool'] },
};

describe('SectorDive truthfulness', () => {
    beforeEach(() => {
        api.getSectorDetail.mockReset();
        api.getSectorHealth.mockReset();
        api.getRecentTradeTickets.mockReset();
        api.getRecentTradeTickets.mockResolvedValue(null);
    });

    it('renders unavailable health and missing signals as -- rather than neutral/0', async () => {
        api.getSectorDetail.mockResolvedValue(DETAIL_NO_SIGNALS);
        api.getSectorHealth.mockResolvedValue(HEALTH_UNAVAILABLE);

        render(<SectorDive sector="Technology" onBack={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText('unavailable')).toBeInTheDocument();
        });
        const text = document.body.textContent;
        expect(text).not.toMatch(/neutral/i);
        expect(text).not.toMatch(/stable/);
        // Insider / Congressional tiles are '--', not '0'
        expect(screen.getAllByText('--').length).toBeGreaterThanOrEqual(3);
    });

    it('renders a reported signal and real counts when the payload has them', async () => {
        api.getSectorDetail.mockResolvedValue({
            ...DETAIL_NO_SIGNALS,
            sector_metrics: {
                relative_strength_1m: 0.021,
                dark_pool_signal: 'accumulation',
                insider_activity: [{ ticker: 'NVDA' }, { ticker: 'AMD' }],
                congressional_activity: [],
            },
        });
        api.getSectorHealth.mockResolvedValue({ ...HEALTH_UNAVAILABLE, score: 61.2, trend_30d: 'improving', status: 'ok' });

        render(<SectorDive sector="Technology" onBack={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText('accumulation')).toBeInTheDocument();
        });
        expect(screen.getByText('2')).toBeInTheDocument();
        expect(screen.getByText('0')).toBeInTheDocument(); // a real empty list is a real 0
        expect(screen.getByText('improving')).toBeInTheDocument();
    });

    // 2026-10-19 trace, case B: nine Technology tickers dated 2026-03-09 with
    // short_volume NULL were served as "accumulation". The API now sends no
    // signal plus the reason; the #865 provenance (stale, 224 d) is shown.
    it('shows -- plus the provenance status for a stale dark-pool table, never a signal', async () => {
        api.getSectorDetail.mockResolvedValue({
            ...DETAIL_NO_SIGNALS,
            sector_metrics: {
                relative_strength_1m: 0.02,
                dark_pool_signal: null,
                dark_pool_reason: 'stale',
                dark_pool_as_of: '2026-03-09',
            },
            provenance: {
                dark_pool_weekly: { unavailable: true, reason: 'stale', age_days: 224, stale_after_days: 35 },
                generated_at: '2026-10-19T21:00:00+00:00',
            },
        });
        api.getSectorHealth.mockResolvedValue(HEALTH_UNAVAILABLE);

        render(<SectorDive sector="Technology" onBack={vi.fn()} />);

        const note = await screen.findByTestId('note-Dark Pool Signal');
        expect(note.textContent).toContain('table stale 224d');
        expect(note.textContent).toContain('week too old for a signal');
        expect(note.textContent).toContain('week of 2026-03-09');
        const card = note.parentElement;
        expect(card.textContent).toContain('--');
        expect(document.body.textContent).not.toMatch(/accumulation|distribution/i);
        expect(document.body.textContent).not.toMatch(/Dark Pool: /);
    });

    it('explains a missing dark-pool signal when only short volume is absent', async () => {
        api.getSectorDetail.mockResolvedValue({
            ...DETAIL_NO_SIGNALS,
            sector_metrics: { dark_pool_signal: null, dark_pool_reason: 'no_short_volume', dark_pool_as_of: '2026-10-05' },
        });
        api.getSectorHealth.mockResolvedValue(HEALTH_UNAVAILABLE);

        render(<SectorDive sector="Technology" onBack={vi.fn()} />);

        const note = await screen.findByTestId('note-Dark Pool Signal');
        expect(note.textContent).toBe('no short volume reported · week of 2026-10-05');
    });

    // Case C: a future-dated disclosure was listed first and undated.
    it('shows disclosure dates for congressional rows and surfaces future-dated rows via provenance', async () => {
        api.getSectorDetail.mockResolvedValue({
            ...DETAIL_NO_SIGNALS,
            sector_metrics: {
                relative_strength_1m: 0.02,
                congressional_activity: [
                    { ticker: 'AAPL', date: '2026-09-22', representative: 'Nancy Example', type: 'BUY', amount: '$15,001' },
                ],
            },
            provenance: {
                congressional_trades: {
                    unavailable: true, reason: 'stale', age_days: 27, stale_after_days: 3, future_dated_rows: 8,
                },
                generated_at: '2026-10-19T21:00:00+00:00',
            },
        });
        api.getSectorHealth.mockResolvedValue(HEALTH_UNAVAILABLE);

        render(<SectorDive sector="Technology" onBack={vi.fn()} />);

        expect(await screen.findByText('2026-09-22')).toBeInTheDocument();
        const note = screen.getByTestId('note-Congressional (60d)');
        expect(note.textContent).toBe('table stale 27d · 8 future-dated not shown');
    });

    it('renders no provenance note when the tables are available', async () => {
        api.getSectorDetail.mockResolvedValue({
            ...DETAIL_NO_SIGNALS,
            sector_metrics: { dark_pool_signal: 'accumulation', dark_pool_as_of: '2026-10-05', congressional_activity: [] },
            provenance: {
                dark_pool_weekly: { unavailable: false, reason: null, age_days: 14 },
                congressional_trades: { unavailable: false, reason: null, future_dated_rows: 0 },
            },
        });
        api.getSectorHealth.mockResolvedValue(HEALTH_UNAVAILABLE);

        render(<SectorDive sector="Technology" onBack={vi.fn()} />);

        const note = await screen.findByTestId('note-Dark Pool Signal');
        expect(note.textContent).toBe('week of 2026-10-05');
        expect(screen.queryByTestId('note-Congressional (60d)')).toBeNull();
    });
});
