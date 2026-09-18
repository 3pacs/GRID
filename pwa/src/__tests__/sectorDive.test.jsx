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
});
