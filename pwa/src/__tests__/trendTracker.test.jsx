import fs from 'node:fs';
import path from 'node:path';
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import TrendTracker from '../views/TrendTracker.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getTrends: vi.fn(),
    },
}));

// Names and figures that lived in the deleted generatePlaceholderData() and
// must never render again, whatever the API returns.
const FABRICATED = [
    'Tech Sector Death Cross',
    'Regime Shift: GROWTH -> FRAGILE',
    'VIX Term Structure in Backwardation',
    'BTC-NASDAQ Decoupling',
    'VIX (22.4)',
];

describe('TrendTracker truthfulness', () => {
    beforeEach(() => {
        api.getTrends.mockReset();
    });

    it('renders an honest empty state, not fabricated trends, when the API returns nothing', async () => {
        api.getTrends.mockResolvedValue({ trends: [] });

        render(<TrendTracker />);

        await waitFor(() => {
            expect(screen.getByText(/No trends detected for the last 90 days/)).toBeInTheDocument();
        });
        for (const name of FABRICATED) {
            expect(screen.queryByText(name, { exact: false })).not.toBeInTheDocument();
        }
        // A client-synthesized payload used to self-certify with new Date();
        // nothing should claim a generation time when the API sent none.
        expect(screen.queryByText(/Generated:/)).not.toBeInTheDocument();
    });

    it('surfaces an API error marker as an error state instead of fabricating data', async () => {
        api.getTrends.mockResolvedValue({ error: true, status: 503, message: 'trends backend down' });

        render(<TrendTracker />);

        await waitFor(() => {
            expect(screen.getByText('trends backend down')).toBeInTheDocument();
        });
        expect(screen.getByText('Trend data unavailable')).toBeInTheDocument();
        for (const name of FABRICATED) {
            expect(screen.queryByText(name, { exact: false })).not.toBeInTheDocument();
        }
    });

    it('surfaces a thrown fetch error as an error state instead of fabricating data', async () => {
        api.getTrends.mockRejectedValue(new Error('network unreachable'));

        render(<TrendTracker />);

        await waitFor(() => {
            expect(screen.getByText('network unreachable')).toBeInTheDocument();
        });
        for (const name of FABRICATED) {
            expect(screen.queryByText(name, { exact: false })).not.toBeInTheDocument();
        }
    });

    it('renders real API trends and the API-supplied generation time', async () => {
        api.getTrends.mockResolvedValue({
            trends: [
                {
                    name: 'Real momentum trend',
                    category: 'momentum',
                    direction: 'bullish',
                    strength: 0.61,
                    description: 'from the API',
                    confidence: 0.7,
                    start_date: '2026-09-01',
                    data_points: [],
                },
            ],
            category_summaries: {},
            narrative: 'api narrative',
            generated_at: '2026-09-17T12:00:00Z',
        });

        render(<TrendTracker />);

        await waitFor(() => {
            expect(screen.getByText('Real momentum trend', { exact: false })).toBeInTheDocument();
        });
        expect(screen.getByText(/Generated:/)).toBeInTheDocument();
    });

    it('source contains no placeholder generator, no random walk, and no synthetic time axis', () => {
        const src = fs.readFileSync(path.resolve(__dirname, '../views/TrendTracker.jsx'), 'utf8');
        expect(src).not.toMatch(/generatePlaceholderData/);
        expect(src).not.toMatch(/Math\.random\(/);
        expect(src).not.toMatch(/TREND STRENGTH OVER TIME/);
        expect(src).not.toMatch(/generated_at: new Date\(\)/);
    });
});
