import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import WhyView from '../views/WhyView.jsx';
import { api } from '../api.js';

beforeAll(() => {
    if (typeof globalThis.ResizeObserver === 'undefined') {
        globalThis.ResizeObserver = class {
            observe() {}
            unobserve() {}
            disconnect() {}
        };
    }
});

vi.mock('../api.js', () => ({
    api: {
        getWatchlist: vi.fn().mockResolvedValue([]),
        getForensicReports: vi.fn(),
        analyzeForensicMove: vi.fn(),
        getCausalLinks: vi.fn(),
        getEventTimeline: vi.fn(),
    },
}));

const MOVE = {
    move_date: '2026-09-20',
    move_pct: 0.06,
    move_direction: 'up',
    warning_signals: 2,
    confidence: 0.7,
    preceding_events: [{ ts: '2026-09-19T00:00:00Z', description: 'test event' }],
    total_dollar_flow: 0,
};

describe('WhyView — causal-link honesty', () => {
    it('never claims the causation engine "runs periodically" when nothing was generated', async () => {
        api.getForensicReports.mockResolvedValue({ reports: [MOVE] });
        api.getCausalLinks.mockResolvedValue({ causes: [] });
        api.getEventTimeline.mockResolvedValue({ events: [] });

        render(<WhyView />);

        fireEvent.change(screen.getByPlaceholderText('Ticker (e.g. NVDA)'), { target: { value: 'SPY' } });
        fireEvent.click(screen.getByText('Investigate'));

        await waitFor(() => {
            expect(screen.getByText(/SIGNIFICANT MOVES DETECTED/)).toBeInTheDocument();
        });
        fireEvent.click(screen.getByText('2026-09-20'));

        await waitFor(() => {
            expect(screen.getByText(/Not generated — no causal links identified/)).toBeInTheDocument();
        });
        // The false claim this replaces ("runs periodically") must be gone.
        expect(screen.queryByText(/causation engine runs periodically/)).not.toBeInTheDocument();
    });
});
