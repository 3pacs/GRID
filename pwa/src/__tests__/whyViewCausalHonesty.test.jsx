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
        api.getCausalLinks.mockResolvedValue({ causes: [], generated: false, as_of: null });
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

    it('shows persisted links as preceding events with a heuristic score and as-of label', async () => {
        api.getForensicReports.mockResolvedValue({ reports: [MOVE] });
        api.getCausalLinks.mockResolvedValue({
            generated: true,
            as_of: '2026-09-27T07:40:00+00:00',
            causes: [{
                ticker: 'SPY', cause_type: 'earnings', probable_cause: 'Earnings beat released 2026-09-10',
                actor: 'Jane Doe', action: 'SELL', action_date: '2026-09-15', score: 0.62,
                probability: 0.62, lead_time_days: 4, known_at: '2026-09-17T00:00:00+00:00', evidence: [],
            }],
        });
        api.getEventTimeline.mockResolvedValue({ events: [] });

        render(<WhyView />);

        fireEvent.change(screen.getByPlaceholderText('Ticker (e.g. NVDA)'), { target: { value: 'SPY' } });
        fireEvent.click(screen.getByText('Investigate'));
        await waitFor(() => {
            expect(screen.getByText(/SIGNIFICANT MOVES DETECTED/)).toBeInTheDocument();
        });
        fireEvent.click(screen.getByText('2026-09-20'));

        await waitFor(() => {
            expect(screen.getByText('PRECEDING PUBLIC EVENTS')).toBeInTheDocument();
        });
        expect(screen.getByText(/timing, not proof of cause/)).toBeInTheDocument();
        expect(screen.getByText(/As of 2026-09-27 07:40 UTC/)).toBeInTheDocument();
        expect(screen.getByText('score 0.62')).toBeInTheDocument();
        expect(screen.queryByText('62%')).not.toBeInTheDocument();
    });
});
