import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import Timeline from '../views/Timeline.jsx';
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
        getRecurringPatterns: vi.fn().mockResolvedValue({ patterns: [] }),
        getEventTimeline: vi.fn(),
        get: vi.fn(),
    },
}));

describe('Timeline — causal-link honesty', () => {
    it('says no causal links were generated instead of silently drawing nothing', async () => {
        api.getEventTimeline.mockResolvedValue({ events: [] });
        api.get.mockResolvedValue({ links: [], generated: false, as_of: null });

        render(<Timeline selectedTicker="SPY" />);

        await waitFor(() => {
            expect(
                screen.getByText(/No causal links generated for SPY.*no causal-link run has completed.*not scheduled/),
            ).toBeInTheDocument();
        });
    });

    it('labels an empty result from a finished run with its as-of time', async () => {
        api.getEventTimeline.mockResolvedValue({ events: [] });
        api.get.mockResolvedValue({ links: [], generated: true, as_of: '2026-09-27T07:40:00+00:00' });

        render(<Timeline selectedTicker="SPY" />);

        await waitFor(() => {
            expect(
                screen.getByText(/No preceding-event links for SPY.*as of 2026-09-27 07:40 UTC/),
            ).toBeInTheDocument();
        });
        expect(screen.queryByText(/No causal links generated/)).not.toBeInTheDocument();
    });

    it('shows the as-of label and the not-proof-of-cause caveat when links exist', async () => {
        api.getEventTimeline.mockResolvedValue({ events: [] });
        api.get.mockResolvedValue({
            generated: true,
            as_of: '2026-09-27T07:40:00+00:00',
            links: [{
                id: '1', cause_type: 'earnings', cause_date: '2026-09-10',
                effect_date: '2026-09-20', score: 0.55, known_at: '2026-09-22T00:00:00+00:00',
            }],
        });

        render(<Timeline selectedTicker="SPY" />);

        await waitFor(() => {
            expect(screen.getByText(/as of 2026-09-27 07:40 UTC\. Timing, not proof of cause/)).toBeInTheDocument();
        });
    });

    it('shows nothing when links exist (no false empty banner)', async () => {
        api.getEventTimeline.mockResolvedValue({ events: [] });
        api.get.mockResolvedValue({
            links: [{ id: '1', cause_type: 'congressional', action_date: '2026-09-20' }],
        });

        render(<Timeline selectedTicker="SPY" />);

        await waitFor(() => {
            expect(api.get).toHaveBeenCalled();
        });
        expect(screen.queryByText(/No causal links generated/)).not.toBeInTheDocument();
    });
});
