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
        api.get.mockResolvedValue({ links: [] });

        render(<Timeline selectedTicker="SPY" />);

        await waitFor(() => {
            expect(
                screen.getByText(/No causal links generated for SPY.*no scheduled writer/),
            ).toBeInTheDocument();
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
