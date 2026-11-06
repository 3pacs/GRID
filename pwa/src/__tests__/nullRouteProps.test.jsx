// Live test 2026-11-06 (release 5bafdb0b): Timeline, Causal Map (same chunk) and
// Influence crashed with "Cannot read properties of null (reading 'trim')".
// app.jsx passes selectedTicker as null (useState(null)); the `= ''` prop default
// only covers undefined. These render the views the way the app does.
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import Timeline from '../views/Timeline.jsx';
import InfluenceNetwork from '../views/InfluenceNetwork.jsx';
import ViewErrorBoundary from '../components/ViewErrorBoundary.jsx';
import {
    asText, routeTicker, routeText, textOr, upperOr, humanizeOr, lowerText,
} from '../utils/text.js';

vi.mock('../api.js', () => ({
    api: {
        getWatchlist: vi.fn(),
        getRecurringPatterns: vi.fn(),
        getEventTimeline: vi.fn(),
        get: vi.fn(),
    },
}));

beforeAll(() => {
    if (typeof globalThis.ResizeObserver === 'undefined') {
        globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    }
});

const ABSENT = [['null', null], ['undefined', undefined], ['empty', ''], ['whitespace', '  '], ['number', 0], ['object', {}]];

function expectNoBoundary(viewName) {
    expect(screen.queryByText(new RegExp(`${viewName} Error`, 'i'))).toBeNull();
}

describe('null-safe text helpers', () => {
    it('never throws on non-strings and renders an explicit fallback', () => {
        for (const [, v] of ABSENT) {
            expect(asText(v)).toBeNull();
            expect(routeTicker(v)).toBe('');
            expect(routeText(v)).toBe('');
            expect(textOr(v)).toBe('unknown');
            expect(upperOr(v)).toBe('UNKNOWN');
            expect(humanizeOr(v, 'source unknown')).toBe('source unknown');
        }
        expect(lowerText(null)).toBe('');
        expect(routeTicker(' spy ')).toBe('SPY');
        expect(textOr('  desc ')).toBe('desc');
        expect(upperOr('bullish')).toBe('BULLISH');
        expect(humanizeOr('insider_trade')).toBe('insider trade');
    });
});

describe('Timeline / Causal Map with a nullable selected ticker', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        api.getWatchlist.mockResolvedValue([]);
        api.getRecurringPatterns.mockResolvedValue({ patterns: [] });
        api.getEventTimeline.mockResolvedValue({ events: [] });
        api.get.mockResolvedValue({ links: [], generated: false, as_of: null });
    });

    it.each(ABSENT)('mounts with %s selectedTicker and invents no ticker', async (_, value) => {
        render(<ViewErrorBoundary viewName="timeline"><Timeline selectedTicker={value} /></ViewErrorBoundary>);
        await waitFor(() => expect(api.getRecurringPatterns).toHaveBeenCalled());
        expectNoBoundary('timeline');
        expect(screen.getByPlaceholderText(/ticker/i).value).toBe('');
    });

    it('renders events whose description/direction/event_type are null without crashing', async () => {
        api.getEventTimeline.mockResolvedValue({
            events: [
                { timestamp: '2026-10-01T00:00:00Z', event_type: null, direction: null, description: null, amount_usd: null },
                { timestamp: '2026-10-02T00:00:00Z', event_type: 'price_move', direction: null, description: null },
            ],
        });
        render(<ViewErrorBoundary viewName="timeline"><Timeline selectedTicker="spy" /></ViewErrorBoundary>);
        await waitFor(() => expect(api.getEventTimeline).toHaveBeenCalledWith('SPY', expect.anything()));
        await waitFor(() => expectNoBoundary('timeline'));
    });
});

describe('Influence with a nullable selected ticker', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        api.get.mockImplementation((path) => {
            if (path.includes('circular-flows')) return Promise.resolve({ loops: [] });
            if (path.includes('hypocrisy')) return Promise.resolve({ flags: [] });
            return Promise.resolve({
                nodes: [
                    { id: 'c1', type: 'company', ticker: null, label: null },
                    { id: 'p1', type: null, label: null },
                ],
                edges: [],
            });
        });
    });

    it.each(ABSENT)('mounts with %s selectedTicker after the graph loads', async (_, value) => {
        render(<ViewErrorBoundary viewName="influence"><InfluenceNetwork selectedTicker={value} /></ViewErrorBoundary>);
        await waitFor(() => expect(api.get).toHaveBeenCalled());
        await waitFor(() => expect(screen.queryByTestId('loading-skeleton')).toBeNull());
        expectNoBoundary('influence');
    });
});
