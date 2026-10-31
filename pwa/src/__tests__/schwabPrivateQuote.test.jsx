import React from 'react';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import SchwabPrivateQuote, { describeError, describeRow } from '../components/SchwabPrivateQuote.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: { getSchwabPrivateQuotes: vi.fn() },
}));

afterEach(() => { cleanup(); vi.clearAllMocks(); });

const freshness = { as_of: '2026-10-31T14:29:59+00:00', vintage: '2026-10-31T14:30:00+00:00', age_seconds: 1, stale: false, stale_after_seconds: 20 };

function row(over = {}) {
    return {
        schema: 'schwab-private-quote-view-v1', symbol: 'SPY', provider_symbol: 'SPY', quote_status: 'fresh', live: true,
        status: 'available', last: 731.37, bid: 731.35, ask: 731.39, bid_size: 100, ask_size: 200, freshness, ...over,
    };
}

describe('describeRow', () => {
    it('labels LIVE only when the server says live and fresh', () => {
        expect(describeRow(row()).label).toBe('LIVE');
        expect(describeRow(row({ live: false })).label).toBe('FRESH');
        expect(describeRow(row({ quote_status: 'stale', live: true })).label).toBe('STALE');
        expect(describeRow(row({ quote_status: 'delayed', live: false })).label).toBe('DELAYED');
    });

    it('never shows numbers for unmeasured rows', () => {
        const d = describeRow(row({ quote_status: 'missing', live: false, status: 'unavailable', last: null, bid: null, ask: null, reason: 'no row', freshness: { as_of: null, vintage: freshness.vintage, age_seconds: null } }));
        expect(d.label).toBe('MISSING');
        expect([d.last, d.bid, d.ask, d.bidSize]).toEqual(['—', '—', '—', '—']);
        expect(d.asOf).toBeNull();
        expect(d.vintage).toBe(freshness.vintage);
        expect(d.reason).toBe('no row');
    });

    it('carries identity and as-of/vintage through', () => {
        const d = describeRow(row({ provider_symbol: 'SPY.X' }));
        expect(d.symbol).toBe('SPY');
        expect(d.providerSymbol).toBe('SPY.X');
        expect(d.asOf).toBe(freshness.as_of);
        expect(d.vintage).toBe(freshness.vintage);
    });

    it('does not render a count that JSON parsing could not carry exactly', () => {
        expect(describeRow(row({ bid_size: 9007199254740993 })).bidSize).toBe('n/a (>2^53)');
        expect(describeRow(row({ bid_size: 100 })).bidSize).toBe('100');
        expect(describeRow(row({ bid_size: 1.5 })).bidSize).toBe('—');
    });

    it('treats garbage as unavailable', () => {
        expect(describeRow(null).label).toBe('UNAVAILABLE');
        expect(describeRow({ quote_status: 'live', live: true }).label).toBe('UNAVAILABLE');
        expect(describeRow(row({ last: 'NaN' })).last).toBe('—');
    });
});

describe('describeError', () => {
    it('maps auth, flag and validation failures', () => {
        expect(describeError({ error: true, status: 401 })).toMatch(/Admin sign-in/);
        expect(describeError({ error: true, status: 403 })).toMatch(/Admin sign-in/);
        expect(describeError({ error: true, status: 503 })).toMatch(/disabled or not configured/);
        expect(describeError({ error: true, status: 422 })).toMatch(/rejected/);
        expect(describeError({ error: true, status: 0 })).toMatch(/Network/);
        expect(describeError(undefined)).toMatch(/Network/);
    });
});

describe('<SchwabPrivateQuote />', () => {
    it('renders live and stale rows with their own labels', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue({
            snapshot: {},
            view: {
                evaluated_at: '2026-10-31T14:30:00+00:00', source: 'schwab-marketdata-v1', redistribution: 'prohibited',
                rows: { SPY: row(), QQQ: row({ symbol: 'QQQ', provider_symbol: 'QQQ', quote_status: 'stale', live: false, reason: 'aged out' }) },
            },
        });
        render(<SchwabPrivateQuote symbols={['SPY', 'QQQ']} />);
        fireEvent.click(screen.getByText('Read private quote'));
        await waitFor(() => expect(screen.getByText('LIVE')).toBeTruthy());
        expect(screen.getByText('STALE')).toBeTruthy();
        expect(screen.getAllByText('LIVE')).toHaveLength(1);
        expect(screen.getByText(/redistribution prohibited/)).toBeTruthy();
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledWith(['SPY', 'QQQ']);
    });

    it('shows the auth message on 403 and no rows', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue({ error: true, status: 403 });
        render(<SchwabPrivateQuote />);
        fireEvent.click(screen.getByText('Read private quote'));
        await waitFor(() => expect(screen.getByText(/Admin sign-in required/)).toBeTruthy());
        expect(screen.queryByText('LIVE')).toBeNull();
    });

    it('shows the disabled message on 503', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue({ error: true, status: 503 });
        render(<SchwabPrivateQuote />);
        fireEvent.click(screen.getByText('Read private quote'));
        await waitFor(() => expect(screen.getByText(/disabled or not configured/)).toBeTruthy());
    });
});
