import React from 'react';
import { cleanup, fireEvent, render, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

// B-H8 / B-M20: a missing options snapshot and an empty scorecard must render as gaps,
// never as a number.
if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

const getEarningsCalendar = vi.fn();
const getRecentEarnings = vi.fn();
const getEarningsScorecard = vi.fn();

vi.mock('../api.js', () => ({
    api: {
        get getEarningsCalendar() {
            return getEarningsCalendar;
        },
        get getRecentEarnings() {
            return getRecentEarnings;
        },
        get getEarningsScorecard() {
            return getEarningsScorecard;
        },
        predictEarnings: vi.fn().mockResolvedValue({}),
    },
}));

const ENTRY_NO_IV = {
    ticker: 'AAPL',
    earnings_date: '2026-10-01',
    fiscal_quarter: 'Q4',
    eps_estimate: 1.5,
    revenue_estimate: null,
    reported: false,
    days_until: 14,
    iv_rank: null,
    iv_atm: null,
    expected_move_options: null,
    prediction: { direction: 'up', move_pct: 2.75, confidence: 0.4, verdict: 'pending', move_basis: 'history_only' },
};

const EMPTY_SCORECARD = {
    overall: { accuracy_pct: null, scored_n: 0, total_scored: 0, hits: 0, misses: 0, partials: 0, pending: 7 },
    per_direction: [],
    calibration: [],
    recent: [
        {
            ticker: 'MSFT',
            earnings_date: '2026-07-01',
            predicted_direction: 'flat',
            predicted_move_pct: null,
            predicted_move_basis: 'unavailable',
            expected_move_options: null,
            confidence: 0.1,
            actual_direction: null,
            actual_move_pct: null,
            verdict: 'pending',
            scored_at: null,
        },
    ],
};

async function renderView() {
    const { default: EarningsCalendar } = await import('../views/EarningsCalendar.jsx');
    return render(<EarningsCalendar />);
}

describe('EarningsCalendar null handling', () => {
    afterEach(() => {
        cleanup();
        getEarningsCalendar.mockReset();
        getRecentEarnings.mockReset();
        getEarningsScorecard.mockReset();
    });

    it('shows the expected-move gap instead of a number when there is no IV row', async () => {
        getEarningsCalendar.mockResolvedValue({ entries: [ENTRY_NO_IV] });
        getRecentEarnings.mockResolvedValue({ entries: [] });
        getEarningsScorecard.mockResolvedValue(EMPTY_SCORECARD);

        const { container, getByText } = await renderView();
        await waitFor(() => expect(container.textContent).toContain('AAPL'));
        fireEvent.click(getByText('Upcoming'));
        await waitFor(() => expect(container.textContent).toContain('Exp Move'));

        const text = container.textContent;
        expect(text).toContain('Exp Move (no IV)');
        expect(text).not.toContain('Exp Move (IV)');
        // The move that was computed from history alone says so.
        expect(text.toUpperCase()).toContain('HISTORY ONLY');
        // A missing implied move must never print as 0.0%.
        expect(text).not.toContain('0.0%');
    });

    it('renders no accuracy percentage when nothing has been scored', async () => {
        getEarningsCalendar.mockResolvedValue({ entries: [] });
        getRecentEarnings.mockResolvedValue({ entries: [] });
        getEarningsScorecard.mockResolvedValue(EMPTY_SCORECARD);

        const { container, getByText } = await renderView();
        await waitFor(() => expect(container.textContent).toContain('Scorecard'));
        fireEvent.click(getByText('Scorecard'));

        await waitFor(() => expect(container.textContent).toContain('Accuracy (nothing scored)'));
        const text = container.textContent;
        expect(text).not.toMatch(/\b0%/);
        expect(text.toUpperCase()).toContain('UNAVAILABLE');
    });
});
