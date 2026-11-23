import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import MarketDiary from '../views/MarketDiary.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getDiaryList: vi.fn(),
        getDiaryEntry: vi.fn(),
        searchDiary: vi.fn(),
        generateDiary: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('MarketDiary view async data', () => {
    beforeEach(() => {
        api.getDiaryList.mockReset();
        api.getDiaryEntry.mockReset();
        api.getDiaryEntry.mockResolvedValue({ error: 'no entry' });
    });

    it('shows a loading skeleton while fetching, then renders the entry list', async () => {
        const gate = deferred();
        api.getDiaryList.mockImplementation(() => gate.promise);

        render(<MarketDiary />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ entries: [{ date: '2026-09-01', thesis_verdict: 'correct' }] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(api.getDiaryList).toHaveBeenCalledTimes(1);
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getDiaryList.mockRejectedValueOnce(new Error('diary list unavailable'));

        render(<MarketDiary />);

        expect(await screen.findByText('diary list unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getDiaryList.mockResolvedValueOnce({ entries: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getDiaryList).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('diary list unavailable')).not.toBeInTheDocument();
    });

    it('renders the missed verdict badge, calendar dot, and legend entry', async () => {
        const now = new Date();
        const entryDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-15`;
        api.getDiaryList.mockResolvedValueOnce({
            entries: [{ date: entryDate, verdict: 'missed' }],
        });
        api.getDiaryEntry.mockResolvedValueOnce({
            date: entryDate,
            content: 'Diary body.',
            generated_at: `${entryDate}T22:00:00Z`,
            thesis_accuracy: { verdict: 'missed', morning_thesis: 'NEUTRAL', sp500_return_pct: 1.85 },
        });

        render(<MarketDiary />);

        await waitFor(() => {
            expect(screen.getAllByText('MISSED').length).toBe(2);
        });
        expect(screen.queryByText('N/A')).not.toBeInTheDocument();
        const dayButton = screen.getByRole('button', { name: '15' });
        const dot = dayButton.querySelector('div');
        expect(dot).not.toBeNull();
        expect(dot.style.background).toMatch(/rgb\(249,\s*115,\s*22\)|#f97316/i);
    });
});
