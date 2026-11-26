/**
 * Tests for the async post-mortem Lessons widget.
 *
 * Verifies:
 *   1. On mount, fetches via api.getPostmortemLessons with default n=5, days=30
 *   2. Loading state renders briefly, then lessons render
 *   3. Refresh button triggers fetch with refresh=true
 *   4. Error responses render the "unavailable" state instead of crashing
 *   5. Empty payloads render the empty state message
 */
import React from 'react';
import { render, screen, waitFor, fireEvent, cleanup } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import LessonsWidget from '../components/LessonsWidget.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getPostmortemLessons: vi.fn(),
    },
}));

afterEach(() => {
    cleanup();
    vi.clearAllMocks();
});

describe('LessonsWidget', () => {
    beforeEach(() => {
        api.getPostmortemLessons.mockReset();
    });

    it('fetches lessons on mount with default params', async () => {
        api.getPostmortemLessons.mockResolvedValue({
            lessons: ['Cover the puts before earnings.', 'Avoid Friday OPEX bull runs.'],
            generated_at: new Date().toISOString(),
        });

        render(<LessonsWidget />);

        await waitFor(() => {
            expect(api.getPostmortemLessons).toHaveBeenCalledWith(5, 30, false);
        });

        await waitFor(() => {
            expect(screen.getByText('Cover the puts before earnings.')).toBeInTheDocument();
        });
        expect(screen.getByText('Avoid Friday OPEX bull runs.')).toBeInTheDocument();
    });

    it('shows loading state before resolving', async () => {
        let resolveFn;
        api.getPostmortemLessons.mockReturnValue(new Promise(r => { resolveFn = r; }));

        render(<LessonsWidget />);

        expect(screen.getByText(/Loading lessons/i)).toBeInTheDocument();

        resolveFn({ lessons: ['Done.'], generated_at: null });
        await waitFor(() => {
            expect(screen.getByText('Done.')).toBeInTheDocument();
        });
    });

    it('refresh button calls endpoint with refresh=true', async () => {
        api.getPostmortemLessons.mockResolvedValue({ lessons: ['L1'], generated_at: null });

        render(<LessonsWidget />);

        // Wait for the first load to *render*, not just for the call to be
        // recorded: clicking Refresh while the widget is still loading is a
        // no-op, which made this flaky on slow CI runners.
        await waitFor(() => {
            expect(api.getPostmortemLessons).toHaveBeenCalledTimes(1);
            expect(screen.getByText('L1')).toBeInTheDocument();
        });

        fireEvent.click(screen.getByTestId('lessons-refresh'));

        await waitFor(() => {
            expect(api.getPostmortemLessons).toHaveBeenCalledTimes(2);
        });
        expect(api.getPostmortemLessons).toHaveBeenLastCalledWith(5, 30, true);
    });

    it('renders the unavailable state on error response', async () => {
        api.getPostmortemLessons.mockResolvedValue({
            error: true,
            status: 503,
            message: 'Backend offline',
        });

        render(<LessonsWidget />);

        await waitFor(() => {
            expect(screen.getByText(/Lessons unavailable/i)).toBeInTheDocument();
        });
        expect(screen.getByText(/Backend offline/i)).toBeInTheDocument();
    });

    it('renders empty state when there are no lessons', async () => {
        api.getPostmortemLessons.mockResolvedValue({ lessons: [], generated_at: null });

        render(<LessonsWidget days={14} />);

        await waitFor(() => {
            expect(api.getPostmortemLessons).toHaveBeenCalledWith(5, 14, false);
        });
        expect(await screen.findByText(/No post-mortem lessons in the last 14 days/i)).toBeInTheDocument();
    });

    it('shows the summary the endpoint really returns: lessons as {text, count, label} (#959)', async () => {
        // Measured on the served build 2026-11-26: label "LLM summary of 5 postmortems"
        // sat above "No post-mortem lessons" because the object shape normalized to [].
        api.getPostmortemLessons.mockResolvedValue({
            lessons: {
                text: 'Cover the puts before earnings.\nAvoid Friday OPEX bull runs.',
                count: 5,
                label: 'LLM summary of 5 postmortems dated 2026-11-26',
            },
            generated_at: new Date().toISOString(),
            label: 'LLM summary of 5 postmortems dated 2026-11-26',
            cached: false,
        });

        render(<LessonsWidget />);

        const report = await screen.findByTestId('lessons-report');
        expect(report).toHaveTextContent('Cover the puts before earnings.');
        expect(report).toHaveTextContent('Avoid Friday OPEX bull runs.');
        expect(screen.queryByText(/No post-mortem lessons/i)).not.toBeInTheDocument();
    });

    it('shows the whole multi-section text report, not its first n lines (#959)', async () => {
        // Measured on the served build 2026-11-26 (c6cdc663): the report was split into
        // lines and capped at n=5, so only the ruler lines, the title and the first two
        // stats showed and every section after them was cut off.
        const text = [
            '='.repeat(60), '  GRID Post-Mortem — Lessons Learned', '='.repeat(60), '',
            'Failures analysed: 5', 'Average return:    -8.00%', '',
            '-- Failure Category Breakdown --', '  wrong_signal                          3  (60%)', '',
            '-- Most Affected Tickers --', '  ACME      2 failures', '',
            '-- Recommendations --', '  [!] >40% of failures are wrong signals — review scanner signal quality.',
        ].join('\n');
        api.getPostmortemLessons.mockResolvedValue({
            lessons: { text, count: 5, label: 'Rule-based summary of 5 postmortems' },
            generated_at: new Date().toISOString(), label: 'Rule-based summary of 5 postmortems',
        });

        render(<LessonsWidget />);

        const report = await screen.findByTestId('lessons-report');
        expect(report).toHaveTextContent('-- Failure Category Breakdown --');
        expect(report).toHaveTextContent('wrong_signal');
        expect(report).toHaveTextContent('-- Most Affected Tickers --');
        expect(report).toHaveTextContent('-- Recommendations --');
        expect(report).toHaveTextContent('review scanner signal quality');
        // Layout is kept as written (columns, indents), not reflowed into bullets.
        expect(report.textContent).toBe(text);
    });

    it('still reads an empty window as empty when the object says count 0 (#959)', async () => {
        api.getPostmortemLessons.mockResolvedValue({
            lessons: { lessons: [], text: 'No post-mortems found in the last 30 days.', count: 0 },
            generated_at: null, label: null, note: 'no postmortems in the last 30 days',
        });

        render(<LessonsWidget />);

        expect(await screen.findByText(/No post-mortem lessons in the last 30 days/i)).toBeInTheDocument();
        expect(screen.queryByText(/No post-mortems found/i)).not.toBeInTheDocument();
    });

    it('shows the honesty label ("LLM summary of N postmortems dated X-Y")', async () => {
        // Wave 3 #6 (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md): the backend
        // now returns a `label` describing exactly what was summarized —
        // the widget must surface it, not just a relative timestamp.
        api.getPostmortemLessons.mockResolvedValue({
            lessons: ['Cover the puts before earnings.'],
            generated_at: new Date().toISOString(),
            label: 'LLM summary of 5 postmortems dated 2026-09-14 to 2026-09-18',
        });

        render(<LessonsWidget />);

        expect(await screen.findByText(/LLM summary of 5 postmortems dated 2026-09-14 to 2026-09-18/))
            .toBeInTheDocument();
    });

    it('tolerates a legacy text blob (string lessons) and shows it whole', async () => {
        api.getPostmortemLessons.mockResolvedValue({
            lessons: '- First bullet\n- Second bullet\n* Third bullet\n- Fourth\n- Fifth\n- Sixth bullet',
            generated_at: null,
        });

        render(<LessonsWidget />);

        const report = await screen.findByTestId('lessons-report');
        expect(report).toHaveTextContent('First bullet');
        expect(report).toHaveTextContent('Third bullet');
        // Past the old n=5 cap.
        expect(report).toHaveTextContent('Sixth bullet');
    });
});
