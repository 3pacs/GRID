import fs from 'node:fs';
import path from 'node:path';
import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import CorrelationMatrix from '../views/CorrelationMatrix.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getDiscoveryCorrelationMatrix: vi.fn(),
        getTimeseries: vi.fn(),
    },
}));

const MATRIX_RESULT = {
    features: ['SPY', 'QQQ'],
    matrix: [[1, 0.5], [0.5, 1]],
    regime_matrices: {},
    breakdowns: [],
    current_regime: 'GROWTH',
    pca: { components: [], total_variance: 0 },
    n_observations: 120,
};

async function openScatterForFirstOffDiagonalCell(container) {
    let cell;
    await waitFor(() => {
        const cells = container.querySelectorAll('rect.corr-cell');
        expect(cells.length).toBeGreaterThan(0);
        cell = Array.from(cells).find(c => c.getAttribute('x') !== c.getAttribute('y'));
        expect(cell).toBeTruthy();
    });
    fireEvent.click(cell);
}

describe('CorrelationMatrix scatter plot truthfulness', () => {
    beforeEach(() => {
        api.getDiscoveryCorrelationMatrix.mockReset();
        api.getTimeseries.mockReset();
        api.getDiscoveryCorrelationMatrix.mockResolvedValue(MATRIX_RESULT);
    });

    it('renders real backend time series in the scatter modal instead of synthetic points', async () => {
        api.getTimeseries.mockResolvedValue({
            series: { spy_close: [1, 2, 3, 4], qqq_close: [2, 3, 4, 5] },
            dates: {
                spy_close: ['2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04'],
                qqq_close: ['2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04'],
            },
            days: 90,
            count: 2,
        });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(api.getTimeseries).toHaveBeenCalledWith(['spy_close', 'qqq_close'], 90);
        });

        await waitFor(() => {
            expect(container.querySelectorAll('svg circle').length).toBe(4);
        });
    });

    it('joins mismatched-calendar series on date and plots only the overlap, never index-pairs mismatched points', async () => {
        // spy_close trades weekdays only; qqq_close has an extra weekend row and
        // a gap -- naive index pairing would misalign every point past the gap.
        api.getTimeseries.mockResolvedValue({
            series: {
                spy_close: [100, 101, 102, 103],
                qqq_close: [200, 201, 202, 203, 204],
            },
            dates: {
                spy_close: ['2026-09-01', '2026-09-02', '2026-09-04', '2026-09-05'],
                qqq_close: ['2026-08-31', '2026-09-01', '2026-09-02', '2026-09-03', '2026-09-05'],
            },
            days: 90,
            count: 2,
        });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(api.getTimeseries).toHaveBeenCalledWith(['spy_close', 'qqq_close'], 90);
        });

        // Common dates: 2026-09-01, 2026-09-02, 2026-09-05 -- three points only.
        await waitFor(() => {
            expect(container.querySelectorAll('svg circle').length).toBe(3);
        });
    });

    it('renders an honest empty state instead of a random scatter when no real pair data exists', async () => {
        api.getTimeseries.mockResolvedValue({ series: {}, dates: {}, days: 90, count: 0 });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(screen.getByText('Pair time series not available yet')).toBeInTheDocument();
        });
        expect(container.querySelectorAll('svg circle').length).toBe(0);
    });

    it('shows the empty state instead of index-pairing when an older backend omits "dates"', async () => {
        api.getTimeseries.mockResolvedValue({
            series: { spy_close: [1, 2, 3, 4], qqq_close: [2, 3, 4, 5] },
            days: 90,
            count: 2,
        });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(screen.getByText('Pair time series not available yet')).toBeInTheDocument();
        });
        expect(container.querySelectorAll('svg circle').length).toBe(0);
    });

    it('shows a no-overlap empty state when both series exist but share no dates', async () => {
        api.getTimeseries.mockResolvedValue({
            series: { spy_close: [1, 2], qqq_close: [3, 4] },
            dates: {
                spy_close: ['2026-09-01', '2026-09-02'],
                qqq_close: ['2026-10-01', '2026-10-02'],
            },
            days: 90,
            count: 2,
        });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(screen.getByText('No overlapping dates for this pair')).toBeInTheDocument();
        });
        expect(container.querySelectorAll('svg circle').length).toBe(0);
    });

    it('shows an error state when the timeseries fetch fails, never fabricated points', async () => {
        api.getTimeseries.mockResolvedValue({ error: true, status: 500, message: 'Timeseries fetch failed' });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(screen.getByText('Timeseries fetch failed')).toBeInTheDocument();
        });
        expect(container.querySelectorAll('svg circle').length).toBe(0);
    });
});

describe('CorrelationMatrix source', () => {
    it('contains no Math.random or placeholder data-fabrication paths', () => {
        const src = fs.readFileSync(
            path.resolve(process.cwd(), 'src/views/CorrelationMatrix.jsx'),
            'utf8',
        );
        expect(src).not.toMatch(/Math\.random/i);
        expect(src).not.toMatch(/\bplaceholder\b/i);
    });
});
