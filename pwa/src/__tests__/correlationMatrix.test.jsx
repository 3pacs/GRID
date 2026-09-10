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

    it('renders an honest empty state instead of a random scatter when no real pair data exists', async () => {
        api.getTimeseries.mockResolvedValue({ series: {}, days: 90, count: 0 });

        const { container } = render(<CorrelationMatrix />);
        await openScatterForFirstOffDiagonalCell(container);

        await waitFor(() => {
            expect(screen.getByText('Pair time series not available yet')).toBeInTheDocument();
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
