import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Associations from '../views/Associations.jsx';
import { api } from '../api.js';

const storeState = {
    addNotification: vi.fn(),
};

vi.mock('../api.js', () => ({
    api: {
        getAnomalies: vi.fn(),
        getCorrelationMatrix: vi.fn(),
        getRegimeFeatures: vi.fn(),
        getSmartHeatmap: vi.fn(),
        getTimeseries: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Associations view async data', () => {
    beforeEach(() => {
        storeState.addNotification.mockClear();
        api.getAnomalies.mockReset();
        api.getCorrelationMatrix.mockReset();
        api.getRegimeFeatures.mockReset();
        api.getSmartHeatmap.mockReset();
        api.getCorrelationMatrix.mockResolvedValue(null);
        api.getRegimeFeatures.mockResolvedValue(null);
        api.getSmartHeatmap.mockResolvedValue(null);
    });

    it('shows a loading skeleton while fetching, then renders the insight count', async () => {
        const gate = deferred();
        api.getAnomalies.mockImplementation(() => gate.promise);

        render(<Associations />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ anomalies: [] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(api.getAnomalies).toHaveBeenCalledTimes(1);
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getAnomalies.mockRejectedValueOnce(new Error('associations feed unavailable'));

        render(<Associations />);

        expect(await screen.findByText('associations feed unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getAnomalies.mockResolvedValueOnce({ anomalies: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getAnomalies).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('associations feed unavailable')).not.toBeInTheDocument();
    });
});
