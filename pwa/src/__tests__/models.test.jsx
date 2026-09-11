import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Models from '../views/Models.jsx';
import { api } from '../api.js';

const storeState = {
    allModels: [],
    productionModels: {},
    setAllModels: vi.fn((m) => { storeState.allModels = m; }),
    setProductionModels: vi.fn((m) => { storeState.productionModels = m; }),
    addNotification: vi.fn(),
};

vi.mock('../api.js', () => ({
    api: {
        getModels: vi.fn(),
        getProductionModels: vi.fn(),
        transitionModel: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Models view async data', () => {
    beforeEach(() => {
        storeState.allModels = [];
        storeState.productionModels = {};
        storeState.setAllModels.mockClear();
        storeState.setProductionModels.mockClear();
        api.getModels.mockReset();
        api.getProductionModels.mockReset();
        api.getProductionModels.mockResolvedValue({ models: {} });
    });

    it('shows a loading skeleton while fetching, then renders the registered models', async () => {
        const gate = deferred();
        api.getModels.mockImplementation(() => gate.promise);

        render(<Models />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ models: [{ id: 'm1', name: 'alpha-v2', version: 3, state: 'STAGING', layer: 'TACTICAL' }] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(storeState.setAllModels).toHaveBeenCalledWith([
            { id: 'm1', name: 'alpha-v2', version: 3, state: 'STAGING', layer: 'TACTICAL' },
        ]);
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getModels.mockRejectedValueOnce(new Error('model registry unavailable'));

        render(<Models />);

        expect(await screen.findByText('model registry unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getModels.mockResolvedValueOnce({ models: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getModels).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('model registry unavailable')).not.toBeInTheDocument();
    });
});
