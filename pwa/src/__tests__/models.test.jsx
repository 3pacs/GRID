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

    // Item #19 (Wave 3 triage report): a PRODUCTION row created from a
    // PASSED hypothesis (hypothesis_id set) is autoresearch lineage, not an
    // actively maintained model — the view must say so, with the row's own
    // date, rather than presenting it as current.
    it('shows an autoresearch-lineage banner when a PRODUCTION model has a hypothesis_id', async () => {
        storeState.productionModels = {
            REGIME: {
                id: 42, name: 'regime-autoresearch-v1', version: '1',
                hypothesis_id: 7, created_at: '2026-03-25T00:00:00Z',
            },
        };
        api.getModels.mockResolvedValue({ models: [] });
        api.getProductionModels.mockResolvedValue({ models: storeState.productionModels });

        render(<Models />);

        await waitFor(() => expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument());

        const banner = screen.getByTestId('models-autoresearch-banner');
        expect(banner.textContent).toContain('regime-autoresearch-v1');
        expect(banner.textContent).toContain('2026-03-25');
        expect(banner.textContent).toContain('not an actively');
    });

    it('does not show the autoresearch banner when no PRODUCTION model has a hypothesis_id', async () => {
        storeState.productionModels = {
            REGIME: { id: 9, name: 'regime-maintained', version: '2', hypothesis_id: null, created_at: '2026-09-01T00:00:00Z' },
        };
        api.getModels.mockResolvedValue({ models: [] });
        api.getProductionModels.mockResolvedValue({ models: storeState.productionModels });

        render(<Models />);

        await waitFor(() => expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument());

        expect(screen.queryByTestId('models-autoresearch-banner')).not.toBeInTheDocument();
    });
});
