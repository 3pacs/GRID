import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Physics from '../views/Physics.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getPhysicsDashboard: vi.fn(),
        getNewsEnergy: vi.fn(),
        runPhysicsVerification: vi.fn(),
        getConventions: vi.fn(),
        getOUParams: vi.fn(),
        getHurst: vi.fn(),
        getEnergy: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Physics dashboard tab async data', () => {
    beforeEach(() => {
        api.getPhysicsDashboard.mockReset();
        api.getNewsEnergy.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the summary', async () => {
        const gate = deferred();
        api.getPhysicsDashboard.mockImplementation(() => gate.promise);

        render(<Physics />);

        fireEvent.click(screen.getByText('Load Physics Dashboard'));
        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            summary: 'Markets are calm.',
            market_energy: {}, news_energy: {}, hurst_exponents: {}, ou_parameters: {},
            energy_conservation: { state: 'equilibrium' },
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('Markets are calm.')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getPhysicsDashboard.mockRejectedValueOnce(new Error('physics dashboard unavailable'));

        render(<Physics />);
        fireEvent.click(screen.getByText('Load Physics Dashboard'));

        expect(await screen.findByText('physics dashboard unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getPhysicsDashboard.mockResolvedValueOnce({
            summary: 'Recovered.',
            market_energy: {}, news_energy: {}, hurst_exponents: {}, ou_parameters: {},
            energy_conservation: { state: 'equilibrium' },
        });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getPhysicsDashboard).toHaveBeenCalledTimes(2);
        });
        expect(screen.getByText('Recovered.')).toBeInTheDocument();
    });

    it('shows an error state when the api client resolves an error marker instead of throwing', async () => {
        api.getPhysicsDashboard.mockResolvedValueOnce({ error: true, status: 503, message: 'physics service unavailable' });

        render(<Physics />);
        fireEvent.click(screen.getByText('Load Physics Dashboard'));

        expect(await screen.findByText('physics service unavailable')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
    });
});
