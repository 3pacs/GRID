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

describe('Physics news-energy honesty states', () => {
    beforeEach(() => {
        api.getNewsEnergy.mockReset();
    });

    function openNewsEnergyTab() {
        render(<Physics />);
        fireEvent.click(screen.getByText('News Energy'));
    }

    it('renders an explicit unavailable state instead of fabricated zeros when no source has usable data', async () => {
        api.getNewsEnergy.mockResolvedValueOnce({
            as_of_date: '2026-09-26', as_of: null, lookback_days: 0,
            n_news_sources: 0, energy_by_source: [], excluded_sources: [],
            total_news_energy: 0.0,
            coherence: { coherence: 0.0, dominant_direction: 'neutral', aligned_sources: [], n_sources: 0 },
            force_vector: [],
            regime_signal: { equilibrium: true, violations: 0, violating_sources: [], interpretation: 'no news features available' },
            freshness: { as_of: null, age_days: null, stale: null, stale_after_days: 5 },
            available: false,
            stale: null,
            summary: 'no news features available',
        });

        openNewsEnergyTab();
        fireEvent.click(screen.getByText('Load News Energy'));

        expect(await screen.findByText('News Energy Unavailable')).toBeInTheDocument();
        expect(screen.getByText('no news features available')).toBeInTheDocument();
        // Must not render a fabricated "0.00" total-energy metric tile.
        expect(screen.queryByText('Total News Energy')).not.toBeInTheDocument();
    });

    it('shows a stale banner and names excluded sources instead of presenting old numbers as current', async () => {
        api.getNewsEnergy.mockResolvedValueOnce({
            as_of_date: '2026-09-26', as_of: '2026-09-01', lookback_days: 30,
            n_news_sources: 1,
            energy_by_source: [{
                feature: 'crucix_conflict', kinetic_energy: 0.1, potential_energy: 0.1, total_energy: 0.2,
                energy_level: 'low', conservation_ratio: 1.0, market_correlations: {},
                last_observed: '2026-09-01', stale: true,
            }],
            excluded_sources: [{ feature: 'gdelt_theme_econ', reason: 'only 2 observations in the lookback window (need 10)', last_observed: '2026-03-19' }],
            total_news_energy: 0.2,
            coherence: { coherence: 1.0, dominant_direction: 'increasing', aligned_sources: ['crucix_conflict'], n_sources: 1 },
            force_vector: [],
            regime_signal: { equilibrium: true, violations: 0, violating_sources: [], interpretation: 'stable' },
            freshness: { as_of: '2026-09-01', age_days: 25, stale: true, stale_after_days: 5 },
            available: true,
            stale: true,
            summary: 'STALE: newest news observation is 25 day(s) old.',
        });

        openNewsEnergyTab();
        fireEvent.click(screen.getByText('Load News Energy'));

        expect(await screen.findByText(/Stale — as of 2026-09-01/)).toBeInTheDocument();
        expect(screen.getByText(/gdelt_theme_econ/)).toBeInTheDocument();
        expect(screen.getByText('1 known source(s) excluded from this analysis:')).toBeInTheDocument();
    });
});
