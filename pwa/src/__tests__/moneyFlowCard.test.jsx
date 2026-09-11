/**
 * Tests for the money_flow (MoneyFlowCard) home widget.
 *
 * `GET /api/v1/flows/sectors` (api.getSectorFlows()) returns `sectors` as an
 * object keyed by sector name (api/routers/flows.py), not an array — the
 * card used to spread it into an array literal and throw. These tests cover
 * the real payload shape plus the cold-process shape from PR #441
 * (`{ sectors: {}, stale: true, unavailable: true }`).
 *
 * Rendered through the exported `WidgetGrid` (the only export from
 * widgets.jsx) rather than importing MoneyFlowCard directly, since it is
 * not itself exported.
 */
import React from 'react';
import { render, screen, waitFor, fireEvent, cleanup } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { WidgetGrid } from '../components/home/widgets.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getSectorFlows: vi.fn(),
    },
}));

const MONEY_FLOW_WIDGETS = [{ type: 'money_flow', title: 'Where attention is going' }];

afterEach(() => {
    cleanup();
    vi.clearAllMocks();
});

describe('MoneyFlowCard', () => {
    beforeEach(() => {
        api.getSectorFlows.mockReset();
    });

    it('renders sectors ranked by absolute sector_stress from the real dict payload', async () => {
        api.getSectorFlows.mockResolvedValue({
            sectors: {
                Technology: {
                    etf: 'XLK',
                    etf_price: 210.5,
                    etf_change_30d: 0.04,
                    etf_z: 1.2,
                    actors: [],
                    sector_stress: 0.4,
                    subsectors: ['Semiconductors'],
                },
                Energy: {
                    etf: 'XLE',
                    etf_price: 90.1,
                    etf_change_30d: -0.02,
                    etf_z: -0.9,
                    actors: [],
                    sector_stress: -1.8,
                    subsectors: ['Oil & Gas'],
                },
                Utilities: {
                    etf: 'XLU',
                    etf_price: 65.0,
                    etf_change_30d: 0.01,
                    etf_z: 0.1,
                    actors: [],
                    sector_stress: 0.9,
                    subsectors: ['Electric Utilities'],
                },
            },
        });

        render(<WidgetGrid widgets={MONEY_FLOW_WIDGETS} />);

        await waitFor(() => {
            expect(screen.getByText('Energy')).toBeInTheDocument();
        });
        expect(screen.getByText('Utilities')).toBeInTheDocument();
        expect(screen.getByText('Technology')).toBeInTheDocument();

        // Ranked by |sector_stress| descending: Energy (1.8), Utilities (0.9), Technology (0.4)
        const names = screen.getAllByText(/Energy|Utilities|Technology/).map((el) => el.textContent);
        expect(names).toEqual(['Energy', 'Utilities', 'Technology']);

        expect(screen.getAllByText('▼ money pulling out')).toHaveLength(1);
        expect(screen.getAllByText('▲ money coming in')).toHaveLength(2);
    });

    it('renders the warming-up line and no rows for the cold-process payload', async () => {
        api.getSectorFlows.mockResolvedValue({ sectors: {}, stale: true, unavailable: true });

        render(<WidgetGrid widgets={MONEY_FLOW_WIDGETS} />);

        await waitFor(() => {
            expect(screen.getByText(/Money-flow data is still warming up\. Try again in a minute\./i)).toBeInTheDocument();
        });
        expect(screen.queryByText(/money coming in|money pulling out/)).not.toBeInTheDocument();
    });

    it('renders the existing empty-state line for an empty (non-stale) dict', async () => {
        api.getSectorFlows.mockResolvedValue({ sectors: {} });

        render(<WidgetGrid widgets={MONEY_FLOW_WIDGETS} />);

        await waitFor(() => {
            expect(screen.getByText('Nothing notable moving right now.')).toBeInTheDocument();
        });
    });

    it('shows the data age when the payload was seeded from an aged persisted snapshot', async () => {
        // snapshot_age_s only appears when api/routers/flows.py seeds the
        // stale tier from a persisted snapshot on cold start — it's the
        // signal that this data may not be fresh.
        const computedAt = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(); // 3h ago
        api.getSectorFlows.mockResolvedValue({
            sectors: {
                Energy: { etf: 'XLE', actors: [], sector_stress: -1.8, subsectors: [] },
            },
            computed_at: computedAt,
            snapshot_age_s: 3 * 60 * 60,
        });

        render(<WidgetGrid widgets={MONEY_FLOW_WIDGETS} />);

        await waitFor(() => {
            expect(screen.getByText('Energy')).toBeInTheDocument();
        });
        expect(screen.getByText(/Data as of 3 hours ago/i)).toBeInTheDocument();
    });

    it('does not show a data-age line for a normal fresh payload', async () => {
        api.getSectorFlows.mockResolvedValue({
            sectors: {
                Energy: { etf: 'XLE', actors: [], sector_stress: -1.8, subsectors: [] },
            },
            computed_at: new Date().toISOString(),
        });

        render(<WidgetGrid widgets={MONEY_FLOW_WIDGETS} />);

        await waitFor(() => {
            expect(screen.getByText('Energy')).toBeInTheDocument();
        });
        expect(screen.queryByText(/Data as of/i)).not.toBeInTheDocument();
    });

    it('renders the error state on a rejected fetch and retries on click', async () => {
        api.getSectorFlows.mockRejectedValueOnce(new Error('network down'));

        render(<WidgetGrid widgets={MONEY_FLOW_WIDGETS} />);

        await waitFor(() => {
            expect(api.getSectorFlows).toHaveBeenCalledTimes(1);
        });
        const retryButton = await screen.findByRole('button', { name: /try again/i });

        api.getSectorFlows.mockResolvedValueOnce({ sectors: {} });
        fireEvent.click(retryButton);

        await waitFor(() => {
            expect(api.getSectorFlows).toHaveBeenCalledTimes(2);
        });
        await waitFor(() => {
            expect(screen.getByText('Nothing notable moving right now.')).toBeInTheDocument();
        });
    });
});
