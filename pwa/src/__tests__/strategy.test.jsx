/**
 * Tests for the Strategy view's honesty labeling (GRID-WAVE3-HELD-WRITERS-
 * TRIAGE-20260927.md #10): strategy/engine.py already tags default (never
 * explicitly assigned) strategies with source: "default" — the view must
 * render that distinctly from a strategy someone actually assigned via
 * POST /strategy/assign, not identically.
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Strategy from '../views/Strategy.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getActiveStrategies: vi.fn(),
    },
}));

vi.mock('../components/ViewHelp.jsx', () => ({
    default: () => <div data-testid="view-help" />,
}));

describe('Strategy view default-source labeling', () => {
    beforeEach(() => {
        api.getActiveStrategies.mockReset();
    });

    it('renders a DEFAULT badge for source: "default" strategies', async () => {
        api.getActiveStrategies.mockResolvedValue([
            {
                id: null, regime_state: 'GROWTH', name: 'Momentum Long', posture: 'AGGRESSIVE',
                allocation: '', risk_level: 'Medium', action: '', rationale: '',
                assigned_at: '', active: true, source: 'default',
            },
        ]);

        render(<Strategy />);

        await waitFor(() => {
            expect(screen.getByText('Momentum Long')).toBeInTheDocument();
        });
        expect(screen.getByText(/DEFAULT/)).toBeInTheDocument();
    });

    it('does not render a DEFAULT badge for an explicitly-assigned strategy', async () => {
        api.getActiveStrategies.mockResolvedValue([
            {
                id: 7, regime_state: 'GROWTH', name: 'Custom Momentum', posture: 'AGGRESSIVE',
                allocation: '', risk_level: 'Medium', action: '', rationale: '',
                assigned_at: '2026-09-20T00:00:00Z', active: true, source: 'database',
            },
        ]);

        render(<Strategy />);

        await waitFor(() => {
            expect(screen.getByText('Custom Momentum')).toBeInTheDocument();
        });
        expect(screen.queryByText(/DEFAULT/)).not.toBeInTheDocument();
    });
});
