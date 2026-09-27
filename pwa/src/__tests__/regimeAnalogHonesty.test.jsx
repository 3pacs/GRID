import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import RegimeAnalog from '../views/RegimeAnalog.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getRegimeAnalogs: vi.fn(),
    },
}));

const BASE = {
    regime: { axes: [] },
    forecast: { confidence_level: 'low', outcomes: {}, disagreement_flags: [] },
    matches: { n_matches: 0, episodes: [], mean_quality: 0, effective_sample_size: 0, dimension_importance: {} },
};

describe('RegimeAnalog — honest availability states', () => {
    it('surfaces the TimesFM unavailable reason instead of silently dropping the tab', async () => {
        api.getRegimeAnalogs.mockResolvedValue({
            ...BASE,
            timesfm: { available: false, reason: 'insufficient GPU memory (0.5GB free)' },
        });

        render(<RegimeAnalog />);

        await waitFor(() => {
            expect(
                screen.getByText(/Not generated — TimesFM comparison unavailable: insufficient GPU memory/),
            ).toBeInTheDocument();
        });
        expect(screen.queryByText('TimesFM Compare')).not.toBeInTheDocument();
    });

    it('shows the TimesFM tab with no unavailable note when it is available', async () => {
        api.getRegimeAnalogs.mockResolvedValue({
            ...BASE,
            timesfm: { available: true, model: 'timesfm-2.5-200m', forecasts: {} },
        });

        render(<RegimeAnalog />);

        await waitFor(() => {
            expect(screen.getByText('TimesFM Compare')).toBeInTheDocument();
        });
        expect(screen.queryByText(/TimesFM comparison unavailable/)).not.toBeInTheDocument();
    });

    it('states no historical episode matched, rather than an empty table with no explanation', async () => {
        api.getRegimeAnalogs.mockResolvedValue(BASE);

        render(<RegimeAnalog />);

        await waitFor(() => {
            expect(screen.getByText('Episodes (0)')).toBeInTheDocument();
        });
        fireEvent.click(screen.getByText('Episodes (0)'));

        await waitFor(() => {
            expect(
                screen.getByText(/Not generated — no historical episode matched/),
            ).toBeInTheDocument();
        });
    });
});
