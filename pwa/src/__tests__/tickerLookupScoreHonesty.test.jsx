import React from 'react';
import { cleanup, render, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

// B-M18 / B-L7 (stepdad.finance dad path): a 0-100 conviction gauge may only render
// alongside the `weights` object that explains it.
if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

const getDadTickerGold = vi.fn();
const streamDadTickerGold = vi.fn(() => ({ close: vi.fn() }));

vi.mock('../api.js', () => ({
    api: {
        get getDadTickerGold() {
            return getDadTickerGold;
        },
        get streamDadTickerGold() {
            return streamDadTickerGold;
        },
        getDadTickerChart: vi.fn().mockResolvedValue({}),
        getDadTickerEvidence: vi.fn().mockResolvedValue({}),
        getDadTickerFinviz: vi.fn().mockResolvedValue({}),
        getDadTickerOptions: vi.fn().mockResolvedValue({}),
    },
}));

vi.mock('../components/PriceChart.jsx', () => ({
    default: () => <div data-testid="price-chart" />,
}));

const WEIGHTS = {
    workbook_prior_multiplier: 0.35,
    grid_return_1y_strong_ge_20pct: 15,
    finviz_stale: -5,
    _clamp: { min: 0, max: 100 },
    _stance_thresholds: { 'Deep review first': 70 },
};

const SCORED = {
    ticker: 'RXT',
    summary: { mentions: 10, file_count: 3, sheet_count: 4, evidence_score: 8 },
    gold: {
        verdict: 'High workbook conviction',
        heuristic_score: 84,
        weights: { file_count: { weight: 8, cap: null, input: 3, points: 24 } },
        score_basis: 'workbook_footprint_weighted_count',
        tone: 'strong',
        one_liner: 'Workbooks mention this ticker repeatedly.',
    },
    decision_stack: {
        stance: 'Deep review first',
        stance_basis: 'heuristic_score_thresholds',
        tone: 'strong',
        heuristic_score: 72.5,
        weights: WEIGHTS,
        score_basis: 'hand_picked_point_awards',
        cards: [{ source: 'Dad workbooks', state: 'strong', points: 29.4, detail: 'Workbook prior.' }],
        reasons: ['GRID 1Y trend is strong.'],
        blockers: [],
        method: 'Hand-picked point awards.',
    },
};

const UNSCORED = {
    ticker: 'ZZZZ',
    summary: {},
    gold: {
        verdict: 'No workbook history yet',
        heuristic_score: null,
        weights: null,
        score_basis: 'no_workbook_history',
        tone: 'neutral',
        one_liner: 'Not in the workbook corpus yet.',
    },
    decision_stack: {
        stance: 'Do not surface hard yet',
        stance_basis: 'heuristic_score_thresholds',
        tone: 'light',
        heuristic_score: 4.0,
        weights: null,
        score_basis: 'hand_picked_point_awards',
        cards: [],
        reasons: [],
        blockers: ['GRID has no resolved price history for this ticker yet.'],
        method: 'Hand-picked point awards.',
    },
};

describe('TickerLookup conviction gauges', () => {
    afterEach(() => {
        cleanup();
        getDadTickerGold.mockReset();
        streamDadTickerGold.mockClear();
    });

    it('renders the numeric gauges when the weights recipe is present', async () => {
        getDadTickerGold.mockResolvedValue(SCORED);
        const { default: TickerLookup } = await import('../views/TickerLookup.jsx');
        const { container } = render(<TickerLookup />);

        await waitFor(() => expect(container.textContent).toContain('Deep review first'));
        const text = container.textContent;
        expect(text).toContain('72.5');
        expect(text).toContain('84');
        expect(text).toMatch(/Heuristic score from hand-picked weights/);
        expect(container.querySelectorAll('.tl-score-track').length).toBeGreaterThan(0);
    });

    it('renders no numeric gauge when the payload carries no weights', async () => {
        getDadTickerGold.mockResolvedValue(UNSCORED);
        const { default: TickerLookup } = await import('../views/TickerLookup.jsx');
        const { container } = render(<TickerLookup />);

        await waitFor(() => expect(container.textContent).toContain('Do not surface hard yet'));
        const text = container.textContent;
        // Neither the decision-stack score nor the gold score may show a bar or a number.
        expect(text).not.toContain('4.0');
        expect(container.querySelectorAll('.tl-score-track').length).toBe(0);
        expect(text).toMatch(/No heuristic score/);
        expect(text).toMatch(/No workbook rows, so no score/);
        // A missing score must never be rendered as a zero.
        expect(container.querySelectorAll('.tl-no-score').length).toBe(2);
    });
});
