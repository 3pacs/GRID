import React from 'react';
import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Surfacer from '../views/Surfacer.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

describe('Surfacer operator brief', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('turns the alpha queue into an explicit sizing decision', async () => {
        api.get.mockResolvedValue({
            generated_at: '2026-04-18T18:00:00Z',
            brief: {
                posture: 'watch',
                stance: 'No size yet',
                headline: 'Watch AMD until the weak gates clear',
                primary_action: 'Resolve the open gates before turning this into a ticket.',
                selected_candidate_id: 'oracle-2',
                selected_score: 69,
                next_actions: [
                    'Do not size AMD until weak or missing gates clear.',
                    'Close missing gates: track record.',
                ],
                blockers: ['Missing: track record (1)', 'Weak: execution (1)'],
                label_counts: { watch: 1, play: 0, research: 0 },
            },
            candidates: [
                {
                    id: 'oracle-2',
                    title: 'AMD Bearish setup',
                    summary: 'AMD has a bearish oracle read.',
                    why_now: 'Fresh model prediction with supporting signal stack.',
                    alpha_score: 72,
                    score_parts: { signal: 70, freshness: 80, confidence: 69, backtest: 55 },
                    confidence: 0.69,
                    expected_move_pct: 8,
                    direction: 'bearish',
                    horizon: 'multi_week',
                    tickers: ['AMD'],
                    trade_expression: 'Short or put bias in AMD; invalidate on reversal strength',
                    status: 'watch',
                    freshness: { label: 'fresh', age_hours: 2 },
                    evidence: [{ source: 'oracle', label: 'Oracle', detail: 'Fresh bearish evidence.' }],
                    contradictions: [],
                    invalidation: 'Kill on reversal strength.',
                    source_modules: ['oracle', 'signal_data'],
                    conviction: {
                        label: 'watch',
                        action: 'Watch',
                        score: 69,
                        summary: 'Promising, but one or more gates need confirmation before sizing.',
                        missing: ['track record'],
                        gates: [
                            { name: 'track record', status: 'missing', score: 0, weight: 18, detail: 'No scored analogs yet.' },
                            { name: 'execution', status: 'weak', score: 5, weight: 10, detail: 'Liquidity not confirmed.' },
                        ],
                    },
                },
            ],
            meta: {
                count: 1,
                actionable_count: 0,
                average_conviction: 69,
                sources: { oracle: 1 },
            },
        });

        render(<Surfacer />);

        expect(await screen.findByText('No size yet')).toBeInTheDocument();
        expect(screen.getByText('Watch AMD until the weak gates clear')).toBeInTheDocument();
        expect(screen.getByText('Do not size AMD until weak or missing gates clear.')).toBeInTheDocument();
        expect(screen.getByText(/Missing: track record/)).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Inspect top setup' })).toBeInTheDocument();
    });
});
