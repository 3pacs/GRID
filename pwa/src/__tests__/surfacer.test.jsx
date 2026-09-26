import React from 'react';
import { render, screen } from '@testing-library/react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import Surfacer from '../views/Surfacer.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

beforeAll(() => {
    if (typeof globalThis.ResizeObserver === 'undefined') {
        // jsdom has no ResizeObserver; the layout only uses it to re-measure.
        globalThis.ResizeObserver = class {
            observe() {}
            unobserve() {}
            disconnect() {}
        };
    }
});

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
                decision_path: [
                    'Scanned 1 visible candidate.',
                    'No act-ready play exists; the best candidate is still a watch.',
                ],
                next_actions: [
                    'Do not size AMD until weak or missing gates clear.',
                    'Close gates: track record.',
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
                    score_parts: { signal: 70, freshness: 80, confidence: 69, prior_weight: 55, prior_weight_basis: 'static' },
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
        expect(screen.getByText('No act-ready play exists; the best candidate is still a watch.')).toBeInTheDocument();
        expect(screen.getByText('Do not size AMD until weak or missing gates clear.')).toBeInTheDocument();
        expect(screen.getByText(/Missing: track record/)).toBeInTheDocument();
        expect(screen.getByText('What can I do right now?')).toBeInTheDocument();
        expect(screen.getByText('Why')).toBeInTheDocument();
        expect(screen.getByText('Do Next')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Show me this setup' })).toBeInTheDocument();
    });
});

describe('Surfacer candidate honesty', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    const candidateWithUnknowns = {
        id: 'signal-9',
        title: 'NVDA Insider Cluster',
        summary: 'Cluster of Form 4 buys.',
        why_now: 'New insider_cluster event ranked by recency and magnitude.',
        alpha_score: 31,
        // A signal row with no stored confidence and no verdict history.
        score_parts: {
            signal: 62,
            freshness: 80,
            confidence: null,
            prior_weight: null,
            prior_weight_basis: 'unavailable',
            tradability: 65,
            tradability_basis: 'static',
            risk_penalty: 13,
        },
        confidence: null,
        direction: 'bullish',
        horizon: 'swing',
        tickers: ['NVDA'],
        trade_expression: 'Long bias in NVDA; enter only on confirmed follow-through',
        status: 'needs_research',
        freshness: { label: 'fresh', age_hours: 3 },
        evidence: [{ source: 'signal', label: 'Form 4', detail: 'three buys', weight: null }],
        contradictions: [],
        invalidation: 'Do not size until price confirms.',
        source_modules: ['signal_data'],
        conviction: {
            label: 'research',
            action: 'Research',
            score: 41,
            summary: 'Needs confirmation before sizing.',
            missing: ['track record'],
            gates: [
                { name: 'evidence', status: 'weak', score: 1, weight: 15, detail: '1 evidence item; confidence unknown.' },
            ],
        },
    };

    function mockPayload(candidate) {
        api.get.mockResolvedValue({
            generated_at: '2026-04-18T18:00:00Z',
            status: 'ok',
            error: null,
            brief: {
                posture: 'research',
                stance: 'Research only',
                headline: 'No front-page setup is ready',
                primary_action: 'Keep candidates in research.',
                selected_candidate_id: candidate.id,
                selected_score: 41,
                decision_path: ['Scanned 1 visible candidate.'],
                next_actions: ['Promote only after fresh confirmation.'],
                blockers: [],
                label_counts: { research: 1 },
            },
            candidates: [candidate],
            meta: { count: 1, sources: { signal_data: 1 } },
        });
    }

    it('says "confidence unknown" instead of inventing 0%', async () => {
        mockPayload(candidateWithUnknowns);

        render(<Surfacer />);

        expect(await screen.findByText('confidence unknown')).toBeInTheDocument();
        expect(screen.queryByText('0% confidence')).not.toBeInTheDocument();
        expect(screen.queryByText('30% confidence')).not.toBeInTheDocument();
    });

    it('still renders a measured confidence as a percentage', async () => {
        mockPayload({ ...candidateWithUnknowns, confidence: 0.69 });

        render(<Surfacer />);

        expect(await screen.findByText('69% confidence')).toBeInTheDocument();
        expect(screen.queryByText('confidence unknown')).not.toBeInTheDocument();
    });

    it('skips null score parts rather than drawing a zero-width bar', async () => {
        mockPayload(candidateWithUnknowns);

        render(<Surfacer />);

        // Measured parts still draw.
        expect(await screen.findByText('signal')).toBeInTheDocument();
        expect(screen.getByText('tradability')).toBeInTheDocument();
        // Null parts draw nothing, and are named as unavailable instead.
        expect(screen.queryByText('prior weight')).not.toBeInTheDocument();
        expect(screen.getByText(/Unavailable:/)).toBeInTheDocument();
        expect(screen.getByText(/prior weight/)).toBeInTheDocument();
        // Basis markers are metadata, not bars.
        expect(screen.queryByText('prior weight basis')).not.toBeInTheDocument();
    });

    it('reports a degraded backend as an outage, never as a stand-down call', async () => {
        api.get.mockResolvedValue({
            generated_at: '2026-04-18T18:00:00Z',
            status: 'degraded',
            error: 'Surfacer candidate query unavailable: connection refused',
            message: 'Surfacer could not read candidates. This is an outage, not a market call.',
            candidates: null,
            thesis: null,
            meta: { status: 'degraded', count: null },
            brief: null,
        });

        render(<Surfacer />);

        expect(await screen.findByText(/This is an outage, not a market call/)).toBeInTheDocument();
        expect(screen.queryByText('Stand down')).not.toBeInTheDocument();
        expect(screen.queryByText('Nothing cleared the front page')).not.toBeInTheDocument();
    });
});
