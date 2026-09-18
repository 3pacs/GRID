import React from 'react';
import { cleanup, render, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

// A-H14: `expected_edge_pct` was a percentage return with no backtest. The view must
// render a dimensionless heuristic rank with its basis, never an "% edge".
if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

const getMarketEdges = vi.fn();

vi.mock('../api.js', () => ({
    api: {
        get getMarketEdges() {
            return getMarketEdges;
        },
    },
}));

const OPPORTUNITY = {
    id: 'defense-procurement-stack',
    title: 'Defense procurement is clustering in the contractor stack',
    category: 'Defense',
    setup_type: 'procurement-compound',
    data_mode: 'live',
    bias: 'long',
    heuristic_rank: 19,
    basis: 'playbook_prior',
    score: 81,
    heuristic_confidence: 81,
    heuristic_confidence_label: 'high',
    heuristic_confidence_inputs: [
        { component: 'playbook_prior', points: 74, detail: 'fixed prior for playbook defense-procurement-stack' },
        { component: 'unique_source_types', points: 9, detail: '3 distinct supporting source type(s)' },
        { component: 'quality_penalty', points: -2, detail: 'deductions for late confirmation rows' },
    ],
    confidence: null,
    confidence_basis: 'no_scored_track_record',
    status: 'active',
    horizon: '1-3 months',
    sector_focus: 'Defense primes',
    thesis: 'Thesis text.',
    summary: 'Summary text.',
    why_now: 'Why now text.',
    mispricing_test: 'Mispricing test.',
    clues: [],
    proof_needed: 'More awards.',
    entry_rule: 'Entry rule.',
    exit_rule: 'Exit rule.',
    kill_switch: 'Kill switch.',
    data_hooks: [],
    targets: ['GD', 'LDOS'],
    evidence: ['GD award tape'],
    source_tags: ['Gov Contracts'],
    supporting_source_types: ['Gov Contracts'],
    decision_window: { status: 'fresh', label: 'Fresh', detail: 'x' },
    driver_stack: [],
    confirmation_board: [],
    stakes: { breadth_count: 2, source_family_count: 1, capital_signal: '$50m' },
    lagging_factors: [],
    upgrade_trigger: 'Upgrade trigger.',
    quality_label: 'tight',
    route_hint: 'influence',
};

const PAYLOAD = {
    as_of: '2026-09-17',
    generated_at: '2026-09-17T12:00:00+00:00',
    opportunities: [OPPORTUNITY],
    coverage_gaps: [],
    summary: {
        count: 1,
        active_count: 1,
        arming_count: 0,
        watch_count: 0,
        background_count: 0,
        live_count: 1,
        high_heuristic_count: 1,
        avg_heuristic_rank: 19,
        heuristic_basis: 'playbook_prior',
        evidence_count: 1,
        coverage_gap_count: 0,
        top_setup: 'defense-procurement-stack',
        public_data_only: true,
    },
};

describe('EdgeScanner honesty', () => {
    afterEach(() => {
        cleanup();
        getMarketEdges.mockReset();
    });

    it('renders a heuristic rank with its basis and never a "% edge"', async () => {
        getMarketEdges.mockResolvedValue(PAYLOAD);
        const { default: EdgeScanner } = await import('../views/EdgeScanner.jsx');
        const { container, findAllByText } = render(<EdgeScanner onNavigate={() => {}} />);

        await waitFor(() => expect(getMarketEdges).toHaveBeenCalled());
        await findAllByText(/Defense procurement is clustering/);

        const text = container.textContent;
        expect(text).toContain('HEUR RANK');
        expect(text).toContain('PLAYBOOK PRIOR');
        expect(text).toContain('19');
        // The percentage-return framing is gone in every place it used to appear.
        expect(text).not.toMatch(/EDGE\s*19%/);
        expect(text).not.toMatch(/\bCONF\s*\d+%/);
        expect(text).not.toContain('Avg Edge');
    });

    it('publishes the point awards behind the heuristic score', async () => {
        getMarketEdges.mockResolvedValue(PAYLOAD);
        const { default: EdgeScanner } = await import('../views/EdgeScanner.jsx');
        const { container, findAllByText } = render(<EdgeScanner onNavigate={() => {}} />);

        await findAllByText(/How The Heuristic Score Was Built/);
        const text = container.textContent;
        expect(text).toContain('playbook prior');
        expect(text).toContain('+74');
        expect(text).toContain('-2');
        expect(text).toMatch(/Not a backtest/);
    });

    it('shows a dash rather than a number when nothing is ranked', async () => {
        getMarketEdges.mockResolvedValue({
            ...PAYLOAD,
            opportunities: [{ ...OPPORTUNITY, heuristic_rank: null, basis: null }],
            summary: { ...PAYLOAD.summary, avg_heuristic_rank: null, count: 0 },
        });
        const { default: EdgeScanner } = await import('../views/EdgeScanner.jsx');
        const { container, findAllByText } = render(<EdgeScanner onNavigate={() => {}} />);

        await findAllByText(/Defense procurement is clustering/);
        const text = container.textContent;
        expect(text).toContain('UNRANKED');
        // A missing rank must never render as 0.
        expect(text).not.toMatch(/HEUR RANK\s*0\b/);
    });
});
