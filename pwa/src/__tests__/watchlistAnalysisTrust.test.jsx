import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { TrustBar, InsiderEdgePanel } from '../views/WatchlistAnalysis.jsx';

/**
 * Audit C-M22 — the trust bar on GET /api/v1/watchlist/{ticker}/edge.
 *
 * `signal_sources.trust_score` is NULL until intelligence/trust_scorer.py
 * scores the source, and `convergence.confidence` is now null when no source in
 * the convergence is scored. The old bar did `score || 0.5` upstream and
 * `Math.max(0, Math.min(1, score || 0))` here, so:
 *
 *   - an unscored source drew a half-filled bar nobody measured, and
 *   - a *measured* 0.0 was indistinguishable from "no measurement".
 *
 * And `convergence.direction` was permanently "neutral" because the router read
 * a key `detect_convergence` never emitted.
 *
 * Contract (docs/reference/CONFIDENCE_POLICY.md): null renders as "unscored"
 * with no fill; 0 renders as a real 0% bar; a null direction is never labelled
 * "neutral".
 */

// jsdom implements neither of these; d3 (imported by the view module) touches
// SVGElement.prototype.transform, and child charts observe their container.
if (typeof globalThis.ResizeObserver === 'undefined') {
    globalThis.ResizeObserver = class {
        observe() {}
        unobserve() {}
        disconnect() {}
    };
}
if (typeof SVGElement !== 'undefined' && !('transform' in SVGElement.prototype)) {
    Object.defineProperty(SVGElement.prototype, 'transform', {
        configurable: true,
        get() { return { baseVal: { consolidate: () => null, numberOfItems: 0 } }; },
    });
}
if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({
        matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn(),
    });
}

const fill = (container) => container.querySelector('[data-testid="trustbar-fill"]');

const edge = (convergence, overrides = {}) => ({
    congressional: [{
        member: 'Rep. A', action: 'BUY', amount: 'N/A', date: '2026-09-15',
        committee: 'N/A', trust_score: null,
    }],
    insider: [],
    dark_pool: null,
    whale_flow: [],
    prediction_markets: [],
    smart_money: [],
    lever_pullers: [],
    leads: [],
    convergence,
    edge_summary: '',
    ...overrides,
});

describe('TrustBar', () => {
    it('renders "unscored" and draws no fill for a null score', () => {
        const { container } = render(<TrustBar score={null} />);
        expect(screen.getByText('unscored')).toBeInTheDocument();
        expect(fill(container)).toBeNull();
    });

    it('renders "unscored" for an undefined score', () => {
        const { container } = render(<TrustBar score={undefined} />);
        expect(screen.getByText('unscored')).toBeInTheDocument();
        expect(fill(container)).toBeNull();
    });

    it('renders a measured 0 as a real 0% bar, not as unscored', () => {
        const { container } = render(<TrustBar score={0} />);
        expect(screen.getByText('0')).toBeInTheDocument();
        expect(screen.queryByText('unscored')).toBeNull();
        expect(fill(container)).not.toBeNull();
        expect(fill(container).style.width).toBe('0%');
    });

    it('renders a scored value as a filled bar', () => {
        const { container } = render(<TrustBar score={0.82} />);
        expect(screen.getByText('82')).toBeInTheDocument();
        expect(fill(container).style.width).toBe('82%');
    });

    it('never invents a half bar', () => {
        const { container } = render(<TrustBar score={null} width={120} />);
        expect(fill(container)).toBeNull();
        expect(screen.queryByText('50')).toBeNull();
    });
});

describe('InsiderEdgePanel convergence rendering', () => {
    it('shows "unscored" instead of a half bar when confidence is null', () => {
        const { container } = render(<InsiderEdgePanel loading={false} edgeData={edge({
            direction: 'bullish',
            direction_basis: 'inferred_from_signal_types',
            source_count: 3,
            scored_source_count: 0,
            confidence: null,
            confidence_basis: 'unscored',
            status: 'detected',
        })} />);

        expect(screen.getAllByText('unscored').length).toBeGreaterThan(0);
        expect(container.textContent).not.toMatch(/·\s*50%/);
        // The 120px convergence bar and the per-row bar both stay empty.
        expect(fill(container)).toBeNull();
    });

    it('renders a measured 0.0 convergence confidence as 0%, not unscored', () => {
        const { container } = render(<InsiderEdgePanel loading={false} edgeData={edge({
            direction: 'bullish',
            source_count: 3,
            scored_source_count: 3,
            confidence: 0,
            confidence_basis: 'mean_trust_of_scored_sources',
            status: 'detected',
        })} />);

        expect(container.textContent).toContain('0%');
        const fills = container.querySelectorAll('[data-testid="trustbar-fill"]');
        expect(fills.length).toBeGreaterThan(0);
        expect(fills[fills.length - 1].style.width).toBe('0%');
    });

    it('never labels a null direction "neutral"', () => {
        const { container } = render(<InsiderEdgePanel loading={false} edgeData={edge({
            direction: null,
            direction_basis: 'sources_disagree',
            source_count: 3,
            scored_source_count: 0,
            confidence: null,
            confidence_basis: 'unscored',
            status: 'detected',
        })} />);

        expect(container.textContent.toLowerCase()).not.toContain('neutral');
        expect(container.textContent).toContain('direction unresolved');
    });

    it('renders a scored convergence normally', () => {
        const { container } = render(<InsiderEdgePanel loading={false} edgeData={edge({
            direction: 'bearish',
            source_count: 4,
            scored_source_count: 4,
            confidence: 0.82,
            confidence_basis: 'mean_trust_of_scored_sources',
            status: 'detected',
        })} />);

        expect(container.textContent).toContain('bearish');
        expect(container.textContent).toContain('82%');
    });

    it('draws no convergence banner for the "none" object', () => {
        const { container } = render(<InsiderEdgePanel loading={false} edgeData={edge({
            direction: null, source_count: 0, confidence: null, status: 'none',
        })} />);

        expect(container.textContent.toLowerCase()).not.toContain('neutral');
        expect(container.textContent).not.toContain('independent source');
    });

    it('draws no trust bar at all for an unscored signal row', () => {
        // SignalCard omits the bar when trustScore is null — the same rule the
        // whale/dark-pool cards already rely on, where no trust exists to show.
        // What must never happen is a half-filled bar standing in for it.
        const { container } = render(<InsiderEdgePanel loading={false} edgeData={edge({
            direction: null, source_count: 0, confidence: null, status: 'none',
        })} />);
        expect(fill(container)).toBeNull();
        expect(container.textContent).not.toContain('50');
    });

    it('renders a measured 0.0 per-row trust score as 0', () => {
        render(<InsiderEdgePanel loading={false} edgeData={edge(
            { direction: null, source_count: 0, confidence: null, status: 'none' },
            {
                congressional: [{
                    member: 'Rep. A', action: 'BUY', amount: 'N/A',
                    date: '2026-09-15', committee: 'N/A', trust_score: 0,
                }],
            },
        )} />);
        expect(screen.getByText('0')).toBeInTheDocument();
        expect(screen.queryByText('unscored')).toBeNull();
    });
});
