import fs from 'node:fs';
import path from 'node:path';
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeAll, beforeEach } from 'vitest';
import RiskMap from '../views/RiskMap.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getRiskMap: vi.fn(),
    },
}));

// jsdom has no SVGElement.transform; d3-transition's transform tween calls
// node.transform.baseVal.consolidate() and throws without it. Returning null
// makes d3 treat the current transform as identity, which is all the gauge
// and treemap animations need to run headlessly.
beforeAll(() => {
    if (typeof globalThis.ResizeObserver === 'undefined') {
        // jsdom has no ResizeObserver; the treemap only uses it to re-measure.
        globalThis.ResizeObserver = class {
            observe() {}
            unobserve() {}
            disconnect() {}
        };
    }
    if (typeof SVGElement !== 'undefined' && !('transform' in SVGElement.prototype)) {
        Object.defineProperty(SVGElement.prototype, 'transform', {
            configurable: true,
            get() {
                return { baseVal: { consolidate: () => null } };
            },
        });
    }
});

const MEASURED = {
    dealer_risk: { gex_regime: 'long_gamma', net_gex: 1200, gamma_flip_distance_pct: 2.1, days_to_opex: 12, risk_level: 'low', available: true },
    volatility_risk: { vix: 18.2, vix_percentile_1y: 40, vix_term_structure: 'contango', realized_vs_implied: null, risk_level: 'moderate', available: true },
    concentration_risk: { risk_level: 'unknown', available: false, reason: 'no active watchlist positions' },
    correlation_risk: { risk_level: 'unknown', available: false, reason: 'insufficient 90d close history' },
    credit_risk: { hy_spread: 350, ig_spread: null, ted_spread: null, spread_direction: 'stable', risk_level: 'moderate', available: true },
    liquidity_risk: { risk_level: 'unknown', available: false, reason: 'no Fed balance sheet series' },
    overall_risk_score: 0.31,
    available_subsystems: 3,
    unavailable_subsystems: ['Concentration', 'Correlation', 'Liquidity'],
    risk_narrative: 'All measured risk categories are within normal ranges.',
    generated_at: '2026-09-17T12:00:00Z',
    errors: [],
};

describe('RiskMap truthfulness', () => {
    beforeEach(() => {
        api.getRiskMap.mockReset();
    });

    it('shows the current snapshot only and states that no 30-day history exists', async () => {
        api.getRiskMap.mockResolvedValue(MEASURED);

        render(<RiskMap onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText(/No 30-day risk history is stored yet/)).toBeInTheDocument();
        });
        expect(screen.queryByText(/RISK TIMELINE \(30D\)/)).not.toBeInTheDocument();
        expect(screen.queryByText(/Hover to inspect, click to select/)).not.toBeInTheDocument();
    });

    it('renders unavailable sub-systems as UNAVAILABLE, never as a default moderate reading', async () => {
        api.getRiskMap.mockResolvedValue(MEASURED);

        render(<RiskMap onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getAllByText('UNAVAILABLE').length).toBeGreaterThanOrEqual(3);
        });
        // Measured levels still render from the payload.
        expect(screen.getAllByText('LOW').length).toBeGreaterThanOrEqual(1);
        expect(screen.getAllByText('MODERATE').length).toBeGreaterThanOrEqual(1);
    });

    it('renders a null overall score as UNAVAILABLE on the gauge, never as 0 or 50', async () => {
        const allUnavailable = {
            ...MEASURED,
            dealer_risk: { risk_level: 'unknown', available: false, reason: 'no chain' },
            volatility_risk: { risk_level: 'unknown', available: false, reason: 'no VIX series' },
            credit_risk: { risk_level: 'unknown', available: false, reason: 'no HY series' },
            overall_risk_score: null,
            available_subsystems: 0,
            unavailable_subsystems: ['Dealer positioning', 'Volatility', 'Concentration', 'Correlation', 'Credit spreads', 'Liquidity'],
            risk_narrative: 'No risk sub-system could be measured; the overall score is unavailable.',
        };
        api.getRiskMap.mockResolvedValue(allUnavailable);

        render(<RiskMap onNavigate={vi.fn()} />);

        await waitFor(() => {
            // six category chips + the gauge label
            expect(screen.getAllByText('UNAVAILABLE').length).toBeGreaterThanOrEqual(7);
        });
        expect(screen.queryByText('LOW')).not.toBeInTheDocument();
        expect(screen.queryByText('MODERATE')).not.toBeInTheDocument();
        expect(screen.queryByText('50')).not.toBeInTheDocument();
        // Treemap subtitles must not print a default 0 for an unmeasured sub-system.
        expect(document.body.textContent).not.toMatch(/Top5: 0%/);
        expect(document.body.textContent).not.toMatch(/Fed: 0/);
        expect(document.body.textContent).toMatch(/no data/);
    });

    it('source contains no random walk and no synthetic timeline', () => {
        const src = fs.readFileSync(path.resolve(__dirname, '../views/RiskMap.jsx'), 'utf8');
        expect(src).not.toMatch(/Math\.random\(/);
        expect(src).not.toMatch(/Generate synthetic timeline/);
        expect(src).not.toMatch(/Generate 30 synthetic points/);
        expect(src).not.toMatch(/\|\| 'moderate'/);
    });
});
