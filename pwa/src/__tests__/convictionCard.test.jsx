import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ConvictionCard, summarizeDecision, tickerFromNode } from '../canvas/panels/DetailPanel.jsx';

vi.mock('../api.js', () => ({
    api: { getConvictionDecision: vi.fn() },
}));

const DECISION = {
    ticker: 'NVDA',
    horizon_days: 90,
    unified_verdict: 'low',
    verdict_reasons: ['only 40% of signal weight has a calibrated scorecard (floor 50%)'],
    provenance_report: {
        verdict: 'low',
        verdict_reason: 'only 40% of signal weight has a calibrated scorecard (floor 50%)',
        confidence: 0.71,
        aggregate_conviction: 1.0,
        direction: 'bullish',
        layers_present: 11,
        layers_total: 16,
        evidence_coverage: 0.4,
        signal_evidence: [
            { signal_source: 'flow_momentum', shapley_weight: 0.6, classification: 'no_history' },
            { signal_source: 'regime_contrarian', shapley_weight: 0.4, classification: 'strong' },
        ],
    },
    trade_ticket: { kelly_size_pct: 0.02 },
    stage_errors: { red_team: 'llm unavailable' },
};

describe('tickerFromNode', () => {
    it('prefers data.ticker and strips the canvas prefix', () => {
        expect(tickerFromNode({ id: 't:nvda', data: {} })).toBe('NVDA');
        expect(tickerFromNode({ id: 'x', data: { ticker: 'amd' } })).toBe('AMD');
        expect(tickerFromNode(null)).toBe('');
    });
});

describe('summarizeDecision', () => {
    it('surfaces the verdict, its reason, coverage and the top evidence', () => {
        const s = summarizeDecision(DECISION);
        expect(s.verdict).toBe('low');
        expect(s.reason).toMatch(/calibrated scorecard/);
        expect(s.layersPresent).toBe(11);
        expect(s.layersTotal).toBe(16);
        expect(s.evidenceCoverage).toBeCloseTo(0.4);
        expect(s.evidence[0].signal_source).toBe('flow_momentum');
        expect(s.kellyPct).toBe(0.02);
        expect(s.stageErrors).toEqual(['red_team']);
        expect(summarizeDecision(null)).toBeNull();
    });
});

describe('ConvictionCard', () => {
    it('runs the stack on demand at the chosen horizon and renders the verdict and why', async () => {
        const fetchDecision = vi.fn().mockResolvedValue(DECISION);
        render(<ConvictionCard ticker="NVDA" fetchDecision={fetchDecision} />);

        expect(screen.getByText(/Runs the full decision stack for NVDA/)).toBeTruthy();
        fireEvent.click(screen.getByText('90d'));
        fireEvent.click(screen.getByText('Run stack'));

        await waitFor(() => expect(screen.getByTestId('conviction-result')).toBeTruthy());
        expect(fetchDecision).toHaveBeenCalledWith('NVDA', { horizonDays: 90 });
        expect(screen.getByText('LOW')).toBeTruthy();
        expect(screen.getByText(/only 40% of signal weight/)).toBeTruthy();
        expect(screen.getByText('11/16')).toBeTruthy();
        // Coverage row plus the flow_momentum evidence weight both read 40%.
        expect(screen.getAllByText('40%').length).toBeGreaterThanOrEqual(1);
        expect(screen.getByText('2.0%')).toBeTruthy();
        expect(screen.getByText(/Partial: red_team unavailable/)).toBeTruthy();
    });

    it('shows the error when the stack fails', async () => {
        const fetchDecision = vi.fn().mockRejectedValue(new Error('stack down'));
        render(<ConvictionCard ticker="NVDA" fetchDecision={fetchDecision} />);
        fireEvent.click(screen.getByText('Run stack'));
        await waitFor(() => expect(screen.getByText('stack down')).toBeTruthy());
    });
});
