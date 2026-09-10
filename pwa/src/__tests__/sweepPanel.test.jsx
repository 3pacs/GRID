import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import SweepPanel, { formatSweepMeta } from '../canvas/panels/SweepPanel.jsx';

const SWEEP = {
    generated_at: '2026-09-06T05:00:00+00:00',
    universe_name: 'custom',
    horizon_days: 90,
    tickers_attempted: 33,
    tickers_succeeded: 31,
    regime_signature: 'trending',
    narrative: 'Two names actionable at 90 days.',
    top_k: [
        { ticker: 'NVDA', sector: 'tech', verdict: 'high', composite_score: 1.31 },
        { ticker: 'AMD', sector: 'tech', verdict: 'moderate', composite_score: 0.8 },
        { ticker: 'XYZ', verdict: 'high', error: 'stack raised' },
    ],
};

describe('formatSweepMeta', () => {
    it('summarises horizon, universe, regime, coverage and date', () => {
        const m = formatSweepMeta(SWEEP);
        expect(m.title).toBe('90d · custom');
        expect(m.subtitle).toBe('trending · 31/33 scored');
        expect(m.generated).toBe('2026-09-06');
        expect(formatSweepMeta(null).title).toBe('No sweep yet');
    });
});

describe('SweepPanel', () => {
    it('lists ranked rows with verdict chips and picks a ticker on click', () => {
        const onPick = vi.fn();
        render(<SweepPanel sweep={SWEEP} onPick={onPick} activeTicker="amd" />);
        expect(screen.getByText('90d · custom')).toBeTruthy();
        expect(screen.getByText('NVDA')).toBeTruthy();
        expect(screen.getAllByText('high').length).toBe(1); // XYZ errored → 'error'
        expect(screen.getByText('error')).toBeTruthy();
        expect(screen.getByText('1.31')).toBeTruthy();
        expect(screen.getByText(/Two names actionable/)).toBeTruthy();
        fireEvent.click(screen.getByTestId('sweep-row-NVDA'));
        expect(onPick).toHaveBeenCalledWith('NVDA');
    });

    it('explains the empty state and closes', () => {
        const onClose = vi.fn();
        render(<SweepPanel sweep={null} onClose={onClose} />);
        expect(screen.getByText(/No persisted sweep at this horizon yet/)).toBeTruthy();
        fireEvent.click(screen.getByLabelText('Close sweep panel'));
        expect(onClose).toHaveBeenCalled();
    });
});
