import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

// oracle_predictions.entry_price (D-M32) and .confidence (D-H11) are
// nullable: they hold the measured value, or NULL when nothing measured them.
//
// The card used to read `${pred.entry_price?.toFixed(2) || '---'}` and
// `Math.round((pred.confidence || 0) * 100)`. Neither fired for a genuine 0
// (not nullish / not distinguishable from missing), and the confidence one
// printed "0%" for a prediction that stated no confidence at all — a
// no-confidence call the model never made.
//
// The contract the card renders now:
//
//   confidence  null -> "unscored"   (the same word ConfidenceMeter uses)
//   confidence  0    -> "0%"         (a measurement, rendered as one)
//   entry_price null -> "---"        (no price to show at all)
//   entry_price 0    -> "$0.00"      (a measurement, rendered as one)
//
// Every guard is `== null`, never falsiness.

if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

vi.mock('../api.js', () => ({ api: {} }));

const { PredictionCard } = await import('../views/Predictions.jsx');

const base = {
    id: 'p1',
    ticker: 'AAPL',
    direction: 'CALL',
    model_name: 'astrogrid',
    signals: [],
    anti_signals: [],
    expiry: '2026-09-24',
    days_left: 7,
    target_price: null,
    entry_price: null,
    confidence: null,
};

afterEach(cleanup);

describe('PredictionCard entry price', () => {
    it('renders --- and no dollar amount when entry_price is null', () => {
        const { container } = render(<PredictionCard pred={{ ...base }} />);
        expect(container.textContent).not.toContain('$0.00');
        const entry = [...container.querySelectorAll('div')]
            .find(d => d.textContent.trim() === '---');
        expect(entry).toBeTruthy();
    });

    it('renders the measured price when one was observed', () => {
        render(<PredictionCard pred={{ ...base, entry_price: 214.5 }} />);
        expect(screen.getByText('$214.50')).toBeTruthy();
    });

    it('renders a genuine measured 0 as $0.00, not as a gap', () => {
        // The guard is `== null`, not falsiness: a real measured zero must not
        // be laundered into "unknown".
        const { container } = render(<PredictionCard pred={{ ...base, entry_price: 0 }} />);
        expect(container.textContent).toContain('$0.00');
    });
});

describe('PredictionCard confidence', () => {
    it('renders the word unscored when no confidence was stated', () => {
        const { container } = render(<PredictionCard pred={{ ...base }} />);
        // Not 0%, and not "---" either: "---" is what a missing *entry price*
        // reads as, and the two gaps must stay tellable apart on the card.
        expect(container.textContent).toContain('unscored');
        expect(container.textContent).not.toContain('0%');
    });

    it('renders a genuine measured 0 as 0%, not as unscored', () => {
        const { container } = render(<PredictionCard pred={{ ...base, confidence: 0 }} />);
        expect(container.textContent).toContain('0%');
        expect(container.textContent).not.toContain('unscored');
    });

    it('does not read unscored once a confidence was stated', () => {
        const { container } = render(<PredictionCard pred={{ ...base, confidence: 0.82 }} />);
        expect(container.textContent).not.toContain('unscored');
    });

    it('keeps the two gaps distinct on one card', () => {
        // A prediction that measured neither: entry reads "---", confidence
        // reads "unscored". Neither borrows the other's vocabulary.
        const { container } = render(
            <PredictionCard pred={{ ...base, entry_price: null, confidence: null }} />,
        );
        expect(container.textContent).toContain('unscored');
        const entry = [...container.querySelectorAll('div')]
            .find(d => d.textContent.trim() === '---');
        expect(entry).toBeTruthy();
    });

    it('renders a stated confidence as a percentage', () => {
        const { container } = render(<PredictionCard pred={{ ...base, confidence: 0.82 }} />);
        expect(container.textContent).toContain('82%');
    });

    it('never substitutes 0.5 / 50% for a missing confidence', () => {
        const { container } = render(<PredictionCard pred={{ ...base }} />);
        expect(container.textContent).not.toContain('50%');
    });
});
