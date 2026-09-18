import React from 'react';
import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

// D-M32: oracle_predictions.entry_price is the measured spot or NULL. The card
// used to read `${pred.entry_price?.toFixed(2) || '---'}`, which never fired for
// the old fabricated 0.0 (not nullish) and printed "$0.00" — a price nobody
// looked up, rendered as if somebody had.

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
    confidence: null,
    signals: [],
    anti_signals: [],
    expiry: '2026-09-24',
    days_left: 7,
    target_price: null,
};

afterEach(cleanup);

describe('PredictionCard entry price', () => {
    it('renders --- when entry_price is null', () => {
        const { container } = render(<PredictionCard pred={{ ...base, entry_price: null }} />);
        const entry = [...container.querySelectorAll('div')]
            .find(d => d.textContent.trim() === '---');
        expect(entry).toBeTruthy();
        expect(container.textContent).not.toContain('$0.00');
    });

    it('renders --- when entry_price is undefined', () => {
        const { container } = render(<PredictionCard pred={{ ...base }} />);
        expect(container.textContent).not.toContain('$0.00');
        expect(container.textContent).toContain('---');
    });

    it('renders the measured price when one was observed', () => {
        render(<PredictionCard pred={{ ...base, entry_price: 214.5 }} />);
        expect(screen.getByText('$214.50')).toBeTruthy();
    });

    it('still renders a genuine 0 as a number, not as a gap', () => {
        // The guard is `== null`, not falsiness: a real measured zero (an
        // expired option, say) must not be laundered into "unknown".
        const { container } = render(<PredictionCard pred={{ ...base, entry_price: 0 }} />);
        expect(container.textContent).toContain('$0.00');
    });
});
