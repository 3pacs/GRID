import React from 'react';
import { render } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { finiteNumberOrNull } from '../utils/numbers.js';
import { TicketCard, ticketMoney, ticketKellyConfText } from '../views/canvas_lenses/CapitalLens.jsx';
import { ticketKellyText, ticketConfidenceTier } from '../views/SectorDive.jsx';

vi.mock('../api.js', () => ({ api: { getRecentTradeTickets: vi.fn(), getActorCapitalFlows: vi.fn() } }));

// #959: ticket confidence and kelly size are measured values or not reported. Number(x) || 0 turned a
// missing value into a 0 (Kelly 0.0%, conf 0%, $0.00 or $NaN) on the Capital lens.
describe('ticket fields are values or not reported', () => {
    it('finiteNumberOrNull shares one strict reading', () => {
        for (const v of [undefined, null, '', '  ', false, true, [], [0], {}, 'abc', '0x10', 'NaN', NaN, Infinity]) {
            expect(finiteNumberOrNull(v)).toBeNull();
        }
        expect(finiteNumberOrNull(0)).toBe(0);
        expect(finiteNumberOrNull('0.05')).toBe(0.05);
    });

    it('Capital lens money is n/a for anything that is not a number, never $0.00 or $NaN', () => {
        expect(ticketMoney(undefined)).toBe('n/a');
        expect(ticketMoney(null)).toBe('n/a');
        expect(ticketMoney('')).toBe('n/a');
        expect(ticketMoney(NaN)).toBe('n/a');
        expect(ticketMoney(0)).toBe('$0.00');          // a stated 0 is a value
        expect(ticketMoney(1.234)).toBe('$1.23');
    });

    it('Capital lens kelly and confidence say not reported, and flag a default-sized kelly', () => {
        expect(ticketKellyConfText(undefined, undefined)).toBe('Kelly not reported · conf not reported');
        expect(ticketKellyConfText(NaN, 'x')).toBe('Kelly not reported · conf not reported');
        expect(ticketKellyConfText(0, 0)).toBe('Kelly 0.0% · conf 0%');
        expect(ticketKellyConfText(0.05, 0.62)).toBe('Kelly 5.0% · conf 62%');
        // No measured accuracy: the API sends confidence null and a default-basis kelly.
        expect(ticketKellyConfText(0.05, null, 'default_no_history'))
            .toBe('Kelly 5.0% (default, no history) · conf not reported');
    });

    it('a rendered Capital lens ticket with missing fields shows no zero or NaN', () => {
        const { container } = render(<TicketCard ticket={{ ticker: 'acme', direction: 'short', instrument: 'put', expiry: '2026-12-18', thesis: '' }} />);
        const text = container.textContent;
        expect(text).toContain('Kelly not reported');
        expect(text).toContain('conf not reported');
        expect(text).not.toMatch(/Kelly 0\.0%|conf 0%|NaN|\$0\.00/);
        expect(text.match(/n\/a/g).length).toBe(4);
    });

    it('a rendered ticket with a no-history default says so', () => {
        const { container } = render(<TicketCard ticket={{
            ticker: 'acme', direction: 'short', instrument: 'put', expiry: '2026-12-18', thesis: '',
            entry_premium: 1.5, target_premium: 3, stop_premium: 0.75, invalidation_price: 99,
            kelly_size: 0.05, confidence: null, kelly_basis: 'default_no_history',
        }} />);
        expect(container.textContent).toContain('Kelly 5.0% (default, no history) · conf not reported');
        expect(container.textContent).toContain('$1.50');
    });

    it('Sector Dive feed marks a default-sized kelly and keeps confidence unknown', () => {
        expect(ticketKellyText(0.05, 'default_no_history')).toBe('kelly 5.0% (default, no history)');
        expect(ticketKellyText(0.05, 'backtest_accuracy')).toBe('kelly 5.0%');
        expect(ticketKellyText(0.05)).toBe('kelly 5.0%');
        expect(ticketKellyText(undefined, 'default_no_history')).toBe('kelly not reported');
        expect(ticketConfidenceTier(null)).toBe('unknown');
    });
});
