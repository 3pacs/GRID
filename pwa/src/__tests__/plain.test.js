/**
 * Tests for plain-English helpers used by the stepdad.finance home page.
 */
import { describe, it, expect } from 'vitest';
import { tickerName, plainSentiment, plainRegime, warmError } from '../components/home/plain.js';

describe('tickerName', () => {
    it('returns the friendly company name for a known ticker', () => {
        expect(tickerName('AAPL')).toBe('Apple');
        expect(tickerName('tsla')).toBe('Tesla');
    });

    it('falls back to the base symbol for a known share-class suffix', () => {
        expect(tickerName('BRK-B')).toBe('Berkshire');
    });

    it('falls back to the raw uppercased symbol when unknown', () => {
        expect(tickerName('ZZZZ')).toBe('ZZZZ');
    });

    it('handles empty/nullish input without throwing', () => {
        expect(tickerName('')).toBe('');
        expect(tickerName(undefined)).toBe('');
        expect(tickerName(null)).toBe('');
    });
});

describe('plainSentiment', () => {
    it('maps bullish/positive words to "Looking up" with an up tone', () => {
        expect(plainSentiment('bullish')).toEqual({ label: 'Looking up', tone: 'up' });
        expect(plainSentiment('risk-on')).toEqual({ label: 'Looking up', tone: 'up' });
        expect(plainSentiment('inflow')).toEqual({ label: 'Looking up', tone: 'up' });
    });

    it('maps bearish/negative words to "Looking shaky" with a down tone', () => {
        expect(plainSentiment('bearish')).toEqual({ label: 'Looking shaky', tone: 'down' });
        expect(plainSentiment('risk_off')).toEqual({ label: 'Looking shaky', tone: 'down' });
        expect(plainSentiment('outflow')).toEqual({ label: 'Looking shaky', tone: 'down' });
    });

    it('falls back to calm/mixed for unknown or missing input', () => {
        expect(plainSentiment('neutral')).toEqual({ label: 'Calm / mixed', tone: 'flat' });
        expect(plainSentiment(undefined)).toEqual({ label: 'Calm / mixed', tone: 'flat' });
        expect(plainSentiment('')).toEqual({ label: 'Calm / mixed', tone: 'flat' });
    });
});

describe('plainRegime', () => {
    it('describes risk-off/stress regimes as nervous with a down tone', () => {
        expect(plainRegime('risk_off')).toEqual({
            sentence: 'Investors are nervous and playing it safe.',
            tone: 'down',
        });
        expect(plainRegime('DEFENSIVE')).toMatchObject({ tone: 'down' });
    });

    it('describes risk-on/growth regimes as confident with an up tone', () => {
        expect(plainRegime('risk_on')).toEqual({
            sentence: 'Investors are feeling confident.',
            tone: 'up',
        });
        expect(plainRegime('expansion')).toMatchObject({ tone: 'up' });
    });

    it('describes neutral/mixed regimes with a flat tone', () => {
        expect(plainRegime('neutral')).toEqual({
            sentence: 'The market is calm and mixed — no strong direction.',
            tone: 'flat',
        });
    });

    it('cleans and echoes an unrecognized regime label with a flat tone', () => {
        expect(plainRegime('some_weird_state')).toEqual({
            sentence: 'The market read is "some weird state".',
            tone: 'flat',
        });
    });
});

describe('warmError', () => {
    it('returns the price-specific message for kind "price"', () => {
        expect(warmError('price')).toMatch(/isn.t updating right now/);
    });

    it('returns the verdict-specific message for kind "verdict"', () => {
        expect(warmError('verdict')).toMatch(/couldn.t get an answer/);
    });

    it('returns a generic calm message for an unknown or missing kind', () => {
        expect(warmError()).toBe('Something went wrong — please try again.');
        expect(warmError('something-else')).toBe('Something went wrong — please try again.');
    });
});
