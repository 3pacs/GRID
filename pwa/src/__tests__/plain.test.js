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
            available: true,
            confidencePct: null,
        });
        expect(plainRegime('DEFENSIVE')).toMatchObject({ tone: 'down' });
    });

    it('describes risk-on/growth regimes as confident with an up tone', () => {
        expect(plainRegime('risk_on')).toEqual({
            sentence: 'Investors are feeling confident.',
            tone: 'up',
            available: true,
            confidencePct: null,
        });
        expect(plainRegime('expansion')).toMatchObject({ tone: 'up' });
    });

    it('describes neutral/mixed regimes with a flat tone', () => {
        expect(plainRegime('neutral')).toEqual({
            sentence: 'The market is calm and mixed — no strong direction.',
            tone: 'flat',
            available: true,
            confidencePct: null,
        });
    });

    it('cleans and echoes an unrecognized regime label with a flat tone', () => {
        expect(plainRegime('some_weird_state')).toEqual({
            sentence: 'The market read is "some weird state".',
            tone: 'flat',
            available: true,
            confidencePct: null,
        });
    });

    // Regression: api/routers/regime.py:188-199 returns state="UNCALIBRATED",
    // confidence=0.0 when no decision_journal row exists yet at all. That is
    // not a real regime reading — plainRegime must say so plainly, with no
    // numeric confidence, instead of cleaning it up into
    // `The market read is "UNCALIBRATED".` as if it were a real label.
    it('treats UNCALIBRATED as "no reading yet", not a real regime label', () => {
        const r = plainRegime('UNCALIBRATED', 0.0);
        expect(r.available).toBe(false);
        expect(r.confidencePct).toBeNull();
        expect(r.sentence).not.toMatch(/UNCALIBRATED/i);
        expect(r.sentence).toMatch(/isn't ready yet|no regime reading/i);
    });

    it('treats a null/missing state the same as UNCALIBRATED', () => {
        expect(plainRegime(null).available).toBe(false);
        expect(plainRegime(undefined).available).toBe(false);
        expect(plainRegime('').available).toBe(false);
        expect(plainRegime(null).confidencePct).toBeNull();
    });

    // Regression: a genuine reading can legitimately have confidence 0.0 (a
    // real regime the model just isn't confident about). That must still
    // render as a real reading with 0% — never collapse a falsy-but-valid
    // 0.0 into "unavailable" the way `confidence && ...` would.
    it('renders a genuine reading with confidence exactly 0 as a real 0% reading, not unavailable', () => {
        const r = plainRegime('risk_off', 0.0);
        expect(r.available).toBe(true);
        expect(r.confidencePct).toBe(0);
        expect(r.sentence).toBe('Investors are nervous and playing it safe.');
    });

    it('carries a real confidence value through unchanged for a genuine reading', () => {
        const r = plainRegime('risk_on', 0.62);
        expect(r).toEqual({
            sentence: 'Investors are feeling confident.',
            tone: 'up',
            available: true,
            confidencePct: 62,
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
