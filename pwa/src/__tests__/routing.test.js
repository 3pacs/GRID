import { describe, expect, it } from 'vitest';
import { buildRouteHash, parseHashRoute } from '../routing.js';

describe('routing helpers', () => {
    it('opens Surfacer as the front page', () => {
        expect(parseHashRoute('')).toEqual({ view: 'surfacer' });
        expect(parseHashRoute('#/')).toEqual({ view: 'surfacer' });
    });

    it('parses canonical watchlist links', () => {
        expect(parseHashRoute('#/watchlist/NVDA')).toEqual({
            view: 'watchlist-analysis',
            selectedTicker: 'NVDA',
        });
    });

    it('keeps legacy watchlist query links working', () => {
        expect(parseHashRoute('#/watchlist-analysis?ticker=TSLA')).toEqual({
            view: 'watchlist-analysis',
            selectedTicker: 'TSLA',
        });
    });

    it('routes canvas deep links back to the canvas module', () => {
        expect(parseHashRoute('#/canvas/AAPL/capital')).toEqual({ view: 'canvas' });
        expect(parseHashRoute('#/canvas?board=abc-123')).toEqual({ view: 'canvas' });
    });

    it('builds child route hashes through canonical paths', () => {
        expect(buildRouteHash('watchlist-analysis', 'MSFT')).toBe('#/watchlist/MSFT');
        expect(buildRouteHash('sector-dive', 'Semiconductors')).toBe('#/sector-dive/Semiconductors');
    });

    it('preserves focus query parameters for routed search results', () => {
        expect(parseHashRoute('#/signals?feature=yield_curve')).toEqual({
            view: 'signals',
            focusFeature: 'yield_curve',
        });
        expect(parseHashRoute('#/discovery?hypothesis=42')).toEqual({
            view: 'discovery',
            focusHypothesis: '42',
        });
        expect(parseHashRoute('#/actor-network?actor=Nancy%20Pelosi')).toEqual({
            view: 'actor-network',
            focusActor: 'Nancy Pelosi',
        });
        expect(parseHashRoute('#/system?source=fred')).toEqual({
            view: 'system',
            focusSource: 'fred',
        });
    });

    it('builds focused search hashes for generic views', () => {
        expect(buildRouteHash('signals', 'yield_curve')).toBe('#/signals?feature=yield_curve');
        expect(buildRouteHash('discovery', '42')).toBe('#/discovery?hypothesis=42');
        expect(buildRouteHash('actor-network', 'Nancy Pelosi')).toBe('#/actor-network?actor=Nancy%20Pelosi');
        expect(buildRouteHash('system', 'fred')).toBe('#/system?source=fred');
    });
});
