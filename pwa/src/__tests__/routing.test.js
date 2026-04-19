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
        expect(parseHashRoute('#/watchlist/NVDA?from=dashboard')).toEqual({
            view: 'watchlist-analysis',
            selectedTicker: 'NVDA',
            originView: 'dashboard',
        });
    });

    it('keeps legacy watchlist query links working', () => {
        expect(parseHashRoute('#/watchlist-analysis?ticker=TSLA')).toEqual({
            view: 'watchlist-analysis',
            selectedTicker: 'TSLA',
        });
    });

    it('routes canvas deep links back to the canvas module', () => {
        expect(parseHashRoute('#/canvas/AAPL/capital?from=intelligence-search')).toEqual({
            view: 'canvas',
            actorId: 'AAPL',
            lens: 'capital',
            boardId: null,
            originView: 'intelligence-search',
        });
        expect(parseHashRoute('#/canvas?board=abc-123')).toEqual({
            view: 'canvas',
            actorId: null,
            lens: 'graph',
            boardId: 'abc-123',
        });
    });

    it('builds child route hashes through canonical paths', () => {
        expect(buildRouteHash('watchlist-analysis', 'MSFT')).toBe('#/watchlist/MSFT');
        expect(buildRouteHash('sector-dive', 'Semiconductors')).toBe('#/sector-dive/Semiconductors');
        expect(buildRouteHash('watchlist-analysis', { ticker: 'MSFT', from: 'dashboard' })).toBe('#/watchlist/MSFT?from=dashboard');
        expect(buildRouteHash('sector-dive', { sector: 'Semiconductors', from: 'dashboard' })).toBe('#/sector-dive/Semiconductors?from=dashboard');
        expect(buildRouteHash('intelligence-search', { from: 'dashboard' })).toBe('#/intelligence-search?from=dashboard');
        expect(buildRouteHash('canvas', { actorId: 'AAPL', lens: 'capital', from: 'sector-dive' })).toBe('#/canvas/AAPL/capital?from=sector-dive');
        expect(buildRouteHash('canvas', { board: 'abc-123', from: 'intelligence-search' })).toBe('#/canvas?board=abc-123&from=intelligence-search');
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
