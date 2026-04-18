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
});
