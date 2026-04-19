import { describe, expect, it } from 'vitest';
import { resolvePaletteNavigation, sanitizePaletteResult } from '../components/CommandPalette.jsx';

describe('CommandPalette navigation resolution', () => {
    it('keeps valid routes and normalizes stale aliases', () => {
        expect(resolvePaletteNavigation({
            type: 'view',
            action: 'dashboard',
            param: null,
        })).toEqual({
            action: 'dashboard',
            param: undefined,
        });

        expect(resolvePaletteNavigation({
            type: 'view',
            action: 'graph-analytics',
            param: null,
        })).toEqual({
            action: 'spider-stats',
            param: undefined,
        });
    });

    it('falls back to canonical routed views for typed results', () => {
        expect(resolvePaletteNavigation({
            type: 'ticker',
            title: 'NVDA',
            action: 'stale-view',
        })).toEqual({
            action: 'watchlist-analysis',
            param: 'NVDA',
        });

        expect(resolvePaletteNavigation({
            type: 'sector',
            title: 'Semiconductors',
            action: null,
        })).toEqual({
            action: 'sector-dive',
            param: 'Semiconductors',
        });

        expect(resolvePaletteNavigation({
            type: 'feature',
            title: 'yield_curve_2s10s',
            action: '',
        })).toEqual({
            action: 'signals',
            param: 'yield_curve_2s10s',
        });
    });

    it('filters stale view-only results with nowhere safe to go', () => {
        expect(resolvePaletteNavigation({
            type: 'view',
            action: 'ghost-view',
            param: null,
        })).toBeNull();

        expect(sanitizePaletteResult({
            type: 'view',
            action: 'ghost-view',
            title: 'Ghost',
        })).toBeNull();
    });
});
