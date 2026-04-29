import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import {
    drawerSections,
    hiddenDrawerRouteIds,
    isNavigableRouteId,
    normalizeNavigableRouteId,
    routes,
    secondaryTabRoutes,
    tabRoutes,
} from '../routes.js';

const srcRoot = path.resolve(process.cwd(), 'src');

describe('route registry', () => {
    it('has unique route ids', () => {
        const ids = routes.map(route => route.id);
        expect(new Set(ids).size).toBe(ids.length);
    });

    it('points every registered route at an existing view component', () => {
        const missing = routes
            .map(route => ({ id: route.id, component: route.component }))
            .filter(route => !fs.existsSync(path.resolve(srcRoot, route.component)));

        expect(missing).toEqual([]);
    });

    it('registers operations views that used to be stale mobile-only targets', () => {
        const ids = new Set(routes.map(route => route.id));
        expect(ids.has('operator')).toBe(true);
        expect(ids.has('snapshots')).toBe(true);
    });

    it('keeps Surfacer separate from the Canvas toy workspace', () => {
        const surfacer = routes.find(route => route.id === 'surfacer');
        const canvas = routes.find(route => route.id === 'canvas');

        expect(surfacer?.component).toBe('./views/Surfacer.jsx');
        expect(surfacer?.group).toBe('worldView');
        expect(surfacer?.nav).toBe('tab');
        expect(canvas?.component).toBe('./views/Canvas.jsx');
        expect(canvas?.nav).toBe('drawer');
    });

    it('surfaces only the core alpha views as top tabs', () => {
        expect(tabRoutes.map(route => route.id)).toEqual([
            'surfacer',
            'dashboard',
            'money-flow',
            'actor-network',
            'risk',
            'intelligence',
        ]);
    });

    it('demotes internal tooling and secondary world views into Homework', () => {
        const homework = drawerSections.find(section => section.label === 'HOMEWORK');
        const ids = new Set(homework?.items.map(route => route.id));

        expect(ids.has('actor-universe')).toBe(true);
        expect(ids.has('lever-map')).toBe(true);
        expect(ids.has('edge-scanner')).toBe(false);
        expect(ids.has('operator')).toBe(true);
        expect(ids.has('system')).toBe(true);
        expect(ids.has('settings')).toBe(true);
    });

    it('keeps Edge Scanner in the trading drawer surface', () => {
        const trading = drawerSections.find(section => section.label === 'TRADING');
        const ids = new Set(trading?.items.map(route => route.id));

        expect(ids.has('edge-scanner')).toBe(true);
    });

    it('suppresses duplicate alias routes from the visible drawer surface', () => {
        const drawerSectionIds = new Set(drawerSections.flatMap(section => section.items.map(route => route.id)));

        expect(drawerSectionIds.has('graph-analytics')).toBe(false);
        expect(drawerSectionIds.has('causal-map')).toBe(false);
    });

    it('keeps every visible non-primary view reachable through exactly one drawer section', () => {
        const drawerSectionIds = drawerSections.flatMap(section => section.items.map(route => route.id));
        const expected = [
            ...secondaryTabRoutes.map(route => route.id),
            ...routes
                .filter(route => route.nav === 'drawer' && !hiddenDrawerRouteIds.has(route.id))
                .map(route => route.id),
        ];

        expect(new Set(drawerSectionIds)).toEqual(new Set(expected));
        expect(drawerSectionIds.length).toBe(expected.length);
    });

    it('normalizes stale route aliases back to canonical navigable ids', () => {
        expect(normalizeNavigableRouteId('graph-analytics')).toBe('spider-stats');
        expect(normalizeNavigableRouteId('causal-map')).toBe('timeline');
        expect(normalizeNavigableRouteId('watchlist-analysis')).toBe('watchlist-analysis');
        expect(isNavigableRouteId('ghost-view')).toBe(false);
    });
});
