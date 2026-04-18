import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { routes } from '../routes.js';

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
});
