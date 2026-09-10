import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';

const indexHtmlPath = path.resolve(process.cwd(), 'index.html');

// Two manifest.json files exist: pwa/manifest.json (unused duplicate) and
// pwa/public/manifest.json (what Vite's publicDir actually copies into
// pwa_dist/, and what index.html's <link rel="manifest"> resolves to at
// runtime). Both must carry the new branding so neither drifts back to GRID.
const manifestPaths = {
    root: path.resolve(process.cwd(), 'manifest.json'),
    public: path.resolve(process.cwd(), 'public', 'manifest.json'),
};

describe.each(Object.entries(manifestPaths))('PWA manifest (%s)', (_label, manifestPath) => {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));

    it('is branded as stepdad.finance, not GRID', () => {
        expect(manifest.name).toBe('stepdad.finance');
        expect(manifest.short_name).toBe('stepdad');
        expect(manifest.name).not.toMatch(/grid/i);
        expect(manifest.short_name).not.toMatch(/grid/i);
    });

    it('has a description matching the composer product', () => {
        expect(manifest.description).toBeTruthy();
        expect(manifest.description).not.toMatch(/grid/i);
    });

    it('points every icon at a file that exists on disk', () => {
        expect(manifest.icons.length).toBeGreaterThan(0);
        for (const icon of manifest.icons) {
            const iconSrc = icon.src.split('?')[0].replace(/^\//, '');
            const iconPath = path.resolve(process.cwd(), 'public', iconSrc);
            expect(fs.existsSync(iconPath), `missing icon file: ${icon.src}`).toBe(true);
        }
    });
});

describe('index.html branding', () => {
    const html = fs.readFileSync(indexHtmlPath, 'utf-8');

    it('has a stepdad.finance document title, not GRID', () => {
        const titleMatch = html.match(/<title>(.*?)<\/title>/);
        expect(titleMatch).not.toBeNull();
        expect(titleMatch[1]).toBe('stepdad.finance');
    });

    it('does not flash "GRID" branding on the loading screen', () => {
        const loadingMatch = html.match(/<div class="loading-screen">\s*<span>(.*?)<\/span>/);
        expect(loadingMatch).not.toBeNull();
        expect(loadingMatch[1]).toBe('stepdad.finance');
    });
});
