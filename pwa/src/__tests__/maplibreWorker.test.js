// Real maplibre-gl (no mock): the bundled-worker URL must replace the
// import.meta-relative default, which resolves to a file the Vite build never emits.
import { describe, expect, it } from 'vitest';

if (typeof window.URL.createObjectURL !== 'function') {
    window.URL.createObjectURL = () => 'blob:mock';
}

const maplibre = await import('maplibre-gl');
const { ensureMaplibreWorker, MAPLIBRE_WORKER_URL } = await import('../utils/maplibreWorker.js');

describe('maplibre worker wiring', () => {
    it('uses a Vite worker URL, not maplibre-gl-worker.mjs next to the bundled chunk', () => {
        expect(typeof MAPLIBRE_WORKER_URL).toBe('string');
        expect(MAPLIBRE_WORKER_URL).toMatch(/maplibre-gl-worker/);
        expect(MAPLIBRE_WORKER_URL).not.toMatch(/^https?:\/\/[^/]+\/assets\/maplibre-gl-worker\.mjs/);
    });

    it('sets the real maplibre worker URL idempotently', () => {
        expect(ensureMaplibreWorker()).toBe(MAPLIBRE_WORKER_URL);
        expect(maplibre.getWorkerUrl()).toBe(MAPLIBRE_WORKER_URL);
        expect(ensureMaplibreWorker()).toBe(MAPLIBRE_WORKER_URL);
    });
});
