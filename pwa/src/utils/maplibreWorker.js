/**
 * MapLibre GL v6 worker wiring for the Vite build.
 *
 * maplibre-gl 6 resolves its worker as
 * `new URL('./maplibre-gl-worker.mjs', import.meta.url)`. Once Vite bundles
 * maplibre into `assets/maplibre-<hash>.js`, that resolves to
 * `/assets/maplibre-gl-worker.mjs`, a file the build never emits; the server
 * answers 404 (or the SPA index.html) and the map fails with
 * "Worker failed to load. Check that the worker URL is correct."
 *
 * `?worker&url` makes Vite bundle the worker entry as its own hashed `.js`
 * asset (served with a JavaScript MIME type by Caddy and FastAPI alike) and
 * hands back its same-origin URL, which the CSP already allows
 * (`worker-src 'self' blob:` in api/main.py).
 */
import { getWorkerUrl, setWorkerUrl } from 'maplibre-gl';
import maplibreWorkerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';

export const MAPLIBRE_WORKER_URL = maplibreWorkerUrl;

/** Point MapLibre at the bundled worker. Idempotent; call before `new Map()`. */
export function ensureMaplibreWorker() {
    if (getWorkerUrl() !== MAPLIBRE_WORKER_URL) {
        setWorkerUrl(MAPLIBRE_WORKER_URL);
    }
    return getWorkerUrl();
}
