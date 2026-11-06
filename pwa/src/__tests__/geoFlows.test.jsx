import React from 'react';
import { act, render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

// jsdom has no real WebGL context, so the real MapLibre Map throws on
// construction. Stub it out — this view's async-data behavior doesn't
// depend on the base map actually rendering.
const mapState = vi.hoisted(() => ({ workerUrl: '', instances: [], workerUrlAtConstruct: [] }));
vi.mock('maplibre-gl', () => ({
    Map: class {
        constructor() {
            mapState.workerUrlAtConstruct.push(mapState.workerUrl);
            this.handlers = {};
            this.removed = 0;
            mapState.instances.push(this);
        }
        on(type, handler) { this.handlers[type] = handler; return this; }
        jumpTo() {}
        remove() { this.removed += 1; }
    },
    getWorkerUrl: () => mapState.workerUrl,
    setWorkerUrl: (url) => { mapState.workerUrl = url; },
}));

vi.mock('@deck.gl/react', () => ({
    default: () => null,
}));

if (typeof globalThis.ResizeObserver === 'undefined') {
    globalThis.ResizeObserver = class {
        observe() {}
        unobserve() {}
        disconnect() {}
    };
}

// jsdom's canvas has no real WebGL context, which would otherwise make the
// view's own feature-detection render a "WebGL Not Available" screen instead
// of the data-loading UI under test.
HTMLCanvasElement.prototype.getContext = vi.fn(() => ({}));

// maplibre-gl calls this at module load time to spin up its worker.
if (typeof window.URL.createObjectURL !== 'function') {
    window.URL.createObjectURL = vi.fn(() => 'blob:mock');
}

const { default: GeoFlows } = await import('../views/GeoFlows.jsx');
const { MAPLIBRE_WORKER_URL } = await import('../utils/maplibreWorker.js');

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('GeoFlows view async data', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('shows a loading skeleton while fetching, then clears it once data resolves', async () => {
        const gate = deferred();
        api.get.mockImplementation((path) => {
            if (path.includes('/geo/flows')) return gate.promise;
            if (path.includes('/geo/actors')) return Promise.resolve({ actors: [] });
            return Promise.resolve({ density: [] });
        });

        render(<GeoFlows />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ flows: [] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.get.mockImplementation((path) => {
            if (path.includes('/geo/flows')) return Promise.reject(new Error('geo flow data unavailable'));
            if (path.includes('/geo/actors')) return Promise.resolve({ actors: [] });
            return Promise.resolve({ density: [] });
        });

        render(<GeoFlows />);

        expect(await screen.findByText('geo flow data unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.get.mockImplementation((path) => {
            if (path.includes('/geo/flows')) return Promise.resolve({ flows: [] });
            if (path.includes('/geo/actors')) return Promise.resolve({ actors: [] });
            return Promise.resolve({ density: [] });
        });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(screen.queryByText('geo flow data unavailable')).not.toBeInTheDocument();
        });
    });

});

describe('GeoFlows MapLibre basemap (#527)', () => {
    beforeEach(() => {
        api.get.mockReset();
        api.get.mockImplementation(() => new Promise(() => {}));
        mapState.workerUrl = '';
        mapState.instances.length = 0;
        mapState.workerUrlAtConstruct.length = 0;
    });

    it('points MapLibre at the bundled worker before constructing the map', () => {
        render(<GeoFlows />);
        expect(mapState.instances).toHaveLength(1);
        expect(MAPLIBRE_WORKER_URL).toBeTruthy();
        // Not the import.meta-relative default that 404s in the Vite build.
        expect(MAPLIBRE_WORKER_URL).not.toMatch(/\/assets\/maplibre-gl-worker\.mjs/);
        expect(mapState.workerUrlAtConstruct).toEqual([MAPLIBRE_WORKER_URL]);
    });

    it('surfaces a basemap worker/style error instead of a silent blank map', () => {
        render(<GeoFlows />);
        expect(screen.queryByTestId('geo-basemap-error')).toBeNull();
        act(() => {
            mapState.instances[0].handlers.error({
                error: new Error('Worker failed to load. Check that the worker URL is correct.'),
            });
        });
        expect(screen.getByTestId('geo-basemap-error').textContent)
            .toBe('Basemap unavailable: Worker failed to load. Check that the worker URL is correct.');
    });

    it('removes the map on unmount and creates exactly one new map on re-entry', () => {
        const first = render(<GeoFlows />);
        first.unmount();
        expect(mapState.instances).toHaveLength(1);
        expect(mapState.instances[0].removed).toBe(1);
        const second = render(<GeoFlows />);
        expect(mapState.instances).toHaveLength(2);
        second.unmount();
        expect(mapState.instances.map(m => m.removed)).toEqual([1, 1]);
    });

    it('reports a failed flows marker as unavailable instead of "0 flows" on an empty map', async () => {
        api.get.mockImplementation((path) => {
            if (path.includes('/geo/flows')) return Promise.resolve({ error: true, status: 503, message: 'geo flows cold' });
            if (path.includes('/geo/actors')) return Promise.resolve({ error: true, status: 500, message: 'actors down' });
            return Promise.resolve({ density: [] });
        });
        render(<GeoFlows />);
        expect(await screen.findByText('geo flows cold')).toBeInTheDocument();
    });

    it('lists failed optional overlays while flows render', async () => {
        api.get.mockImplementation((path) => {
            if (path.includes('/geo/flows')) return Promise.resolve({ flows: [] });
            if (path.includes('/geo/actors')) return Promise.resolve({ error: true, status: 500, message: 'actors down' });
            return Promise.resolve({ density: [] });
        });
        render(<GeoFlows />);
        expect((await screen.findByTestId('geo-layer-errors')).textContent).toBe('Unavailable: actors down');
    });
});
