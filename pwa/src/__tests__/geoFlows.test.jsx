import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
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
vi.mock('maplibre-gl', () => ({
    Map: class {
        jumpTo() {}
        remove() {}
    },
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
