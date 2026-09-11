import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import InfluenceNetwork from '../views/InfluenceNetwork.jsx';
import { api } from '../api.js';

if (typeof globalThis.ResizeObserver === 'undefined') {
    globalThis.ResizeObserver = class {
        observe() {}
        unobserve() {}
        disconnect() {}
    };
}

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

function mockRoutes({ graph, loops, hypocrisy }) {
    api.get.mockImplementation((path) => {
        if (path.includes('circular-flows')) return Promise.resolve(loops);
        if (path.includes('hypocrisy')) return Promise.resolve(hypocrisy);
        return Promise.resolve(graph);
    });
}

describe('InfluenceNetwork view async data', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the fetched stats', async () => {
        const gate = deferred();
        api.get.mockImplementation((path) => {
            if (path.includes('/influence') && !path.includes('circular') && !path.includes('hypocrisy')) {
                return gate.promise;
            }
            if (path.includes('circular-flows')) return Promise.resolve({ loops: [] });
            return Promise.resolve({ flags: [] });
        });

        render(<InfluenceNetwork />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            nodes: [], links: [],
            metadata: { companies_with_data: 7, total_nodes: 12, total_links: 20, total_lobbying: 1000, total_pac: 500, total_contracts: 250 },
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('7 companies')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        mockRoutes({ graph: null, loops: { loops: [] }, hypocrisy: { flags: [] } });
        api.get.mockImplementation((path) => {
            if (path.includes('circular-flows')) return Promise.resolve({ loops: [] });
            if (path.includes('hypocrisy')) return Promise.resolve({ flags: [] });
            return Promise.reject(new Error('influence graph unavailable'));
        });

        render(<InfluenceNetwork />);

        expect(await screen.findByText('influence graph unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        mockRoutes({
            graph: { nodes: [], links: [], metadata: { companies_with_data: 3, total_nodes: 3, total_links: 1 } },
            loops: { loops: [] },
            hypocrisy: { flags: [] },
        });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(screen.queryByText('influence graph unavailable')).not.toBeInTheDocument();
        });
        expect(screen.getByText('3 companies')).toBeInTheDocument();
    });
});
