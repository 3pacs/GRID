import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Settings from '../views/Settings.jsx';
import { api } from '../api.js';

const storeState = {
    systemStatus: {},
    wsConnected: true,
    addNotification: vi.fn(),
    userRole: 'admin',
    username: 'anik',
    pushSupported: false,
    pushPermission: 'default',
    pushSubscription: null,
    pushPreferences: null,
    setPushPermission: vi.fn(),
    setPushSubscription: vi.fn(),
    setPushPreferences: vi.fn(),
    theme: 'dark',
    setTheme: vi.fn(),
};

vi.mock('../api.js', () => ({
    api: {
        getServices: vi.fn(),
        getApiKeys: vi.fn(),
        getHermesStatus: vi.fn(),
        getFreshness: vi.fn(),
        getSources: vi.fn(),
        listUsers: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Settings Status tab async data', () => {
    beforeEach(() => {
        api.getServices.mockReset();
        storeState.addNotification.mockClear();
    });

    it('shows a loading skeleton while fetching, then renders the service tiles', async () => {
        const gate = deferred();
        api.getServices.mockImplementation(() => gate.promise);

        render(<Settings />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            services: [{ name: 'API', status: 'online', uptime_seconds: 3600 }],
            online: 1, total: 1, resources: {}, start_time: '2026-09-10T00:00:00Z',
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('1 of 1 services online')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getServices.mockResolvedValueOnce({ error: true, message: 'service status unavailable' });

        render(<Settings />);

        expect(await screen.findByText('service status unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getServices.mockResolvedValueOnce({ services: [], online: 0, total: 0, resources: {} });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getServices).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('service status unavailable')).not.toBeInTheDocument();
    });
});
