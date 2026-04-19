import React from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const localStorageState = {};
let documentVisibilityState = 'visible';
const localStorageShim = {
    getItem: vi.fn((key) => (key in localStorageState ? localStorageState[key] : null)),
    setItem: vi.fn((key, value) => {
        localStorageState[key] = String(value);
    }),
    removeItem: vi.fn((key) => {
        delete localStorageState[key];
    }),
    clear: vi.fn(() => {
        for (const key of Object.keys(localStorageState)) {
            delete localStorageState[key];
        }
    }),
};

if (typeof globalThis.localStorage === 'undefined' || typeof globalThis.localStorage.getItem !== 'function') {
    Object.defineProperty(globalThis, 'localStorage', {
        value: localStorageShim,
        configurable: true,
    });
}
if (typeof window !== 'undefined' && (typeof window.localStorage === 'undefined' || typeof window.localStorage.getItem !== 'function')) {
    Object.defineProperty(window, 'localStorage', {
        value: globalThis.localStorage,
        configurable: true,
    });
}
if (typeof document !== 'undefined') {
    Object.defineProperty(document, 'visibilityState', {
        configurable: true,
        get: () => documentVisibilityState,
    });
}

vi.mock('../api.js', () => ({
    api: {
        connectWebSocket: vi.fn(),
        disconnectWebSocket: vi.fn(),
        getRecentRealtimeEvents: vi.fn(),
        login: vi.fn(),
        register: vi.fn(),
    },
}));

vi.mock('../components/NavBar.jsx', () => ({
    default: () => <div data-testid="nav-bar" />,
}));

vi.mock('../components/ViewErrorBoundary.jsx', () => ({
    default: ({ children }) => <>{children}</>,
}));

vi.mock('../components/ChatPanel.jsx', () => ({
    default: () => null,
}));

vi.mock('../components/CommandPalette.jsx', () => ({
    default: () => null,
}));

vi.mock('../components/Onboarding.jsx', () => ({
    default: () => null,
}));

vi.mock('../views/EdgeScanner.jsx', () => ({
    default: () => <div>Edge Scanner Ready</div>,
}));

vi.mock('../views/Dashboard.jsx', () => ({
    default: () => <div>Dashboard Ready</div>,
}));

const { api } = await import('../api.js');
const { clearAuthSession } = await import('../authSession.js');
const { default: useAuthStore } = await import('../stores/authStore.js');
const { default: useDomainStore } = await import('../stores/domainStore.js');
const { default: useRealtimeStore } = await import('../stores/realtimeStore.js');
const { default: useUiStore } = await import('../stores/uiStore.js');
const { App } = await import('../app.jsx');
const { default: Login } = await import('../views/Login.jsx');

function resetStores() {
    documentVisibilityState = 'visible';
    clearAuthSession();
    useAuthStore.setState({
        token: null,
        isAuthenticated: false,
        userRole: 'admin',
        username: 'operator',
    });
    useUiStore.setState({
        theme: 'dark',
        activeView: 'surfacer',
        loading: {},
        errors: {},
        notifications: [],
    });
    useDomainStore.setState({
        systemStatus: null,
        latestSignals: null,
        currentRegime: null,
        regimeHistory: [],
        journalEntries: [],
        journalStats: null,
        productionModels: {},
        allModels: [],
        jobs: [],
        hypotheses: [],
        agentProgress: null,
        agentLastComplete: null,
    });
    useRealtimeStore.setState({
        wsConnected: false,
        livePriceUpdates: {},
        liveAlerts: [],
        liveRecommendations: [],
        lastRegimeChange: null,
        lastSocketEventAt: null,
        pushSubscription: null,
        chatMessages: [],
        chatUnread: 0,
    });
    window.location.hash = '#/login';
}

describe('edge scanner auth reload path', () => {
    beforeEach(() => {
        resetStores();
        api.login.mockReset();
        api.register.mockReset();
        api.connectWebSocket.mockReset();
        api.disconnectWebSocket.mockReset();
        api.getRecentRealtimeEvents.mockReset();
    });

    afterEach(() => {
        window.location.hash = '#/login';
    });

    it('persists auth on login so the session survives a reload', async () => {
        api.login.mockResolvedValue({
            token: 'session-token',
            role: 'admin',
            username: 'operator',
        });

        render(<Login />);

        fireEvent.change(screen.getByPlaceholderText('Password'), {
            target: { value: 'gridmaster2026' },
        });
        fireEvent.click(screen.getByRole('button', { name: 'AUTHENTICATE' }));

        await waitFor(() => {
            expect(api.login).toHaveBeenCalledWith('gridmaster2026');
            expect(useAuthStore.getState().isAuthenticated).toBe(true);
        });

        expect(localStorage.getItem('grid_token')).toBe('session-token');
        expect(localStorage.getItem('grid_role')).toBe('admin');
        expect(localStorage.getItem('grid_username')).toBe('operator');
    });

    it('does not open a websocket for authenticated edge scanner remounts', async () => {
        useAuthStore.getState().setAuth('session-token', 'admin', 'operator');
        useUiStore.getState().setActiveView('edge-scanner');
        window.location.hash = '#/edge-scanner';

        const first = render(<App />);

        expect(await screen.findByText('Edge Scanner Ready')).toBeInTheDocument();
        expect(useUiStore.getState().activeView).toBe('edge-scanner');
        expect(window.location.hash).toBe('#/edge-scanner');
        expect(api.connectWebSocket).not.toHaveBeenCalled();

        first.unmount();

        render(<App />);

        expect(await screen.findByText('Edge Scanner Ready')).toBeInTheDocument();
        expect(useUiStore.getState().activeView).toBe('edge-scanner');
        expect(window.location.hash).toBe('#/edge-scanner');
        expect(api.connectWebSocket).not.toHaveBeenCalled();
    });

    it('keeps the websocket on live views only and disconnects hidden tabs', async () => {
        useAuthStore.getState().setAuth('session-token', 'admin', 'operator');
        useUiStore.getState().setActiveView('dashboard');
        window.location.hash = '#/dashboard';

        render(<App />);

        expect(await screen.findByText('Dashboard Ready')).toBeInTheDocument();
        await waitFor(() => {
            expect(api.connectWebSocket).toHaveBeenCalledTimes(1);
        });
        expect(useUiStore.getState().activeView).toBe('dashboard');

        act(() => {
            documentVisibilityState = 'hidden';
            document.dispatchEvent(new Event('visibilitychange'));
        });

        await waitFor(() => {
            expect(api.disconnectWebSocket).toHaveBeenCalledTimes(1);
        });

        act(() => {
            documentVisibilityState = 'visible';
            document.dispatchEvent(new Event('visibilitychange'));
        });

        await waitFor(() => {
            expect(api.connectWebSocket).toHaveBeenCalledTimes(2);
        });
    });

    it('replays missed realtime events after a live view reconnect', async () => {
        useAuthStore.getState().setAuth('session-token', 'admin', 'operator');
        useUiStore.getState().setActiveView('dashboard');
        useRealtimeStore.setState({
            lastSocketEventAt: '2026-04-19T05:00:00.000Z',
        });
        api.getRecentRealtimeEvents.mockResolvedValue({
            events: [
                {
                    type: 'alert',
                    timestamp: '2026-04-19T05:00:02.000Z',
                    data: { severity: 'warning', message: 'Recovered alert' },
                },
                {
                    type: 'recommendation',
                    timestamp: '2026-04-19T05:00:03.000Z',
                    data: { ticker: 'GD', direction: 'CALL', strike: 300 },
                },
            ],
        });
        window.location.hash = '#/dashboard';

        render(<App />);

        expect(await screen.findByText('Dashboard Ready')).toBeInTheDocument();

        act(() => {
            useRealtimeStore.setState({ wsConnected: true });
        });

        await waitFor(() => {
            expect(api.getRecentRealtimeEvents).toHaveBeenCalledTimes(1);
        });

        const replayArgs = api.getRecentRealtimeEvents.mock.calls[0][0];
        expect(replayArgs.since).toBe('2026-04-19T05:00:00.000Z');
        expect(replayArgs.before).toBeTruthy();
        expect(useRealtimeStore.getState().liveAlerts).toHaveLength(1);
        expect(useRealtimeStore.getState().liveRecommendations).toHaveLength(1);
        expect(useRealtimeStore.getState().lastSocketEventAt).toBe('2026-04-19T05:00:03.000Z');
    });
});
