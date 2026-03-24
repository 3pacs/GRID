/**
 * Tests for Zustand store (store.js).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock localStorage
const localStorageMock = (() => {
    let store = {};
    return {
        getItem: vi.fn((key) => store[key] || null),
        setItem: vi.fn((key, val) => { store[key] = val; }),
        removeItem: vi.fn((key) => { delete store[key]; }),
        clear: vi.fn(() => { store = {}; }),
    };
})();
Object.defineProperty(global, 'localStorage', { value: localStorageMock });

const { default: useStore } = await import('../store.js');

describe('Store', () => {
    beforeEach(() => {
        localStorageMock.clear();
        vi.clearAllMocks();
        useStore.setState({
            token: null,
            isAuthenticated: false,
            wsConnected: false,
            activeView: 'dashboard',
            notifications: [],
            currentRegime: null,
            latestSignals: null,
            systemStatus: null,
            agentProgress: null,
            agentLastComplete: null,
        });
    });

    describe('initial state', () => {
        it('has null token by default', () => {
            expect(useStore.getState().token).toBeNull();
        });

        it('has wsConnected false by default', () => {
            expect(useStore.getState().wsConnected).toBe(false);
        });

        it('has activeView set to dashboard', () => {
            expect(useStore.getState().activeView).toBe('dashboard');
        });
    });

    describe('setAuth / clearAuth', () => {
        it('setAuth stores token and sets authenticated', () => {
            useStore.getState().setAuth('test-token');
            expect(useStore.getState().token).toBe('test-token');
            expect(useStore.getState().isAuthenticated).toBe(true);
        });

        it('setAuth persists token to localStorage', () => {
            useStore.getState().setAuth('persist-me');
            expect(localStorageMock.setItem).toHaveBeenCalledWith('grid_token', 'persist-me');
        });

        it('clearAuth removes token and sets isAuthenticated false', () => {
            useStore.getState().setAuth('test-token');
            useStore.getState().clearAuth();
            expect(useStore.getState().token).toBeNull();
            expect(useStore.getState().isAuthenticated).toBe(false);
        });

        it('clearAuth removes token from localStorage', () => {
            useStore.getState().setAuth('to-remove');
            useStore.getState().clearAuth();
            expect(localStorageMock.removeItem).toHaveBeenCalledWith('grid_token');
        });
    });

    describe('setActiveView', () => {
        it('changes the active view', () => {
            useStore.getState().setActiveView('journal');
            expect(useStore.getState().activeView).toBe('journal');
        });
    });

    describe('addNotification', () => {
        it('adds a notification to the list', () => {
            useStore.getState().addNotification('info', 'Test message');
            const notifs = useStore.getState().notifications;
            expect(notifs).toHaveLength(1);
            expect(notifs[0].type).toBe('info');
            expect(notifs[0].message).toBe('Test message');
            expect(notifs[0].id).toBeDefined();
        });

        it('caps at 5 notifications', () => {
            for (let i = 0; i < 7; i++) {
                useStore.getState().addNotification('info', `msg ${i}`);
            }
            expect(useStore.getState().notifications).toHaveLength(5);
        });
    });

    describe('handleWsMessage', () => {
        it('handles regime_update', () => {
            const regimeData = { state: 'EXPANSION', confidence: 0.85 };
            useStore.getState().handleWsMessage({ type: 'regime_update', data: regimeData });
            expect(useStore.getState().currentRegime).toEqual(regimeData);
        });

        it('handles signal_update', () => {
            const signalData = { signals: [{ name: 'SPY', value: 1.2 }] };
            useStore.getState().handleWsMessage({ type: 'signal_update', data: signalData });
            expect(useStore.getState().latestSignals).toEqual(signalData);
        });

        it('handles agent_progress', () => {
            const progressData = { step: 3, total: 10 };
            useStore.getState().handleWsMessage({ type: 'agent_progress', data: progressData });
            expect(useStore.getState().agentProgress).toEqual(progressData);
        });

        it('handles agent_run_complete', () => {
            useStore.setState({ agentProgress: { step: 5 } });
            const completeData = { result: 'done' };
            useStore.getState().handleWsMessage({ type: 'agent_run_complete', data: completeData });
            expect(useStore.getState().agentProgress).toBeNull();
            expect(useStore.getState().agentLastComplete).toEqual(completeData);
        });

        it('handles connected by setting wsConnected true', () => {
            useStore.setState({ wsConnected: false });
            useStore.getState().handleWsMessage({ type: 'connected', data: null });
            expect(useStore.getState().wsConnected).toBe(true);
        });

        it('handles ping by setting wsConnected true', () => {
            useStore.setState({ wsConnected: false });
            useStore.getState().handleWsMessage({ type: 'ping', data: {} });
            expect(useStore.getState().wsConnected).toBe(true);
        });

        it('handles node_update by merging into systemStatus', () => {
            useStore.setState({ systemStatus: { db: 'ok' } });
            const nodeData = { peers: 3 };
            useStore.getState().handleWsMessage({ type: 'node_update', data: nodeData });
            expect(useStore.getState().systemStatus).toEqual({ db: 'ok', hyperspace: nodeData });
        });

        it('does not crash on unknown message type', () => {
            expect(() => {
                useStore.getState().handleWsMessage({ type: 'unknown_type', data: {} });
            }).not.toThrow();
        });
    });
});
