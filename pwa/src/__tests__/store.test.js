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
        useStore.setState({
            token: null,
            isAuthenticated: false,
            activeView: 'dashboard',
            notifications: [],
            currentRegime: null,
        });
    });

    describe('Auth', () => {
        it('setAuth stores token and sets authenticated', () => {
            useStore.getState().setAuth('test-token');
            expect(useStore.getState().token).toBe('test-token');
            expect(useStore.getState().isAuthenticated).toBe(true);
            expect(localStorageMock.setItem).toHaveBeenCalledWith('grid_token', 'test-token');
        });

        it('clearAuth removes token', () => {
            useStore.getState().setAuth('test-token');
            useStore.getState().clearAuth();
            expect(useStore.getState().token).toBeNull();
            expect(useStore.getState().isAuthenticated).toBe(false);
        });
    });

    describe('Navigation', () => {
        it('setActiveView changes view', () => {
            useStore.getState().setActiveView('journal');
            expect(useStore.getState().activeView).toBe('journal');
        });
    });

    describe('Notifications', () => {
        it('addNotification adds to list', () => {
            useStore.getState().addNotification('info', 'Test message');
            const notifs = useStore.getState().notifications;
            expect(notifs).toHaveLength(1);
            expect(notifs[0].type).toBe('info');
            expect(notifs[0].message).toBe('Test message');
        });

        it('caps at 5 notifications', () => {
            for (let i = 0; i < 7; i++) {
                useStore.getState().addNotification('info', `msg ${i}`);
            }
            expect(useStore.getState().notifications.length).toBeLessThanOrEqual(5);
        });
    });

    describe('WebSocket handler', () => {
        it('handles regime_update', () => {
            useStore.getState().handleWsMessage({
                type: 'regime_update',
                data: { state: 'EXPANSION', confidence: 0.85 },
            });
            expect(useStore.getState().currentRegime).toEqual({
                state: 'EXPANSION',
                confidence: 0.85,
            });
        });

        it('handles ping', () => {
            useStore.getState().handleWsMessage({ type: 'ping', data: {} });
            expect(useStore.getState().wsConnected).toBe(true);
        });
    });
});
