/**
 * useWebSocket — Real-time WebSocket hook for GRID PWA.
 *
 * Connects to the backend WebSocket at /ws, handles authentication,
 * auto-reconnects with exponential backoff, and routes all incoming
 * messages through the Zustand store.
 *
 * Returns: { connected, lastMessage, prices, alerts, recommendations }
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import useStore from '../store.js';

const WS_INITIAL_DELAY = 1000;
const WS_MAX_DELAY = 30000;
const WS_BACKOFF_FACTOR = 2;

export function useWebSocket() {
    const wsRef = useRef(null);
    const reconnectTimer = useRef(null);
    const delayRef = useRef(WS_INITIAL_DELAY);
    const mountedRef = useRef(true);
    const [lastMessage, setLastMessage] = useState(null);

    const token = useStore(s => s.token);
    const isAuthenticated = useStore(s => s.isAuthenticated);
    const connected = useStore(s => s.wsConnected);
    const prices = useStore(s => s.livePriceUpdates);
    const alerts = useStore(s => s.liveAlerts);
    const recommendations = useStore(s => s.liveRecommendations);
    const setWsConnected = useStore(s => s.setWsConnected);
    const handleWsMessage = useStore(s => s.handleWsMessage);

    const connect = useCallback(() => {
        if (!token || !mountedRef.current) return;
        // Clean up any existing connection
        if (wsRef.current) {
            try { wsRef.current.close(); } catch (_) { /* ignore */ }
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${protocol}//${window.location.host}/ws?token=${encodeURIComponent(token)}`;

        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
            if (!mountedRef.current) return;
            delayRef.current = WS_INITIAL_DELAY;
            setWsConnected(true);
        };

        ws.onmessage = (event) => {
            if (!mountedRef.current) return;
            try {
                const parsed = JSON.parse(event.data);
                setLastMessage(parsed);
                handleWsMessage(parsed);
            } catch (e) {
                // non-JSON message, ignore
            }
        };

        ws.onclose = () => {
            if (!mountedRef.current) return;
            setWsConnected(false);
            // Exponential backoff reconnect
            const delay = delayRef.current;
            delayRef.current = Math.min(delay * WS_BACKOFF_FACTOR, WS_MAX_DELAY);
            reconnectTimer.current = setTimeout(() => {
                if (mountedRef.current && token) {
                    connect();
                }
            }, delay);
        };

        ws.onerror = () => {
            // onclose will fire after onerror, which handles reconnect
        };
    }, [token, setWsConnected, handleWsMessage]);

    useEffect(() => {
        mountedRef.current = true;

        if (isAuthenticated && token) {
            connect();
        }

        return () => {
            mountedRef.current = false;
            if (reconnectTimer.current) {
                clearTimeout(reconnectTimer.current);
                reconnectTimer.current = null;
            }
            if (wsRef.current) {
                try { wsRef.current.close(); } catch (_) { /* ignore */ }
                wsRef.current = null;
            }
            setWsConnected(false);
        };
    }, [isAuthenticated, token, connect, setWsConnected]);

    return {
        connected,
        lastMessage,
        prices,
        alerts,
        recommendations,
    };
}
