/**
 * useEventStream — SSE client hook for the GRID event bus.
 *
 * Connects to /api/v1/events/stream and dispatches events to the
 * appropriate Zustand store slices. Runs alongside the existing WebSocket
 * connection (which handles bidirectional chat + prices).
 *
 * Usage:
 *   const { connected, lastEvent } = useEventStream();
 *   const { connected } = useEventStream({ channels: ['grid_signal_fire'] });
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import useAuthStore from '../stores/authStore.js';
import { api } from '../api.js';

const EVENTS_PATH = '/api/v1/events/stream';
const RECONNECT_DELAY_MS = 3000;
const MAX_RECONNECT_DELAY_MS = 30000;

/**
 * @param {Object} options
 * @param {string[]} options.channels - Channel names to subscribe to (default: all)
 * @param {(event: {channel, payload, timestamp}) => void} options.onEvent - Custom event handler
 */
export function useEventStream(options = {}) {
    const { channels, onEvent } = options;
    const [connected, setConnected] = useState(false);
    const [lastEvent, setLastEvent] = useState(null);
    const sourceRef = useRef(null);
    const delayRef = useRef(RECONNECT_DELAY_MS);
    const mountedRef = useRef(true);
    const reconnectTimer = useRef(null);

    const token = useAuthStore(s => s.token);
    const isAuthenticated = useAuthStore(s => s.isAuthenticated);

    const attach = (source, scheduleReconnect) => {
        source.onopen = () => {
            if (!mountedRef.current) return;
            setConnected(true);
            delayRef.current = RECONNECT_DELAY_MS;
        };

        source.addEventListener('connected', () => {
            if (!mountedRef.current) return;
            setConnected(true);
        });

        source.onmessage = (e) => {
            if (!mountedRef.current) return;
            try {
                const parsed = JSON.parse(e.data);
                setLastEvent(parsed);
                onEvent?.(parsed);
            } catch (_) {
                // non-JSON message
            }
        };

        source.onerror = () => {
            if (!mountedRef.current) return;
            setConnected(false);
            source.close();
            // Reconnect with backoff (connect() mints a fresh ticket; the old
            // one was single-use).
            scheduleReconnect();
        };
    };

    const connect = useCallback(() => {
        if (!token || !mountedRef.current) return;

        // Close existing connection
        if (sourceRef.current) {
            sourceRef.current.close();
        }

        const scheduleReconnect = () => {
            const delay = delayRef.current;
            delayRef.current = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
            reconnectTimer.current = setTimeout(() => {
                if (mountedRef.current && token) connect();
            }, delay);
        };

        const params = channels && channels.length > 0 ? { channels: channels.join(',') } : null;

        // EventSource cannot send an Authorization header. Never put the
        // session JWT in the URL (it lands in proxy/access logs): exchange it
        // for a 60 s single-use stream ticket, fetched fresh on every connect.
        sourceRef.current = api.openTicketedEventSource(EVENTS_PATH, params, {
            relative: true,
            onTicketError: () => {
                if (!mountedRef.current) return;
                setConnected(false);
                scheduleReconnect();
            },
            onOpen: source => attach(source, scheduleReconnect),
        });
    }, [token, channels, onEvent]);

    useEffect(() => {
        mountedRef.current = true;
        if (isAuthenticated && token) {
            connect();
        }
        return () => {
            mountedRef.current = false;
            if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
            if (sourceRef.current) sourceRef.current.close();
        };
    }, [isAuthenticated, token, connect]);

    return { connected, lastEvent };
}
