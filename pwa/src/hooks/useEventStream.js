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

    const connect = useCallback(() => {
        if (!token || !mountedRef.current) return;

        // Close existing connection
        if (sourceRef.current) {
            sourceRef.current.close();
        }

        let url = `/api/v1/events/stream`;
        if (channels && channels.length > 0) {
            url += `?channels=${channels.join(',')}`;
        }

        // EventSource doesn't support Authorization headers natively,
        // so we pass the token as a query param (same pattern as WebSocket).
        url += `${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}`;

        const source = new EventSource(url);
        sourceRef.current = source;

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
            // Reconnect with backoff
            const delay = delayRef.current;
            delayRef.current = Math.min(delay * 2, MAX_RECONNECT_DELAY_MS);
            reconnectTimer.current = setTimeout(() => {
                if (mountedRef.current && token) connect();
            }, delay);
        };
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
