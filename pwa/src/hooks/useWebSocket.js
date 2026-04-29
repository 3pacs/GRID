/**
 * useWebSocket — Read-only selector for the app-level WebSocket state.
 *
 * The root app owns the single live socket connection through `api.connectWebSocket()`.
 * View hooks should read from store state instead of opening their own socket.
 */
import useStore from '../store.js';

export function useWebSocket() {
    const connected = useStore(s => s.wsConnected);
    const prices = useStore(s => s.livePriceUpdates);
    const alerts = useStore(s => s.liveAlerts);
    const recommendations = useStore(s => s.liveRecommendations);

    return {
        connected,
        lastMessage: null,
        prices,
        alerts,
        recommendations,
    };
}
