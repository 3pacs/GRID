import { describe, it, expect, beforeEach } from 'vitest';
import Graph from 'graphology';

const { default: useCanvasStore, activityKeysFromEvent, ACTIVITY_SIZE_BOOST } = await import('../canvas/CanvasStore.js');

describe('CanvasStore live activity (contracts → SSE → pulse)', () => {
    beforeEach(() => {
        useCanvasStore.setState({ graph: new Graph(), activity: {}, lastActivityEvent: null });
    });

    it('extracts identity keys from a contract payload, lowercased and de-duplicated', () => {
        const keys = activityKeysFromEvent({
            channel: 'grid_contracts_actor_materialized',
            payload: { ticker: 'NVDA', actor_id: 'A1', aliases: ['Nvidia Corp', ' NVDA '], canonical_name: 'NVIDIA' },
        });
        expect([...keys].sort()).toEqual(['a1', 'nvda', 'nvidia', 'nvidia corp']);
        expect(activityKeysFromEvent({}).size).toBe(0);
        expect(activityKeysFromEvent(null).size).toBe(0);
    });

    it('pulses matching nodes in place without replacing the graph', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'n1', ticker: 'NVDA', label: 'NVIDIA' },
                { id: 'n2', label: 'Unrelated' },
            ],
        });
        const graphBefore = useCanvasStore.getState().graph;
        const baseSize = graphBefore.getNodeAttributes('n1').size;

        const hits = useCanvasStore.getState().markActivity({
            channel: 'grid_contracts_signal_fired',
            timestamp: '2026-09-10T00:00:00Z',
            payload: { event_id: 'e1', ticker: 'nvda', signal_type: 'cluster_buy' },
        });

        expect(hits).toEqual(['n1']);
        // Same graphology instance: Sigma re-renders from attribute events and
        // ForceAtlas2 is not re-run, so positions do not jump on a pulse.
        expect(useCanvasStore.getState().graph).toBe(graphBefore);
        const n1 = graphBefore.getNodeAttributes('n1');
        expect(n1.highlighted).toBe(true);
        expect(n1.baseSize).toBe(baseSize);
        expect(n1.size).toBeCloseTo(baseSize * ACTIVITY_SIZE_BOOST, 6);
        expect(n1.activityChannel).toBe('grid_contracts_signal_fired');
        expect(graphBefore.getNodeAttributes('n2').highlighted).toBeFalsy();

        const state = useCanvasStore.getState();
        expect(state.activity.n1.count).toBe(1);
        expect(state.lastActivityEvent.matched).toEqual(['n1']);

        // A second pulse increments the count and keeps the base size stable.
        useCanvasStore.getState().markActivity({ payload: { actor_id: 'n1' } });
        expect(useCanvasStore.getState().activity.n1.count).toBe(2);
        expect(graphBefore.getNodeAttributes('n1').baseSize).toBe(baseSize);
    });

    it('matches on entity id and data name as well as ticker', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'a-77', entity_id: 'ACT-77', label: 'Jerome Powell', data: { name: 'Jerome Powell' } },
            ],
        });
        expect(useCanvasStore.getState().markActivity({ payload: { actor_id: 'act-77' } })).toEqual(['a-77']);
        expect(useCanvasStore.getState().markActivity({ payload: { canonical_name: 'jerome powell' } })).toEqual(['a-77']);
    });

    it('ignores events that name nothing on the board', () => {
        useCanvasStore.getState().loadGraph({ nodes: [{ id: 'n1', ticker: 'NVDA' }] });
        expect(useCanvasStore.getState().markActivity({ payload: { ticker: 'AMD' } })).toEqual([]);
        expect(useCanvasStore.getState().activity).toEqual({});
        expect(useCanvasStore.getState().lastActivityEvent).toBeNull();
    });

    it('decays expired pulses back to the base size', () => {
        useCanvasStore.getState().loadGraph({ nodes: [{ id: 'n1', ticker: 'NVDA' }] });
        const g = useCanvasStore.getState().graph;
        const baseSize = g.getNodeAttributes('n1').size;
        useCanvasStore.getState().markActivity({ payload: { ticker: 'NVDA' } });

        // Still fresh: nothing decays.
        expect(useCanvasStore.getState().decayActivity()).toBe(false);
        expect(g.getNodeAttributes('n1').highlighted).toBe(true);

        // Force expiry.
        expect(useCanvasStore.getState().decayActivity(0)).toBe(true);
        const attrs = g.getNodeAttributes('n1');
        expect(attrs.size).toBe(baseSize);
        expect(attrs.highlighted).toBe(false);
        expect(attrs.activityAt).toBeNull();
        expect(useCanvasStore.getState().activity).toEqual({});
    });
});
