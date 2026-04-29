import { describe, it, expect, beforeEach } from 'vitest';
import Graph from 'graphology';

const { default: useCanvasStore } = await import('../canvas/CanvasStore.js');

describe('CanvasStore graph normalization', () => {
    beforeEach(() => {
        useCanvasStore.setState({
            graph: new Graph(),
            selectedNode: null,
            detailPanelOpen: false,
            detailData: null,
            boardId: null,
            boardName: 'Untitled Investigation',
            visibleDepth: 4,
        });
    });

    it('loads board-shaped nodes and edges', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { node_id: 'a:corp_nvda', node_type: 'actor', label: 'NVIDIA', data: { trust_score: 0.82 } },
                { node_id: 't:NVDA', node_type: 'ticker', label: 'NVDA', data: { ticker: 'NVDA' } },
            ],
            edges: [
                {
                    source_node_id: 'a:corp_nvda',
                    target_node_id: 't:NVDA',
                    edge_type: 'signal_link',
                    strength: 0.7,
                },
            ],
        });

        const graph = useCanvasStore.getState().graph;
        expect(graph.hasNode('a:corp_nvda')).toBe(true);
        expect(graph.getNodeAttribute('a:corp_nvda', 'nodeType')).toBe('actor');
        expect(graph.getNodeAttribute('t:NVDA', 'nodeType')).toBe('ticker');
        expect(graph.size).toBe(1);
    });

    it('adds expand payload nodes with a new graph reference', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [{ id: 'a:corp_nvda', type: 'actor', label: 'NVIDIA' }],
            edges: [],
        });
        const before = useCanvasStore.getState().graph;

        useCanvasStore.getState().addNodes({
            new_nodes: [
                { node_id: 's:123', node_type: 'signal', label: 'Insider buy', confidence: 0.91 },
            ],
            new_edges: [
                {
                    source_node_id: 'a:corp_nvda',
                    target_node_id: 's:123',
                    edge_type: 'signal_link',
                    weight: 0.8,
                },
            ],
        });

        const after = useCanvasStore.getState().graph;
        expect(after).not.toBe(before);
        expect(after.hasNode('s:123')).toBe(true);
        expect(after.getNodeAttribute('s:123', 'nodeType')).toBe('signal');
        expect(after.getNodeAttribute('s:123', 'confidence')).toBe(0.91);
        expect(after.size).toBe(1);
    });

    it('hides nodes beyond the visible depth while keeping them loaded', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'a:root', type: 'actor', label: 'Root', graph_depth: 0 },
                { id: 'a:near', type: 'actor', label: 'Near', graph_depth: 3 },
                { id: 'a:deep', type: 'actor', label: 'Deep', graph_depth: 5 },
            ],
            edges: [
                { source: 'a:root', target: 'a:near', type: 'connection', strength: 0.6 },
                { source: 'a:near', target: 'a:deep', type: 'connection', strength: 0.6 },
            ],
        });

        const graph = useCanvasStore.getState().graph;
        expect(graph.hasNode('a:deep')).toBe(true);
        expect(graph.getNodeAttribute('a:root', 'hidden')).toBe(false);
        expect(graph.getNodeAttribute('a:near', 'hidden')).toBe(false);
        expect(graph.getNodeAttribute('a:deep', 'hidden')).toBe(true);

        const deepEdge = graph.edge('a:near', 'a:deep');
        expect(graph.getEdgeAttribute(deepEdge, 'hidden')).toBe(true);
    });

    it('updates hidden state when the surfaced depth changes', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'a:root', type: 'actor', label: 'Root', graph_depth: 0 },
                { id: 'a:deep', type: 'actor', label: 'Deep', graph_depth: 5 },
            ],
            edges: [
                { source: 'a:root', target: 'a:deep', type: 'connection', strength: 0.6 },
            ],
        });

        useCanvasStore.getState().setVisibleDepth(6);
        let graph = useCanvasStore.getState().graph;
        let deepEdge = graph.edge('a:root', 'a:deep');
        expect(graph.getNodeAttribute('a:deep', 'hidden')).toBe(false);
        expect(graph.getEdgeAttribute(deepEdge, 'hidden')).toBe(false);

        useCanvasStore.getState().setVisibleDepth(3);
        graph = useCanvasStore.getState().graph;
        deepEdge = graph.edge('a:root', 'a:deep');
        expect(graph.getNodeAttribute('a:deep', 'hidden')).toBe(true);
        expect(graph.getEdgeAttribute(deepEdge, 'hidden')).toBe(true);
    });
});
