/**
 * Canvas store — investigation board state (boards, nodes, edges).
 */
import { create } from 'zustand';

const useCanvasStore = create((set, get) => ({
    // State
    boards: [],
    currentBoardId: null,
    nodes: [],
    edges: [],
    selectedNodeId: null,
    selectedEdgeId: null,
    loading: false,

    // Setters
    setBoards: (boards) => set({ boards }),
    setCurrentBoardId: (id) => set({ currentBoardId: id }),
    setNodes: (nodes) => set({ nodes }),
    setEdges: (edges) => set({ edges }),
    setSelectedNodeId: (id) => set({ selectedNodeId: id }),
    setSelectedEdgeId: (id) => set({ selectedEdgeId: id }),
    setLoading: (loading) => set({ loading }),

    // Node operations (immutable)
    updateNodePosition: (nodeId, position) => set((state) => ({
        nodes: state.nodes.map((n) =>
            n.id === nodeId ? { ...n, position: { ...position } } : n
        ),
    })),

    addNode: (node) => set((state) => ({
        nodes: [...state.nodes, node],
    })),

    removeNode: (nodeId) => set((state) => ({
        nodes: state.nodes.filter((n) => n.id !== nodeId),
        edges: state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
    })),

    // Edge operations (immutable)
    addEdge: (edge) => set((state) => ({
        edges: [...state.edges, edge],
    })),

    removeEdge: (edgeId) => set((state) => ({
        edges: state.edges.filter((e) => e.id !== edgeId),
    })),

    /**
     * Convert DB rows → React Flow format and set nodes/edges.
     * @param {{ nodes: Array, edges: Array }} graph
     */
    loadGraph: (graph) => {
        const rfNodes = (graph.nodes || []).map((n) => ({
            id: String(n.node_id),
            type: n.node_type,
            position: { x: n.position_x ?? 0, y: n.position_y ?? 0 },
            data: {
                label: n.label,
                entityId: n.entity_id,
                ...(n.data || {}),
            },
        }));
        const rfEdges = (graph.edges || []).map((e) => ({
            id: String(e.edge_id),
            source: String(e.source_node_id),
            target: String(e.target_node_id),
            type: e.edge_type || 'smoothstep',
            label: e.label || '',
        }));
        set({ nodes: rfNodes, edges: rfEdges });
    },

    /**
     * Convert current React Flow state → DB format for bulk save.
     * @returns {{ nodes: Array, edges: Array }}
     */
    toDbFormat: () => {
        const { nodes, edges } = get();
        return {
            nodes: nodes.map((n) => ({
                node_id: n.id,
                node_type: n.type || 'note',
                position_x: n.position?.x ?? 0,
                position_y: n.position?.y ?? 0,
                label: n.data?.label || '',
                entity_id: n.data?.entityId || null,
                data: { ...n.data },
            })),
            edges: edges.map((e) => ({
                edge_id: e.id,
                source_node_id: e.source,
                target_node_id: e.target,
                edge_type: e.type || 'smoothstep',
                label: e.label || '',
            })),
        };
    },
}));

export default useCanvasStore;
