/**
 * CanvasStore — Zustand store for the Gotham Canvas investigation workspace.
 * Manages graphology Graph state, selection, layers, time filters, and board persistence.
 */

import { create } from 'zustand';
import Graph from 'graphology';

const useCanvasStore = create((set, get) => ({
    // ── Graph state ──
    graph: new Graph(),

    // ── Selection ──
    selectedNode: null, // { id, type } | null
    hoveredNode: null,  // string | null

    // ── Layers ──
    activeLayers: new Set(['financial', 'insider']),

    // ── Temporal filter ──
    timeRange: { start: null, end: null },

    // ── Board persistence ──
    boardId: null,
    boardName: 'Untitled Investigation',
    boards: [],

    // ── Detail panel ──
    detailPanelOpen: false,
    detailData: null,

    // ── Loading ──
    loading: false,

    // ── Context menu ──
    contextMenu: { x: 0, y: 0, node: null, visible: false },

    // ── Search ──
    searchQuery: '',

    // ── Actions ──

    /**
     * loadGraph — imports { nodes, edges } from API into graphology Graph.
     * Clears existing graph and replaces with fresh data.
     */
    loadGraph: (data) => {
        const g = new Graph();
        const { nodes = [], edges = [] } = _normalizeGraphPayload(data);

        nodes.forEach((node) => {
            const rawId = node.id || node.node_id || node.key;
            if (!rawId) return;
            const id = String(rawId);
            if (!g.hasNode(id)) {
                g.addNode(id, _nodeAttributes(node, id));
            }
        });

        edges.forEach((edge) => {
            const rawSource = edge.source || edge.from || edge.source_node_id;
            const rawTarget = edge.target || edge.to || edge.target_node_id;
            if (!rawSource || !rawTarget) return;
            const source = String(rawSource);
            const target = String(rawTarget);
            if (g.hasNode(source) && g.hasNode(target) && !g.hasEdge(source, target)) {
                g.addEdge(source, target, _edgeAttributes(edge));
            }
        });

        set({ graph: g, loading: false });
    },

    /**
     * addNodes — incremental expansion. Merges new nodes/edges into existing graph.
     */
    addNodes: (data) => {
        const g = get().graph.copy();
        const { nodes = [], edges = [] } = _normalizeGraphPayload(data);

        nodes.forEach((node) => {
            const rawId = node.id || node.node_id || node.key;
            if (!rawId) return;
            const id = String(rawId);
            if (!g.hasNode(id)) {
                g.addNode(id, _nodeAttributes(node, id));
            }
        });

        edges.forEach((edge) => {
            const rawSource = edge.source || edge.from || edge.source_node_id;
            const rawTarget = edge.target || edge.to || edge.target_node_id;
            if (!rawSource || !rawTarget) return;
            const source = String(rawSource);
            const target = String(rawTarget);
            if (g.hasNode(source) && g.hasNode(target) && !g.hasEdge(source, target)) {
                g.addEdge(source, target, _edgeAttributes(edge));
            }
        });

        set({ graph: g });
    },

    /**
     * removeNodes — hide nodes from graph by id array.
     */
    removeNodes: (ids) => {
        const g = get().graph.copy();
        ids.forEach((id) => {
            const nodeId = String(id);
            if (g.hasNode(nodeId)) {
                g.dropNode(nodeId);
            }
        });
        set({ graph: g });
    },

    /**
     * selectNode — set selected node, open detail panel.
     */
    selectNode: (id, type) => {
        set({
            selectedNode: id ? { id: String(id), type: type || 'actor' } : null,
            detailPanelOpen: !!id,
        });
    },

    /**
     * hoverNode — set hovered node id.
     */
    hoverNode: (id) => set({ hoveredNode: id ? String(id) : null }),

    /**
     * clearSelection — deselect node, close detail panel.
     */
    clearSelection: () => set({
        selectedNode: null,
        detailPanelOpen: false,
        detailData: null,
    }),

    /**
     * toggleLayer — add or remove a layer from activeLayers.
     */
    toggleLayer: (name) => {
        const layers = new Set(get().activeLayers);
        if (layers.has(name)) {
            layers.delete(name);
        } else {
            layers.add(name);
        }
        set({ activeLayers: layers });
    },

    /**
     * setTimeRange — set temporal filter bounds.
     */
    setTimeRange: (start, end) => set({ timeRange: { start, end } }),

    /**
     * saveBoard — serialize current graph + camera + filters for persistence.
     */
    saveBoard: () => {
        const { graph, boardId, boardName, activeLayers, timeRange } = get();
        const nodes = [];
        const edges = [];

        graph.forEachNode((id, attrs) => {
            nodes.push({ id, ...attrs });
        });
        graph.forEachEdge((id, attrs, source, target) => {
            edges.push({ id, source, target, ...attrs });
        });

        return {
            boardId,
            boardName,
            activeLayers: Array.from(activeLayers),
            timeRange,
            graph: { nodes, edges },
        };
    },

    /**
     * loadBoard — fetch and deserialize a saved board.
     */
    loadBoard: (boardData) => {
        if (!boardData) return;
        const { boardId, boardName, activeLayers, timeRange, graph } = boardData;
        set({
            boardId: boardId || null,
            boardName: boardName || 'Untitled Investigation',
            activeLayers: new Set(activeLayers || ['financial', 'insider']),
            timeRange: timeRange || { start: null, end: null },
        });
        get().loadGraph(graph);
    },

    /**
     * showContextMenu — position and show the context menu at (x, y) for a node.
     */
    showContextMenu: (x, y, nodeId) => set({
        contextMenu: { x, y, node: nodeId, visible: true },
    }),

    /**
     * hideContextMenu — dismiss context menu.
     */
    hideContextMenu: () => set({
        contextMenu: { x: 0, y: 0, node: null, visible: false },
    }),

    /**
     * setDetailData — populate the detail panel with node data.
     */
    setDetailData: (data) => set({ detailData: data }),

    /**
     * setLoading — toggle loading state.
     */
    setLoading: (val) => set({ loading: !!val }),

    /**
     * setSearchQuery — update the search query.
     */
    setSearchQuery: (q) => set({ searchQuery: q }),

    /**
     * setBoardName — rename the current board.
     */
    setBoardName: (name) => set({ boardName: name }),

    /**
     * setBoards — update the list of saved boards.
     */
    setBoards: (boards) => set({ boards }),
}));

// ── Node size helper ──
// Returns size in range 3-12px. Actors scale by influence, others are smaller.
// Mobile-friendly: nothing huge, everything readable.
function _nodeSize(node) {
    const type = node.type || 'actor';
    const inf = Math.min(node.influence || 0.3, 1.0); // clamp to 0-1
    switch (type) {
        case 'actor':
            // Range: 4-12px. Sovereign/high-influence actors are bigger.
            return Math.max(4, Math.min(12, inf * 12 + 2));
        case 'ticker':
            return 6; // fixed medium
        case 'signal':
            // Smaller, scale by confidence
            return Math.max(3, Math.min(7, (node.confidence || 0.5) * 6 + 2));
        case 'event':
            return 5;
        default:
            return 5;
    }
}

function _safeObject(value) {
    if (!value) return {};
    if (typeof value === 'string') {
        try {
            return JSON.parse(value);
        } catch {
            return {};
        }
    }
    return typeof value === 'object' ? value : {};
}

function _normalizeGraphPayload(data) {
    const payload = data || {};
    return {
        nodes: payload.nodes || payload.new_nodes || [],
        edges: payload.edges || payload.new_edges || [],
    };
}

function _nodeAttributes(node, id) {
    const data = _safeObject(node.data);
    const nodeType = node.type || node.nodeType || node.node_type || data.type || 'actor';
    const tier = node.tier || data.tier || 'individual';
    const category = node.category || data.category || null;
    const influence = node.influence || data.influence || data.influence_score || 0.3;
    const label = node.label || node.name || data.label || data.name || id;

    return {
        ...node.attributes,
        label,
        x: node.x ?? node.position_x ?? data.x ?? Math.random() * 1000,
        y: node.y ?? node.position_y ?? data.y ?? Math.random() * 1000,
        size: _nodeSize({ ...node, type: nodeType, tier, category, influence }),
        color: _nodeColor({ ...node, type: nodeType, tier, category }),
        // nodeType carries the domain kind. `type` is reserved by Sigma v3.
        nodeType,
        tier,
        category,
        influence,
        trust_score: node.trust_score || data.trust_score || data.trustScore,
        title: node.title || data.title || data.subtitle || '',
        entityId: node.entityId || node.entity_id || data.entityId || data.entity_id,
        ticker: node.ticker || data.ticker,
        direction: node.direction || data.direction,
        confidence: node.confidence || data.confidence,
        magnitude: node.magnitude || data.magnitude,
        source_type: node.source_type || data.source_type || data.sourceType,
        data: {
            ...data,
            id: data.id || node.id || node.node_id || id,
            label,
            name: data.name || node.name || label,
            nodeType,
            tier,
            category,
            influence,
        },
    };
}

function _edgeAttributes(edge) {
    const data = _safeObject(edge.data);
    const strength = edge.strength || edge.weight || data.strength || 0.3;
    return {
        ...edge.attributes,
        label: edge.label || data.label || '',
        color: _edgeColor(edge),
        size: strength * 3 + 0.5,
        // edgeKind carries the domain type. `type` is reserved by Sigma v3.
        edgeKind: edge.type || edge.edgeKind || edge.edge_type || data.type || 'connection',
        strength,
        data,
    };
}

// ── Node color helper ──
function _nodeColor(node) {
    const type = node.type || 'actor';
    switch (type) {
        case 'actor': {
            const tier = node.tier || 'individual';
            const tierColors = {
                sovereign: '#FFD700',
                regional: '#3B82F6',
                institutional: '#8B5CF6',
                individual: '#06B6D4',
            };
            return tierColors[tier] || '#06B6D4';
        }
        case 'ticker':
            return '#1A6EBF';
        case 'signal': {
            const dir = (node.direction || '').toLowerCase();
            return dir === 'bullish' ? '#10B981'
                : dir === 'bearish' ? '#EF4444'
                    : '#8AA0B8';
        }
        case 'news':
            return '#F97316';
        case 'hypothesis':
            return '#A855F7';
        case 'evidence':
            return '#F59E0B';
        case 'company':
            return '#1A6EBF';
        case 'event': {
            const cat = (node.category || '').toLowerCase();
            return cat === 'macro' ? '#F59E0B'
                : cat === 'earnings' ? '#3B82F6'
                    : '#8AA0B8';
        }
        default:
            return '#4A5568';
    }
}

// ── Edge color helper ──
function _edgeColor(edge) {
    const type = edge.type || edge.edgeKind || edge.edge_type || 'connection';
    switch (type) {
        case 'connection': return '#1A2332';
        case 'signal_link': return '#1A6EBF';
        case 'flow': return '#10B981';
        case 'co_traded': return '#8B5CF6';
        default: return '#1A2332';
    }
}

export default useCanvasStore;
