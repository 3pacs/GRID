/**
 * GothamCanvas — Full-viewport investigation workspace.
 *
 * Layout:
 *   CommandBar (top)  — search, layers, time range, save, board name, connect dots
 *   SigmaGraph (main) — WebGL graph via Sigma.js
 *   DetailPanel (side) — rich node intelligence (360px, slides in)
 *   IntelFeed (bottom) — scrolling cross-reference intelligence findings
 *   ContextMenu       — right-click actions on nodes
 */

import React, { useEffect, useCallback, useRef, useState } from 'react';
import {
    Search, Save, X, GitBranch, Eye, EyeOff,
    ExternalLink, Trash2, Zap, AlertTriangle, Link2,
    DollarSign, UserCheck, Landmark, Shield, Globe,
    ChevronDown, ChevronUp, Workflow, Hexagon,
    Share2, Factory, Coins,
} from 'lucide-react';
import { colors, tokens, shared, glassMorphism } from '../styles/shared.js';
import { api } from '../api.js';
import useCanvasStore from './CanvasStore.js';
import SigmaGraph from './SigmaGraph.jsx';
import DetailPanel from './panels/DetailPanel.jsx';
import ContextMenu from './ContextMenu.jsx';
import LayerControls from './LayerControls.jsx';
import TemporalScrubber from './TemporalScrubber.jsx';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts.js';
import { useCommunities } from './hooks/useCommunities.js';

// ── Lens lenses — lazy-loaded to keep the graph bundle lean ──
const CanvasSupplyLens = React.lazy(() => import('../views/canvas_lenses/SupplyLens.jsx'));
const CanvasCapitalLens = React.lazy(() => import('../views/canvas_lenses/CapitalLens.jsx'));

// ── Lens constants ──
const LENS_GRAPH = 'graph';
const LENS_SUPPLY = 'supply';
const LENS_CAPITAL = 'capital';
const VALID_LENSES = new Set([LENS_GRAPH, LENS_SUPPLY, LENS_CAPITAL]);

// Parse `#/canvas[/{actorId}[/{lens}]]` → { actorId, lens }
function parseCanvasHash() {
    if (typeof window === 'undefined') return { actorId: null, lens: LENS_GRAPH };
    const raw = window.location.hash.slice(2) || '';
    const parts = raw.split('/').filter(Boolean);
    if (parts[0] !== 'canvas') return { actorId: null, lens: LENS_GRAPH };
    const actorId = parts[1] ? decodeURIComponent(parts[1]) : null;
    const lens = VALID_LENSES.has(parts[2]) ? parts[2] : LENS_GRAPH;
    return { actorId, lens };
}
function writeCanvasHash(actorId, lens) {
    if (typeof window === 'undefined') return;
    const parts = ['canvas'];
    if (actorId) parts.push(encodeURIComponent(actorId));
    if (lens && lens !== LENS_GRAPH) parts.push(lens);
    const target = `#/${parts.join('/')}`;
    if (window.location.hash !== target) window.location.hash = target;
}

// ── Design tokens ──
const MONO = colors.mono || "'IBM Plex Mono', monospace";
const SANS = colors.sans || "'IBM Plex Sans', sans-serif";
const CMD_HEIGHT = 48;

// ── Time range presets ──
const TIME_PRESETS = [
    { label: '7d', days: 7 },
    { label: '30d', days: 30 },
    { label: '90d', days: 90 },
    { label: '365d', days: 365 },
];

// ── Dot connection type icons + colors ──
const DOT_TYPES = {
    insider_cluster:       { icon: UserCheck,      color: '#F59E0B', label: 'Insider Cluster' },
    congressional_timing:  { icon: Landmark,       color: '#8B5CF6', label: 'Congressional Timing' },
    whale_convergence:     { icon: Zap,            color: '#10B981', label: 'Whale Convergence' },
    signal_divergence:     { icon: AlertTriangle,  color: '#EF4444', label: 'Signal Divergence' },
    board_interlock:       { icon: Link2,          color: '#3B82F6', label: 'Board Interlock' },
    money_trail:           { icon: DollarSign,     color: '#22C55E', label: 'Money Trail' },
    offshore_connection:   { icon: Globe,          color: '#EC4899', label: 'Offshore Connection' },
};

// ── Styles ──
const S = {
    workspace: {
        position: 'relative',
        height: 'calc(100vh - 56px)',
        display: 'grid',
        gridTemplateRows: `${CMD_HEIGHT}px 1fr auto`,
        background: colors.bg,
        overflow: 'hidden',
    },
    commandBar: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        padding: '0 16px',
        height: CMD_HEIGHT,
        background: colors.card,
        borderBottom: `1px solid ${colors.border}`,
        fontFamily: SANS,
        zIndex: 10,
        overflowX: 'auto',
        scrollbarWidth: 'none',
    },
    searchWrap: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        padding: '0 10px',
        height: '34px',
        minWidth: '180px',
        flex: '0 1 260px',
    },
    searchInput: {
        background: 'none',
        border: 'none',
        outline: 'none',
        color: colors.text,
        fontSize: '13px',
        fontFamily: MONO,
        width: '100%',
    },
    timePill: (active) => ({
        padding: '4px 8px',
        borderRadius: tokens.radius.sm,
        fontSize: '11px',
        fontWeight: 600,
        fontFamily: MONO,
        cursor: 'pointer',
        border: 'none',
        background: active ? colors.accent : 'transparent',
        color: active ? '#fff' : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
    }),
    actionBtn: (accent) => ({
        display: 'flex',
        alignItems: 'center',
        gap: '6px',
        padding: '6px 12px',
        borderRadius: tokens.radius.sm,
        fontSize: '11px',
        fontWeight: 600,
        fontFamily: MONO,
        cursor: 'pointer',
        border: `1px solid ${accent ? colors.accent : colors.border}`,
        background: accent ? `${colors.accent}18` : 'transparent',
        color: accent ? colors.accentLight : colors.textDim,
        transition: `all ${tokens.transition.fast}`,
        whiteSpace: 'nowrap',
    }),
    boardName: {
        fontSize: '13px',
        fontWeight: 600,
        color: colors.text,
        fontFamily: SANS,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        maxWidth: '180px',
    },
    mainArea: {
        position: 'relative',
        overflow: 'hidden',
    },
    graphContainer: {
        position: 'absolute',
        inset: 0,
        overflow: 'hidden',
    },
    loadingOverlay: {
        position: 'absolute',
        inset: 0,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'rgba(8, 12, 16, 0.7)',
        zIndex: 30,
    },
    loadingText: {
        fontSize: '14px',
        color: colors.textDim,
        fontFamily: MONO,
    },
    emptyState: {
        position: 'absolute',
        inset: 0,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '16px',
        zIndex: 5,
        pointerEvents: 'none',
    },
    emptyIcon: {
        width: 64,
        height: 64,
        borderRadius: '50%',
        background: `${colors.accent}15`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
    },
    emptyTitle: {
        fontSize: '18px',
        fontWeight: 700,
        color: colors.text,
        fontFamily: SANS,
    },
    emptyDesc: {
        fontSize: '13px',
        color: colors.textMuted,
        fontFamily: SANS,
        textAlign: 'center',
        maxWidth: '380px',
        lineHeight: '1.5',
    },
    emptyAction: {
        ...shared.buttonSmall,
        pointerEvents: 'auto',
        marginTop: '8px',
    },
    // ── Intel Feed (bottom) ──
    intelFeed: (expanded) => ({
        background: colors.card,
        borderTop: `1px solid ${colors.border}`,
        maxHeight: expanded ? '240px' : '40px',
        minHeight: '40px',
        transition: `max-height ${tokens.transition.slow}`,
        overflow: 'hidden',
        zIndex: 15,
    }),
    intelHeader: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        padding: '8px 16px',
        cursor: 'pointer',
        height: '40px',
    },
    intelTitle: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: MONO,
    },
    intelCount: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textMuted,
        background: colors.bg,
        padding: '2px 8px',
        borderRadius: tokens.radius.sm,
    },
    intelBody: {
        padding: '0 16px 12px',
        overflowY: 'auto',
        maxHeight: '190px',
        display: 'flex',
        flexDirection: 'column',
        gap: '6px',
    },
    dotCard: (typeColor) => ({
        display: 'flex',
        alignItems: 'flex-start',
        gap: '10px',
        padding: '10px 12px',
        background: colors.bg,
        border: `1px solid ${typeColor}22`,
        borderLeft: `3px solid ${typeColor}`,
        borderRadius: tokens.radius.sm,
        fontSize: '12px',
        fontFamily: SANS,
        color: colors.textDim,
        lineHeight: '1.5',
    }),
    dotBadge: (color) => ({
        fontSize: '9px',
        fontWeight: 700,
        fontFamily: MONO,
        letterSpacing: '0.5px',
        color: color,
        background: `${color}15`,
        padding: '2px 6px',
        borderRadius: tokens.radius.sm,
        whiteSpace: 'nowrap',
        flexShrink: 0,
    }),
    dotConfidence: (conf) => ({
        fontSize: '10px',
        fontFamily: MONO,
        color: conf >= 0.8 ? colors.green : conf >= 0.5 ? colors.yellow : colors.red,
        flexShrink: 0,
    }),
    dotActors: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textMuted,
        marginTop: '2px',
    },
    countBadge: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textMuted,
        padding: '3px 8px',
        background: colors.bg,
        borderRadius: tokens.radius.sm,
        border: `1px solid ${colors.borderSubtle}`,
        whiteSpace: 'nowrap',
    },
    // ── Lens switcher ──
    lensGroup: {
        display: 'flex',
        gap: '2px',
        padding: '2px',
        borderRadius: tokens.radius.sm,
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        flexShrink: 0,
    },
    lensBtn: (active) => ({
        display: 'flex',
        alignItems: 'center',
        gap: '5px',
        padding: '5px 10px',
        borderRadius: tokens.radius.sm,
        fontSize: '11px',
        fontWeight: 600,
        fontFamily: MONO,
        cursor: 'pointer',
        border: 'none',
        background: active ? colors.accent : 'transparent',
        color: active ? '#fff' : colors.textDim,
        transition: `all ${tokens.transition.fast}`,
        whiteSpace: 'nowrap',
    }),
    lensShell: {
        position: 'absolute',
        inset: 0,
        overflow: 'hidden',
        background: colors.bg,
    },
};

// ── Mobile detection ──
function useIsMobile() {
    const [mobile, setMobile] = useState(
        typeof window !== 'undefined' ? window.innerWidth < 768 : false,
    );
    useEffect(() => {
        const h = () => setMobile(window.innerWidth < 768);
        window.addEventListener('resize', h);
        return () => window.removeEventListener('resize', h);
    }, []);
    return mobile;
}

export default function GothamCanvas() {
    const store = useCanvasStore();
    const {
        graph, selectedNode, detailPanelOpen, detailData, loading,
        contextMenu, activeLayers, boardName, searchQuery,
        loadGraph, addNodes, selectNode, clearSelection, toggleLayer,
        setTimeRange, hideContextMenu, showContextMenu,
    } = store;

    const isMobile = useIsMobile();

    // Local state
    const [activeTime, setActiveTime] = useState(null);
    const [editingName, setEditingName] = useState(false);
    const [dots, setDots] = useState([]);         // cross-reference intelligence
    const [dotsLoading, setDotsLoading] = useState(false);
    const [feedExpanded, setFeedExpanded] = useState(false);
    const [showCommunities, setShowCommunities] = useState(false);
    const nameInputRef = useRef(null);
    const sigmaRef = useRef(null);

    // ── Lens switcher state (graph | supply | capital) ──
    // Initial values come from `#/canvas/{actorId}/{lens}` hash.
    const [lens, setLensState] = useState(() => parseCanvasHash().lens);
    const [lensActorId, setLensActorId] = useState(() => parseCanvasHash().actorId);

    // Keep lensActorId in sync with whichever node is selected in the graph.
    useEffect(() => {
        if (selectedNode?.id) setLensActorId(selectedNode.id);
    }, [selectedNode]);

    // Mirror lens + focal actor to URL hash.
    useEffect(() => {
        writeCanvasHash(lensActorId, lens);
    }, [lens, lensActorId]);

    // Parse hash changes (back/forward, manual edit) so the view stays consistent.
    useEffect(() => {
        const h = () => {
            const { actorId, lens: nextLens } = parseCanvasHash();
            if (nextLens !== lens) setLensState(nextLens);
            if (actorId && actorId !== lensActorId) setLensActorId(actorId);
        };
        window.addEventListener('hashchange', h);
        return () => window.removeEventListener('hashchange', h);
    }, [lens, lensActorId]);

    const setLens = useCallback((next) => {
        if (!VALID_LENSES.has(next)) return;
        setLensState(next);
    }, []);

    // Focal actor object for lenses (id + label when available).
    const focalActor = lensActorId
        ? {
            id: lensActorId,
            label: (graph.hasNode(lensActorId) && graph.getNodeAttributes(lensActorId)?.label) || lensActorId,
            type: (graph.hasNode(lensActorId) && (graph.getNodeAttributes(lensActorId)?.nodeType || graph.getNodeAttributes(lensActorId)?.type)) || 'actor',
        }
        : null;

    // Community detection
    const { communities, communityColors, communityLabels } = useCommunities(graph);

    // Wire keyboard shortcuts
    useKeyboardShortcuts({
        sigmaRef,
        selectedNode,
        onDeselect: clearSelection,
        onClosePanel: clearSelection,
        onHideSelected: () => {
            if (selectedNode) {
                useCanvasStore.getState().removeNodes([selectedNode.id]);
                clearSelection();
            }
        },
        onExpandSelected: () => {
            if (!selectedNode) return;
            const existingIds = [];
            graph.forEachNode((id) => existingIds.push(id));
            api.expandNode(selectedNode.type || 'actor', selectedNode.id, 1, existingIds)
                .then((data) => { if (data && !data.error) addNodes(data); });
        },
        onToggleLayer: toggleLayer,
        onToggleCommunities: () => setShowCommunities((prev) => !prev),
        onSetLens: setLens,
    });

    // ── Initial load ──
    useEffect(() => {
        let cancelled = false;
        async function load() {
            useCanvasStore.getState().setLoading(true);
            try {
                const data = await api.getCanvasGraph('all', 2, 'all', null, 500);
                if (!cancelled && data && !data.error) {
                    loadGraph(data);
                }
            } catch (e) {
                // silenced — loading state handles UX
            } finally {
                if (!cancelled) useCanvasStore.getState().setLoading(false);
            }
        }
        load();
        return () => { cancelled = true; };
    }, []);

    // ── Connect Dots: fetch cross-reference intelligence ──
    const connectDots = useCallback(async (center) => {
        setDotsLoading(true);
        try {
            const data = await api.getCanvasDots(center || 'all');
            if (data && !data.error && data.connections) {
                setDots(data.connections);
                setFeedExpanded(true);
            }
        } catch (e) {
            // silenced
        } finally {
            setDotsLoading(false);
        }
    }, []);

    // Auto-connect dots when graph loads
    useEffect(() => {
        if (graph.order > 0 && dots.length === 0 && !dotsLoading) {
            connectDots('all');
        }
    }, [graph.order]);

    // ── Expand node handler (double-click) ──
    useEffect(() => {
        const handler = async (e) => {
            const { nodeId, nodeType } = e.detail;
            try {
                const existingIds = [];
                graph.forEachNode((id) => existingIds.push(id));
                const data = await api.expandNode(nodeType, nodeId, 1, existingIds);
                if (data && !data.error) {
                    addNodes(data);
                }
            } catch (err) {
                // silenced
            }
        };
        window.addEventListener('canvas:expandNode', handler);
        return () => window.removeEventListener('canvas:expandNode', handler);
    }, [graph, addNodes]);

    // ── Fetch detail data when node is selected ──
    useEffect(() => {
        if (!selectedNode) {
            useCanvasStore.getState().setDetailData(null);
            return;
        }
        let cancelled = false;
        async function fetchDetail() {
            try {
                const data = await api.getNodeDetail(selectedNode.type, selectedNode.id);
                if (!cancelled && data && !data.error) {
                    useCanvasStore.getState().setDetailData(data);
                }
            } catch (e) {
                // silenced
            }
        }
        fetchDetail();
        return () => { cancelled = true; };
    }, [selectedNode]);

    // ── Time range handler ──
    const handleTimePreset = useCallback((days) => {
        setActiveTime(days);
        const end = new Date().toISOString();
        const start = new Date(Date.now() - days * 86400000).toISOString();
        setTimeRange(start, end);
    }, [setTimeRange]);

    // ── Board name editing ──
    const startEditName = () => {
        setEditingName(true);
        setTimeout(() => nameInputRef.current?.focus(), 50);
    };
    const finishEditName = () => setEditingName(false);

    // ── Search ──
    const handleSearchSubmit = useCallback(async (e) => {
        if (e.key !== 'Enter' || !searchQuery.trim()) return;
        useCanvasStore.getState().setLoading(true);
        try {
            const data = await api.getCanvasGraph(searchQuery.trim(), 2, 'all', null, 200);
            if (data && !data.error) {
                loadGraph(data);
                // Also connect dots for new search
                connectDots(searchQuery.trim());
            }
        } catch (err) {
            // silenced
        } finally {
            useCanvasStore.getState().setLoading(false);
        }
    }, [searchQuery, loadGraph, connectDots]);

    // ── Context menu actions ──
    const handleContextAction = useCallback(async (action) => {
        const nodeId = contextMenu.node;
        hideContextMenu();
        if (!nodeId) return;
        const attrs = graph.hasNode(nodeId) ? graph.getNodeAttributes(nodeId) : {};

        switch (action) {
            case 'details':
                selectNode(nodeId, attrs.nodeType || attrs.type || 'actor');
                break;
            case 'expand':
            case 'expandDeep': {
                const depth = action === 'expandDeep' ? 3 : 1;
                const existingIds = [];
                graph.forEachNode((id) => existingIds.push(id));
                try {
                    const data = await api.expandNode(attrs.nodeType || attrs.type || 'actor', nodeId, depth, existingIds);
                    if (data && !data.error) addNodes(data);
                } catch (e) {
                    // silenced
                }
                break;
            }
            case 'dots':
                connectDots(nodeId);
                break;
            case 'remove':
                useCanvasStore.getState().removeNodes([nodeId]);
                break;
            default:
                break;
        }
    }, [contextMenu, graph, hideContextMenu, selectNode, addNodes, connectDots]);

    const nodeCount = graph.order;
    const edgeCount = graph.size;

    return (
        <div style={S.workspace}>
            {/* ══ Command Bar ══ */}
            <div style={{
                ...S.commandBar,
                gap: isMobile ? '6px' : '10px',
                padding: isMobile ? '0 8px' : '0 16px',
            }}>
                {/* Search — always visible, flex to fill on mobile */}
                <div style={{
                    ...S.searchWrap,
                    flex: isMobile ? '1 1 120px' : '0 1 260px',
                    minWidth: isMobile ? '100px' : '180px',
                }}>
                    <Search size={14} color={colors.textMuted} />
                    <input
                        style={S.searchInput}
                        placeholder={isMobile ? "Search..." : "Search actors, tickers... (Enter)"}
                        value={searchQuery}
                        onChange={(e) => useCanvasStore.getState().setSearchQuery(e.target.value)}
                        onKeyDown={handleSearchSubmit}
                        spellCheck={false}
                    />
                </div>

                {/* Layer controls — hidden on mobile */}
                {!isMobile && (
                    <LayerControls
                        activeLayers={activeLayers}
                        onToggleLayer={toggleLayer}
                    />
                )}

                {/* Time range — hidden on mobile */}
                {!isMobile && (
                    <div style={{ display: 'flex', gap: '2px', alignItems: 'center', flexShrink: 0 }}>
                        {TIME_PRESETS.map((preset) => (
                            <button
                                key={preset.days}
                                style={S.timePill(activeTime === preset.days)}
                                onClick={() => handleTimePreset(preset.days)}
                            >
                                {preset.label}
                            </button>
                        ))}
                    </div>
                )}

                {/* Lens switcher — Graph / Supply / Capital */}
                <div style={S.lensGroup} role="group" aria-label="Canvas lens">
                    <button
                        style={S.lensBtn(lens === LENS_GRAPH)}
                        onClick={() => setLens(LENS_GRAPH)}
                        title="Graph lens (G)">
                        <Share2 size={12} />
                        {!isMobile && 'Graph'}
                    </button>
                    <button
                        style={S.lensBtn(lens === LENS_SUPPLY)}
                        onClick={() => setLens(LENS_SUPPLY)}
                        title="Supply chain lens (S)">
                        <Factory size={12} />
                        {!isMobile && 'Supply'}
                    </button>
                    <button
                        style={S.lensBtn(lens === LENS_CAPITAL)}
                        onClick={() => setLens(LENS_CAPITAL)}
                        title="Capital flow lens (F)">
                        <Coins size={12} />
                        {!isMobile && 'Capital'}
                    </button>
                </div>

                {/* Connect Dots button — icon-only on mobile */}
                <button
                    style={S.actionBtn(true)}
                    onClick={() => connectDots(selectedNode?.id || 'all')}
                    disabled={dotsLoading}
                >
                    <Zap size={13} />
                    {!isMobile && (dotsLoading ? 'Connecting...' : 'Dots')}
                </button>

                {/* Community hulls toggle — icon-only on mobile */}
                <button
                    style={S.actionBtn(showCommunities)}
                    onClick={() => setShowCommunities((prev) => !prev)}
                    title="Toggle community clusters (C)"
                >
                    <Hexagon size={13} />
                    {!isMobile && `Clusters${communities.size > 0 ? ` (${communities.size})` : ''}`}
                </button>

                {/* Spacer */}
                <div style={{ flex: isMobile ? 0 : 1 }} />

                {/* Save — hidden on mobile */}
                {!isMobile && (
                    <button
                        style={S.actionBtn(false)}
                        onClick={async () => {
                            try {
                                const state = useCanvasStore.getState();
                                const graphState = state.graph.export();
                                if (state.boardId) {
                                    await api.saveBoard(state.boardId, {
                                        graph_state: graphState,
                                        filters: { layers: [...state.activeLayers], timeRange: state.timeRange },
                                    });
                                } else {
                                    const result = await api.createBoard(state.boardName);
                                    if (result && result.id) {
                                        useCanvasStore.setState({ boardId: result.id });
                                        await api.saveBoard(result.id, {
                                            graph_state: graphState,
                                            filters: { layers: [...state.activeLayers], timeRange: state.timeRange },
                                        });
                                    }
                                }
                            } catch (e) {
                                // silenced
                            }
                        }}
                    >
                        <Save size={13} />
                        Save
                    </button>
                )}

                {/* Board name — hidden on mobile */}
                {!isMobile && (editingName ? (
                    <input
                        ref={nameInputRef}
                        style={{
                            ...S.boardName,
                            background: colors.bg,
                            border: `1px solid ${colors.accent}`,
                            borderRadius: tokens.radius.sm,
                            padding: '2px 8px',
                            outline: 'none',
                        }}
                        value={boardName}
                        onChange={(e) => useCanvasStore.setState({ boardName: e.target.value })}
                        onBlur={finishEditName}
                        onKeyDown={(e) => e.key === 'Enter' && finishEditName()}
                    />
                ) : (
                    <span
                        style={{ ...S.boardName, cursor: 'pointer' }}
                        onClick={startEditName}
                        title="Click to rename"
                    >
                        {boardName}
                    </span>
                ))}

                {/* Count badge */}
                <span style={S.countBadge}>
                    {nodeCount}n / {edgeCount}e
                </span>
            </div>

            {/* ══ Main Area (Graph + Panels) ══ */}
            <div style={S.mainArea}>
                {/* Supply + Capital lenses — rendered on top of the graph area */}
                {lens !== LENS_GRAPH && (
                    <div style={S.lensShell}>
                        <React.Suspense fallback={
                            <div style={S.loadingOverlay}>
                                <div style={S.loadingText}>Loading {lens} lens…</div>
                            </div>
                        }>
                            {focalActor ? (
                                lens === LENS_SUPPLY
                                    ? <CanvasSupplyLens actor={focalActor} onFocus={(id) => setLensActorId(id)} />
                                    : <CanvasCapitalLens actor={focalActor} />
                            ) : (
                                <div style={S.emptyState}>
                                    <div style={S.emptyIcon}>
                                        {lens === LENS_SUPPLY ? <Factory size={28} color={colors.accent} /> : <Coins size={28} color={colors.accent} />}
                                    </div>
                                    <div style={S.emptyTitle}>{lens === LENS_SUPPLY ? 'Supply Chain Lens' : 'Capital Flow Lens'}</div>
                                    <div style={S.emptyDesc}>
                                        Select an actor in the graph lens first, then switch back here.
                                    </div>
                                    <button style={S.emptyAction} onClick={() => setLens(LENS_GRAPH)}>
                                        Back to graph
                                    </button>
                                </div>
                            )}
                        </React.Suspense>
                    </div>
                )}

                {/* Graph */}
                <div style={{ ...S.graphContainer, visibility: lens === LENS_GRAPH ? 'visible' : 'hidden' }}>
                    {nodeCount > 0 ? (
                        <SigmaGraph
                            ref={sigmaRef}
                            communities={communities}
                            communityColors={communityColors}
                            communityLabels={communityLabels}
                            showCommunities={showCommunities}
                        />
                    ) : !loading ? (
                        <div style={S.emptyState}>
                            <div style={S.emptyIcon}>
                                <Workflow size={28} color={colors.accent} />
                            </div>
                            <div style={S.emptyTitle}>Investigation Canvas</div>
                            <div style={S.emptyDesc}>
                                Search for an actor, ticker, or event to begin.
                                Double-click nodes to expand. Right-click for actions.
                                Press <span style={{ fontFamily: MONO, color: colors.accent }}>Cmd+K</span> for command palette.
                            </div>
                            <button
                                style={S.emptyAction}
                                onClick={async () => {
                                    useCanvasStore.getState().setLoading(true);
                                    try {
                                        const data = await api.getCanvasGraph('all', 2, 'all', null, 300);
                                        if (data && !data.error) loadGraph(data);
                                    } catch (e) {
                                        // silenced
                                    } finally {
                                        useCanvasStore.getState().setLoading(false);
                                    }
                                }}
                            >
                                Load Power Map
                            </button>
                        </div>
                    ) : null}
                </div>

                {/* Loading overlay */}
                {loading && (
                    <div style={S.loadingOverlay}>
                        <div style={S.loadingText}>Mapping intelligence network...</div>
                    </div>
                )}

                {/* Detail Panel — side panel on desktop, bottom sheet on mobile */}
                {detailPanelOpen && detailData && (
                    <div style={isMobile ? {
                        position: 'absolute', bottom: 0, left: 0, right: 0,
                        maxHeight: '60vh', zIndex: 100,
                        borderTop: `1px solid ${colors.border}`,
                        borderRadius: '14px 14px 0 0',
                        overflow: 'hidden',
                    } : undefined}>
                    <DetailPanel
                        node={{
                            ...detailData,
                            type: selectedNode?.type || 'actor',
                            id: selectedNode?.id,
                        }}
                        onClose={clearSelection}
                        onExpand={() => {
                            if (!selectedNode) return;
                            const existingIds = [];
                            graph.forEachNode((id) => existingIds.push(id));
                            api.expandNode(selectedNode.type || 'actor', selectedNode.id, 1, existingIds)
                                .then((d) => { if (d && !d.error) addNodes(d); });
                        }}
                        onInvestigate={() => {
                            if (selectedNode) connectDots(selectedNode.id);
                        }}
                        onHide={() => {
                            if (selectedNode) {
                                useCanvasStore.getState().removeNodes([selectedNode.id]);
                                clearSelection();
                            }
                        }}
                        onPin={() => {
                            // Pin node position (stop force layout from moving it)
                            if (selectedNode && graph.hasNode(selectedNode.id)) {
                                const attrs = graph.getNodeAttributes(selectedNode.id);
                                graph.setNodeAttribute(selectedNode.id, 'fixed', !attrs.fixed);
                            }
                        }}
                    />
                    </div>
                )}

                {/* Context Menu — rich component */}
                {contextMenu.visible && (
                    <ContextMenu
                        x={contextMenu.x}
                        y={contextMenu.y}
                        node={contextMenu.node && graph.hasNode(contextMenu.node)
                            ? { id: contextMenu.node, ...graph.getNodeAttributes(contextMenu.node) }
                            : { id: contextMenu.node, type: 'actor' }}
                        onAction={handleContextAction}
                        onClose={hideContextMenu}
                    />
                )}
            </div>

            {/* ══ Intelligence Feed (bottom) ══ */}
            <div style={{
                ...S.intelFeed(feedExpanded),
                maxHeight: feedExpanded ? (isMobile ? '160px' : '240px') : '36px',
                minHeight: '36px',
            }}>
                <div
                    style={S.intelHeader}
                    onClick={() => setFeedExpanded(!feedExpanded)}
                >
                    <Zap size={13} color={colors.accent} />
                    <span style={S.intelTitle}>CONNECTED DOTS</span>
                    <span style={S.intelCount}>
                        {dots.length} finding{dots.length !== 1 ? 's' : ''}
                    </span>
                    {dotsLoading && (
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                            scanning...
                        </span>
                    )}
                    <div style={{ flex: 1 }} />
                    {feedExpanded
                        ? <ChevronDown size={14} color={colors.textMuted} />
                        : <ChevronUp size={14} color={colors.textMuted} />}
                </div>

                {feedExpanded && dots.length > 0 && (
                    <div style={S.intelBody}>
                        {dots.map((dot, i) => {
                            const dotType = DOT_TYPES[dot.type] || DOT_TYPES.insider_cluster;
                            const Icon = dotType.icon;
                            return (
                                <div key={i} style={S.dotCard(dotType.color)}>
                                    <Icon size={16} color={dotType.color} style={{ flexShrink: 0, marginTop: '1px' }} />
                                    <div style={{ flex: 1, minWidth: 0 }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
                                            <span style={S.dotBadge(dotType.color)}>{dotType.label}</span>
                                            <span style={S.dotConfidence(dot.confidence || 0)}>
                                                {((dot.confidence || 0) * 100).toFixed(0)}% conf
                                            </span>
                                        </div>
                                        <div style={{ color: colors.text, fontSize: '12px' }}>
                                            {dot.description || 'Cross-reference detected'}
                                        </div>
                                        {dot.actors?.length > 0 && (
                                            <div style={S.dotActors}>
                                                {dot.actors.slice(0, 5).join(' → ')}
                                                {dot.actors.length > 5 && ` +${dot.actors.length - 5} more`}
                                            </div>
                                        )}
                                        {dot.evidence?.length > 0 && (
                                            <div style={{ marginTop: '4px' }}>
                                                {dot.evidence.slice(0, 2).map((ev, j) => (
                                                    <div key={j} style={{
                                                        fontSize: '10px',
                                                        color: colors.textMuted,
                                                        fontFamily: MONO,
                                                        padding: '2px 0',
                                                    }}>
                                                        {typeof ev === 'string' ? ev : ev.description || JSON.stringify(ev)}
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                )}
            </div>
        </div>
    );
}
