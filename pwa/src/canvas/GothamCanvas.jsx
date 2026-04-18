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
function _stripCanvasPrefix(id) {
    if (!id) return null;
    for (const pfx of ['a:corp_', 'a:ticker_', 'a:person_', 'a:govt_', 'a:org_', 'a:fund_', 'a:']) {
        if (id.startsWith(pfx)) return id.slice(pfx.length) || null;
    }
    return id;
}

function _canvasApiId(nodeType, nodeId) {
    if (!nodeId) return nodeId;
    const id = String(nodeId);
    if ((nodeType === 'actor' || nodeType === 'company') && id.startsWith('a:')) return id.slice(2);
    if (nodeType === 'ticker' && id.startsWith('t:')) return id.slice(2);
    if (nodeType === 'signal' && id.startsWith('s:')) return id.slice(2);
    return id;
}

function _nodeAttrs(graph, nodeId) {
    return nodeId && graph.hasNode(nodeId) ? graph.getNodeAttributes(nodeId) : {};
}

function _legacyExpandTarget(nodeType, nodeId, attrs = {}) {
    const data = attrs.data || {};
    if (nodeType === 'company') {
        const ticker = attrs.ticker || data.ticker;
        if (ticker) return { type: 'ticker', id: String(ticker).toUpperCase() };
        return { type: 'actor', id: _canvasApiId('actor', nodeId) };
    }
    if (['actor', 'ticker', 'signal'].includes(nodeType)) {
        return { type: nodeType, id: _canvasApiId(nodeType, nodeId) };
    }
    return null;
}

function _detailApiTarget(nodeType, nodeId, attrs = {}) {
    const data = attrs.data || {};
    if (nodeType === 'company') {
        const ticker = attrs.ticker || data.ticker;
        return ticker ? { type: 'ticker', id: String(ticker).toUpperCase() } : null;
    }
    if (['actor', 'ticker', 'signal'].includes(nodeType)) {
        return { type: nodeType, id: _canvasApiId(nodeType, nodeId) };
    }
    return null;
}

function _fallbackDetailFromAttrs(nodeId, nodeType, attrs = {}) {
    return {
        id: nodeId,
        type: nodeType || attrs.nodeType || 'actor',
        label: attrs.label || attrs.name || nodeId,
        name: attrs.name || attrs.label || nodeId,
        title: attrs.title || attrs.subtitle || '',
        tier: attrs.tier,
        category: attrs.category,
        data: {
            ...(attrs.data || {}),
            trust_score: attrs.trust_score,
            trustScore: attrs.trust_score,
            confidence: attrs.confidence,
            direction: attrs.direction,
            magnitude: attrs.magnitude,
            source_type: attrs.source_type,
            ticker: attrs.ticker,
            category: attrs.category,
            title: attrs.title,
        },
    };
}

function _normalizeDetailForPanel(detail, selectedNode, attrs = {}) {
    const fallback = _fallbackDetailFromAttrs(selectedNode?.id, selectedNode?.type, attrs);
    if (!detail || detail.error) return fallback;

    if (detail.node_type === 'actor' && detail.actor) {
        const actor = detail.actor;
        const out = (detail.wealth_flows_out || []).map((f) => ({
            direction: 'out',
            counterparty: f.to_entity,
            amount: f.amount_estimate,
            confidence: f.confidence,
            date: f.flow_date,
        }));
        const incoming = (detail.wealth_flows_in || []).map((f) => ({
            direction: 'in',
            counterparty: f.from_actor,
            amount: f.amount_estimate,
            confidence: f.confidence,
            date: f.flow_date,
        }));
        return {
            ...fallback,
            type: 'actor',
            id: selectedNode?.id || actor.id,
            label: actor.name || fallback.label,
            name: actor.name || fallback.name,
            title: actor.title || fallback.title,
            tier: actor.tier || fallback.tier,
            category: actor.category || fallback.category,
            data: {
                ...fallback.data,
                ...actor,
                trust_score: actor.trust_score,
                trustScore: actor.trust_score,
                influence_rank: actor.influence_score ? Math.max(1, Math.round((1 - actor.influence_score) * 100)) : null,
                recent_actions: (detail.recent_signals || []).map((s) => ({
                    type: s.signal_type,
                    direction: s.direction,
                    ticker: s.ticker,
                    date: s.signal_date,
                    description: s.description,
                })),
                wealth_flows: [...out, ...incoming],
                connections: detail.connected_actors || [],
                known_positions: detail.dollar_flows || [],
            },
        };
    }

    if (detail.node_type === 'ticker') {
        return {
            ...fallback,
            type: 'ticker',
            id: selectedNode?.id || detail.ticker,
            label: detail.ticker || fallback.label,
            name: detail.ticker || fallback.name,
            data: {
                ...fallback.data,
                ticker: detail.ticker,
                recent_signals: detail.recent_signals || [],
                related_actors: detail.related_actors || [],
                dollar_flows: detail.dollar_flows || [],
                options: {
                    signals: detail.options_positioning || [],
                },
            },
        };
    }

    if (detail.node_type === 'signal' && detail.signal) {
        const signal = detail.signal;
        return {
            ...fallback,
            type: 'signal',
            id: selectedNode?.id || `s:${signal.id}`,
            label: signal.description || fallback.label,
            name: signal.description || fallback.name,
            data: {
                ...fallback.data,
                ...signal,
                source_type: signal.signal_type,
                date: signal.signal_date,
                text: signal.description,
            },
        };
    }

    return fallback;
}

function parseCanvasHash() {
    if (typeof window === 'undefined') return { actorId: null, lens: LENS_GRAPH, boardId: null };
    const raw = window.location.hash.slice(2) || '';
    const [path, search = ''] = raw.split('?');
    const pathParts = path.split('/').filter(Boolean);
    const params = new URLSearchParams(search);
    if (pathParts[0] !== 'canvas') return { actorId: null, lens: LENS_GRAPH, boardId: null };
    const actorId = _stripCanvasPrefix(pathParts[1] ? decodeURIComponent(pathParts[1]) : null);
    const lens = VALID_LENSES.has(pathParts[2]) ? pathParts[2] : LENS_GRAPH;
    return { actorId, lens, boardId: params.get('board') || null };
}
function writeCanvasHash(actorId, lens) {
    if (typeof window === 'undefined') return;
    const boardId = parseCanvasHash().boardId;
    const cleanId = _stripCanvasPrefix(actorId);
    const parts = ['canvas'];
    if (cleanId) parts.push(encodeURIComponent(cleanId));
    if (lens && lens !== LENS_GRAPH) parts.push(lens);
    const query = boardId ? `?board=${encodeURIComponent(boardId)}` : '';
    const target = `#/${parts.join('/')}${query}`;
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

const GRAPH_CACHE_TTL_MS = 5 * 60 * 1000;
const DOTS_CACHE_TTL_MS = 2 * 60 * 1000;

function cacheKey(scope, parts) {
    return `grid:${scope}:${parts.map((part) => encodeURIComponent(String(part ?? ''))).join(':')}`;
}

function readSessionCache(key, ttlMs) {
    if (typeof window === 'undefined' || !window.sessionStorage) return null;
    try {
        const raw = window.sessionStorage.getItem(key);
        if (!raw) return null;
        const cached = JSON.parse(raw);
        if (!cached || Date.now() - cached.savedAt > ttlMs) return null;
        return cached.value ?? null;
    } catch {
        return null;
    }
}

function writeSessionCache(key, value) {
    if (typeof window === 'undefined' || !window.sessionStorage || value == null) return;
    try {
        window.sessionStorage.setItem(key, JSON.stringify({ savedAt: Date.now(), value }));
    } catch {
        // Storage can be unavailable in private mode or full; network fallback still works.
    }
}

function canvasGraphCacheKey(center, depth, layers, since, limit) {
    return cacheKey('canvas-graph', [center, depth, layers, since || 'none', limit]);
}

function canvasDotsCacheKey(center) {
    return cacheKey('canvas-dots', [center || 'all']);
}

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
    statusBanner: {
        display: 'flex',
        alignItems: 'flex-start',
        gap: '8px',
        margin: '10px 16px 0',
        padding: '10px 12px',
        borderRadius: tokens.radius.sm,
        border: `1px solid ${colors.red}40`,
        background: `${colors.red}12`,
        color: colors.text,
        fontFamily: SANS,
        fontSize: '12px',
        lineHeight: 1.45,
    },
    statusBannerText: {
        minWidth: 0,
        flex: 1,
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

function DetailFallbackPanel({ isMobile, title, message, onClose }) {
    return (
        <div style={isMobile ? {
            position: 'absolute', bottom: 0, left: 0, right: 0,
            maxHeight: '60vh', zIndex: 100,
            borderTop: `1px solid ${colors.border}`,
            borderRadius: '14px 14px 0 0',
            overflow: 'hidden',
            background: colors.card,
        } : {
            position: 'absolute',
            top: 0, right: 0, bottom: 0,
            width: '360px',
            background: colors.card,
            borderLeft: `1px solid ${colors.border}`,
            zIndex: 100,
            display: 'flex',
            flexDirection: 'column',
            boxShadow: '-4px 0 24px rgba(0,0,0,0.4)',
        }}>
            <div style={{
                ...glassMorphism,
                padding: '16px',
                borderBottom: `1px solid ${colors.border}`,
                display: 'flex',
                alignItems: 'flex-start',
                justifyContent: 'space-between',
                gap: '8px',
            }}>
                <div>
                    <div style={{ fontSize: '10px', color: colors.accent, fontFamily: MONO, fontWeight: 700, letterSpacing: '1px' }}>
                        NODE DETAIL
                    </div>
                    <div style={{ fontSize: '17px', color: colors.text, fontFamily: SANS, fontWeight: 700, marginTop: '4px' }}>
                        {title}
                    </div>
                </div>
                <button
                    style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', fontSize: '18px', padding: '2px 6px' }}
                    onClick={onClose}
                    title="Close"
                >
                    &times;
                </button>
            </div>
            <div style={{ padding: '18px 16px', color: colors.textMuted, fontFamily: SANS, fontSize: '13px', lineHeight: 1.5 }}>
                {message}
            </div>
        </div>
    );
}

export default function GothamCanvas() {
    const store = useCanvasStore();
    const {
        graph, selectedNode, detailPanelOpen, detailData, loading,
        contextMenu, activeLayers, boardName, searchQuery, boardId,
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
    const [detailLoading, setDetailLoading] = useState(false);
    const [detailError, setDetailError] = useState(null);
    const [canvasStatus, setCanvasStatus] = useState(null);
    const nameInputRef = useRef(null);
    const sigmaRef = useRef(null);

    const getBoardIdFromHash = useCallback(() => parseCanvasHash().boardId, []);

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
            if (actorId !== lensActorId) setLensActorId(actorId);
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
    const { communities, communityColors, communityLabels } = useCommunities(graph, showCommunities);

    const expandCanvasNode = useCallback(async (nodeId, requestedType, depth = 1) => {
        if (!nodeId) return null;
        const attrs = _nodeAttrs(graph, nodeId);
        const nodeType = requestedType || attrs.nodeType || attrs.type || 'actor';

        if (boardId) {
            const result = await api.expandCanvasNode(boardId, nodeId, depth);
            if (result && !result.error) return result;
        }

        const existingIds = [];
        graph.forEachNode((id) => existingIds.push(id));
        const legacyTarget = _legacyExpandTarget(nodeType, nodeId, attrs);
        if (!legacyTarget) {
            return { error: true, message: `Cannot expand ${nodeType} without a saved board.` };
        }
        return api.expandNode(legacyTarget.type, legacyTarget.id, depth, existingIds);
    }, [boardId, graph]);

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
            expandCanvasNode(selectedNode.id, selectedNode.type || 'actor', 1)
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
                const boardId = getBoardIdFromHash();
                if (boardId) {
                    const board = await api.getBoard(boardId);
                    const graphState = board?.graph_state || { nodes: [], edges: [] };
                    if (!cancelled && board && !board.error) {
                        setCanvasStatus(null);
                        useCanvasStore.setState({
                            boardId: board.id || boardId,
                            boardName: board.name || 'Untitled Investigation',
                            activeLayers: new Set(board.filters?.layers || ['financial', 'insider']),
                            timeRange: board.filters?.timeRange || { start: null, end: null },
                        });
                        loadGraph(graphState);
                        return;
                    } else if (!cancelled) {
                        setCanvasStatus({
                            type: 'error',
                            message: 'Saved board could not be loaded. Showing the fallback canvas instead.',
                        });
                    }
                }

                const graphKey = canvasGraphCacheKey('all', 2, 'all', null, 250);
                const cachedGraph = readSessionCache(graphKey, GRAPH_CACHE_TTL_MS);
                if (!cancelled && cachedGraph) {
                    loadGraph(cachedGraph);
                    setCanvasStatus(null);
                }

                const data = await api.getCanvasGraph('all', 2, 'all', null, 250);
                if (!cancelled && data && !data.error) {
                    writeSessionCache(graphKey, data);
                    loadGraph(data);
                    setCanvasStatus(null);
                } else if (!cancelled) {
                    setCanvasStatus({
                        type: 'error',
                        message: 'The canvas failed to load. You can try loading again from the graph toolbar.',
                    });
                }
            } catch (e) {
                if (!cancelled) {
                    setCanvasStatus({
                        type: 'error',
                        message: 'The canvas failed to load. You can try loading again from the graph toolbar.',
                    });
                }
            } finally {
                if (!cancelled) useCanvasStore.getState().setLoading(false);
            }
        }
        load();
        return () => { cancelled = true; };
    }, [getBoardIdFromHash, loadGraph]);

    // ── Connect Dots: fetch cross-reference intelligence ──
    const connectDots = useCallback(async (center) => {
        const resolvedCenter = center || 'all';
        const dotsKey = canvasDotsCacheKey(resolvedCenter);
        const cachedDots = readSessionCache(dotsKey, DOTS_CACHE_TTL_MS);
        if (Array.isArray(cachedDots)) {
            setDots(cachedDots);
            if (cachedDots.length > 0) setFeedExpanded(true);
        }
        setDotsLoading(!cachedDots);
        try {
            const data = await api.getCanvasDots(resolvedCenter);
            if (data && !data.error && data.connections) {
                writeSessionCache(dotsKey, data.connections);
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
                const data = await expandCanvasNode(nodeId, nodeType, 1);
                if (data && !data.error) {
                    addNodes(data);
                }
            } catch (err) {
                // silenced
            }
        };
        window.addEventListener('canvas:expandNode', handler);
        return () => window.removeEventListener('canvas:expandNode', handler);
    }, [expandCanvasNode, addNodes]);

    // ── Fetch detail data when node is selected ──
    useEffect(() => {
        if (!selectedNode) {
            useCanvasStore.getState().setDetailData(null);
            setDetailLoading(false);
            setDetailError(null);
            return;
        }
        let cancelled = false;
        async function fetchDetail() {
            setDetailLoading(true);
            setDetailError(null);
            useCanvasStore.getState().setDetailData(null);
            const attrs = _nodeAttrs(graph, selectedNode.id);
            const detailType = selectedNode.type || attrs.nodeType || 'actor';
            const fallback = _fallbackDetailFromAttrs(selectedNode.id, detailType, attrs);
            try {
                const detailTarget = _detailApiTarget(detailType, selectedNode.id, attrs);
                if (!detailTarget) {
                    useCanvasStore.getState().setDetailData(fallback);
                    return;
                }
                const data = await api.getNodeDetail(detailTarget.type, detailTarget.id);
                if (!cancelled && data && !data.error) {
                    useCanvasStore.getState().setDetailData(_normalizeDetailForPanel(data, selectedNode, attrs));
                } else if (!cancelled) {
                    useCanvasStore.getState().setDetailData(fallback);
                    setDetailError('Showing graph intelligence. No deep detail record was returned for this node.');
                }
            } catch (e) {
                if (!cancelled) {
                    useCanvasStore.getState().setDetailData(fallback);
                    setDetailError('Showing graph intelligence. Deep detail failed to load.');
                }
            } finally {
                if (!cancelled) setDetailLoading(false);
            }
        }
        fetchDetail();
        return () => { cancelled = true; };
    }, [selectedNode, graph]);

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
        const query = searchQuery.trim();
        useCanvasStore.getState().setLoading(true);
        try {
            const graphKey = canvasGraphCacheKey(query, 2, 'all', null, 200);
            const cachedGraph = readSessionCache(graphKey, GRAPH_CACHE_TTL_MS);
            if (cachedGraph) {
                loadGraph(cachedGraph);
                useCanvasStore.getState().setLoading(false);
                setCanvasStatus(null);
            }
            const data = await api.getCanvasGraph(query, 2, 'all', null, 200);
            if (data && !data.error) {
                writeSessionCache(graphKey, data);
                loadGraph(data);
                setCanvasStatus(null);
                // Update the focal actor to the search query so lenses
                // and hash track the new center, not the stale one.
                const cleanQuery = _stripCanvasPrefix(query);
                setLensActorId(cleanQuery);
                connectDots(query);
            } else {
                setCanvasStatus({
                    type: 'error',
                    message: 'Search results could not be loaded. Please try again.',
                });
            }
        } catch (err) {
            setCanvasStatus({
                type: 'error',
                message: 'Search results could not be loaded. Please try again.',
            });
        } finally {
            useCanvasStore.getState().setLoading(false);
        }
    }, [searchQuery, loadGraph, connectDots, setLensActorId]);

    // ── Context menu actions ──
    const handleContextAction = useCallback(async (action) => {
        const nodeId = contextMenu.node;
        hideContextMenu();
        if (!nodeId) return;
        const attrs = graph.hasNode(nodeId) ? graph.getNodeAttributes(nodeId) : {};

        switch (action) {
            case 'details':
            case 'showWealthFlows':
            case 'showTradingHistory':
            case 'showTrustBreakdown':
            case 'showInsiderActivity':
            case 'showCongressionalTrades':
            case 'showOptionsData':
            case 'viewSourceData':
            case 'markInvestigated':
            case 'viewFullDetails':
                selectNode(nodeId, attrs.nodeType || attrs.type || 'actor');
                break;
            case 'pin': {
                if (graph.hasNode(nodeId)) {
                    graph.setNodeAttribute(nodeId, 'fixed', !attrs.fixed);
                }
                break;
            }
            case 'hide':
                useCanvasStore.getState().removeNodes([nodeId]);
                clearSelection();
                break;
            case 'expand':
            case 'expandDeep': {
                const depth = action === 'expandDeep' ? 3 : 1;
                try {
                    const data = await expandCanvasNode(nodeId, attrs.nodeType || attrs.type || 'actor', depth);
                    if (data && !data.error) addNodes(data);
                } catch (e) {
                    // silenced
                }
                break;
            }
            case 'connectRelatedActors':
            case 'showRelatedActors':
            case 'showRelatedSignals': {
                try {
                    const data = await expandCanvasNode(nodeId, attrs.nodeType || attrs.type || 'actor', 1);
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
    }, [contextMenu, graph, hideContextMenu, selectNode, addNodes, connectDots, clearSelection, expandCanvasNode]);

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
                                const payload = {
                                    graph_state: graphState,
                                    filters: { layers: [...state.activeLayers], timeRange: state.timeRange },
                                    name: state.boardName,
                                };
                                if (state.boardId) {
                                    await api.saveBoard(state.boardId, payload);
                                } else {
                                    const result = await api.createBoard(state.boardName);
                                    if (result && result.id) {
                                        useCanvasStore.setState({
                                            boardId: result.id,
                                            boardName: result.name || state.boardName,
                                        });
                                        await api.saveBoard(result.id, payload);
                                    }
                                }
                                setCanvasStatus(null);
                            } catch (e) {
                                setCanvasStatus({
                                    type: 'error',
                                    message: 'Save failed. Your canvas changes were not persisted.',
                                });
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

            {canvasStatus?.message && (
                <div style={S.statusBanner} role="status" aria-live="polite">
                    <AlertTriangle size={14} color={colors.red} style={{ flexShrink: 0, marginTop: '1px' }} />
                    <div style={S.statusBannerText}>{canvasStatus.message}</div>
                </div>
            )}

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
                                        if (data && !data.error) {
                                            loadGraph(data);
                                            setCanvasStatus(null);
                                        } else {
                                            setCanvasStatus({
                                                type: 'error',
                                                message: 'The power map could not be loaded. Please try again.',
                                            });
                                        }
                                    } catch (e) {
                                        setCanvasStatus({
                                            type: 'error',
                                            message: 'The power map could not be loaded. Please try again.',
                                        });
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
                {detailPanelOpen && (detailData ? (
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
                            type: detailData.type || selectedNode?.type || 'actor',
                            id: detailData.id || selectedNode?.id,
                        }}
                        onClose={clearSelection}
                        onExpand={() => {
                            if (!selectedNode) return;
                            expandCanvasNode(selectedNode.id, selectedNode.type || 'actor', 1)
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
                ) : (
                    <DetailFallbackPanel
                        isMobile={isMobile}
                        title={selectedNode?.id || 'Selected node'}
                        message={detailLoading ? 'Loading node intelligence...' : detailError || 'No detail data is available for this node yet.'}
                        onClose={clearSelection}
                    />
                ))}

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
