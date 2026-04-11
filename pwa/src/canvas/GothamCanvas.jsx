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
    ChevronDown, ChevronUp, Workflow,
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
};

export default function GothamCanvas() {
    const store = useCanvasStore();
    const {
        graph, selectedNode, detailPanelOpen, detailData, loading,
        contextMenu, activeLayers, boardName, searchQuery,
        loadGraph, addNodes, selectNode, clearSelection, toggleLayer,
        setTimeRange, hideContextMenu, showContextMenu,
    } = store;

    // Local state
    const [activeTime, setActiveTime] = useState(null);
    const [editingName, setEditingName] = useState(false);
    const [dots, setDots] = useState([]);         // cross-reference intelligence
    const [dotsLoading, setDotsLoading] = useState(false);
    const [feedExpanded, setFeedExpanded] = useState(false);
    const nameInputRef = useRef(null);
    const sigmaRef = useRef(null);

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
                console.error('Canvas load error:', e);
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
            console.error('Connect dots error:', e);
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
                console.error('Expand node error:', err);
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
                console.error('Detail fetch error:', e);
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
            console.error('Search error:', err);
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
                selectNode(nodeId, attrs.type || 'actor');
                break;
            case 'expand':
            case 'expandDeep': {
                const depth = action === 'expandDeep' ? 3 : 1;
                const existingIds = [];
                graph.forEachNode((id) => existingIds.push(id));
                try {
                    const data = await api.expandNode(attrs.type || 'actor', nodeId, depth, existingIds);
                    if (data && !data.error) addNodes(data);
                } catch (e) {
                    console.error('Context expand error:', e);
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
            <div style={S.commandBar}>
                {/* Search */}
                <div style={S.searchWrap}>
                    <Search size={14} color={colors.textMuted} />
                    <input
                        style={S.searchInput}
                        placeholder="Search actors, tickers... (Enter)"
                        value={searchQuery}
                        onChange={(e) => useCanvasStore.getState().setSearchQuery(e.target.value)}
                        onKeyDown={handleSearchSubmit}
                        spellCheck={false}
                    />
                </div>

                {/* Layer controls */}
                <LayerControls
                    activeLayers={activeLayers}
                    onToggleLayer={toggleLayer}
                />

                {/* Time range */}
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

                {/* Connect Dots button */}
                <button
                    style={S.actionBtn(true)}
                    onClick={() => connectDots(selectedNode?.id || 'all')}
                    disabled={dotsLoading}
                >
                    <Zap size={13} />
                    {dotsLoading ? 'Connecting...' : 'Connect Dots'}
                </button>

                {/* Spacer */}
                <div style={{ flex: 1 }} />

                {/* Save */}
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
                            console.error('Save error:', e);
                        }
                    }}
                >
                    <Save size={13} />
                    Save
                </button>

                {/* Board name */}
                {editingName ? (
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
                )}

                {/* Count badge */}
                <span style={S.countBadge}>
                    {nodeCount}n / {edgeCount}e
                </span>
            </div>

            {/* ══ Main Area (Graph + Panels) ══ */}
            <div style={S.mainArea}>
                {/* Graph */}
                <div style={S.graphContainer}>
                    {nodeCount > 0 ? (
                        <SigmaGraph ref={sigmaRef} />
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
                                        console.error(e);
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

                {/* Detail Panel — rich component */}
                {detailPanelOpen && detailData && (
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
            <div style={S.intelFeed(feedExpanded)}>
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
