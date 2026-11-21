/**
 * SigmaGraph — WebGL graph renderer using Sigma.js v4 + Graphology.
 * Core rendering component for the Gotham Canvas.
 *
 * Uses @react-sigma/core (v6) for React integration.
 * ForceAtlas2 runs in bounded scheduled batches with persistent matrix state.
 *
 * Sigma v4 notes (see https://www.sigmajs.org/how-to/technical/migration-v3-v4/):
 * - Appearance comes from declarative `styles`, not settings. Reducers are
 *   constructor options only (no `setSetting('nodeReducer')`), so hover/select
 *   focus is pushed through custom graph state and read by static reducers.
 * - The `hidden` attribute is no longer read by sigma; the styles below map it
 *   to `visibility` so the depth filter in CanvasStore keeps working.
 * - `highlighted` (activity pulse) maps to a forced label, as v3 drew it.
 * - Do not import from 'sigma' here: its module init touches
 *   WebGL2RenderingContext, which jsdom tests do not have.
 */

import React, { useEffect, useCallback, useState } from 'react';
import { SigmaContainer, useRegisterEvents, useSigma, useLoadGraph } from '@react-sigma/core';
import { startIncrementalLayout } from './incrementalLayout.js';
import '@react-sigma/core/lib/style.css';
import useCanvasStore from './CanvasStore.js';
import CommunityHulls from './CommunityHulls.jsx';

const DIM_COLOR = '#1A2332';

// ── Sigma styles (v4) ──
// A user `styles.nodes` / `styles.edges` replaces sigma's DEFAULT_STYLES
// wholesale, so the defaults we rely on (x/y, hover depth, hover backdrop) are
// restated here alongside the v3-era look (colours, label font).
const hiddenByAttribute = { whenData: { hidden: true }, then: 'hidden', else: { whenState: 'isHidden', then: 'hidden', else: 'visible' } };

export const SIGMA_STYLES = {
    nodes: {
        depth: { whenState: 'isHovered', then: 'topNodes', else: 'nodes' },
        x: { attribute: 'x' },
        y: { attribute: 'y' },
        size: { attribute: 'size', defaultValue: 10 },
        color: { attribute: 'color', defaultValue: '#4A5568' },
        label: { attribute: 'label' },
        labelColor: '#8AA0B8',
        labelFont: 'IBM Plex Mono',
        labelSize: 12,
        visibility: hiddenByAttribute,
        labelVisibility: { whenState: 'isHovered', then: 'visible', else: { whenData: { highlighted: true }, then: 'visible', else: 'auto' } },
        backdropVisibility: { whenState: 'isHovered', then: 'visible', else: 'hidden' },
        backdropColor: '#ffffff',
        backdropShadowColor: 'rgba(0, 0, 0, 0.5)',
        backdropShadowBlur: 12,
        backdropPadding: 6,
    },
    edges: {
        depth: { whenState: ['isHighlighted', 'isHovered'], then: 'topEdges', else: 'edges' },
        size: { attribute: 'size', defaultValue: 1 },
        color: { attribute: 'color', defaultValue: DIM_COLOR },
        label: { attribute: 'label' },
        visibility: hiddenByAttribute,
    },
};

// ── Focus (hover / selection) via graph state + static reducers ──
// graphState.hoverFocus / selectFocus: { id, neighbors: Set<string> } | null.
// Hover wins over selection while the pointer is on a node.
export const CUSTOM_GRAPH_STATE = { hoverFocus: null, selectFocus: null };

export function focusNodeReducer(key, data, attrs, state, graphState) {
    const hover = graphState?.hoverFocus;
    const focus = hover || graphState?.selectFocus;
    if (!focus) return {};
    if (!hover && key === focus.id) {
        return { highlighted: true, labelVisibility: 'visible', depth: 'topNodes', zIndex: 2, size: data.size * 1.3 };
    }
    if (focus.neighbors.has(key)) return { depth: 'topNodes', zIndex: 1 };
    return { color: DIM_COLOR, labelVisibility: 'hidden', depth: 'nodes', zIndex: 0 };
}

export function focusEdgeReducer(key, data, attrs, state, graphState, graph) {
    const hover = graphState?.hoverFocus;
    const focus = hover || graphState?.selectFocus;
    if (!focus) return {};
    const src = graph.source(key);
    const tgt = graph.target(key);
    if (!hover && (src === focus.id || tgt === focus.id)) {
        return { visibility: 'visible', size: data.size * 1.5, depth: 'topEdges', zIndex: 1 };
    }
    if (focus.neighbors.has(src) && focus.neighbors.has(tgt)) {
        return { visibility: 'visible', depth: hover ? 'topEdges' : 'edges', zIndex: hover ? 1 : 0 };
    }
    return { visibility: 'hidden', zIndex: 0 };
}

function focusOf(graph, id) {
    const neighbors = new Set(graph.neighbors(id));
    neighbors.add(id);
    return { id, neighbors };
}

// Module-level so @react-sigma's options equality check never rebuilds sigma.
export const SIGMA_OPTIONS = {
    settings: {
        allowInvalidContainer: true,
        renderLabels: true,
        enableEdgeEvents: true,
        // v3 sized nodes/edges in screen pixels; v4 defaults to graph positions.
        itemSizesReference: 'screen',
        // Right-click is the node context menu; v4 binds right-drag to rotation.
        enableCameraMouseRotation: false,
        // Performance
        hideEdgesOnMove: true,
        hideLabelsOnMove: false,
        renderEdgeLabels: false,
        labelRenderedSizeThreshold: 6,
        labelDensity: 0.07,
        labelGridCellSize: 60,
    },
    styles: SIGMA_STYLES,
    customGraphState: CUSTOM_GRAPH_STATE,
    nodeReducer: focusNodeReducer,
    edgeReducer: focusEdgeReducer,
};

// ── ForceAtlas2 settings ──
const FA2_SETTINGS = {
    gravity: 1,
    scalingRatio: 2,
    barnesHutOptimize: true,
    slowDown: 5,
    adjustSizes: false,
    strongGravityMode: false,
};

const MAX_LAYOUT_NODES = 900;

function layoutIterations(order) {
    if (order > 600) return 80;
    if (order > 300) return 120;
    return Math.min(220, Math.max(60, order * 2));
}

/**
 * GraphLoader — loads the graphology Graph from CanvasStore into Sigma.
 */
export function GraphLoader() {
    const loadGraph = useLoadGraph();
    const sigma = useSigma();
    const graph = useCanvasStore((s) => s.graph);
    const [layout, setLayout] = useState({ phase: 'idle' });

    useEffect(() => {
        if (!graph || graph.order === 0) { setLayout({ phase: 'idle' }); return; }

        loadGraph(graph);

        if (graph.getAttribute('canvasPreserveLayout')) {
            setLayout({ phase: 'saved' });
            sigma.refresh();
            return;
        }
        if (graph.order > MAX_LAYOUT_NODES) {
            setLayout({ phase: 'skipped' });
            sigma.refresh();
            return;
        }
        let active = true;
        setLayout({ phase: 'arranging', completed: 0, total: layoutIterations(graph.order) });
        const cancel = startIncrementalLayout(graph, {
            iterations: layoutIterations(graph.order), settings: FA2_SETTINGS,
            onProgress: (progress) => { if (active) setLayout({ phase: 'arranging', ...progress }); },
            onComplete: () => { if (active) { sigma.refresh(); setLayout({ phase: 'ready' }); } },
            onCancel: () => { if (active) setLayout({ phase: 'paused' }); },
            onError: (error) => {
                if (active) { console.warn('ForceAtlas2 layout error:', error); setLayout({ phase: 'error' }); }
            },
        });
        return () => { active = false; cancel(); };
    }, [graph, graph.order, loadGraph, sigma]);

    const message = layout.phase === 'arranging' ? `Arranging graph (${layout.completed}/${layout.total})…`
        : layout.phase === 'paused' ? 'Layout paused after graph interaction. Current positions retained.'
        : layout.phase === 'error' ? 'Layout unavailable. Current graph positions retained.' : null;
    return message ? <div role="status" aria-live="polite" style={{ position: 'absolute', bottom: 8, left: 8, zIndex: 5, color: '#8AA0B8', fontSize: 12 }}>{message}</div> : null;
}

/**
 * GraphEvents — handles user interactions on the graph.
 */
function GraphEvents() {
    const sigma = useSigma();
    const registerEvents = useRegisterEvents();
    const selectNode = useCanvasStore((s) => s.selectNode);
    const hoverNode = useCanvasStore((s) => s.hoverNode);
    const showContextMenu = useCanvasStore((s) => s.showContextMenu);
    const hideContextMenu = useCanvasStore((s) => s.hideContextMenu);
    const clearSelection = useCanvasStore((s) => s.clearSelection);
    const addNodes = useCanvasStore((s) => s.addNodes);
    const selectedNode = useCanvasStore((s) => s.selectedNode);
    const hoveredNode = useCanvasStore((s) => s.hoveredNode);

    // Register sigma events
    useEffect(() => {
        registerEvents({
            clickNode: ({ node, event }) => {
                hideContextMenu();
                const graph = sigma.getGraph();
                const attrs = graph.getNodeAttributes(node);
                selectNode(node, attrs.nodeType || attrs.type || 'actor');
            },
            clickStage: () => {
                hideContextMenu();
                clearSelection();
            },
            enterNode: ({ node }) => {
                hoverNode(node);
                // Highlight neighbors (read by focusNodeReducer/focusEdgeReducer)
                sigma.setGraphState({ hoverFocus: focusOf(sigma.getGraph(), node) });
            },
            leaveNode: () => {
                hoverNode(null);
                sigma.setGraphState({ hoverFocus: null });
            },
            rightClickNode: ({ node, event }) => {
                event.original.preventDefault();
                showContextMenu(
                    event.original.clientX,
                    event.original.clientY,
                    node
                );
            },
            doubleClickNode: ({ node, event }) => {
                event.preventSigmaDefault();
                // Double-click to expand — dispatch a custom event for the parent to handle
                const expandEvent = new CustomEvent('canvas:expandNode', {
                    detail: {
                        nodeId: node,
                        nodeType: sigma.getGraph().getNodeAttributes(node).nodeType || 'actor',
                    },
                });
                window.dispatchEvent(expandEvent);
            },
        });
    }, [registerEvents, sigma, selectNode, hoverNode, showContextMenu, hideContextMenu, clearSelection]);

    // Highlight selected node
    useEffect(() => {
        if (!selectedNode) return;

        const graph = sigma.getGraph();
        if (!graph.hasNode(selectedNode.id)) return;

        sigma.setGraphState({ selectFocus: focusOf(graph, selectedNode.id) });

        return () => {
            sigma.setGraphState({ selectFocus: null });
        };
    }, [selectedNode, sigma]);

    return null;
}

/**
 * SigmaGraph — main Sigma.js container component.
 *
 * @param {{ style?: object, communities?: Map, communityColors?: Map, communityLabels?: Map, showCommunities?: boolean }} props
 */
export default function SigmaGraph({ style, communities, communityColors, communityLabels, showCommunities }) {
    const containerStyle = {
        width: '100%',
        height: '100%',
        background: '#080C10',
        ...style,
    };

    return (
        <SigmaContainer
            style={containerStyle}
            options={SIGMA_OPTIONS}
            className="gotham-sigma"
        >
            <GraphLoader />
            <GraphEvents />
            <CommunityHulls
                communities={communities}
                communityColors={communityColors}
                communityLabels={communityLabels}
                visible={!!showCommunities}
            />
        </SigmaContainer>
    );
}
