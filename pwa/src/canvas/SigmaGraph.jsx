/**
 * SigmaGraph — WebGL graph renderer using Sigma.js v3 + Graphology.
 * Core rendering component for the Gotham Canvas.
 *
 * Uses @react-sigma/core for React integration.
 * ForceAtlas2 layout via graphology-layout-forceatlas2 (Web Worker).
 */

import React, { useEffect, useCallback, useState } from 'react';
import { SigmaContainer, useRegisterEvents, useSigma, useLoadGraph } from '@react-sigma/core';
import EdgeCurveProgram from '@sigma/edge-curve';
import forceAtlas2 from 'graphology-layout-forceatlas2';
import '@react-sigma/core/lib/style.css';
import useCanvasStore from './CanvasStore.js';
import CommunityHulls from './CommunityHulls.jsx';

// ── Sigma settings ──
const SIGMA_SETTINGS = {
    defaultNodeColor: '#4A5568',
    defaultEdgeColor: '#1A2332',
    labelFont: 'IBM Plex Mono',
    labelColor: { color: '#8AA0B8' },
    labelSize: 12,
    labelRenderedSizeThreshold: 6,
    edgeProgramClasses: { curved: EdgeCurveProgram },
    allowInvalidContainer: true,
    renderLabels: true,
    enableEdgeEvents: true,
    zIndex: true,
    // Performance
    hideEdgesOnMove: true,
    hideLabelsOnMove: false,
    renderEdgeLabels: false,
    // Anti-aliasing
    labelDensity: 0.07,
    labelGridCellSize: 60,
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

function scheduleIdle(callback) {
    if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') {
        const id = window.requestIdleCallback(callback, { timeout: 900 });
        return () => window.cancelIdleCallback?.(id);
    }
    const id = window.setTimeout(callback, 80);
    return () => window.clearTimeout(id);
}

function layoutIterations(order) {
    if (order > 600) return 80;
    if (order > 300) return 120;
    return Math.min(220, Math.max(60, order * 2));
}

/**
 * GraphLoader — loads the graphology Graph from CanvasStore into Sigma.
 */
function GraphLoader() {
    const loadGraph = useLoadGraph();
    const sigma = useSigma();
    const graph = useCanvasStore((s) => s.graph);

    useEffect(() => {
        if (!graph || graph.order === 0) return;

        loadGraph(graph);

        if (graph.order > MAX_LAYOUT_NODES) {
            sigma.refresh();
            return;
        }

        let cancelled = false;
        const cancelIdle = scheduleIdle(() => {
            if (cancelled) return;
            try {
                forceAtlas2.assign(graph, {
                    iterations: layoutIterations(graph.order),
                    settings: FA2_SETTINGS,
                });
                sigma.refresh();
            } catch (e) {
                console.warn('ForceAtlas2 layout error:', e);
            }
        });

        return () => {
            cancelled = true;
            cancelIdle?.();
        };
    }, [graph, graph.order, loadGraph, sigma]);

    return null;
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
                selectNode(node, attrs.type || 'actor');
            },
            clickStage: () => {
                hideContextMenu();
                clearSelection();
            },
            enterNode: ({ node }) => {
                hoverNode(node);
                // Highlight neighbors
                const graph = sigma.getGraph();
                const neighbors = new Set(graph.neighbors(node));
                neighbors.add(node);

                sigma.setSetting('nodeReducer', (n, data) => {
                    if (neighbors.has(n)) {
                        return { ...data, zIndex: 1 };
                    }
                    return { ...data, color: '#1A2332', label: null, zIndex: 0 };
                });

                sigma.setSetting('edgeReducer', (edge, data) => {
                    const graph = sigma.getGraph();
                    const src = graph.source(edge);
                    const tgt = graph.target(edge);
                    if (neighbors.has(src) && neighbors.has(tgt)) {
                        return { ...data, hidden: false, zIndex: 1 };
                    }
                    return { ...data, hidden: true, zIndex: 0 };
                });

                sigma.refresh();
            },
            leaveNode: () => {
                hoverNode(null);
                sigma.setSetting('nodeReducer', null);
                sigma.setSetting('edgeReducer', null);
                sigma.refresh();
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
                        nodeType: sigma.getGraph().getNodeAttributes(node).type || 'actor',
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

        const neighbors = new Set(graph.neighbors(selectedNode.id));
        neighbors.add(selectedNode.id);

        sigma.setSetting('nodeReducer', (n, data) => {
            if (n === selectedNode.id) {
                return {
                    ...data,
                    highlighted: true,
                    zIndex: 2,
                    size: data.size * 1.3,
                };
            }
            if (neighbors.has(n)) {
                return { ...data, zIndex: 1 };
            }
            return { ...data, color: '#1A2332', label: null, zIndex: 0 };
        });

        sigma.setSetting('edgeReducer', (edge, data) => {
            const src = graph.source(edge);
            const tgt = graph.target(edge);
            if (src === selectedNode.id || tgt === selectedNode.id) {
                return { ...data, hidden: false, size: data.size * 1.5, zIndex: 1 };
            }
            if (neighbors.has(src) && neighbors.has(tgt)) {
                return { ...data, hidden: false, zIndex: 0 };
            }
            return { ...data, hidden: true, zIndex: 0 };
        });

        sigma.refresh();

        return () => {
            sigma.setSetting('nodeReducer', null);
            sigma.setSetting('edgeReducer', null);
            sigma.refresh();
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
            settings={SIGMA_SETTINGS}
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
