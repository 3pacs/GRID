/**
 * SigmaGraph — WebGL network graph via Sigma.js + graphology.
 *
 * Renders network graphs with up to 50K nodes at 60fps using WebGL.
 * Supports force-directed layouts, node/edge styling, hover, and click.
 *
 * Props:
 *   nodes     — array of { id, label, x?, y?, size?, color?, category? }
 *   edges     — array of { source, target, weight?, color?, label? }
 *   height    — container height (default: 500)
 *   layout    — 'forceatlas2' | 'random' | 'preset' (default: 'forceatlas2')
 *   onNodeClick — callback(nodeId, nodeAttrs)
 *   onNodeHover — callback(nodeId, nodeAttrs)
 *   settings  — sigma renderer settings overrides
 */
import { useRef, useEffect, useCallback } from 'react';
import Graph from 'graphology';
import Sigma from 'sigma';
import forceAtlas2 from 'graphology-layout-forceatlas2';

const GRID_COLORS = {
    bg: '#0D1520',
    node: '#1A6EBF',
    edge: '#1A2332',
    label: '#8AA0B8',
    highlight: '#10B981',
    categories: {
        sovereign: '#EF4444',
        regional: '#F59E0B',
        institutional: '#1A6EBF',
        individual: '#10B981',
        default: '#8B5CF6',
    },
};

export default function SigmaGraph({
    nodes = [],
    edges = [],
    height = 500,
    layout = 'forceatlas2',
    onNodeClick,
    onNodeHover,
    settings = {},
}) {
    const containerRef = useRef(null);
    const sigmaRef = useRef(null);
    const graphRef = useRef(null);

    useEffect(() => {
        if (!containerRef.current || nodes.length === 0) return;

        // Clean up previous instance
        if (sigmaRef.current) {
            sigmaRef.current.kill();
            sigmaRef.current = null;
        }

        // Build graph
        const graph = new Graph();
        graphRef.current = graph;

        nodes.forEach((node) => {
            const category = (node.category || 'default').toLowerCase();
            graph.addNode(node.id, {
                label: node.label || node.id,
                x: node.x ?? Math.random() * 100,
                y: node.y ?? Math.random() * 100,
                size: node.size || 6,
                color: node.color || GRID_COLORS.categories[category] || GRID_COLORS.node,
            });
        });

        edges.forEach((edge, i) => {
            if (graph.hasNode(edge.source) && graph.hasNode(edge.target)) {
                graph.addEdge(edge.source, edge.target, {
                    weight: edge.weight || 1,
                    color: edge.color || GRID_COLORS.edge,
                    label: edge.label || '',
                    size: Math.min((edge.weight || 1) * 0.5, 4),
                });
            }
        });

        // Apply layout
        if (layout === 'forceatlas2' && nodes.length > 1) {
            forceAtlas2.assign(graph, {
                iterations: 100,
                settings: {
                    gravity: 1,
                    scalingRatio: 10,
                    barnesHutOptimize: nodes.length > 500,
                    strongGravityMode: true,
                },
            });
        }

        // Create renderer
        const renderer = new Sigma(graph, containerRef.current, {
            renderEdgeLabels: false,
            defaultNodeColor: GRID_COLORS.node,
            defaultEdgeColor: GRID_COLORS.edge,
            labelColor: { color: GRID_COLORS.label },
            labelFont: 'IBM Plex Mono',
            labelSize: 11,
            labelRenderedSizeThreshold: 8,
            nodeReducer: (node, data) => {
                const res = { ...data };
                if (renderer._hoveredNode && renderer._hoveredNode !== node) {
                    if (!graph.hasEdge(renderer._hoveredNode, node) &&
                        !graph.hasEdge(node, renderer._hoveredNode)) {
                        res.color = '#1A2332';
                        res.label = '';
                    }
                }
                return res;
            },
            edgeReducer: (edge, data) => {
                const res = { ...data };
                if (renderer._hoveredNode) {
                    const [src, tgt] = graph.extremities(edge);
                    if (src !== renderer._hoveredNode && tgt !== renderer._hoveredNode) {
                        res.hidden = true;
                    } else {
                        res.color = GRID_COLORS.highlight;
                        res.size = 2;
                    }
                }
                return res;
            },
            ...settings,
        });

        sigmaRef.current = renderer;
        renderer._hoveredNode = null;

        // Event handlers
        renderer.on('enterNode', ({ node }) => {
            renderer._hoveredNode = node;
            renderer.refresh();
            if (onNodeHover) onNodeHover(node, graph.getNodeAttributes(node));
        });

        renderer.on('leaveNode', () => {
            renderer._hoveredNode = null;
            renderer.refresh();
            if (onNodeHover) onNodeHover(null, null);
        });

        renderer.on('clickNode', ({ node }) => {
            if (onNodeClick) onNodeClick(node, graph.getNodeAttributes(node));
        });

        return () => {
            renderer.kill();
            sigmaRef.current = null;
            graphRef.current = null;
        };
    }, [nodes, edges, layout, settings, onNodeClick, onNodeHover]);

    return (
        <div
            ref={containerRef}
            style={{
                width: '100%',
                height,
                background: GRID_COLORS.bg,
                borderRadius: 6,
                overflow: 'hidden',
            }}
        />
    );
}
