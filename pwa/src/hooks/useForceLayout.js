/**
 * useForceLayout — d3-force simulation for React Flow nodes.
 *
 * Runs a force-directed layout: connected nodes attract, all nodes repel,
 * edges pull by strength. Returns a `runLayout` function that repositions
 * nodes via setNodes.
 */
import { useCallback, useRef } from 'react';
import {
    forceSimulation,
    forceLink,
    forceManyBody,
    forceCenter,
    forceCollide,
    forceX,
    forceY,
} from 'd3';

/**
 * Estimate node dimensions by type for collision radius.
 */
const NODE_RADIUS = {
    actor: 80,
    company: 90,
    hypothesis: 85,
    signal: 70,
    note: 75,
    evidence: 75,
    chart: 100,
    timeline: 100,
    news: 75,
};

/**
 * @param {Function} setNodes — React Flow setNodes from useNodesState
 * @param {Object} [opts]
 * @param {number} [opts.chargeStrength=-400] — repulsion force
 * @param {number} [opts.linkDistance=180] — ideal edge length
 * @param {number} [opts.collisionPadding=20] — extra padding between nodes
 * @param {number} [opts.iterations=120] — simulation ticks
 * @param {boolean} [opts.animate=true] — animate position transitions
 */
export default function useForceLayout(setNodes, opts = {}) {
    const {
        chargeStrength = -400,
        linkDistance = 180,
        collisionPadding = 20,
        iterations = 120,
        animate = true,
    } = opts;

    const running = useRef(false);

    /**
     * Run force layout on current nodes + edges.
     * @param {Array} nodes — React Flow nodes
     * @param {Array} edges — React Flow edges
     * @param {Object} [options]
     * @param {string[]} [options.pinned] — node IDs to keep fixed
     * @param {string} [options.centerId] — node to center around (e.g. expanded node)
     */
    const runLayout = useCallback((nodes, edges, options = {}) => {
        if (running.current || nodes.length < 2) return;
        running.current = true;

        const { pinned = [], centerId } = options;

        // Build simulation nodes — copy positions
        const simNodes = nodes.map((n) => ({
            id: n.id,
            x: n.position.x,
            y: n.position.y,
            fx: pinned.includes(n.id) ? n.position.x : null,
            fy: pinned.includes(n.id) ? n.position.y : null,
            type: n.type,
        }));

        // Build simulation links
        const nodeIdSet = new Set(nodes.map((n) => n.id));
        const simLinks = edges
            .filter((e) => nodeIdSet.has(e.source) && nodeIdSet.has(e.target))
            .map((e) => ({
                source: e.source,
                target: e.target,
                strength: e.data?.strength ?? 0.5,
            }));

        // Determine center point
        let cx = 0, cy = 0;
        if (centerId) {
            const centerNode = simNodes.find((n) => n.id === centerId);
            if (centerNode) {
                cx = centerNode.x;
                cy = centerNode.y;
            }
        } else {
            // Use centroid of existing nodes
            cx = simNodes.reduce((s, n) => s + n.x, 0) / simNodes.length;
            cy = simNodes.reduce((s, n) => s + n.y, 0) / simNodes.length;
        }

        const simulation = forceSimulation(simNodes)
            .force('link', forceLink(simLinks)
                .id((d) => d.id)
                .distance(linkDistance)
                .strength((d) => 0.3 + d.strength * 0.5))
            .force('charge', forceManyBody()
                .strength(chargeStrength)
                .distanceMax(800))
            .force('center', forceCenter(cx, cy).strength(0.05))
            .force('collide', forceCollide()
                .radius((d) => (NODE_RADIUS[d.type] || 80) + collisionPadding)
                .iterations(3))
            .force('x', forceX(cx).strength(0.02))
            .force('y', forceY(cy).strength(0.02))
            .stop();

        // Run synchronous ticks
        for (let i = 0; i < iterations; i++) {
            simulation.tick();
        }

        // Build position map from simulation result
        const posMap = {};
        for (const sn of simNodes) {
            posMap[sn.id] = { x: Math.round(sn.x), y: Math.round(sn.y) };
        }

        if (animate) {
            // Animate over ~300ms in frames
            const startPositions = {};
            for (const n of nodes) {
                startPositions[n.id] = { x: n.position.x, y: n.position.y };
            }
            const duration = 300;
            const startTime = performance.now();

            const step = (now) => {
                const t = Math.min((now - startTime) / duration, 1);
                // Ease-out cubic
                const ease = 1 - Math.pow(1 - t, 3);

                setNodes((prev) =>
                    prev.map((n) => {
                        const from = startPositions[n.id];
                        const to = posMap[n.id];
                        if (!from || !to) return n;
                        return {
                            ...n,
                            position: {
                                x: from.x + (to.x - from.x) * ease,
                                y: from.y + (to.y - from.y) * ease,
                            },
                        };
                    })
                );

                if (t < 1) {
                    requestAnimationFrame(step);
                } else {
                    running.current = false;
                }
            };
            requestAnimationFrame(step);
        } else {
            setNodes((prev) =>
                prev.map((n) => {
                    const pos = posMap[n.id];
                    if (!pos) return n;
                    return { ...n, position: pos };
                })
            );
            running.current = false;
        }
    }, [setNodes, chargeStrength, linkDistance, collisionPadding, iterations, animate]);

    return { runLayout, isRunning: running };
}
