/**
 * useCommunities — Louvain community detection on graphology graph.
 *
 * Runs community detection debounced by 500ms, assigns community attributes
 * to each node, and returns community assignments, colors, and labels.
 *
 * Usage:
 *   const { communities, communityColors, communityLabels } = useCommunities(graph, showCommunities);
 */

import { useEffect, useState, useRef } from 'react';
import louvain from 'graphology-communities-louvain';

const COMMUNITY_COLORS = [
    '#3B82F6', '#10B981', '#F59E0B', '#8B5CF6',
    '#EC4899', '#06B6D4', '#EF4444', '#22C55E',
    '#F97316', '#6366F1', '#14B8A6', '#E11D48',
];

/**
 * @param {import('graphology').default} graph - Graphology graph instance
 * @param {boolean} enabled - Whether detection should run.
 * @returns {{ communities: Map, communityColors: Map, communityLabels: Map }}
 */
export function useCommunities(graph, enabled = false) {
    const [result, setResult] = useState({
        communities: new Map(),
        communityColors: new Map(),
        communityLabels: new Map(),
    });
    const timerRef = useRef(null);

    useEffect(() => {
        if (!enabled) {
            if (timerRef.current) {
                clearTimeout(timerRef.current);
            }
            setResult({
                communities: new Map(),
                communityColors: new Map(),
                communityLabels: new Map(),
            });
            return;
        }

        if (!graph) return;

        // Clear any pending debounce
        if (timerRef.current) {
            clearTimeout(timerRef.current);
        }

        timerRef.current = setTimeout(() => {
            // Edge cases: empty or too-small graph
            if (graph.order === 0) {
                setResult({
                    communities: new Map(),
                    communityColors: new Map(),
                    communityLabels: new Map(),
                });
                return;
            }

            if (graph.order < 3) {
                // Skip community detection for tiny graphs
                setResult({
                    communities: new Map(),
                    communityColors: new Map(),
                    communityLabels: new Map(),
                });
                return;
            }

            try {
                // Run Louvain — assigns `community` attribute to each node
                louvain.assign(graph);

                // Collect communities: communityId -> [nodeIds]
                const commMap = new Map();
                graph.forEachNode((nodeId, attrs) => {
                    const cId = attrs.community;
                    if (cId === undefined || cId === null) return;
                    if (!commMap.has(cId)) {
                        commMap.set(cId, []);
                    }
                    commMap.get(cId).push(nodeId);
                });

                // If only one community, skip drawing
                if (commMap.size <= 1) {
                    setResult({
                        communities: new Map(),
                        communityColors: new Map(),
                        communityLabels: new Map(),
                    });
                    return;
                }

                // Assign colors
                const colorMap = new Map();
                let colorIdx = 0;
                for (const cId of commMap.keys()) {
                    colorMap.set(cId, COMMUNITY_COLORS[colorIdx % COMMUNITY_COLORS.length]);
                    colorIdx++;
                }

                // Label each community by its most influential member
                const labelMap = new Map();
                for (const [cId, nodeIds] of commMap.entries()) {
                    let bestId = nodeIds[0];
                    let bestInfluence = -Infinity;
                    for (const nId of nodeIds) {
                        const attrs = graph.getNodeAttributes(nId);
                        const influence = attrs.influence || attrs.size || 0;
                        if (influence > bestInfluence) {
                            bestInfluence = influence;
                            bestId = nId;
                        }
                    }
                    const bestAttrs = graph.getNodeAttributes(bestId);
                    labelMap.set(cId, bestAttrs.label || bestId);
                }

                setResult({
                    communities: commMap,
                    communityColors: colorMap,
                    communityLabels: labelMap,
                });
            } catch (e) {
                console.warn('Community detection error:', e);
                setResult({
                    communities: new Map(),
                    communityColors: new Map(),
                    communityLabels: new Map(),
                });
            }
        }, 500);

        return () => {
            if (timerRef.current) {
                clearTimeout(timerRef.current);
            }
        };
    }, [enabled, graph, graph?.order, graph?.size]);

    return result;
}

export { COMMUNITY_COLORS };
export default useCommunities;
