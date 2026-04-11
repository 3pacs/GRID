/**
 * CommunityHulls — SVG overlay that draws convex hull outlines for each
 * detected Louvain community on the Gotham Canvas.
 *
 * Must be rendered INSIDE a <SigmaContainer> so it can use useSigma().
 *
 * Approach:
 *   1. Convert each community's node positions to screen coords via sigma.graphToViewport()
 *   2. Compute convex hull (Graham scan) with padding
 *   3. Draw smooth SVG paths with community color fills/strokes
 *   4. Place label at the centroid
 *
 * The SVG is absolutely positioned over the Sigma canvas with pointer-events: none.
 * Updates on camera move (zoom, pan) and community changes.
 */

import React, { useEffect, useState, useRef, useCallback } from 'react';
import { useSigma } from '@react-sigma/core';

// ── Convex hull (Graham scan) ──

function convexHull(points) {
    if (points.length < 3) return [...points];

    const sorted = [...points].sort((a, b) => a.x - b.x || a.y - b.y);
    const cross = (O, A, B) =>
        (A.x - O.x) * (B.y - O.y) - (A.y - O.y) * (B.x - O.x);

    const lower = [];
    for (const p of sorted) {
        while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], p) <= 0)
            lower.pop();
        lower.push(p);
    }

    const upper = [];
    for (const p of [...sorted].reverse()) {
        while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], p) <= 0)
            upper.pop();
        upper.push(p);
    }

    return lower.slice(0, -1).concat(upper.slice(0, -1));
}

// ── Expand hull outward by `padding` pixels ──

function expandHull(hull, padding) {
    if (hull.length < 2) return hull;

    // Compute centroid
    let cx = 0, cy = 0;
    for (const p of hull) { cx += p.x; cy += p.y; }
    cx /= hull.length;
    cy /= hull.length;

    // Push each point outward from centroid
    return hull.map((p) => {
        const dx = p.x - cx;
        const dy = p.y - cy;
        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
        return {
            x: p.x + (dx / dist) * padding,
            y: p.y + (dy / dist) * padding,
        };
    });
}

// ── Build a smooth closed SVG path through hull points using cubic beziers ──

function hullToSmoothPath(hull) {
    if (hull.length === 0) return '';
    if (hull.length === 1) {
        // Draw a circle for single-point "hull"
        const { x, y } = hull[0];
        return `M ${x - 20},${y} a 20,20 0 1,0 40,0 a 20,20 0 1,0 -40,0`;
    }
    if (hull.length === 2) {
        // Ellipse between two points
        const [a, b] = hull;
        const mx = (a.x + b.x) / 2;
        const my = (a.y + b.y) / 2;
        const rx = Math.max(Math.abs(a.x - b.x) / 2 + 20, 25);
        const ry = Math.max(Math.abs(a.y - b.y) / 2 + 20, 25);
        return `M ${mx - rx},${my} a ${rx},${ry} 0 1,0 ${rx * 2},0 a ${rx},${ry} 0 1,0 ${-rx * 2},0`;
    }

    // Catmull-Rom to cubic bezier for smooth curve through points
    const pts = [...hull, hull[0], hull[1]]; // wrap around
    const n = hull.length;
    let d = '';

    for (let i = 0; i < n; i++) {
        const p0 = pts[i];
        const p1 = pts[i + 1];
        const p2 = pts[i + 2];
        const pPrev = i === 0 ? pts[n - 1] : pts[i - 1];

        // Catmull-Rom control points -> cubic bezier control points
        const tension = 6; // Higher = less rounding
        const cp1x = p0.x + (p1.x - pPrev.x) / tension;
        const cp1y = p0.y + (p1.y - pPrev.y) / tension;
        const cp2x = p1.x - (p2.x - p0.x) / tension;
        const cp2y = p1.y - (p2.y - p0.y) / tension;

        if (i === 0) {
            d += `M ${p0.x},${p0.y} `;
        }
        d += `C ${cp1x},${cp1y} ${cp2x},${cp2y} ${p1.x},${p1.y} `;
    }

    d += 'Z';
    return d;
}

// ── Centroid helper ──

function centroid(points) {
    if (points.length === 0) return { x: 0, y: 0 };
    let cx = 0, cy = 0;
    for (const p of points) { cx += p.x; cy += p.y; }
    return { x: cx / points.length, y: cy / points.length };
}

// ── Styles ──

const HULL_PADDING = 20;

const S = {
    overlay: {
        position: 'absolute',
        inset: 0,
        pointerEvents: 'none',
        zIndex: 1,
        overflow: 'hidden',
    },
    label: {
        fontSize: '10px',
        fontFamily: "'IBM Plex Mono', monospace",
        fontWeight: 600,
        letterSpacing: '0.5px',
        pointerEvents: 'none',
        userSelect: 'none',
    },
};

/**
 * CommunityHulls — rendered inside SigmaContainer.
 *
 * @param {{ communities: Map, communityColors: Map, communityLabels: Map, visible: boolean }} props
 */
export default function CommunityHulls({ communities, communityColors, communityLabels, visible }) {
    const sigma = useSigma();
    const [hulls, setHulls] = useState([]);
    const rafRef = useRef(null);

    // Compute screen-space hulls from graph positions
    const computeHulls = useCallback(() => {
        if (!visible || !sigma || !communities || communities.size === 0) {
            setHulls([]);
            return;
        }

        const graph = sigma.getGraph();
        const newHulls = [];

        for (const [cId, nodeIds] of communities.entries()) {
            // Get screen coordinates for all nodes in this community
            const screenPts = [];
            for (const nId of nodeIds) {
                if (!graph.hasNode(nId)) continue;
                const attrs = graph.getNodeAttributes(nId);
                if (attrs.x === undefined || attrs.y === undefined) continue;
                const viewPt = sigma.graphToViewport({ x: attrs.x, y: attrs.y });
                screenPts.push(viewPt);
            }

            if (screenPts.length < 2) continue;

            // Compute hull and expand
            const hull = convexHull(screenPts);
            const expanded = expandHull(hull, HULL_PADDING);
            const path = hullToSmoothPath(expanded);
            const center = centroid(expanded);
            const color = communityColors.get(cId) || '#3B82F6';
            const label = communityLabels.get(cId) || `Cluster ${cId}`;

            newHulls.push({ id: cId, path, center, color, label, nodeCount: nodeIds.length });
        }

        setHulls(newHulls);
    }, [sigma, communities, communityColors, communityLabels, visible]);

    // Update hulls on camera changes
    useEffect(() => {
        if (!sigma) return;

        const scheduleUpdate = () => {
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
            rafRef.current = requestAnimationFrame(computeHulls);
        };

        // Initial compute
        scheduleUpdate();

        // Listen to camera updates
        const camera = sigma.getCamera();
        camera.on('updated', scheduleUpdate);

        // Also listen to sigma refresh (e.g. after layout)
        sigma.on('afterRender', scheduleUpdate);

        return () => {
            camera.removeListener('updated', scheduleUpdate);
            sigma.removeListener('afterRender', scheduleUpdate);
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
        };
    }, [sigma, computeHulls]);

    // Re-compute when communities change
    useEffect(() => {
        computeHulls();
    }, [communities, visible]);

    if (!visible || hulls.length === 0) return null;

    return (
        <svg style={S.overlay} width="100%" height="100%">
            {hulls.map((h) => (
                <g key={h.id}>
                    {/* Hull fill + stroke */}
                    <path
                        d={h.path}
                        fill={h.color}
                        fillOpacity={0.06}
                        stroke={h.color}
                        strokeOpacity={0.20}
                        strokeWidth={1}
                    />
                    {/* Community label at centroid */}
                    <text
                        x={h.center.x}
                        y={h.center.y - 8}
                        textAnchor="middle"
                        fill={h.color}
                        fillOpacity={0.60}
                        style={S.label}
                    >
                        {h.label}
                    </text>
                    {/* Node count below label */}
                    <text
                        x={h.center.x}
                        y={h.center.y + 6}
                        textAnchor="middle"
                        fill={h.color}
                        fillOpacity={0.35}
                        style={{ ...S.label, fontSize: '9px' }}
                    >
                        {h.nodeCount} nodes
                    </text>
                </g>
            ))}
        </svg>
    );
}
