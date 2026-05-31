/**
 * Edge relationship-type → color encoding.
 *
 * Verifies the canonical type→color map (competitor=red, supplier=blue,
 * investor=green, government=gold, …) and that the Sigma CanvasStore applies
 * that colour from the edge's relationship type while keeping thickness=strength.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import Graph from 'graphology';

import {
    EDGE_TYPE_COLORS,
    EDGE_DEFAULT_COLOR,
    EDGE_LEGEND,
    edgeColorForType,
} from '../components/canvas/nodeStyles.js';

const { default: useCanvasStore } = await import('../canvas/CanvasStore.js');

describe('edgeColorForType — relationship type → color', () => {
    it('maps the four headline families to their spec colors', () => {
        expect(edgeColorForType('competitor')).toBe('#EF4444'); // red
        expect(edgeColorForType('supplier')).toBe('#3B82F6');   // blue
        expect(edgeColorForType('supply_chain')).toBe('#3B82F6'); // blue (alias)
        expect(edgeColorForType('investor')).toBe('#22C55E');   // green
        expect(edgeColorForType('co_investor')).toBe('#22C55E'); // green (alias)
        expect(edgeColorForType('government')).toBe('#EAB308'); // gold
        expect(edgeColorForType('committee')).toBe('#EAB308');  // gold (jurisdiction family)
    });

    it('colors the causation + member-trade chain types', () => {
        expect(edgeColorForType('causation')).toBe('#F97316');
        expect(edgeColorForType('member_trade')).toBe('#EC4899');
    });

    it('falls back to keyword matching on the label when type is unknown', () => {
        expect(edgeColorForType('mystery', 'is a competitor of')).toBe('#EF4444');
        expect(edgeColorForType(undefined, 'key supplier')).toBe('#3B82F6');
        expect(edgeColorForType(null, 'congressional oversight')).toBe('#EAB308');
    });

    it('falls back to the neutral default for fully unknown edges', () => {
        expect(edgeColorForType('totally_unknown_type')).toBe(EDGE_DEFAULT_COLOR);
        expect(edgeColorForType()).toBe(EDGE_DEFAULT_COLOR);
    });

    it('keeps the legend in sync with the color map', () => {
        // every legend entry must resolve to a real color, and the four
        // headline families must be present and correct.
        const byKey = Object.fromEntries(EDGE_LEGEND.map((e) => [e.key, e.color]));
        expect(byKey.competitor).toBe(EDGE_TYPE_COLORS.competitor);
        expect(byKey.supplier).toBe(EDGE_TYPE_COLORS.supplier);
        expect(byKey.investor).toBe(EDGE_TYPE_COLORS.investor);
        expect(byKey.government).toBe(EDGE_TYPE_COLORS.government);
        for (const entry of EDGE_LEGEND) {
            expect(entry.color).toMatch(/^#[0-9A-Fa-f]{6}$/);
        }
    });
});

describe('CanvasStore — edge color from relationship type', () => {
    beforeEach(() => {
        useCanvasStore.setState({ graph: new Graph(), visibleDepth: 6 });
    });

    it('colors a typed `relationship` edge and sizes it by strength', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'a:aapl', type: 'company', label: 'Apple' },
                { id: 'a:tsmc', type: 'company', label: 'TSMC' },
            ],
            edges: [
                { source: 'a:aapl', target: 'a:tsmc', relationship: 'supplier', strength: 0.9 },
            ],
        });

        const graph = useCanvasStore.getState().graph;
        const e = graph.edge('a:aapl', 'a:tsmc');
        expect(graph.getEdgeAttribute(e, 'color')).toBe('#3B82F6'); // supplier=blue
        expect(graph.getEdgeAttribute(e, 'edgeKind')).toBe('supplier');
        // thickness still encodes strength (0.9 * 3 + 0.5)
        expect(graph.getEdgeAttribute(e, 'size')).toBeCloseTo(3.2, 5);
    });

    it('colors `edge_type`, `type`, and competitor edges distinctly', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'n1', type: 'actor', label: 'One' },
                { id: 'n2', type: 'actor', label: 'Two' },
                { id: 'n3', type: 'actor', label: 'Three' },
            ],
            edges: [
                { source: 'n1', target: 'n2', edge_type: 'competitor', strength: 0.5 },
                { source: 'n2', target: 'n3', type: 'co_investor', strength: 0.5 },
            ],
        });

        const graph = useCanvasStore.getState().graph;
        expect(graph.getEdgeAttribute(graph.edge('n1', 'n2'), 'color')).toBe('#EF4444'); // competitor=red
        expect(graph.getEdgeAttribute(graph.edge('n2', 'n3'), 'color')).toBe('#22C55E'); // investor=green
    });

    it('falls back to neutral grey for an untyped connection edge', () => {
        useCanvasStore.getState().loadGraph({
            nodes: [
                { id: 'x', type: 'actor', label: 'X' },
                { id: 'y', type: 'actor', label: 'Y' },
            ],
            edges: [{ source: 'x', target: 'y', strength: 0.3 }],
        });
        const graph = useCanvasStore.getState().graph;
        // default edgeKind is 'connection' → mapped color
        expect(graph.getEdgeAttribute(graph.edge('x', 'y'), 'color')).toBe(EDGE_TYPE_COLORS.connection);
    });
});
