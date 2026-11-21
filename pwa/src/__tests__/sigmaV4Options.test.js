/**
 * Sigma v4 options for the Gotham Canvas (SigmaGraph.jsx).
 *
 * Sigma v4 dropped settings-level reducers, `edgeProgramClasses`, label/colour
 * settings and the `hidden` node/edge attribute. These tests pin the v4 shape
 * GRID relies on and the hover/selection focus reducers that replaced the
 * v3 `setSetting('nodeReducer', …)` calls.
 */
import { describe, it, expect, vi } from 'vitest';
import Graph from 'graphology';

vi.mock('@react-sigma/core', () => ({
    SigmaContainer: () => null, useLoadGraph: () => vi.fn(), useSigma: () => ({}), useRegisterEvents: () => vi.fn(),
}));

const {
    SIGMA_OPTIONS, SIGMA_STYLES, CUSTOM_GRAPH_STATE, focusNodeReducer, focusEdgeReducer,
} = await import('../canvas/SigmaGraph.jsx');

function triangleWithTail() {
    const g = new Graph();
    ['a', 'b', 'c', 'd'].forEach((id) => g.addNode(id, { x: 0, y: 0, size: 4 }));
    g.addEdgeWithKey('ab', 'a', 'b', { size: 2 });
    g.addEdgeWithKey('bc', 'b', 'c', { size: 2 });
    g.addEdgeWithKey('cd', 'c', 'd', { size: 2 });
    return g;
}

const focus = (id, ids) => ({ id, neighbors: new Set(ids) });

describe('SIGMA_OPTIONS — v4 shape', () => {
    it('passes no v3-only settings', () => {
        const removed = ['edgeProgramClasses', 'nodeProgramClasses', 'nodeReducer', 'edgeReducer',
            'defaultNodeColor', 'defaultEdgeColor', 'labelFont', 'labelColor', 'labelSize', 'zIndex'];
        removed.forEach((key) => expect(SIGMA_OPTIONS.settings).not.toHaveProperty(key));
    });

    it('keeps v3 screen-pixel sizing and frees right-click for the context menu', () => {
        expect(SIGMA_OPTIONS.settings.itemSizesReference).toBe('screen');
        expect(SIGMA_OPTIONS.settings.enableCameraMouseRotation).toBe(false);
        expect(SIGMA_OPTIONS.settings.allowInvalidContainer).toBe(true);
        expect(SIGMA_OPTIONS.settings.enableEdgeEvents).toBe(true);
    });

    it('wires reducers and custom graph state at construction', () => {
        expect(SIGMA_OPTIONS.nodeReducer).toBe(focusNodeReducer);
        expect(SIGMA_OPTIONS.edgeReducer).toBe(focusEdgeReducer);
        expect(SIGMA_OPTIONS.customGraphState).toBe(CUSTOM_GRAPH_STATE);
        expect(CUSTOM_GRAPH_STATE).toEqual({ hoverFocus: null, selectFocus: null });
    });

    it('restates position bindings, since user styles replace sigma defaults', () => {
        expect(SIGMA_STYLES.nodes.x).toEqual({ attribute: 'x' });
        expect(SIGMA_STYLES.nodes.y).toEqual({ attribute: 'y' });
        expect(SIGMA_STYLES.nodes.color).toEqual({ attribute: 'color', defaultValue: '#4A5568' });
        expect(SIGMA_STYLES.edges.color).toEqual({ attribute: 'color', defaultValue: '#1A2332' });
    });

    it('maps the legacy `hidden` attribute to visibility for nodes and edges', () => {
        for (const style of [SIGMA_STYLES.nodes, SIGMA_STYLES.edges]) {
            expect(style.visibility.whenData).toEqual({ hidden: true });
            expect(style.visibility.then).toBe('hidden');
            expect(style.visibility.else).toEqual({ whenState: 'isHidden', then: 'hidden', else: 'visible' });
        }
    });

    it('forces the label of activity-highlighted nodes', () => {
        const lv = SIGMA_STYLES.nodes.labelVisibility;
        expect(lv.whenState).toBe('isHovered');
        expect(lv.else).toEqual({ whenData: { highlighted: true }, then: 'visible', else: 'auto' });
    });
});

describe('focus reducers', () => {
    const g = triangleWithTail();
    const data = { size: 4, color: '#fff' };

    it('are no-ops without focus', () => {
        expect(focusNodeReducer('a', data, {}, {}, { hoverFocus: null, selectFocus: null })).toEqual({});
        expect(focusEdgeReducer('ab', data, {}, {}, { hoverFocus: null, selectFocus: null }, g)).toEqual({});
    });

    it('selection enlarges the selected node, lifts neighbours, dims the rest', () => {
        const gs = { hoverFocus: null, selectFocus: focus('b', ['a', 'b', 'c']) };
        expect(focusNodeReducer('b', data, {}, {}, gs)).toMatchObject({ highlighted: true, labelVisibility: 'visible', size: 5.2, depth: 'topNodes' });
        expect(focusNodeReducer('a', data, {}, {}, gs)).toEqual({ depth: 'topNodes', zIndex: 1 });
        expect(focusNodeReducer('d', data, {}, {}, gs)).toMatchObject({ color: '#1A2332', labelVisibility: 'hidden' });
    });

    it('selection thickens incident edges and hides edges outside the neighbourhood', () => {
        const gs = { hoverFocus: null, selectFocus: focus('b', ['a', 'b', 'c']) };
        expect(focusEdgeReducer('ab', { size: 2 }, {}, {}, gs, g)).toMatchObject({ visibility: 'visible', size: 3 });
        expect(focusEdgeReducer('cd', { size: 2 }, {}, {}, gs, g)).toMatchObject({ visibility: 'hidden' });
    });

    it('hover wins over selection and does not enlarge the hovered node', () => {
        const gs = { hoverFocus: focus('d', ['c', 'd']), selectFocus: focus('b', ['a', 'b', 'c']) };
        expect(focusNodeReducer('d', data, {}, {}, gs)).toEqual({ depth: 'topNodes', zIndex: 1 });
        expect(focusNodeReducer('b', data, {}, {}, gs)).toMatchObject({ labelVisibility: 'hidden' });
        expect(focusEdgeReducer('cd', { size: 2 }, {}, {}, gs, g)).toEqual({ visibility: 'visible', depth: 'topEdges', zIndex: 1 });
        expect(focusEdgeReducer('ab', { size: 2 }, {}, {}, gs, g)).toMatchObject({ visibility: 'hidden' });
    });
});
