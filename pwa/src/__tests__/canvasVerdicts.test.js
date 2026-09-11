import Graph from 'graphology';
import { describe, expect, it, beforeEach } from 'vitest';
import useCanvasStore, {
    VERDICT_COLORS,
    applyVerdictLayer,
    normalizeVerdict,
    tickerOfNode,
    verdictMapFromSweep,
} from '../canvas/CanvasStore.js';

const SWEEP = {
    horizon_days: 90,
    universe_name: 'custom',
    top_k: [
        { ticker: 'nvda', verdict: 'high', composite_score: 1.31, aggregate_conviction: 1.2, robustness_label: 'robust' },
        { ticker: 'AMD', verdict: 'moderate', composite_score: 0.8 },
        { ticker: 'XYZ', verdict: 'high', error: 'should_i_trade raised' },
        { ticker: 'AMD', verdict: 'low' }, // duplicate ignored
    ],
};

function graphWith() {
    const g = new Graph();
    g.addNode('t:nvda', { label: 'NVDA', color: '#111111', size: 6, nodeType: 'ticker', data: {} });
    g.addNode('actor:1', { label: 'F', color: '#222222', size: 6, nodeType: 'actor', data: {} });
    g.addNode('c:amd', { label: 'AMD', color: '#333333', size: 6, nodeType: 'company', data: { ticker: 'AMD' } });
    return g;
}

describe('verdictMapFromSweep', () => {
    it('keys by upper-cased ticker, keeps first occurrence, flags errors', () => {
        const m = verdictMapFromSweep(SWEEP);
        expect(Object.keys(m)).toEqual(['NVDA', 'AMD', 'XYZ']);
        expect(m.NVDA).toMatchObject({ verdict: 'high', composite: 1.31, rank: 1, robustness: 'robust' });
        expect(m.AMD.verdict).toBe('moderate');
        expect(m.XYZ.verdict).toBe('error');
        expect(verdictMapFromSweep(null)).toEqual({});
    });
});

describe('normalizeVerdict / tickerOfNode', () => {
    it('maps unknown verdicts to error and empty to no_trade', () => {
        expect(normalizeVerdict('HIGH')).toBe('high');
        expect(normalizeVerdict('weird')).toBe('error');
        expect(normalizeVerdict('')).toBe('no_trade');
    });
    it('answers only for ticker-shaped nodes', () => {
        expect(tickerOfNode('t:nvda', { label: 'NVDA', data: {} })).toBe('NVDA');
        expect(tickerOfNode('c:amd', { nodeType: 'company', label: 'AMD', data: {} })).toBe('AMD');
        expect(tickerOfNode('actor:1', { nodeType: 'actor', label: 'F', data: {} })).toBeNull();
        expect(tickerOfNode('x', { data: { ticker: 'spy' } })).toBe('SPY');
    });
});

describe('applyVerdictLayer', () => {
    it('paints matching nodes, keeps the base colour, and restores on deactivate', () => {
        const g = graphWith();
        const painted = applyVerdictLayer(g, verdictMapFromSweep(SWEEP), true);
        expect(painted).toBe(2);
        expect(g.getNodeAttribute('t:nvda', 'color')).toBe(VERDICT_COLORS.high);
        expect(g.getNodeAttribute('t:nvda', 'baseColor')).toBe('#111111');
        expect(g.getNodeAttribute('t:nvda', 'verdictRank')).toBe(1);
        expect(g.getNodeAttribute('c:amd', 'color')).toBe(VERDICT_COLORS.moderate);
        expect(g.getNodeAttribute('actor:1', 'color')).toBe('#222222');

        applyVerdictLayer(g, verdictMapFromSweep(SWEEP), false);
        expect(g.getNodeAttribute('t:nvda', 'color')).toBe('#111111');
        expect(g.getNodeAttribute('t:nvda', 'verdict')).toBeNull();
    });
});

describe('store wiring', () => {
    beforeEach(() => {
        useCanvasStore.setState({
            graph: graphWith(),
            activeLayers: new Set(['financial']),
            sweep: null,
            verdictsByTicker: {},
        });
    });

    it('setSweep stores the map and paints only once the layer is toggled on', () => {
        const { setSweep, toggleLayer } = useCanvasStore.getState();
        setSweep(SWEEP);
        let g = useCanvasStore.getState().graph;
        expect(useCanvasStore.getState().verdictsByTicker.NVDA.verdict).toBe('high');
        expect(g.getNodeAttribute('t:nvda', 'color')).toBe('#111111');

        toggleLayer('verdicts');
        g = useCanvasStore.getState().graph;
        expect(g.getNodeAttribute('t:nvda', 'color')).toBe(VERDICT_COLORS.high);

        toggleLayer('verdicts');
        expect(useCanvasStore.getState().graph.getNodeAttribute('t:nvda', 'color')).toBe('#111111');
    });

    it('loadGraph re-applies the verdict layer to freshly loaded nodes', () => {
        const { setSweep, toggleLayer, loadGraph } = useCanvasStore.getState();
        setSweep(SWEEP);
        toggleLayer('verdicts');
        loadGraph({ nodes: [{ id: 't:amd', label: 'AMD', type: 'ticker' }], edges: [] });
        const g = useCanvasStore.getState().graph;
        expect(g.getNodeAttribute('t:amd', 'color')).toBe(VERDICT_COLORS.moderate);
    });
});
