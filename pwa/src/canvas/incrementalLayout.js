/** Bounded FA2 scheduling against the locked 0.10.x matrix API.
 * One matrix survives every yield: repeated public assign calls reset momentum.
 */
import iterate from 'graphology-layout-forceatlas2/iterate.js';
import helpers from 'graphology-layout-forceatlas2/helpers.js';
import defaults from 'graphology-layout-forceatlas2/defaults.js';
import { createEdgeWeightGetter } from 'graphology-utils/getters.js';

export function scheduleLayoutFrame(callback) {
    if (typeof requestAnimationFrame === 'function') {
        const id = requestAnimationFrame(callback);
        return () => cancelAnimationFrame(id);
    }
    const id = setTimeout(callback, 16);
    return () => clearTimeout(id);
}

export function startIncrementalLayout(graph, {
    iterations, settings, onProgress = () => {}, onComplete = () => {},
    onCancel = () => {}, onError = () => {}, schedule = scheduleLayoutFrame,
    now = () => performance.now(), maxBatchMs = 4, maxBatchIterations = 8,
}) {
    let stopped = false, cancelScheduled = null, completed = 0, matrices, nodes, edges;
    const resolved = { ...defaults, ...settings };
    const getWeight = createEdgeWeightGetter('weight').fromEntry;
    // Capture ownership before yielding so a drag during the first paint wins.
    nodes = [];
    edges = [];
    graph.forEachNode((id, a) => nodes.push({ id, x: a.x, y: a.y, size: a.size, fixed: !!a.fixed }));
    graph.forEachEdge((id, a, source, target, sa, ta, undirected) => edges.push({ id, source, target, weight: getWeight(id, a, source, target, sa, ta, undirected) }));
    const stop = () => { stopped = true; cancelScheduled?.(); };
    function unchanged() {
        if (graph.order !== nodes.length || graph.size !== edges.length) return false;
        let valid = true, i = 0;
        graph.forEachNode((id, a) => {
            const before = nodes[i++];
            if (id !== before.id || !Object.is(a.x, before.x) || !Object.is(a.y, before.y)
                || !Object.is(a.size, before.size) || !!a.fixed !== before.fixed) valid = false;
        });
        i = 0;
        graph.forEachEdge((id, a, source, target, sa, ta, undirected) => {
            const before = edges[i++];
            if (id !== before.id || source !== before.source || target !== before.target
                || getWeight(id, a, source, target, sa, ta, undirected) !== before.weight) valid = false;
        });
        return valid;
    }
    function step() {
        if (stopped) return;
        try {
            const start = now();
            if (!unchanged()) { stop(); onCancel('Graph positions, pins or structure changed.'); return; }
            if (!matrices) {
                if (!Number.isInteger(iterations) || iterations <= 0 || maxBatchIterations < 1 || maxBatchMs <= 0) throw new Error('Invalid layout budget');
                const error = helpers.validateSettings(resolved);
                if (error) throw new Error(error.message);
                matrices = helpers.graphToByteArrays(graph, getWeight);
            }
            if (!unchanged()) { stop(); onCancel('Graph positions, pins or structure changed.'); return; }
            let batch = 0, largestIterationMs = 0;
            // One iteration is indivisible; the time budget is a soft bound.
            do {
                const iterationStart = now();
                iterate(resolved, matrices.nodes, matrices.edges);
                largestIterationMs = Math.max(largestIterationMs, now() - iterationStart);
                completed += 1;
                batch += 1;
            } while (completed < iterations && batch < maxBatchIterations && now() - start < maxBatchMs);
            onProgress({ completed, total: iterations, batchMs: now() - start, largestIterationMs });
            if (stopped) return;
            if (completed === iterations) {
                if (!unchanged()) { stop(); onCancel('Graph positions, pins or structure changed.'); return; }
                // Preserve fixed coordinates exactly (matrix storage is Float32).
                const fixed = new Map(nodes.filter((n) => n.fixed).map((n) => [n.id, n]));
                helpers.assignLayoutChanges(graph, matrices.nodes, (id, a) => {
                    if (fixed.has(id)) { a.x = fixed.get(id).x; a.y = fixed.get(id).y; }
                    return a;
                });
                stop();
                onComplete();
            } else cancelScheduled = schedule(step);
        } catch (error) { stop(); onError(error); }
    }
    // Two frame boundaries give Sigma a paint opportunity before matrix work.
    cancelScheduled = schedule(() => {
        if (!stopped) cancelScheduled = schedule(step);
    });
    return stop;
}
