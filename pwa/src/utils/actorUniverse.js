// Helpers for the actor-network / actor-universe views: the overview request
// (N + category groups), merging an ego-network expansion into the graph,
// and the computed_at freshness label. Pure functions (unit-tested).

export const NODE_COUNT_OPTIONS = [500, 2000, 5000, 10000, 20000];
export const DEFAULT_NODE_COUNT = 2000;

// Bulk lists are opt-in groups on top of the default "market" population.
export const BULK_GROUPS = [
    { id: 'pep', label: 'PEP' },
    { id: 'sanctioned', label: 'Sanctioned' },
    { id: 'donor', label: 'Donors' },
    { id: 'icij', label: 'ICIJ' },
    { id: 'watchlist', label: 'Watchlists' },
    { id: 'quarantined', label: 'Quarantined' },
];

/** Query params for GET /intelligence/actor-network; {} keeps the default URL. */
export function buildActorNetworkParams({ n = DEFAULT_NODE_COUNT, groups = [], sector = null } = {}) {
    const params = {};
    if (n && n !== DEFAULT_NODE_COUNT) params.limit = n;
    const extra = (groups || []).filter(g => BULK_GROUPS.some(b => b.id === g));
    if (extra.length) params.groups = ['market', ...extra].join(',');
    if (sector) params.sector = sector;
    return params;
}

const linkKey = (l) => {
    const s = l.source?.id || l.source;
    const t = l.target?.id || l.target;
    return s < t ? `${s}|${t}` : `${t}|${s}`;
};

/**
 * Merge an ego-network response ({nodes, edges}) into an overview graph
 * ({nodes, links}). Existing nodes are kept as-is (positions survive); new
 * nodes are flagged `expanded` and placed near the centre node when it has
 * coordinates. Returns a new object; `data` is not mutated.
 */
export function mergeEgoIntoGraph(data, ego, centerId) {
    if (!data || !ego || !Array.isArray(ego.nodes)) return data;
    const nodes = [...(data.nodes || [])];
    const byId = new Map(nodes.map(n => [n.id, n]));
    const center = byId.get(centerId);
    let added = 0;
    for (const n of ego.nodes) {
        if (byId.has(n.id)) continue;
        const node = {
            ...n,
            label: n.label || n.name || n.id,
            influence: n.influence ?? n.importance ?? 0.3,
            expanded: true,
        };
        if (center && Number.isFinite(center.x) && Number.isFinite(center.y)) {
            node.x = center.x + (Math.random() - 0.5) * 40;
            node.y = center.y + (Math.random() - 0.5) * 40;
        }
        nodes.push(node);
        byId.set(node.id, node);
        added += 1;
    }
    const links = [...(data.links || [])];
    const seen = new Set(links.map(linkKey));
    for (const e of ego.edges || []) {
        const l = { source: e.source, target: e.target, strength: e.strength ?? 0.5, relationship: e.relationship || '' };
        const k = linkKey(l);
        if (seen.has(k) || !byId.has(l.source) || !byId.has(l.target)) continue;
        seen.add(k);
        links.push(l);
    }
    return { ...data, nodes, links, expandedFrom: [...(data.expandedFrom || []), centerId], lastExpansionAdded: added };
}

/** "computed 3h ago" from an ISO timestamp (null when unknown). */
export function formatComputedAt(computedAt, now = Date.now()) {
    if (!computedAt) return null;
    const t = Date.parse(computedAt);
    if (!Number.isFinite(t)) return null;
    const mins = Math.max(0, Math.round((now - t) / 60000));
    if (mins < 60) return `computed ${mins}m ago`;
    const hours = Math.round(mins / 60);
    if (hours < 48) return `computed ${hours}h ago`;
    return `computed ${Math.round(hours / 24)}d ago`;
}
