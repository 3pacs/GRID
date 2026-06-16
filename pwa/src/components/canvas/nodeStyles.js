/**
 * Shared constants and style helpers for canvas node components.
 * Premium dark theme with glow effects and pulse animations.
 */

export const NODE_COLORS = {
    actor: '#8B5CF6',
    company: '#3B82F6',
    hypothesis: '#10B981',
    signal: '#F59E0B',
    note: '#6B7280',
    evidence: '#EC4899',
    chart: '#06B6D4',
    timeline: '#F97316',
    news: '#EF4444',
};

/** Build a node glow box-shadow from a hex color. */
export const nodeGlow = (color, intensity = 0.3) =>
    `0 0 12px rgba(${hexToRgb(color)}, ${intensity}), inset 0 1px 0 rgba(255,255,255,0.04)`;

/** Stronger glow for selected/active nodes. */
export const nodeGlowActive = (color) =>
    `0 0 20px rgba(${hexToRgb(color)}, 0.5), 0 0 40px rgba(${hexToRgb(color)}, 0.15), inset 0 1px 0 rgba(255,255,255,0.06)`;

function hexToRgb(hex) {
    const h = hex.replace('#', '');
    return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)].join(',');
}

export const baseNodeStyle = {
    background: 'linear-gradient(135deg, #0D1117 0%, #111820 100%)',
    borderRadius: 10,
    fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
    fontSize: 12,
    color: '#C8D8E8',
    minWidth: 170,
    padding: '11px 14px',
    border: '1.5px solid #1E2A3A',
    transition: 'box-shadow 0.3s ease, border-color 0.3s ease',
};

/** Get the full node style with type-specific glow. */
export const glowNodeStyle = (type, selected = false) => ({
    ...baseNodeStyle,
    borderColor: NODE_COLORS[type] || '#1E2A3A',
    boxShadow: selected
        ? nodeGlowActive(NODE_COLORS[type] || '#6B7280')
        : nodeGlow(NODE_COLORS[type] || '#6B7280'),
});

export const labelStyle = {
    fontWeight: 600,
    fontSize: 13,
    marginBottom: 4,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    letterSpacing: '0.01em',
};

export const metaStyle = {
    fontSize: 11,
    color: '#5A7A90',
    marginTop: 2,
    fontFamily: "'IBM Plex Mono', monospace",
};

export const badgeStyle = (bg) => ({
    display: 'inline-block',
    padding: '2px 7px',
    borderRadius: 4,
    fontSize: 10,
    fontWeight: 600,
    letterSpacing: '0.5px',
    background: bg,
    color: '#fff',
    marginRight: 4,
    boxShadow: `0 0 6px rgba(${hexToRgb(bg)}, 0.3)`,
});

/** CSS keyframes string for pulse animation on fresh nodes. */
export const pulseKeyframes = `
@keyframes nodePulse {
    0%, 100% { box-shadow: 0 0 12px rgba(59, 130, 246, 0.3); }
    50% { box-shadow: 0 0 24px rgba(59, 130, 246, 0.6), 0 0 48px rgba(59, 130, 246, 0.15); }
}
@keyframes feedPulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
}
@keyframes glowBreathing {
    0%, 100% { opacity: 0.6; }
    50% { opacity: 1; }
}
`;

/**
 * Edge color by relationship keyword.
 * Matches partial keywords in the edge label.
 */
const EDGE_RELATIONSHIP_COLORS = [
    [/compet|rival/i, '#EF4444'],       // red — competitors
    [/suppli|vendor|procure/i, '#3B82F6'], // blue — supply chain
    [/invest|fund|capital|shareholder|stake/i, '#10B981'], // green — investors
    [/govern|regulat|sanction|congress|lobby/i, '#EAB308'], // gold — government
    [/insider|form.?4|officer|director/i, '#EC4899'], // pink — insider
    [/flow|dollar|transfer/i, '#06B6D4'], // cyan — money flow
    [/lever|pull/i, '#F97316'],          // orange — lever puller
    [/predict|oracle|hypothesis/i, '#A78BFA'], // violet — prediction
];

/** Get edge stroke color based on label text, with fallback. */
export const edgeColorForLabel = (label) => {
    if (!label) return '#3B82F6';
    for (const [re, color] of EDGE_RELATIONSHIP_COLORS) {
        if (re.test(label)) return color;
    }
    return '#3B82F6';
};

/** Neutral fallback for unknown/structural edges. */
export const EDGE_DEFAULT_COLOR = '#64748B';

/**
 * Canonical relationship-type → color map for the Sigma canvas.
 *
 * Keyed by the discrete ``edge.type`` / ``relationship`` value emitted by the
 * backend (``actor_connections.relationship``, ``_RELATIONSHIP_COLORS`` in
 * ``api/routers/intelligence_actors.py``, and the intel-expand typed edges).
 * Thickness still encodes strength — this map encodes *what kind* of tie it is.
 *
 * The four headline families requested by the spec:
 *   competitor  → red    (#EF4444)
 *   supplier    → blue   (#3B82F6)
 *   investor    → green  (#22C55E)
 *   government  → gold   (#EAB308)
 * plus the remaining relationship types the backend already produces.
 */
export const EDGE_TYPE_COLORS = {
    // ── Competitors (red) ──
    competitor: '#EF4444',
    industry_peer: '#3B82F6',
    // ── Suppliers / supply chain (blue) ──
    supplier: '#3B82F6',
    supply_chain: '#3B82F6',
    customer: '#60A5FA',
    // ── Investors / capital (green) ──
    investor: '#22C55E',
    co_investor: '#22C55E',
    co_investment: '#22C55E',
    institutional_holding: '#2DD4BF',
    business_partner: '#14B8A6',
    wealth_management: '#6366F1',
    // ── Government / political (gold) ──
    government: '#EAB308',
    committee: '#EAB308',
    jurisdiction: '#EAB308',
    gov_contract: '#10B981',
    co_contractor: '#059669',
    lobbying: '#A78BFA',
    lobbying_influence: '#7C3AED',
    foreign_lobbying: '#C084FC',
    congressional_trade: '#EC4899',
    congress_insider_overlap: '#F43F5E',
    // ── Insider / officer (pink/violet) ──
    insider_trade: '#F59E0B',
    insider_cluster: '#FBBF24',
    officer_of: '#8B5CF6',
    co_traded_insider: '#FB923C',
    co_traded_congress: '#F472B6',
    // ── Causal / market mechanics ──
    causation: '#F97316',
    member_trade: '#EC4899',
    // ── Structural / generic graph edges ──
    signal_linked: '#06B6D4',
    signal_link: '#1A6EBF',
    flow: '#10B981',
    co_traded: '#8B5CF6',
    filing_related: '#64748B',
    darkpool_activity: '#38BDF8',
    connection: '#1A2332',
};

/**
 * Get edge stroke color for a discrete relationship type.
 *
 * Tries the exact type first, then falls back to keyword matching on the
 * (optional) human label, then to the neutral default. This keeps typed
 * backend edges crisp while still colouring older label-only edges.
 *
 * @param {string} [type] discrete relationship/edge type
 * @param {string} [label] human-readable edge label (fallback)
 * @returns {string} hex color
 */
export const edgeColorForType = (type, label) => {
    if (type && EDGE_TYPE_COLORS[type]) return EDGE_TYPE_COLORS[type];
    if (label) {
        for (const [re, color] of EDGE_RELATIONSHIP_COLORS) {
            if (re.test(label)) return color;
        }
    }
    return EDGE_DEFAULT_COLOR;
};

/**
 * Legend entries for the canvas edge-color encoding.
 * One row per headline relationship family (label + representative color).
 */
export const EDGE_LEGEND = [
    { key: 'competitor', label: 'Competitor', color: EDGE_TYPE_COLORS.competitor },
    { key: 'supplier', label: 'Supplier', color: EDGE_TYPE_COLORS.supplier },
    { key: 'investor', label: 'Investor', color: EDGE_TYPE_COLORS.investor },
    { key: 'government', label: 'Government', color: EDGE_TYPE_COLORS.government },
    { key: 'causation', label: 'Causation', color: EDGE_TYPE_COLORS.causation },
    { key: 'insider_trade', label: 'Insider', color: EDGE_TYPE_COLORS.insider_trade },
    { key: 'connection', label: 'Other', color: EDGE_DEFAULT_COLOR },
];

/** Handle dot style with glow. */
export const handleStyle = (color) => ({
    background: color,
    width: 8,
    height: 8,
    border: `2px solid #0D1117`,
    boxShadow: `0 0 6px ${color}`,
});
