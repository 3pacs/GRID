/**
 * Shared constants and style helpers for canvas node components.
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
};

export const baseNodeStyle = {
    background: '#0D1117',
    borderRadius: 8,
    fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
    fontSize: 12,
    color: '#C8D8E8',
    minWidth: 160,
    padding: '10px 12px',
    border: '1px solid #1E2A3A',
};

export const labelStyle = {
    fontWeight: 600,
    fontSize: 13,
    marginBottom: 4,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
};

export const metaStyle = {
    fontSize: 11,
    color: '#5A7080',
    marginTop: 2,
};

export const badgeStyle = (bg) => ({
    display: 'inline-block',
    padding: '1px 6px',
    borderRadius: 4,
    fontSize: 10,
    fontWeight: 600,
    letterSpacing: '0.5px',
    background: bg,
    color: '#fff',
    marginRight: 4,
});
