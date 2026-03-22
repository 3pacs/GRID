/**
 * Shared styles for GRID PWA views.
 */
export const colors = {
    bg: '#080C10',
    card: '#0D1520',
    border: '#1A2840',
    text: '#C8D8E8',
    textDim: '#8AA0B8',
    textMuted: '#5A7080',
    accent: '#1A6EBF',
    green: '#22C55E',
    greenBg: '#0D3320',
    red: '#EF4444',
    redBg: '#3B1111',
    yellow: '#F59E0B',
    yellowBg: '#5A3A00',
    mono: "'IBM Plex Mono', monospace",
    sans: "'IBM Plex Sans', sans-serif",
};

export const shared = {
    container: { padding: '16px', maxWidth: '900px', margin: '0 auto' },
    header: {
        fontSize: '22px', fontWeight: 600, color: '#E8F0F8',
        marginBottom: '16px', fontFamily: colors.sans,
    },
    card: {
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: '12px', padding: '16px', marginBottom: '12px',
    },
    sectionTitle: {
        fontSize: '14px', fontWeight: 600, color: colors.textDim,
        marginTop: '16px', marginBottom: '8px',
    },
    label: { fontSize: '12px', color: colors.textMuted, marginBottom: '4px', display: 'block' },
    value: { fontSize: '14px', color: colors.text, fontFamily: colors.mono },
    badge: (color) => ({
        display: 'inline-block', padding: '2px 10px', borderRadius: '6px',
        fontSize: '12px', fontWeight: 600, background: color, color: '#fff',
    }),
    button: {
        background: colors.accent, color: '#fff', border: 'none', borderRadius: '8px',
        padding: '10px 20px', fontSize: '14px', fontWeight: 600, cursor: 'pointer',
        fontFamily: colors.sans,
    },
    buttonSmall: {
        background: colors.accent, color: '#fff', border: 'none', borderRadius: '6px',
        padding: '6px 14px', fontSize: '12px', fontWeight: 600, cursor: 'pointer',
        fontFamily: colors.sans,
    },
    buttonDanger: { background: '#8B1F1F' },
    buttonSuccess: { background: '#1A7A4A' },
    buttonDisabled: { background: colors.border, color: colors.textMuted, cursor: 'not-allowed' },
    input: {
        background: colors.bg, border: `1px solid ${colors.border}`, borderRadius: '6px',
        color: colors.text, padding: '8px 12px', fontSize: '14px',
        fontFamily: colors.mono, width: '100%', boxSizing: 'border-box',
    },
    textarea: {
        background: colors.bg, border: `1px solid ${colors.border}`, borderRadius: '6px',
        color: colors.text, padding: '10px 12px', fontSize: '13px',
        fontFamily: colors.mono, width: '100%', minHeight: '80px',
        boxSizing: 'border-box', resize: 'vertical',
    },
    prose: {
        background: colors.bg, borderRadius: '8px', padding: '16px',
        fontSize: '13px', color: colors.textDim, lineHeight: '1.7',
        whiteSpace: 'pre-wrap', maxHeight: '500px', overflowY: 'auto',
        fontFamily: colors.mono,
    },
    metricGrid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))',
        gap: '10px', marginTop: '8px',
    },
    metric: {
        background: colors.bg, borderRadius: '8px', padding: '12px', textAlign: 'center',
    },
    metricValue: {
        fontSize: '18px', fontWeight: 700, color: '#E8F0F8', fontFamily: colors.mono,
    },
    metricLabel: { fontSize: '11px', color: colors.textMuted, marginTop: '4px' },
    row: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '10px 0', borderBottom: `1px solid ${colors.border}`,
    },
    error: { color: colors.red, fontSize: '13px', marginTop: '8px' },
    tabs: {
        display: 'flex', gap: '4px', marginBottom: '16px', overflowX: 'auto',
    },
    tab: (active) => ({
        padding: '8px 16px', borderRadius: '8px', fontSize: '13px', fontWeight: 600,
        cursor: 'pointer', border: 'none', fontFamily: colors.sans,
        background: active ? colors.accent : colors.card,
        color: active ? '#fff' : colors.textMuted,
    }),
};
