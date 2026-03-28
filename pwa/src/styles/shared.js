/**
 * Shared design tokens and styles for GRID PWA.
 */

export const breakpoints = {
    mobile: '480px',
    tablet: '768px',
    desktop: '1024px',
    wide: '1440px',
};

export const responsive = {
    mobile: `@media (max-width: 480px)`,
    tablet: `@media (max-width: 768px)`,
    desktop: `@media (min-width: 1024px)`,
};

export const colors = {
    bg: '#080C10',
    card: '#0D1520',
    cardElevated: '#111B2A',
    border: '#1A2840',
    borderSubtle: '#14203A',
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
    glassOverlay: 'rgba(13, 21, 32, 0.88)',
    gradientCard: 'linear-gradient(145deg, #0D1520 0%, #111B2A 100%)',
    accentGlow: 'rgba(26, 110, 191, 0.15)',
    shadow: {
        sm: '0 1px 3px rgba(0,0,0,0.3)',
        md: '0 4px 12px rgba(0,0,0,0.4)',
        lg: '0 8px 24px rgba(0,0,0,0.5)',
    },
};

export const tokens = {
    fontSize: {
        xs: '11px',
        sm: '12px',
        md: '13px',
        lg: '15px',
        xl: '18px',
        xxl: '22px',
    },
    space: {
        xs: '4px',
        sm: '8px',
        md: '12px',
        lg: '16px',
        xl: '20px',
        xxl: '24px',
    },
    radius: {
        sm: '6px',
        md: '10px',
        lg: '14px',
        xl: '20px',
        pill: '999px',
    },
    minTouch: '44px',
    transition: {
        fast: '0.15s ease',
        normal: '0.25s ease',
        slow: '0.4s cubic-bezier(0.4, 0, 0.2, 1)',
    },
};

export const shared = {
    container: { padding: tokens.space.lg, maxWidth: '900px', margin: '0 auto' },
    header: {
        fontSize: tokens.fontSize.xxl, fontWeight: 600, color: '#E8F0F8',
        marginBottom: tokens.space.lg, fontFamily: colors.sans,
    },
    card: {
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: '14px 16px',
        marginBottom: tokens.space.sm,
    },
    cardElevated: {
        background: colors.cardElevated, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: '14px 16px',
        marginBottom: tokens.space.sm,
    },
    cardGradient: {
        background: colors.gradientCard, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: tokens.space.xl,
        marginBottom: tokens.space.sm, boxShadow: colors.shadow.md,
    },
    glassCard: {
        background: colors.glassOverlay,
        backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)',
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: '14px 16px',
        boxShadow: colors.shadow.md,
    },
    sectionTitle: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
        marginBottom: tokens.space.sm,
    },
    label: { fontSize: tokens.fontSize.sm, color: colors.textMuted, marginBottom: tokens.space.xs, display: 'block' },
    value: { fontSize: '14px', color: colors.text, fontFamily: colors.mono },
    badge: (color) => ({
        display: 'inline-flex', alignItems: 'center', padding: '3px 10px',
        borderRadius: tokens.radius.sm, fontSize: tokens.fontSize.sm,
        fontWeight: 600, background: color, color: '#fff', minHeight: '24px',
    }),
    button: {
        background: colors.accent, color: '#fff', border: 'none',
        borderRadius: tokens.radius.sm, padding: '12px 24px',
        fontSize: '14px', fontWeight: 600, cursor: 'pointer',
        fontFamily: colors.sans, minHeight: tokens.minTouch,
        transition: `all ${tokens.transition.fast}`,
    },
    buttonSmall: {
        background: colors.accent, color: '#fff', border: 'none',
        borderRadius: tokens.radius.sm, padding: '8px 16px',
        fontSize: tokens.fontSize.sm, fontWeight: 600, cursor: 'pointer',
        fontFamily: colors.sans, minHeight: '36px',
        transition: `all ${tokens.transition.fast}`,
    },
    buttonDanger: { background: '#8B1F1F' },
    buttonSuccess: { background: '#1A7A4A' },
    buttonDisabled: { background: colors.border, color: colors.textMuted, cursor: 'not-allowed' },
    input: {
        background: colors.bg, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm, color: colors.text,
        padding: '10px 12px', fontSize: '14px',
        fontFamily: colors.mono, width: '100%', boxSizing: 'border-box',
        minHeight: tokens.minTouch,
    },
    textarea: {
        background: colors.bg, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm, color: colors.text,
        padding: '10px 12px', fontSize: tokens.fontSize.md,
        fontFamily: colors.mono, width: '100%', minHeight: '80px',
        boxSizing: 'border-box', resize: 'vertical',
    },
    prose: {
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: tokens.space.lg, fontSize: tokens.fontSize.md,
        color: colors.textDim, lineHeight: '1.7',
        whiteSpace: 'pre-wrap', maxHeight: '500px', overflowY: 'auto',
        fontFamily: colors.mono,
    },
    metricGrid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(90px, 1fr))',
        gap: '10px', marginTop: tokens.space.sm,
    },
    metric: {
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: tokens.space.md, textAlign: 'center',
    },
    metricValue: {
        fontSize: tokens.fontSize.xl, fontWeight: 700, color: '#E8F0F8',
        fontFamily: colors.mono,
    },
    metricLabel: { fontSize: tokens.fontSize.xs, color: colors.textMuted, marginTop: tokens.space.xs },
    row: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '12px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        minHeight: tokens.minTouch,
    },
    error: { color: colors.red, fontSize: tokens.fontSize.md, marginTop: tokens.space.sm },
    tabs: {
        display: 'flex', gap: '6px', marginBottom: tokens.space.lg,
        overflowX: 'auto', WebkitOverflowScrolling: 'touch',
        scrollbarWidth: 'none', msOverflowStyle: 'none',
        paddingBottom: '2px',
    },
    tab: (active) => ({
        padding: '10px 18px', borderRadius: tokens.radius.md,
        fontSize: tokens.fontSize.md, fontWeight: 600,
        cursor: 'pointer', border: 'none', fontFamily: colors.sans,
        background: active ? colors.accent : colors.card,
        color: active ? '#fff' : colors.textMuted,
        minHeight: tokens.minTouch, whiteSpace: 'nowrap',
        transition: `all ${tokens.transition.fast}`,
        boxShadow: active ? '0 2px 8px rgba(26,110,191,0.3)' : 'none',
        display: 'inline-flex', alignItems: 'center',
    }),
    divider: {
        height: '1px', background: colors.borderSubtle,
        margin: `${tokens.space.md} 0`, border: 'none',
    },
};

/**
 * Mobile-adjusted values — use with isMobile from useDevice().
 * Example: padding: isMobile ? mobileOverrides.cardPadding : '14px 16px'
 */
export const mobileOverrides = {
    bodyFontSize: '13px',
    cardPadding: '12px',
    gridGap: '8px',
    containerPadding: '8px',
    noHorizontalScroll: { overflowX: 'hidden' },
};
