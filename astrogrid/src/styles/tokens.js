/**
 * AstroGrid deep space design tokens.
 */

export const tokens = {
    bg: '#050810',
    bgGradient: 'linear-gradient(180deg, #050810 0%, #0A1628 100%)',
    card: 'rgba(15, 25, 45, 0.8)',
    cardBorder: 'rgba(74, 158, 255, 0.15)',
    accent: '#4A9EFF',
    purple: '#8B5CF6',
    gold: '#D4A574',
    green: '#22C55E',
    red: '#EF4444',
    text: '#C8D8E8',
    textMuted: '#5A7080',
    textBright: '#E8F0F8',
    fontSans: "'IBM Plex Sans', sans-serif",
    fontMono: "'IBM Plex Mono', monospace",
    radius: { sm: '8px', md: '12px', lg: '16px', xl: '24px', pill: '999px' },
    spacing: { xs: '4px', sm: '8px', md: '12px', lg: '16px', xl: '24px', xxl: '32px' },
    glass: 'backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);',
};

/** Reusable style objects */
export const styles = {
    container: {
        padding: tokens.spacing.lg,
        maxWidth: '900px',
        margin: '0 auto',
    },
    header: {
        fontSize: '22px',
        fontWeight: 700,
        color: tokens.textBright,
        marginBottom: tokens.spacing.lg,
        fontFamily: tokens.fontSans,
    },
    subheader: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '2px',
        textTransform: 'uppercase',
        color: tokens.accent,
        fontFamily: tokens.fontMono,
        marginBottom: tokens.spacing.sm,
    },
    card: {
        background: tokens.card,
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md,
        padding: '16px',
        marginBottom: tokens.spacing.md,
    },
    value: {
        fontSize: '14px',
        color: tokens.text,
        fontFamily: tokens.fontMono,
    },
    label: {
        fontSize: '12px',
        color: tokens.textMuted,
        marginBottom: tokens.spacing.xs,
    },
    metricGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))',
        gap: tokens.spacing.md,
    },
    metric: {
        background: 'rgba(10, 18, 35, 0.6)',
        borderRadius: tokens.radius.md,
        padding: tokens.spacing.md,
        textAlign: 'center',
        border: `1px solid ${tokens.cardBorder}`,
    },
    metricValue: {
        fontSize: '20px',
        fontWeight: 700,
        color: tokens.textBright,
        fontFamily: tokens.fontMono,
    },
    metricLabel: {
        fontSize: '11px',
        color: tokens.textMuted,
        marginTop: tokens.spacing.xs,
    },
    loading: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '40px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        fontSize: '13px',
    },
    error: {
        color: tokens.red,
        fontSize: '13px',
        fontFamily: tokens.fontMono,
        padding: tokens.spacing.md,
    },
};
