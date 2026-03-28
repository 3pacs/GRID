/**
 * AstroGrid deep space design tokens.
 */

export const tokens = {
    bg: '#0B0D1A',
    bgGradient: 'radial-gradient(circle at top, rgba(74, 144, 217, 0.16) 0%, rgba(11, 13, 26, 0.0) 36%), linear-gradient(180deg, #0B0D1A 0%, #101525 100%)',
    surface: 'rgba(17, 24, 39, 0.82)',
    surfaceAlt: 'rgba(10, 18, 35, 0.74)',
    card: 'rgba(17, 24, 39, 0.82)',
    cardBorder: 'rgba(74, 144, 217, 0.18)',
    border: 'rgba(74, 144, 217, 0.18)',
    accent: '#4A90D9',
    purple: '#7C3AED',
    gold: '#F59E0B',
    green: '#22C55E',
    red: '#EF4444',
    text: '#E2E8F0',
    textMuted: '#475569',
    textBright: '#F8FAFC',
    fontSans: "'JetBrains Mono', monospace",
    fontMono: "'JetBrains Mono', monospace",
    radius: { sm: '4px', md: '8px', lg: '12px', xl: '18px', pill: '999px' },
    spacing: { xs: '4px', sm: '8px', md: '12px', lg: '16px', xl: '24px', xxl: '32px' },
    glass: 'backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);',
};

/** Reusable style objects */
export const styles = {
    container: {
        padding: 'clamp(16px, 4vw, 24px)',
        maxWidth: '1080px',
        margin: '0 auto',
    },
    header: {
        fontSize: 'clamp(20px, 3vw, 24px)',
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
        backdropFilter: 'blur(14px)',
        WebkitBackdropFilter: 'blur(14px)',
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
