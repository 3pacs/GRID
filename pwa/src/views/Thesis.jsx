import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import { formatDateTime } from '../utils/formatTime.js';

// ── Styles ─────────────────────────────────────────────────────────────

const s = {
    container: { padding: tokens.space.lg, maxWidth: '1200px', margin: '0 auto' },
    header: { ...shared.header, marginBottom: tokens.space.xl },

    /* Unified thesis hero card */
    heroCard: {
        background: colors.gradientCard,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.lg,
        padding: tokens.space.xxl,
        marginBottom: tokens.space.xl,
        boxShadow: colors.shadow.lg,
        position: 'relative',
        overflow: 'hidden',
    },
    heroGlow: {
        position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
        pointerEvents: 'none', borderRadius: tokens.radius.lg,
    },
    heroTitle: {
        fontSize: tokens.fontSize.xl, fontWeight: 700, color: '#E8F0F8',
        fontFamily: colors.sans, marginBottom: tokens.space.sm,
    },
    heroSubtitle: {
        fontSize: tokens.fontSize.sm, color: colors.textMuted,
        fontFamily: colors.mono, marginBottom: tokens.space.lg,
        letterSpacing: '1px',
    },
    directionRow: {
        display: 'flex', alignItems: 'center', gap: tokens.space.lg,
        marginBottom: tokens.space.lg, flexWrap: 'wrap',
    },
    directionBadge: (dir) => ({
        display: 'inline-flex', alignItems: 'center', gap: '8px',
        padding: '10px 24px', borderRadius: tokens.radius.md,
        fontSize: '20px', fontWeight: 800, fontFamily: colors.mono,
        letterSpacing: '2px',
        background: dir === 'BULLISH' ? colors.greenBg
            : dir === 'BEARISH' ? colors.redBg
            : `${colors.border}`,
        color: dir === 'BULLISH' ? colors.green
            : dir === 'BEARISH' ? colors.red
            : colors.textDim,
        border: `1px solid ${dir === 'BULLISH' ? colors.green
            : dir === 'BEARISH' ? colors.red
            : colors.border}40`,
    }),
    convictionMeter: {
        flex: 1, minWidth: '200px',
    },
    meterTrack: {
        height: '10px', background: colors.bg, borderRadius: '5px',
        overflow: 'hidden', marginTop: tokens.space.xs,
    },
    meterFill: (pct, dir) => ({
        height: '100%', borderRadius: '5px',
        width: `${Math.min(100, Math.max(2, pct))}%`,
        background: dir === 'BULLISH' ? colors.green
            : dir === 'BEARISH' ? colors.red
            : colors.accent,
        transition: 'width 0.6s ease',
    }),
    meterLabel: {
        fontSize: tokens.fontSize.sm, color: colors.textDim,
        fontFamily: colors.mono,
    },
    narrativeBox: {
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: tokens.space.lg, fontSize: tokens.fontSize.md,
        color: colors.textDim, lineHeight: '1.7',
        fontFamily: colors.sans, marginTop: tokens.space.lg,
        border: `1px solid ${colors.borderSubtle}`,
    },

    /* Driver/risk pills */
    driverRow: {
        display: 'flex', gap: tokens.space.sm, flexWrap: 'wrap',
        marginBottom: tokens.space.md,
    },
    driverLabel: {
        fontSize: tokens.fontSize.xs, fontWeight: 700, color: colors.textMuted,
        fontFamily: colors.mono, letterSpacing: '1px', marginBottom: tokens.space.xs,
    },
    driverPill: (isBull) => ({
        display: 'inline-flex', alignItems: 'center', gap: '6px',
        padding: '6px 14px', borderRadius: tokens.radius.pill,
        fontSize: tokens.fontSize.sm, fontWeight: 600,
        background: isBull ? `${colors.green}15` : `${colors.red}15`,
        color: isBull ? colors.green : colors.red,
        border: `1px solid ${isBull ? colors.green : colors.red}30`,
        fontFamily: colors.sans,
    }),

    /* Section titles */
    sectionTitle: {
        ...shared.sectionTitle,
        marginTop: tokens.space.xl,
        marginBottom: tokens.space.md,
    },

    /* Model card grid */
    cardGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
        gap: tokens.space.md,
        marginBottom: tokens.space.xl,
    },
    modelCard: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: tokens.space.lg,
        cursor: 'pointer',
        transition: `all ${tokens.transition.fast}`,
    },
    modelCardExpanded: {
        background: colors.cardElevated,
        border: `1px solid ${colors.accent}40`,
        boxShadow: `0 0 20px ${colors.accentGlow}`,
    },
    modelHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
        marginBottom: tokens.space.sm,
    },
    modelName: {
        fontSize: tokens.fontSize.lg, fontWeight: 700, color: '#E8F0F8',
        fontFamily: colors.sans,
    },
    modelSummary: {
        fontSize: tokens.fontSize.sm, color: colors.textDim,
        fontFamily: colors.sans, lineHeight: '1.5',
        marginBottom: tokens.space.sm,
        display: '-webkit-box', WebkitLineClamp: 2,
        WebkitBoxOrient: 'vertical', overflow: 'hidden',
    },
    modelSummaryFull: {
        fontSize: tokens.fontSize.sm, color: colors.textDim,
        fontFamily: colors.sans, lineHeight: '1.5',
        marginBottom: tokens.space.sm,
    },
    badgeRow: {
        display: 'flex', gap: tokens.space.xs, flexWrap: 'wrap',
        alignItems: 'center',
    },
    signalArrow: (dir) => ({
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
        width: '28px', height: '28px', borderRadius: '50%',
        fontSize: '14px', fontWeight: 700,
        background: dir === 'bullish' ? colors.greenBg
            : dir === 'bearish' ? colors.redBg
            : colors.bg,
        color: dir === 'bullish' ? colors.green
            : dir === 'bearish' ? colors.red
            : colors.textMuted,
        border: `1px solid ${dir === 'bullish' ? colors.green
            : dir === 'bearish' ? colors.red
            : colors.border}40`,
        flexShrink: 0,
    }),
    confidenceBadge: (level) => ({
        display: 'inline-flex', padding: '2px 8px',
        borderRadius: tokens.radius.sm,
        fontSize: tokens.fontSize.xs, fontWeight: 600,
        fontFamily: colors.mono,
        background: level === 'high' ? `${colors.green}15`
            : level === 'moderate' ? `${colors.yellow}15`
            : `${colors.border}`,
        color: level === 'high' ? colors.green
            : level === 'moderate' ? colors.yellow
            : colors.textMuted,
    }),
    sourceBadge: (src) => ({
        display: 'inline-flex', padding: '2px 8px',
        borderRadius: tokens.radius.sm,
        fontSize: tokens.fontSize.xs, fontWeight: 600,
        fontFamily: colors.mono,
        background: src === 'confirmed' ? `${colors.accent}15`
            : src === 'derived' ? `${colors.yellow}15`
            : `${colors.border}`,
        color: src === 'confirmed' ? colors.accent
            : src === 'derived' ? colors.yellow
            : colors.textMuted,
    }),
    expandedContent: {
        marginTop: tokens.space.md,
        paddingTop: tokens.space.md,
        borderTop: `1px solid ${colors.borderSubtle}`,
    },
    expandedLabel: {
        fontSize: tokens.fontSize.xs, fontWeight: 700, color: colors.textMuted,
        fontFamily: colors.mono, letterSpacing: '1px',
        marginBottom: tokens.space.xs, marginTop: tokens.space.sm,
    },
    expandedValue: {
        fontSize: tokens.fontSize.md, color: colors.text,
        fontFamily: colors.sans, lineHeight: '1.6',
    },
    metricRow: {
        display: 'flex', gap: tokens.space.lg, flexWrap: 'wrap',
        marginTop: tokens.space.sm,
    },
    metricItem: {
        background: colors.bg, borderRadius: tokens.radius.sm,
        padding: '8px 12px', textAlign: 'center', minWidth: '80px',
    },
    metricVal: {
        fontSize: tokens.fontSize.lg, fontWeight: 700, color: '#E8F0F8',
        fontFamily: colors.mono,
    },
    metricLbl: {
        fontSize: tokens.fontSize.xs, color: colors.textMuted, marginTop: '2px',
    },

    /* Agreement matrix */
    matrixCard: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: tokens.space.lg,
        marginBottom: tokens.space.xl,
    },
    agreementGroup: {
        display: 'flex', alignItems: 'center', gap: tokens.space.sm,
        padding: '10px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        flexWrap: 'wrap',
    },
    agreementDir: (dir) => ({
        padding: '4px 12px', borderRadius: tokens.radius.pill,
        fontSize: tokens.fontSize.sm, fontWeight: 700,
        fontFamily: colors.mono,
        background: dir === 'bullish' ? colors.greenBg
            : dir === 'bearish' ? colors.redBg
            : colors.bg,
        color: dir === 'bullish' ? colors.green
            : dir === 'bearish' ? colors.red
            : colors.textDim,
    }),
    memberChip: {
        padding: '3px 10px', borderRadius: tokens.radius.sm,
        fontSize: tokens.fontSize.xs, fontWeight: 600,
        background: colors.bg, color: colors.text,
        fontFamily: colors.mono,
    },
    convictionTag: (level) => ({
        marginLeft: 'auto',
        padding: '3px 10px', borderRadius: tokens.radius.pill,
        fontSize: tokens.fontSize.xs, fontWeight: 700,
        fontFamily: colors.mono,
        background: level === 'high' ? `${colors.green}20` : `${colors.yellow}20`,
        color: level === 'high' ? colors.green : colors.yellow,
    }),
    contradictionRow: {
        display: 'flex', alignItems: 'center', gap: tokens.space.sm,
        padding: '10px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        flexWrap: 'wrap',
    },
    vsLabel: {
        fontSize: tokens.fontSize.xs, fontWeight: 700, color: colors.red,
        fontFamily: colors.mono,
    },

    /* Loading/error */
    loading: {
        textAlign: 'center', padding: '80px 20px',
        color: colors.textMuted, fontFamily: colors.mono,
        fontSize: tokens.fontSize.md,
    },
    errorBox: {
        background: colors.redBg, border: `1px solid ${colors.red}40`,
        borderRadius: tokens.radius.md, padding: tokens.space.lg,
        color: colors.red, fontFamily: colors.mono, fontSize: tokens.fontSize.md,
    },
    refreshBtn: {
        ...shared.buttonSmall,
        marginLeft: tokens.space.md,
    },
    scoreRow: {
        display: 'flex', gap: tokens.space.xl, marginBottom: tokens.space.md,
        flexWrap: 'wrap',
    },
    scoreItem: {
        display: 'flex', alignItems: 'center', gap: tokens.space.xs,
        fontSize: tokens.fontSize.sm, fontFamily: colors.mono,
    },
};


// ── Helper Components ──────────────────────────────────────────────────

function DirectionArrow({ direction }) {
    const arrow = direction === 'bullish' ? '\u2191' : direction === 'bearish' ? '\u2193' : '\u2194';
    return <span style={s.signalArrow(direction)}>{arrow}</span>;
}

function ConfidenceBadge({ level }) {
    return <span style={s.confidenceBadge(level)}>{level}</span>;
}

function SourceBadge({ source }) {
    return <span style={s.sourceBadge(source)}>{source}</span>;
}


// ── Model Card ─────────────────────────────────────────────────────────

function ModelCard({ thesis }) {
    const [expanded, setExpanded] = useState(false);

    const truncatedThesis = thesis.thesis && thesis.thesis.length > 120
        ? thesis.thesis.slice(0, 120) + '...'
        : thesis.thesis;

    return (
        <div
            style={{
                ...s.modelCard,
                ...(expanded ? s.modelCardExpanded : {}),
            }}
            onClick={() => setExpanded(!expanded)}
        >
            <div style={s.modelHeader}>
                <div style={{ flex: 1 }}>
                    <div style={s.modelName}>{thesis.name}</div>
                </div>
                <DirectionArrow direction={thesis.direction} />
            </div>

            <div style={expanded ? s.modelSummaryFull : s.modelSummary}>
                {expanded ? thesis.thesis : truncatedThesis}
            </div>

            <div style={s.badgeRow}>
                <ConfidenceBadge level={thesis.confidence} />
                <SourceBadge source={thesis.source} />
                {thesis.detail && thesis.detail !== 'No data' && (
                    <span style={{
                        fontSize: tokens.fontSize.xs, color: colors.textDim,
                        fontFamily: colors.mono, marginLeft: tokens.space.xs,
                    }}>
                        {thesis.detail}
                    </span>
                )}
            </div>

            {expanded && (
                <div style={s.expandedContent}>
                    <div style={s.expandedLabel}>MECHANISM</div>
                    <div style={s.expandedValue}>{thesis.mechanism}</div>

                    <div style={s.metricRow}>
                        {thesis.key_metric && (
                            <div style={s.metricItem}>
                                <div style={s.metricVal}>{thesis.key_metric}</div>
                                <div style={s.metricLbl}>Key Metric</div>
                            </div>
                        )}
                        {thesis.lead_time_days != null && (
                            <div style={s.metricItem}>
                                <div style={s.metricVal}>{thesis.lead_time_days}d</div>
                                <div style={s.metricLbl}>Lead Time</div>
                            </div>
                        )}
                        {thesis.correlation_to_spy != null && (
                            <div style={s.metricItem}>
                                <div style={s.metricVal}>{thesis.correlation_to_spy}</div>
                                <div style={s.metricLbl}>SPY Corr</div>
                            </div>
                        )}
                        {thesis.timing && (
                            <div style={s.metricItem}>
                                <div style={{ ...s.metricVal, fontSize: tokens.fontSize.sm }}>{thesis.timing}</div>
                                <div style={s.metricLbl}>Timing</div>
                            </div>
                        )}
                    </div>

                    {thesis.current_value != null && typeof thesis.current_value !== 'boolean' && (
                        <>
                            <div style={s.expandedLabel}>CURRENT VALUE</div>
                            <div style={s.expandedValue}>
                                {typeof thesis.current_value === 'object'
                                    ? JSON.stringify(thesis.current_value, null, 2)
                                    : String(thesis.current_value)}
                            </div>
                        </>
                    )}
                </div>
            )}
        </div>
    );
}


// ── Main Component ─────────────────────────────────────────────────────

export default function Thesis() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    const fetchThesis = async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await api.getThesis();
            if (result.error) {
                setError(result.message || 'Failed to load thesis');
            } else {
                setData(result);
            }
        } catch (e) {
            setError(e.message || 'Network error');
        }
        setLoading(false);
    };

    useEffect(() => { fetchThesis(); }, []);

    if (loading) {
        return <div style={s.loading}>Loading unified thesis...</div>;
    }

    if (error) {
        return (
            <div style={s.container}>
                <div style={s.header}>Thesis</div>
                <div style={s.errorBox}>{error}</div>
            </div>
        );
    }

    if (!data) return null;

    const dir = (data.overall_direction || 'NEUTRAL').toUpperCase();

    return (
        <div style={s.container}>
            {/* ── Unified Thesis Hero ── */}
            <div style={s.heroCard}>
                <div style={{
                    ...s.heroGlow,
                    background: dir === 'BULLISH'
                        ? 'radial-gradient(ellipse at 30% 20%, rgba(34,197,94,0.06) 0%, transparent 70%)'
                        : dir === 'BEARISH'
                        ? 'radial-gradient(ellipse at 30% 20%, rgba(239,68,68,0.06) 0%, transparent 70%)'
                        : 'radial-gradient(ellipse at 30% 20%, rgba(26,110,191,0.04) 0%, transparent 70%)',
                }} />
                <div style={{ position: 'relative', zIndex: 1 }}>
                    <div style={s.heroSubtitle}>UNIFIED MARKET THESIS</div>
                    <div style={s.heroTitle}>GRID's Current Thesis</div>

                    <div style={s.directionRow}>
                        <span style={s.directionBadge(dir)}>
                            {dir === 'BULLISH' ? '\u2191' : dir === 'BEARISH' ? '\u2193' : '\u2194'}
                            {' '}{dir}
                        </span>
                        <div style={s.convictionMeter}>
                            <div style={s.meterLabel}>
                                Conviction: {data.conviction}%
                            </div>
                            <div style={s.meterTrack}>
                                <div style={s.meterFill(data.conviction, dir === 'BULLISH' ? 'BULLISH' : dir === 'BEARISH' ? 'BEARISH' : 'NEUTRAL')} />
                            </div>
                        </div>
                        <button style={s.refreshBtn} onClick={fetchThesis}>
                            Refresh
                        </button>
                    </div>

                    <div style={s.scoreRow}>
                        <div style={s.scoreItem}>
                            <span style={{ color: colors.green }}>{'\u25B2'}</span>
                            Bull: {data.bullish_score}
                        </div>
                        <div style={s.scoreItem}>
                            <span style={{ color: colors.red }}>{'\u25BC'}</span>
                            Bear: {data.bearish_score}
                        </div>
                        <div style={s.scoreItem}>
                            <span style={{ color: colors.textMuted }}>{'\u25CF'}</span>
                            Active: {data.active_theses} models
                        </div>
                    </div>

                    {/* Key Drivers */}
                    {data.key_drivers && data.key_drivers.length > 0 && (
                        <>
                            <div style={s.driverLabel}>KEY DRIVERS</div>
                            <div style={s.driverRow}>
                                {data.key_drivers.map((d, i) => (
                                    <span key={i} style={s.driverPill(d.direction === 'bullish')}>
                                        {d.direction === 'bullish' ? '\u2191' : '\u2193'} {d.name}
                                    </span>
                                ))}
                            </div>
                        </>
                    )}

                    {/* Risk Factors */}
                    {data.risk_factors && data.risk_factors.length > 0 && (
                        <>
                            <div style={s.driverLabel}>RISK FACTORS</div>
                            <div style={s.driverRow}>
                                {data.risk_factors.map((r, i) => (
                                    <span key={i} style={s.driverPill(r.direction === 'bullish')}>
                                        {r.direction === 'bullish' ? '\u2191' : '\u2193'} {r.name}
                                    </span>
                                ))}
                            </div>
                        </>
                    )}

                    {/* Narrative */}
                    {data.narrative && (
                        <div style={s.narrativeBox}>
                            {data.narrative}
                        </div>
                    )}
                </div>
            </div>

            {/* ── Model Cards ── */}
            <div style={s.sectionTitle}>THESIS MODELS</div>
            <div style={s.cardGrid}>
                {(data.theses || []).map((thesis) => (
                    <ModelCard key={thesis.key} thesis={thesis} />
                ))}
            </div>

            {/* ── Agreement Matrix ── */}
            <div style={s.sectionTitle}>AGREEMENT MATRIX</div>
            <div style={s.matrixCard}>
                {/* Agreements */}
                {data.agreements && data.agreements.length > 0 ? (
                    <>
                        <div style={{
                            fontSize: tokens.fontSize.sm, fontWeight: 700,
                            color: colors.textDim, fontFamily: colors.mono,
                            marginBottom: tokens.space.sm, letterSpacing: '1px',
                        }}>
                            CONVERGENCE
                        </div>
                        {data.agreements.map((a, i) => (
                            <div key={i} style={s.agreementGroup}>
                                <span style={s.agreementDir(a.direction)}>
                                    {a.direction === 'bullish' ? '\u2191' : a.direction === 'bearish' ? '\u2193' : '\u2194'}
                                    {' '}{a.direction.toUpperCase()}
                                </span>
                                {a.members.map((m, j) => (
                                    <span key={j} style={s.memberChip}>
                                        {m.replace(/_/g, ' ')}
                                    </span>
                                ))}
                                <span style={s.convictionTag(a.conviction)}>
                                    {a.count} models = {a.conviction}
                                </span>
                            </div>
                        ))}
                    </>
                ) : (
                    <div style={{ color: colors.textMuted, fontSize: tokens.fontSize.sm, fontFamily: colors.mono }}>
                        No strong convergence detected across models.
                    </div>
                )}

                {/* Contradictions */}
                {data.contradictions && data.contradictions.length > 0 && (
                    <>
                        <div style={{
                            fontSize: tokens.fontSize.sm, fontWeight: 700,
                            color: colors.red, fontFamily: colors.mono,
                            marginTop: tokens.space.lg, marginBottom: tokens.space.sm,
                            letterSpacing: '1px',
                        }}>
                            CONTRADICTIONS
                        </div>
                        {data.contradictions.map((c, i) => (
                            <div key={i} style={s.contradictionRow}>
                                <span style={s.driverPill(true)}>
                                    {'\u2191'} {c.bullish.replace(/_/g, ' ')}
                                </span>
                                <span style={s.vsLabel}>VS</span>
                                <span style={s.driverPill(false)}>
                                    {'\u2193'} {c.bearish.replace(/_/g, ' ')}
                                </span>
                            </div>
                        ))}
                    </>
                )}
            </div>

            {/* Timestamp */}
            {data.generated_at && (
                <div style={{
                    textAlign: 'center', color: colors.textMuted,
                    fontSize: tokens.fontSize.xs, fontFamily: colors.mono,
                    paddingBottom: tokens.space.xl,
                }}>
                    Generated {formatDateTime(data.generated_at)}
                </div>
            )}
        </div>
    );
}
