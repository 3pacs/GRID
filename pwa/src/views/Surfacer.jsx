import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
    Activity,
    AlertTriangle,
    Clock,
    Crosshair,
    RefreshCw,
    ShieldAlert,
    Target,
    TrendingUp,
} from 'lucide-react';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const styles = {
    page: {
        width: '100%',
        maxWidth: '100vw',
        minHeight: 'calc(100vh - 64px)',
        background: colors.bg,
        color: colors.text,
        padding: 18,
        boxSizing: 'border-box',
        overflowX: 'hidden',
    },
    header: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        gap: 12,
        marginBottom: 14,
        minWidth: 0,
    },
    headerText: {
        minWidth: 0,
    },
    eyebrow: {
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        color: colors.accentLight || colors.accent,
        fontSize: 11,
        fontFamily: mono,
        fontWeight: 800,
        letterSpacing: 0,
        textTransform: 'uppercase',
        marginBottom: 8,
    },
    title: {
        margin: 0,
        color: '#E8F0F8',
        fontSize: 28,
        lineHeight: 1.15,
        fontWeight: 800,
        overflowWrap: 'anywhere',
    },
    subtitle: {
        marginTop: 8,
        color: colors.textDim,
        fontSize: 14,
        lineHeight: 1.45,
        maxWidth: 620,
        minWidth: 0,
        overflowWrap: 'anywhere',
    },
    thesisStrip: {
        display: 'grid',
        gridTemplateColumns: 'minmax(180px, 0.35fr) minmax(280px, 1fr)',
        gap: 10,
        border: `1px solid ${colors.border}`,
        borderRadius: 8,
        background: colors.card,
        padding: 12,
        marginBottom: 14,
        minWidth: 0,
    },
    thesisSignal: {
        borderRight: `1px solid ${colors.borderSubtle}`,
        paddingRight: 12,
    },
    thesisDirection: {
        marginTop: 6,
        color: '#E8F0F8',
        fontSize: 22,
        fontWeight: 900,
        textTransform: 'capitalize',
    },
    thesisCopy: {
        color: colors.textDim,
        fontSize: 13,
        lineHeight: 1.5,
        overflowWrap: 'anywhere',
    },
    driverList: {
        display: 'grid',
        gap: 7,
    },
    driverItem: {
        color: colors.textDim,
        fontSize: 12,
        lineHeight: 1.45,
        overflowWrap: 'anywhere',
    },
    driverName: {
        color: '#E8F0F8',
        fontWeight: 800,
    },
    button: {
        minHeight: 38,
        display: 'inline-flex',
        alignItems: 'center',
        gap: 7,
        border: `1px solid ${colors.border}`,
        borderRadius: 8,
        background: colors.card,
        color: colors.text,
        padding: '8px 12px',
        fontSize: 12,
        fontWeight: 800,
        cursor: 'pointer',
        whiteSpace: 'nowrap',
        flexShrink: 0,
    },
    kpis: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(145px, 1fr))',
        gap: 8,
        marginBottom: 14,
    },
    kpi: {
        border: `1px solid ${colors.border}`,
        borderRadius: 8,
        background: colors.card,
        padding: 12,
        minHeight: 74,
    },
    kpiLabel: {
        color: colors.textMuted,
        fontSize: 10,
        letterSpacing: 0,
        fontFamily: mono,
        fontWeight: 800,
        textTransform: 'uppercase',
    },
    kpiValue: {
        color: '#E8F0F8',
        fontSize: 24,
        fontFamily: mono,
        fontWeight: 900,
        marginTop: 6,
        overflowWrap: 'anywhere',
    },
    shell: {
        display: 'grid',
        gridTemplateColumns: 'minmax(280px, 0.9fr) minmax(320px, 1.2fr) minmax(260px, 0.8fr)',
        gap: 12,
        alignItems: 'start',
    },
    panel: {
        border: `1px solid ${colors.border}`,
        borderRadius: 8,
        background: colors.card,
        minHeight: 220,
        overflow: 'hidden',
        minWidth: 0,
    },
    panelHeader: {
        minHeight: 42,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 8,
        padding: '10px 12px',
        borderBottom: `1px solid ${colors.border}`,
        color: colors.textDim,
        fontFamily: mono,
        fontSize: 11,
        letterSpacing: 0,
        textTransform: 'uppercase',
        fontWeight: 800,
        minWidth: 0,
    },
    queue: {
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
        padding: 10,
        maxHeight: 'calc(100vh - 260px)',
        overflowY: 'auto',
    },
    candidate: (active) => ({
        width: '100%',
        textAlign: 'left',
        border: `1px solid ${active ? colors.accent : colors.borderSubtle}`,
        borderRadius: 8,
        background: active ? colors.cardHover : colors.bg,
        color: colors.text,
        padding: 10,
        cursor: 'pointer',
    }),
    candidateTop: {
        display: 'grid',
        gridTemplateColumns: '52px 1fr',
        gap: 10,
        alignItems: 'start',
    },
    score: {
        minWidth: 52,
        minHeight: 52,
        borderRadius: 8,
        background: colors.accentGlow || `${colors.accent}22`,
        border: `1px solid ${colors.accent}`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: '#E8F0F8',
        fontFamily: mono,
        fontSize: 18,
        fontWeight: 900,
    },
    candidateTitle: {
        color: '#E8F0F8',
        fontSize: 13,
        fontWeight: 800,
        lineHeight: 1.3,
        wordBreak: 'break-word',
    },
    candidateSummary: {
        color: colors.textDim,
        fontSize: 12,
        lineHeight: 1.45,
        marginTop: 5,
        overflowWrap: 'anywhere',
    },
    chipRow: {
        display: 'flex',
        flexWrap: 'wrap',
        gap: 5,
        marginTop: 9,
    },
    chip: (tone = 'neutral') => ({
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        minHeight: 22,
        padding: '3px 7px',
        borderRadius: 6,
        fontSize: 10,
        fontFamily: mono,
        fontWeight: 800,
        background: tone === 'bullish' ? colors.greenBg :
            tone === 'bearish' ? colors.redBg :
            tone === 'play' ? colors.greenBg :
            tone === 'blocked' ? colors.redBg :
            tone === 'fresh' ? `${colors.accent}22` : colors.card,
        color: tone === 'bullish' ? colors.green :
            tone === 'bearish' ? colors.red :
            tone === 'play' ? colors.green :
            tone === 'blocked' ? colors.red :
            tone === 'fresh' ? colors.accentLight || colors.accent : colors.textDim,
        border: `1px solid ${colors.borderSubtle}`,
    }),
    scoreCaption: {
        color: colors.textMuted,
        fontSize: 9,
        fontFamily: mono,
        fontWeight: 800,
        textTransform: 'uppercase',
        marginTop: 3,
    },
    detail: {
        padding: 14,
        minWidth: 0,
    },
    detailTitle: {
        margin: 0,
        color: '#E8F0F8',
        fontSize: 22,
        lineHeight: 1.2,
        fontWeight: 850,
        wordBreak: 'break-word',
    },
    setup: {
        marginTop: 12,
        border: `1px solid ${colors.accent}`,
        borderRadius: 8,
        background: colors.bg,
        padding: 12,
    },
    setupLabel: {
        color: colors.textMuted,
        fontSize: 10,
        fontFamily: mono,
        letterSpacing: 0,
        textTransform: 'uppercase',
        fontWeight: 800,
    },
    setupText: {
        marginTop: 6,
        color: '#E8F0F8',
        fontSize: 14,
        fontWeight: 800,
        lineHeight: 1.45,
        overflowWrap: 'anywhere',
    },
    gateGrid: {
        display: 'grid',
        gap: 8,
    },
    gateRow: {
        display: 'grid',
        gridTemplateColumns: 'minmax(82px, 0.8fr) minmax(0, 1.6fr) minmax(42px, 0.45fr)',
        gap: 8,
        alignItems: 'start',
        border: `1px solid ${colors.borderSubtle}`,
        borderRadius: 8,
        background: colors.bg,
        padding: 9,
    },
    gateName: {
        color: '#E8F0F8',
        fontSize: 11,
        fontFamily: mono,
        fontWeight: 900,
        textTransform: 'capitalize',
    },
    gateDetail: {
        color: colors.textDim,
        fontSize: 11,
        lineHeight: 1.4,
        minWidth: 0,
        overflowWrap: 'anywhere',
    },
    gateScore: {
        color: colors.textDim,
        fontSize: 11,
        fontFamily: mono,
        textAlign: 'right',
    },
    paragraph: {
        color: colors.textDim,
        fontSize: 13,
        lineHeight: 1.55,
        marginTop: 12,
        overflowWrap: 'anywhere',
    },
    sectionTitle: {
        display: 'flex',
        alignItems: 'center',
        gap: 7,
        marginTop: 18,
        marginBottom: 8,
        color: colors.text,
        fontFamily: mono,
        fontSize: 11,
        letterSpacing: 0,
        textTransform: 'uppercase',
        fontWeight: 900,
    },
    evidenceList: {
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
    },
    evidenceItem: {
        borderLeft: `2px solid ${colors.accent}`,
        background: colors.bg,
        padding: '8px 10px',
        borderRadius: 6,
    },
    evidenceLabel: {
        color: '#E8F0F8',
        fontSize: 12,
        fontWeight: 800,
        lineHeight: 1.35,
    },
    evidenceDetail: {
        color: colors.textDim,
        fontSize: 12,
        lineHeight: 1.45,
        marginTop: 4,
        wordBreak: 'break-word',
    },
    rail: {
        padding: 12,
        display: 'flex',
        flexDirection: 'column',
        gap: 12,
    },
    barRow: {
        display: 'grid',
        gridTemplateColumns: '90px 1fr 42px',
        gap: 8,
        alignItems: 'center',
        minHeight: 24,
    },
    barLabel: {
        color: colors.textDim,
        fontSize: 11,
        fontFamily: mono,
        textTransform: 'capitalize',
    },
    barOuter: {
        height: 8,
        borderRadius: 6,
        background: colors.bg,
        overflow: 'hidden',
        border: `1px solid ${colors.borderSubtle}`,
    },
    barValue: (value, danger = false) => ({
        width: `${Math.max(0, Math.min(100, Number(value) || 0))}%`,
        height: '100%',
        background: danger ? colors.red : colors.accent,
    }),
    barNumber: {
        color: colors.textDim,
        fontSize: 11,
        fontFamily: mono,
        textAlign: 'right',
    },
    noteBox: {
        border: `1px solid ${colors.borderSubtle}`,
        borderRadius: 8,
        background: colors.bg,
        padding: 10,
        color: colors.textDim,
        fontSize: 12,
        lineHeight: 1.45,
    },
    empty: {
        padding: 28,
        color: colors.textMuted,
        fontSize: 13,
        lineHeight: 1.5,
        textAlign: 'center',
    },
    error: {
        border: `1px solid ${colors.red}`,
        borderRadius: 8,
        background: colors.redBg,
        color: colors.red,
        padding: 12,
        marginBottom: 12,
        fontSize: 13,
    },
    backendNotice: {
        display: 'grid',
        gridTemplateColumns: 'minmax(220px, 0.8fr) minmax(260px, 1.2fr)',
        gap: 12,
        alignItems: 'stretch',
        border: `1px solid ${colors.yellow || colors.accent}`,
        borderRadius: 8,
        background: `linear-gradient(135deg, ${colors.yellowBg || colors.card} 0%, ${colors.card} 52%, ${colors.bg} 100%)`,
        padding: 12,
        marginBottom: 14,
        minWidth: 0,
    },
    backendLead: {
        display: 'flex',
        gap: 10,
        alignItems: 'flex-start',
        borderRight: `1px solid ${colors.borderSubtle}`,
        paddingRight: 12,
    },
    backendBeacon: {
        width: 34,
        height: 34,
        flex: '0 0 34px',
        borderRadius: 8,
        border: `1px solid ${colors.yellow || colors.accent}`,
        background: colors.bg,
        color: colors.yellow || colors.accent,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        animation: 'surfacer-thump 1.2s ease-in-out infinite',
    },
    backendTitle: {
        color: '#E8F0F8',
        fontSize: 13,
        fontFamily: mono,
        fontWeight: 900,
        letterSpacing: 0,
        textTransform: 'uppercase',
        lineHeight: 1.35,
    },
    backendCopy: {
        color: colors.textDim,
        fontSize: 13,
        lineHeight: 1.5,
        marginTop: 6,
    },
    backendStats: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(118px, 1fr))',
        gap: 8,
    },
    backendStat: {
        border: `1px solid ${colors.borderSubtle}`,
        borderRadius: 8,
        background: colors.bg,
        padding: 9,
        minHeight: 60,
    },
    backendStatLabel: {
        color: colors.textMuted,
        fontSize: 9,
        fontFamily: mono,
        fontWeight: 900,
        letterSpacing: 0,
        textTransform: 'uppercase',
    },
    backendStatValue: {
        color: '#E8F0F8',
        fontSize: 18,
        fontFamily: mono,
        fontWeight: 900,
        marginTop: 5,
    },
    backendTypes: {
        display: 'flex',
        flexWrap: 'wrap',
        gap: 6,
        marginTop: 9,
    },
    brief: (tone) => ({
        display: 'grid',
        gridTemplateColumns: 'minmax(260px, 0.9fr) minmax(320px, 1.15fr) minmax(220px, 0.75fr)',
        gap: 12,
        alignItems: 'stretch',
        border: `1px solid ${tone.border}`,
        borderRadius: 8,
        background: colors.card,
        padding: 12,
        marginBottom: 14,
        minWidth: 0,
    }),
    briefLead: {
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
        borderRight: `1px solid ${colors.borderSubtle}`,
        paddingRight: 12,
        minWidth: 0,
    },
    briefStance: (tone) => ({
        display: 'inline-flex',
        width: 'fit-content',
        border: `1px solid ${tone.border}`,
        borderRadius: 6,
        color: tone.fg,
        background: tone.bg,
        padding: '4px 7px',
        fontSize: 10,
        fontFamily: mono,
        fontWeight: 900,
        letterSpacing: 0,
        textTransform: 'uppercase',
    }),
    briefHeadline: {
        color: '#E8F0F8',
        fontSize: 22,
        lineHeight: 1.2,
        fontWeight: 900,
        overflowWrap: 'anywhere',
    },
    briefAction: {
        color: '#E8F0F8',
        fontSize: 15,
        lineHeight: 1.45,
        fontWeight: 800,
        overflowWrap: 'anywhere',
    },
    blockerStrip: {
        border: `1px solid ${colors.yellow || colors.accent}`,
        borderRadius: 8,
        background: colors.bg,
        color: colors.textDim,
        padding: 9,
        fontSize: 12,
        lineHeight: 1.45,
        overflowWrap: 'anywhere',
    },
    briefList: {
        display: 'flex',
        flexDirection: 'column',
        gap: 7,
        minWidth: 0,
    },
    briefSectionGap: {
        marginTop: 7,
    },
    briefItem: {
        display: 'grid',
        gridTemplateColumns: '24px 1fr',
        gap: 9,
        color: colors.text,
        fontSize: 14,
        lineHeight: 1.45,
        alignItems: 'start',
        minWidth: 0,
    },
    briefText: {
        minWidth: 0,
        overflowWrap: 'anywhere',
    },
    briefIndex: (tone) => ({
        width: 24,
        height: 24,
        borderRadius: 6,
        color: tone.fg,
        background: tone.bg,
        border: `1px solid ${tone.border}`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontFamily: mono,
        fontSize: 12,
        fontWeight: 900,
    }),
    briefSide: {
        display: 'flex',
        flexDirection: 'column',
        gap: 8,
        minWidth: 0,
    },
    briefMetricGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
        gap: 8,
    },
    briefMetric: {
        border: `1px solid ${colors.borderSubtle}`,
        borderRadius: 8,
        background: colors.bg,
        padding: 9,
        minHeight: 56,
    },
    briefMetricValue: {
        color: '#E8F0F8',
        fontSize: 22,
        fontFamily: mono,
        fontWeight: 900,
    },
    briefButton: {
        minHeight: 42,
        border: `1px solid ${colors.accent}`,
        borderRadius: 8,
        background: colors.accent,
        color: '#fff',
        fontSize: 14,
        fontWeight: 900,
        cursor: 'pointer',
        width: '100%',
    },
    skipLink: {
        position: 'absolute',
        left: 12,
        top: 8,
        transform: 'translateY(-160%)',
        background: colors.accent,
        color: '#fff',
        borderRadius: 6,
        padding: '8px 10px',
        fontSize: 12,
        fontWeight: 800,
        zIndex: 5,
    },
};

function formatAge(freshness) {
    const hours = freshness?.age_hours;
    if (typeof hours !== 'number') return freshness?.label || 'unknown';
    if (hours < 1) return '<1h';
    if (hours < 48) return `${Math.round(hours)}h`;
    return `${Math.round(hours / 24)}d`;
}

function scoreTone(direction) {
    if (direction === 'bullish') return 'bullish';
    if (direction === 'bearish') return 'bearish';
    return 'neutral';
}

function convictionTone(label) {
    if (label === 'play') return 'play';
    if (label === 'blocked') return 'blocked';
    return 'neutral';
}

function formatSource(value) {
    return String(value || 'none')
        .replace(/_/g, ' ')
        .replace(/\b\w/g, char => char.toUpperCase());
}

function formatCount(value) {
    const number = Number(value) || 0;
    return number.toLocaleString();
}

function formatType(value) {
    return String(value || 'unknown')
        .replace(/_/g, ' ')
        .replace(/\b\w/g, char => char.toUpperCase());
}

function formatTimestamp(value) {
    if (!value) return 'pending';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return 'pending';
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function thesisDigest(thesis) {
    if (!thesis) return null;
    const drivers = Array.isArray(thesis.key_drivers) ? thesis.key_drivers.slice(0, 3) : [];
    return {
        direction: thesis.overall_direction || 'neutral',
        conviction: Math.round((Number(thesis.conviction) || 0) * 100),
        drivers,
    };
}

function Kpi({ label, value }) {
    return (
        <div style={styles.kpi}>
            <div style={styles.kpiLabel}>{label}</div>
            <div style={styles.kpiValue}>{value}</div>
        </div>
    );
}

function BackendWorkNotice({ generatedAt, loading, meta }) {
    const missingRequests = Number(meta.missing_data_requests || 0);
    const rawRequests = Number(meta.missing_data_request_objects || 0);
    const queued = Number(meta.missing_data_queued || 0);
    const skipped = Number(meta.missing_data_skipped || 0);
    const byType = Object.entries(meta.missing_data_by_type || {})
        .sort((a, b) => Number(b[1]) - Number(a[1]));
    const hasWork = loading || missingRequests > 0 || queued > 0 || skipped > 0 || rawRequests > 0;

    if (!hasWork) return null;

    const copy = loading
        ? 'Surfacer is loading candidates and evidence backlog state. Keep the queue in research until the sync completes.'
        : 'Evidence gaps are queued or processing. Candidates stay below sizing threshold until required data clears.';

    return (
        <section
            aria-busy={loading}
            aria-live="polite"
            className="surfacer-backend-notice"
            role="status"
            style={styles.backendNotice}
        >
            <div className="surfacer-backend-lead" style={styles.backendLead}>
                <div style={styles.backendBeacon}>
                    <RefreshCw size={17} />
                </div>
                <div>
                    <div style={styles.backendTitle}>Evidence Backfill Running</div>
                    <div style={styles.backendCopy}>{copy}</div>
                    {byType.length ? (
                        <div style={styles.backendTypes}>
                            {byType.map(([type, count]) => (
                                <span key={type} style={styles.chip('fresh')}>
                                    {formatType(type)}: {formatCount(count)}
                                </span>
                            ))}
                        </div>
                    ) : null}
                </div>
            </div>
            <div style={styles.backendStats}>
                <div style={styles.backendStat}>
                    <div style={styles.backendStatLabel}>Unique Gaps</div>
                    <div style={styles.backendStatValue}>{formatCount(missingRequests)}</div>
                </div>
                <div style={styles.backendStat}>
                    <div style={styles.backendStatLabel}>Raw Requests</div>
                    <div style={styles.backendStatValue}>{formatCount(rawRequests)}</div>
                </div>
                <div style={styles.backendStat}>
                    <div style={styles.backendStatLabel}>Queued Now</div>
                    <div style={styles.backendStatValue}>{formatCount(queued)}</div>
                </div>
                <div style={styles.backendStat}>
                    <div style={styles.backendStatLabel}>Already Cooking</div>
                    <div style={styles.backendStatValue}>{formatCount(skipped)}</div>
                </div>
                <div style={styles.backendStat}>
                    <div style={styles.backendStatLabel}>Last Sync</div>
                    <div style={styles.backendStatValue}>{formatTimestamp(generatedAt)}</div>
                </div>
            </div>
        </section>
    );
}

function briefTone(posture) {
    if (posture === 'act') {
        return { fg: colors.green, bg: colors.greenBg, border: colors.green };
    }
    if (posture === 'watch' || posture === 'backfill') {
        return { fg: colors.yellow, bg: colors.yellowBg, border: colors.yellow };
    }
    if (posture === 'stand_down') {
        return { fg: colors.red, bg: colors.redBg, border: colors.red };
    }
    return { fg: colors.accentLight || colors.accent, bg: colors.accentGlow || `${colors.accent}22`, border: colors.accent };
}

function OperatorBrief({ brief, loading, onSelectCandidate }) {
    if (!brief && !loading) return null;
    const fallback = {
        posture: 'backfill',
        stance: 'Loading',
        headline: 'Checking the front page',
        primary_action: 'Waiting for candidates and evidence state.',
        next_actions: ['Hold until the sync finishes.'],
        blockers: [],
        label_counts: {},
    };
    const data = brief || fallback;
    const tone = briefTone(data.posture);
    const counts = data.label_counts || {};
    const blockers = data.blockers || [];
    const actBlockers = data.act_blockers || [];
    const actions = data.next_actions?.length ? data.next_actions : ['Refresh after the next ingestion cycle.'];
    const logic = data.decision_path?.length ? data.decision_path : ['Decision logic is pending.'];

    return (
        <section className="surfacer-brief" style={styles.brief(tone)} aria-live="polite">
            <div className="surfacer-brief-lead" style={styles.briefLead}>
                <span style={styles.briefStance(tone)}>{data.stance || 'Stand down'}</span>
                <div style={styles.briefHeadline}>{data.headline || 'Nothing cleared the front page'}</div>
                <div style={styles.briefAction}>{data.primary_action}</div>
                {actBlockers.length ? (
                    <div style={styles.blockerStrip}>
                        Blocking gates: {actBlockers.join(', ')}.
                    </div>
                ) : null}
                {data.selected_candidate_id ? (
                    <button
                        type="button"
                        style={styles.briefButton}
                        onClick={() => onSelectCandidate(data.selected_candidate_id)}
                    >
                        Show me this setup
                    </button>
                ) : null}
            </div>
            <div style={styles.briefList}>
                <div style={styles.kpiLabel}>Why</div>
                {logic.map((step, index) => (
                    <div key={`${step}-${index}`} style={styles.briefItem}>
                        <span style={styles.briefIndex(tone)}>{index + 1}</span>
                        <span style={styles.briefText}>{step}</span>
                    </div>
                ))}
                <div style={{ ...styles.kpiLabel, ...styles.briefSectionGap }}>Do Next</div>
                {actions.map((action, index) => (
                    <div key={`${action}-${index}`} style={styles.briefItem}>
                        <span style={styles.briefIndex(tone)}>{index + 1}</span>
                        <span style={styles.briefText}>{action}</span>
                    </div>
                ))}
            </div>
            <aside style={styles.briefSide}>
                <div className="surfacer-brief-metrics" style={styles.briefMetricGrid}>
                    <div className="surfacer-brief-metric" style={styles.briefMetric}>
                        <div style={styles.kpiLabel}>Ready</div>
                        <div className="surfacer-brief-metric-value" style={styles.briefMetricValue}>{counts.play || 0}</div>
                    </div>
                    <div className="surfacer-brief-metric" style={styles.briefMetric}>
                        <div style={styles.kpiLabel}>Wait</div>
                        <div className="surfacer-brief-metric-value" style={styles.briefMetricValue}>{counts.watch || 0}</div>
                    </div>
                    <div className="surfacer-brief-metric" style={styles.briefMetric}>
                        <div style={styles.kpiLabel}>Research</div>
                        <div className="surfacer-brief-metric-value" style={styles.briefMetricValue}>{counts.research || 0}</div>
                    </div>
                    <div className="surfacer-brief-metric" style={styles.briefMetric}>
                        <div style={styles.kpiLabel}>Score</div>
                        <div className="surfacer-brief-metric-value" style={styles.briefMetricValue}>{data.selected_score ?? '-'}</div>
                    </div>
                </div>
                {blockers.length ? (
                    <div style={styles.noteBox}>
                        {blockers.join(' · ')}
                    </div>
                ) : (
                    <div style={styles.noteBox}>No queue-wide blockers attached.</div>
                )}
            </aside>
        </section>
    );
}

function ScoreBars({ parts }) {
    const rows = Object.entries(parts || {});
    if (!rows.length) {
        return <div style={styles.noteBox}>No score anatomy available yet.</div>;
    }
    return rows.map(([label, value]) => (
        <div key={label} style={styles.barRow}>
            <div style={styles.barLabel}>{label.replace('_', ' ')}</div>
            <div style={styles.barOuter}>
                <div style={styles.barValue(value, label.includes('penalty'))} />
            </div>
            <div style={styles.barNumber}>{Math.round(Number(value) || 0)}</div>
        </div>
    ));
}

function CandidateCard({ candidate, active, onSelect }) {
    const conviction = candidate.conviction || {};
    const score = Math.round(conviction.score ?? candidate.alpha_score ?? 0);
    return (
        <button type="button" style={styles.candidate(active)} onClick={() => onSelect(candidate.id)}>
            <div className="surfacer-candidate-top" style={styles.candidateTop}>
                <div>
                    <div className="surfacer-score" style={styles.score}>{score}</div>
                    <div style={styles.scoreCaption}>Score</div>
                </div>
                <div>
                    <div style={styles.candidateTitle}>{candidate.title}</div>
                    <div style={styles.candidateSummary}>{conviction.summary || candidate.why_now || candidate.summary}</div>
                </div>
            </div>
            <div style={styles.chipRow}>
                <span style={styles.chip(convictionTone(conviction.label))}>{conviction.action || 'Research'}</span>
                <span style={styles.chip(scoreTone(candidate.direction))}>{candidate.direction || 'watch'}</span>
                <span style={styles.chip(candidate.freshness?.label === 'fresh' ? 'fresh' : 'neutral')}>
                    <Clock size={11} />
                    {formatAge(candidate.freshness)}
                </span>
                <span style={styles.chip()}>{candidate.horizon || 'watch'}</span>
                {(candidate.tickers || []).slice(0, 3).map(ticker => (
                    <span key={ticker} style={styles.chip()}>{ticker}</span>
                ))}
            </div>
        </button>
    );
}

export default function Surfacer() {
    const [payload, setPayload] = useState(null);
    const [selectedId, setSelectedId] = useState('');
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');

    const load = useCallback(async () => {
        setLoading(true);
        setError('');
        const data = await api.get('/api/v1/surfacer/candidates?limit=18&fresh_only=false');
        if (data?.error) {
            setError(data.message || 'Surfacer failed to load.');
            setPayload({ candidates: [], meta: {} });
        } else {
            setPayload(data);
            setSelectedId(current => current || data?.candidates?.[0]?.id || '');
        }
        setLoading(false);
    }, []);

    useEffect(() => {
        load();
    }, [load]);

    const candidates = payload?.candidates || [];
    const selected = useMemo(
        () => candidates.find(item => item.id === selectedId) || candidates[0],
        [candidates, selectedId],
    );
    const meta = payload?.meta || {};
    const topSource = Object.entries(meta.sources || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || 'none';
    const thesis = thesisDigest(payload?.thesis);

    return (
        <div className="surfacer-page" style={styles.page}>
            <style>
                {`
                    .surfacer-page, .surfacer-page * { box-sizing: border-box; max-width: 100%; }
                    .surfacer-skip:focus { transform: translateY(0) !important; }
                    @media (max-width: 1060px) {
                        .surfacer-shell { grid-template-columns: 1fr !important; }
                        .surfacer-queue { max-height: none !important; }
                        .surfacer-thesis { grid-template-columns: 1fr !important; }
                        .surfacer-thesis-signal { border-right: none !important; border-bottom: 1px solid ${colors.borderSubtle} !important; padding-right: 0 !important; padding-bottom: 10px !important; }
                        .surfacer-backend-notice { grid-template-columns: 1fr !important; }
                        .surfacer-backend-lead { border-right: none !important; border-bottom: 1px solid ${colors.borderSubtle} !important; padding-right: 0 !important; padding-bottom: 10px !important; }
                        .surfacer-brief { grid-template-columns: 1fr !important; }
                        .surfacer-brief-lead { border-right: none !important; border-bottom: 1px solid ${colors.borderSubtle} !important; padding-right: 0 !important; padding-bottom: 10px !important; }
                    }
                    @media (max-width: 720px) {
                        .surfacer-page { padding: 10px !important; }
                        .surfacer-header { flex-direction: column !important; align-items: stretch !important; }
                        .surfacer-refresh { width: 100% !important; justify-content: center !important; min-height: 44px !important; }
                        .surfacer-title { font-size: 24px !important; }
                        .surfacer-subtitle { font-size: 13px !important; max-width: none !important; }
                        .surfacer-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)) !important; }
                        .surfacer-kpis > div { min-height: 66px !important; padding: 10px !important; }
                        .surfacer-shell .surfacer-detail-panel { order: 1; }
                        .surfacer-shell .surfacer-queue-panel { order: 2; }
                        .surfacer-shell .surfacer-check-panel { order: 3; }
                        .surfacer-panel-header { min-height: 38px !important; padding: 9px 10px !important; font-size: 10px !important; }
                        .surfacer-gate-row { grid-template-columns: 1fr !important; }
                        .surfacer-gate-score { text-align: left !important; }
                        .surfacer-brief-metrics { grid-template-columns: repeat(4, minmax(0, 1fr)) !important; }
                        .surfacer-brief-metric { min-height: 50px !important; padding: 7px !important; }
                        .surfacer-brief-metric-value { font-size: 18px !important; }
                    }
                    @media (max-width: 560px) {
                        .surfacer-kpis { grid-template-columns: 1fr !important; }
                        .surfacer-brief-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)) !important; }
                        .surfacer-candidate-top { grid-template-columns: 44px 1fr !important; }
                        .surfacer-score { min-width: 44px !important; min-height: 44px !important; font-size: 16px !important; }
                    }
                    @keyframes surfacer-thump {
                        0%, 100% { transform: scale(1); opacity: 0.72; }
                        50% { transform: scale(1.08); opacity: 1; }
                    }
                `}
            </style>
            <a className="surfacer-skip" href="#selected-setup" style={styles.skipLink}>Skip to selected setup</a>
            <div className="surfacer-header" style={styles.header}>
                <div style={styles.headerText}>
                    <div style={styles.eyebrow}><Crosshair size={15} /> Surfacer</div>
                    <h1 className="surfacer-title" style={styles.title}>What can I do right now?</h1>
                    <div className="surfacer-subtitle" style={styles.subtitle}>
                        Read the decision first. If it says wait, do not size it. The reasons and next step are directly below.
                    </div>
                </div>
                <button type="button" className="surfacer-refresh" style={styles.button} onClick={load} disabled={loading}>
                    <RefreshCw size={15} />
                    {loading ? 'Loading' : 'Refresh'}
                </button>
            </div>

            {error ? <div style={styles.error}>{error}</div> : null}
            <BackendWorkNotice generatedAt={payload?.generated_at} loading={loading} meta={meta} />
            <OperatorBrief brief={payload?.brief} loading={loading} onSelectCandidate={setSelectedId} />

            <div className="surfacer-kpis" style={styles.kpis}>
                <Kpi label="Setups" value={meta.count ?? candidates.length} />
                <Kpi label="Ready" value={meta.actionable_count ?? 0} />
                <Kpi label="Avg Score" value={meta.average_conviction ?? 0} />
                <Kpi label="Main Source" value={formatSource(topSource)} />
            </div>

            {thesis ? (
                <section className="surfacer-thesis" style={styles.thesisStrip}>
                    <div className="surfacer-thesis-signal" style={styles.thesisSignal}>
                        <div style={styles.kpiLabel}>Market View</div>
                        <div style={styles.thesisDirection}>{thesis.direction}</div>
                        <div style={styles.thesisCopy}>{thesis.conviction}% conviction</div>
                    </div>
                    <div>
                        <div style={styles.kpiLabel}>Why It Matters</div>
                        <div style={styles.driverList}>
                            {thesis.drivers.map(driver => (
                                <div key={driver.key || driver.name} style={styles.driverItem}>
                                    <span style={styles.driverName}>{driver.name || driver.key}:</span>{' '}
                                    {driver.detail}
                                </div>
                            ))}
                        </div>
                    </div>
                </section>
            ) : null}

            <div className="surfacer-shell" style={styles.shell}>
                <section className="surfacer-queue-panel" style={styles.panel}>
                    <div className="surfacer-panel-header" style={styles.panelHeader}>
                        <span>Other Setups</span>
                        <span>{loading ? 'syncing' : `${candidates.length} live`}</span>
                    </div>
                    <div className="surfacer-queue" style={styles.queue}>
                        {loading ? <div style={styles.empty}>Loading current candidates.</div> : null}
                        {!loading && candidates.length === 0 ? (
                            <div style={styles.empty}>No candidates cleared the filters.</div>
                        ) : null}
                        {candidates.map(candidate => (
                            <CandidateCard
                                key={candidate.id}
                                candidate={candidate}
                                active={candidate.id === selected?.id}
                                onSelect={setSelectedId}
                            />
                        ))}
                    </div>
                </section>

                <section id="selected-setup" className="surfacer-detail-panel" style={styles.panel}>
                    <div className="surfacer-panel-header" style={styles.panelHeader}>
                        <span>Selected Setup</span>
                        <span>{selected?.status || 'idle'}</span>
                    </div>
                    {selected ? (
                        <div style={styles.detail}>
                            {(() => {
                                const conviction = selected.conviction || {};
                                return (
                                    <div style={styles.setup}>
                                        <div style={styles.setupLabel}>Decision Score</div>
                                        <div style={styles.setupText}>
                                            {conviction.action || 'Research'} · {Math.round(conviction.score ?? 0)}/100
                                        </div>
                                        <div style={styles.paragraph}>{conviction.summary || 'No conviction gate attached yet.'}</div>
                                    </div>
                                );
                            })()}
                            <h2 style={styles.detailTitle}>{selected.title}</h2>
                            <div style={styles.chipRow}>
                                <span style={styles.chip(convictionTone(selected.conviction?.label))}>{selected.conviction?.label || 'research'}</span>
                                <span style={styles.chip(scoreTone(selected.direction))}>{selected.direction}</span>
                                <span style={styles.chip()}>{Math.round((selected.confidence || 0) * 100)}% confidence</span>
                                <span style={styles.chip()}>{selected.horizon}</span>
                            </div>

                            <div style={styles.setup}>
                                <div style={styles.setupLabel}>Trade Idea</div>
                                <div style={styles.setupText}>{selected.trade_expression}</div>
                            </div>

                            <div style={styles.paragraph}>{selected.summary}</div>
                            <div style={styles.paragraph}>{selected.why_now}</div>

                            <div style={styles.sectionTitle}><Activity size={14} /> Evidence</div>
                            <div style={styles.evidenceList}>
                                {(selected.evidence || []).length ? selected.evidence.map((item, idx) => (
                                    <div key={`${item.source}-${idx}`} style={styles.evidenceItem}>
                                        <div style={styles.evidenceLabel}>{item.label}</div>
                                        <div style={styles.evidenceDetail}>{item.detail}</div>
                                    </div>
                                )) : <div style={styles.noteBox}>No evidence payload has been attached yet.</div>}
                            </div>
                        </div>
                    ) : (
                        <div style={styles.empty}>Select a candidate to inspect the evidence.</div>
                    )}
                </section>

                <aside className="surfacer-check-panel" style={styles.panel}>
                    <div className="surfacer-panel-header" style={styles.panelHeader}>
                        <span>Safety Checks</span>
                        <span>{selected ? Math.round(selected.alpha_score || 0) : '-'}</span>
                    </div>
                    {selected ? (
                        <div style={styles.rail}>
                            <div>
                                <div style={styles.sectionTitle}><Target size={14} /> Conviction Checks</div>
                                <div style={styles.gateGrid}>
                                    {(selected.conviction?.gates || []).length ? selected.conviction.gates.map(gate => (
                                        <div key={gate.name} className="surfacer-gate-row" style={styles.gateRow}>
                                            <div style={styles.gateName}>{gate.name}</div>
                                            <div style={styles.gateDetail}>{gate.detail}</div>
                                            <div className="surfacer-gate-score" style={styles.gateScore}>{Math.round(gate.score)}/{Math.round(gate.weight)}</div>
                                        </div>
                                    )) : <div style={styles.noteBox}>Conviction checks have not run yet.</div>}
                                </div>
                            </div>

                            <div>
                                <div style={styles.sectionTitle}><TrendingUp size={14} /> Score Anatomy</div>
                                <ScoreBars parts={selected.score_parts} />
                            </div>

                            <div>
                                <div style={styles.sectionTitle}><ShieldAlert size={14} /> Invalidation</div>
                                <div style={styles.noteBox}>{selected.invalidation}</div>
                            </div>

                            <div>
                                <div style={styles.sectionTitle}><AlertTriangle size={14} /> Contradictions</div>
                                {(selected.contradictions || []).length ? selected.contradictions.map((item, idx) => (
                                    <div key={idx} style={styles.noteBox}>{item}</div>
                                )) : <div style={styles.noteBox}>No anti-signal attached.</div>}
                            </div>

                            <div>
                                <div style={styles.sectionTitle}><Target size={14} /> Source Stack</div>
                                <div style={styles.chipRow}>
                                    {(selected.source_modules || []).map(source => (
                                        <span key={source} style={styles.chip()}>{source}</span>
                                    ))}
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div style={styles.empty}>No candidate selected.</div>
                    )}
                </aside>
            </div>
        </div>
    );
}
