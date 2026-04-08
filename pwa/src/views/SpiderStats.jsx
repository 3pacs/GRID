/**
 * SpiderStats — Connection mapper dashboard showing graph statistics,
 * degree distribution, and source breakdown with auto-refresh.
 */
import React, { useEffect, useState, useRef } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

const SOURCE_COLORS = {
    wikidata: '#3B82F6',
    sec_crossref: '#22C55E',
    icij_offshore: '#EF4444',
    opencorporates: '#F59E0B',
    news_cooccurrence: '#8B5CF6',
    google_kg: '#06B6D4',
    operator_input: '#EC4899',
    operator: '#EC4899',
    unknown: '#6B7280',
};

function degreeColor(degree, maxDeg) {
    const t = maxDeg > 0 ? degree / maxDeg : 0;
    const r = Math.round(34 + t * 205);
    const g = Math.round(197 - t * 128);
    const b = Math.round(94 - t * 30);
    return `rgb(${r},${g},${b})`;
}

function fmtNum(n) {
    if (n == null) return '--';
    return n.toLocaleString();
}

const S = {
    page: {
        padding: '16px',
        paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)',
        maxWidth: '1000px',
        margin: '0 auto',
        fontFamily: MONO,
    },
    header: {
        display: 'flex', alignItems: 'center', gap: '10px',
        marginBottom: '20px',
    },
    title: {
        fontSize: '14px', letterSpacing: '2px',
        color: colors.textMuted, fontFamily: MONO,
    },
    dot: (alive) => ({
        width: '8px', height: '8px', borderRadius: '50%',
        background: alive ? colors.green : colors.red,
        boxShadow: alive ? `0 0 6px ${colors.green}` : 'none',
        animation: alive ? 'pulse-dot 2s ease-in-out infinite' : 'none',
    }),
    cards: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
        gap: '12px', marginBottom: '24px',
    },
    card: {
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: '8px', padding: '16px', textAlign: 'center',
    },
    cardNum: {
        fontSize: '28px', fontWeight: 700,
        color: colors.text, fontFamily: MONO, lineHeight: 1.1,
    },
    cardLabel: {
        fontSize: '9px', letterSpacing: '1.5px',
        color: colors.textMuted, marginTop: '4px',
    },
    charts: {
        display: 'grid', gridTemplateColumns: '1fr 1fr',
        gap: '16px',
    },
    chartBox: {
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: '8px', padding: '16px',
    },
    chartTitle: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.accent, fontFamily: MONO, marginBottom: '12px',
    },
    barRow: {
        display: 'flex', alignItems: 'center', gap: '8px',
        marginBottom: '6px', fontSize: '10px',
    },
    barLabel: {
        minWidth: '60px', textAlign: 'right',
        color: colors.textMuted, fontFamily: MONO,
    },
    barTrack: {
        flex: 1, height: '14px', background: colors.bg,
        borderRadius: '3px', overflow: 'hidden', position: 'relative',
    },
    barCount: {
        fontSize: '9px', color: colors.textDim,
        fontFamily: MONO, minWidth: '40px',
    },
    footer: {
        marginTop: '16px', fontSize: '9px',
        color: colors.textMuted, fontFamily: MONO,
        textAlign: 'center',
    },
};

export default function SpiderStats() {
    const [stats, setStats] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [lastUpdate, setLastUpdate] = useState(null);
    const [secondsAgo, setSecondsAgo] = useState(0);
    const timerRef = useRef(null);

    const fetchStats = async () => {
        try {
            const d = await api.getSpiderStats();
            setStats(d);
            setLastUpdate(Date.now());
            setSecondsAgo(0);
            setError(null);
        } catch (err) {
            setError(err.message || 'Failed to load spider stats');
        }
        setLoading(false);
    };

    useEffect(() => {
        fetchStats();
        const interval = setInterval(fetchStats, 30000);
        return () => clearInterval(interval);
    }, []);

    useEffect(() => {
        timerRef.current = setInterval(() => {
            if (lastUpdate) setSecondsAgo(Math.floor((Date.now() - lastUpdate) / 1000));
        }, 1000);
        return () => clearInterval(timerRef.current);
    }, [lastUpdate]);

    if (loading && !stats) {
        return (
            <div style={S.page}>
                <div style={S.header}>
                    <div style={S.dot(false)} />
                    <div style={S.title}>SPIDER · CONNECTION MAPPER</div>
                </div>
                <div style={{ color: colors.textMuted, fontSize: '12px' }}>Loading spider stats...</div>
            </div>
        );
    }

    if (error && !stats) {
        return (
            <div style={S.page}>
                <div style={S.header}>
                    <div style={S.dot(false)} />
                    <div style={S.title}>SPIDER · CONNECTION MAPPER</div>
                </div>
                <div style={{ color: colors.red, fontSize: '12px' }}>{error}</div>
                <button onClick={fetchStats} style={{ ...shared.buttonSmall, marginTop: '8px' }}>Retry</button>
            </div>
        );
    }

    const byDegree = stats?.by_degree || {};
    const bySource = stats?.by_source || {};
    const maxDeg = stats?.max_degree || 0;
    const maxDegCount = Math.max(1, ...Object.values(byDegree));
    const maxSrcCount = Math.max(1, ...Object.values(bySource));
    const sortedSources = Object.entries(bySource).sort((a, b) => b[1] - a[1]);
    const sortedDegrees = Object.entries(byDegree).sort((a, b) => Number(a[0]) - Number(b[0]));

    return (
        <div style={S.page}>
            <style>{`
                @keyframes pulse-dot {
                    0%, 100% { opacity: 1; }
                    50% { opacity: 0.4; }
                }
            `}</style>

            <div style={S.header}>
                <div style={S.dot(true)} />
                <div style={S.title}>SPIDER · CONNECTION MAPPER</div>
            </div>

            {/* Stats cards */}
            <div style={S.cards}>
                <div style={S.card}>
                    <div style={S.cardNum}>{fmtNum(stats?.total_actors)}</div>
                    <div style={S.cardLabel}>TOTAL ACTORS</div>
                </div>
                <div style={S.card}>
                    <div style={S.cardNum}>{fmtNum(stats?.total_connections)}</div>
                    <div style={S.cardLabel}>TOTAL CONNECTIONS</div>
                </div>
                <div style={S.card}>
                    <div style={{ ...S.cardNum, color: '#FFD700' }}>{maxDeg}</div>
                    <div style={S.cardLabel}>MAX DEGREE</div>
                </div>
                <div style={S.card}>
                    <div style={{ ...S.cardNum, color: colors.accent }}>{sortedSources.length}</div>
                    <div style={S.cardLabel}>SOURCES ACTIVE</div>
                </div>
            </div>

            {/* Charts */}
            <div style={S.charts}>
                {/* Degree distribution */}
                <div style={S.chartBox}>
                    <div style={S.chartTitle}>DEGREE DISTRIBUTION</div>
                    {sortedDegrees.map(([deg, count]) => (
                        <div key={deg} style={S.barRow}>
                            <div style={S.barLabel}>deg {deg}</div>
                            <div style={S.barTrack}>
                                <div style={{
                                    height: '100%',
                                    width: `${(count / maxDegCount) * 100}%`,
                                    background: degreeColor(Number(deg), maxDeg),
                                    borderRadius: '3px',
                                    transition: 'width 0.3s ease',
                                }} />
                            </div>
                            <div style={S.barCount}>{fmtNum(count)}</div>
                        </div>
                    ))}
                    {sortedDegrees.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '10px' }}>No degree data</div>
                    )}
                </div>

                {/* Source breakdown */}
                <div style={S.chartBox}>
                    <div style={S.chartTitle}>SOURCE BREAKDOWN</div>
                    {sortedSources.map(([src, count]) => (
                        <div key={src} style={S.barRow}>
                            <div style={S.barLabel}>{src}</div>
                            <div style={S.barTrack}>
                                <div style={{
                                    height: '100%',
                                    width: `${(count / maxSrcCount) * 100}%`,
                                    background: SOURCE_COLORS[src] || SOURCE_COLORS.unknown,
                                    borderRadius: '3px',
                                    transition: 'width 0.3s ease',
                                }} />
                            </div>
                            <div style={S.barCount}>{fmtNum(count)}</div>
                        </div>
                    ))}
                    {sortedSources.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '10px' }}>No source data</div>
                    )}
                </div>
            </div>

            <div style={S.footer}>
                Last updated: {secondsAgo}s ago · Auto-refresh every 30s
            </div>
        </div>
    );
}
