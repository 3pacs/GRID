/**
 * IntelDashboard -- Intelligence command center.
 * Every displayed metric, badge, and count is interactive.
 */
import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const hoverBrighten = {
    onMouseEnter: (e) => { e.currentTarget.style.filter = 'brightness(1.2)'; e.currentTarget.style.transform = 'translateY(-2px)'; },
    onMouseLeave: (e) => { e.currentTarget.style.filter = 'brightness(1)'; e.currentTarget.style.transform = 'translateY(0)'; },
};

export default function IntelDashboard({ onNavigate }) {
    const [trustSources, setTrustSources] = useState(null);
    const [convergence, setConvergence] = useState(null);
    const [crossRef, setCrossRef] = useState(null);
    const [briefing, setBriefing] = useState(null);
    const [loading, setLoading] = useState(true);
    const [expandedSource, setExpandedSource] = useState(null);
    const [expandedAlert, setExpandedAlert] = useState(null);

    const loadData = useCallback(async () => {
        setLoading(true);
        try {
            const [ts, conv, xref, brief] = await Promise.all([
                api.getTrustScores?.().catch(() => null),
                api.getConvergenceAlerts?.().catch(() => null),
                api.getCrossReference?.().catch(() => null),
                api.getLatestBriefing?.('hourly').catch(() => null),
            ]);
            setTrustSources(ts);
            setConvergence(conv);
            setCrossRef(xref);
            setBriefing(brief);
        } catch {
            // graceful degradation
        }
        setLoading(false);
    }, []);

    useEffect(() => { loadData(); }, [loadData]);

    // Derived counts
    const redFlagCount = crossRef?.redFlags?.length || 0;
    const alertCount = convergence?.alerts?.length || 0;
    const sourceCount = trustSources?.sources?.length || 0;

    // Placeholder sources if API not wired
    const sources = trustSources?.sources || [
        { name: 'FRED', trust_score: 0.94, accuracy_30d: 0.96, signals: 142, category: 'macro' },
        { name: 'BLS', trust_score: 0.87, accuracy_30d: 0.89, signals: 38, category: 'employment' },
        { name: 'Unusual Whales', trust_score: 0.72, accuracy_30d: 0.68, signals: 256, category: 'flow' },
        { name: 'Congressional Trades', trust_score: 0.81, accuracy_30d: 0.77, signals: 24, category: 'insider' },
        { name: 'Dark Pool (FINRA)', trust_score: 0.76, accuracy_30d: 0.71, signals: 89, category: 'flow' },
        { name: 'Polymarket', trust_score: 0.69, accuracy_30d: 0.65, signals: 67, category: 'prediction' },
        { name: 'Satellite/Alt Data', trust_score: 0.83, accuracy_30d: 0.80, signals: 31, category: 'physical' },
        { name: 'Reddit (Trust-Scored)', trust_score: 0.44, accuracy_30d: 0.38, signals: 412, category: 'social' },
    ];

    const alerts = convergence?.alerts || [
        { ticker: 'NVDA', type: 'multi-signal', message: 'Congressional buy + unusual call flow + dark pool accumulation', severity: 'high', timestamp: new Date().toISOString() },
        { ticker: 'TLT', type: 'divergence', message: 'Fed liquidity expanding but bond prices falling -- divergence', severity: 'medium', timestamp: new Date().toISOString() },
        { ticker: 'SPY', type: 'regime-shift', message: 'Regime probability shifting from GROWTH to FRAGILE', severity: 'high', timestamp: new Date().toISOString() },
    ];

    const trustColor = (score) => {
        if (score >= 0.85) return colors.green;
        if (score >= 0.7) return colors.yellow;
        if (score >= 0.5) return '#F97316';
        return colors.red;
    };

    const severityColor = (sev) => {
        if (sev === 'high') return colors.red;
        if (sev === 'medium') return colors.yellow;
        return colors.accent;
    };

    return (
        <div style={{ ...shared.container, maxWidth: '1200px' }}>
            {/* Header */}
            <div style={{ marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>THE INTELLIGENCE</div>
                <div style={{
                    fontSize: tokens.fontSize.xxl, fontWeight: 600, color: '#E8F0F8',
                    fontFamily: SANS,
                }}>
                    Intelligence Command Center
                </div>
            </div>

            {/* ── Metric Cards Row ── */}
            <div style={{
                display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)',
                gap: tokens.space.sm, marginBottom: tokens.space.lg,
            }}>
                {/* Red Flags */}
                <div
                    onClick={() => onNavigate?.('cross-reference')}
                    title="Click to view cross-reference engine"
                    style={{
                        ...shared.cardGradient,
                        textAlign: 'center', padding: tokens.space.md,
                        cursor: 'pointer', transition: 'all 0.2s ease',
                        borderLeft: `3px solid ${redFlagCount > 0 ? colors.red : colors.green}`,
                    }}
                    {...hoverBrighten}
                >
                    <div style={{
                        fontSize: '28px', fontWeight: 800, fontFamily: MONO,
                        color: redFlagCount > 0 ? colors.red : colors.green,
                    }}>{redFlagCount}</div>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.textMuted, fontFamily: MONO, marginTop: '4px',
                    }}>RED FLAGS</div>
                </div>

                {/* Convergence Alerts */}
                <div
                    onClick={() => {
                        const el = document.getElementById('intel-alerts');
                        el?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                    }}
                    title="Click to scroll to convergence alerts"
                    style={{
                        ...shared.cardGradient,
                        textAlign: 'center', padding: tokens.space.md,
                        cursor: 'pointer', transition: 'all 0.2s ease',
                        borderLeft: `3px solid ${alertCount > 0 ? colors.yellow : colors.green}`,
                    }}
                    {...hoverBrighten}
                >
                    <div style={{
                        fontSize: '28px', fontWeight: 800, fontFamily: MONO,
                        color: alertCount > 0 ? colors.yellow : colors.green,
                    }}>{alertCount}</div>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.textMuted, fontFamily: MONO, marginTop: '4px',
                    }}>CONVERGENCE ALERTS</div>
                </div>

                {/* Active Sources */}
                <div
                    onClick={() => {
                        const el = document.getElementById('intel-sources');
                        el?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                    }}
                    title="Click to scroll to trust sources"
                    style={{
                        ...shared.cardGradient,
                        textAlign: 'center', padding: tokens.space.md,
                        cursor: 'pointer', transition: 'all 0.2s ease',
                        borderLeft: `3px solid ${colors.accent}`,
                    }}
                    {...hoverBrighten}
                >
                    <div style={{
                        fontSize: '28px', fontWeight: 800, fontFamily: MONO,
                        color: colors.accent,
                    }}>{sources.length}</div>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.textMuted, fontFamily: MONO, marginTop: '4px',
                    }}>TRUST SOURCES</div>
                </div>

                {/* Briefing */}
                <div
                    onClick={() => onNavigate?.('briefings')}
                    title="Click to view AI briefings"
                    style={{
                        ...shared.cardGradient,
                        textAlign: 'center', padding: tokens.space.md,
                        cursor: 'pointer', transition: 'all 0.2s ease',
                        borderLeft: `3px solid ${colors.accent}`,
                    }}
                    {...hoverBrighten}
                >
                    <div style={{
                        fontSize: '28px', fontWeight: 800, fontFamily: MONO,
                        color: briefing ? colors.green : colors.textMuted,
                    }}>{briefing ? 'LIVE' : '--'}</div>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.textMuted, fontFamily: MONO, marginTop: '4px',
                    }}>AI BRIEFING</div>
                </div>
            </div>

            {/* ── Convergence Alerts ── */}
            <div id="intel-alerts" style={{ marginBottom: tokens.space.lg }}>
                <div style={{ ...shared.sectionTitle, marginBottom: tokens.space.sm }}>
                    CONVERGENCE ALERTS
                </div>
                {alerts.length === 0 ? (
                    <div style={{ ...shared.card, color: colors.textMuted, fontSize: '12px', fontFamily: MONO, textAlign: 'center', padding: tokens.space.lg }}>
                        No active convergence alerts
                    </div>
                ) : (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {alerts.map((alert, i) => {
                            const isExpanded = expandedAlert === i;
                            const sevColor = severityColor(alert.severity);
                            return (
                                <div key={i}
                                    onClick={() => {
                                        if (isExpanded) {
                                            onNavigate?.('watchlist-analysis', alert.ticker);
                                        } else {
                                            setExpandedAlert(isExpanded ? null : i);
                                        }
                                    }}
                                    title={isExpanded ? `Click to navigate to ${alert.ticker}` : 'Click to expand'}
                                    style={{
                                        ...shared.card,
                                        borderLeft: `3px solid ${sevColor}`,
                                        cursor: 'pointer',
                                        transition: 'all 0.2s ease',
                                        overflow: 'hidden',
                                    }}
                                    onMouseEnter={(e) => { e.currentTarget.style.borderColor = sevColor; e.currentTarget.style.boxShadow = `0 4px 16px ${sevColor}15`; }}
                                    onMouseLeave={(e) => { e.currentTarget.style.boxShadow = 'none'; }}
                                >
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                            <span style={{
                                                fontFamily: MONO, fontSize: '14px', fontWeight: 800,
                                                color: '#E8F0F8',
                                            }}>{alert.ticker}</span>
                                            <span style={{
                                                padding: '2px 8px', borderRadius: '4px',
                                                fontSize: '9px', fontWeight: 700, fontFamily: MONO,
                                                background: `${sevColor}20`, color: sevColor,
                                                letterSpacing: '0.5px',
                                            }}>{alert.severity?.toUpperCase()}</span>
                                            <span style={{
                                                padding: '2px 8px', borderRadius: '4px',
                                                fontSize: '9px', fontFamily: MONO,
                                                background: `${colors.accent}15`, color: colors.accent,
                                            }}>{alert.type}</span>
                                        </div>
                                        <span style={{
                                            fontSize: '10px', color: colors.textMuted, fontFamily: MONO,
                                            transform: isExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                                            transition: 'transform 0.2s ease',
                                        }}>{'\u25BC'}</span>
                                    </div>
                                    <div style={{
                                        fontSize: '12px', color: colors.textDim, fontFamily: MONO,
                                        marginTop: '6px', lineHeight: 1.5,
                                    }}>{alert.message}</div>
                                    {isExpanded && (
                                        <div style={{
                                            marginTop: '10px', paddingTop: '10px',
                                            borderTop: `1px solid ${colors.borderSubtle}`,
                                            fontSize: '11px', color: colors.accent, fontFamily: MONO,
                                            fontWeight: 600,
                                        }}>
                                            Click again to view {alert.ticker} full analysis {'\u2192'}
                                        </div>
                                    )}
                                </div>
                            );
                        })}
                    </div>
                )}
            </div>

            {/* ── Trust Sources ── */}
            <div id="intel-sources" style={{ marginBottom: tokens.space.lg }}>
                <div style={{ ...shared.sectionTitle, marginBottom: tokens.space.sm }}>
                    TRUST-SCORED SOURCES
                </div>
                <div style={{
                    display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
                    gap: '8px',
                }}>
                    {sources.map((src, i) => {
                        const isExpanded = expandedSource === i;
                        const tColor = trustColor(src.trust_score);
                        return (
                            <div key={src.name}
                                onClick={() => setExpandedSource(isExpanded ? null : i)}
                                title={isExpanded ? 'Click to collapse' : 'Click to expand source details'}
                                style={{
                                    ...shared.card,
                                    cursor: 'pointer',
                                    transition: 'all 0.2s ease',
                                    borderLeft: `3px solid ${tColor}`,
                                }}
                                onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.15)'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
                                onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; e.currentTarget.style.transform = 'translateY(0)'; }}
                            >
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span style={{
                                        fontFamily: MONO, fontSize: '13px', fontWeight: 700,
                                        color: '#E8F0F8',
                                    }}>{src.name}</span>
                                    <span style={{
                                        fontFamily: MONO, fontSize: '14px', fontWeight: 800,
                                        color: tColor,
                                    }}>{(src.trust_score * 100).toFixed(0)}%</span>
                                </div>
                                <div style={{
                                    display: 'flex', gap: '8px', marginTop: '6px', flexWrap: 'wrap',
                                }}>
                                    <span style={{
                                        padding: '2px 6px', borderRadius: '3px',
                                        fontSize: '9px', fontFamily: MONO,
                                        background: `${colors.accent}15`, color: colors.accent,
                                    }}>{src.category}</span>
                                    <span style={{
                                        fontSize: '10px', color: colors.textMuted, fontFamily: MONO,
                                    }}>{src.signals} signals</span>
                                </div>

                                {/* Expanded detail */}
                                {isExpanded && (
                                    <div style={{
                                        marginTop: '10px', paddingTop: '10px',
                                        borderTop: `1px solid ${colors.borderSubtle}`,
                                    }}>
                                        {/* Trust bar */}
                                        <div style={{
                                            height: '4px', background: colors.bg,
                                            borderRadius: '2px', overflow: 'hidden', marginBottom: '8px',
                                        }}>
                                            <div style={{
                                                height: '100%', width: `${src.trust_score * 100}%`,
                                                background: tColor, borderRadius: '2px',
                                                transition: 'width 0.4s ease',
                                            }} />
                                        </div>
                                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px' }}>
                                            <div>
                                                <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO }}>TRUST SCORE</div>
                                                <div style={{ fontSize: '13px', fontWeight: 700, color: tColor, fontFamily: MONO }}>{(src.trust_score * 100).toFixed(1)}%</div>
                                            </div>
                                            <div>
                                                <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO }}>30D ACCURACY</div>
                                                <div style={{ fontSize: '13px', fontWeight: 700, color: trustColor(src.accuracy_30d), fontFamily: MONO }}>{(src.accuracy_30d * 100).toFixed(1)}%</div>
                                            </div>
                                        </div>
                                        <div style={{
                                            fontSize: '10px', color: colors.textDim, fontFamily: MONO,
                                            marginTop: '6px', lineHeight: 1.4,
                                        }}>
                                            {src.trust_score >= 0.85 ? 'High confidence source. Signals are weighted heavily in convergence scoring.'
                                                : src.trust_score >= 0.7 ? 'Moderate confidence. Signals contribute but require corroboration.'
                                                : src.trust_score >= 0.5 ? 'Low-moderate confidence. Used as supporting evidence only.'
                                                : 'Low confidence. Signals are heavily discounted or used as contrarian indicators.'}
                                        </div>
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* ── Quick Links ── */}
            <div style={{ marginBottom: tokens.space.lg }}>
                <div style={{ ...shared.sectionTitle, marginBottom: tokens.space.sm }}>
                    INTELLIGENCE MODULES
                </div>
                <div style={{
                    display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))',
                    gap: '8px',
                }}>
                    {[
                        { label: 'Cross-Reference', desc: 'Govt stats vs physical reality', view: 'cross-reference', color: colors.red },
                        { label: 'Predictions', desc: 'Oracle scoreboard + calibration', view: 'predictions', color: colors.yellow },
                        { label: 'Actor Network', desc: 'Who moves the markets', view: 'actor-network', color: colors.accent },
                        { label: 'Risk Map', desc: 'Unified risk exposure', view: 'risk', color: '#F97316' },
                        { label: 'Money Flow', desc: 'Global capital flows', view: 'money-flow', color: colors.green },
                        { label: 'Trends', desc: 'Momentum, regime, rotation', view: 'trends', color: '#8B5CF6' },
                    ].map(item => (
                        <div key={item.view}
                            onClick={() => onNavigate?.(item.view)}
                            title={`Navigate to ${item.label}`}
                            style={{
                                ...shared.card,
                                cursor: 'pointer',
                                transition: 'all 0.2s ease',
                                borderLeft: `3px solid ${item.color}`,
                                padding: '14px 16px',
                            }}
                            onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.2)'; e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.boxShadow = `0 4px 16px ${item.color}15`; }}
                            onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = 'none'; }}
                        >
                            <div style={{
                                fontFamily: MONO, fontSize: '12px', fontWeight: 700,
                                color: item.color, marginBottom: '4px',
                            }}>{item.label}</div>
                            <div style={{
                                fontSize: '11px', color: colors.textDim, fontFamily: MONO,
                            }}>{item.desc}</div>
                        </div>
                    ))}
                </div>
            </div>

            <div style={{ height: '40px' }} />
        </div>
    );
}
