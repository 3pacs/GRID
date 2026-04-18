import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import {
    Gem, Calendar, TrendingUp, AlertTriangle, ShieldCheck,
    Clock, Building2, Activity, ChevronRight, Beaker,
} from 'lucide-react';

/* ─────────────── Helpers ─────────────── */

const fmt = (v, dec = 1) => v != null ? Number(v).toFixed(dec) : '--';
const fmtMcap = (v) => {
    if (v == null) return '--';
    const n = Number(v);
    if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
    if (n >= 1e6) return `$${(n / 1e6).toFixed(0)}M`;
    return `$${n.toLocaleString()}`;
};
const fmtDate = (d) => {
    if (!d) return '--';
    const dt = new Date(d.includes('T') ? d : d + 'T00:00:00');
    return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const REGIME_COLORS = {
    GROWTH: '#10B981',
    NEUTRAL: '#3B82F6',
    FRAGILE: '#F59E0B',
    CRISIS: '#EF4444',
};

const SIGNAL_MAP = {
    BUY: { bg: 'rgba(16, 185, 129, 0.15)', color: '#10B981', border: '#10B981' },
    WATCHLIST: { bg: 'rgba(245, 158, 11, 0.15)', color: '#F59E0B', border: '#F59E0B' },
    AVOID: { bg: 'rgba(100, 116, 139, 0.15)', color: '#94A3B8', border: '#475569' },
};

const PROXIMITY_COLOR = (days) => {
    if (days == null) return colors.textMuted;
    if (days < 30) return '#EF4444';
    if (days < 60) return '#F59E0B';
    if (days < 90) return '#FBBF24';
    return colors.textDim;
};

/* ─────────────── Sub-components ─────────────── */

function SignalBadge({ signal }) {
    const s = SIGNAL_MAP[signal] || SIGNAL_MAP.AVOID;
    return (
        <span style={{
            ...shared.badge(s.bg),
            color: s.color,
            fontSize: '10px',
            letterSpacing: '1px',
            fontWeight: 700,
            border: `1px solid ${s.border}`,
        }}>
            {signal || 'N/A'}
        </span>
    );
}

function RegimeBadge({ regime }) {
    const c = REGIME_COLORS[regime] || colors.textMuted;
    return (
        <span style={{
            display: 'inline-flex', alignItems: 'center', gap: '6px',
            padding: '4px 12px', borderRadius: tokens.radius.pill,
            background: `${c}18`, border: `1px solid ${c}40`,
            fontSize: '11px', fontWeight: 700, letterSpacing: '1.5px',
            color: c, fontFamily: colors.mono,
        }}>
            <span style={{
                width: '6px', height: '6px', borderRadius: '50%',
                background: c, boxShadow: `0 0 6px ${c}`,
            }} />
            {regime || 'UNKNOWN'}
        </span>
    );
}

function ScoreBar({ score, max = 100 }) {
    const pct = Math.min((score / max) * 100, 100);
    const hue = pct > 70 ? 145 : pct > 40 ? 45 : 0;
    return (
        <div style={{
            display: 'flex', alignItems: 'center', gap: '8px', minWidth: '120px',
        }}>
            <div style={{
                flex: 1, height: '6px', borderRadius: '3px',
                background: colors.border, overflow: 'hidden',
            }}>
                <div style={{
                    width: `${pct}%`, height: '100%', borderRadius: '3px',
                    background: `hsl(${hue}, 70%, 55%)`,
                    transition: `width ${tokens.transition.normal}`,
                }} />
            </div>
            <span style={{
                fontSize: '11px', fontFamily: colors.mono,
                color: colors.textDim, minWidth: '28px', textAlign: 'right',
            }}>
                {fmt(score, 0)}
            </span>
        </div>
    );
}

function LoadingSkeleton({ lines = 5 }) {
    return (
        <div style={{ padding: tokens.space.lg }}>
            {Array.from({ length: lines }).map((_, i) => (
                <div key={i} style={{
                    height: '14px', borderRadius: '4px',
                    background: colors.border, marginBottom: '12px',
                    width: `${60 + Math.random() * 35}%`,
                    opacity: 0.5, animation: 'pulse 1.5s ease-in-out infinite',
                }} />
            ))}
            <style>{`@keyframes pulse { 0%,100% { opacity:0.3 } 50% { opacity:0.6 } }`}</style>
        </div>
    );
}

function ErrorBanner({ message }) {
    return (
        <div style={{
            ...shared.card, display: 'flex', alignItems: 'center', gap: '10px',
            borderColor: '#EF444440', background: '#EF444410',
        }}>
            <AlertTriangle size={16} color="#EF4444" />
            <span style={{ fontSize: '13px', color: '#EF4444' }}>{message}</span>
        </div>
    );
}

/* ─────────────── Main Component ─────────────── */

export default function TrialGems() {
    const [signals, setSignals] = useState([]);
    const [catalysts, setCatalysts] = useState([]);
    const [sponsors, setSponsors] = useState([]);
    const [regime, setRegime] = useState(null);
    const [loading, setLoading] = useState({ signals: true, catalysts: true, sponsors: true });
    const [errors, setErrors] = useState({ signals: null, catalysts: null, sponsors: null });

    const fetchData = useCallback(async () => {
        setLoading({ signals: true, catalysts: true, sponsors: true });
        setErrors({ signals: null, catalysts: null, sponsors: null });

        const results = await Promise.allSettled([
            api.getTrialSignals(50),
            api.getTrialCatalysts(),
            api.getTrialSponsors(20),
        ]);

        // Signals
        const sigRes = results[0];
        if (sigRes.status === 'fulfilled' && !sigRes.value?.error) {
            const data = Array.isArray(sigRes.value) ? sigRes.value : (sigRes.value?.data || sigRes.value?.signals || []);
            const sorted = [...data].sort((a, b) => (b.trial_strength_score ?? 0) - (a.trial_strength_score ?? 0));
            setSignals(sorted);
            // Extract regime from first signal
            if (data.length > 0 && data[0].regime_at_signal) {
                setRegime(data[0].regime_at_signal);
            }
        } else {
            const msg = sigRes.status === 'rejected' ? sigRes.reason?.message : sigRes.value?.message;
            setErrors(prev => ({ ...prev, signals: msg || 'Failed to load signals' }));
        }
        setLoading(prev => ({ ...prev, signals: false }));

        // Catalysts
        const catRes = results[1];
        if (catRes.status === 'fulfilled' && !catRes.value?.error) {
            const data = Array.isArray(catRes.value) ? catRes.value : (catRes.value?.data || catRes.value?.catalysts || []);
            setCatalysts(data);
        } else {
            const msg = catRes.status === 'rejected' ? catRes.reason?.message : catRes.value?.message;
            setErrors(prev => ({ ...prev, catalysts: msg || 'Failed to load catalysts' }));
        }
        setLoading(prev => ({ ...prev, catalysts: false }));

        // Sponsors
        const spoRes = results[2];
        if (spoRes.status === 'fulfilled' && !spoRes.value?.error) {
            const data = Array.isArray(spoRes.value) ? spoRes.value : (spoRes.value?.data || spoRes.value?.sponsors || []);
            setSponsors(data);
        } else {
            const msg = spoRes.status === 'rejected' ? spoRes.reason?.message : spoRes.value?.message;
            setErrors(prev => ({ ...prev, sponsors: msg || 'Failed to load sponsors' }));
        }
        setLoading(prev => ({ ...prev, sponsors: false }));
    }, []);

    useEffect(() => { fetchData(); }, [fetchData]);

    // Stats
    const buys = signals.filter(s => s.signal === 'BUY').length;
    const watchlist = signals.filter(s => s.signal === 'WATCHLIST').length;
    const avoids = signals.filter(s => s.signal === 'AVOID').length;

    return (
        <div style={{
            padding: tokens.space.xl, minHeight: '100vh',
            background: colors.bg, color: colors.text,
            fontFamily: colors.sans,
        }}>
            {/* ── Header ────────────────────────────── */}
            <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                flexWrap: 'wrap', gap: '12px', marginBottom: tokens.space.xl,
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <Gem size={22} color={colors.accent} />
                    <h1 style={{
                        ...shared.header, margin: 0, fontSize: '20px',
                        background: 'linear-gradient(135deg, #E2E8F0 0%, #94A3B8 100%)',
                        WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent',
                    }}>
                        Trial Gem Hunter
                    </h1>
                    <RegimeBadge regime={regime} />
                </div>

                <div style={{ display: 'flex', gap: '16px', alignItems: 'center' }}>
                    <StatPill icon={<TrendingUp size={13} />} label="BUY" count={buys} color="#10B981" />
                    <StatPill icon={<Clock size={13} />} label="WATCH" count={watchlist} color="#F59E0B" />
                    <StatPill icon={<ShieldCheck size={13} />} label="AVOID" count={avoids} color="#94A3B8" />
                </div>
            </div>

            {/* ── Body: Table + Sidebar ────────────── */}
            <div style={{
                display: 'flex', gap: tokens.space.lg,
                alignItems: 'flex-start',
            }}>
                {/* Signal Table */}
                <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={shared.sectionTitle}>
                        <Activity size={11} style={{ marginRight: '6px', verticalAlign: 'middle' }} />
                        SIGNAL BOARD
                    </div>
                    {errors.signals && <ErrorBanner message={errors.signals} />}
                    {loading.signals ? <LoadingSkeleton lines={8} /> : (
                        <div style={{
                            ...shared.card, padding: 0, overflow: 'hidden',
                            overflowX: 'auto',
                        }}>
                            <table style={{
                                width: '100%', borderCollapse: 'collapse',
                                fontSize: tokens.fontSize.sm, fontFamily: colors.mono,
                            }}>
                                <thead>
                                    <tr style={{ background: colors.cardElevated }}>
                                        {['Ticker', 'Company', 'Indication', 'Phase', 'Score', 'Signal', 'Days', 'Mkt Cap', 'FDA'].map(h => (
                                            <th key={h} style={{
                                                padding: '10px 12px', textAlign: 'left',
                                                fontSize: '10px', fontWeight: 700,
                                                letterSpacing: '1px', color: colors.textMuted,
                                                borderBottom: `1px solid ${colors.border}`,
                                                whiteSpace: 'nowrap',
                                            }}>
                                                {h}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {signals.length === 0 && (
                                        <tr>
                                            <td colSpan={9} style={{
                                                padding: '40px', textAlign: 'center',
                                                color: colors.textMuted, fontSize: '13px',
                                            }}>
                                                No trial signals found
                                            </td>
                                        </tr>
                                    )}
                                    {signals.map((row, i) => {
                                        const sig = SIGNAL_MAP[row.signal_type] || SIGNAL_MAP.AVOID;
                                        return (
                                            <tr key={row.ticker + '-' + i} style={{
                                                borderLeft: `3px solid ${sig.border}`,
                                                background: i % 2 === 0 ? 'transparent' : `${colors.cardElevated}40`,
                                                transition: `background ${tokens.transition.fast}`,
                                                cursor: 'pointer',
                                            }}
                                            onMouseEnter={e => e.currentTarget.style.background = colors.cardHover}
                                            onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? 'transparent' : `${colors.cardElevated}40`}
                                            onClick={() => { window.location.hash = `#/watchlist/${encodeURIComponent(row.ticker)}`; }}
                                            >
                                                <td style={cellStyle}>
                                                    <span style={{
                                                        fontWeight: 700, color: '#E8F0F8',
                                                        fontSize: '13px',
                                                    }}>
                                                        {row.ticker || '--'}
                                                    </span>
                                                </td>
                                                <td style={{ ...cellStyle, color: colors.textDim, maxWidth: '160px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                                    {row.company_name || row.company || '--'}
                                                </td>
                                                <td style={{ ...cellStyle, color: colors.textDim }}>
                                                    {row.primary_indication || '--'}
                                                </td>
                                                <td style={cellStyle}>
                                                    <PhaseBadge phase={row.trial_phase} />
                                                </td>
                                                <td style={{ ...cellStyle, minWidth: '120px' }}>
                                                    <ScoreBar score={row.trial_strength_score ?? 0} />
                                                </td>
                                                <td style={cellStyle}>
                                                    <SignalBadge signal={row.signal_type} />
                                                </td>
                                                <td style={cellStyle}>
                                                    <span style={{
                                                        color: PROXIMITY_COLOR(row.days_to_completion),
                                                        fontWeight: 600,
                                                    }}>
                                                        {row.days_to_completion != null ? `${row.days_to_completion}d` : '--'}
                                                    </span>
                                                </td>
                                                <td style={{ ...cellStyle, color: colors.textDim }}>
                                                    {fmtMcap(row.market_cap_mm)}
                                                </td>
                                                <td style={cellStyle}>
                                                    {row.fda_designation ? (
                                                        <span style={{
                                                            ...shared.badge(colors.accentGlow),
                                                            color: colors.accentLight,
                                                            fontSize: '9px', letterSpacing: '0.5px',
                                                        }}>
                                                            {row.fda_designation}
                                                        </span>
                                                    ) : (
                                                        <span style={{ color: colors.textMuted }}>--</span>
                                                    )}
                                                </td>
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>

                {/* ── Catalyst Calendar Sidebar ─────── */}
                <div style={{ width: '300px', flexShrink: 0 }}>
                    <div style={shared.sectionTitle}>
                        <Calendar size={11} style={{ marginRight: '6px', verticalAlign: 'middle' }} />
                        CATALYST CALENDAR
                    </div>
                    {errors.catalysts && <ErrorBanner message={errors.catalysts} />}
                    {loading.catalysts ? <LoadingSkeleton lines={6} /> : (
                        <div style={{
                            ...shared.card, padding: '8px',
                            maxHeight: '600px', overflowY: 'auto',
                        }}>
                            {catalysts.length === 0 && (
                                <div style={{
                                    padding: '24px', textAlign: 'center',
                                    color: colors.textMuted, fontSize: '12px',
                                }}>
                                    No upcoming catalysts
                                </div>
                            )}
                            {catalysts.map((cat, i) => (
                                <div key={cat.ticker + '-' + i} style={{
                                    display: 'flex', alignItems: 'center', gap: '10px',
                                    padding: '10px 8px',
                                    borderBottom: i < catalysts.length - 1 ? `1px solid ${colors.border}` : 'none',
                                    transition: `background ${tokens.transition.fast}`,
                                    borderRadius: tokens.radius.sm,
                                    cursor: 'default',
                                }}
                                onMouseEnter={e => e.currentTarget.style.background = colors.cardHover}
                                onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                                >
                                    <div style={{
                                        width: '4px', height: '36px', borderRadius: '2px',
                                        background: PROXIMITY_COLOR(cat.days_out),
                                        flexShrink: 0,
                                    }} />
                                    <div style={{ flex: 1, minWidth: 0 }}>
                                        <div style={{
                                            display: 'flex', justifyContent: 'space-between',
                                            alignItems: 'center', marginBottom: '2px',
                                        }}>
                                            <span style={{
                                                fontFamily: colors.mono, fontWeight: 700,
                                                fontSize: '12px', color: '#E8F0F8',
                                            }}>
                                                {cat.ticker}
                                            </span>
                                            <span style={{
                                                fontSize: '10px', fontFamily: colors.mono,
                                                color: PROXIMITY_COLOR(cat.days_out),
                                                fontWeight: 600,
                                            }}>
                                                {cat.days_out != null ? `${cat.days_out}d` : '--'}
                                            </span>
                                        </div>
                                        <div style={{
                                            fontSize: '10px', color: colors.textMuted,
                                            whiteSpace: 'nowrap', overflow: 'hidden',
                                            textOverflow: 'ellipsis',
                                        }}>
                                            {fmtDate(cat.expected_date)} &middot; {cat.event_type || cat.event || 'Readout'}
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>

            {/* ── Top Sponsors ──────────────────────── */}
            <div style={{ marginTop: tokens.space.xl }}>
                <div style={shared.sectionTitle}>
                    <Building2 size={11} style={{ marginRight: '6px', verticalAlign: 'middle' }} />
                    TOP SPONSORS BY INFLUENCE
                </div>
                {errors.sponsors && <ErrorBanner message={errors.sponsors} />}
                {loading.sponsors ? <LoadingSkeleton lines={3} /> : (
                    <div style={{
                        display: 'flex', gap: '12px', overflowX: 'auto',
                        paddingBottom: '8px',
                    }}>
                        {sponsors.length === 0 && (
                            <div style={{
                                ...shared.card, width: '100%', textAlign: 'center',
                                color: colors.textMuted, fontSize: '12px', padding: '24px',
                            }}>
                                No sponsor data available
                            </div>
                        )}
                        {sponsors.map((sp, i) => (
                            <div key={sp.name + '-' + i} style={{
                                ...shared.cardGradient,
                                minWidth: '200px', maxWidth: '240px',
                                flexShrink: 0, padding: '14px 16px',
                                transition: `transform ${tokens.transition.fast}, box-shadow ${tokens.transition.fast}`,
                                cursor: 'default',
                            }}
                            onMouseEnter={e => {
                                e.currentTarget.style.transform = 'translateY(-2px)';
                                e.currentTarget.style.boxShadow = colors.shadow.lg;
                            }}
                            onMouseLeave={e => {
                                e.currentTarget.style.transform = 'translateY(0)';
                                e.currentTarget.style.boxShadow = colors.shadow.md;
                            }}
                            >
                                <div style={{
                                    fontSize: '13px', fontWeight: 700, color: '#E8F0F8',
                                    marginBottom: '6px', whiteSpace: 'nowrap',
                                    overflow: 'hidden', textOverflow: 'ellipsis',
                                }}>
                                    {sp.name || '--'}
                                </div>
                                <div style={{
                                    display: 'flex', justifyContent: 'space-between',
                                    fontSize: '11px', color: colors.textMuted, marginBottom: '8px',
                                }}>
                                    <span>{sp.category || 'Pharma'}</span>
                                    <span style={{ fontFamily: colors.mono }}>
                                        {sp.trial_count != null ? `${sp.trial_count} trials` : '--'}
                                    </span>
                                </div>
                                <div style={{
                                    display: 'flex', alignItems: 'center', gap: '6px',
                                }}>
                                    <div style={{
                                        flex: 1, height: '4px', borderRadius: '2px',
                                        background: colors.border, overflow: 'hidden',
                                    }}>
                                        <div style={{
                                            width: `${Math.min((sp.influence_score ?? 0), 100)}%`,
                                            height: '100%', borderRadius: '2px',
                                            background: `linear-gradient(90deg, ${colors.accent}, ${colors.accentLight})`,
                                        }} />
                                    </div>
                                    <span style={{
                                        fontSize: '10px', fontFamily: colors.mono,
                                        color: colors.accentLight, fontWeight: 600,
                                    }}>
                                        {fmt(sp.influence_score, 0)}
                                    </span>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}

/* ─────────────── Small Inline Components ─────────────── */

function StatPill({ icon, label, count, color }) {
    return (
        <div style={{
            display: 'flex', alignItems: 'center', gap: '6px',
            padding: '4px 10px', borderRadius: tokens.radius.pill,
            background: `${color}12`, border: `1px solid ${color}30`,
            fontSize: '11px', fontFamily: colors.mono,
        }}>
            <span style={{ color, display: 'flex' }}>{icon}</span>
            <span style={{ color: colors.textMuted, fontWeight: 600 }}>{label}</span>
            <span style={{ color, fontWeight: 700 }}>{count}</span>
        </div>
    );
}

function PhaseBadge({ phase }) {
    const p = String(phase || '').toUpperCase();
    const phaseColor = p.includes('3') ? '#10B981' : p.includes('2') ? '#3B82F6' : colors.textMuted;
    return (
        <span style={{
            fontSize: '10px', fontWeight: 700, letterSpacing: '0.5px',
            color: phaseColor, fontFamily: colors.mono,
        }}>
            {phase || '--'}
        </span>
    );
}

/* ─────────────── Shared Cell Style ─────────────── */

const cellStyle = {
    padding: '10px 12px',
    borderBottom: `1px solid ${colors.border}`,
    whiteSpace: 'nowrap',
    fontSize: '12px',
    verticalAlign: 'middle',
};
