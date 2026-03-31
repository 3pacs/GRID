import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import StatusDot from '../components/StatusDot.jsx';
import { colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';
import { useWebSocket } from '../hooks/useWebSocket.js';
import DashboardFlows from '../components/DashboardFlows.jsx';
import { formatUtcClock } from '../utils/formatTime.js';

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const REGIME_COLORS = {
    GROWTH:      { bg: '#0D3320', color: colors.green },
    EXPANSION:   { bg: '#0D3320', color: colors.green },
    FRAGILE:     { bg: '#3D2800', color: colors.yellow },
    CRISIS:      { bg: '#3B1111', color: colors.red },
    CONTRACTION: { bg: '#3B1111', color: colors.red },
    NEUTRAL:     { bg: colors.card, color: colors.textMuted },
};

const PULSE_TICKERS = [
    { key: 'SPY', label: 'SPY' },
    { key: 'QQQ', label: 'QQQ' },
    { key: 'VIX', label: 'VIX' },
    { key: 'BTC-USD', label: 'BTC' },
];

const FEED_ICONS = {
    insider: '\u{1F4CA}', congressional: '\u{1F3DB}', contract: '\u{1F4DC}',
    sleuth: '\u{1F50D}', thesis: '\u{1F9ED}', red_flag: '\u{1F6A9}',
    regime: '\u{26A1}', convergence: '\u{1F4E1}', default: '\u{25CF}',
};

const fmtPrice = (v) => {
    if (v == null) return '--';
    const n = typeof v === 'number' ? v : parseFloat(v);
    if (isNaN(n)) return '--';
    if (n >= 10000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    return n.toFixed(2);
};
const fmtPct = (p) => {
    if (p == null) return '';
    const v = (p * 100).toFixed(1);
    return p >= 0 ? `+${v}%` : `${v}%`;
};
const pctColor = (p) => p == null ? colors.textMuted : p >= 0 ? colors.green : colors.red;
const timeAgo = (ts) => {
    if (!ts) return '';
    const diff = Date.now() - new Date(ts).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
};

function LiveClock() {
    const [now, setNow] = useState(new Date());
    useEffect(() => { const id = setInterval(() => setNow(new Date()), 1000); return () => clearInterval(id); }, []);
    const utc = formatUtcClock(now);
    return (
        <span style={{ fontFamily: MONO, fontSize: '11px', color: colors.textMuted }}>
            <span style={{ fontSize: '9px', color: colors.textDim, marginRight: '3px' }}>UTC</span>{utc}
        </span>
    );
}

function buildChangeFeed(intel) {
    if (!intel) return [];
    const items = [];
    for (const ev of (intel?.levers?.active_events || [])) {
        const tickers = ev.tickers?.join(', ') || '';
        const cat = (ev.category || '').toLowerCase();
        const type = cat.includes('congress') ? 'congressional'
            : (cat.includes('insider') || cat.includes('corporate')) ? 'insider'
            : cat.includes('contract') || cat.includes('government') ? 'contract' : 'default';
        items.push({
            type, timestamp: ev.timestamp,
            text: `${ev.puller_name}: ${ev.action}${tickers ? ` (${tickers})` : ''}`,
            nav: type === 'congressional' || type === 'insider' ? 'flows' : null,
        });
    }
    for (const f of (intel?.cross_ref?.red_flags || [])) {
        items.push({
            type: 'red_flag', timestamp: f.checked_at || f.timestamp || intel.generated_at,
            text: `${f.category || 'Unknown'}: ${f.description || f.indicator || 'divergence detected'}`,
            nav: 'cross-reference',
        });
    }
    for (const c of (intel?.trust?.convergence_events || [])) {
        items.push({
            type: 'convergence', timestamp: c.detected_at || c.timestamp || intel.generated_at,
            text: `Convergence: ${c.description || c.feature || 'multiple sources agree'}`,
            nav: null,
        });
    }
    items.sort((a, b) => (new Date(b.timestamp || 0) - new Date(a.timestamp || 0)));
    return items.slice(0, 20);
}

/* ═══════════════════════════ DASHBOARD ═════���═════════════════════ */

export default function Dashboard({ onNavigate }) {
    const { currentRegime, systemStatus, setCurrentRegime, setSystemStatus,
        setLoading, addNotification, livePriceUpdates } = useStore();
    const { isMobile } = useDevice();
    const { connected: wsConnected, prices: wsPrices } = useWebSocket();
    const [thesis, setThesis] = useState(null);
    const [thesisLoading, setThesisLoading] = useState(false);
    const [changeFeed, setChangeFeed] = useState([]);
    const [pulsePrices, setPulsePrices] = useState({});
    const [watchlistItems, setWatchlistItems] = useState([]);
    const [intelData, setIntelData] = useState(null);

    const loadData = useCallback(async () => {
        setLoading('dashboard', true);
        try {
            // Fast: regime + status load first (small payloads)
            const [regime, status] = await Promise.all([
                api.getCurrent().catch(() => null),
                api.getStatus().catch(() => null),
            ]);
            if (regime) setCurrentRegime(regime);
            if (status) setSystemStatus(status);
            setLoading('dashboard', false);

            // Background: thesis + intel + prices (heavier, don't block UI)
            api.getThesis().then(t => { if (t && !t.error) setThesis(t); }).catch(() => {});
            api.getIntelDashboard().then(d => { setChangeFeed(buildChangeFeed(d)); setIntelData(d); }).catch(() => {});
            api.refreshWatchlistPrices().then(r => { if (r?.prices) setPulsePrices(r.prices); }).catch(() => {});
            api.getWatchlistEnriched(8).then(r => { if (r?.items) setWatchlistItems(r.items); }).catch(() => {});
        } catch { addNotification('error', 'Failed to load dashboard'); setLoading('dashboard', false); }
    }, []);

    useEffect(() => { loadData(); }, []);

    const handleRefreshThesis = useCallback(async () => {
        setThesisLoading(true);
        try {
            const r = await api.getThesis();
            if (r && !r.error) setThesis(r);
            else addNotification('error', 'Thesis refresh failed');
        } catch { addNotification('error', 'Thesis refresh failed'); }
        setThesisLoading(false);
    }, []);

    const dbOnline = systemStatus?.database?.connected;
    const allConnected = wsConnected && dbOnline;
    const regime = currentRegime;
    const regimeState = regime?.state || 'NEUTRAL';
    const regimeConf = regime?.confidence;
    const rc = REGIME_COLORS[regimeState.toUpperCase()] || REGIME_COLORS.NEUTRAL;

    const pulse = useMemo(() => PULSE_TICKERS.map(({ key, label }) => {
        const best = livePriceUpdates?.[key] || wsPrices?.[key] || pulsePrices[key];
        return { key, label, price: best?.price, pct: best?.pct_1d };
    }), [pulsePrices, livePriceUpdates, wsPrices]);

    const direction = thesis?.overall_direction || 'NEUTRAL';
    const conviction = thesis?.conviction;
    const narrative = thesis?.narrative || '';
    const keyDrivers = thesis?.key_drivers || [];
    const riskFactors = thesis?.risk_factors || [];
    const thesisTime = thesis?.generated_at;
    const pad = isMobile ? '12px' : '20px';
    const card = { background: colors.gradientCard, border: `1px solid ${colors.border}`, borderRadius: tokens.radius.md };

    return (
        <div style={{ padding: pad, paddingTop: `calc(env(safe-area-inset-top, 0px) + ${pad})`,
            maxWidth: '960px', margin: '0 auto', display: 'flex', flexDirection: 'column',
            gap: isMobile ? '16px' : '20px' }}>

            {/* ═══ 1. HEADER ═══ */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                flexWrap: 'wrap', gap: '10px', paddingBottom: '12px', borderBottom: `1px solid ${colors.border}` }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    <span style={{ fontFamily: MONO, fontSize: '20px', fontWeight: 800,
                        color: colors.accent, letterSpacing: '3px' }}>GRID</span>
                    <div onClick={() => onNavigate('regime')} style={{
                        display: 'inline-flex', alignItems: 'center', gap: '5px',
                        padding: '3px 10px', borderRadius: '5px', cursor: 'pointer',
                        background: rc.bg, border: `1px solid ${rc.color}40`,
                        fontFamily: MONO, fontSize: '11px', fontWeight: 700, color: rc.color, letterSpacing: '1px',
                    }}>
                        <span style={{ width: 5, height: 5, borderRadius: '50%',
                            background: rc.color, boxShadow: `0 0 5px ${rc.color}` }} />
                        {regimeState}
                        {regimeConf != null && <span style={{ opacity: 0.7, fontSize: '10px' }}>
                            {(regimeConf * 100).toFixed(0)}%</span>}
                    </div>
                </div>
                {!isMobile && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: '16px', fontFamily: MONO, fontSize: '11px' }}>
                        {pulse.map(p => (
                            <span key={p.key} style={{ display: 'inline-flex', gap: '4px', alignItems: 'baseline' }}>
                                <span style={{ color: colors.textDim, fontSize: '10px' }}>{p.label}</span>
                                <span style={{ color: colors.text }}>{fmtPrice(p.price)}</span>
                                {p.pct != null && <span style={{ color: pctColor(p.pct), fontSize: '10px' }}>{fmtPct(p.pct)}</span>}
                            </span>
                        ))}
                    </div>
                )}
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <StatusDot status={allConnected ? 'online' : 'offline'} size={7} />
                    <LiveClock />
                </div>
            </div>

            {/* ═══ 2. INTELLIGENCE BRIEF ═══ */}
            <div style={{ ...card, padding: isMobile ? '16px' : '20px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '14px', flexWrap: 'wrap', gap: '8px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <span style={{ fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                            letterSpacing: '1.5px', color: colors.textMuted }}>INTELLIGENCE BRIEF</span>
                        {direction !== 'NEUTRAL' && (
                            <span style={{ fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                                padding: '2px 8px', borderRadius: '3px',
                                background: direction === 'BULLISH' ? `${colors.green}18` : `${colors.red}18`,
                                color: direction === 'BULLISH' ? colors.green : colors.red,
                                border: `1px solid ${direction === 'BULLISH' ? colors.green : colors.red}30`,
                            }}>{direction}{conviction != null ? ` ${(conviction * 100).toFixed(0)}%` : ''}</span>
                        )}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        {thesisTime && <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim }}>{timeAgo(thesisTime)}</span>}
                        <button onClick={handleRefreshThesis} disabled={thesisLoading} style={{
                            fontFamily: MONO, fontSize: '10px', fontWeight: 600, padding: '3px 10px',
                            borderRadius: '4px', background: 'transparent', border: `1px solid ${colors.border}`,
                            color: thesisLoading ? colors.textDim : colors.textMuted,
                            cursor: thesisLoading ? 'wait' : 'pointer',
                        }}>{thesisLoading ? 'Refreshing...' : 'Refresh'}</button>
                    </div>
                </div>
                {narrative
                    ? <div style={{ fontFamily: SANS, fontSize: '14px', lineHeight: 1.6, color: colors.text, marginBottom: '14px' }}>{narrative}</div>
                    : <div style={{ fontFamily: SANS, fontSize: '13px', color: colors.textDim, fontStyle: 'italic', marginBottom: '14px' }}>No thesis available. Click Refresh to generate.</div>
                }
                {(keyDrivers.length > 0 || riskFactors.length > 0) && (
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                        {keyDrivers.slice(0, 6).map((d, i) => (
                            <span key={`d-${i}`} style={{ fontFamily: MONO, fontSize: '10px', padding: '2px 8px',
                                borderRadius: '3px', background: `${colors.green}12`, color: colors.green,
                                border: `1px solid ${colors.green}25` }}>
                                {typeof d === 'string' ? d : d.name || d.label || JSON.stringify(d)}</span>
                        ))}
                        {riskFactors.slice(0, 4).map((r, i) => (
                            <span key={`r-${i}`} style={{ fontFamily: MONO, fontSize: '10px', padding: '2px 8px',
                                borderRadius: '3px', background: `${colors.red}12`, color: colors.red,
                                border: `1px solid ${colors.red}25` }}>
                                {typeof r === 'string' ? r : r.name || r.label || JSON.stringify(r)}</span>
                        ))}
                    </div>
                )}
            </div>

            {/* ═══ CAPITAL FLOWS ═══ */}
            <DashboardFlows data={intelDash} onNavigate={onNavigate} />

            {/* ═══ WATCHLIST BRIEFING ═══ */}
            {watchlistItems.length > 0 && (
                <div style={{ ...card, padding: isMobile ? '14px' : '18px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                        <span style={{ fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                            letterSpacing: '1.5px', color: colors.textMuted }}>WATCHLIST</span>
                        <span onClick={() => onNavigate('flows')} style={{ fontFamily: MONO, fontSize: '10px',
                            color: colors.accent, cursor: 'pointer' }}>View All</span>
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : 'repeat(2, 1fr)', gap: '8px' }}>
                        {watchlistItems.slice(0, 6).map(item => {
                            const pct1d = item.pct_1d;
                            const pct1m = item.pct_1m;
                            const pc = pct1d != null ? pctColor(pct1d) : colors.textMuted;
                            const optsSent = item.options?.pcr > 1.2 ? 'PUT' : item.options?.pcr < 0.7 ? 'CALL' : null;
                            const optColor = optsSent === 'PUT' ? colors.red : optsSent === 'CALL' ? colors.green : null;
                            return (
                                <div key={item.ticker} onClick={() => onNavigate('watchlist-analysis', item.ticker)}
                                    style={{
                                        background: colors.bg, borderRadius: '8px', padding: '10px 12px',
                                        border: `1px solid ${colors.border}`, cursor: 'pointer',
                                        transition: 'border-color 0.15s',
                                    }}
                                    onMouseEnter={e => { e.currentTarget.style.borderColor = colors.accent; }}
                                    onMouseLeave={e => { e.currentTarget.style.borderColor = colors.border; }}>
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                            <span style={{ fontFamily: MONO, fontSize: '12px', fontWeight: 700, color: colors.text }}>
                                                {item.ticker}
                                            </span>
                                            {item.sector && (
                                                <span style={{ fontSize: '9px', padding: '1px 5px', borderRadius: '3px',
                                                    background: `${colors.accent}15`, color: colors.accent }}>
                                                    {item.sector}
                                                </span>
                                            )}
                                        </div>
                                        <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px' }}>
                                            <span style={{ fontFamily: MONO, fontSize: '12px', color: colors.text }}>
                                                {item.price != null ? fmtPrice(item.price) : '--'}
                                            </span>
                                            {pct1d != null && (
                                                <span style={{ fontFamily: MONO, fontSize: '10px', color: pc }}>
                                                    {fmtPct(pct1d)}
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                    <div style={{ fontSize: '10px', color: colors.textDim, lineHeight: '1.4' }}>
                                        {item.insight || 'No context available'}
                                    </div>
                                    <div style={{ display: 'flex', gap: '8px', marginTop: '6px', fontSize: '10px' }}>
                                        {pct1m != null && (
                                            <span style={{ color: pctColor(pct1m), fontFamily: MONO }}>
                                                30d: {fmtPct(pct1m)}
                                            </span>
                                        )}
                                        {item.influence != null && (
                                            <span style={{ color: colors.textMuted }}>
                                                wt: <span style={{ fontFamily: MONO }}>{(item.influence * 100).toFixed(0)}%</span>
                                            </span>
                                        )}
                                        {optsSent && (
                                            <span style={{ color: optColor, fontWeight: 600, fontFamily: MONO }}>
                                                {optsSent} {item.options.pcr.toFixed(1)}
                                            </span>
                                        )}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* ═══ CONVERGENCE SIGNALS ═══ */}
            {intelData?.trust?.convergence_events?.length > 0 && (
                <div style={{ ...card, padding: isMobile ? '14px' : '18px', borderLeft: `3px solid ${colors.accent}` }}>
                    <div style={{ fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                        letterSpacing: '1.5px', color: colors.accent, marginBottom: '10px' }}>
                        CONVERGENCE SIGNALS — {intelData.trust.convergence_events.length} ACTIVE
                    </div>
                    <div style={{ fontSize: '10px', color: colors.textDim, marginBottom: '10px', lineHeight: '1.5' }}>
                        3+ independent source types agree on direction
                    </div>
                    {intelData.trust.convergence_events.slice(0, 5).map((evt, i) => {
                        const isBuy = evt.signal_type === 'BUY';
                        const dirColor = isBuy ? colors.green : colors.red;
                        return (
                            <div key={i} onClick={() => onNavigate('watchlist-analysis', evt.ticker)}
                                style={{
                                    display: 'flex', alignItems: 'center', gap: '10px',
                                    padding: '8px 10px', borderRadius: '6px', marginBottom: '6px',
                                    background: `${dirColor}08`, border: `1px solid ${dirColor}20`,
                                    cursor: 'pointer',
                                }}>
                                <span style={{ fontFamily: MONO, fontSize: '12px', fontWeight: 700,
                                    color: dirColor, width: '36px' }}>
                                    {evt.signal_type}
                                </span>
                                <span style={{ fontFamily: MONO, fontSize: '12px', fontWeight: 700,
                                    color: colors.text }}>
                                    {evt.ticker}
                                </span>
                                <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.textMuted }}>
                                    {evt.source_count} sources
                                </span>
                                <div style={{ flex: 1, display: 'flex', gap: '4px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                                    {(evt.sources || []).map((src, j) => (
                                        <span key={j} style={{
                                            fontSize: '9px', padding: '1px 5px', borderRadius: '3px',
                                            background: `${colors.accent}15`, color: colors.accent,
                                            fontFamily: MONO,
                                        }}>
                                            {src.source_type}
                                        </span>
                                    ))}
                                </div>
                                <span style={{ fontFamily: MONO, fontSize: '11px', fontWeight: 600,
                                    color: dirColor }}>
                                    {(evt.combined_confidence * 100).toFixed(0)}%
                                </span>
                            </div>
                        );
                    })}
                </div>
            )}

            {/* ═══ 3. WHAT CHANGED ═══ */}
            <div style={{ ...card, padding: isMobile ? '14px' : '18px' }}>
                <div style={{ fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                    letterSpacing: '1.5px', color: colors.textMuted, marginBottom: '12px' }}>WHAT CHANGED</div>
                {changeFeed.length === 0
                    ? <div style={{ fontFamily: SANS, fontSize: '13px', color: colors.textDim, padding: '12px 0' }}>
                        No significant changes in the last 6 hours.</div>
                    : <div style={{ display: 'flex', flexDirection: 'column', gap: '1px' }}>
                        {changeFeed.map((item, i) => (
                            <div key={i} onClick={() => item.nav && onNavigate(item.nav)}
                                style={{ display: 'flex', alignItems: 'flex-start', gap: '10px', padding: '7px 4px',
                                    borderBottom: i < changeFeed.length - 1 ? `1px solid ${colors.border}40` : 'none',
                                    cursor: item.nav ? 'pointer' : 'default', borderRadius: '3px', transition: 'background 0.15s' }}
                                onMouseEnter={e => { if (item.nav) e.currentTarget.style.background = `${colors.accent}08`; }}
                                onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}>
                                <span style={{ fontSize: '13px', lineHeight: 1, flexShrink: 0, marginTop: '1px' }}>
                                    {FEED_ICONS[item.type] || FEED_ICONS.default}</span>
                                <span style={{ fontFamily: SANS, fontSize: '12px', color: colors.text, lineHeight: 1.4,
                                    flex: 1, overflow: 'hidden', textOverflow: 'ellipsis',
                                    display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' }}>{item.text}</span>
                                <span style={{ fontFamily: MONO, fontSize: '10px', color: colors.textDim,
                                    flexShrink: 0, whiteSpace: 'nowrap' }}>{timeAgo(item.timestamp)}</span>
                            </div>
                        ))}
                    </div>
                }
            </div>

            {/* ═══ 4. MARKET PULSE ═══ */}
            <div style={{ ...card, display: 'flex', alignItems: 'center', gap: isMobile ? '6px' : '0',
                flexWrap: isMobile ? 'wrap' : 'nowrap', padding: isMobile ? '12px' : '10px 16px',
                fontFamily: MONO, fontSize: isMobile ? '11px' : '12px' }}>
                <span style={{ fontFamily: MONO, fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                    color: colors.textDim, marginRight: isMobile ? 0 : '12px',
                    width: isMobile ? '100%' : 'auto', marginBottom: isMobile ? '4px' : 0 }}>PULSE</span>
                {pulse.map((p, i) => (
                    <React.Fragment key={p.key}>
                        <span onClick={() => onNavigate('watchlist-analysis', p.key)}
                            style={{ display: 'inline-flex', alignItems: 'baseline', gap: '5px',
                                cursor: 'pointer', padding: '2px 6px', borderRadius: '3px', transition: 'background 0.15s' }}
                            onMouseEnter={e => { e.currentTarget.style.background = `${colors.accent}10`; }}
                            onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}>
                            <span style={{ color: colors.textDim, fontSize: '10px', fontWeight: 600 }}>{p.label}</span>
                            <span style={{ color: colors.text }}>{fmtPrice(p.price)}</span>
                            <span style={{ color: pctColor(p.pct), fontSize: '10px' }}>{fmtPct(p.pct)}</span>
                        </span>
                        {!isMobile && i < pulse.length - 1 && <span style={{ color: colors.border, margin: '0 4px', fontSize: '10px' }}>|</span>}
                    </React.Fragment>
                ))}
            </div>

            <div style={{ height: '60px' }} />
        </div>
    );
}
