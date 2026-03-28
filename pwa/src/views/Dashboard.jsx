import React, { useEffect, useState, useRef, useCallback, useMemo } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import StatusDot from '../components/StatusDot.jsx';
import RegimeThermometer from '../components/RegimeThermometer.jsx';
import CapitalFlowAnalysis from '../components/CapitalFlowAnalysis.jsx';
import WidgetManager, { loadWidgetPrefs, isWidgetVisible } from '../components/WidgetManager.jsx';
import { shared, colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';
import { useWebSocket } from '../hooks/useWebSocket.js';
import ViewHelp from '../components/ViewHelp.jsx';

/* ─────────────────────────── Design Constants ─────────────────────────── */

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";
const SPACE = { xs: '4px', sm: '8px', md: '16px', lg: '24px', xl: '32px' };

const REGIME_BADGE = {
    GROWTH:    { bg: 'linear-gradient(135deg, #0D3320, #143D28)', border: colors.green, color: colors.green },
    EXPANSION: { bg: 'linear-gradient(135deg, #0D3320, #143D28)', border: colors.green, color: colors.green },
    FRAGILE:   { bg: 'linear-gradient(135deg, #3D2800, #5A3A00)', border: colors.yellow, color: colors.yellow },
    CRISIS:    { bg: 'linear-gradient(135deg, #3B1111, #4D1616)', border: colors.red, color: colors.red },
    CONTRACTION: { bg: 'linear-gradient(135deg, #3B1111, #4D1616)', border: colors.red, color: colors.red },
};

const SECTOR_COLORS = {
    Technology: '#3B82F6',
    Energy: '#22C55E',
    Financials: '#F59E0B',
    Healthcare: '#EC4899',
    'Consumer Discretionary': '#8B5CF6',
    'Communication Services': '#06B6D4',
    Industrials: '#78716C',
    'Real Estate': '#D946EF',
    Utilities: '#14B8A6',
    Materials: '#FB923C',
    'Consumer Staples': '#A3E635',
    Crypto: '#F59E0B',
};

const MARKET_PULSE_TICKERS = ['SPY', 'QQQ', 'VIX', 'DX-Y.NYB', 'BTC-USD', 'TLT'];
const MARKET_PULSE_LABELS = { 'SPY': 'S&P 500', 'QQQ': 'NASDAQ', 'VIX': 'VIX', 'DX-Y.NYB': 'DXY', 'BTC-USD': 'BTC', 'TLT': '10Y' };

/* ─────────────────────────── Utility Fns ─────────────────────────── */

const flowColor = (pct) => {
    if (pct == null) return colors.textMuted;
    if (pct > 0.03) return colors.green;
    if (pct > 0) return '#4ADE80';
    if (pct < -0.03) return colors.red;
    if (pct < 0) return '#F97316';
    return colors.textMuted;
};

const formatPrice = (val) => {
    if (val == null) return '--';
    const n = typeof val === 'number' ? val : parseFloat(val);
    if (isNaN(n)) return '--';
    if (n >= 10000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    if (n >= 100) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const formatPct = (pct) => {
    if (pct == null) return null;
    const v = (pct * 100).toFixed(1);
    return pct >= 0 ? `+${v}%` : `${v}%`;
};

const timeAgo = (ts) => {
    if (!ts) return '';
    const diff = Date.now() - new Date(ts).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
};

/* ─────────────────────── Sparkline Component ─────────────────────── */

function Sparkline({ data, width = 64, height = 20, color }) {
    if (!data || data.length < 2) return null;
    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    const points = data.map((v, i) => {
        const x = (i / (data.length - 1)) * width;
        const y = height - ((v - min) / range) * (height - 2) - 1;
        return `${x},${y}`;
    }).join(' ');

    return (
        <svg width={width} height={height} style={{ display: 'block', overflow: 'visible' }}>
            <defs>
                <linearGradient id={`spark-${color.replace('#','')}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={color} stopOpacity="0.3" />
                    <stop offset="100%" stopColor={color} stopOpacity="0" />
                </linearGradient>
            </defs>
            <polygon
                points={`0,${height} ${points} ${width},${height}`}
                fill={`url(#spark-${color.replace('#','')})`}
            />
            <polyline
                points={points}
                fill="none"
                stroke={color}
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
            />
        </svg>
    );
}

/* ─────────────────── Animated Number Component ─────────────────── */

function AnimatedNumber({ value, format = 'price', style: extraStyle }) {
    const [display, setDisplay] = useState(value);
    const prevRef = useRef(value);
    const frameRef = useRef(null);

    useEffect(() => {
        const prev = prevRef.current;
        const next = typeof value === 'number' ? value : parseFloat(value);
        const start = typeof prev === 'number' ? prev : parseFloat(prev);

        if (isNaN(next) || isNaN(start) || start === next) {
            setDisplay(next);
            prevRef.current = next;
            return;
        }

        const duration = 400;
        const startTime = performance.now();

        const animate = (now) => {
            const elapsed = now - startTime;
            const progress = Math.min(elapsed / duration, 1);
            // ease out cubic
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = start + (next - start) * eased;
            setDisplay(current);
            if (progress < 1) {
                frameRef.current = requestAnimationFrame(animate);
            } else {
                prevRef.current = next;
            }
        };

        frameRef.current = requestAnimationFrame(animate);
        return () => { if (frameRef.current) cancelAnimationFrame(frameRef.current); };
    }, [value]);

    const formatted = format === 'price' ? formatPrice(display)
        : format === 'pct' ? (typeof display === 'number' ? (display >= 0 ? '+' : '') + (display * 100).toFixed(1) + '%' : '--')
        : String(display);

    return <span style={extraStyle}>{formatted}</span>;
}

/* ─────────────────────── Live Clock Component ─────────────────────── */

function LiveClock() {
    const [now, setNow] = useState(new Date());

    useEffect(() => {
        const id = setInterval(() => setNow(new Date()), 1000);
        return () => clearInterval(id);
    }, []);

    const utc = now.toLocaleTimeString('en-GB', { timeZone: 'UTC', hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const local = now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });

    return (
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center', fontFamily: MONO, fontSize: '11px' }}>
            <span style={{ color: colors.textMuted }}>
                <span style={{ color: colors.textDim, fontSize: '9px', letterSpacing: '0.5px', marginRight: '4px' }}>UTC</span>
                {utc}
            </span>
            <span style={{ color: colors.textMuted }}>
                <span style={{ color: colors.textDim, fontSize: '9px', letterSpacing: '0.5px', marginRight: '4px' }}>LOC</span>
                {local}
            </span>
        </div>
    );
}

/* ─────────────────── Stagger Fade-In Wrapper ─────────────────────── */

function FadeIn({ delay = 0, children, style: extraStyle }) {
    const [visible, setVisible] = useState(false);
    useEffect(() => {
        const t = setTimeout(() => setVisible(true), delay);
        return () => clearTimeout(t);
    }, [delay]);
    return (
        <div style={{
            opacity: visible ? 1 : 0,
            transform: visible ? 'translateY(0)' : 'translateY(8px)',
            transition: `opacity 0.4s cubic-bezier(0.4, 0, 0.2, 1), transform 0.4s cubic-bezier(0.4, 0, 0.2, 1)`,
            ...extraStyle,
        }}>
            {children}
        </div>
    );
}

/* ─────────────────── Alert Banner Component ─────────────────── */

function AlertBanner({ alerts, onDismiss }) {
    if (!alerts || alerts.length === 0) return null;
    const latest = alerts[0];
    const severityColors = {
        high: colors.red,
        medium: colors.yellow,
        low: colors.accent,
        info: colors.textMuted,
    };
    const borderColor = severityColors[latest.severity] || colors.accent;

    return (
        <div style={{
            position: 'fixed', top: 0, left: 0, right: 0, zIndex: 900,
            animation: 'slideDown 0.3s ease-out',
        }}>
            <div style={{
                maxWidth: '1200px', margin: '0 auto',
                padding: '10px 16px',
                background: `linear-gradient(135deg, ${colors.card} 0%, ${colors.cardElevated} 100%)`,
                borderBottom: `2px solid ${borderColor}`,
                display: 'flex', alignItems: 'center', gap: '12px',
                backdropFilter: 'blur(12px)',
                boxShadow: `0 4px 24px rgba(0,0,0,0.5), 0 0 16px ${borderColor}20`,
            }}>
                <span style={{
                    width: '8px', height: '8px', borderRadius: '50%',
                    background: borderColor,
                    boxShadow: `0 0 8px ${borderColor}`,
                    flexShrink: 0,
                    animation: 'pulse 1.5s ease-in-out infinite',
                }} />
                <span style={{
                    fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                    color: borderColor, letterSpacing: '1px',
                    textTransform: 'uppercase', flexShrink: 0,
                }}>
                    {latest.severity || 'ALERT'}
                </span>
                <span style={{
                    fontFamily: SANS, fontSize: '12px', color: colors.text,
                    flex: 1, overflow: 'hidden', textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                }}>
                    {latest.message}
                </span>
                {alerts.length > 1 && (
                    <span style={{
                        fontFamily: MONO, fontSize: '10px', color: colors.textMuted,
                        flexShrink: 0,
                    }}>
                        +{alerts.length - 1} more
                    </span>
                )}
                <button onClick={() => onDismiss(latest.id)} style={{
                    background: 'none', border: 'none', color: colors.textMuted,
                    cursor: 'pointer', fontSize: '16px', padding: '2px 6px',
                    flexShrink: 0,
                }}>{'\u00d7'}</button>
            </div>
        </div>
    );
}

/* ─────────────────── Recommendation Toast Component ─────────────────── */

function RecommendationToast({ recommendations, onDismiss }) {
    if (!recommendations || recommendations.length === 0) return null;
    const latest = recommendations[0];
    const dirColor = latest.direction === 'CALL' ? colors.green : colors.red;

    return (
        <div style={{
            position: 'fixed', bottom: '90px', right: '16px', zIndex: 800,
            width: '320px',
            animation: 'slideUp 0.3s ease-out',
        }}>
            <div style={{
                background: colors.gradientCard,
                border: `1px solid ${dirColor}30`,
                borderLeft: `3px solid ${dirColor}`,
                borderRadius: tokens.radius.md,
                padding: '12px 16px',
                boxShadow: `0 8px 32px rgba(0,0,0,0.5), 0 0 12px ${dirColor}10`,
                backdropFilter: 'blur(8px)',
            }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                    <span style={{
                        fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                        letterSpacing: '1px', color: dirColor,
                    }}>
                        NEW {latest.direction} REC
                    </span>
                    <button onClick={() => onDismiss(latest.id)} style={{
                        background: 'none', border: 'none', color: colors.textMuted,
                        cursor: 'pointer', fontSize: '14px', padding: '0 4px',
                    }}>{'\u00d7'}</button>
                </div>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: '8px', marginBottom: '4px' }}>
                    <span style={{ fontFamily: MONO, fontSize: '16px', fontWeight: 700, color: '#E8F0F8' }}>
                        {latest.ticker}
                    </span>
                    <span style={{ fontFamily: MONO, fontSize: '12px', color: colors.textDim }}>
                        K={latest.strike} {latest.expiry ? `exp ${latest.expiry}` : ''}
                    </span>
                </div>
                {latest.confidence != null && (
                    <div style={{ fontFamily: MONO, fontSize: '10px', color: colors.textMuted }}>
                        conf {(latest.confidence * 100).toFixed(0)}%
                        {latest.expected_return != null && ` | E[R]=${(latest.expected_return * 100).toFixed(1)}%`}
                    </div>
                )}
                {latest.thesis && (
                    <div style={{
                        fontFamily: SANS, fontSize: '11px', color: colors.textDim,
                        marginTop: '4px', lineHeight: 1.3,
                        overflow: 'hidden', textOverflow: 'ellipsis',
                        display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
                    }}>
                        {latest.thesis}
                    </div>
                )}
            </div>
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════════ */
/* ║                          DASHBOARD                                 ║ */
/* ═══════════════════════════════════════════════════════════════════════ */

export default function Dashboard({ onNavigate }) {
    const {
        currentRegime, journalEntries, systemStatus,
        setCurrentRegime, setJournalEntries, setSystemStatus,
        setLoading, addNotification, agentProgress,
        liveAlerts, liveRecommendations, dismissAlert, dismissRecommendation,
        livePriceUpdates,
    } = useStore();

    const { isMobile } = useDevice();
    const { connected: wsConnected, prices: wsPrices } = useWebSocket();
    const [ollamaStatus, setOllamaStatus] = useState(null);
    const [latestBriefing, setLatestBriefing] = useState(null);
    const [watchlist, setWatchlist] = useState([]);
    const [addingTicker, setAddingTicker] = useState(false);
    const [newTicker, setNewTicker] = useState('');
    const [liveSignals, setLiveSignals] = useState(null);
    const [askOpen, setAskOpen] = useState(false);
    const [askQuery, setAskQuery] = useState('');
    const [askResult, setAskResult] = useState(null);
    const [actionResult, setActionResult] = useState(null);

    const [widgetPrefs, setWidgetPrefs] = useState(loadWidgetPrefs);
    const [widgetPanelOpen, setWidgetPanelOpen] = useState(false);

    const [enrichedWatchlist, setEnrichedWatchlist] = useState([]);
    const [suggestions, setSuggestions] = useState([]);
    const [livePrices, setLivePrices] = useState({});
    const [refreshingPrices, setRefreshingPrices] = useState(false);

    // Hover state for watchlist cards
    const [hoveredCard, setHoveredCard] = useState(null);

    // Pull-to-refresh state
    const [pullRefreshing, setPullRefreshing] = useState(false);
    const pullStartY = useRef(null);
    const [pullDistance, setPullDistance] = useState(0);

    // Swipe-to-delete state
    const [swipedTicker, setSwipedTicker] = useState(null);
    const swipeStartX = useRef(null);

    // Ticker search autocomplete state
    const [searchResults, setSearchResults] = useState([]);
    const [searchOpen, setSearchOpen] = useState(false);
    const [searchLoading, setSearchLoading] = useState(false);
    const [selectedIdx, setSelectedIdx] = useState(-1);
    const [selectedMeta, setSelectedMeta] = useState(null);
    const searchRef = useRef(null);
    const debounceRef = useRef(null);
    const dropdownRef = useRef(null);

    // Debounced ticker search
    const doSearch = useCallback((query) => {
        if (debounceRef.current) clearTimeout(debounceRef.current);
        if (!query || query.trim().length < 1) {
            setSearchResults([]);
            setSearchOpen(false);
            return;
        }
        setSearchLoading(true);
        debounceRef.current = setTimeout(async () => {
            try {
                const res = await api.searchWatchlistTickers(query.trim());
                setSearchResults(res?.results || []);
                setSearchOpen(true);
                setSelectedIdx(-1);
            } catch {
                setSearchResults([]);
            }
            setSearchLoading(false);
        }, 300);
    }, []);

    // Close dropdown on outside click
    useEffect(() => {
        const handleClick = (e) => {
            if (dropdownRef.current && !dropdownRef.current.contains(e.target)
                && searchRef.current && !searchRef.current.contains(e.target)) {
                setSearchOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClick);
        return () => document.removeEventListener('mousedown', handleClick);
    }, []);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading('dashboard', true);
        try {
            const [regime, journal, status, ollama, briefing, wl, signals, enrichedWl] = await Promise.all([
                api.getCurrent().catch(() => null),
                api.getJournal({ limit: 3 }).catch(() => ({ entries: [] })),
                api.getStatus().catch(() => null),
                api.getOllamaStatus().catch(() => null),
                api.getLatestBriefing('hourly').catch(() => null),
                api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] })),
                api.getSignalSnapshot().catch(() => null),
                api.getWatchlistEnriched(10).catch(() => ({ items: [], suggestions: [] })),
            ]);
            if (regime) setCurrentRegime(regime);
            if (journal?.entries) setJournalEntries(journal.entries);
            if (status) setSystemStatus(status);
            setOllamaStatus(ollama);
            setLatestBriefing(briefing);
            if (wl?.items) setWatchlist(wl.items);
            setLiveSignals(signals);
            if (enrichedWl?.items) setEnrichedWatchlist(enrichedWl.items);
            if (enrichedWl?.suggestions) setSuggestions(enrichedWl.suggestions);
            // Trigger batch price refresh in background
            api.refreshWatchlistPrices().then(r => {
                if (r?.prices) setLivePrices(r.prices);
            }).catch(() => {});
            // Preload analysis data for all watchlist tickers in background
            // so detail pages load instantly when user clicks a ticker
            api.preloadWatchlist().catch(() => {});
        } catch {
            addNotification('error', 'Failed to load dashboard');
        }
        setLoading('dashboard', false);
    };

    // Pull-to-refresh handlers
    const handleTouchStart = useCallback((e) => {
        if (window.scrollY === 0) {
            pullStartY.current = e.touches[0].clientY;
        }
    }, []);

    const handleTouchMove = useCallback((e) => {
        if (pullStartY.current == null) return;
        const dist = e.touches[0].clientY - pullStartY.current;
        if (dist > 0 && dist < 120) {
            setPullDistance(dist);
        }
    }, []);

    const handleTouchEnd = useCallback(() => {
        if (pullDistance > 60) {
            setPullRefreshing(true);
            loadData().finally(() => {
                setPullRefreshing(false);
                setPullDistance(0);
            });
        } else {
            setPullDistance(0);
        }
        pullStartY.current = null;
    }, [pullDistance]);

    // Swipe-to-delete handlers
    const handleSwipeStart = useCallback((ticker, e) => {
        swipeStartX.current = e.touches[0].clientX;
    }, []);

    const handleSwipeEnd = useCallback((ticker, e) => {
        if (swipeStartX.current == null) return;
        const dist = swipeStartX.current - e.changedTouches[0].clientX;
        if (dist > 80) {
            setSwipedTicker(ticker);
        } else {
            setSwipedTicker(null);
        }
        swipeStartX.current = null;
    }, []);

    const handleSelectSearchResult = (result) => {
        setNewTicker(result.ticker);
        setSelectedMeta(result);
        setSearchOpen(false);
        setSearchResults([]);
    };

    const handleAddTicker = async () => {
        if (!newTicker.trim()) return;
        setAddingTicker(true);
        try {
            const payload = {
                ticker: newTicker.trim(),
                ...(selectedMeta?.name && { display_name: selectedMeta.name }),
                ...(selectedMeta?.asset_type && { asset_type: selectedMeta.asset_type }),
            };
            await api.addToWatchlist(payload);
            setNewTicker('');
            setSelectedMeta(null);
            setSearchResults([]);
            setSearchOpen(false);
            addNotification('success', `Added ${newTicker.trim().toUpperCase()}`);
            const [wl, enrichedWl] = await Promise.all([
                api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] })),
                api.getWatchlistEnriched(10).catch(() => ({ items: [], suggestions: [] })),
            ]);
            if (wl?.items) setWatchlist(wl.items);
            if (enrichedWl?.items) setEnrichedWatchlist(enrichedWl.items);
            if (enrichedWl?.suggestions) setSuggestions(enrichedWl.suggestions);
        } catch (err) {
            addNotification('error', err.message || 'Failed');
        }
        setAddingTicker(false);
    };

    const handleRefreshPrices = async () => {
        setRefreshingPrices(true);
        try {
            const r = await api.refreshWatchlistPrices();
            if (r?.prices) setLivePrices(r.prices);
        } catch {
            addNotification('error', 'Price refresh failed');
        }
        setRefreshingPrices(false);
    };

    const _runAction = async (type) => {
        setActionResult(`Running ${type}...`);
        try {
            if (type === 'briefing') {
                const r = await api.generateBriefing('daily');
                setActionResult(`Briefing: ${r?.content?.length || 0} chars`);
                loadData();
            } else if (type === 'regime') {
                await api.runWorkflow('auto-regime');
                setActionResult('Regime updated');
                loadData();
            } else if (type === 'pull') {
                await api.runWorkflow('daily-pulls');
                setActionResult('Data pull started');
            } else if (type === 'orthogonality') {
                await api.runOrthogonality();
                setActionResult('Orthogonality started');
            }
        } catch (err) {
            setActionResult(`Error: ${err.message || 'failed'}`);
        }
        setTimeout(() => setActionResult(null), 5000);
    };

    /* ─── Derived state ─── */
    const dbOnline = systemStatus?.database?.connected;
    const hsOnline = systemStatus?.hyperspace?.node_online;
    const ollamaOnline = ollamaStatus?.available;
    const allConnected = wsConnected && dbOnline && (hsOnline || ollamaOnline);

    const regime = currentRegime;
    const regimeState = regime?.state || '?';
    const regimeConf = regime?.confidence;
    const regimeBadge = REGIME_BADGE[regimeState?.toUpperCase()] || REGIME_BADGE.FRAGILE;

    const feats = liveSignals?.features || [];
    const withZ = feats.filter(f => f.z_score != null);
    const bullish = withZ.filter(f => f.z_score > 0.5).length;
    const bearish = withZ.filter(f => f.z_score < -0.5).length;
    const extreme = withZ.filter(f => Math.abs(f.z_score) > 2.5).length;

    // Build market pulse from enriched watchlist + live prices + WebSocket prices
    const marketPulse = useMemo(() => {
        return MARKET_PULSE_TICKERS.map(ticker => {
            const enriched = enrichedWatchlist.find(w => w.ticker === ticker);
            const lp = livePrices[ticker];
            // WebSocket real-time prices take highest priority
            const wsPrice = livePriceUpdates?.[ticker] || wsPrices?.[ticker];
            const best = wsPrice || lp;
            const item = best ? { ...enriched, price: best.price, pct_1d: best.pct_1d, pct_1w: best.pct_1w } : enriched;
            return {
                ticker,
                label: MARKET_PULSE_LABELS[ticker] || ticker,
                price: item?.price,
                pct_1d: item?.pct_1d,
                prevPrice: enriched?.price,
                sparkData: item?.spark_data || item?.sparkline || null,
            };
        });
    }, [enrichedWatchlist, livePrices, livePriceUpdates, wsPrices]);

    /* ═══════════════════════════ RENDER ═══════════════════════════ */

    return (
        <div
            onTouchStart={isMobile ? handleTouchStart : undefined}
            onTouchMove={isMobile ? handleTouchMove : undefined}
            onTouchEnd={isMobile ? handleTouchEnd : undefined}
            style={{
                padding: isMobile ? SPACE.sm : SPACE.md,
                paddingTop: `calc(env(safe-area-inset-top, 0px) + ${SPACE.md})`,
                maxWidth: '1200px',
                margin: '0 auto',
                overflowX: 'hidden',
                fontSize: isMobile ? '13px' : '14px',
            }}
        >
            {/* Pull-to-refresh indicator */}
            {(pullDistance > 0 || pullRefreshing) && (
                <div style={{
                    textAlign: 'center',
                    padding: '8px 0',
                    fontSize: '11px',
                    color: colors.accent,
                    fontFamily: MONO,
                    opacity: Math.min(pullDistance / 60, 1),
                    transform: `translateY(${Math.min(pullDistance * 0.3, 20)}px)`,
                    transition: pullRefreshing ? 'none' : 'transform 0.1s',
                }}>
                    {pullRefreshing ? 'Refreshing...' : pullDistance > 60 ? 'Release to refresh' : 'Pull to refresh'}
                </div>
            )}

            {/* ═══════════════ LIVE OVERLAYS ═══════════════ */}
            <AlertBanner alerts={liveAlerts} onDismiss={dismissAlert} />
            <RecommendationToast recommendations={liveRecommendations} onDismiss={dismissRecommendation} />

            {/* ═══════════════════ HEADER BAR ═══════════════════ */}
            <FadeIn delay={0}>
                <div style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    marginBottom: SPACE.lg,
                    paddingBottom: SPACE.md,
                    borderBottom: `1px solid ${colors.borderSubtle}`,
                }}>
                    {/* Left: Logo + Regime */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: SPACE.md }}>
                        <span style={{
                            fontFamily: MONO,
                            fontSize: '22px',
                            fontWeight: 800,
                            color: colors.accent,
                            letterSpacing: '4px',
                            textShadow: '0 0 20px rgba(26, 110, 191, 0.3)',
                        }}>GRID</span>

                        {/* Regime badge */}
                        <div style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '6px',
                            padding: '4px 12px',
                            borderRadius: '6px',
                            background: regimeBadge.bg,
                            border: `1px solid ${regimeBadge.border}30`,
                            fontFamily: MONO,
                            fontSize: '11px',
                            fontWeight: 700,
                            color: regimeBadge.color,
                            letterSpacing: '1px',
                        }}>
                            <span style={{
                                width: '6px', height: '6px', borderRadius: '50%',
                                background: regimeBadge.color,
                                boxShadow: `0 0 6px ${regimeBadge.color}`,
                                animation: 'pulse 2s ease-in-out infinite',
                            }} />
                            {regimeState}
                            {regimeConf != null && (
                                <span style={{ color: `${regimeBadge.color}90`, fontWeight: 500, fontSize: '10px' }}>
                                    {(regimeConf * 100).toFixed(0)}%
                                </span>
                            )}
                        </div>
                    </div>

                    {/* Right: Clock + Status + Config */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: SPACE.md }}>
                        {!isMobile && <LiveClock />}

                        {/* Connection dot */}
                        <div style={{
                            display: 'flex', alignItems: 'center', gap: '6px',
                            padding: '4px 10px',
                            borderRadius: '6px',
                            background: allConnected ? `${colors.green}10` : `${colors.red}10`,
                            border: `1px solid ${allConnected ? colors.green : colors.red}20`,
                        }}>
                            <span style={{
                                width: '7px', height: '7px', borderRadius: '50%',
                                background: allConnected ? colors.green : colors.red,
                                boxShadow: `0 0 8px ${allConnected ? colors.green : colors.red}60`,
                            }} />
                            <span style={{
                                fontFamily: MONO, fontSize: '9px', fontWeight: 600,
                                color: allConnected ? colors.green : colors.red,
                                letterSpacing: '0.5px',
                            }}>
                                {allConnected ? 'LIVE' : 'OFFLINE'}
                            </span>
                        </div>

                        <ViewHelp id="dashboard" />

                        <button onClick={() => setWidgetPanelOpen(true)} style={{
                            background: colors.card, border: `1px solid ${colors.border}`,
                            borderRadius: '6px', padding: '6px 10px', cursor: 'pointer',
                            fontSize: '11px', color: colors.textMuted, minHeight: '32px',
                            fontFamily: MONO,
                            transition: `all ${tokens.transition.fast}`,
                        }}
                            onMouseEnter={(e) => { e.currentTarget.style.borderColor = colors.accent; e.currentTarget.style.color = colors.accent; }}
                            onMouseLeave={(e) => { e.currentTarget.style.borderColor = colors.border; e.currentTarget.style.color = colors.textMuted; }}
                        >cfg</button>
                    </div>
                </div>
            </FadeIn>

            {/* ═══════════════ MARKET PULSE ROW ═══════════════ */}
            <FadeIn delay={50}>
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: isMobile ? 'repeat(2, 1fr)' : `repeat(${MARKET_PULSE_TICKERS.length}, 1fr)`,
                    gap: isMobile ? '8px' : '10px',
                    marginBottom: SPACE.lg,
                }}>
                    {marketPulse.map((mp, idx) => {
                        const pctColor = mp.pct_1d == null ? colors.textMuted
                            : mp.pct_1d >= 0 ? colors.green : colors.red;
                        // Flash effect: detect if price changed from WS update
                        const priceChanged = mp.prevPrice != null && mp.price != null
                            && mp.prevPrice !== mp.price;
                        const flashColor = priceChanged
                            ? (mp.price > mp.prevPrice ? `${colors.green}18` : `${colors.red}18`)
                            : null;
                        return (
                            <div key={mp.ticker} style={{
                                background: flashColor
                                    ? `linear-gradient(145deg, ${flashColor} 0%, ${colors.card} 100%)`
                                    : colors.gradientCard,
                                border: `1px solid ${flashColor ? (mp.price > mp.prevPrice ? `${colors.green}30` : `${colors.red}30`) : colors.border}`,
                                borderRadius: tokens.radius.md,
                                padding: isMobile ? '10px 12px' : '12px 14px',
                                transition: 'all 0.6s ease-out',
                                cursor: 'default',
                                position: 'relative',
                                overflow: 'hidden',
                            }}
                                onMouseEnter={(e) => {
                                    e.currentTarget.style.borderColor = `${colors.accent}50`;
                                    e.currentTarget.style.boxShadow = '0 4px 16px rgba(0,0,0,0.3)';
                                }}
                                onMouseLeave={(e) => {
                                    e.currentTarget.style.borderColor = colors.border;
                                    e.currentTarget.style.boxShadow = 'none';
                                }}
                            >
                                {/* Label */}
                                <div style={{
                                    fontFamily: SANS, fontSize: '10px', fontWeight: 600,
                                    color: colors.textMuted, letterSpacing: '0.5px',
                                    marginBottom: '6px', textTransform: 'uppercase',
                                }}>
                                    {mp.label}
                                </div>

                                {/* Price */}
                                <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px', marginBottom: '4px' }}>
                                    <span style={{
                                        fontFamily: MONO, fontSize: '15px', fontWeight: 700,
                                        color: '#E8F0F8',
                                    }}>
                                        {mp.price != null ? (
                                            <AnimatedNumber value={mp.price} format="price" />
                                        ) : '--'}
                                    </span>
                                </div>

                                {/* Change */}
                                <div style={{
                                    fontFamily: MONO, fontSize: '11px', fontWeight: 600,
                                    color: pctColor,
                                }}>
                                    {formatPct(mp.pct_1d) || '--'}
                                </div>

                                {/* Sparkline positioned bottom-right */}
                                {mp.sparkData && mp.sparkData.length >= 2 && (
                                    <div style={{ position: 'absolute', bottom: '4px', right: '8px', opacity: 0.6 }}>
                                        <Sparkline data={mp.sparkData} width={48} height={16} color={pctColor} />
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            </FadeIn>

            {/* ═══════════════ SITUATION REPORT ═══════════════ */}
            <FadeIn delay={100}>
                <div style={{
                    background: colors.gradientCard,
                    border: `1px solid ${colors.border}`,
                    borderLeft: `3px solid ${regimeBadge.color}`,
                    borderRadius: tokens.radius.md,
                    padding: SPACE.md,
                    marginBottom: SPACE.md,
                    boxShadow: colors.shadow.sm,
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                        <span style={{
                            fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                            letterSpacing: '1.5px', color: colors.accent,
                        }}>SITUATION REPORT</span>
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                            {regime?.as_of ? timeAgo(regime.as_of) : ''}
                        </span>
                    </div>

                    <RegimeThermometer regime={regime} />

                    {/* Signal summary */}
                    <div style={{
                        display: 'flex', gap: SPACE.md, marginTop: '12px',
                        fontSize: '12px', fontFamily: MONO,
                    }}>
                        <span style={{ color: colors.textDim }}>
                            <span style={{ color: colors.green, fontWeight: 700, fontSize: '14px' }}>{bullish}</span>
                            <span style={{ marginLeft: '4px' }}>bullish</span>
                        </span>
                        <span style={{ color: colors.textDim }}>
                            <span style={{ color: colors.red, fontWeight: 700, fontSize: '14px' }}>{bearish}</span>
                            <span style={{ marginLeft: '4px' }}>bearish</span>
                        </span>
                        {extreme > 0 && (
                            <span style={{ color: colors.yellow, fontWeight: 700 }}>
                                {extreme} extreme
                            </span>
                        )}
                        <span style={{ color: colors.textMuted, marginLeft: 'auto' }}>
                            {withZ.length} signals
                        </span>
                    </div>

                    {/* Latest posture */}
                    {journalEntries.length > 0 && (() => {
                        const e = journalEntries[0];
                        const posture = e.grid_recommendation || e.action_taken || '';
                        return (
                            <div style={{
                                marginTop: '12px', paddingTop: '12px',
                                borderTop: `1px solid ${colors.borderSubtle}`,
                                fontSize: '12px', color: colors.textDim,
                                display: 'flex', alignItems: 'center', gap: '8px',
                            }}>
                                <span style={{
                                    fontFamily: MONO, fontWeight: 700, color: colors.text,
                                    padding: '2px 8px', borderRadius: '4px',
                                    background: `${colors.accent}15`,
                                }}>
                                    {posture}
                                </span>
                                {e.counterfactual && (
                                    <span style={{ fontStyle: 'italic', color: colors.textMuted, fontSize: '11px' }}>
                                        {e.counterfactual}
                                    </span>
                                )}
                            </div>
                        );
                    })()}
                </div>
            </FadeIn>

            {/* ═══════════════ CAPITAL FLOW ANALYSIS ═══════════════ */}
            <FadeIn delay={150}>
                <div style={{ marginBottom: SPACE.md }}>
                    <CapitalFlowAnalysis />
                </div>
            </FadeIn>

            {/* ═══════════════ INTELLIGENCE FEED ═══════════════ */}
            <FadeIn delay={200}>
                {latestBriefing?.content && (() => {
                    const content = latestBriefing.content || '';
                    const bottomLineMatch = content.match(/## (?:Bottom Line|What'?s Happening[^\n]*)\n+([\s\S]*?)(?=\n## |\n---|\n\*Generated|$)/i);
                    const actionMatch = content.match(/## (?:Action|Tomorrow|Playbook|Opportunities)[^\n]*\n+([\s\S]*?)(?=\n## |\n---|\n\*Generated|$)/i);

                    const bottomLine = bottomLineMatch ? bottomLineMatch[1].trim().replace(/\*\*/g, '').substring(0, 300) : null;
                    const action = actionMatch ? actionMatch[1].trim().replace(/\*\*/g, '').substring(0, 200) : null;
                    const fallback = content.replace(/^#[^\n]*\n+/gm, '').trim().substring(0, 300);

                    return (
                        <div style={{ marginBottom: SPACE.md }}>
                            <div style={{
                                fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                                letterSpacing: '1.5px', color: colors.accent,
                                marginBottom: SPACE.sm,
                            }}>INTELLIGENCE</div>

                            <div style={{
                                display: 'grid',
                                gridTemplateColumns: action ? (isMobile ? '1fr' : '2fr 1fr') : '1fr',
                                gap: '10px',
                            }}>
                                {/* AI Briefing */}
                                <div
                                    onClick={() => onNavigate('briefings')}
                                    style={{
                                        background: colors.gradientCard,
                                        border: `1px solid ${colors.border}`,
                                        borderRadius: tokens.radius.md,
                                        padding: SPACE.md,
                                        cursor: 'pointer',
                                        transition: `all ${tokens.transition.fast}`,
                                        position: 'relative',
                                        overflow: 'hidden',
                                    }}
                                    onMouseEnter={(e) => {
                                        e.currentTarget.style.borderColor = `${colors.accent}50`;
                                        e.currentTarget.style.boxShadow = `0 4px 20px rgba(26, 110, 191, 0.1)`;
                                    }}
                                    onMouseLeave={(e) => {
                                        e.currentTarget.style.borderColor = colors.border;
                                        e.currentTarget.style.boxShadow = 'none';
                                    }}
                                >
                                    {/* Subtle accent gradient overlay */}
                                    <div style={{
                                        position: 'absolute', top: 0, left: 0, right: 0, height: '2px',
                                        background: `linear-gradient(90deg, ${colors.accent}, transparent)`,
                                    }} />

                                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '10px' }}>
                                        <span style={{
                                            fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                                            letterSpacing: '1px', color: colors.accent,
                                        }}>AI BRIEFING</span>
                                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                                            {latestBriefing.generated_at ? timeAgo(latestBriefing.generated_at) : ''} · Full ›
                                        </span>
                                    </div>

                                    <div style={{
                                        fontSize: '13px', lineHeight: '1.7', color: colors.text,
                                        fontFamily: SANS, fontWeight: 500,
                                    }}>
                                        {bottomLine || fallback}
                                    </div>
                                </div>

                                {/* Trade Recommendations / Action */}
                                {action && (
                                    <div style={{
                                        background: `linear-gradient(145deg, ${colors.accent}08, ${colors.accent}03)`,
                                        border: `1px solid ${colors.accent}25`,
                                        borderRadius: tokens.radius.md,
                                        padding: SPACE.md,
                                    }}>
                                        <div style={{
                                            fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                                            letterSpacing: '1px', color: colors.accent,
                                            marginBottom: '10px',
                                        }}>RECOMMENDED</div>
                                        <div style={{
                                            fontSize: '12px', lineHeight: '1.6', color: colors.textDim,
                                            fontFamily: SANS,
                                        }}>
                                            {action}
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* Convergence / Agent alerts */}
                            {agentProgress && (
                                <div style={{
                                    marginTop: '10px',
                                    background: `linear-gradient(145deg, ${colors.accent}06, transparent)`,
                                    border: `1px solid ${colors.accent}20`,
                                    borderRadius: tokens.radius.md,
                                    padding: '10px 14px',
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center',
                                }}>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                        <span style={{
                                            width: '6px', height: '6px', borderRadius: '50%',
                                            background: colors.accent,
                                            animation: 'pulse 1.5s ease-in-out infinite',
                                        }} />
                                        <span style={{ fontFamily: MONO, fontSize: '11px', color: colors.accent, fontWeight: 600 }}>
                                            AGENT
                                        </span>
                                        <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                            {agentProgress.stage}: {agentProgress.detail}
                                        </span>
                                    </div>
                                    <span style={{
                                        fontFamily: MONO, fontSize: '11px', fontWeight: 700,
                                        padding: '2px 8px', borderRadius: '4px',
                                        background: `${colors.accent}20`, color: colors.accent,
                                    }}>{agentProgress.ticker}</span>
                                </div>
                            )}
                        </div>
                    );
                })()}
            </FadeIn>

            {/* ═══════════════ WATCHLIST ═══════════════ */}
            <FadeIn delay={250}>
                <div style={{ marginBottom: SPACE.md }} data-onboarding="watchlist">
                    {/* Watchlist header */}
                    <div style={{
                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                        marginBottom: SPACE.sm,
                    }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            <span style={{
                                fontFamily: MONO, fontSize: '10px', fontWeight: 700,
                                letterSpacing: '1.5px', color: colors.accent,
                            }}>WATCHLIST</span>
                            <button onClick={handleRefreshPrices} disabled={refreshingPrices}
                                title="Refresh prices"
                                style={{
                                    background: 'none', border: `1px solid ${colors.border}`, borderRadius: '6px',
                                    padding: '3px 8px', fontSize: '12px', color: colors.textMuted, cursor: 'pointer',
                                    fontFamily: MONO,
                                    opacity: refreshingPrices ? 0.5 : 1,
                                    transition: `all ${tokens.transition.fast}`,
                                }}
                                onMouseEnter={(e) => { e.currentTarget.style.borderColor = colors.accent; e.currentTarget.style.color = colors.accent; }}
                                onMouseLeave={(e) => { e.currentTarget.style.borderColor = colors.border; e.currentTarget.style.color = colors.textMuted; }}
                            >&#x21bb;</button>
                        </div>
                        <button onClick={() => setAskOpen(true)} style={{
                            background: `${colors.accent}10`,
                            border: `1px solid ${colors.accent}30`,
                            borderRadius: '6px',
                            padding: '5px 12px', fontSize: '10px', color: colors.accent, cursor: 'pointer',
                            fontFamily: MONO, fontWeight: 600, letterSpacing: '0.5px',
                            transition: `all ${tokens.transition.fast}`,
                        }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = `${colors.accent}20`; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = `${colors.accent}10`; }}
                        >Ask GRID</button>
                    </div>

                    {/* Search bar */}
                    <div style={{ position: 'relative', marginBottom: SPACE.sm }}>
                        <div style={{ display: 'flex', gap: '8px' }}>
                            <div style={{ flex: 1, position: 'relative' }}>
                                <input ref={searchRef} type="text" value={newTicker}
                                    onChange={(e) => {
                                        const val = e.target.value;
                                        setNewTicker(val);
                                        setSelectedMeta(null);
                                        doSearch(val);
                                    }}
                                    onKeyDown={(e) => {
                                        if (e.key === 'Escape') { setSearchOpen(false); return; }
                                        if (e.key === 'ArrowDown') { e.preventDefault(); setSelectedIdx(i => Math.min(i + 1, searchResults.length - 1)); return; }
                                        if (e.key === 'ArrowUp') { e.preventDefault(); setSelectedIdx(i => Math.max(i - 1, -1)); return; }
                                        if (e.key === 'Enter') {
                                            if (searchOpen && selectedIdx >= 0 && searchResults[selectedIdx]) {
                                                handleSelectSearchResult(searchResults[selectedIdx]);
                                            } else { handleAddTicker(); }
                                            return;
                                        }
                                    }}
                                    onFocus={() => { if (searchResults.length > 0) setSearchOpen(true); }}
                                    placeholder="Search ticker..."
                                    autoComplete="off"
                                    style={{
                                        background: colors.bg, border: `1px solid ${colors.border}`,
                                        borderRadius: tokens.radius.sm, color: colors.text,
                                        padding: '10px 14px', fontSize: '13px',
                                        fontFamily: MONO, width: '100%', boxSizing: 'border-box',
                                        minHeight: tokens.minTouch,
                                        transition: `border-color ${tokens.transition.fast}`,
                                        outline: 'none',
                                    }}
                                    onFocusCapture={(e) => { e.currentTarget.style.borderColor = colors.accent; }}
                                    onBlurCapture={(e) => { e.currentTarget.style.borderColor = colors.border; }}
                                />
                                {selectedMeta && (
                                    <div style={{
                                        position: 'absolute', right: '10px', top: '50%', transform: 'translateY(-50%)',
                                        fontSize: '9px', color: colors.textMuted, fontFamily: MONO,
                                    }}>
                                        <span style={{
                                            padding: '2px 6px', borderRadius: '3px',
                                            background: selectedMeta.asset_type === 'etf' ? `${colors.accent}20` :
                                                selectedMeta.asset_type === 'crypto' ? `${colors.yellow}20` :
                                                selectedMeta.asset_type === 'index' ? `${colors.green}20` :
                                                colors.border,
                                            color: selectedMeta.asset_type === 'etf' ? colors.accent :
                                                selectedMeta.asset_type === 'crypto' ? colors.yellow :
                                                selectedMeta.asset_type === 'index' ? colors.green :
                                                colors.textMuted,
                                        }}>{selectedMeta.asset_type}</span>
                                    </div>
                                )}
                            </div>
                            <button onClick={handleAddTicker} disabled={addingTicker || !newTicker.trim()}
                                style={{
                                    ...shared.buttonSmall,
                                    ...(addingTicker || !newTicker.trim() ? shared.buttonDisabled : {}),
                                    borderRadius: tokens.radius.sm,
                                    fontFamily: MONO,
                                    fontWeight: 600,
                                    letterSpacing: '0.5px',
                                }}
                            >
                                {addingTicker ? '...' : 'Add'}
                            </button>
                        </div>

                        {/* Search dropdown */}
                        {searchOpen && searchResults.length > 0 && (
                            <div ref={dropdownRef} style={{
                                position: 'absolute', top: '100%', left: 0, right: 0,
                                zIndex: 100, marginTop: '4px',
                                background: colors.card, border: `1px solid ${colors.border}`,
                                borderRadius: tokens.radius.sm, boxShadow: colors.shadow.lg,
                                maxHeight: '280px', overflowY: 'auto',
                            }}>
                                {searchResults.map((r, idx) => {
                                    const isSelected = idx === selectedIdx;
                                    const typeIcon = r.asset_type === 'stock' ? 'S' :
                                        r.asset_type === 'etf' ? 'E' :
                                        r.asset_type === 'crypto' ? 'C' :
                                        r.asset_type === 'index' ? 'I' :
                                        r.asset_type === 'commodity' ? 'CM' :
                                        r.asset_type === 'forex' ? 'FX' : '?';
                                    const typeColor = r.asset_type === 'etf' ? colors.accent :
                                        r.asset_type === 'crypto' ? colors.yellow :
                                        r.asset_type === 'index' ? colors.green :
                                        r.asset_type === 'commodity' ? '#F97316' :
                                        r.asset_type === 'forex' ? '#8B5CF6' : colors.textMuted;
                                    return (
                                        <div key={`${r.ticker}-${r.source}`}
                                            onClick={() => handleSelectSearchResult(r)}
                                            onMouseEnter={() => setSelectedIdx(idx)}
                                            style={{
                                                display: 'flex', alignItems: 'center', gap: '10px',
                                                padding: '10px 12px', cursor: 'pointer',
                                                background: isSelected ? colors.cardElevated : 'transparent',
                                                borderBottom: idx < searchResults.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                                                transition: `background ${tokens.transition.fast}`,
                                            }}
                                        >
                                            <span style={{
                                                fontSize: '9px', fontWeight: 700, padding: '2px 5px',
                                                borderRadius: '3px', minWidth: '22px', textAlign: 'center',
                                                background: `${typeColor}20`, color: typeColor,
                                                fontFamily: MONO,
                                            }}>{typeIcon}</span>
                                            <div style={{ flex: 1, minWidth: 0 }}>
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                                    <span style={{
                                                        fontSize: '13px', fontWeight: 700, color: '#E8F0F8',
                                                        fontFamily: MONO,
                                                    }}>{r.ticker}</span>
                                                    <span style={{
                                                        fontSize: '9px', padding: '1px 4px', borderRadius: '3px',
                                                        background: r.source === 'grid' ? `${colors.green}20` :
                                                            r.source === 'sector_map' ? `${colors.accent}20` : colors.bg,
                                                        color: r.source === 'grid' ? colors.green :
                                                            r.source === 'sector_map' ? colors.accent : colors.textMuted,
                                                        fontFamily: MONO,
                                                    }}>{r.source}</span>
                                                </div>
                                                <div style={{
                                                    fontSize: '11px', color: colors.textDim,
                                                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                                                }}>{r.name}</div>
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        )}
                        {searchOpen && searchLoading && searchResults.length === 0 && (
                            <div style={{
                                position: 'absolute', top: '100%', left: 0, right: 0,
                                zIndex: 100, marginTop: '4px',
                                background: colors.card, border: `1px solid ${colors.border}`,
                                borderRadius: tokens.radius.sm, padding: '12px',
                                fontSize: '11px', color: colors.textMuted, textAlign: 'center',
                            }}>Searching...</div>
                        )}
                    </div>

                    {/* Watchlist grid */}
                    {enrichedWatchlist.length > 0 ? (
                        <div style={{
                            display: 'grid',
                            gridTemplateColumns: isMobile ? '1fr' : enrichedWatchlist.length > 4 ? 'repeat(3, 1fr)' : 'repeat(2, 1fr)',
                            gap: isMobile ? '8px' : '10px',
                        }}>
                            {enrichedWatchlist.map((rawItem, cardIdx) => {
                                const lp = livePrices[rawItem.ticker];
                                const item = lp ? {
                                    ...rawItem,
                                    price: lp.price,
                                    pct_1d: lp.pct_1d,
                                    pct_1w: lp.pct_1w,
                                    price_source: 'batch',
                                } : rawItem;
                                const hasLivePrice = !!lp;
                                const sectorColor = SECTOR_COLORS[item.sector] || colors.accent;
                                const isHovered = hoveredCard === item.ticker;
                                const isSwiped = swipedTicker === item.ticker;

                                return (
                                    <FadeIn key={item.id || item.ticker} delay={300 + cardIdx * 50}>
                                      <div style={{ position: 'relative', overflow: 'hidden', borderRadius: tokens.radius.md }}>
                                        {isMobile && isSwiped && (
                                            <div onClick={async (e) => {
                                                e.stopPropagation();
                                                try {
                                                    await api.removeFromWatchlist(item.ticker);
                                                    addNotification('success', `Removed ${item.ticker}`);
                                                    setSwipedTicker(null);
                                                    const [wl2, ew2] = await Promise.all([
                                                        api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] })),
                                                        api.getWatchlistEnriched(10).catch(() => ({ items: [], suggestions: [] })),
                                                    ]);
                                                    if (wl2?.items) setWatchlist(wl2.items);
                                                    if (ew2?.items) setEnrichedWatchlist(ew2.items);
                                                    if (ew2?.suggestions) setSuggestions(ew2.suggestions);
                                                } catch (err) { addNotification('error', err.message || 'Failed'); }
                                            }} style={{
                                                position: 'absolute', top: 0, right: 0, bottom: 0, width: '80px',
                                                background: colors.red, display: 'flex', alignItems: 'center',
                                                justifyContent: 'center', color: '#fff', fontSize: '12px',
                                                fontWeight: 700, fontFamily: MONO, cursor: 'pointer', zIndex: 1,
                                            }}>DELETE</div>
                                        )}
                                        <div
                                            onClick={() => { if (!isSwiped) onNavigate('watchlist-analysis', item.ticker); else setSwipedTicker(null); }}
                                            onMouseEnter={() => setHoveredCard(item.ticker)}
                                            onMouseLeave={() => setHoveredCard(null)}
                                            onTouchStart={isMobile ? (e) => handleSwipeStart(item.ticker, e) : undefined}
                                            onTouchEnd={isMobile ? (e) => handleSwipeEnd(item.ticker, e) : undefined}
                                            style={{
                                                background: colors.gradientCard,
                                                border: `1px solid ${isHovered ? `${sectorColor}40` : colors.border}`,
                                                borderLeft: `3px solid ${sectorColor}`,
                                                borderRadius: tokens.radius.md,
                                                padding: isMobile ? '12px' : '14px 16px',
                                                cursor: 'pointer',
                                                position: 'relative',
                                                transition: `all ${tokens.transition.normal}`,
                                                boxShadow: isHovered
                                                    ? `0 8px 24px rgba(0,0,0,0.4), 0 0 0 1px ${sectorColor}15`
                                                    : colors.shadow.sm,
                                                transform: isSwiped ? 'translateX(-80px)' : isHovered ? 'translateY(-2px)' : 'translateY(0)',
                                                minHeight: '44px',
                                            }}
                                        >
                                            {/* Delete X — only on hover (desktop) */}
                                            <button
                                                onClick={async (e) => {
                                                    e.stopPropagation();
                                                    try {
                                                        await api.removeFromWatchlist(item.ticker);
                                                        addNotification('success', `Removed ${item.ticker}`);
                                                        const [wl, enrichedWl] = await Promise.all([
                                                            api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] })),
                                                            api.getWatchlistEnriched(10).catch(() => ({ items: [], suggestions: [] })),
                                                        ]);
                                                        if (wl?.items) setWatchlist(wl.items);
                                                        if (enrichedWl?.items) setEnrichedWatchlist(enrichedWl.items);
                                                        if (enrichedWl?.suggestions) setSuggestions(enrichedWl.suggestions);
                                                    } catch (err) {
                                                        addNotification('error', err.message || 'Failed to remove');
                                                    }
                                                }}
                                                style={{
                                                    position: 'absolute', top: '8px', right: '8px',
                                                    background: 'none', border: 'none', cursor: 'pointer',
                                                    color: colors.textMuted, fontSize: '14px', padding: '2px 6px',
                                                    borderRadius: '4px', lineHeight: 1,
                                                    opacity: isHovered ? 1 : 0,
                                                    transition: `all ${tokens.transition.fast}`,
                                                    zIndex: 2,
                                                }}
                                                onMouseEnter={(e) => { e.currentTarget.style.color = colors.red; e.currentTarget.style.background = colors.redBg; }}
                                                onMouseLeave={(e) => { e.currentTarget.style.color = colors.textMuted; e.currentTarget.style.background = 'none'; }}
                                            >\u00d7</button>

                                            {/* Ticker + Price row */}
                                            <div style={{
                                                display: 'flex', justifyContent: 'space-between',
                                                alignItems: 'flex-start', marginBottom: '8px',
                                                paddingRight: '20px',
                                            }}>
                                                <div>
                                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '2px' }}>
                                                        <span style={{
                                                            fontFamily: MONO, fontSize: '16px', fontWeight: 800,
                                                            color: '#E8F0F8', letterSpacing: '0.5px',
                                                        }}>
                                                            {item.ticker}
                                                        </span>
                                                        {hasLivePrice && (
                                                            <span style={{
                                                                fontSize: '8px', fontWeight: 700, padding: '1px 5px',
                                                                borderRadius: '3px', background: `${colors.green}15`,
                                                                color: colors.green, letterSpacing: '0.5px',
                                                                fontFamily: MONO,
                                                            }}>LIVE</span>
                                                        )}
                                                    </div>
                                                    {item.price != null && (
                                                        <span style={{
                                                            fontFamily: MONO, fontSize: '18px', fontWeight: 700,
                                                            color: '#E8F0F8',
                                                        }}>
                                                            ${formatPrice(item.price)}
                                                        </span>
                                                    )}
                                                </div>

                                                {/* Options data */}
                                                {item.options && (
                                                    <span style={{
                                                        fontSize: '9px', padding: '2px 6px', borderRadius: '3px',
                                                        background: item.options.pcr > 1.2 ? colors.redBg : item.options.pcr < 0.7 ? colors.greenBg : colors.bg,
                                                        color: item.options.pcr > 1.2 ? colors.red : item.options.pcr < 0.7 ? colors.green : colors.textMuted,
                                                        fontFamily: MONO, fontWeight: 600,
                                                    }}>P/C {item.options.pcr?.toFixed(2)}</span>
                                                )}
                                            </div>

                                            {/* Change badges row */}
                                            <div style={{
                                                display: 'flex', gap: '6px', marginBottom: '10px',
                                                flexWrap: 'wrap',
                                            }}>
                                                {item.pct_1d != null && (
                                                    <span style={{
                                                        fontFamily: MONO, fontSize: '11px', fontWeight: 600,
                                                        padding: '2px 8px', borderRadius: '4px',
                                                        color: flowColor(item.pct_1d),
                                                        background: `${flowColor(item.pct_1d)}12`,
                                                    }}>
                                                        1D {formatPct(item.pct_1d)}
                                                    </span>
                                                )}
                                                {item.pct_1w != null && (
                                                    <span style={{
                                                        fontFamily: MONO, fontSize: '11px', fontWeight: 600,
                                                        padding: '2px 8px', borderRadius: '4px',
                                                        color: flowColor(item.pct_1w),
                                                        background: `${flowColor(item.pct_1w)}12`,
                                                    }}>
                                                        1W {formatPct(item.pct_1w)}
                                                    </span>
                                                )}
                                                {item.pct_1m != null && (
                                                    <span style={{
                                                        fontFamily: MONO, fontSize: '11px', fontWeight: 600,
                                                        padding: '2px 8px', borderRadius: '4px',
                                                        color: flowColor(item.pct_1m),
                                                        background: `${flowColor(item.pct_1m)}12`,
                                                    }}>
                                                        1M {formatPct(item.pct_1m)}
                                                    </span>
                                                )}
                                            </div>

                                            {/* Insight text */}
                                            {item.insight && (
                                                <div style={{
                                                    fontSize: '11px', color: colors.textMuted,
                                                    lineHeight: '1.5', fontFamily: SANS,
                                                    borderTop: `1px solid ${colors.borderSubtle}`,
                                                    paddingTop: '8px',
                                                }}>
                                                    {item.insight}
                                                </div>
                                            )}
                                        </div>
                                      </div>
                                    </FadeIn>
                                );
                            })}
                        </div>
                    ) : watchlist.length > 0 ? (
                        <div style={{
                            color: colors.textMuted, fontSize: '11px', textAlign: 'center',
                            padding: SPACE.lg, fontFamily: MONO,
                        }}>
                            Loading enriched data...
                        </div>
                    ) : (
                        <div style={{
                            color: colors.textMuted, fontSize: '12px', textAlign: 'center',
                            padding: SPACE.xl, fontFamily: SANS,
                            border: `1px dashed ${colors.border}`, borderRadius: tokens.radius.md,
                        }}>
                            Add tickers to build your watchlist
                        </div>
                    )}

                    {/* Auto-suggestions */}
                    {suggestions.length > 0 && (
                        <div style={{ marginTop: SPACE.sm, padding: '8px 0' }}>
                            <div style={{
                                fontSize: '9px', color: colors.textMuted, letterSpacing: '1px',
                                marginBottom: '6px', fontFamily: MONO, fontWeight: 600,
                            }}>
                                SUGGESTED
                            </div>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                                {suggestions.map(s => (
                                    <button key={s.ticker} onClick={async (e) => {
                                        e.stopPropagation();
                                        try { await api.addToWatchlist({ ticker: s.ticker }); loadData(); }
                                        catch (err) { addNotification('error', err.message); }
                                    }} style={{
                                        background: 'transparent',
                                        border: `1px dashed ${colors.border}`,
                                        borderRadius: '4px', padding: '4px 10px', fontSize: '10px',
                                        color: colors.textMuted, cursor: 'pointer', fontFamily: MONO,
                                        transition: `all ${tokens.transition.fast}`,
                                    }}
                                        onMouseEnter={(e) => { e.currentTarget.style.borderColor = colors.accent; e.currentTarget.style.color = colors.accent; }}
                                        onMouseLeave={(e) => { e.currentTarget.style.borderColor = colors.border; e.currentTarget.style.color = colors.textMuted; }}
                                    >+ {s.ticker}</button>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            </FadeIn>

            {/* ═══════════════ ASK GRID MODAL ═══════════════ */}
            {askOpen && (
                <div style={{
                    position: 'fixed', inset: 0, zIndex: 1000,
                    background: 'rgba(0,0,0,0.7)',
                    backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)',
                    display: 'flex', alignItems: 'flex-end', justifyContent: 'center',
                }} onClick={() => setAskOpen(false)}>
                    <div style={{
                        width: '100%', maxWidth: '640px',
                        background: `linear-gradient(180deg, ${colors.cardElevated}, ${colors.bg})`,
                        borderTop: `2px solid ${colors.accent}`,
                        borderRadius: '16px 16px 0 0', padding: SPACE.lg,
                        paddingBottom: `calc(${SPACE.lg} + env(safe-area-inset-bottom, 0px))`,
                        boxShadow: '0 -8px 40px rgba(0,0,0,0.5)',
                    }} onClick={e => e.stopPropagation()}>
                        <div style={{
                            fontFamily: MONO, fontSize: '14px', fontWeight: 700,
                            color: colors.text, marginBottom: SPACE.md,
                            display: 'flex', alignItems: 'center', gap: '8px',
                        }}>
                            <span style={{
                                width: '8px', height: '8px', borderRadius: '50%',
                                background: colors.accent,
                                boxShadow: `0 0 10px ${colors.accent}`,
                            }} />
                            Ask GRID
                        </div>
                        <textarea value={askQuery} onChange={e => setAskQuery(e.target.value)}
                            placeholder="e.g., What's the current risk to equities from credit spreads?"
                            style={{
                                ...shared.textarea, minHeight: '80px', marginBottom: '12px',
                                fontFamily: SANS, fontSize: '14px',
                                border: `1px solid ${colors.accent}30`,
                            }} />
                        {askResult && (
                            <div style={{
                                ...shared.prose, marginBottom: '12px', maxHeight: '300px',
                                background: colors.bg,
                                border: `1px solid ${colors.border}`,
                            }}>{askResult}</div>
                        )}
                        <div style={{ display: 'flex', gap: '8px' }}>
                            <button onClick={async () => {
                                if (!askQuery.trim()) return;
                                setAskResult('Thinking...');
                                try { const r = await api.askOllama(askQuery); setAskResult(r?.response || r?.content || 'No response'); }
                                catch (err) { setAskResult('Error: ' + (err.message || 'failed')); }
                            }} style={{
                                ...shared.button, flex: 1,
                                background: `linear-gradient(135deg, ${colors.accent}, #1A5A9F)`,
                                fontFamily: SANS,
                            }}>Ask</button>
                            <button onClick={() => { setAskOpen(false); setAskResult(null); }}
                                style={{ ...shared.buttonSmall, background: colors.card, fontFamily: SANS }}>Close</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Toast */}
            {actionResult && (
                <div style={{
                    position: 'fixed', bottom: '80px', left: SPACE.md, right: SPACE.md,
                    background: colors.gradientCard,
                    border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.md, padding: '12px 16px', zIndex: 500,
                    boxShadow: colors.shadow.lg,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    backdropFilter: 'blur(8px)',
                }}>
                    <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS }}>{actionResult}</span>
                    <button onClick={() => setActionResult(null)} style={{
                        background: 'none', border: 'none', color: colors.textMuted,
                        cursor: 'pointer', fontSize: '16px', padding: '4px',
                    }}>\u00d7</button>
                </div>
            )}

            <div style={{ height: '80px' }} />
            <WidgetManager prefs={widgetPrefs} onPrefsChange={setWidgetPrefs} open={widgetPanelOpen} onClose={() => setWidgetPanelOpen(false)} />

            {/* Keyframe animations */}
            <style>{`
                @keyframes pulse {
                    0%, 100% { opacity: 1; }
                    50% { opacity: 0.4; }
                }
                @keyframes slideDown {
                    from { transform: translateY(-100%); opacity: 0; }
                    to { transform: translateY(0); opacity: 1; }
                }
                @keyframes slideUp {
                    from { transform: translateY(20px); opacity: 0; }
                    to { transform: translateY(0); opacity: 1; }
                }
            `}</style>
        </div>
    );
}
