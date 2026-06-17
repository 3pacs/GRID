import React from 'react';
import { MessageSquare, RefreshCw, Search, TrendingUp } from 'lucide-react';
import { colors } from '../styles/shared.js';

const SANS = "'IBM Plex Sans', -apple-system, sans-serif";
const MONO = "'IBM Plex Mono', monospace";

const TABS = [
    { id: 'home', label: 'Ask', Icon: MessageSquare },
    { id: 'ticker-lookup', label: 'Ticker Lookup', Icon: Search },
    { id: 'ten-year', label: '10-Year Plan', Icon: TrendingUp },
];

export default function DadNav({
    activeView,
    onNavigate,
    onSignOut,
    onRefreshVisuals,
    isRefreshingVisuals = false,
    preloadStatus = null,
}) {
    const refreshTitle = isRefreshingVisuals
        ? 'Refreshing visualizations'
        : preloadStatus?.state === 'ready'
            ? `Refresh visualizations. Warm cache ${preloadStatus.completed}/${preloadStatus.total}`
            : preloadStatus?.state === 'warming'
                ? `Warming visualizations ${preloadStatus.completed}/${preloadStatus.total}`
                : 'Refresh visualizations';

    return (
        <div style={S.bar}>
            <button style={S.brand} onClick={() => onNavigate('home')} aria-label="Home">
                <span style={S.brandStep}>stepdad</span><span style={S.brandDot}>.</span><span style={S.brandFin}>finance</span>
            </button>

            <div style={S.tabs}>
                {TABS.map((t) => {
                    const active = activeView === t.id;
                    const Icon = t.Icon;
                    return (
                        <button
                            key={t.id}
                            onClick={() => onNavigate(t.id)}
                            style={{ ...S.tab, ...(active ? S.tabActive : null) }}
                            aria-current={active ? 'page' : undefined}
                        >
                            <Icon size={18} strokeWidth={2.4} />{t.label}
                        </button>
                    );
                })}
            </div>

            <button
                style={{ ...S.iconBtn, ...(isRefreshingVisuals ? S.iconBtnActive : null) }}
                onClick={onRefreshVisuals}
                disabled={isRefreshingVisuals}
                aria-label="Refresh visualizations"
                title={refreshTitle}
            >
                <RefreshCw size={20} strokeWidth={2.4} />
            </button>
            <button style={S.signout} onClick={onSignOut}>Sign out</button>
        </div>
    );
}

const S = {
    bar: {
        position: 'fixed', top: 0, left: 0, right: 0, height: '64px', zIndex: 50,
        display: 'flex', alignItems: 'center', gap: '14px',
        padding: '0 18px', background: colors.card,
        borderBottom: `1px solid ${colors.border}`,
        boxSizing: 'border-box',
    },
    brand: {
        background: 'none', border: 'none', cursor: 'pointer', padding: '8px 6px',
        fontFamily: MONO, fontSize: '20px', fontWeight: 800, letterSpacing: 0,
        flexShrink: 0,
    },
    brandStep: { color: colors.text },
    brandDot: { color: colors.accent },
    brandFin: { color: colors.accent },
    tabs: {
        display: 'flex', gap: '8px', marginLeft: 'auto', marginRight: 'auto',
        overflowX: 'auto', minWidth: 0, maxWidth: '100%',
    },
    tab: {
        display: 'flex', alignItems: 'center', gap: '8px',
        fontFamily: SANS, fontSize: '17px', fontWeight: 600, color: colors.textDim,
        background: 'transparent', border: `2px solid transparent`, borderRadius: '8px',
        padding: '10px 14px', minHeight: '48px', cursor: 'pointer', whiteSpace: 'nowrap',
        flexShrink: 0,
    },
    tabActive: {
        color: '#fff', background: colors.accent, borderColor: colors.accent,
    },
    iconBtn: {
        flexShrink: 0, display: 'grid', placeItems: 'center',
        width: '44px', height: '44px',
        color: colors.textDim, background: 'transparent',
        border: `1px solid ${colors.border}`, borderRadius: '8px',
        cursor: 'pointer',
    },
    iconBtnActive: {
        color: colors.accent,
        opacity: 0.62,
    },
    signout: {
        flexShrink: 0, fontFamily: SANS, fontSize: '15px', fontWeight: 600,
        color: colors.textDim, background: 'transparent',
        border: `1px solid ${colors.border}`, borderRadius: '8px',
        padding: '9px 14px', minHeight: '44px', cursor: 'pointer',
    },
};
