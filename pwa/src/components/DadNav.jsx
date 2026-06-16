import React from 'react';
import { colors } from '../styles/shared.js';

const SANS = "'IBM Plex Sans', -apple-system, sans-serif";
const MONO = "'IBM Plex Mono', monospace";

// The simple top bar for a non-technical user (dad). Two big, plainly-labelled
// tabs and a sign-out — none of the operator cockpit. Big text, high contrast.
const TABS = [
    { id: 'home', label: 'Ask', emoji: '💬' },
    { id: 'ten-year', label: '10-Year Plan', emoji: '📈' },
];

export default function DadNav({ activeView, onNavigate, onSignOut }) {
    return (
        <div style={S.bar}>
            <button style={S.brand} onClick={() => onNavigate('home')} aria-label="Home">
                <span style={S.brandStep}>stepdad</span><span style={S.brandDot}>.</span><span style={S.brandFin}>finance</span>
            </button>

            <div style={S.tabs}>
                {TABS.map((t) => {
                    const active = activeView === t.id;
                    return (
                        <button
                            key={t.id}
                            onClick={() => onNavigate(t.id)}
                            style={{ ...S.tab, ...(active ? S.tabActive : null) }}
                            aria-current={active ? 'page' : undefined}
                        >
                            <span style={S.tabEmoji}>{t.emoji}</span>{t.label}
                        </button>
                    );
                })}
            </div>

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
        fontFamily: MONO, fontSize: '20px', fontWeight: 800, letterSpacing: '-0.5px',
        flexShrink: 0,
    },
    brandStep: { color: colors.text },
    brandDot: { color: colors.accent },
    brandFin: { color: colors.accent },
    tabs: { display: 'flex', gap: '10px', marginLeft: 'auto', marginRight: 'auto' },
    tab: {
        display: 'flex', alignItems: 'center', gap: '8px',
        fontFamily: SANS, fontSize: '17px', fontWeight: 600, color: colors.textDim,
        background: 'transparent', border: `2px solid transparent`, borderRadius: '12px',
        padding: '10px 18px', minHeight: '48px', cursor: 'pointer', whiteSpace: 'nowrap',
    },
    tabActive: {
        color: '#fff', background: colors.accent, borderColor: colors.accent,
    },
    tabEmoji: { fontSize: '18px' },
    signout: {
        flexShrink: 0, fontFamily: SANS, fontSize: '15px', fontWeight: 600,
        color: colors.textDim, background: 'transparent',
        border: `1px solid ${colors.border}`, borderRadius: '10px',
        padding: '9px 14px', minHeight: '44px', cursor: 'pointer',
    },
};
