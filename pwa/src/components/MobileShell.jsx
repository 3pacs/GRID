/**
 * MobileShell - iOS-optimized app shell for GRID PWA.
 * Provides compact header, segmented navigation, sub-tabs,
 * pull-to-refresh, and haptic feedback.
 */
import React, { useState, useRef, useCallback } from 'react';
import { colors } from '../styles/shared.js';

/* ---------- sub-tab definitions per main section ---------- */
const SUB_TABS = {
    dashboard:  ['Overview', 'Metrics', 'Sources', 'Freshness'],
    regime:     ['State', 'Drivers', 'Transitions', 'History', 'Confidence'],
    signals:    ['All', 'Rates', 'Equity', 'Vol', 'Credit', 'Macro', 'Alt', 'Crypto'],
    journal:    ['Recent', 'Stats', 'Outcomes', 'Contradictions'],
    discovery:  ['Clusters', 'Orthogonality', 'Hypotheses', 'Transition Leaders'],
    operator:   ['Health', 'Issues', 'Cycles', 'Autoresearch', 'Git Sync'],
    snapshots:  ['Browse', 'Compare', 'Trends', 'Export'],
    options:    ['Scanner', '100x', 'Signals', 'History'],
};

const MAIN_SECTIONS = Object.keys(SUB_TABS);

/* ---------- haptic helper ---------- */
function haptic(style = 'light') {
    try {
        if (navigator.vibrate) {
            const ms = style === 'medium' ? 15 : style === 'heavy' ? 25 : 8;
            navigator.vibrate(ms);
        }
    } catch (_) {
        /* vibration not available */
    }
}

/* ---------- styles ---------- */
const s = {
    shell: {
        display: 'flex',
        flexDirection: 'column',
        height: '100vh',
        background: colors.bg,
        color: colors.text,
        fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
        fontSize: '12px',
        overflow: 'hidden',
    },
    header: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '6px 10px',
        paddingTop: 'calc(env(safe-area-inset-top, 0px) + 6px)',
        background: colors.card,
        borderBottom: `1px solid ${colors.border}`,
        flexShrink: 0,
    },
    logo: {
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: '14px',
        fontWeight: 700,
        color: colors.accent,
        letterSpacing: '2px',
    },
    statusRow: {
        display: 'flex',
        gap: '6px',
        alignItems: 'center',
    },
    dot: (color) => ({
        width: 6,
        height: 6,
        borderRadius: '50%',
        background: color,
    }),
    segmentContainer: {
        display: 'flex',
        overflowX: 'auto',
        WebkitOverflowScrolling: 'touch',
        padding: '6px 10px',
        gap: '4px',
        background: colors.card,
        borderBottom: `1px solid ${colors.border}`,
        flexShrink: 0,
    },
    segment: (active) => ({
        padding: '6px 12px',
        borderRadius: '14px',
        fontSize: '11px',
        fontWeight: 600,
        cursor: 'pointer',
        border: 'none',
        fontFamily: "'IBM Plex Sans', sans-serif",
        background: active ? colors.accent : 'transparent',
        color: active ? '#fff' : colors.textMuted,
        whiteSpace: 'nowrap',
        minHeight: '28px',
        flexShrink: 0,
        transition: 'background 0.15s, color 0.15s',
    }),
    subTabContainer: {
        display: 'flex',
        overflowX: 'auto',
        WebkitOverflowScrolling: 'touch',
        padding: '4px 10px',
        gap: '2px',
        background: colors.bg,
        borderBottom: `1px solid ${colors.border}`,
        flexShrink: 0,
    },
    subTab: (active) => ({
        padding: '4px 10px',
        borderRadius: '10px',
        fontSize: '10px',
        fontWeight: active ? 600 : 500,
        cursor: 'pointer',
        border: 'none',
        fontFamily: "'IBM Plex Sans', sans-serif",
        background: active ? `${colors.accent}33` : 'transparent',
        color: active ? colors.accent : colors.textMuted,
        whiteSpace: 'nowrap',
        minHeight: '24px',
        flexShrink: 0,
        transition: 'background 0.15s, color 0.15s',
    }),
    content: {
        flex: 1,
        overflowY: 'auto',
        WebkitOverflowScrolling: 'touch',
        position: 'relative',
    },
    pullIndicator: (visible) => ({
        textAlign: 'center',
        padding: visible ? '8px 0' : '0',
        height: visible ? '28px' : '0',
        overflow: 'hidden',
        fontSize: '10px',
        color: colors.textMuted,
        transition: 'height 0.2s, padding 0.2s',
    }),
};

export default function MobileShell({
    activeSection,
    onSectionChange,
    statusDots,
    children,
    onRefresh,
}) {
    const [activeSubTab, setActiveSubTab] = useState({});
    const [pulling, setPulling] = useState(false);
    const [pullDistance, setPullDistance] = useState(0);
    const contentRef = useRef(null);
    const touchStartY = useRef(0);
    const isPulling = useRef(false);

    const currentSection = activeSection || 'dashboard';
    const subTabs = SUB_TABS[currentSection] || [];
    const currentSubTab = activeSubTab[currentSection] || subTabs[0] || '';

    /* ---------- section change ---------- */
    const handleSectionChange = useCallback((section) => {
        haptic('light');
        if (onSectionChange) onSectionChange(section);
    }, [onSectionChange]);

    /* ---------- sub-tab change ---------- */
    const handleSubTabChange = useCallback((tab) => {
        haptic('light');
        setActiveSubTab((prev) => ({ ...prev, [currentSection]: tab }));
    }, [currentSection]);

    /* ---------- pull-to-refresh ---------- */
    const onTouchStart = useCallback((e) => {
        if (contentRef.current && contentRef.current.scrollTop === 0) {
            touchStartY.current = e.touches[0].clientY;
            isPulling.current = true;
        }
    }, []);

    const onTouchMove = useCallback((e) => {
        if (!isPulling.current) return;
        const dy = e.touches[0].clientY - touchStartY.current;
        if (dy > 0 && dy < 120) {
            setPullDistance(dy);
            setPulling(true);
        }
    }, []);

    const onTouchEnd = useCallback(() => {
        if (pulling && pullDistance > 60 && onRefresh) {
            haptic('medium');
            onRefresh();
        }
        setPulling(false);
        setPullDistance(0);
        isPulling.current = false;
    }, [pulling, pullDistance, onRefresh]);

    return (
        <div style={s.shell}>
            {/* Compact header */}
            <div style={s.header}>
                <span style={s.logo}>GRID</span>
                <div style={s.statusRow}>
                    {(statusDots || []).map((d) => (
                        <div key={d.label} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                            <div style={s.dot(d.online ? colors.green : colors.red)} />
                            <span style={{ fontSize: '9px', color: colors.textMuted }}>{d.label}</span>
                        </div>
                    ))}
                </div>
            </div>

            {/* Segmented navigation */}
            <div style={s.segmentContainer}>
                {MAIN_SECTIONS.map((sec) => (
                    <button
                        key={sec}
                        style={s.segment(currentSection === sec)}
                        onClick={() => handleSectionChange(sec)}
                    >
                        {sec.charAt(0).toUpperCase() + sec.slice(1)}
                    </button>
                ))}
            </div>

            {/* Sub-tabs */}
            {subTabs.length > 0 && (
                <div style={s.subTabContainer}>
                    {subTabs.map((tab) => (
                        <button
                            key={tab}
                            style={s.subTab(currentSubTab === tab)}
                            onClick={() => handleSubTabChange(tab)}
                        >
                            {tab}
                        </button>
                    ))}
                </div>
            )}

            {/* Pull-to-refresh indicator */}
            <div style={s.pullIndicator(pulling && pullDistance > 20)}>
                {pullDistance > 60 ? 'Release to refresh' : 'Pull to refresh'}
            </div>

            {/* Content area */}
            <div
                ref={contentRef}
                style={s.content}
                onTouchStart={onTouchStart}
                onTouchMove={onTouchMove}
                onTouchEnd={onTouchEnd}
            >
                {typeof children === 'function'
                    ? children({ subTab: currentSubTab })
                    : children}
            </div>
        </div>
    );
}

export { SUB_TABS, MAIN_SECTIONS, haptic };
