import React, { useState, useEffect } from 'react';
import {
    Home, GitBranch, Network, Shield, Globe2, AlertTriangle, Zap,
    Menu, X, ChevronRight, Radar, Activity, BarChart3, Layers,
    FlaskConical, Target, TrendingUp, Atom, Settings, Terminal,
    FileText, Bot, Workflow, CircleDollarSign, Eye, BookOpen,
    Crosshair, Search, Grid3X3,
    PieChart,
    HeartPulse,
    LineChart,
} from 'lucide-react';

/* ─────────────── World View Primary Tabs ─────────────── */

const worldViewTabs = [
    { id: 'dashboard',       icon: Home,            label: 'HOME' },
    { id: 'money-flow',      icon: GitBranch,       label: 'FLOW' },
    { id: 'actor-network',   icon: Network,         label: 'POWER' },
    { id: 'cross-reference', icon: Shield,          label: 'TRUTH' },
    { id: 'globe',           icon: Globe2,          label: 'GLOBE' },
    { id: 'risk',            icon: AlertTriangle,   label: 'RISK' },
    { id: 'intelligence',    icon: Zap,             label: 'SIGNAL' },
];

const worldViewIds = new Set(worldViewTabs.map(t => t.id));

/* ─────────────── Drawer Menu Sections ─────────────── */

const drawerSections = [
    {
        label: 'MARKETS',
        items: [
            { id: 'regime',    icon: Radar,          label: 'Regime',    desc: 'Current market regime state' },
            { id: 'signals',   icon: Activity,       label: 'Signals',   desc: 'Live feature values' },
            { id: 'heatmap',   icon: BarChart3,      label: 'Heatmap',   desc: 'Sector & asset heatmap' },
            { id: 'options',   icon: Layers,         label: 'Options',   desc: 'Options flow & Greeks' },
            { id: 'flows',     icon: CircleDollarSign, label: 'Flows',  desc: 'Sector flows & influence' },
            { id: 'trends',    icon: TrendingUp,     label: 'Trends',    desc: 'Momentum & rotation trends' },
        ],
    },
    {
        label: 'RESEARCH',
        items: [
            { id: 'thesis',        icon: Eye,          label: 'Thesis',     desc: 'Unified market thesis & model views' },
            { id: 'correlation-matrix', icon: Grid3X3, label: 'Correlations', desc: 'Cross-asset correlation matrix & regimes' },
            { id: 'discovery',     icon: FlaskConical, label: 'Discovery',   desc: 'Hypotheses & clustering' },
            { id: 'associations',  icon: Network,      label: 'Associations', desc: 'Feature correlations & anomalies' },
            { id: 'backtest',      icon: TrendingUp,   label: 'Backtest',    desc: 'Track record & paper trades' },
            { id: 'physics',       icon: Atom,         label: 'Physics',     desc: 'Market dynamics verification' },
            { id: 'models',        icon: Layers,       label: 'Models',      desc: 'Model registry & governance' },
        ],
    },
    {
        label: 'TRADING',
        items: [
            { id: 'predictions', icon: Target,    label: 'Predictions', desc: 'Oracle predictions & track record' },
            { id: 'portfolio',   icon: PieChart,  label: 'Portfolio',   desc: 'Position analytics & allocation' },
            { id: 'strategy',    icon: Crosshair, label: 'Strategy',    desc: 'Regime-linked action plans' },
            { id: 'strategies',  icon: LineChart,  label: 'Paper Trading', desc: 'Backtest winners & live paper strategies' },
            { id: 'journal',     icon: BookOpen,  label: 'Journal',     desc: 'Decision log & outcomes' },
        ],
    },
    {
        label: 'OPERATIONS',
        items: [
            { id: 'settings',   icon: Settings, label: 'Settings',   desc: 'Connection & logout' },
            { id: 'pipeline-health', icon: HeartPulse, label: 'Pipeline', desc: 'Data pipeline health & freshness' },
            { id: 'system',     icon: Terminal,  label: 'System',     desc: 'Logs, config & sources' },
            { id: 'agents',     icon: Bot,       label: 'Agents',     desc: 'Multi-agent deliberation' },
            { id: 'briefings',  icon: FileText,  label: 'Briefings',  desc: 'AI market analysis reports' },
            { id: 'workflows',  icon: Workflow,  label: 'Workflows',  desc: 'Data & compute pipelines' },
            { id: 'weights',    icon: Settings,  label: 'Weights',    desc: 'Tune regime feature influence' },
            { id: 'hyperspace', icon: Globe2,    label: 'Hyperspace', desc: 'Distributed compute node' },
        ],
    },
];

/* ─────────────── Responsive Helpers ─────────────── */

const ACCENT = '#1A6EBF';
const BG_NAV = '#0D1520';
const BG_DRAWER = '#0A1018';
const BORDER = '#1A2840';
const TEXT_DIM = '#5A7080';
const TEXT_ACTIVE = '#E8F0F8';
const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";
const DESKTOP_BP = 1024;

function useIsDesktop() {
    const [isDesktop, setIsDesktop] = useState(
        typeof window !== 'undefined' ? window.innerWidth >= DESKTOP_BP : false
    );
    useEffect(() => {
        const onResize = () => setIsDesktop(window.innerWidth >= DESKTOP_BP);
        window.addEventListener('resize', onResize);
        return () => window.removeEventListener('resize', onResize);
    }, []);
    return isDesktop;
}

/* ─────────────── Styles ─────────────── */

const s = {
    /* ── Mobile: bottom bar ── */
    mobileNav: {
        position: 'fixed', bottom: 0, left: 0, right: 0,
        background: BG_NAV, borderTop: `1px solid ${BORDER}`,
        zIndex: 100, display: 'flex', flexDirection: 'column',
    },
    mobileTabRow: {
        display: 'flex', justifyContent: 'space-around',
        paddingTop: '4px',
        paddingBottom: 'calc(4px + env(safe-area-inset-bottom, 0px))',
    },
    mobileTab: {
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        gap: '1px', border: 'none', background: 'none', cursor: 'pointer',
        padding: '4px 2px', minWidth: '36px', minHeight: '44px', flex: 1,
    },
    mobileTabLabel: {
        fontSize: '8px', fontWeight: 600, letterSpacing: '0.5px',
        fontFamily: MONO,
    },

    /* ── Desktop: top bar ── */
    desktopNav: {
        position: 'fixed', top: 0, left: 0, right: 0,
        background: BG_NAV, borderBottom: `1px solid ${BORDER}`,
        zIndex: 100, display: 'flex', alignItems: 'center',
        height: '48px', padding: '0 16px',
    },
    desktopBrand: {
        fontFamily: MONO, fontSize: '14px', fontWeight: 700,
        color: ACCENT, letterSpacing: '3px', marginRight: '24px',
        flexShrink: 0,
    },
    desktopTabRow: {
        display: 'flex', gap: '2px', alignItems: 'center',
        flex: 1, overflow: 'hidden',
    },
    desktopTab: {
        display: 'flex', alignItems: 'center', gap: '6px',
        border: 'none', background: 'none', cursor: 'pointer',
        padding: '8px 14px', borderRadius: '6px',
        transition: 'background 0.15s',
        height: '48px', boxSizing: 'border-box',
        borderBottom: '2px solid transparent',
    },
    desktopTabLabel: {
        fontSize: '11px', fontWeight: 600, letterSpacing: '1px',
        fontFamily: MONO,
    },
    desktopMore: {
        display: 'flex', alignItems: 'center', gap: '6px',
        border: 'none', background: 'none', cursor: 'pointer',
        padding: '8px 14px', borderRadius: '6px', marginLeft: 'auto',
        height: '48px', boxSizing: 'border-box', flexShrink: 0,
    },

    /* ── Drawer (shared) ── */
    overlay: {
        position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
        background: 'rgba(0,0,0,0.6)', zIndex: 98,
    },
    drawer: {
        position: 'fixed', right: 0, top: 0, bottom: 0,
        width: '320px', maxWidth: '85vw',
        background: BG_DRAWER,
        borderLeft: `1px solid ${BORDER}`,
        zIndex: 99, overflowY: 'auto',
        WebkitOverflowScrolling: 'touch',
        display: 'flex', flexDirection: 'column',
        boxShadow: '-4px 0 24px rgba(0,0,0,0.5)',
        transition: 'transform 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
    },
    drawerMobile: {
        position: 'fixed', left: 0, right: 0, bottom: 0,
        maxHeight: '70vh',
        background: BG_DRAWER,
        borderTop: `1px solid ${BORDER}`,
        borderRadius: '16px 16px 0 0',
        zIndex: 99, overflowY: 'auto',
        WebkitOverflowScrolling: 'touch',
        display: 'flex', flexDirection: 'column',
        paddingBottom: 'calc(70px + env(safe-area-inset-bottom, 0px))',
        boxShadow: '0 -4px 24px rgba(0,0,0,0.5)',
    },
    drawerHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '20px 20px 12px 20px',
        borderBottom: `1px solid ${BORDER}`,
    },
    drawerTitle: {
        fontFamily: MONO, fontSize: '14px', fontWeight: 700,
        color: ACCENT, letterSpacing: '3px',
    },
    closeBtn: {
        background: 'none', border: 'none', cursor: 'pointer',
        padding: '8px', borderRadius: '8px',
    },
    sectionLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '2px',
        color: TEXT_DIM, padding: '16px 20px 6px 20px',
        fontFamily: MONO,
    },
    menuItem: {
        display: 'flex', alignItems: 'center', gap: '12px',
        padding: '12px 20px', cursor: 'pointer',
        borderLeft: '3px solid transparent',
        transition: 'background 0.15s',
        minHeight: '44px',
    },
    menuItemActive: {
        background: `${ACCENT}15`,
        borderLeftColor: ACCENT,
    },
    menuIcon: {
        width: '32px', height: '32px', borderRadius: '8px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        background: BG_NAV, flexShrink: 0,
    },
    menuLabel: {
        fontSize: '14px', fontWeight: 600, fontFamily: SANS,
    },
    menuDesc: {
        fontSize: '11px', marginTop: '1px', fontFamily: SANS,
    },
    chevron: {
        marginLeft: 'auto', flexShrink: 0,
    },
};

/* ─────────────── Component ─────────────── */

export default function NavBar({ activeView, onNavigate, onSearchOpen }) {
    const [showDrawer, setShowDrawer] = useState(false);
    const isDesktop = useIsDesktop();

    const handleTabNav = (id) => {
        setShowDrawer(false);
        onNavigate(id);
    };

    const handleDrawerNav = (id) => {
        setShowDrawer(false);
        onNavigate(id);
    };

    const toggleDrawer = () => setShowDrawer(prev => !prev);

    // Determine if "More" button should look active (current view is in the drawer)
    const isDrawerViewActive = !worldViewIds.has(activeView)
        && activeView !== 'journal-entry'
        && activeView !== 'watchlist-analysis';

    /* ── Drawer content (shared between mobile/desktop) ── */
    const drawerContent = (
        <>
            <div style={s.drawerHeader}>
                <span style={s.drawerTitle}>GRID VIEWS</span>
                <button
                    style={s.closeBtn}
                    onClick={() => setShowDrawer(false)}
                    aria-label="Close menu"
                >
                    <X size={20} color={TEXT_DIM} />
                </button>
            </div>
            {drawerSections.map(section => (
                <div key={section.label}>
                    <div style={s.sectionLabel}>{section.label}</div>
                    {section.items.map(item => {
                        const Icon = item.icon;
                        const isActive = activeView === item.id;
                        return (
                            <div
                                key={item.id}
                                onClick={() => handleDrawerNav(item.id)}
                                style={{
                                    ...s.menuItem,
                                    ...(isActive ? s.menuItemActive : {}),
                                }}
                            >
                                <div style={{
                                    ...s.menuIcon,
                                    background: isActive ? `${ACCENT}20` : BG_NAV,
                                }}>
                                    <Icon size={16} color={isActive ? ACCENT : TEXT_DIM} />
                                </div>
                                <div>
                                    <div style={{
                                        ...s.menuLabel,
                                        color: isActive ? TEXT_ACTIVE : '#C8D8E8',
                                    }}>{item.label}</div>
                                    <div style={{
                                        ...s.menuDesc,
                                        color: isActive ? '#8AA0B8' : '#4A6070',
                                    }}>{item.desc}</div>
                                </div>
                                <ChevronRight
                                    size={14}
                                    color={isActive ? ACCENT : '#2A3A4A'}
                                    style={s.chevron}
                                />
                            </div>
                        );
                    })}
                </div>
            ))}
        </>
    );

    /* ── Desktop Layout ── */
    if (isDesktop) {
        return (
            <>
                {showDrawer && (
                    <>
                        <div style={s.overlay} onClick={() => setShowDrawer(false)} />
                        <div style={s.drawer}>{drawerContent}</div>
                    </>
                )}
                <nav style={s.desktopNav}>
                    <span style={s.desktopBrand}>GRID</span>
                    <div style={s.desktopTabRow}>
                        {worldViewTabs.map(tab => {
                            const Icon = tab.icon;
                            const isActive = activeView === tab.id;
                            return (
                                <button
                                    key={tab.id}
                                    onClick={() => handleTabNav(tab.id)}
                                    style={{
                                        ...s.desktopTab,
                                        borderBottomColor: isActive ? ACCENT : 'transparent',
                                        background: isActive ? `${ACCENT}10` : 'none',
                                    }}
                                    aria-label={tab.label}
                                >
                                    <Icon size={16} color={isActive ? ACCENT : TEXT_DIM} />
                                    <span style={{
                                        ...s.desktopTabLabel,
                                        color: isActive ? ACCENT : TEXT_DIM,
                                    }}>{tab.label}</span>
                                </button>
                            );
                        })}
                    </div>
                    <button
                        onClick={() => onSearchOpen?.()}
                        style={{
                            ...s.desktopMore,
                            marginLeft: 'auto',
                            marginRight: '0',
                        }}
                        aria-label="Search (Cmd+K)"
                        title="Search (Cmd+K)"
                    >
                        <Search size={16} color={TEXT_DIM} />
                        <span style={{
                            fontSize: '11px', color: '#5A7080',
                            fontFamily: "'JetBrains Mono', monospace",
                            background: '#111B2A', padding: '1px 6px',
                            borderRadius: '4px', border: '1px solid #1A2840',
                        }}>
                            {typeof navigator !== 'undefined' && /Mac/.test(navigator.platform) ? '\u2318K' : 'Ctrl+K'}
                        </span>
                    </button>
                    <button
                        onClick={toggleDrawer}
                        style={{
                            ...s.desktopMore,
                            marginLeft: '0',
                            background: (showDrawer || isDrawerViewActive) ? `${ACCENT}10` : 'none',
                            borderBottom: `2px solid ${(showDrawer || isDrawerViewActive) ? ACCENT : 'transparent'}`,
                        }}
                        aria-label="More views"
                    >
                        <Menu size={16} color={(showDrawer || isDrawerViewActive) ? ACCENT : TEXT_DIM} />
                        <span style={{
                            ...s.desktopTabLabel,
                            color: (showDrawer || isDrawerViewActive) ? ACCENT : TEXT_DIM,
                        }}>MORE</span>
                    </button>
                </nav>
            </>
        );
    }

    /* ── Mobile Layout ── */

    // On mobile, show 7 tabs + More = 8 items in bottom bar.
    // That's a lot for mobile, so we combine into: first 4 tabs + "More" on very small screens,
    // or all 7 + More on wider phones. We'll show all 7 + More and let them be compact.
    const mobileTabs = [...worldViewTabs];

    return (
        <>
            {showDrawer && (
                <>
                    <div style={s.overlay} onClick={() => setShowDrawer(false)} />
                    <div style={s.drawerMobile}>
                        {/* Bottom sheet drag handle */}
                        <div style={{ display: 'flex', justifyContent: 'center', padding: '10px 0 4px 0' }}>
                            <div style={{ width: '36px', height: '4px', borderRadius: '2px', background: '#2A3A4A' }} />
                        </div>
                        {drawerContent}
                    </div>
                </>
            )}
            <nav style={s.mobileNav}>
                <div style={s.mobileTabRow}>
                    {mobileTabs.map(tab => {
                        const Icon = tab.icon;
                        const isActive = activeView === tab.id;
                        return (
                            <button
                                key={tab.id}
                                onClick={() => handleTabNav(tab.id)}
                                style={{
                                    ...s.mobileTab,
                                    borderTop: isActive ? `2px solid ${ACCENT}` : '2px solid transparent',
                                }}
                                aria-label={tab.label}
                            >
                                <Icon size={20} color={isActive ? ACCENT : TEXT_DIM} />
                                <span style={{
                                    ...s.mobileTabLabel,
                                    color: isActive ? ACCENT : TEXT_DIM,
                                }}>{tab.label}</span>
                            </button>
                        );
                    })}
                    {/* More / drawer toggle */}
                    <button
                        onClick={toggleDrawer}
                        style={{
                            ...s.mobileTab,
                            borderTop: (showDrawer || isDrawerViewActive)
                                ? `2px solid ${ACCENT}` : '2px solid transparent',
                        }}
                        aria-label="More views"
                    >
                        <Menu size={20} color={(showDrawer || isDrawerViewActive) ? ACCENT : TEXT_DIM} />
                        <span style={{
                            ...s.mobileTabLabel,
                            color: (showDrawer || isDrawerViewActive) ? ACCENT : TEXT_DIM,
                        }}>MORE</span>
                    </button>
                </div>
            </nav>
        </>
    );
}
