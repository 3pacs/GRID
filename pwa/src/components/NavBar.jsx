import React, { useState } from 'react';
import {
    Home, Radar, BookOpen, FlaskConical, Bot, Settings, FileText,
    Workflow, Atom, Terminal, TrendingUp, BarChart3, Globe, Layers,
    Activity, Menu, X, ChevronRight, History, Cpu, AlertTriangle,
} from 'lucide-react';

const menuSections = [
    {
        label: 'OVERVIEW',
        items: [
            { id: 'dashboard', icon: Home, label: 'Dashboard', desc: 'System overview & status' },
            { id: 'regime', icon: Radar, label: 'Regime', desc: 'Current market regime state' },
            { id: 'signals', icon: Activity, label: 'Signals', desc: 'Live feature values' },
        ],
    },
    {
        label: 'INTELLIGENCE',
        items: [
            { id: 'briefings', icon: FileText, label: 'Briefings', desc: 'AI market analysis reports' },
            { id: 'agents', icon: Bot, label: 'Agents', desc: 'Multi-agent deliberation' },
            { id: 'discovery', icon: FlaskConical, label: 'Discovery', desc: 'Hypotheses & clustering' },
            { id: 'models', icon: Layers, label: 'Models', desc: 'Model registry & governance' },
        ],
    },
    {
        label: 'PERFORMANCE',
        items: [
            { id: 'backtest', icon: TrendingUp, label: 'Backtest', desc: 'Track record & paper trades' },
            { id: 'journal', icon: BookOpen, label: 'Journal', desc: 'Decision log & outcomes' },
            { id: 'physics', icon: Atom, label: 'Physics', desc: 'Market dynamics verification' },
        ],
    },
    {
        label: 'OPERATIONS',
        items: [
            { id: 'workflows', icon: Workflow, label: 'Workflows', desc: 'Data & compute pipelines' },
            { id: 'hyperspace', icon: Globe, label: 'Hyperspace', desc: 'Distributed compute node' },
            { id: 'system', icon: Terminal, label: 'System', desc: 'Logs, config & sources' },
            { id: 'settings', icon: Settings, label: 'Settings', desc: 'Connection & logout' },
        ],
    },
];

const allItems = menuSections.flatMap(s => s.items);

const primaryTabs = [
    { id: 'dashboard', icon: Home, label: 'Home' },
    { id: 'briefings', icon: FileText, label: 'Briefings' },
    { id: 'agents', icon: Bot, label: 'Agents' },
    { id: 'regime', icon: Radar, label: 'Regime' },
    { id: 'menu', icon: Menu, label: 'Menu' },
];

const styles = {
    nav: {
        position: 'fixed', bottom: 0, left: 0, right: 0,
        background: '#0D1520', borderTop: '1px solid #1A2840',
        zIndex: 100,
    },
    primaryRow: {
        display: 'flex', justifyContent: 'space-around',
        paddingTop: '8px',
        paddingBottom: 'calc(8px + env(safe-area-inset-bottom, 0px))',
    },
    tab: {
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        gap: '2px', border: 'none', background: 'none', cursor: 'pointer',
        padding: '4px 12px', minWidth: '44px', minHeight: '44px',
    },
    label: { fontSize: '10px', fontFamily: "'IBM Plex Sans', sans-serif" },
    overlay: {
        position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
        background: 'rgba(0,0,0,0.6)', zIndex: 98,
    },
    drawer: {
        position: 'fixed', top: 0, right: 0, bottom: 0,
        width: '300px', maxWidth: '85vw',
        background: '#0A1018',
        borderLeft: '1px solid #1A2840',
        zIndex: 99, overflowY: 'auto',
        display: 'flex', flexDirection: 'column',
        paddingBottom: 'calc(70px + env(safe-area-inset-bottom, 0px))',
    },
    drawerHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '20px 20px 12px 20px',
        borderBottom: '1px solid #1A2840',
    },
    drawerTitle: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '16px',
        fontWeight: 700, color: '#1A6EBF', letterSpacing: '3px',
    },
    closeBtn: {
        background: 'none', border: 'none', cursor: 'pointer',
        padding: '8px', borderRadius: '8px',
    },
    sectionLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '2px',
        color: '#5A7080', padding: '16px 20px 6px 20px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    menuItem: {
        display: 'flex', alignItems: 'center', gap: '12px',
        padding: '12px 20px', cursor: 'pointer',
        borderLeft: '3px solid transparent',
        transition: 'background 0.15s',
    },
    menuItemActive: {
        background: '#1A6EBF15',
        borderLeftColor: '#1A6EBF',
    },
    menuIcon: {
        width: '32px', height: '32px', borderRadius: '8px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        background: '#0D1520', flexShrink: 0,
    },
    menuLabel: {
        fontSize: '14px', fontWeight: 600,
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    menuDesc: {
        fontSize: '11px', marginTop: '1px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    chevron: {
        marginLeft: 'auto', flexShrink: 0,
    },
};

const isPrimaryView = (view) => ['dashboard', 'briefings', 'agents', 'regime'].includes(view);

export default function NavBar({ activeView, onNavigate }) {
    const [showMenu, setShowMenu] = useState(false);

    const handleNav = (id) => {
        if (id === 'menu') {
            setShowMenu(!showMenu);
            return;
        }
        setShowMenu(false);
        onNavigate(id);
    };

    const isSecondaryView = !isPrimaryView(activeView) && activeView !== 'journal-entry';

    return (
        <>
            {showMenu && (
                <>
                    <div style={styles.overlay} onClick={() => setShowMenu(false)} />
                    <div style={styles.drawer}>
                        <div style={styles.drawerHeader}>
                            <span style={styles.drawerTitle}>GRID</span>
                            <button
                                style={styles.closeBtn}
                                onClick={() => setShowMenu(false)}
                                aria-label="Close menu"
                            >
                                <X size={20} color="#5A7080" />
                            </button>
                        </div>
                        {menuSections.map(section => (
                            <div key={section.label}>
                                <div style={styles.sectionLabel}>{section.label}</div>
                                {section.items.map(item => {
                                    const Icon = item.icon;
                                    const isActive = activeView === item.id;
                                    return (
                                        <div
                                            key={item.id}
                                            onClick={() => handleNav(item.id)}
                                            style={{
                                                ...styles.menuItem,
                                                ...(isActive ? styles.menuItemActive : {}),
                                            }}
                                        >
                                            <div style={{
                                                ...styles.menuIcon,
                                                background: isActive ? '#1A6EBF20' : '#0D1520',
                                            }}>
                                                <Icon size={16} color={isActive ? '#1A6EBF' : '#5A7080'} />
                                            </div>
                                            <div>
                                                <div style={{
                                                    ...styles.menuLabel,
                                                    color: isActive ? '#E8F0F8' : '#C8D8E8',
                                                }}>{item.label}</div>
                                                <div style={{
                                                    ...styles.menuDesc,
                                                    color: isActive ? '#8AA0B8' : '#4A6070',
                                                }}>{item.desc}</div>
                                            </div>
                                            <ChevronRight
                                                size={14}
                                                color={isActive ? '#1A6EBF' : '#2A3A4A'}
                                                style={styles.chevron}
                                            />
                                        </div>
                                    );
                                })}
                            </div>
                        ))}
                    </div>
                </>
            )}
            <nav style={styles.nav}>
                <div style={styles.primaryRow}>
                    {primaryTabs.map(tab => {
                        const Icon = tab.icon;
                        const isMenu = tab.id === 'menu';
                        const isActive = isMenu
                            ? (showMenu || isSecondaryView)
                            : activeView === tab.id;
                        return (
                            <button
                                key={tab.id}
                                onClick={() => handleNav(tab.id)}
                                style={styles.tab}
                                aria-label={tab.label}
                            >
                                <Icon
                                    size={22}
                                    color={isActive ? '#1A6EBF' : '#5A7080'}
                                />
                                <span style={{
                                    ...styles.label,
                                    color: isActive ? '#1A6EBF' : '#5A7080',
                                }}>{tab.label}</span>
                            </button>
                        );
                    })}
                </div>
            </nav>
        </>
    );
}
