import React, { useState } from 'react';
import { Home, Radar, BookOpen, FlaskConical, Bot, Settings, FileText, Workflow, Atom, Terminal } from 'lucide-react';

const primaryTabs = [
    { id: 'dashboard', icon: Home, label: 'Home' },
    { id: 'briefings', icon: FileText, label: 'Briefings' },
    { id: 'agents', icon: Bot, label: 'Agents' },
    { id: 'regime', icon: Radar, label: 'Regime' },
    { id: 'more', icon: null, label: 'More' },
];

const moreTabs = [
    { id: 'journal', icon: BookOpen, label: 'Journal' },
    { id: 'workflows', icon: Workflow, label: 'Workflows' },
    { id: 'physics', icon: Atom, label: 'Physics' },
    { id: 'discovery', icon: FlaskConical, label: 'Discovery' },
    { id: 'system', icon: Terminal, label: 'System' },
    { id: 'models', icon: null, label: 'Models' },
    { id: 'signals', icon: null, label: 'Signals' },
    { id: 'hyperspace', icon: null, label: 'Hyperspace' },
    { id: 'settings', icon: Settings, label: 'Settings' },
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
    morePanel: {
        position: 'fixed', bottom: 'calc(60px + env(safe-area-inset-bottom, 0px))',
        left: 0, right: 0, background: '#0D1520',
        borderTop: '1px solid #1A2840', padding: '12px 16px',
        display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '8px',
        zIndex: 99,
    },
    moreTab: {
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        gap: '4px', padding: '12px 8px', borderRadius: '8px',
        background: '#080C10', border: '1px solid #1A2840',
        cursor: 'pointer',
    },
    moreLabel: {
        fontSize: '11px', fontFamily: "'IBM Plex Sans', sans-serif",
    },
    overlay: {
        position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
        background: 'rgba(0,0,0,0.5)', zIndex: 98,
    },
    dots: {
        display: 'flex', gap: '3px', alignItems: 'center', justifyContent: 'center',
        height: '22px',
    },
    dot: { width: '4px', height: '4px', borderRadius: '50%' },
};

const isMoreView = (view) => moreTabs.some(t => t.id === view);

export default function NavBar({ activeView, onNavigate }) {
    const [showMore, setShowMore] = useState(false);

    const handleNav = (id) => {
        if (id === 'more') {
            setShowMore(!showMore);
            return;
        }
        setShowMore(false);
        onNavigate(id);
    };

    return (
        <>
            {showMore && (
                <>
                    <div style={styles.overlay} onClick={() => setShowMore(false)} />
                    <div style={styles.morePanel}>
                        {moreTabs.map(tab => {
                            const Icon = tab.icon;
                            const isActive = activeView === tab.id;
                            return (
                                <div
                                    key={tab.id}
                                    onClick={() => handleNav(tab.id)}
                                    style={{
                                        ...styles.moreTab,
                                        borderColor: isActive ? '#1A6EBF' : '#1A2840',
                                    }}
                                >
                                    {Icon && <Icon size={18} color={isActive ? '#1A6EBF' : '#5A7080'} />}
                                    <span style={{
                                        ...styles.moreLabel,
                                        color: isActive ? '#1A6EBF' : '#5A7080',
                                    }}>{tab.label}</span>
                                </div>
                            );
                        })}
                    </div>
                </>
            )}
            <nav style={styles.nav}>
                <div style={styles.primaryRow}>
                    {primaryTabs.map(tab => {
                        const Icon = tab.icon;
                        const isMore = tab.id === 'more';
                        const isActive = isMore
                            ? (showMore || isMoreView(activeView))
                            : activeView === tab.id;
                        return (
                            <button
                                key={tab.id}
                                onClick={() => handleNav(tab.id)}
                                style={styles.tab}
                                aria-label={tab.label}
                            >
                                {isMore ? (
                                    <div style={styles.dots}>
                                        <div style={{ ...styles.dot, background: isActive ? '#1A6EBF' : '#5A7080' }} />
                                        <div style={{ ...styles.dot, background: isActive ? '#1A6EBF' : '#5A7080' }} />
                                        <div style={{ ...styles.dot, background: isActive ? '#1A6EBF' : '#5A7080' }} />
                                    </div>
                                ) : (
                                    <Icon size={22} color={isActive ? '#1A6EBF' : '#5A7080'} />
                                )}
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
