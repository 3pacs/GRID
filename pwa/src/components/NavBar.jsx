import React from 'react';
import { Home, Radar, BookOpen, FlaskConical, Settings } from 'lucide-react';

const tabs = [
    { id: 'dashboard', icon: Home, label: 'Home' },
    { id: 'regime', icon: Radar, label: 'Regime' },
    { id: 'journal', icon: BookOpen, label: 'Journal' },
    { id: 'discovery', icon: FlaskConical, label: 'Discovery' },
    { id: 'settings', icon: Settings, label: 'Settings' },
];

const styles = {
    nav: {
        position: 'fixed',
        bottom: 0,
        left: 0,
        right: 0,
        background: '#0D1520',
        borderTop: '1px solid #1A2840',
        display: 'flex',
        justifyContent: 'space-around',
        paddingTop: '8px',
        paddingBottom: 'calc(8px + env(safe-area-inset-bottom, 0px))',
        zIndex: 100,
    },
    tab: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: '2px',
        border: 'none',
        background: 'none',
        cursor: 'pointer',
        padding: '4px 12px',
        minWidth: '44px',
        minHeight: '44px',
    },
    label: {
        fontSize: '10px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
};

export default function NavBar({ activeView, onNavigate }) {
    return (
        <nav style={styles.nav}>
            {tabs.map(tab => {
                const Icon = tab.icon;
                const isActive = activeView === tab.id;
                return (
                    <button
                        key={tab.id}
                        onClick={() => onNavigate(tab.id)}
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
                        }}>
                            {tab.label}
                        </span>
                    </button>
                );
            })}
        </nav>
    );
}
