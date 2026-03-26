import React from 'react';
import { Orbit, Moon, Star, Grid3X3, CalendarDays, BookOpen } from 'lucide-react';
import { tokens } from '../styles/tokens.js';

const tabs = [
    { id: 'orrery', label: 'Orrery', Icon: Orbit },
    { id: 'lunar', label: 'Moon', Icon: Moon },
    { id: 'ephemeris', label: 'Stars', Icon: Star },
    { id: 'correlations', label: 'Corr', Icon: Grid3X3 },
    { id: 'timeline', label: 'Timeline', Icon: CalendarDays },
    { id: 'narrative', label: 'Narrative', Icon: BookOpen },
];

const navStyles = {
    nav: {
        position: 'fixed',
        bottom: 0,
        left: 0,
        right: 0,
        display: 'flex',
        justifyContent: 'space-around',
        alignItems: 'center',
        height: 'calc(60px + env(safe-area-inset-bottom, 0px))',
        paddingBottom: 'env(safe-area-inset-bottom, 0px)',
        background: 'rgba(5, 8, 16, 0.85)',
        backdropFilter: 'blur(16px)',
        WebkitBackdropFilter: 'blur(16px)',
        borderTop: `1px solid ${tokens.cardBorder}`,
        zIndex: 100,
    },
    tab: (active) => ({
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '3px',
        padding: '6px 0',
        flex: 1,
        cursor: 'pointer',
        border: 'none',
        background: 'transparent',
        color: active ? tokens.accent : tokens.textMuted,
        transition: 'color 0.2s ease',
        WebkitTapHighlightColor: 'transparent',
    }),
    label: {
        fontSize: '10px',
        fontFamily: tokens.fontSans,
        fontWeight: 600,
        letterSpacing: '0.5px',
    },
};

export default function NavBar({ activeView, onNavigate }) {
    return (
        <nav style={navStyles.nav}>
            {tabs.map(({ id, label, Icon }) => (
                <button
                    key={id}
                    style={navStyles.tab(activeView === id)}
                    onClick={() => onNavigate(id)}
                >
                    <Icon size={20} strokeWidth={activeView === id ? 2.2 : 1.5} />
                    <span style={navStyles.label}>{label}</span>
                </button>
            ))}
        </nav>
    );
}
