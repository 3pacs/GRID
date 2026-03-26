import React from 'react';
import { Activity, BarChart2, Layers, TrendingUp, FileText, Grid3x3, Search } from 'lucide-react';
import { tokens } from '../styles/tokens.js';

const tabs = [
    { key: 'dealer-flow', label: 'Flow', Icon: Activity },
    { key: 'gamma-profile', label: 'Gamma', Icon: BarChart2 },
    { key: 'vol-surface', label: 'Surface', Icon: Layers },
    { key: 'term-structure', label: 'Term', Icon: TrendingUp },
    { key: 'flow-narrative', label: 'Narrative', Icon: FileText },
    { key: 'position-heatmap', label: 'OI Map', Icon: Grid3x3 },
    { key: 'scanner', label: 'Scanner', Icon: Search },
];

const styles = {
    nav: {
        position: 'fixed',
        bottom: 0,
        left: 0,
        right: 0,
        display: 'flex',
        justifyContent: 'space-around',
        alignItems: 'center',
        height: '60px',
        background: tokens.bgSecondary,
        borderTop: `1px solid ${tokens.cardBorder}`,
        paddingBottom: 'env(safe-area-inset-bottom, 0px)',
        zIndex: 1000,
    },
    tab: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        flex: 1,
        height: '100%',
        cursor: 'pointer',
        border: 'none',
        background: 'none',
        gap: '2px',
        padding: 0,
    },
    label: {
        fontSize: '9px',
        fontFamily: tokens.fontMono,
        letterSpacing: '0.5px',
        textTransform: 'uppercase',
    },
};

function NavBar({ activeView, onNavigate }) {
    return (
        <nav style={styles.nav}>
            {tabs.map(({ key, label, Icon }) => {
                const isActive = activeView === key;
                return (
                    <button
                        key={key}
                        style={styles.tab}
                        onClick={() => onNavigate(key)}
                    >
                        <Icon
                            size={18}
                            color={isActive ? tokens.accent : tokens.textMuted}
                            strokeWidth={isActive ? 2.5 : 1.5}
                        />
                        <span style={{
                            ...styles.label,
                            color: isActive ? tokens.accent : tokens.textMuted,
                            fontWeight: isActive ? 600 : 400,
                        }}>
                            {label}
                        </span>
                    </button>
                );
            })}
        </nav>
    );
}

export default NavBar;
