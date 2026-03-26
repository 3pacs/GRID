import React, { useEffect } from 'react';
import useStore from './store.js';
import NavBar from './components/NavBar.jsx';
import Orrery from './views/Orrery.jsx';
import LunarDashboard from './views/LunarDashboard.jsx';
import Ephemeris from './views/Ephemeris.jsx';
import Correlations from './views/Correlations.jsx';
import Timeline from './views/Timeline.jsx';
import Narrative from './views/Narrative.jsx';
import Settings from './views/Settings.jsx';
import { tokens } from './styles/tokens.js';

const appStyles = {
    app: {
        background: tokens.bgGradient,
        minHeight: '100vh',
        color: tokens.text,
        fontFamily: tokens.fontSans,
        display: 'flex',
        flexDirection: 'column',
        paddingBottom: 'calc(60px + env(safe-area-inset-bottom, 0px))',
    },
    content: {
        flex: 1,
        overflowY: 'auto',
        WebkitOverflowScrolling: 'touch',
    },
    masthead: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: tokens.spacing.md,
        padding: `${tokens.spacing.lg} ${tokens.spacing.lg} ${tokens.spacing.md}`,
        position: 'sticky',
        top: 0,
        zIndex: 10,
        background: 'linear-gradient(180deg, rgba(5, 8, 16, 0.96) 0%, rgba(5, 8, 16, 0.74) 100%)',
        backdropFilter: 'blur(18px)',
        borderBottom: `1px solid ${tokens.cardBorder}`,
    },
    brandBlock: {
        display: 'flex',
        flexDirection: 'column',
        gap: '4px',
    },
    brandTitle: {
        fontFamily: tokens.fontMono,
        fontSize: '22px',
        fontWeight: 700,
        color: tokens.accent,
        letterSpacing: '4px',
    },
    brandSubtitle: {
        fontSize: '11px',
        color: tokens.textMuted,
        letterSpacing: '1.5px',
        textTransform: 'uppercase',
        fontFamily: tokens.fontMono,
    },
    statusPill: (status) => ({
        borderRadius: tokens.radius.pill,
        border: `1px solid ${tokens.cardBorder}`,
        padding: '10px 14px',
        fontFamily: tokens.fontMono,
        fontSize: '11px',
        textTransform: 'uppercase',
        letterSpacing: '1px',
        color: status === 'connected' ? tokens.green : status === 'demo' ? tokens.gold : tokens.text,
        background: 'rgba(15, 25, 45, 0.72)',
    },
};

function App() {
    const { activeView, setActiveView, apiMode, connectionStatus, connectionMessage } = useStore();

    useEffect(() => {
        const syncHash = () => {
            const hash = window.location.hash.slice(2) || 'orrery';
            setActiveView(hash);
        };

        syncHash();
        window.addEventListener('hashchange', syncHash);
        return () => window.removeEventListener('hashchange', syncHash);
    }, [setActiveView]);

    const navigate = (view) => {
        window.location.hash = `#/${view}`;
        setActiveView(view);
    };

    const renderView = () => {
        switch (activeView) {
            case 'orrery': return <Orrery />;
            case 'lunar': return <LunarDashboard />;
            case 'ephemeris': return <Ephemeris />;
            case 'correlations': return <Correlations />;
            case 'timeline': return <Timeline />;
            case 'narrative': return <Narrative />;
            case 'settings': return <Settings />;
            default: return <Orrery />;
        }
    };

    return (
        <div style={appStyles.app}>
            <div style={appStyles.masthead}>
                <div style={appStyles.brandBlock}>
                    <div style={appStyles.brandTitle}>ASTROGRID</div>
                    <div style={appStyles.brandSubtitle}>Standalone celestial intelligence console</div>
                </div>
                <div
                    style={appStyles.statusPill(connectionStatus)}
                    title={connectionMessage}
                >
                    {apiMode === 'live' ? `Live: ${connectionStatus}` : 'Demo mode'}
                </div>
            </div>
            <div style={appStyles.content}>
                {renderView()}
            </div>
            <NavBar activeView={activeView} onNavigate={navigate} />
        </div>
    );
}

export default App;
