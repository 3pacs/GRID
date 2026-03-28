import React, { Suspense, lazy, useEffect } from 'react';
import api from './api.js';
import useStore from './store.js';
import NavBar from './components/NavBar.jsx';
import { tokens } from './styles/tokens.js';

const Orrery = lazy(() => import('./views/Orrery.jsx'));
const LunarDashboard = lazy(() => import('./views/LunarDashboard.jsx'));
const Ephemeris = lazy(() => import('./views/Ephemeris.jsx'));
const Correlations = lazy(() => import('./views/Correlations.jsx'));
const Timeline = lazy(() => import('./views/Timeline.jsx'));
const Narrative = lazy(() => import('./views/Narrative.jsx'));
const Settings = lazy(() => import('./views/Settings.jsx'));

const VIEW_IDS = new Set([
    'orrery',
    'lunar',
    'ephemeris',
    'correlations',
    'timeline',
    'narrative',
    'settings',
]);

function getViewFromHash() {
    if (typeof window === 'undefined') {
        return 'orrery';
    }

    const hash = window.location.hash.startsWith('#/')
        ? window.location.hash.slice(2)
        : '';

    return VIEW_IDS.has(hash) ? hash : 'orrery';
}

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
    loadingShell: {
        minHeight: 'calc(100vh - 76px)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        fontSize: '13px',
        letterSpacing: '1px',
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
        setActiveView(view);
        window.location.hash = `#/${view}`;
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
                <Suspense fallback={<div style={appStyles.loadingShell}>Loading AstroGrid...</div>}>
                    {renderView()}
                </Suspense>
            </div>
            <NavBar activeView={activeView} onNavigate={navigate} />
        </div>
    );
}

export default App;
