import React, { useState, useEffect, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import useStore from './store.js';
import { api } from './api.js';
import NavBar from './components/NavBar.jsx';
import ErrorBoundary from './components/ErrorBoundary.jsx';
import Login from './views/Login.jsx';
import ChatPanel from './components/ChatPanel.jsx';
import CommandPalette from './components/CommandPalette.jsx';
import Onboarding from './components/Onboarding.jsx';

// Lazy view components — keyed by route id.
// Static import strings are required for Rollup/Vite tree-shaking.
const routeComponents = {
    home:               React.lazy(() => import('./views/Home.jsx')),
    dashboard:          React.lazy(() => import('./views/Dashboard.jsx')),
    regime:             React.lazy(() => import('./views/Regime.jsx')),
    strategy:           React.lazy(() => import('./views/Strategy.jsx')),
    strategies:         React.lazy(() => import('./views/Strategies.jsx')),
    signals:            React.lazy(() => import('./views/Signals.jsx')),
    journal:            React.lazy(() => import('./views/Journal.jsx')),
    models:             React.lazy(() => import('./views/Models.jsx')),
    discovery:          React.lazy(() => import('./views/Discovery.jsx')),
    associations:       React.lazy(() => import('./views/Associations.jsx')),
    agents:             React.lazy(() => import('./views/Agents.jsx')),
    briefings:          React.lazy(() => import('./views/Briefings.jsx')),
    workflows:          React.lazy(() => import('./views/Workflows.jsx')),
    physics:            React.lazy(() => import('./views/Physics.jsx')),
    system:             React.lazy(() => import('./views/SystemLogs.jsx')),
    'pipeline-health':  React.lazy(() => import('./views/PipelineHealth.jsx')),
    backtest:           React.lazy(() => import('./views/Backtest.jsx')),
    portfolio:          React.lazy(() => import('./views/Portfolio.jsx')),
    options:            React.lazy(() => import('./views/Options.jsx')),
    heatmap:            React.lazy(() => import('./views/Heatmap.jsx')),
    flows:              React.lazy(() => import('./views/Flows.jsx')),
    'money-flow':       React.lazy(() => import('./views/MoneyFlow.jsx')),
    predictions:        React.lazy(() => import('./views/Predictions.jsx')),
    'cross-reference':  React.lazy(() => import('./views/CrossReference.jsx')),
    'regime-analog':    React.lazy(() => import('./views/RegimeAnalog.jsx')),
    trends:             React.lazy(() => import('./views/TrendTracker.jsx')),
    intelligence:       React.lazy(() => import('./views/IntelDashboard.jsx')),
    influence:          React.lazy(() => import('./views/InfluenceNetwork.jsx')),
    'actor-network':    React.lazy(() => import('./views/ActorNetwork.jsx')),
    'actor-universe':   React.lazy(() => import('./views/ActorUniverse.jsx')),
    'lever-map':        React.lazy(() => import('./views/LeverMap.jsx')),
    globe:              React.lazy(() => import('./views/GlobeView.jsx')),
    risk:               React.lazy(() => import('./views/RiskMap.jsx')),
    thesis:             React.lazy(() => import('./views/Thesis.jsx')),
    earnings:           React.lazy(() => import('./views/EarningsCalendar.jsx')),
    'market-diary':     React.lazy(() => import('./views/MarketDiary.jsx')),
    timeline:           React.lazy(() => import('./views/Timeline.jsx')),
    why:                React.lazy(() => import('./views/WhyView.jsx')),
    'correlation-matrix': React.lazy(() => import('./views/CorrelationMatrix.jsx')),
    architecture:       React.lazy(() => import('./views/AppArchitecture.jsx')),
    weights:            React.lazy(() => import('./views/WeightSliders.jsx')),
    hyperspace:         React.lazy(() => import('./views/Hyperspace.jsx')),
    settings:           React.lazy(() => import('./views/Settings.jsx')),
    archive:            React.lazy(() => import('./views/Archive.jsx')),
};

// Sub-routes — not in routes.js because they are child views with bespoke props.
const JournalEntry      = React.lazy(() => import('./views/JournalEntry.jsx'));
const WatchlistAnalysis = React.lazy(() => import('./views/WatchlistAnalysis.jsx'));
const SectorDive        = React.lazy(() => import('./views/SectorDive.jsx'));
const AssociationsLegacy = React.lazy(() => import('./views/AssociationsLegacy.jsx'));

const styles = {
    app: {
        background: '#080C10',
        minHeight: '100vh',
        color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
        display: 'flex',
        flexDirection: 'column',
    },
    content: {
        flex: 1,
        overflowY: 'auto',
        WebkitOverflowScrolling: 'touch',
    },
    notifContainer: {
        position: 'fixed',
        top: 'calc(env(safe-area-inset-top, 0px) + 8px)',
        left: '16px',
        right: '16px',
        zIndex: 1000,
        display: 'flex',
        flexDirection: 'column',
        gap: '6px',
        pointerEvents: 'none',
    },
    notification: {
        padding: '12px 16px',
        borderRadius: '8px',
        fontFamily: "'IBM Plex Sans', sans-serif",
        fontSize: '14px',
        animation: 'slideDown 0.3s ease',
        pointerEvents: 'auto',
        boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
    },
};

function useIsDesktop() {
    const [d, setD] = React.useState(typeof window !== 'undefined' ? window.innerWidth >= 1024 : false);
    React.useEffect(() => {
        const h = () => setD(window.innerWidth >= 1024);
        window.addEventListener('resize', h);
        return () => window.removeEventListener('resize', h);
    }, []);
    return d;
}

function App() {
    const {
        isAuthenticated, activeView, notifications, setActiveView,
        clearAuth, handleWsMessage, removeNotification,
    } = useStore();

    const isDesktop = useIsDesktop();
    const [entryId, setEntryId] = useState(null);
    const [selectedTicker, setSelectedTicker] = useState(null);
    const [selectedSector, setSelectedSector] = useState(null);
    const [paletteOpen, setPaletteOpen] = useState(false);
    const [showTour, setShowTour] = useState(false);

    // Cmd+K / Ctrl+K global shortcut for command palette
    useEffect(() => {
        const handleKeyDown = (e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
                e.preventDefault();
                setPaletteOpen(prev => !prev);
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, []);

    useEffect(() => {
        const hash = window.location.hash.slice(2) || 'dashboard';
        if (hash.startsWith('journal/')) {
            setEntryId(parseInt(hash.split('/')[1]));
            setActiveView('journal-entry');
        } else if (hash.startsWith('watchlist/')) {
            setSelectedTicker(hash.split('/')[1]);
            setActiveView('watchlist-analysis');
        } else if (hash.startsWith('sector-dive/')) {
            setSelectedSector(decodeURIComponent(hash.split('/')[1]));
            setActiveView('sector-dive');
        } else {
            setActiveView(hash);
        }
    }, []);

    useEffect(() => {
        if (isAuthenticated) {
            api.connectWebSocket((msg) => {
                handleWsMessage(msg);
            });
            return () => api.disconnectWebSocket();
        }
    }, [isAuthenticated]);

    const navigate = (view, id) => {
        if (view === 'journal-entry' && id) {
            setEntryId(id);
            window.location.hash = `#/journal/${id}`;
        } else if (view === 'watchlist-analysis' && id) {
            setSelectedTicker(id);
            window.location.hash = `#/watchlist/${id}`;
        } else if (view === 'sector-dive' && id) {
            setSelectedSector(id);
            window.location.hash = `#/sector-dive/${encodeURIComponent(id)}`;
        } else {
            window.location.hash = `#/${view}`;
        }
        setActiveView(view);
    };

    if (!isAuthenticated) {
        return <Login />;
    }

    const renderView = () => {
        // Sub-routes with bespoke props — handled before the generic lookup.
        if (activeView === 'journal-entry') {
            return <JournalEntry entryId={entryId} onBack={() => navigate('journal')} />;
        }
        if (activeView === 'watchlist-analysis') {
            return <WatchlistAnalysis ticker={selectedTicker} onBack={() => navigate('dashboard')} />;
        }
        if (activeView === 'sector-dive') {
            return <SectorDive sector={selectedSector} onBack={() => navigate('money-flow')} />;
        }
        if (activeView === 'associations-legacy') {
            return <AssociationsLegacy />;
        }

        // Views that need onNavigate / onLogout props wired explicitly.
        const navigatePropViews = new Set([
            'dashboard', 'money-flow', 'cross-reference', 'intelligence',
            'timeline', 'why', 'journal',
        ]);
        const Component = routeComponents[activeView] || routeComponents['dashboard'];

        if (activeView === 'settings') {
            return <Component onLogout={() => { clearAuth(); }} onShowTour={() => setShowTour(true)} />;
        }
        if (activeView === 'associations') {
            return <Component onNavigate={(v) => { window.location.hash = `#/${v}`; }} />;
        }
        if (navigatePropViews.has(activeView)) {
            return <Component onNavigate={navigate} />;
        }
        return <Component />;
    };

    const notifColors = {
        info: '#1A6EBF',
        success: '#1A7A4A',
        error: '#8B1F1F',
        warning: '#8A6000',
    };

    const appStyle = {
        ...styles.app,
        paddingTop: isDesktop ? '48px' : 0,
        paddingBottom: isDesktop ? 0 : 'calc(60px + env(safe-area-inset-bottom, 0px))',
    };

    return (
        <div style={appStyle}>
            <div style={styles.notifContainer}>
                {notifications.map((n, i) => (
                    <div
                        key={n.id}
                        onClick={() => removeNotification?.(n.id)}
                        style={{
                            ...styles.notification,
                            background: notifColors[n.type] || notifColors.info,
                            cursor: 'pointer',
                        }}
                    >
                        {n.message}
                    </div>
                ))}
            </div>
            <div style={styles.content}>
                <ErrorBoundary key={activeView}>
                    <Suspense fallback={<div style={{ padding: '60px 20px', textAlign: 'center', color: '#5A7080', fontFamily: "'IBM Plex Mono', monospace", fontSize: '13px' }}>Loading view...</div>}>
                        {renderView()}
                    </Suspense>
                </ErrorBoundary>
            </div>
            <NavBar activeView={activeView} onNavigate={navigate} onSearchOpen={() => setPaletteOpen(true)} />
            <ChatPanel />
            <CommandPalette
                open={paletteOpen}
                onClose={() => setPaletteOpen(false)}
                onNavigate={(view, id) => { navigate(view, id); setPaletteOpen(false); }}
            />
            <Onboarding
                forceShow={showTour}
                onDismiss={() => setShowTour(false)}
            />
        </div>
    );
}

const root = createRoot(document.getElementById('root'));
root.render(<App />);
