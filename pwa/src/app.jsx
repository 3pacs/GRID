import React, { useState, useEffect, Suspense } from 'react';
import { createRoot } from 'react-dom/client';
import useStore from './store.js';
import { api } from './api.js';
import NavBar from './components/NavBar.jsx';
import ViewErrorBoundary from './components/ViewErrorBoundary.jsx';
import Login from './views/Login.jsx';
import ChatPanel from './components/ChatPanel.jsx';
import CommandPalette from './components/CommandPalette.jsx';
import Onboarding from './components/Onboarding.jsx';
import { buildRouteHash, parseHashRoute } from './routing.js';
import { routes } from './routes.js';

// Build generic routed views from routes.js so route metadata is the source
// of truth while Vite still discovers lazy chunks statically.
const viewModules = import.meta.glob(['./views/*.jsx', '!./views/Login.jsx']);

function lazyView(path) {
    const loader = viewModules[path];
    if (!loader) {
        throw new Error(`Route component not found: ${path}`);
    }
    return React.lazy(loader);
}

const routeComponents = Object.fromEntries(
    routes.map(route => [route.id, lazyView(route.component)]),
);

const extraRouteComponents = {
    home: lazyView('./views/Home.jsx'),
    'intel-mod': lazyView('./views/IntelModeration.jsx'),
    'intel-submit': lazyView('./views/IntelSubmit.jsx'),
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
        const syncRouteFromHash = () => {
            const route = parseHashRoute(window.location.hash);
            if (route.view === 'login') {
                clearAuth();
                return;
            }

            if (route.entryId) setEntryId(route.entryId);
            if (route.selectedTicker) setSelectedTicker(route.selectedTicker);
            if (route.selectedSector) setSelectedSector(route.selectedSector);
            setActiveView(route.view);
        };

        const handleAuthExpired = () => {
            clearAuth();
            if (window.location.hash !== '#/login') {
                window.location.hash = '#/login';
            }
        };

        syncRouteFromHash();
        window.addEventListener('hashchange', syncRouteFromHash);
        window.addEventListener('grid:auth-expired', handleAuthExpired);
        return () => {
            window.removeEventListener('hashchange', syncRouteFromHash);
            window.removeEventListener('grid:auth-expired', handleAuthExpired);
        };
    }, [clearAuth, setActiveView]);

    useEffect(() => {
        if (isAuthenticated) {
            api.connectWebSocket((msg) => {
                handleWsMessage(msg);
            });
            return () => api.disconnectWebSocket();
        }
    }, [isAuthenticated]);

    const navigate = (view, id) => {
        const targetHash = buildRouteHash(view, id);
        const route = parseHashRoute(targetHash);

        if (route.entryId) setEntryId(route.entryId);
        if (route.selectedTicker) setSelectedTicker(route.selectedTicker);
        if (route.selectedSector) setSelectedSector(route.selectedSector);

        if (window.location.hash === targetHash) {
            const event = typeof HashChangeEvent === 'function'
                ? new HashChangeEvent('hashchange', {
                    oldURL: window.location.href,
                    newURL: window.location.href,
                })
                : new Event('hashchange');
            window.dispatchEvent(event);
        } else {
            window.location.hash = targetHash;
        }
        setActiveView(route.view);
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

        const Component = routeComponents[activeView] || extraRouteComponents[activeView];
        if (!Component) {
            return (
                <div style={{ padding: '60px 20px', color: '#C8D8E8', fontFamily: "'IBM Plex Sans', sans-serif" }}>
                    <div style={{ fontSize: '12px', color: '#8AA0B8', fontFamily: "'IBM Plex Mono', monospace", marginBottom: '8px' }}>
                        UNKNOWN MODULE
                    </div>
                    <div style={{ fontSize: '22px', fontWeight: 700, marginBottom: '10px' }}>
                        {activeView}
                    </div>
                    <button
                        onClick={() => navigate('canvas')}
                        style={{
                            background: '#1A6EBF',
                            color: '#fff',
                            border: 'none',
                            borderRadius: '6px',
                            padding: '9px 14px',
                            fontWeight: 700,
                            cursor: 'pointer',
                        }}
                    >
                        Open Canvas
                    </button>
                </div>
            );
        }

        if (activeView === 'settings') {
            return <Component onNavigate={navigate} onLogout={() => { clearAuth(); }} onShowTour={() => setShowTour(true)} />;
        }
        return <Component onNavigate={navigate} />;
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
                <ViewErrorBoundary key={activeView} viewName={activeView} onNavigateHome={() => navigate('canvas')}>
                    <Suspense fallback={<div style={{ padding: '60px 20px', textAlign: 'center', color: '#5A7080', fontFamily: "'IBM Plex Mono', monospace", fontSize: '13px' }}>Loading view...</div>}>
                        {renderView()}
                    </Suspense>
                </ViewErrorBoundary>
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
