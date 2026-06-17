import React, { useState, useEffect, Suspense, useCallback } from 'react';
import { createRoot } from 'react-dom/client';
import useStore from './store.js';
import { api } from './api.js';
import NavBar from './components/NavBar.jsx';
import DadNav from './components/DadNav.jsx';
import { isSimpleUser } from './authSession.js';

// Views a simple/dad user is allowed to see. Everything else (the operator
// cockpit) is hidden and redirects back to the composer.
const DAD_VIEWS = new Set(['home', 'ten-year', 'ticker-lookup']);
import ViewErrorBoundary from './components/ViewErrorBoundary.jsx';
import Login from './views/Login.jsx';
import ChatPanel from './components/ChatPanel.jsx';
import CommandPalette from './components/CommandPalette.jsx';
import Onboarding from './components/Onboarding.jsx';
import { buildRouteHash, parseHashRoute } from './routing.js';
import { routes } from './routes.js';
import Surfacer from './views/Surfacer.jsx';

// Build generic routed views from routes.js so route metadata is the source
// of truth while Vite still discovers lazy chunks statically.
const viewModules = import.meta.glob(['./views/*.jsx', '!./views/Login.jsx', '!./views/Surfacer.jsx']);

function lazyView(path) {
    const loader = viewModules[path];
    if (!loader) {
        throw new Error(`Route component not found: ${path}`);
    }
    return React.lazy(loader);
}

const routeLoaders = Object.fromEntries(
    routes.map(route => [
        route.id,
        route.id === 'surfacer'
            ? () => Promise.resolve({ default: Surfacer })
            : viewModules[route.component],
    ]),
);

const routeComponents = Object.fromEntries(
    routes.map(route => [
        route.id,
        route.id === 'surfacer' ? Surfacer : lazyView(route.component),
    ]),
);

const extraRouteComponents = {
    home: lazyView('./views/Home.jsx'),
    'ticker-lookup': lazyView('./views/TickerLookup.jsx'),
    'intel-mod': lazyView('./views/IntelModeration.jsx'),
    'intel-submit': lazyView('./views/IntelSubmit.jsx'),
};

const extraRouteLoaders = {
    home: viewModules['./views/Home.jsx'],
    'ticker-lookup': viewModules['./views/TickerLookup.jsx'],
    'intel-mod': viewModules['./views/IntelModeration.jsx'],
    'intel-submit': viewModules['./views/IntelSubmit.jsx'],
};

const REALTIME_SOCKET_VIEWS = new Set([
    'dashboard',
    'agents',
    'settings',
    'regime',
    'hyperspace',
]);

function isDocumentVisible() {
    if (typeof document === 'undefined' || typeof document.visibilityState !== 'string') {
        return true;
    }
    return document.visibilityState !== 'hidden';
}

// Sub-routes — not in routes.js because they are child views with bespoke props.
const journalEntryLoader = () => import('./views/JournalEntry.jsx');
const watchlistAnalysisLoader = () => import('./views/WatchlistAnalysis.jsx');
const sectorDiveLoader = () => import('./views/SectorDive.jsx');
const associationsLegacyLoader = () => import('./views/AssociationsLegacy.jsx');

const JournalEntry      = React.lazy(journalEntryLoader);
const WatchlistAnalysis = React.lazy(watchlistAnalysisLoader);
const SectorDive        = React.lazy(sectorDiveLoader);
const AssociationsLegacy = React.lazy(associationsLegacyLoader);

const childRouteLoaders = {
    'journal-entry': journalEntryLoader,
    'watchlist-analysis': watchlistAnalysisLoader,
    'sector-dive': sectorDiveLoader,
    'associations-legacy': associationsLegacyLoader,
};

const DAD_PRELOAD_VIEW_IDS = ['home', 'ticker-lookup', 'ten-year'];
const OPERATOR_HOT_PRELOAD_VIEW_IDS = [
    'ten-year',
    'dashboard',
    'surfacer',
    'money-flow',
    'actor-network',
    'risk',
    'intelligence',
    'regime',
    'signals',
    'options',
    'flows',
    'heatmap',
    'architecture',
    'pipeline-health',
];
const ALL_PRELOAD_VIEW_IDS = [
    ...OPERATOR_HOT_PRELOAD_VIEW_IDS,
    ...routes.map(route => route.id),
    ...Object.keys(extraRouteLoaders),
    ...Object.keys(childRouteLoaders),
];

const OPERATOR_PRELOAD_API_PATHS = [
    '/api/v1/regime/current',
    '/api/v1/system/status',
    '/api/v1/watchlist/prices',
    '/api/v1/watchlist/enriched?limit=8',
    '/api/v1/ten-year-portfolio/weekly?capital=1000000&years=10',
    '/api/v1/intelligence/dashboard',
    '/api/v1/intelligence/thesis',
    '/api/v1/intelligence/risk-map',
    '/api/v1/intelligence/actor-network',
    '/api/v1/intelligence/levers',
    '/api/v1/flows/aggregated?period=weekly&days=30',
    '/api/v1/flows/money-map',
    '/api/v1/flows/sectors',
    '/api/v1/flows/sankey',
    '/api/v1/signals',
    '/api/v1/signals/snapshot',
    '/api/v1/options/recommendations',
    '/api/v1/options/signals?limit=50',
    '/api/v1/discovery/smart-heatmap?orthogonal_only=true',
    '/api/v1/associations/correlation-matrix?days=252',
    '/api/v1/system/pipeline-health',
    '/api/v1/system/architecture',
];

const DAD_PRELOAD_API_PATHS = [
    '/api/v1/ten-year-portfolio/weekly?capital=1000000&years=10',
];

function scheduleIdleTask(callback, timeout = 1200) {
    if (typeof window === 'undefined') return undefined;
    if (typeof window.requestIdleCallback === 'function') {
        const id = window.requestIdleCallback(callback, { timeout });
        return () => window.cancelIdleCallback?.(id);
    }
    const id = window.setTimeout(callback, 250);
    return () => window.clearTimeout(id);
}

const styles = {
    app: {
        background: '#080C10',
        width: '100%',
        minHeight: '100vh',
        color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
        display: 'flex',
        flexDirection: 'column',
        overflowX: 'hidden',
    },
    content: {
        flex: 1,
        overflowY: 'auto',
        overflowX: 'hidden',
        minWidth: 0,
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

export function App() {
    const {
        isAuthenticated, activeView, notifications, setActiveView,
        clearAuth, handleWsMessage, removeNotification, wsConnected, lastSocketEventAt,
    } = useStore();

    const isDesktop = useIsDesktop();
    const simple = isAuthenticated && isSimpleUser();
    const dadInitRef = React.useRef(false);
    const [entryId, setEntryId] = useState(null);
    const [selectedTicker, setSelectedTicker] = useState(null);
    const [selectedSector, setSelectedSector] = useState(null);
    const [focusFeature, setFocusFeature] = useState(null);
    const [focusHypothesis, setFocusHypothesis] = useState(null);
    const [focusActor, setFocusActor] = useState(null);
    const [focusSource, setFocusSource] = useState(null);
    const [originView, setOriginView] = useState(null);
    const [paletteOpen, setPaletteOpen] = useState(false);
    const [chatOpen, setChatOpen] = useState(false);
    const [showTour, setShowTour] = useState(false);
    const [documentVisible, setDocumentVisible] = useState(isDocumentVisible);
    const [refreshVersion, setRefreshVersion] = useState(0);
    const [isRefreshingVisuals, setIsRefreshingVisuals] = useState(false);
    const [preloadStatus, setPreloadStatus] = useState({ state: 'idle', completed: 0, total: 0 });
    const lastSocketEventAtRef = React.useRef(lastSocketEventAt);
    const preloadRunRef = React.useRef(0);

    const shouldUseRealtimeSocket = isAuthenticated
        && documentVisible
        && REALTIME_SOCKET_VIEWS.has(activeView);

    useEffect(() => {
        lastSocketEventAtRef.current = lastSocketEventAt;
    }, [lastSocketEventAt]);

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

            setEntryId(route.entryId ?? null);
            setSelectedTicker(route.selectedTicker ?? null);
            setSelectedSector(route.selectedSector ?? null);
            setFocusFeature(route.focusFeature ?? null);
            setFocusHypothesis(route.focusHypothesis ?? null);
            setFocusActor(route.focusActor ?? null);
            setFocusSource(route.focusSource ?? null);
            setOriginView(route.originView ?? null);
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
        if (typeof document === 'undefined') {
            return undefined;
        }

        const handleVisibilityChange = () => {
            setDocumentVisible(isDocumentVisible());
        };

        handleVisibilityChange();
        document.addEventListener('visibilitychange', handleVisibilityChange);
        return () => document.removeEventListener('visibilitychange', handleVisibilityChange);
    }, []);

    // Dad opens to the composer ("Ask"), once per login — regardless of the
    // operator default (ten-year).
    useEffect(() => {
        if (!isAuthenticated) { dadInitRef.current = false; return; }
        if (simple && !dadInitRef.current) {
            dadInitRef.current = true;
            setActiveView('home');
        }
    }, [isAuthenticated, simple, setActiveView]);

    // Keep dad inside his two pages — any stray navigation to an operator view
    // bounces back to the composer.
    useEffect(() => {
        if (simple && activeView && !DAD_VIEWS.has(activeView)) {
            setActiveView('home');
        }
    }, [simple, activeView, setActiveView]);

    const preloadViewModules = useCallback(async (viewIds, { onProgress = null } = {}) => {
        const uniqueViewIds = [...new Set(viewIds)];
        let completed = 0;
        for (const viewId of uniqueViewIds) {
            const loader = routeLoaders[viewId] || extraRouteLoaders[viewId] || childRouteLoaders[viewId];
            if (loader) {
                await loader().catch(() => null);
            }
            completed += 1;
            onProgress?.({ completed, total: uniqueViewIds.length, viewId });
        }
        return { completed, total: uniqueViewIds.length };
    }, []);

    const warmVisualizations = useCallback(async ({ force = false, hotOnly = false } = {}) => {
        const runId = preloadRunRef.current + 1;
        preloadRunRef.current = runId;

        const viewIds = simple
            ? DAD_PRELOAD_VIEW_IDS
            : hotOnly
                ? OPERATOR_HOT_PRELOAD_VIEW_IDS
                : ALL_PRELOAD_VIEW_IDS;
        const paths = simple ? DAD_PRELOAD_API_PATHS : OPERATOR_PRELOAD_API_PATHS;
        const total = viewIds.length + paths.length;

        setPreloadStatus({ state: force ? 'refreshing' : 'warming', completed: 0, total });

        let completed = 0;
        const tick = () => {
            completed += 1;
            if (preloadRunRef.current === runId) {
                setPreloadStatus({ state: force ? 'refreshing' : 'warming', completed, total });
            }
        };

        await preloadViewModules(viewIds, { onProgress: tick });
        await api.preload(paths, {
            ttlMs: force ? 5_000 : 90_000,
            force,
            concurrency: simple ? 2 : 4,
            onProgress: tick,
        });

        if (preloadRunRef.current === runId) {
            setPreloadStatus({ state: 'ready', completed: total, total });
        }
    }, [preloadViewModules, simple]);

    useEffect(() => {
        if (!isAuthenticated || !documentVisible) {
            return undefined;
        }
        return scheduleIdleTask(() => {
            warmVisualizations({ hotOnly: false }).catch(() => {
                setPreloadStatus(status => ({ ...status, state: 'idle' }));
            });
        });
    }, [documentVisible, isAuthenticated, warmVisualizations]);

    const refreshVisualizations = useCallback(async () => {
        if (isRefreshingVisuals) return;
        setIsRefreshingVisuals(true);
        api.clearCache();
        if (typeof window !== 'undefined' && typeof window.dispatchEvent === 'function') {
            window.dispatchEvent(new CustomEvent('grid:visualizations-refresh', {
                detail: { view: activeView, at: new Date().toISOString() },
            }));
        }
        setRefreshVersion(version => version + 1);
        try {
            await warmVisualizations({ force: true, hotOnly: true });
        } finally {
            setIsRefreshingVisuals(false);
        }
    }, [activeView, isRefreshingVisuals, warmVisualizations]);

    useEffect(() => {
        if (!shouldUseRealtimeSocket) return undefined;

        api.connectWebSocket((msg) => {
            handleWsMessage(msg);
        });
        return () => api.disconnectWebSocket();
    }, [handleWsMessage, shouldUseRealtimeSocket]);

    useEffect(() => {
        if (!shouldUseRealtimeSocket || !wsConnected) {
            return undefined;
        }

        const since = lastSocketEventAtRef.current;
        if (!since) {
            return undefined;
        }

        const replayBefore = new Date().toISOString();
        let cancelled = false;

        api.getRecentRealtimeEvents({ since, before: replayBefore, limit: 100 })
            .then((snapshot) => {
                if (cancelled || snapshot?.error) return;
                for (const event of snapshot?.events || []) {
                    handleWsMessage(event);
                }
            })
            .catch(() => {});

        return () => {
            cancelled = true;
        };
    }, [handleWsMessage, shouldUseRealtimeSocket, wsConnected]);

    const navigate = (view, id) => {
        const originAwareView = view === 'journal-entry'
            || view === 'watchlist-analysis'
            || view === 'sector-dive'
            || view === 'intelligence-search';
        const currentOrigin = originView && (
            activeView === 'journal-entry'
            || activeView === 'watchlist-analysis'
            || activeView === 'sector-dive'
            || activeView === 'intelligence-search'
        )
            ? originView
            : activeView;
        const targetId = originAwareView && id && (typeof id !== 'object' || id === null)
            ? { id, from: currentOrigin }
            : originAwareView && view === 'intelligence-search' && (id == null)
                ? { from: currentOrigin }
                : id;
        const targetHash = buildRouteHash(view, targetId);
        const route = parseHashRoute(targetHash);

        setEntryId(route.entryId ?? null);
        setSelectedTicker(route.selectedTicker ?? null);
        setSelectedSector(route.selectedSector ?? null);
        setFocusFeature(route.focusFeature ?? null);
        setFocusHypothesis(route.focusHypothesis ?? null);
        setFocusActor(route.focusActor ?? null);
        setFocusSource(route.focusSource ?? null);
        setOriginView(route.originView ?? null);

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

    const navigateBack = (fallbackView) => {
        if (originView) {
            navigate(originView);
            return;
        }
        if (typeof window !== 'undefined' && window.history.length > 1) {
            window.history.back();
            return;
        }
        navigate(fallbackView);
    };

    const renderView = () => {
        // Sub-routes with bespoke props — handled before the generic lookup.
        if (activeView === 'journal-entry') {
            return <JournalEntry entryId={entryId} onBack={() => navigateBack('journal')} />;
        }
        if (activeView === 'watchlist-analysis') {
            return <WatchlistAnalysis ticker={selectedTicker} onBack={() => navigateBack('dashboard')} />;
        }
        if (activeView === 'sector-dive') {
            return <SectorDive sector={selectedSector} onBack={() => navigateBack('money-flow')} />;
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

        const viewProps = {
            onNavigate: navigate,
            selectedTicker,
            selectedSector,
            focusFeature,
            focusHypothesis,
            focusActor,
            focusSource,
            originView,
        };

        if (activeView === 'settings') {
            return <Component {...viewProps} onLogout={() => { clearAuth(); }} onShowTour={() => setShowTour(true)} />;
        }
        return <Component {...viewProps} />;
    };

    const notifColors = {
        info: '#1A6EBF',
        success: '#1A7A4A',
        error: '#8B1F1F',
        warning: '#8A6000',
    };

    const appStyle = {
        ...styles.app,
        paddingTop: simple ? '64px' : (isDesktop ? '48px' : 0),
        paddingBottom: simple ? 0 : (isDesktop ? 0 : 'calc(60px + env(safe-area-inset-bottom, 0px))'),
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
                <ViewErrorBoundary key={`${activeView}:${refreshVersion}`} viewName={activeView} onNavigateHome={() => navigate('canvas')}>
                    <Suspense fallback={<div style={{ padding: '60px 20px', textAlign: 'center', color: '#5A7080', fontFamily: "'IBM Plex Mono', monospace", fontSize: '13px' }}>Loading view...</div>}>
                        {renderView()}
                    </Suspense>
                </ViewErrorBoundary>
            </div>
            {simple ? (
                <DadNav
                    activeView={activeView}
                    onNavigate={navigate}
                    onSignOut={() => clearAuth()}
                    onRefreshVisuals={refreshVisualizations}
                    isRefreshingVisuals={isRefreshingVisuals}
                    preloadStatus={preloadStatus}
                />
            ) : (
                <>
                    <NavBar
                        activeView={activeView}
                        onNavigate={navigate}
                        onSearchOpen={() => setPaletteOpen(true)}
                        onChatOpen={() => setChatOpen(true)}
                        onRefreshVisuals={refreshVisualizations}
                        isRefreshingVisuals={isRefreshingVisuals}
                        preloadStatus={preloadStatus}
                    />
                    <ChatPanel open={chatOpen} onOpenChange={setChatOpen} />
                    <CommandPalette
                        open={paletteOpen}
                        onClose={() => setPaletteOpen(false)}
                        onNavigate={(view, id) => { navigate(view, id); setPaletteOpen(false); }}
                    />
                    <Onboarding
                        forceShow={showTour}
                        onDismiss={() => setShowTour(false)}
                    />
                </>
            )}
        </div>
    );
}

const rootElement = document.getElementById('root');
if (rootElement) {
    const root = createRoot(rootElement);
    root.render(<App />);
}
