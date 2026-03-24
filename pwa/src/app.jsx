import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import useStore from './store.js';
import { api } from './api.js';
import NavBar from './components/NavBar.jsx';
import ErrorBoundary from './components/ErrorBoundary.jsx';
import Login from './views/Login.jsx';
import Dashboard from './views/Dashboard.jsx';
import Regime from './views/Regime.jsx';
import Signals from './views/Signals.jsx';
import Journal from './views/Journal.jsx';
import JournalEntry from './views/JournalEntry.jsx';
import Models from './views/Models.jsx';
import Discovery from './views/Discovery.jsx';
import Hyperspace from './views/Hyperspace.jsx';
import Agents from './views/Agents.jsx';
import Briefings from './views/Briefings.jsx';
import Knowledge from './views/Knowledge.jsx';
import Workflows from './views/Workflows.jsx';
import Physics from './views/Physics.jsx';
import SystemLogs from './views/SystemLogs.jsx';
import Backtest from './views/Backtest.jsx';
import Settings from './views/Settings.jsx';

const styles = {
    app: {
        background: '#080C10',
        minHeight: '100vh',
        color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
        display: 'flex',
        flexDirection: 'column',
        paddingBottom: 'calc(60px + env(safe-area-inset-bottom, 0px))',
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

function App() {
    const {
        isAuthenticated, activeView, notifications, setActiveView,
        clearAuth, handleWsMessage, removeNotification,
    } = useStore();

    const [entryId, setEntryId] = useState(null);

    useEffect(() => {
        const hash = window.location.hash.slice(2) || 'dashboard';
        if (hash.startsWith('journal/')) {
            setEntryId(parseInt(hash.split('/')[1]));
            setActiveView('journal-entry');
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
        } else {
            window.location.hash = `#/${view}`;
        }
        setActiveView(view);
    };

    if (!isAuthenticated) {
        return <Login />;
    }

    const renderView = () => {
        switch (activeView) {
            case 'dashboard': return <Dashboard onNavigate={navigate} />;
            case 'regime': return <Regime />;
            case 'signals': return <Signals />;
            case 'journal': return <Journal onNavigate={navigate} />;
            case 'journal-entry': return <JournalEntry entryId={entryId} onBack={() => navigate('journal')} />;
            case 'models': return <Models />;
            case 'discovery': return <Discovery />;
            case 'agents': return <Agents />;
            case 'briefings': return <Briefings />;
            case 'knowledge': return <Knowledge />;
            case 'workflows': return <Workflows />;
            case 'physics': return <Physics />;
            case 'system': return <SystemLogs />;
            case 'backtest': return <Backtest />;
            case 'hyperspace': return <Hyperspace />;
            case 'settings': return <Settings onLogout={() => { clearAuth(); }} />;
            default: return <Dashboard onNavigate={navigate} />;
        }
    };

    const notifColors = {
        info: '#1A6EBF',
        success: '#1A7A4A',
        error: '#8B1F1F',
        warning: '#8A6000',
    };

    return (
        <div style={styles.app}>
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
                    {renderView()}
                </ErrorBoundary>
            </div>
            <NavBar activeView={activeView} onNavigate={navigate} />
        </div>
    );
}

const root = createRoot(document.getElementById('root'));
root.render(<App />);
