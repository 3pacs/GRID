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
    loginScreen: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100vh',
        background: tokens.bgGradient,
        gap: '16px',
    },
    loginTitle: {
        fontFamily: tokens.fontMono,
        fontSize: '28px',
        fontWeight: 700,
        color: tokens.accent,
        letterSpacing: '6px',
    },
    loginSubtitle: {
        fontSize: '13px',
        color: tokens.textMuted,
    },
    loginLink: {
        marginTop: '12px',
        padding: '12px 32px',
        background: tokens.accent,
        color: '#fff',
        border: 'none',
        borderRadius: tokens.radius.md,
        fontFamily: tokens.fontSans,
        fontSize: '14px',
        fontWeight: 600,
        cursor: 'pointer',
        textDecoration: 'none',
    },
};

function App() {
    const { isAuthenticated, activeView, setActiveView } = useStore();

    useEffect(() => {
        const hash = window.location.hash.slice(2) || 'orrery';
        setActiveView(hash);
    }, []);

    const navigate = (view) => {
        window.location.hash = `#/${view}`;
        setActiveView(view);
    };

    if (!isAuthenticated) {
        return (
            <div style={appStyles.loginScreen}>
                <div style={appStyles.loginTitle}>ASTROGRID</div>
                <div style={appStyles.loginSubtitle}>Celestial Intelligence</div>
                <div style={{ ...appStyles.loginSubtitle, marginTop: '24px' }}>
                    Please log in to GRID first.
                </div>
                <a href="/" style={appStyles.loginLink}>Go to GRID Login</a>
            </div>
        );
    }

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
            <div style={appStyles.content}>
                {renderView()}
            </div>
            <NavBar activeView={activeView} onNavigate={navigate} />
        </div>
    );
}

export default App;
