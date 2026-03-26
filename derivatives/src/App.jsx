import React, { useEffect } from 'react';
import useStore from './store.js';
import NavBar from './components/NavBar.jsx';
import DealerFlow from './views/DealerFlow.jsx';
import GammaProfile from './views/GammaProfile.jsx';
import VolSurface from './views/VolSurface.jsx';
import TermStructure from './views/TermStructure.jsx';
import FlowNarrative from './views/FlowNarrative.jsx';
import PositionHeatmap from './views/PositionHeatmap.jsx';
import Scanner from './views/Scanner.jsx';
import Settings from './views/Settings.jsx';
import { tokens } from './styles/tokens.js';

const appStyles = {
    app: {
        background: tokens.bg,
        minHeight: '100vh',
        color: tokens.text,
        fontFamily: tokens.fontMono,
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
        background: tokens.bg,
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
        color: '#0A0E14',
        border: 'none',
        borderRadius: tokens.radius.md,
        fontFamily: tokens.fontMono,
        fontSize: '14px',
        fontWeight: 600,
        cursor: 'pointer',
        textDecoration: 'none',
    },
};

function App() {
    const { isAuthenticated, activeView, setActiveView } = useStore();

    useEffect(() => {
        const hash = window.location.hash.slice(2) || 'dealer-flow';
        setActiveView(hash);
    }, []);

    const navigate = (view) => {
        window.location.hash = `#/${view}`;
        setActiveView(view);
    };

    if (!isAuthenticated) {
        return (
            <div style={appStyles.loginScreen}>
                <div style={appStyles.loginTitle}>DERIVATIVESGRID</div>
                <div style={appStyles.loginSubtitle}>Dealer Flow Intelligence</div>
                <div style={{ ...appStyles.loginSubtitle, marginTop: '24px' }}>
                    Please log in to GRID first.
                </div>
                <a href="/" style={appStyles.loginLink}>Go to GRID Login</a>
            </div>
        );
    }

    const renderView = () => {
        switch (activeView) {
            case 'dealer-flow': return <DealerFlow />;
            case 'gamma-profile': return <GammaProfile />;
            case 'vol-surface': return <VolSurface />;
            case 'term-structure': return <TermStructure />;
            case 'flow-narrative': return <FlowNarrative />;
            case 'position-heatmap': return <PositionHeatmap />;
            case 'scanner': return <Scanner />;
            case 'settings': return <Settings />;
            default: return <DealerFlow />;
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
