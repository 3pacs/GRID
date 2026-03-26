import React, { useState } from 'react';
import { ChevronDown, X } from 'lucide-react';
import useStore from '../store.js';
import { tokens } from '../styles/tokens.js';

const FAVORITES = ['SPY', 'QQQ', 'NVDA', 'TSLA', 'AAPL'];

const styles = {
    container: {
        display: 'flex',
        alignItems: 'center',
        gap: tokens.spacing.sm,
        padding: `${tokens.spacing.sm} ${tokens.spacing.lg}`,
    },
    currentTicker: {
        display: 'flex',
        alignItems: 'center',
        gap: tokens.spacing.xs,
        padding: `${tokens.spacing.xs} ${tokens.spacing.md}`,
        background: tokens.card,
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md,
        color: tokens.accent,
        fontFamily: tokens.fontMono,
        fontSize: '16px',
        fontWeight: 700,
        cursor: 'pointer',
        letterSpacing: '1px',
    },
    favorites: {
        display: 'flex',
        gap: tokens.spacing.xs,
        flex: 1,
        overflowX: 'auto',
    },
    favButton: {
        padding: `${tokens.spacing.xs} ${tokens.spacing.sm}`,
        background: 'none',
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.sm,
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        fontSize: '11px',
        fontWeight: 500,
        cursor: 'pointer',
        whiteSpace: 'nowrap',
    },
    favButtonActive: {
        background: 'rgba(0, 212, 170, 0.1)',
        borderColor: tokens.accent,
        color: tokens.accent,
    },
    overlay: {
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: 'rgba(0, 0, 0, 0.85)',
        zIndex: 2000,
        display: 'flex',
        flexDirection: 'column',
        padding: tokens.spacing.lg,
    },
    modalHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: tokens.spacing.lg,
    },
    modalTitle: {
        fontSize: '14px',
        fontWeight: 600,
        color: tokens.textBright,
        fontFamily: tokens.fontMono,
        letterSpacing: '2px',
        textTransform: 'uppercase',
    },
    closeButton: {
        background: 'none',
        border: 'none',
        cursor: 'pointer',
        color: tokens.textMuted,
    },
    tickerGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: tokens.spacing.sm,
        overflowY: 'auto',
        flex: 1,
    },
    tickerItem: {
        padding: `${tokens.spacing.md} ${tokens.spacing.sm}`,
        background: tokens.card,
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md,
        color: tokens.text,
        fontFamily: tokens.fontMono,
        fontSize: '13px',
        fontWeight: 500,
        textAlign: 'center',
        cursor: 'pointer',
    },
    tickerItemActive: {
        background: 'rgba(0, 212, 170, 0.15)',
        borderColor: tokens.accent,
        color: tokens.accent,
        fontWeight: 700,
    },
};

function TickerSelector() {
    const { selectedTicker, setSelectedTicker, tickerList } = useStore();
    const [modalOpen, setModalOpen] = useState(false);

    const selectTicker = (ticker) => {
        setSelectedTicker(ticker);
        setModalOpen(false);
    };

    return (
        <>
            <div style={styles.container}>
                <button style={styles.currentTicker} onClick={() => setModalOpen(true)}>
                    {selectedTicker}
                    <ChevronDown size={14} />
                </button>
                <div style={styles.favorites}>
                    {FAVORITES.map((t) => (
                        <button
                            key={t}
                            style={{
                                ...styles.favButton,
                                ...(t === selectedTicker ? styles.favButtonActive : {}),
                            }}
                            onClick={() => setSelectedTicker(t)}
                        >
                            {t}
                        </button>
                    ))}
                </div>
            </div>

            {modalOpen && (
                <div style={styles.overlay}>
                    <div style={styles.modalHeader}>
                        <span style={styles.modalTitle}>Select Ticker</span>
                        <button style={styles.closeButton} onClick={() => setModalOpen(false)}>
                            <X size={20} />
                        </button>
                    </div>
                    <div style={styles.tickerGrid}>
                        {tickerList.map((t) => (
                            <button
                                key={t}
                                style={{
                                    ...styles.tickerItem,
                                    ...(t === selectedTicker ? styles.tickerItemActive : {}),
                                }}
                                onClick={() => selectTicker(t)}
                            >
                                {t}
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </>
    );
}

export default TickerSelector;
