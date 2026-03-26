import React from 'react';
import useStore from '../store.js';
import { tokens } from '../styles/tokens.js';

const styles = {
    container: { padding: tokens.spacing.lg },
    header: {
        fontSize: '12px', fontWeight: 600, color: tokens.textMuted,
        letterSpacing: '3px', textTransform: 'uppercase',
        marginBottom: tokens.spacing.lg, fontFamily: tokens.fontMono,
    },
    card: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md, padding: tokens.spacing.lg,
        marginBottom: tokens.spacing.md,
    },
    cardTitle: {
        fontSize: '11px', fontWeight: 600, color: tokens.accent,
        fontFamily: tokens.fontMono, letterSpacing: '1px',
        textTransform: 'uppercase', marginBottom: tokens.spacing.md,
    },
    row: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: `${tokens.spacing.sm} 0`,
        borderBottom: `1px solid rgba(0,212,170,0.05)`,
        fontSize: '12px', fontFamily: tokens.fontMono,
    },
    label: { color: tokens.textMuted },
    value: { color: tokens.textBright, fontWeight: 500 },
    version: {
        textAlign: 'center', marginTop: tokens.spacing.xxl,
        fontSize: '10px', color: tokens.textMuted, fontFamily: tokens.fontMono,
    },
};

function Settings() {
    const { selectedTicker, tickerList, isAuthenticated } = useStore();

    return (
        <div style={styles.container}>
            <div style={styles.header}>Settings</div>

            <div style={styles.card}>
                <div style={styles.cardTitle}>Configuration</div>
                <div style={styles.row}>
                    <span style={styles.label}>Selected Ticker</span>
                    <span style={{ ...styles.value, color: tokens.accent }}>{selectedTicker}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Tracked Tickers</span>
                    <span style={styles.value}>{tickerList.length}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Authenticated</span>
                    <span style={{ ...styles.value, color: isAuthenticated ? tokens.safe : tokens.danger }}>
                        {isAuthenticated ? 'Yes' : 'No'}
                    </span>
                </div>
            </div>

            <div style={styles.card}>
                <div style={styles.cardTitle}>API Endpoints</div>
                <div style={styles.row}>
                    <span style={styles.label}>Base</span>
                    <span style={styles.value}>/api/v1/derivatives</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>GEX</span>
                    <span style={styles.value}>/gex/{'{ticker}'}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Vol Surface</span>
                    <span style={styles.value}>/vol-surface/{'{ticker}'}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Term Structure</span>
                    <span style={styles.value}>/term-structure/{'{ticker}'}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>OI Heatmap</span>
                    <span style={styles.value}>/oi-heatmap/{'{ticker}'}</span>
                </div>
            </div>

            <div style={styles.card}>
                <div style={styles.cardTitle}>Data Sources</div>
                <div style={styles.row}>
                    <span style={styles.label}>Options Chain</span>
                    <span style={styles.value}>options_snapshots</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Daily Signals</span>
                    <span style={styles.value}>options_daily_signals</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>GEX Engine</span>
                    <span style={styles.value}>DealerGammaEngine</span>
                </div>
            </div>

            <div style={styles.version}>
                DerivativesGrid v0.1.0 — Dealer Flow Intelligence
            </div>
        </div>
    );
}

export default Settings;
