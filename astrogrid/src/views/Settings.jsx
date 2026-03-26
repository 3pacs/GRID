import React from 'react';
import { tokens, styles } from '../styles/tokens.js';
import useStore from '../store.js';

const settStyles = {
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '14px 0',
        borderBottom: `1px solid rgba(74, 158, 255, 0.08)`,
    },
    logoutBtn: {
        padding: '12px 24px',
        background: 'rgba(239, 68, 68, 0.15)',
        color: tokens.red,
        border: `1px solid rgba(239, 68, 68, 0.3)`,
        borderRadius: tokens.radius.md,
        fontFamily: tokens.fontSans,
        fontSize: '14px',
        fontWeight: 600,
        cursor: 'pointer',
        marginTop: tokens.spacing.xl,
        width: '100%',
    },
    version: {
        textAlign: 'center',
        fontSize: '11px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        marginTop: tokens.spacing.xxl,
    },
};

export default function Settings() {
    const { clearAuth } = useStore();

    return (
        <div style={styles.container}>
            <div style={styles.header}>Settings</div>
            <div style={styles.subheader}>AstroGrid Configuration</div>

            <div style={styles.card}>
                <div style={settStyles.row}>
                    <div style={{ fontSize: '14px', color: tokens.text }}>Theme</div>
                    <div style={{ fontSize: '13px', color: tokens.textMuted, fontFamily: tokens.fontMono }}>
                        Deep Space
                    </div>
                </div>
                <div style={settStyles.row}>
                    <div style={{ fontSize: '14px', color: tokens.text }}>Coordinate System</div>
                    <div style={{ fontSize: '13px', color: tokens.textMuted, fontFamily: tokens.fontMono }}>
                        Tropical
                    </div>
                </div>
                <div style={settStyles.row}>
                    <div style={{ fontSize: '14px', color: tokens.text }}>Ayanamsa</div>
                    <div style={{ fontSize: '13px', color: tokens.textMuted, fontFamily: tokens.fontMono }}>
                        Lahiri
                    </div>
                </div>
                <div style={{ ...settStyles.row, borderBottom: 'none' }}>
                    <div style={{ fontSize: '14px', color: tokens.text }}>API Status</div>
                    <div style={{ fontSize: '13px', color: tokens.green, fontFamily: tokens.fontMono }}>
                        Connected
                    </div>
                </div>
            </div>

            <button
                style={settStyles.logoutBtn}
                onClick={() => {
                    clearAuth();
                    window.location.href = '/';
                }}
            >
                Log Out
            </button>

            <div style={settStyles.version}>AstroGrid v0.1.0</div>
        </div>
    );
}
