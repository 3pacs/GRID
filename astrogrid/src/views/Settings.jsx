import React, { useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import useStore from '../store.js';
import api from '../api.js';

const settStyles = {
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '14px 0',
        borderBottom: `1px solid rgba(74, 158, 255, 0.08)`,
    },
    actionBtn: (variant = 'neutral') => ({
        padding: '12px 24px',
        background: variant === 'danger' ? 'rgba(239, 68, 68, 0.15)' : 'rgba(74, 158, 255, 0.15)',
        color: variant === 'danger' ? tokens.red : tokens.accent,
        border: `1px solid ${variant === 'danger' ? 'rgba(239, 68, 68, 0.3)' : tokens.cardBorder}`,
        borderRadius: tokens.radius.md,
        fontFamily: tokens.fontSans,
        fontSize: '14px',
        fontWeight: 600,
        cursor: 'pointer',
        width: '100%',
    }),
    version: {
        textAlign: 'center',
        fontSize: '11px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        marginTop: tokens.spacing.xxl,
    },
    field: {
        marginBottom: tokens.spacing.md,
    },
    label: {
        display: 'block',
        marginBottom: tokens.spacing.xs,
        fontSize: '12px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        letterSpacing: '1px',
        textTransform: 'uppercase',
    },
    input: {
        width: '100%',
        boxSizing: 'border-box',
        borderRadius: tokens.radius.sm,
        border: `1px solid ${tokens.cardBorder}`,
        background: 'rgba(10, 18, 35, 0.6)',
        color: tokens.text,
        padding: '12px 14px',
        fontSize: '14px',
        fontFamily: tokens.fontMono,
    },
    segmented: {
        display: 'grid',
        gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
        gap: tokens.spacing.sm,
        marginBottom: tokens.spacing.md,
    },
    segmentButton: (active) => ({
        borderRadius: tokens.radius.sm,
        border: `1px solid ${active ? tokens.accent : tokens.cardBorder}`,
        background: active ? 'rgba(74, 158, 255, 0.16)' : 'rgba(10, 18, 35, 0.6)',
        color: active ? tokens.textBright : tokens.textMuted,
        padding: '12px 10px',
        cursor: 'pointer',
        fontFamily: tokens.fontSans,
        fontWeight: 600,
    }),
    helper: {
        fontSize: '12px',
        color: tokens.textMuted,
        lineHeight: '1.6',
        marginTop: tokens.spacing.sm,
    },
};

export default function Settings() {
    const {
        apiMode,
        apiBaseUrl,
        apiToken,
        connectionStatus,
        connectionMessage,
        setApiMode,
        setApiBaseUrl,
        setApiToken,
    } = useStore();
    const [draftBaseUrl, setDraftBaseUrl] = useState(apiBaseUrl);
    const [draftToken, setDraftToken] = useState(apiToken);
    const [testing, setTesting] = useState(false);

    const saveSettings = () => {
        setApiBaseUrl(draftBaseUrl.trim());
        setApiToken(draftToken.trim());
    };

    const testConnection = async () => {
        saveSettings();
        setTesting(true);
        try {
            await api.ping();
        } finally {
            setTesting(false);
        }
    };

    return (
        <div style={styles.container}>
            <div style={styles.header}>Settings</div>
            <div style={styles.subheader}>Standalone Configuration</div>

            <div style={styles.card}>
                <div style={settStyles.field}>
                    <span style={settStyles.label}>Data Source</span>
                    <div style={settStyles.segmented}>
                        <button
                            type="button"
                            style={settStyles.segmentButton(apiMode === 'demo')}
                            onClick={() => setApiMode('demo')}
                        >
                            Demo data
                        </button>
                        <button
                            type="button"
                            style={settStyles.segmentButton(apiMode === 'live')}
                            onClick={() => setApiMode('live')}
                        >
                            Live backend
                        </button>
                    </div>
                    <div style={settStyles.helper}>
                        Demo mode keeps AstroGrid fully usable without the GRID backend. Live mode calls `/api/v1/astrogrid/*` on your configured server.
                    </div>
                </div>

                <div style={settStyles.field}>
                    <label style={settStyles.label} htmlFor="astrogrid-api-base-url">API Base URL</label>
                    <input
                        id="astrogrid-api-base-url"
                        style={settStyles.input}
                        value={draftBaseUrl}
                        onChange={(event) => setDraftBaseUrl(event.target.value)}
                        placeholder="http://localhost:8000"
                    />
                </div>

                <div style={settStyles.field}>
                    <label style={settStyles.label} htmlFor="astrogrid-api-token">Bearer Token</label>
                    <input
                        id="astrogrid-api-token"
                        style={settStyles.input}
                        value={draftToken}
                        onChange={(event) => setDraftToken(event.target.value)}
                        placeholder="Optional token for protected APIs"
                    />
                </div>

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
                    <div style={{ fontSize: '14px', color: tokens.text }}>Connection</div>
                    <div style={{ fontSize: '13px', color: connectionStatus === 'connected' ? tokens.green : tokens.gold, fontFamily: tokens.fontMono }}>
                        {connectionStatus}
                    </div>
                </div>
                <div style={settStyles.helper}>{connectionMessage}</div>
            </div>

            <div style={{ display: 'grid', gap: tokens.spacing.sm }}>
                <button
                    type="button"
                    style={settStyles.actionBtn()}
                    onClick={saveSettings}
                >
                    Save standalone settings
                </button>
                <button
                    type="button"
                    style={settStyles.actionBtn()}
                    onClick={testConnection}
                >
                    {testing ? 'Testing connection...' : 'Test live connection'}
                </button>
                <button
                    type="button"
                    style={settStyles.actionBtn('danger')}
                    onClick={() => {
                        setDraftToken('');
                        setApiToken('');
                    }}
                >
                    Clear token
                </button>
            </div>

            <div style={settStyles.version}>AstroGrid v0.1.0</div>
        </div>
    );
}
