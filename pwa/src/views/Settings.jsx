import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import StatusDot from '../components/StatusDot.jsx';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    section: { marginBottom: '20px' },
    sectionTitle: {
        fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '10px',
    },
    card: {
        background: '#0D1520', borderRadius: '10px', padding: '16px',
        border: '1px solid #1A2840', marginBottom: '12px',
    },
    sourceRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '8px 0', borderBottom: '1px solid #1A284044',
    },
    row: {
        display: 'flex', justifyContent: 'space-between', padding: '6px 0',
        fontSize: '13px',
    },
    label: { color: '#5A7080' },
    value: { color: '#C8D8E8', fontFamily: "'JetBrains Mono', monospace" },
    btn: {
        width: '100%', padding: '12px', borderRadius: '8px', border: '1px solid #1A2840',
        background: 'transparent', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '13px', cursor: 'pointer', minHeight: '44px', marginBottom: '8px',
    },
    logoutBtn: {
        width: '100%', padding: '14px', borderRadius: '8px', border: '1px solid #8B1F1F',
        background: '#8B1F1F22', color: '#8B1F1F', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '14px', fontWeight: 600, cursor: 'pointer', minHeight: '44px',
    },
};

export default function Settings({ onLogout }) {
    const { systemStatus, wsConnected, addNotification } = useStore();
    const [sources, setSources] = useState([]);
    const [config, setConfig] = useState(null);

    useEffect(() => {
        api.getSources().then(d => setSources(d.sources || [])).catch(() => {});
        api.getConfig().then(d => setConfig(d.config || {})).catch(() => {});
    }, []);

    const testConnection = async () => {
        try {
            const data = await api.getStatus();
            addNotification('success', 'Connection OK');
        } catch {
            addNotification('error', 'Connection failed');
        }
    };

    return (
        <div style={styles.container}>
            <div style={styles.title}>SETTINGS</div>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>DATA SOURCES</div>
                <div style={styles.card}>
                    {sources.map(s => (
                        <div key={s.id} style={styles.sourceRow}>
                            <div>
                                <div style={{ fontSize: '14px', fontFamily: "'JetBrains Mono', monospace" }}>
                                    {s.name}
                                </div>
                                <div style={{ fontSize: '11px', color: '#5A7080' }}>
                                    Trust: {s.trust_score} | Priority: {s.priority_rank}
                                </div>
                            </div>
                            <StatusDot status={s.active ? 'online' : 'offline'} />
                        </div>
                    ))}
                    {sources.length === 0 && (
                        <div style={{ color: '#5A7080', fontSize: '13px', textAlign: 'center', padding: '12px' }}>
                            No sources configured
                        </div>
                    )}
                </div>
            </div>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>FEATURES</div>
                <div style={styles.card}>
                    <div style={styles.row}>
                        <span style={styles.label}>Total</span>
                        <span style={styles.value}>{systemStatus?.grid?.features_total || 0}</span>
                    </div>
                    <div style={styles.row}>
                        <span style={styles.label}>Model Eligible</span>
                        <span style={styles.value}>{systemStatus?.grid?.features_model_eligible || 0}</span>
                    </div>
                </div>
            </div>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>CONNECTION</div>
                <div style={styles.card}>
                    <div style={styles.row}>
                        <span style={styles.label}>API</span>
                        <span style={styles.value}>{window.location.origin}</span>
                    </div>
                    <div style={styles.row}>
                        <span style={styles.label}>WebSocket</span>
                        <StatusDot status={wsConnected ? 'online' : 'offline'} label={wsConnected ? 'Connected' : 'Disconnected'} />
                    </div>
                    <button style={{ ...styles.btn, color: '#1A6EBF', marginTop: '8px' }}
                        onClick={testConnection}>
                        TEST CONNECTION
                    </button>
                </div>
            </div>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>ABOUT</div>
                <div style={styles.card}>
                    <div style={styles.row}>
                        <span style={styles.label}>Version</span>
                        <span style={styles.value}>1.0.0</span>
                    </div>
                    <div style={styles.row}>
                        <span style={styles.label}>Uptime</span>
                        <span style={styles.value}>
                            {systemStatus?.uptime_seconds
                                ? `${Math.round(systemStatus.uptime_seconds / 60)}m`
                                : '—'}
                        </span>
                    </div>
                </div>
            </div>

            <div style={styles.section}>
                <button style={styles.logoutBtn} onClick={onLogout}>
                    LOG OUT
                </button>
            </div>
        </div>
    );
}
