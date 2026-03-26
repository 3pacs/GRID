import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import StatusDot from '../components/StatusDot.jsx';
import DecisionModal from '../components/DecisionModal.jsx';
import ViewHelp from '../components/ViewHelp.jsx';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    card: {
        background: '#0D1520', borderRadius: '10px', padding: '16px',
        border: '1px solid #1A2840', marginBottom: '12px',
    },
    cardTitle: {
        fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '10px',
    },
    bigStatus: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '24px', fontWeight: 700,
        marginBottom: '12px',
    },
    row: {
        display: 'flex', justifyContent: 'space-between', padding: '4px 0', fontSize: '13px',
    },
    label: { color: '#5A7080' },
    value: { color: '#C8D8E8', fontFamily: "'JetBrains Mono', monospace" },
    bigPoints: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '32px', fontWeight: 700,
        color: '#B8922A', textAlign: 'center', marginBottom: '12px',
    },
    controlBtn: {
        padding: '12px', borderRadius: '8px', border: '1px solid #1A2840',
        background: 'transparent', color: '#1A6EBF', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '13px', fontWeight: 600, cursor: 'pointer', minHeight: '44px',
        width: '100%', marginBottom: '8px',
    },
    logArea: {
        background: '#080C10', borderRadius: '8px', padding: '12px',
        fontFamily: "'JetBrains Mono', monospace", fontSize: '11px',
        color: '#5A7080', maxHeight: '200px', overflowY: 'auto',
        lineHeight: 1.5, whiteSpace: 'pre-wrap', wordBreak: 'break-all',
    },
    link: {
        color: '#1A6EBF', fontSize: '13px', textDecoration: 'none',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
};

export default function Hyperspace() {
    const { systemStatus, addNotification } = useStore();
    const [showRestart, setShowRestart] = useState(false);
    const [showLogs, setShowLogs] = useState(false);
    const [logs, setLogs] = useState([]);

    const hs = systemStatus?.hyperspace || {};
    const isOnline = hs.node_online;

    const handleRestart = async () => {
        setShowRestart(false);
        try {
            await api.restartHyperspace();
            addNotification('info', 'Hyperspace node restarting');
        } catch (err) {
            addNotification('error', err.message);
        }
    };

    const loadLogs = async () => {
        try {
            const data = await api.getLogs('hyperspace', 50);
            setLogs(data.lines || []);
            setShowLogs(true);
        } catch {
            setLogs(['Could not load logs']);
            setShowLogs(true);
        }
    };

    return (
        <div style={styles.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={styles.title}>HYPERSPACE</div>
                <ViewHelp id="hyperspace" />
            </div>

            <div style={styles.card}>
                <div style={styles.cardTitle}>NODE STATUS</div>
                <div style={{
                    ...styles.bigStatus,
                    color: isOnline ? '#1A7A4A' : '#8B1F1F',
                }}>
                    {isOnline ? 'ONLINE' : 'OFFLINE'}
                </div>
                {hs.peer_id && (
                    <div style={styles.row}>
                        <span style={styles.label}>Peer ID</span>
                        <span style={{ ...styles.value, fontSize: '11px' }}>
                            {hs.peer_id?.substring(0, 16)}...
                        </span>
                    </div>
                )}
                {hs.connected_peers != null && (
                    <div style={styles.row}>
                        <span style={styles.label}>Peers</span>
                        <span style={styles.value}>{hs.connected_peers}</span>
                    </div>
                )}
                <div style={styles.row}>
                    <span style={styles.label}>API</span>
                    <StatusDot status={hs.api_available ? 'online' : 'offline'} />
                </div>
            </div>

            <div style={styles.card}>
                <div style={styles.cardTitle}>EARNINGS</div>
                <div style={styles.bigPoints}>
                    {hs.points != null ? hs.points.toFixed(0) : '—'}
                </div>
                <div style={{ textAlign: 'center', fontSize: '12px', color: '#5A7080' }}>
                    Total Points
                </div>
                <div style={{ textAlign: 'center', marginTop: '12px' }}>
                    <a href="https://agents.hyper.space" target="_blank" rel="noopener"
                        style={styles.link}>
                        Open Agent Dashboard →
                    </a>
                </div>
            </div>

            {hs.model_loaded && (
                <div style={styles.card}>
                    <div style={styles.cardTitle}>MODEL</div>
                    <div style={styles.row}>
                        <span style={styles.label}>Loaded</span>
                        <span style={styles.value}>{hs.model_loaded}</span>
                    </div>
                </div>
            )}

            <div style={styles.card}>
                <div style={styles.cardTitle}>CONTROLS</div>
                <button style={styles.controlBtn} onClick={() => setShowRestart(true)}>
                    RESTART NODE
                </button>
                <button style={styles.controlBtn} onClick={loadLogs}>
                    VIEW LOGS
                </button>
            </div>

            {showLogs && (
                <div style={styles.card}>
                    <div style={styles.cardTitle}>LOGS</div>
                    <div style={styles.logArea}>
                        {logs.join('\n') || 'No logs available'}
                    </div>
                    <button style={{ ...styles.controlBtn, marginTop: '8px' }}
                        onClick={() => setShowLogs(false)}>
                        CLOSE
                    </button>
                </div>
            )}

            {showRestart && (
                <DecisionModal
                    title="Restart Hyperspace"
                    body="This will stop and restart the Hyperspace node. Active tasks will be interrupted."
                    confirmLabel="RESTART"
                    confirmColor="#8A6000"
                    onConfirm={handleRestart}
                    onCancel={() => setShowRestart(false)}
                />
            )}
        </div>
    );
}
