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
    const { systemStatus, wsConnected, addNotification, userRole, username } = useStore();
    const [sources, setSources] = useState([]);
    const [config, setConfig] = useState(null);
    const [users, setUsers] = useState([]);
    const [newUser, setNewUser] = useState({ username: '', password: '', role: 'contributor' });
    const [showAddUser, setShowAddUser] = useState(false);
    const isAdmin = userRole === 'admin';

    useEffect(() => {
        api.getSources().then(d => setSources(d.sources || [])).catch(() => {});
        api.getConfig().then(d => setConfig(d.config || {})).catch(() => {});
        if (isAdmin) {
            api.listUsers().then(u => setUsers(Array.isArray(u) ? u : [])).catch(() => {});
        }
    }, []);

    const testConnection = async () => {
        try {
            const data = await api.getStatus();
            addNotification('success', 'Connection OK');
        } catch {
            addNotification('error', 'Connection failed');
        }
    };

    const handleCreateUser = async () => {
        if (!newUser.username || !newUser.password) {
            addNotification('error', 'Username and password required');
            return;
        }
        if (newUser.password.length < 8) {
            addNotification('error', 'Password must be at least 8 characters');
            return;
        }
        try {
            await api.createUser(newUser.username, newUser.password, newUser.role);
            addNotification('success', `User "${newUser.username}" created`);
            setNewUser({ username: '', password: '', role: 'contributor' });
            setShowAddUser(false);
            api.listUsers().then(u => setUsers(Array.isArray(u) ? u : [])).catch(() => {});
        } catch (err) {
            addNotification('error', err.message || 'Failed to create user');
        }
    };

    const handleDeleteUser = async (uname) => {
        if (!confirm(`Delete user "${uname}"?`)) return;
        try {
            await api.deleteUser(uname);
            addNotification('success', `User "${uname}" deleted`);
            setUsers(users.filter(u => u.username !== uname));
        } catch (err) {
            addNotification('error', err.message || 'Failed to delete user');
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

            {/* Current User */}
            <div style={styles.section}>
                <div style={styles.sectionTitle}>ACCOUNT</div>
                <div style={styles.card}>
                    <div style={styles.row}>
                        <span style={styles.label}>Logged in as</span>
                        <span style={styles.value}>{username}</span>
                    </div>
                    <div style={styles.row}>
                        <span style={styles.label}>Role</span>
                        <span style={{
                            ...styles.value,
                            color: isAdmin ? '#22C55E' : '#3B82F6',
                        }}>{userRole.toUpperCase()}</span>
                    </div>
                </div>
            </div>

            {/* User Management (admin only) */}
            {isAdmin && (
                <div style={styles.section}>
                    <div style={styles.sectionTitle}>USER MANAGEMENT</div>
                    <div style={styles.card}>
                        {users.map(u => (
                            <div key={u.username} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '8px 0', borderBottom: '1px solid #1A284044',
                            }}>
                                <div>
                                    <div style={{ fontSize: '14px', fontFamily: "'JetBrains Mono', monospace", color: '#C8D8E8' }}>
                                        {u.username}
                                    </div>
                                    <div style={{ fontSize: '11px', color: u.role === 'admin' ? '#22C55E' : '#3B82F6' }}>
                                        {u.role}
                                    </div>
                                </div>
                                <button onClick={() => handleDeleteUser(u.username)} style={{
                                    background: 'none', border: '1px solid #8B1F1F44', borderRadius: '6px',
                                    color: '#8B1F1F', fontSize: '11px', padding: '4px 10px', cursor: 'pointer',
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>DELETE</button>
                            </div>
                        ))}
                        {users.length === 0 && (
                            <div style={{ color: '#5A7080', fontSize: '13px', padding: '8px 0' }}>
                                No user accounts yet (only master password)
                            </div>
                        )}

                        {!showAddUser ? (
                            <button onClick={() => setShowAddUser(true)} style={{
                                ...styles.btn, color: '#22C55E', border: '1px solid #22C55E44', marginTop: '12px',
                            }}>+ ADD USER</button>
                        ) : (
                            <div style={{ marginTop: '12px', padding: '12px', background: '#080C10', borderRadius: '8px' }}>
                                <input
                                    type="text" placeholder="Username" value={newUser.username}
                                    onChange={e => setNewUser({ ...newUser, username: e.target.value })}
                                    style={{ ...styles.btn, color: '#C8D8E8', textAlign: 'left', padding: '10px 12px', fontSize: '14px' }}
                                />
                                <input
                                    type="password" placeholder="Password (8+ chars)" value={newUser.password}
                                    onChange={e => setNewUser({ ...newUser, password: e.target.value })}
                                    style={{ ...styles.btn, color: '#C8D8E8', textAlign: 'left', padding: '10px 12px', fontSize: '14px' }}
                                />
                                <div style={{ display: 'flex', gap: '8px', marginBottom: '8px' }}>
                                    {['contributor', 'admin'].map(r => (
                                        <button key={r} onClick={() => setNewUser({ ...newUser, role: r })} style={{
                                            flex: 1, padding: '8px', borderRadius: '6px', border: 'none', cursor: 'pointer',
                                            fontFamily: "'JetBrains Mono', monospace", fontSize: '12px',
                                            background: newUser.role === r ? (r === 'admin' ? '#22C55E' : '#1A6EBF') : '#1A2840',
                                            color: newUser.role === r ? '#fff' : '#5A7080',
                                        }}>{r.toUpperCase()}</button>
                                    ))}
                                </div>
                                <div style={{ display: 'flex', gap: '8px' }}>
                                    <button onClick={handleCreateUser} style={{
                                        flex: 1, ...styles.btn, background: '#22C55E', color: '#fff', border: 'none',
                                    }}>CREATE</button>
                                    <button onClick={() => setShowAddUser(false)} style={{
                                        flex: 1, ...styles.btn, color: '#5A7080',
                                    }}>CANCEL</button>
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            )}

            <div style={styles.section}>
                <button style={styles.logoutBtn} onClick={onLogout}>
                    LOG OUT
                </button>
            </div>
        </div>
    );
}
