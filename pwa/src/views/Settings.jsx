import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import StatusDot from '../components/StatusDot.jsx';
import ViewHelp from '../components/ViewHelp.jsx';
import { colors, tokens, shared } from '../styles/shared.js';

// ── Styles ──────────────────────────────────────────────────────

const s = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)', maxWidth: '960px', margin: '0 auto' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: colors.textMuted, letterSpacing: '2px', marginBottom: '16px',
    },
    section: { marginBottom: '24px' },
    sectionTitle: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
        marginBottom: '10px', textTransform: 'uppercase',
    },
    card: {
        background: colors.card, borderRadius: tokens.radius.md, padding: '16px',
        border: `1px solid ${colors.border}`, marginBottom: '12px',
    },
    row: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '8px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        minHeight: '36px',
    },
    label: { color: colors.textMuted, fontSize: '13px' },
    value: { color: colors.text, fontFamily: "'JetBrains Mono', monospace", fontSize: '13px' },
    // Service tiles grid
    tilesGrid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))',
        gap: '10px', marginBottom: '14px',
    },
    tile: (online) => ({
        background: online ? colors.greenBg : colors.redBg,
        border: `1px solid ${online ? '#1A5A3A' : '#5A1A1A'}`,
        borderRadius: tokens.radius.md, padding: '12px',
        textAlign: 'center', transition: `all ${tokens.transition.fast}`,
    }),
    tileName: { fontSize: '12px', fontWeight: 600, color: colors.text, marginTop: '6px' },
    // Bar
    barOuter: {
        width: '100%', height: '10px', background: colors.bg,
        borderRadius: '5px', overflow: 'hidden', marginTop: '4px',
    },
    barInner: (pct, color) => ({
        width: `${Math.min(pct, 100)}%`, height: '100%',
        background: color || colors.accent,
        borderRadius: '5px', transition: `width ${tokens.transition.normal}`,
    }),
    // Badge
    badge: (success) => ({
        display: 'inline-flex', alignItems: 'center', padding: '2px 8px',
        borderRadius: tokens.radius.sm, fontSize: '11px', fontWeight: 600,
        background: success ? colors.greenBg : colors.redBg,
        color: success ? colors.green : colors.red,
        border: `1px solid ${success ? '#1A5A3A' : '#5A1A1A'}`,
    }),
    // Key row
    keyRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '6px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        fontSize: '13px',
    },
    checkmark: { color: colors.green, fontWeight: 700, fontSize: '14px' },
    crossmark: { color: colors.red, fontWeight: 700, fontSize: '14px' },
    // Button
    btn: {
        ...shared.buttonSmall,
        minWidth: '80px',
    },
    btnDanger: { ...shared.buttonSmall, background: '#8B1F1F' },
    logoutBtn: {
        width: '100%', padding: '14px', borderRadius: tokens.radius.sm,
        border: '1px solid #8B1F1F', background: '#8B1F1F22', color: '#8B1F1F',
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        fontWeight: 600, cursor: 'pointer', minHeight: tokens.minTouch,
    },
    summaryText: {
        fontSize: '12px', color: colors.textDim, fontFamily: "'JetBrains Mono', monospace",
        marginBottom: '8px',
    },
    note: {
        fontSize: '11px', color: colors.textMuted, fontStyle: 'italic',
        marginTop: '6px', lineHeight: '1.5',
    },
    taskRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '8px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        gap: '8px', flexWrap: 'wrap',
    },
    tabs: { ...shared.tabs, marginBottom: '16px' },
    tab: (active) => shared.tab(active),
    // User management
    inputField: {
        ...shared.input, marginBottom: '8px',
    },
};


// ── Helper components ───────────────────────────────────────────

function UsageBar({ label, percent, detail, color }) {
    const barColor = percent > 90 ? colors.red : percent > 70 ? colors.yellow : (color || colors.green);
    return (
        <div style={{ marginBottom: '10px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '2px' }}>
                <span style={s.label}>{label}</span>
                <span style={{ ...s.value, fontSize: '12px' }}>
                    {percent != null ? `${percent}%` : '--'}{detail ? ` (${detail})` : ''}
                </span>
            </div>
            <div style={s.barOuter}>
                <div style={s.barInner(percent || 0, barColor)} />
            </div>
        </div>
    );
}

function ServiceTile({ name, status }) {
    const online = status === 'online';
    return (
        <div style={s.tile(online)}>
            <StatusDot status={online ? 'online' : 'offline'} size={10} />
            <div style={s.tileName}>{name}</div>
        </div>
    );
}

function formatUptime(seconds) {
    if (!seconds) return '--';
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    if (d > 0) return `${d}d ${h}h`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

function formatTime(iso) {
    if (!iso) return '--';
    try {
        const d = new Date(iso);
        return d.toLocaleString(undefined, {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
        });
    } catch { return iso; }
}


// ── Tab sections ────────────────────────────────────────────────

const TAB_NAMES = ['Status', 'API Keys', 'Hermes', 'Coverage', 'Notifications', 'Account'];

// ── Main component ──────────────────────────────────────────────

// ── Toggle switch component ────────────────────────────────────
function ToggleSwitch({ enabled, onToggle, label, description }) {
    return (
        <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '10px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        }}>
            <div style={{ flex: 1 }}>
                <div style={{ fontSize: '13px', color: colors.text }}>{label}</div>
                {description && (
                    <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                        {description}
                    </div>
                )}
            </div>
            <div
                onClick={onToggle}
                style={{
                    width: '44px', height: '24px', borderRadius: '12px', cursor: 'pointer',
                    background: enabled ? colors.green : colors.border,
                    position: 'relative', transition: 'background 0.2s',
                    flexShrink: 0, marginLeft: '12px',
                }}
            >
                <div style={{
                    width: '18px', height: '18px', borderRadius: '50%',
                    background: '#fff', position: 'absolute', top: '3px',
                    left: enabled ? '23px' : '3px', transition: 'left 0.2s',
                }} />
            </div>
        </div>
    );
}

export default function Settings({ onLogout }) {
    const {
        systemStatus, wsConnected, addNotification, userRole, username,
        pushSupported, pushPermission, pushSubscription,
        pushPreferences, setPushPermission, setPushSubscription, setPushPreferences,
    } = useStore();
    const isAdmin = userRole === 'admin';

    const [tab, setTab] = useState('Status');
    const [services, setServices] = useState(null);
    const [apiKeys, setApiKeys] = useState(null);
    const [hermesStatus, setHermesStatus] = useState(null);
    const [freshness, setFreshness] = useState(null);
    const [sources, setSources] = useState([]);
    const [users, setUsers] = useState([]);
    const [newUser, setNewUser] = useState({ username: '', password: '', role: 'contributor' });
    const [showAddUser, setShowAddUser] = useState(false);
    const [loading, setLoading] = useState({});

    // Fetch data for current tab
    useEffect(() => {
        if (tab === 'Status') fetchServices();
        if (tab === 'API Keys') fetchApiKeys();
        if (tab === 'Hermes') fetchHermes();
        if (tab === 'Coverage') fetchCoverage();
        if (tab === 'Notifications') fetchPushPreferences();
        if (tab === 'Account') fetchUsers();
    }, [tab]);

    const fetchServices = () => {
        setLoading(p => ({ ...p, services: true }));
        api.getServices()
            .then(d => setServices(d))
            .catch(() => addNotification('error', 'Failed to load services'))
            .finally(() => setLoading(p => ({ ...p, services: false })));
    };

    const fetchApiKeys = () => {
        setLoading(p => ({ ...p, keys: true }));
        api.getApiKeys()
            .then(d => setApiKeys(d))
            .catch(() => addNotification('error', 'Failed to load API keys'))
            .finally(() => setLoading(p => ({ ...p, keys: false })));
    };

    const fetchHermes = () => {
        setLoading(p => ({ ...p, hermes: true }));
        api.getHermesStatus()
            .then(d => setHermesStatus(d))
            .catch(() => {})
            .finally(() => setLoading(p => ({ ...p, hermes: false })));
    };

    const fetchCoverage = () => {
        setLoading(p => ({ ...p, coverage: true }));
        Promise.all([
            api.getFreshness().catch(() => null),
            api.getSources().then(d => d.sources || []).catch(() => []),
        ]).then(([fr, src]) => {
            setFreshness(fr);
            setSources(src);
        }).finally(() => setLoading(p => ({ ...p, coverage: false })));
    };

    const fetchUsers = () => {
        if (isAdmin) {
            api.listUsers().then(u => setUsers(Array.isArray(u) ? u : [])).catch(() => {});
        }
    };

    // ── Push notification helpers ─────────────────────────────────

    const fetchPushPreferences = async () => {
        if (pushSubscription) {
            try {
                const sub = pushSubscription.toJSON();
                const prefs = await api.getNotificationPreferences(sub.endpoint);
                setPushPreferences(prefs);
            } catch {
                // Subscription might not be registered yet
            }
        }
    };

    const handleEnableNotifications = async () => {
        if (!pushSupported) {
            addNotification('error', 'Push notifications not supported in this browser');
            return;
        }

        setLoading(p => ({ ...p, push: true }));
        try {
            // Request permission
            const permission = await Notification.requestPermission();
            setPushPermission(permission);

            if (permission !== 'granted') {
                addNotification('error', 'Notification permission denied');
                return;
            }

            // Get VAPID key from server
            const { vapid_public_key } = await api.getVapidKey();

            // Convert VAPID key to Uint8Array
            const urlBase64ToUint8Array = (base64String) => {
                const padding = '='.repeat((4 - base64String.length % 4) % 4);
                const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
                const rawData = window.atob(base64);
                return Uint8Array.from([...rawData].map(c => c.charCodeAt(0)));
            };

            // Register service worker and get push subscription
            const registration = await navigator.serviceWorker.ready;
            const subscription = await registration.pushManager.subscribe({
                userVisibleOnly: true,
                applicationServerKey: urlBase64ToUint8Array(vapid_public_key),
            });

            // Send subscription to backend
            await api.subscribePush(subscription, navigator.userAgent);
            setPushSubscription(subscription);

            addNotification('success', 'Push notifications enabled');
        } catch (err) {
            addNotification('error', `Failed to enable notifications: ${err.message}`);
        } finally {
            setLoading(p => ({ ...p, push: false }));
        }
    };

    const handleDisableNotifications = async () => {
        setLoading(p => ({ ...p, push: true }));
        try {
            if (pushSubscription) {
                const sub = pushSubscription.toJSON();
                await api.unsubscribePush(sub.endpoint);
                await pushSubscription.unsubscribe();
                setPushSubscription(null);
            }
            addNotification('success', 'Push notifications disabled');
        } catch (err) {
            addNotification('error', `Failed to disable: ${err.message}`);
        } finally {
            setLoading(p => ({ ...p, push: false }));
        }
    };

    const handleTestPush = async () => {
        if (!pushSubscription) {
            addNotification('error', 'Enable notifications first');
            return;
        }
        try {
            await api.testPush(pushSubscription);
            addNotification('success', 'Test notification sent');
        } catch (err) {
            addNotification('error', `Test failed: ${err.message}`);
        }
    };

    const handlePrefToggle = async (key) => {
        if (!pushSubscription) return;
        const newVal = !pushPreferences[key];
        const updated = { ...pushPreferences, [key]: newVal };
        setPushPreferences(updated);

        try {
            const sub = pushSubscription.toJSON();
            await api.updateNotificationPreferences(sub.endpoint, { [key]: newVal });
        } catch {
            // Revert on failure
            setPushPreferences(pushPreferences);
            addNotification('error', 'Failed to update preference');
        }
    };

    const handleThresholdChange = async (val) => {
        const threshold = parseFloat(val);
        if (isNaN(threshold) || threshold < 0.1 || threshold > 50) return;
        const updated = { ...pushPreferences, price_alert_threshold: threshold };
        setPushPreferences(updated);

        if (pushSubscription) {
            try {
                const sub = pushSubscription.toJSON();
                await api.updateNotificationPreferences(sub.endpoint, { price_alert_threshold: threshold });
            } catch {
                addNotification('error', 'Failed to update threshold');
            }
        }
    };

    const testConnection = async () => {
        try {
            await api.getStatus();
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
            fetchUsers();
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

    const handleRunWorkflow = async (name) => {
        setLoading(p => ({ ...p, [`run_${name}`]: true }));
        try {
            await api.runWorkflow(name);
            addNotification('success', `Workflow "${name}" triggered`);
        } catch (err) {
            addNotification('error', err.message || `Failed to run ${name}`);
        } finally {
            setLoading(p => ({ ...p, [`run_${name}`]: false }));
        }
    };

    // ── Render helpers ──────────────────────────────────────────

    const renderStatus = () => {
        const svc = services;
        const res = svc?.resources || {};
        return (
            <div style={s.section}>
                <div style={s.sectionTitle}>SERVICES</div>
                <div style={s.tilesGrid}>
                    {svc?.services?.map(sv => (
                        <ServiceTile key={sv.name} name={sv.name} status={sv.status} />
                    ))}
                </div>
                {svc && (
                    <div style={s.summaryText}>
                        {svc.online} of {svc.total} services online
                    </div>
                )}

                <div style={s.sectionTitle}>RESOURCES</div>
                <div style={s.card}>
                    <UsageBar
                        label="Disk"
                        percent={res.disk_percent || systemStatus?.server?.disk_percent}
                        detail={`${res.disk_free_gb || systemStatus?.server?.disk_free_gb || '--'} GB free`}
                    />
                    <UsageBar
                        label="Memory"
                        percent={res.memory_percent || systemStatus?.server?.memory_percent}
                        detail={`${res.memory_used_gb || systemStatus?.server?.memory_used_gb || '--'} / ${res.memory_total_gb || systemStatus?.server?.memory_total_gb || '--'} GB`}
                    />
                </div>

                <div style={s.sectionTitle}>CONNECTION</div>
                <div style={s.card}>
                    <div style={s.row}>
                        <span style={s.label}>API</span>
                        <span style={s.value}>{window.location.origin}</span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>WebSocket</span>
                        <StatusDot status={wsConnected ? 'online' : 'offline'} label={wsConnected ? 'Connected' : 'Disconnected'} />
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Uptime</span>
                        <span style={s.value}>
                            {formatUptime(
                                svc?.services?.find(x => x.name === 'API')?.uptime_seconds
                                || systemStatus?.uptime_seconds
                            )}
                        </span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Started</span>
                        <span style={s.value}>{formatTime(svc?.start_time)}</span>
                    </div>
                    <button
                        style={{ ...s.btn, width: '100%', marginTop: '10px', background: 'transparent', border: `1px solid ${colors.accent}`, color: colors.accent }}
                        onClick={testConnection}
                    >
                        TEST CONNECTION
                    </button>
                </div>
            </div>
        );
    };

    const renderApiKeys = () => {
        const keys = apiKeys?.keys || [];
        const configured = apiKeys?.configured || 0;
        const total = apiKeys?.total || 0;
        return (
            <div style={s.section}>
                <div style={s.sectionTitle}>API KEY STATUS</div>
                <div style={s.summaryText}>
                    {configured} of {total} keys configured
                </div>
                <div style={s.card}>
                    {keys.map(k => (
                        <div key={k.name} style={s.keyRow}>
                            <div>
                                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '12px', color: colors.text }}>
                                    {k.name}
                                </span>
                                <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '1px' }}>
                                    {k.description}
                                </div>
                            </div>
                            <span style={k.status === 'configured' ? s.checkmark : s.crossmark}>
                                {k.status === 'configured' ? '\u2713' : '\u2717'}
                            </span>
                        </div>
                    ))}
                    {keys.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '12px' }}>
                            {loading.keys ? 'Loading...' : 'No API key information available'}
                        </div>
                    )}
                </div>
                <div style={s.note}>
                    Missing keys degrade gracefully — data sources requiring those keys will be skipped during ingestion.
                </div>
            </div>
        );
    };

    const renderHermes = () => {
        const sched = hermesStatus?.schedule || {};
        const tasks = hermesStatus?.tasks || [];
        const snapshots = hermesStatus?.snapshots || [];

        const scheduleRows = [
            { label: 'Health cycle', value: sched.cycle_interval || '5 min' },
            { label: 'Full pipeline', value: sched.pipeline_interval || '6 hours' },
            { label: 'Autoresearch', value: sched.autoresearch || 'weekdays 2 AM' },
            { label: 'Daily briefing', value: sched.daily_briefing || 'weekdays 6 AM' },
            { label: 'Weekly briefing', value: sched.weekly_briefing || 'Monday 7 AM' },
            { label: 'Freshness threshold', value: sched.data_freshness_threshold || '26 hours' },
        ];

        return (
            <div style={s.section}>
                <div style={s.sectionTitle}>INTELLIGENCE SCHEDULE</div>
                <div style={s.card}>
                    {scheduleRows.map(r => (
                        <div key={r.label} style={s.row}>
                            <span style={s.label}>{r.label}</span>
                            <span style={s.value}>{r.value}</span>
                        </div>
                    ))}
                </div>

                <div style={s.sectionTitle}>RECENT TASK HISTORY</div>
                <div style={s.card}>
                    {tasks.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '12px' }}>
                            {loading.hermes ? 'Loading...' : 'No task history available'}
                        </div>
                    )}
                    {tasks.slice(0, 15).map(t => (
                        <div key={t.id} style={s.taskRow}>
                            <div style={{ flex: 1, minWidth: 0 }}>
                                <div style={{ fontSize: '12px', color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                                    {t.title}
                                </div>
                                <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                                    {t.category}{t.source ? ` / ${t.source}` : ''} — {formatTime(t.timestamp)}
                                </div>
                            </div>
                            <span style={s.badge(t.result === 'SUCCESS')}>
                                {t.result || t.severity}
                            </span>
                        </div>
                    ))}
                </div>

                {snapshots.length > 0 && (
                    <>
                        <div style={s.sectionTitle}>RECENT CYCLE SNAPSHOTS</div>
                        <div style={s.card}>
                            {snapshots.slice(0, 5).map((snap, i) => (
                                <div key={i} style={s.row}>
                                    <div>
                                        <span style={{ fontSize: '12px', color: colors.text }}>
                                            {formatTime(snap.timestamp)}
                                        </span>
                                        <span style={{ fontSize: '11px', color: colors.textMuted, marginLeft: '8px' }}>
                                            {snap.issues_found || 0} issues, {snap.issues_fixed || 0} fixed
                                        </span>
                                    </div>
                                    <span style={s.badge(!snap.issues_found || snap.issues_fixed >= snap.issues_found)}>
                                        {!snap.issues_found ? 'CLEAN' : snap.issues_fixed >= snap.issues_found ? 'RESOLVED' : 'ISSUES'}
                                    </span>
                                </div>
                            ))}
                        </div>
                    </>
                )}
            </div>
        );
    };

    const renderCoverage = () => {
        const families = freshness?.families || [];
        const overallStatus = freshness?.overall_status || '--';
        const gridStats = systemStatus?.grid || {};

        const statusColor = (st) =>
            st === 'GREEN' ? colors.green : st === 'YELLOW' ? colors.yellow : colors.red;

        return (
            <div style={s.section}>
                <div style={s.sectionTitle}>DATA COVERAGE</div>
                <div style={s.card}>
                    <div style={s.row}>
                        <span style={s.label}>Overall freshness</span>
                        <span style={{
                            ...s.value,
                            color: statusColor(overallStatus),
                            fontWeight: 700,
                        }}>{overallStatus}</span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Total features</span>
                        <span style={s.value}>{gridStats.features_total || 0}</span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Model eligible</span>
                        <span style={s.value}>{gridStats.features_model_eligible || 0}</span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Hypotheses</span>
                        <span style={s.value}>{gridStats.hypotheses_total || 0}</span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>In production</span>
                        <span style={s.value}>{gridStats.hypotheses_in_production || 0}</span>
                    </div>
                </div>

                <div style={s.sectionTitle}>FRESHNESS BY FAMILY</div>
                <div style={s.card}>
                    {families.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '12px' }}>
                            {loading.coverage ? 'Loading...' : 'No family data available'}
                        </div>
                    )}
                    {families.map(f => {
                        const pct = f.total > 0 ? Math.round(f.fresh_today / f.total * 100) : 0;
                        return (
                            <div key={f.family} style={{ marginBottom: '10px' }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '2px' }}>
                                    <span style={{ fontSize: '12px', color: colors.text, fontWeight: 600 }}>
                                        {f.family}
                                    </span>
                                    <span style={{
                                        fontSize: '11px', fontFamily: "'JetBrains Mono', monospace",
                                        color: statusColor(f.status),
                                    }}>
                                        {f.fresh_today}/{f.total} fresh ({pct}%)
                                    </span>
                                </div>
                                <div style={s.barOuter}>
                                    <div style={s.barInner(pct, statusColor(f.status))} />
                                </div>
                            </div>
                        );
                    })}
                </div>

                {sources.length > 0 && (
                    <>
                        <div style={s.sectionTitle}>DATA SOURCES</div>
                        <div style={s.card}>
                            {sources.map(src => (
                                <div key={src.id} style={s.row}>
                                    <div>
                                        <div style={{ fontSize: '13px', fontFamily: "'JetBrains Mono', monospace", color: colors.text }}>
                                            {src.name}
                                        </div>
                                        <div style={{ fontSize: '11px', color: colors.textMuted }}>
                                            Trust: {src.trust_score} | Priority: {src.priority_rank}
                                        </div>
                                    </div>
                                    <StatusDot status={src.active ? 'online' : 'offline'} />
                                </div>
                            ))}
                        </div>
                    </>
                )}
            </div>
        );
    };

    const renderNotifications = () => {
        const isEnabled = pushPermission === 'granted' && pushSubscription;
        const isDenied = pushPermission === 'denied';

        return (
            <div style={s.section}>
                <div style={s.sectionTitle}>PUSH NOTIFICATIONS</div>
                <div style={s.card}>
                    <div style={s.row}>
                        <span style={s.label}>Browser Support</span>
                        <span style={s.badge(pushSupported)}>
                            {pushSupported ? 'SUPPORTED' : 'NOT SUPPORTED'}
                        </span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Permission</span>
                        <span style={{
                            ...s.value,
                            color: pushPermission === 'granted' ? colors.green
                                 : pushPermission === 'denied' ? colors.red
                                 : colors.yellow,
                        }}>
                            {pushPermission.toUpperCase()}
                        </span>
                    </div>
                    <div style={s.row}>
                        <span style={s.label}>Status</span>
                        <span style={s.badge(isEnabled)}>
                            {isEnabled ? 'ACTIVE' : 'INACTIVE'}
                        </span>
                    </div>

                    {isDenied && (
                        <div style={s.note}>
                            Notification permission was denied. To re-enable, reset
                            notification permissions for this site in your browser settings.
                        </div>
                    )}

                    <div style={{ marginTop: '12px', display: 'flex', gap: '8px' }}>
                        {!isEnabled ? (
                            <button
                                style={{
                                    ...s.btn, flex: 1,
                                    background: isDenied ? colors.border : colors.accent,
                                    opacity: isDenied || loading.push ? 0.5 : 1,
                                }}
                                onClick={handleEnableNotifications}
                                disabled={isDenied || loading.push || !pushSupported}
                            >
                                {loading.push ? 'ENABLING...' : 'ENABLE NOTIFICATIONS'}
                            </button>
                        ) : (
                            <>
                                <button
                                    style={{ ...s.btn, flex: 1, background: colors.accent }}
                                    onClick={handleTestPush}
                                >
                                    TEST
                                </button>
                                <button
                                    style={{ ...s.btnDanger, flex: 1 }}
                                    onClick={handleDisableNotifications}
                                    disabled={loading.push}
                                >
                                    DISABLE
                                </button>
                            </>
                        )}
                    </div>
                </div>

                {isEnabled && (
                    <>
                        <div style={s.sectionTitle}>NOTIFICATION CATEGORIES</div>
                        <div style={s.card}>
                            <ToggleSwitch
                                label="Trade Recommendations"
                                description="New options trades, 100x alerts"
                                enabled={pushPreferences.trade_recommendations}
                                onToggle={() => handlePrefToggle('trade_recommendations')}
                            />
                            <ToggleSwitch
                                label="Convergence Alerts"
                                description="Signal alignment and convergence events"
                                enabled={pushPreferences.convergence_alerts}
                                onToggle={() => handlePrefToggle('convergence_alerts')}
                            />
                            <ToggleSwitch
                                label="Regime Changes"
                                description="Market regime transitions"
                                enabled={pushPreferences.regime_changes}
                                onToggle={() => handlePrefToggle('regime_changes')}
                            />
                            <ToggleSwitch
                                label="Red Flags"
                                description="System warnings, data failures"
                                enabled={pushPreferences.red_flags}
                                onToggle={() => handlePrefToggle('red_flags')}
                            />
                            <ToggleSwitch
                                label="Price Alerts"
                                description="Significant price moves"
                                enabled={pushPreferences.price_alerts}
                                onToggle={() => handlePrefToggle('price_alerts')}
                            />
                        </div>

                        {pushPreferences.price_alerts && (
                            <>
                                <div style={s.sectionTitle}>PRICE ALERT THRESHOLD</div>
                                <div style={s.card}>
                                    <div style={{
                                        display: 'flex', alignItems: 'center', gap: '12px',
                                    }}>
                                        <span style={s.label}>Alert when move exceeds</span>
                                        <input
                                            type="number"
                                            value={pushPreferences.price_alert_threshold}
                                            onChange={(e) => handleThresholdChange(e.target.value)}
                                            min="0.1" max="50" step="0.5"
                                            style={{
                                                ...s.inputField,
                                                width: '70px', marginBottom: 0,
                                                textAlign: 'center',
                                            }}
                                        />
                                        <span style={s.label}>%</span>
                                    </div>
                                    <div style={s.note}>
                                        You will receive a push notification when any watched ticker
                                        moves more than this percentage in a single session.
                                    </div>
                                </div>
                            </>
                        )}
                    </>
                )}
            </div>
        );
    };

    const renderAccount = () => (
        <div style={s.section}>
            <div style={s.sectionTitle}>ACCOUNT</div>
            <div style={s.card}>
                <div style={s.row}>
                    <span style={s.label}>Logged in as</span>
                    <span style={s.value}>{username}</span>
                </div>
                <div style={s.row}>
                    <span style={s.label}>Role</span>
                    <span style={{
                        ...s.value,
                        color: isAdmin ? colors.green : colors.accent,
                    }}>{userRole?.toUpperCase()}</span>
                </div>
            </div>

            <div style={s.sectionTitle}>ABOUT</div>
            <div style={s.card}>
                <div style={s.row}>
                    <span style={s.label}>Version</span>
                    <span style={s.value}>1.0.0</span>
                </div>
                <div style={s.row}>
                    <span style={s.label}>Uptime</span>
                    <span style={s.value}>
                        {formatUptime(systemStatus?.uptime_seconds)}
                    </span>
                </div>
            </div>

            {/* User Management (admin only) */}
            {isAdmin && (
                <>
                    <div style={s.sectionTitle}>USER MANAGEMENT</div>
                    <div style={s.card}>
                        {users.map(u => (
                            <div key={u.username} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '8px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
                            }}>
                                <div>
                                    <div style={{ fontSize: '14px', fontFamily: "'JetBrains Mono', monospace", color: colors.text }}>
                                        {u.username}
                                    </div>
                                    <div style={{ fontSize: '11px', color: u.role === 'admin' ? colors.green : colors.accent }}>
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
                            <div style={{ color: colors.textMuted, fontSize: '13px', padding: '8px 0' }}>
                                No user accounts yet (only master password)
                            </div>
                        )}

                        {!showAddUser ? (
                            <button onClick={() => setShowAddUser(true)} style={{
                                ...s.btn, width: '100%', marginTop: '12px',
                                background: 'transparent', border: `1px solid ${colors.green}44`, color: colors.green,
                            }}>+ ADD USER</button>
                        ) : (
                            <div style={{ marginTop: '12px', padding: '12px', background: colors.bg, borderRadius: tokens.radius.sm }}>
                                <input
                                    type="text" placeholder="Username" value={newUser.username}
                                    onChange={e => setNewUser({ ...newUser, username: e.target.value })}
                                    style={s.inputField}
                                />
                                <input
                                    type="password" placeholder="Password (8+ chars)" value={newUser.password}
                                    onChange={e => setNewUser({ ...newUser, password: e.target.value })}
                                    style={s.inputField}
                                />
                                <div style={{ display: 'flex', gap: '8px', marginBottom: '8px' }}>
                                    {['contributor', 'admin'].map(r => (
                                        <button key={r} onClick={() => setNewUser({ ...newUser, role: r })} style={{
                                            flex: 1, padding: '8px', borderRadius: '6px', border: 'none', cursor: 'pointer',
                                            fontFamily: "'JetBrains Mono', monospace", fontSize: '12px',
                                            background: newUser.role === r ? (r === 'admin' ? colors.green : colors.accent) : colors.border,
                                            color: newUser.role === r ? '#fff' : colors.textMuted,
                                        }}>{r.toUpperCase()}</button>
                                    ))}
                                </div>
                                <div style={{ display: 'flex', gap: '8px' }}>
                                    <button onClick={handleCreateUser} style={{
                                        flex: 1, ...s.btn, background: colors.green,
                                    }}>CREATE</button>
                                    <button onClick={() => setShowAddUser(false)} style={{
                                        flex: 1, ...s.btn, background: 'transparent', border: `1px solid ${colors.border}`, color: colors.textMuted,
                                    }}>CANCEL</button>
                                </div>
                            </div>
                        )}
                    </div>
                </>
            )}

            <div style={{ marginTop: '20px' }}>
                <button style={s.logoutBtn} onClick={onLogout}>
                    LOG OUT
                </button>
            </div>
        </div>
    );

    const renderTab = () => {
        switch (tab) {
            case 'Status': return renderStatus();
            case 'API Keys': return renderApiKeys();
            case 'Hermes': return renderHermes();
            case 'Coverage': return renderCoverage();
            case 'Notifications': return renderNotifications();
            case 'Account': return renderAccount();
            default: return renderStatus();
        }
    };

    return (
        <div style={s.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={s.title}>SETTINGS</div>
                <ViewHelp id="settings" />
            </div>

            <div style={s.tabs}>
                {TAB_NAMES.map(name => (
                    <button key={name} style={s.tab(tab === name)} onClick={() => setTab(name)}>
                        {name}
                    </button>
                ))}
            </div>

            {renderTab()}
        </div>
    );
}
