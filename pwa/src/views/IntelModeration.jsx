/**
 * IntelModeration — admin-only queue for user-submitted intel.
 *
 * Route: #/intel-mod
 * Access: admin role required.
 *
 * Lists all pending user_intel submissions with verify / reject / flag
 * buttons. After an action, the row is removed from the queue and the
 * next pending item shows up automatically.
 */
import React, { useEffect, useState, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const styles = {
    page: {
        maxWidth: '1000px', margin: '0 auto', padding: '24px 16px',
        fontFamily: "'IBM Plex Sans', -apple-system, sans-serif",
        color: '#C8D8E8',
    },
    header: {
        fontFamily: mono, fontSize: '22px', fontWeight: 700,
        letterSpacing: '2px', textTransform: 'uppercase',
        color: '#1A6EBF', marginBottom: '6px',
    },
    sub: {
        fontFamily: mono, fontSize: '11px', color: '#5C7B9C',
        marginBottom: '20px', letterSpacing: '0.5px',
    },
    row: {
        border: '1px solid #1A2840', borderRadius: '6px',
        padding: '14px 16px', marginBottom: '10px',
        background: '#0D1520',
    },
    topBar: {
        display: 'flex', alignItems: 'center', gap: '10px',
        marginBottom: '10px', flexWrap: 'wrap',
    },
    badge: (bg, fg) => ({
        padding: '2px 7px', borderRadius: '3px', fontSize: '9px',
        fontFamily: mono, fontWeight: 700, letterSpacing: '0.5px',
        background: bg, color: fg, textTransform: 'uppercase',
    }),
    note: {
        fontSize: '13px', lineHeight: 1.5, color: '#C8D8E8', marginBottom: '8px',
    },
    meta: {
        fontFamily: mono, fontSize: '10px', color: '#5C7B9C',
        marginBottom: '10px',
    },
    actions: {
        display: 'flex', gap: '8px',
    },
    btn: (color) => ({
        padding: '6px 14px', borderRadius: '3px', border: 'none',
        background: color, color: '#fff',
        fontFamily: mono, fontSize: '10px', fontWeight: 700,
        letterSpacing: '1px', textTransform: 'uppercase', cursor: 'pointer',
    }),
    empty: {
        textAlign: 'center', padding: '60px 20px',
        fontFamily: mono, fontSize: '12px', color: '#5C7B9C',
    },
    denied: {
        padding: '60px 20px', textAlign: 'center',
        fontFamily: mono, color: '#8B1F1F', fontSize: '14px',
    },
};

const INTEL_COLOR = {
    biography: '#06B6D4', connection: '#1A6EBF', loyalty: '#A855F7',
    stance: '#F59E0B', rumor: '#FBBF24', tip: '#10B981', fact: '#10B981',
};

export default function IntelModeration() {
    const userRole = useStore(s => s.userRole);
    const [items, setItems] = useState(null);
    const [loading, setLoading] = useState(false);
    const [busyId, setBusyId] = useState(null);
    const [msg, setMsg] = useState(null);

    const load = useCallback(async () => {
        setLoading(true);
        const res = await api.listPendingIntel(200);
        setLoading(false);
        if (!res || res.error) {
            setItems([]);
            if (res?.status === 403) {
                setMsg({ type: 'error', text: 'Admin role required.' });
            }
            return;
        }
        setItems(Array.isArray(res) ? res : (res.data || []));
    }, []);

    useEffect(() => { load(); }, [load]);

    const act = async (id, action) => {
        setBusyId(id);
        const res = await api.verifyIntel(id, action);
        setBusyId(null);
        if (res && !res.error) {
            setItems(prev => (prev || []).filter(it => it.id !== id));
            setMsg({ type: 'success', text: `Intel #${id} ${action}.` });
            setTimeout(() => setMsg(null), 2500);
        } else {
            setMsg({ type: 'error', text: res?.message || 'Action failed' });
            setTimeout(() => setMsg(null), 3000);
        }
    };

    if (userRole !== 'admin') {
        return (
            <div style={styles.page}>
                <div style={styles.denied}>
                    ACCESS DENIED — admin role required.
                </div>
            </div>
        );
    }

    return (
        <div style={styles.page}>
            <div style={styles.header}>INTEL MODERATION</div>
            <div style={styles.sub}>
                Review pending user-submitted intel. Verified items boost trust;
                rejected items are hidden from public views.
            </div>

            {msg && (
                <div style={{
                    padding: '10px 14px', borderRadius: '4px', marginBottom: '14px',
                    background: msg.type === 'error' ? '#8B1F1F30' : '#1A7A4A30',
                    border: `1px solid ${msg.type === 'error' ? '#8B1F1F' : '#1A7A4A'}`,
                    color: msg.type === 'error' ? '#FF6B6B' : '#10B981',
                    fontFamily: mono, fontSize: '11px',
                }}>{msg.text}</div>
            )}

            {loading && !items && (
                <div style={styles.empty}>Loading pending queue…</div>
            )}

            {items && items.length === 0 && !loading && (
                <div style={styles.empty}>
                    No pending intel. The tentacles are quiet.
                </div>
            )}

            {(items || []).map(it => (
                <div key={it.id} style={styles.row}>
                    <div style={styles.topBar}>
                        <span style={styles.badge(`${INTEL_COLOR[it.intel_type] || '#1A6EBF'}30`, INTEL_COLOR[it.intel_type] || '#1A6EBF')}>
                            {it.intel_type}
                        </span>
                        <span style={styles.badge('#1A284060', '#C8D8E8')}>
                            {it.actor_id}
                        </span>
                        {it.confidence && (
                            <span style={styles.badge('#5C7B9C30', '#5C7B9C')}>
                                conf: {it.confidence}
                            </span>
                        )}
                        <div style={{ flex: 1 }} />
                        <span style={{ fontFamily: mono, fontSize: '10px', color: '#5C7B9C' }}>
                            ▲{it.upvotes ?? 0} ▼{it.downvotes ?? 0} ⚑{it.flags ?? 0}
                        </span>
                    </div>
                    <div style={styles.note}>{it.note}</div>
                    <div style={styles.meta}>
                        by {it.submitted_by} • {it.submitted_at ? String(it.submitted_at).slice(0, 19).replace('T', ' ') : ''}
                        {it.source_url && (
                            <> • <a href={it.source_url} target="_blank" rel="noopener noreferrer" style={{ color: '#1A6EBF' }}>
                                source ↗
                            </a></>
                        )}
                    </div>
                    <div style={styles.actions}>
                        <button
                            disabled={busyId === it.id}
                            onClick={() => act(it.id, 'verified')}
                            style={styles.btn('#1A7A4A')}
                        >✓ Verify</button>
                        <button
                            disabled={busyId === it.id}
                            onClick={() => act(it.id, 'rejected')}
                            style={styles.btn('#8B1F1F')}
                        >✗ Reject</button>
                    </div>
                </div>
            ))}
        </div>
    );
}
