import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import {
    Search, CheckCircle, XCircle, Archive,
    RefreshCw, FileText, Activity
} from 'lucide-react';

const DOMAINS = ['pipeline', 'tools', 'alpha', 'intel', 'grid'];
const STATUSES = ['inbox', 'evaluating', 'approved', 'rejected', 'active'];

const PRIORITY_COLORS = {
    urgent: '#ef4444',
    high: '#f59e0b',
    medium: '#6b7280',
    low: '#374151',
};

const STATUS_ICONS = {
    inbox: FileText,
    evaluating: Activity,
    approved: CheckCircle,
    rejected: XCircle,
    active: CheckCircle,
};

export default function Vault() {
    const [domain, setDomain] = useState('');
    const [status, setStatus] = useState('');
    const [notes, setNotes] = useState([]);
    const [total, setTotal] = useState(0);
    const [searchQuery, setSearchQuery] = useState('');
    const [searchResults, setSearchResults] = useState(null);
    const [dashboard, setDashboard] = useState(null);
    const [selectedNote, setSelectedNote] = useState(null);
    const [actions, setActions] = useState([]);
    const [loading, setLoading] = useState(false);
    const [syncing, setSyncing] = useState(false);

    const loadNotes = useCallback(async () => {
        setLoading(true);
        try {
            const params = {};
            if (domain) params.domain = domain;
            if (status) params.status = status;
            const data = await api.vaultNotes(params);
            setNotes(data.notes || []);
            setTotal(data.total || 0);
        } catch (e) {
            console.error('Failed to load notes:', e);
        }
        setLoading(false);
    }, [domain, status]);

    const loadDashboard = useCallback(async () => {
        try {
            const data = await api.vaultDashboard();
            setDashboard(data);
        } catch (e) {
            console.error('Failed to load dashboard:', e);
        }
    }, []);

    useEffect(() => { loadNotes(); loadDashboard(); }, [loadNotes, loadDashboard]);

    const handleSearch = async () => {
        if (!searchQuery.trim()) { setSearchResults(null); return; }
        const data = await api.vaultSearch(searchQuery, domain);
        setSearchResults(data.results || []);
    };

    const handleStatusChange = async (noteId, newStatus) => {
        await api.vaultChangeStatus(noteId, newStatus);
        loadNotes();
        loadDashboard();
    };

    const handleSync = async () => {
        setSyncing(true);
        await api.vaultSync();
        await loadNotes();
        await loadDashboard();
        setSyncing(false);
    };

    const selectNote = async (note) => {
        setSelectedNote(note);
        const acts = await api.vaultActions(note.id);
        setActions(acts || []);
    };

    const displayNotes = searchResults || notes;

    return (
        <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto' }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
                <h1 style={{ fontSize: 24, fontWeight: 700, margin: 0 }}>Intelligence Vault</h1>
                <button
                    onClick={handleSync}
                    disabled={syncing}
                    style={{
                        display: 'flex', alignItems: 'center', gap: 8, padding: '8px 16px',
                        background: '#1a1a2e', border: '1px solid #333', borderRadius: 8,
                        color: '#e0e0e0', cursor: syncing ? 'wait' : 'pointer', fontSize: 13,
                    }}
                >
                    <RefreshCw size={16} style={syncing ? { animation: 'spin 1s linear infinite' } : {}} />
                    {syncing ? 'Syncing...' : 'Sync Now'}
                </button>
            </div>

            {/* Review items */}
            {dashboard?.review_items?.length > 0 && (
                <div style={{
                    background: '#1a1a2e', border: '1px solid #333', borderRadius: 12,
                    padding: 16, marginBottom: 24,
                }}>
                    <h3 style={{ fontSize: 13, color: '#888', marginBottom: 12, marginTop: 0, textTransform: 'uppercase', letterSpacing: 1 }}>Needs Your Review</h3>
                    {dashboard.review_items.map((item, i) => (
                        <div key={i} style={{
                            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                            padding: '8px 0', borderBottom: i < dashboard.review_items.length - 1 ? '1px solid #222' : 'none',
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                                <span style={{
                                    fontSize: 10, fontWeight: 700, padding: '2px 6px', borderRadius: 4,
                                    background: (PRIORITY_COLORS[item.priority] || '#6b7280') + '22',
                                    color: PRIORITY_COLORS[item.priority] || '#6b7280',
                                    textTransform: 'uppercase',
                                }}>
                                    {item.priority}
                                </span>
                                <span style={{ color: '#666', fontSize: 12 }}>{item.domain}</span>
                                <span style={{ color: '#e0e0e0' }}>{item.title}</span>
                            </div>
                            <div style={{ display: 'flex', gap: 6 }}>
                                <button onClick={() => handleStatusChange(item.id, 'approved')}
                                    style={{ background: '#16a34a22', border: '1px solid #16a34a', borderRadius: 6, padding: '4px 12px', color: '#16a34a', cursor: 'pointer', fontSize: 12 }}>
                                    Approve
                                </button>
                                <button onClick={() => handleStatusChange(item.id, 'rejected')}
                                    style={{ background: '#ef444422', border: '1px solid #ef4444', borderRadius: 6, padding: '4px 12px', color: '#ef4444', cursor: 'pointer', fontSize: 12 }}>
                                    Reject
                                </button>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Search + Filters */}
            <div style={{ display: 'flex', gap: 12, marginBottom: 16, flexWrap: 'wrap' }}>
                <div style={{ display: 'flex', flex: 1, minWidth: 200 }}>
                    <input
                        type="text" placeholder="Search vault..."
                        value={searchQuery}
                        onChange={e => setSearchQuery(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && handleSearch()}
                        style={{
                            flex: 1, padding: '8px 12px', background: '#111', border: '1px solid #333',
                            borderRadius: '8px 0 0 8px', color: '#e0e0e0', outline: 'none', fontSize: 13,
                        }}
                    />
                    <button onClick={handleSearch} style={{
                        padding: '8px 12px', background: '#1a1a2e', border: '1px solid #333',
                        borderLeft: 'none', borderRadius: '0 8px 8px 0', color: '#e0e0e0', cursor: 'pointer',
                    }}>
                        <Search size={16} />
                    </button>
                </div>
                <select value={domain} onChange={e => { setDomain(e.target.value); setSearchResults(null); }}
                    style={{ padding: '8px 12px', background: '#111', border: '1px solid #333', borderRadius: 8, color: '#e0e0e0', fontSize: 13 }}>
                    <option value="">All Domains</option>
                    {DOMAINS.map(d => <option key={d} value={d}>{d}</option>)}
                </select>
                <select value={status} onChange={e => { setStatus(e.target.value); setSearchResults(null); }}
                    style={{ padding: '8px 12px', background: '#111', border: '1px solid #333', borderRadius: 8, color: '#e0e0e0', fontSize: 13 }}>
                    <option value="">All Statuses</option>
                    {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
            </div>

            {/* Notes list + Detail panel */}
            <div style={{ display: 'grid', gridTemplateColumns: selectedNote ? '1fr 1fr' : '1fr', gap: 16 }}>
                <div>
                    <div style={{ fontSize: 12, color: '#666', marginBottom: 8 }}>
                        {searchResults ? `${searchResults.length} search results` : `${total} notes`}
                    </div>
                    {displayNotes.map(note => {
                        const Icon = STATUS_ICONS[note.status] || FileText;
                        const isSelected = selectedNote?.id === note.id;
                        return (
                            <div key={note.id} onClick={() => selectNote(note)}
                                style={{
                                    padding: 12, background: isSelected ? '#1a1a3e' : '#111',
                                    border: `1px solid ${isSelected ? '#444' : '#222'}`, borderRadius: 8,
                                    marginBottom: 8, cursor: 'pointer', transition: 'background 0.15s',
                                }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                                    <Icon size={14} style={{ color: note.status === 'approved' ? '#16a34a' : note.status === 'rejected' ? '#ef4444' : '#6b7280' }} />
                                    <span style={{ fontWeight: 600, color: '#e0e0e0' }}>{note.title}</span>
                                    <span style={{ fontSize: 11, color: '#555', marginLeft: 'auto' }}>{note.domain}</span>
                                </div>
                                <div style={{ fontSize: 12, color: '#555' }}>
                                    {note.vault_path} &middot; {note.status}
                                </div>
                            </div>
                        );
                    })}
                    {displayNotes.length === 0 && !loading && (
                        <div style={{ color: '#555', fontSize: 13, padding: 24, textAlign: 'center' }}>No notes found</div>
                    )}
                </div>

                {selectedNote && (
                    <div style={{ background: '#111', border: '1px solid #222', borderRadius: 12, padding: 20, maxHeight: '80vh', overflow: 'auto' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                            <h2 style={{ fontSize: 18, fontWeight: 700, margin: 0, color: '#e0e0e0' }}>{selectedNote.title}</h2>
                            <div style={{ display: 'flex', gap: 6 }}>
                                {selectedNote.status !== 'approved' && (
                                    <button onClick={() => handleStatusChange(selectedNote.id, 'approved')}
                                        style={{ background: '#16a34a22', border: '1px solid #16a34a', borderRadius: 6, padding: '4px 12px', color: '#16a34a', cursor: 'pointer', fontSize: 12 }}>Approve</button>
                                )}
                                {selectedNote.status !== 'rejected' && (
                                    <button onClick={() => handleStatusChange(selectedNote.id, 'rejected')}
                                        style={{ background: '#ef444422', border: '1px solid #ef4444', borderRadius: 6, padding: '4px 12px', color: '#ef4444', cursor: 'pointer', fontSize: 12 }}>Reject</button>
                                )}
                                <button onClick={() => handleStatusChange(selectedNote.id, 'archived')}
                                    style={{ background: '#22222266', border: '1px solid #333', borderRadius: 6, padding: '4px 12px', color: '#888', cursor: 'pointer', fontSize: 12 }}>Archive</button>
                            </div>
                        </div>

                        <div style={{ fontSize: 12, color: '#555', marginBottom: 16, display: 'flex', gap: 16 }}>
                            <span style={{ background: '#1a1a2e', padding: '2px 8px', borderRadius: 4 }}>{selectedNote.domain}</span>
                            <span style={{ background: '#1a1a2e', padding: '2px 8px', borderRadius: 4 }}>{selectedNote.status}</span>
                            <span style={{ color: '#444' }}>{selectedNote.vault_path}</span>
                        </div>

                        <pre style={{ whiteSpace: 'pre-wrap', fontFamily: 'inherit', fontSize: 14, color: '#ccc', lineHeight: 1.7, margin: 0 }}>
                            {selectedNote.body}
                        </pre>

                        {actions.length > 0 && (
                            <div style={{ marginTop: 24, borderTop: '1px solid #222', paddingTop: 16 }}>
                                <h3 style={{ fontSize: 13, color: '#666', marginBottom: 8, marginTop: 0, textTransform: 'uppercase', letterSpacing: 1 }}>Activity Log</h3>
                                {actions.map((a, i) => (
                                    <div key={i} style={{ fontSize: 12, color: '#555', padding: '4px 0', display: 'flex', gap: 8 }}>
                                        <span style={{ color: '#888', minWidth: 50 }}>{a.actor}</span>
                                        <span style={{ color: '#aaa' }}>{a.action}</span>
                                        {a.detail?.reason && <span style={{ color: '#666' }}>— {a.detail.reason}</span>}
                                        <span style={{ marginLeft: 'auto', color: '#333', fontSize: 11 }}>{new Date(a.created_at).toLocaleString()}</span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
