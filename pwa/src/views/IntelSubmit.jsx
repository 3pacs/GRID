/**
 * IntelSubmit — public standalone form for submitting intel about any actor.
 *
 * Route: #/intel/submit
 * Auth: any authenticated user.
 *
 * Users can search for an actor via the sector_map autocomplete, choose
 * intel type, write a note, add a source URL, and submit. Acts as the
 * cooperative tentacle entry point outside the actor drawer flow.
 */
import React, { useEffect, useState, useMemo } from 'react';
import { api } from '../api.js';

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const INTEL_TYPES = [
    { v: 'fact', label: 'Fact — verifiable assertion' },
    { v: 'biography', label: 'Biography — background or history' },
    { v: 'connection', label: 'Connection — relationship to another actor' },
    { v: 'loyalty', label: 'Loyalty — declared or inferred allegiance' },
    { v: 'stance', label: 'Stance — position on a policy/issue' },
    { v: 'tip', label: 'Tip — actionable information' },
    { v: 'rumor', label: 'Rumor — unverified signal worth tracking' },
];

const styles = {
    page: {
        maxWidth: '720px', margin: '0 auto', padding: '24px 16px',
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
        marginBottom: '24px', letterSpacing: '0.5px', lineHeight: 1.5,
    },
    card: {
        border: '1px solid #1A2840', borderRadius: '8px',
        padding: '20px', background: '#0D1520',
    },
    label: {
        fontFamily: mono, fontSize: '10px', fontWeight: 700,
        letterSpacing: '1px', textTransform: 'uppercase',
        color: '#5C7B9C', marginBottom: '6px', display: 'block',
    },
    input: {
        width: '100%', padding: '10px 12px',
        background: '#080C10', color: '#C8D8E8',
        border: '1px solid #1A2840', borderRadius: '4px',
        fontSize: '13px', fontFamily: mono,
        marginBottom: '16px', boxSizing: 'border-box',
    },
    textarea: {
        width: '100%', padding: '10px 12px',
        background: '#080C10', color: '#C8D8E8',
        border: '1px solid #1A2840', borderRadius: '4px',
        fontSize: '13px', fontFamily: mono,
        marginBottom: '16px', boxSizing: 'border-box',
        resize: 'vertical', minHeight: '100px',
    },
    select: {
        width: '100%', padding: '10px 12px',
        background: '#080C10', color: '#C8D8E8',
        border: '1px solid #1A2840', borderRadius: '4px',
        fontSize: '13px', fontFamily: mono,
        marginBottom: '16px', boxSizing: 'border-box',
    },
    btn: {
        padding: '12px 28px', borderRadius: '4px', border: 'none',
        background: '#1A6EBF', color: '#fff',
        fontFamily: mono, fontSize: '11px', fontWeight: 700,
        letterSpacing: '1.5px', textTransform: 'uppercase', cursor: 'pointer',
    },
    suggestions: {
        border: '1px solid #1A2840', borderRadius: '4px',
        background: '#080C10', maxHeight: '200px', overflowY: 'auto',
        marginTop: '-12px', marginBottom: '16px',
    },
    sugRow: {
        padding: '8px 12px', cursor: 'pointer',
        fontSize: '12px', fontFamily: mono, color: '#C8D8E8',
        borderBottom: '1px solid #1A284060',
    },
    status: (type) => ({
        padding: '10px 14px', borderRadius: '4px', marginBottom: '16px',
        background: type === 'error' ? '#8B1F1F30' : '#1A7A4A30',
        border: `1px solid ${type === 'error' ? '#8B1F1F' : '#1A7A4A'}`,
        color: type === 'error' ? '#FF6B6B' : '#10B981',
        fontFamily: mono, fontSize: '11px',
    }),
};

export default function IntelSubmit() {
    const [actorList, setActorList] = useState([]);
    const [actorInput, setActorInput] = useState('');
    const [selectedActor, setSelectedActor] = useState(null);
    const [showSug, setShowSug] = useState(false);

    const [form, setForm] = useState({
        intel_type: 'fact',
        note: '',
        source_url: '',
        confidence: 'medium',
    });
    const [status, setStatus] = useState(null);
    const [submitting, setSubmitting] = useState(false);

    // Load actor list from sector_map once on mount.
    useEffect(() => {
        (async () => {
            const res = await api.getActorNetwork();
            if (!res || res.error) {
                setStatus({ type: 'error', text: res?.message || 'Unable to load tracked actors.' });
                return;
            }
            const actors = [];
            const seen = new Set();
            const walk = (node) => {
                if (!node) return;
                if (Array.isArray(node)) { node.forEach(walk); return; }
                if (typeof node === 'object') {
                    if (node.id && !seen.has(node.id)) {
                        seen.add(node.id);
                        actors.push({
                            id: node.id,
                            label: node.label || node.name || node.id,
                            type: node.type || '',
                        });
                    }
                    Object.values(node).forEach(walk);
                }
            };
            walk(res.nodes || res.data || res);
            setActorList(actors);
        })();
    }, []);

    const suggestions = useMemo(() => {
        const q = actorInput.trim().toLowerCase();
        if (!q || q.length < 2) return [];
        return actorList
            .filter(a => (
                a.id.toLowerCase().includes(q) ||
                a.label.toLowerCase().includes(q)
            ))
            .slice(0, 12);
    }, [actorInput, actorList]);

    const onSubmit = async () => {
        setStatus(null);
        if (!selectedActor) {
            setStatus({ type: 'error', text: 'Pick an actor from the suggestions.' });
            return;
        }
        if (!form.note.trim()) {
            setStatus({ type: 'error', text: 'Note is required.' });
            return;
        }
        setSubmitting(true);
        const res = await api.submitIntel(selectedActor.id, form);
        setSubmitting(false);
        if (res && !res.error) {
            setStatus({
                type: 'success',
                text: `Intel #${res.id} submitted for ${selectedActor.label}. Awaiting review.`,
            });
            setForm({ intel_type: 'fact', note: '', source_url: '', confidence: 'medium' });
            setActorInput('');
            setSelectedActor(null);
        } else {
            setStatus({ type: 'error', text: res?.message || 'Submission failed.' });
        }
    };

    return (
        <div style={styles.page}>
            <div style={styles.header}>CONTRIBUTE INTEL</div>
            <div style={styles.sub}>
                Users act as tentacles. Submit facts, connections, loyalties, stances,
                tips, or rumors about any tracked actor. Verified contributions boost
                trust scoring across the platform.
            </div>

            <div style={styles.card}>
                {status && <div style={styles.status(status.type)}>{status.text}</div>}

                <label style={styles.label}>Actor *</label>
                <input
                    type="text"
                    placeholder="Start typing actor name or id…"
                    value={actorInput}
                    onChange={e => {
                        setActorInput(e.target.value);
                        setSelectedActor(null);
                        setShowSug(true);
                    }}
                    onFocus={() => setShowSug(true)}
                    style={styles.input}
                />
                {showSug && suggestions.length > 0 && !selectedActor && (
                    <div style={styles.suggestions}>
                        {suggestions.map(s => (
                            <div
                                key={s.id}
                                onClick={() => {
                                    setSelectedActor(s);
                                    setActorInput(s.label);
                                    setShowSug(false);
                                }}
                                style={styles.sugRow}
                            >
                                <strong>{s.label}</strong>
                                <span style={{ color: '#5C7B9C', marginLeft: '8px' }}>
                                    {s.id}
                                    {s.type && ` • ${s.type}`}
                                </span>
                            </div>
                        ))}
                    </div>
                )}

                <label style={styles.label}>Intel type *</label>
                <select
                    value={form.intel_type}
                    onChange={e => setForm(f => ({ ...f, intel_type: e.target.value }))}
                    style={styles.select}
                >
                    {INTEL_TYPES.map(t => (
                        <option key={t.v} value={t.v}>{t.label}</option>
                    ))}
                </select>

                <label style={styles.label}>Note *</label>
                <textarea
                    placeholder="What do you know about this actor?"
                    value={form.note}
                    onChange={e => setForm(f => ({ ...f, note: e.target.value }))}
                    style={styles.textarea}
                />

                <label style={styles.label}>Source URL (optional)</label>
                <input
                    type="url"
                    placeholder="https://…"
                    value={form.source_url}
                    onChange={e => setForm(f => ({ ...f, source_url: e.target.value }))}
                    style={styles.input}
                />

                <label style={styles.label}>Your confidence</label>
                <select
                    value={form.confidence}
                    onChange={e => setForm(f => ({ ...f, confidence: e.target.value }))}
                    style={styles.select}
                >
                    <option value="high">High — I have direct knowledge</option>
                    <option value="medium">Medium — credible second-hand</option>
                    <option value="low">Low — speculation / rumor</option>
                </select>

                <button
                    disabled={submitting}
                    onClick={onSubmit}
                    style={{
                        ...styles.btn,
                        opacity: submitting ? 0.6 : 1,
                        cursor: submitting ? 'wait' : 'pointer',
                    }}
                >
                    {submitting ? 'Submitting…' : 'Submit Intel'}
                </button>
            </div>
        </div>
    );
}
