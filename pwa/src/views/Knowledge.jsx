import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

const CATEGORIES = ['all', 'regime', 'macro', 'technical', 'sentiment', 'risk', 'general'];

const categoryColors = {
    regime: '#6B21A8',
    macro: '#1A6EBF',
    technical: '#0D7377',
    sentiment: '#8A6000',
    risk: '#8B1F1F',
    general: '#3A4A5A',
};

const styles = {
    searchRow: {
        display: 'flex', gap: '8px', marginBottom: '12px',
    },
    searchInput: {
        ...shared.input,
        flex: 1,
    },
    chips: {
        display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '16px',
        overflowX: 'auto',
    },
    chip: (active) => ({
        padding: '5px 12px', borderRadius: '14px', fontSize: '12px',
        fontWeight: 600, cursor: 'pointer', border: 'none',
        fontFamily: colors.sans, whiteSpace: 'nowrap',
        background: active ? colors.accent : colors.card,
        color: active ? '#fff' : colors.textMuted,
    }),
    tag: {
        display: 'inline-block', padding: '2px 8px', borderRadius: '4px',
        fontSize: '10px', fontWeight: 600, marginRight: '4px', marginTop: '4px',
        background: '#1A2840', color: colors.textDim,
    },
    question: {
        fontSize: '14px', fontWeight: 600, color: '#E8F0F8',
        marginBottom: '6px', lineHeight: '1.5', wordBreak: 'break-word',
    },
    answer: {
        fontSize: '13px', color: colors.textDim, lineHeight: '1.6',
        whiteSpace: 'pre-wrap', maxHeight: '120px', overflow: 'hidden',
        position: 'relative',
        maskImage: 'linear-gradient(to bottom, black 60%, transparent 100%)',
        WebkitMaskImage: 'linear-gradient(to bottom, black 60%, transparent 100%)',
    },
    answerFull: {
        fontSize: '13px', color: colors.textDim, lineHeight: '1.6',
        whiteSpace: 'pre-wrap',
    },
    meta: {
        display: 'flex', gap: '10px', alignItems: 'center',
        marginTop: '8px', flexWrap: 'wrap',
    },
    metaText: {
        fontSize: '11px', color: colors.textMuted,
    },
    confidenceBar: (val) => ({
        width: '60px', height: '4px', borderRadius: '2px',
        background: colors.border, position: 'relative', display: 'inline-block',
    }),
    confidenceFill: (val) => ({
        width: `${Math.round((val || 0) * 100)}%`, height: '100%',
        borderRadius: '2px', position: 'absolute', top: 0, left: 0,
        background: val > 0.7 ? colors.green : val > 0.4 ? colors.yellow : colors.red,
    }),
    backButton: {
        background: 'none', border: 'none', cursor: 'pointer',
        color: colors.accent, fontSize: '13px', fontWeight: 600,
        padding: '4px 0', marginBottom: '12px', fontFamily: colors.sans,
    },
    relatedEntry: {
        padding: '10px', cursor: 'pointer', borderRadius: '6px',
        background: colors.bg, marginBottom: '6px',
    },
    pagination: {
        display: 'flex', justifyContent: 'center', gap: '8px',
        marginTop: '16px',
    },
};

function ConfidenceBar({ value }) {
    return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
            <span style={styles.confidenceBar(value)}>
                <span style={styles.confidenceFill(value)} />
            </span>
            <span style={styles.metaText}>{Math.round((value || 0) * 100)}%</span>
        </span>
    );
}

function StatsBar({ summary }) {
    if (!summary) return null;
    return (
        <div style={shared.card}>
            <div style={shared.metricGrid}>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{summary.total || 0}</div>
                    <div style={shared.metricLabel}>Total Entries</div>
                </div>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{summary.this_week || 0}</div>
                    <div style={shared.metricLabel}>This Week</div>
                </div>
                {(summary.categories || []).slice(0, 3).map(c => (
                    <div key={c.category} style={shared.metric}>
                        <div style={shared.metricValue}>{c.count}</div>
                        <div style={shared.metricLabel}>{(c.category || 'general').toUpperCase()}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}

function EntryDetail({ entry, related, onBack, onSelect }) {
    if (!entry) return null;
    return (
        <div>
            <button style={styles.backButton} onClick={onBack}>
                &larr; Back to list
            </button>

            <div style={shared.card}>
                <div style={{ display: 'flex', gap: '8px', marginBottom: '10px', alignItems: 'center' }}>
                    <span style={shared.badge(categoryColors[entry.category] || '#3A4A5A')}>
                        {(entry.category || 'general').toUpperCase()}
                    </span>
                    <ConfidenceBar value={entry.confidence} />
                    <span style={styles.metaText}>{entry.source_model}</span>
                </div>

                <div style={styles.question}>{entry.question}</div>
                <div style={styles.answerFull}>{entry.answer}</div>

                <div style={styles.meta}>
                    <span style={styles.metaText}>
                        {entry.created_at ? new Date(entry.created_at).toLocaleString() : ''}
                    </span>
                    <span style={styles.metaText}>by {entry.created_by || 'operator'}</span>
                </div>

                {(entry.tags || []).length > 0 && (
                    <div style={{ marginTop: '8px' }}>
                        {entry.tags.map((t, i) => (
                            <span key={i} style={styles.tag}>{t}</span>
                        ))}
                    </div>
                )}

                {(entry.referenced_tickers || []).length > 0 && (
                    <div style={{ marginTop: '6px' }}>
                        <span style={styles.metaText}>Tickers: </span>
                        {entry.referenced_tickers.map((t, i) => (
                            <span key={i} style={{ ...styles.tag, background: '#1A3A5A' }}>${t}</span>
                        ))}
                    </div>
                )}

                {(entry.referenced_features || []).length > 0 && (
                    <div style={{ marginTop: '6px' }}>
                        <span style={styles.metaText}>Features: </span>
                        {entry.referenced_features.map((f, i) => (
                            <span key={i} style={{ ...styles.tag, background: '#0D3320' }}>{f}</span>
                        ))}
                    </div>
                )}
            </div>

            {related && related.length > 0 && (
                <>
                    <div style={shared.sectionTitle}>Related Q&As</div>
                    <div style={shared.card}>
                        {related.map(r => (
                            <div
                                key={r.id}
                                style={styles.relatedEntry}
                                onClick={() => onSelect(r.id)}
                            >
                                <span style={{
                                    ...shared.badge(categoryColors[r.category] || '#3A4A5A'),
                                    fontSize: '10px', marginRight: '8px',
                                }}>
                                    {(r.category || '').toUpperCase()}
                                </span>
                                <span style={{ fontSize: '13px', color: colors.text }}>
                                    {r.question}
                                </span>
                            </div>
                        ))}
                    </div>
                </>
            )}
        </div>
    );
}

export default function Knowledge() {
    const [query, setQuery] = useState('');
    const [category, setCategory] = useState('all');
    const [entries, setEntries] = useState([]);
    const [total, setTotal] = useState(0);
    const [offset, setOffset] = useState(0);
    const [summary, setSummary] = useState(null);
    const [loading, setLoading] = useState(false);
    const [selectedEntry, setSelectedEntry] = useState(null);
    const [selectedRelated, setSelectedRelated] = useState([]);
    const limit = 20;

    useEffect(() => {
        loadSummary();
        loadEntries();
    }, []);

    useEffect(() => {
        loadEntries();
    }, [category, offset]);

    const loadSummary = async () => {
        try {
            const result = await api._fetch('/api/v1/knowledge/summary');
            setSummary(result);
        } catch (e) {
            console.warn('[GRID] Knowledge summary:', e.message);
        }
    };

    const loadEntries = async () => {
        setLoading(true);
        try {
            const params = new URLSearchParams({ limit, offset });
            if (query) params.set('q', query);
            if (category && category !== 'all') params.set('category', category);
            const result = await api._fetch(`/api/v1/knowledge?${params}`);
            setEntries(result.entries || []);
            setTotal(result.total || 0);
        } catch (e) {
            console.warn('[GRID] Knowledge search:', e.message);
            setEntries([]);
        }
        setLoading(false);
    };

    const handleSearch = (e) => {
        e.preventDefault();
        setOffset(0);
        loadEntries();
    };

    const selectEntry = async (id) => {
        try {
            const result = await api._fetch(`/api/v1/knowledge/${id}`);
            setSelectedEntry(result.entry);
            setSelectedRelated(result.related || []);
        } catch (e) {
            console.warn('[GRID] Knowledge entry:', e.message);
        }
    };

    if (selectedEntry) {
        return (
            <div style={shared.container}>
                <div style={shared.header}>Knowledge Tree</div>
                <EntryDetail
                    entry={selectedEntry}
                    related={selectedRelated}
                    onBack={() => { setSelectedEntry(null); setSelectedRelated([]); }}
                    onSelect={selectEntry}
                />
            </div>
        );
    }

    const totalPages = Math.ceil(total / limit);
    const currentPage = Math.floor(offset / limit) + 1;

    return (
        <div style={shared.container}>
            <div style={shared.header}>Knowledge Tree</div>

            <StatsBar summary={summary} />

            {/* Search */}
            <form onSubmit={handleSearch} style={styles.searchRow}>
                <input
                    style={styles.searchInput}
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search questions & answers..."
                />
                <button type="submit" style={shared.buttonSmall}>Search</button>
            </form>

            {/* Category Chips */}
            <div style={styles.chips}>
                {CATEGORIES.map(cat => (
                    <button
                        key={cat}
                        style={styles.chip(category === cat)}
                        onClick={() => { setCategory(cat); setOffset(0); }}
                    >
                        {cat.charAt(0).toUpperCase() + cat.slice(1)}
                    </button>
                ))}
            </div>

            {/* Results */}
            {loading ? (
                <div style={{ ...shared.card, textAlign: 'center', color: colors.textMuted }}>
                    Loading...
                </div>
            ) : entries.length === 0 ? (
                <div style={{
                    ...shared.card, textAlign: 'center', color: colors.textMuted,
                    padding: '40px', fontSize: '13px',
                }}>
                    {query || category !== 'all'
                        ? 'No entries match your search.'
                        : 'No knowledge entries yet. Ask questions in Briefings to build the tree.'
                    }
                </div>
            ) : (
                entries.map(entry => (
                    <div
                        key={entry.id}
                        style={{ ...shared.card, cursor: 'pointer' }}
                        onClick={() => selectEntry(entry.id)}
                    >
                        <div style={{ display: 'flex', gap: '8px', marginBottom: '6px', alignItems: 'center' }}>
                            <span style={shared.badge(categoryColors[entry.category] || '#3A4A5A')}>
                                {(entry.category || 'general').toUpperCase()}
                            </span>
                            <ConfidenceBar value={entry.confidence} />
                            <span style={styles.metaText}>{entry.source_model}</span>
                        </div>

                        <div style={styles.question}>{entry.question}</div>
                        <div style={styles.answer}>{entry.answer}</div>

                        {(entry.tags || []).length > 0 && (
                            <div style={{ marginTop: '6px' }}>
                                {entry.tags.slice(0, 6).map((t, i) => (
                                    <span key={i} style={styles.tag}>{t}</span>
                                ))}
                                {entry.tags.length > 6 && (
                                    <span style={styles.metaText}>+{entry.tags.length - 6}</span>
                                )}
                            </div>
                        )}

                        <div style={styles.meta}>
                            <span style={styles.metaText}>
                                {entry.created_at ? new Date(entry.created_at).toLocaleString() : ''}
                            </span>
                        </div>
                    </div>
                ))
            )}

            {/* Pagination */}
            {totalPages > 1 && (
                <div style={styles.pagination}>
                    <button
                        style={{
                            ...shared.buttonSmall,
                            ...(currentPage <= 1 ? shared.buttonDisabled : {}),
                        }}
                        onClick={() => setOffset(Math.max(0, offset - limit))}
                        disabled={currentPage <= 1}
                    >
                        Prev
                    </button>
                    <span style={{ ...styles.metaText, lineHeight: '30px' }}>
                        {currentPage} / {totalPages} ({total} total)
                    </span>
                    <button
                        style={{
                            ...shared.buttonSmall,
                            ...(currentPage >= totalPages ? shared.buttonDisabled : {}),
                        }}
                        onClick={() => setOffset(offset + limit)}
                        disabled={currentPage >= totalPages}
                    >
                        Next
                    </button>
                </div>
            )}
        </div>
    );
}
