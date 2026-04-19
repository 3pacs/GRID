import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Search, Plus, X, Loader2, Database, Zap, Lightbulb, Camera, ExternalLink } from 'lucide-react';
import { api } from '../api.js';
import { NODE_COLORS } from './canvas/nodeStyles.js';

const TYPE_META = {
    actor: { label: 'Actor', color: NODE_COLORS.actor, Icon: Database },
    signal: { label: 'Signal', color: NODE_COLORS.signal, Icon: Zap },
    hypothesis: { label: 'Hypothesis', color: NODE_COLORS.hypothesis, Icon: Lightbulb },
    snapshot: { label: 'Snapshot', color: '#3B82F6', Icon: Camera },
};

const styles = {
    panel: {
        position: 'absolute',
        top: 0,
        left: 0,
        width: 320,
        height: '100%',
        background: '#0D1117',
        borderRight: '1px solid #1E2A3A',
        display: 'flex',
        flexDirection: 'column',
        zIndex: 40,
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    panelStacked: {
        position: 'relative',
        width: '100%',
        maxWidth: '100%',
        minHeight: 'min(60vh, 540px)',
        height: 'min(60vh, 540px)',
        borderRight: 'none',
        borderBottom: '1px solid #1E2A3A',
    },
    header: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '10px 12px',
        borderBottom: '1px solid #1E2A3A',
    },
    headerTitle: {
        fontSize: 13,
        fontWeight: 600,
        color: '#C8D8E8',
        display: 'flex',
        alignItems: 'center',
        gap: 6,
    },
    closeBtn: {
        background: 'none',
        border: 'none',
        color: '#5A7080',
        cursor: 'pointer',
        padding: 4,
        display: 'flex',
        alignItems: 'center',
    },
    searchBox: {
        padding: '8px 12px',
        borderBottom: '1px solid #1E2A3A',
    },
    input: {
        width: '100%',
        boxSizing: 'border-box',
        background: '#161B22',
        border: '1px solid #1E2A3A',
        borderRadius: 6,
        color: '#C8D8E8',
        fontSize: 13,
        padding: '8px 10px 8px 32px',
        fontFamily: "'IBM Plex Sans', sans-serif",
        outline: 'none',
    },
    inputWrapper: {
        position: 'relative',
    },
    inputIcon: {
        position: 'absolute',
        left: 10,
        top: '50%',
        transform: 'translateY(-50%)',
        color: '#5A7080',
        pointerEvents: 'none',
    },
    results: {
        flex: 1,
        overflowY: 'auto',
        padding: '8px 0',
    },
    groupHeader: {
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: '1.2px',
        textTransform: 'uppercase',
        padding: '10px 12px 4px',
        display: 'flex',
        alignItems: 'center',
        gap: 6,
    },
    card: {
        margin: '2px 8px',
        padding: '8px 10px',
        background: '#161B22',
        border: '1px solid #1E2A3A',
        borderRadius: 6,
        cursor: 'default',
        transition: 'background 0.15s',
    },
    cardTitle: {
        fontSize: 12,
        fontWeight: 600,
        color: '#C8D8E8',
        marginBottom: 3,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
    },
    cardSnippet: {
        fontSize: 11,
        color: '#5A7080',
        lineHeight: '1.4',
        marginBottom: 4,
        maxHeight: 40,
        overflow: 'hidden',
    },
    cardFooter: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
    },
    badge: (color) => ({
        display: 'inline-block',
        padding: '1px 6px',
        borderRadius: 4,
        fontSize: 10,
        fontWeight: 600,
        letterSpacing: '0.5px',
        background: color,
        color: '#fff',
    }),
    relevance: {
        fontSize: 10,
        color: '#5A7080',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    addBtn: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: 3,
        padding: '3px 8px',
        fontSize: 10,
        fontWeight: 600,
        color: '#C8D8E8',
        background: 'transparent',
        border: '1px solid #1E2A3A',
        borderRadius: 4,
        cursor: 'pointer',
        transition: 'all 0.15s',
    },
    openBtn: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: 4,
        padding: '3px 8px',
        fontSize: 10,
        fontWeight: 700,
        color: '#fff',
        background: '#1A6EBF',
        border: '1px solid #1A6EBF',
        borderRadius: 4,
        cursor: 'pointer',
        transition: 'all 0.15s',
    },
    actions: {
        display: 'flex',
        alignItems: 'center',
        gap: 6,
    },
    loading: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 24,
        color: '#5A7080',
        fontSize: 12,
        gap: 6,
    },
    empty: {
        textAlign: 'center',
        padding: '24px 16px',
        color: '#5A7080',
        fontSize: 12,
    },
    total: {
        fontSize: 10,
        color: '#5A7080',
        padding: '4px 12px 8px',
        borderBottom: '1px solid #1E2A3A',
    },
};

function firstNonEmpty(...values) {
    for (const value of values) {
        if (typeof value === 'string' && value.trim()) {
            return value.trim();
        }
    }
    return null;
}

function looksLikeTicker(value) {
    return typeof value === 'string' && /^[A-Z0-9.\-]{1,12}$/.test(value.trim());
}

function parseSignalTitle(item) {
    const title = firstNonEmpty(item?.signal_type, item?.title);
    if (!title) {
        return { signalType: null, ticker: null };
    }
    const [rawType, rawTicker] = title.split(':');
    const signalType = firstNonEmpty(rawType);
    const tickerCandidate = firstNonEmpty(item?.ticker, rawTicker);
    return {
        signalType,
        ticker: looksLikeTicker(tickerCandidate) ? tickerCandidate : null,
    };
}

export function getIntelligenceSearchOpenTarget(item) {
    const sourceType = firstNonEmpty(item?.source_type, item?.type)?.toLowerCase();
    if (!sourceType) return null;

    switch (sourceType) {
    case 'actor': {
        const actor = firstNonEmpty(item.title, item.label);
        return actor ? { view: 'actor-network', param: actor } : { view: 'actor-network' };
    }
    case 'hypothesis':
        return item?.source_id
            ? { view: 'discovery', param: String(item.source_id) }
            : { view: 'discovery', param: firstNonEmpty(item.title, item.label) ?? undefined };
    case 'signal': {
        const { signalType, ticker } = parseSignalTitle(item);
        if (ticker) {
            return { view: 'watchlist-analysis', param: ticker };
        }
        return signalType
            ? { view: 'signals', param: signalType }
            : { view: 'signals' };
    }
    case 'snapshot':
        return { view: 'snapshots' };
    default:
        return null;
    }
}

function IntelligenceSearch({ onClose, onAddToCanvas, onOpenResult, stacked = false }) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(false);
    const [searched, setSearched] = useState(false);
    const debounceRef = useRef(null);

    const doSearch = useCallback(async (q) => {
        if (!q || q.trim().length < 1) {
            setResults([]);
            setTotal(0);
            setSearched(false);
            return;
        }
        setLoading(true);
        setSearched(true);
        try {
            const res = await api.get(
                `/api/v1/search/intelligence?q=${encodeURIComponent(q.trim())}&limit=50&offset=0`
            );
            setResults(res.results || []);
            setTotal(res.total || 0);
        } catch (err) {
            console.error('Intelligence search failed:', err);
            setResults([]);
            setTotal(0);
        } finally {
            setLoading(false);
        }
    }, []);

    const handleInputChange = useCallback((e) => {
        const val = e.target.value;
        setQuery(val);
        if (debounceRef.current) clearTimeout(debounceRef.current);
        debounceRef.current = setTimeout(() => doSearch(val), 300);
    }, [doSearch]);

    // Cleanup debounce on unmount
    useEffect(() => {
        return () => {
            if (debounceRef.current) clearTimeout(debounceRef.current);
        };
    }, []);

    const handleKeyDown = useCallback((e) => {
        if (e.key === 'Escape') {
            onClose();
        }
    }, [onClose]);

    const handleAdd = useCallback((item) => {
        if (!onAddToCanvas) return;
        // Map source_type to canvas node type
        const nodeType = item.source_type === 'snapshot' ? 'note' : item.source_type;
        onAddToCanvas({
            type: nodeType,
            id: item.source_id,
            label: item.title || `${item.source_type} ${item.source_id}`,
            data: {
                source_type: item.source_type,
                source_id: item.source_id,
                snippet: item.snippet,
            },
        });
    }, [onAddToCanvas]);

    const handleOpen = useCallback((item) => {
        const target = getIntelligenceSearchOpenTarget(item);
        if (!target || !onOpenResult) return;
        onOpenResult(target, item);
    }, [onOpenResult]);

    // Group results by source_type
    const grouped = {};
    for (const r of results) {
        const key = r.source_type;
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(r);
    }

    // Order groups consistently
    const groupOrder = ['actor', 'signal', 'hypothesis', 'snapshot'];
    const panelStyle = stacked
        ? { ...styles.panel, ...styles.panelStacked }
        : styles.panel;

    return (
        <div style={panelStyle}>
            {/* Header */}
            <div style={styles.header}>
                <span style={styles.headerTitle}>
                    <Search size={14} />
                    Intelligence Search
                </span>
                <button style={styles.closeBtn} onClick={onClose} title="Close search panel">
                    <X size={16} />
                </button>
            </div>

            {/* Search input */}
            <div style={styles.searchBox}>
                <div style={styles.inputWrapper}>
                    <div style={styles.inputIcon}>
                        <Search size={14} />
                    </div>
                    <input
                        style={styles.input}
                        type="text"
                        placeholder="Search actors, signals, hypotheses..."
                        value={query}
                        onChange={handleInputChange}
                        onKeyDown={handleKeyDown}
                        autoFocus
                    />
                </div>
            </div>

            {/* Total indicator */}
            {searched && !loading && (
                <div style={styles.total}>
                    {total} result{total !== 1 ? 's' : ''} for &quot;{query}&quot;
                </div>
            )}

            {/* Results */}
            <div style={styles.results}>
                {loading && (
                    <div style={styles.loading}>
                        <Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} />
                        Searching...
                    </div>
                )}

                {!loading && searched && results.length === 0 && (
                    <div style={styles.empty}>
                        No results found for &quot;{query}&quot;
                    </div>
                )}

                {!loading && groupOrder.map((type) => {
                    const items = grouped[type];
                    if (!items || items.length === 0) return null;
                    const meta = TYPE_META[type] || { label: type, color: '#6B7280', Icon: Database };
                    const TypeIcon = meta.Icon;

                    return (
                        <div key={type}>
                            <div style={{ ...styles.groupHeader, color: meta.color }}>
                                <TypeIcon size={12} />
                                {meta.label}s ({items.length})
                            </div>
                            {items.map((item, idx) => {
                                const openTarget = getIntelligenceSearchOpenTarget(item);
                                return (
                                    <div
                                        key={`${item.source_type}-${item.source_id}-${idx}`}
                                        style={styles.card}
                                        onMouseEnter={(e) => { e.currentTarget.style.background = '#1C2633'; }}
                                        onMouseLeave={(e) => { e.currentTarget.style.background = '#161B22'; }}
                                    >
                                        <div style={styles.cardTitle} title={item.title}>
                                            {item.title || 'Untitled'}
                                        </div>
                                        <div
                                            style={styles.cardSnippet}
                                            dangerouslySetInnerHTML={{ __html: item.snippet || '' }}
                                        />
                                        <div style={styles.cardFooter}>
                                            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                                                <span style={styles.badge(meta.color)}>
                                                    {meta.label}
                                                </span>
                                                <span style={styles.relevance}>
                                                    {typeof item.relevance === 'number' ? item.relevance.toFixed(4) : '--'}
                                                </span>
                                            </div>
                                            <div style={styles.actions}>
                                                {openTarget && (
                                                    <button
                                                        style={styles.openBtn}
                                                        onClick={() => handleOpen(item)}
                                                        onMouseEnter={(e) => {
                                                            e.currentTarget.style.background = '#2782D9';
                                                            e.currentTarget.style.borderColor = '#2782D9';
                                                        }}
                                                        onMouseLeave={(e) => {
                                                            e.currentTarget.style.background = '#1A6EBF';
                                                            e.currentTarget.style.borderColor = '#1A6EBF';
                                                        }}
                                                        title="Open matching view"
                                                        aria-label={`Open ${item.title || meta.label}`}
                                                    >
                                                        <ExternalLink size={10} /> Open
                                                    </button>
                                                )}
                                                <button
                                                    style={styles.addBtn}
                                                    onClick={() => handleAdd(item)}
                                                    onMouseEnter={(e) => {
                                                        e.currentTarget.style.background = '#1E2A3A';
                                                        e.currentTarget.style.borderColor = meta.color;
                                                    }}
                                                    onMouseLeave={(e) => {
                                                        e.currentTarget.style.background = 'transparent';
                                                        e.currentTarget.style.borderColor = '#1E2A3A';
                                                    }}
                                                    title="Add to canvas"
                                                    aria-label={`Add ${item.title || meta.label} to Canvas`}
                                                >
                                                    <Plus size={10} /> Add
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    );
                })}
            </div>

            {/* Keyframe for spinner */}
            <style>{`
                @keyframes spin {
                    from { transform: rotate(0deg); }
                    to { transform: rotate(360deg); }
                }
            `}</style>
        </div>
    );
}

export default IntelligenceSearch;
