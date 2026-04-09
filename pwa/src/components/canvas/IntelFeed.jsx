/**
 * IntelFeed — Gotham-style live intelligence feed panel for Canvas.
 *
 * Right-side streaming panel showing recent signals, news, and events.
 * Items matching entities on the current board are highlighted.
 * Click "+" to add any item to the canvas as a connected node.
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { X, Radio, Plus, AlertTriangle, TrendingUp, TrendingDown, Minus, RefreshCw } from 'lucide-react';
import { api } from '../../api.js';
import { NODE_COLORS } from './nodeStyles.js';

const POLL_INTERVAL = 30000; // 30s refresh

const TYPE_ICONS = {
    breaking_news: AlertTriangle,
    insider: TrendingUp,
    congressional: TrendingUp,
    dark_pool: TrendingDown,
    default: Minus,
};

const TYPE_COLORS = {
    breaking_news: '#EF4444',
    insider: '#3B82F6',
    congressional: '#FFD700',
    dark_pool: '#A855F7',
    social: '#10B981',
    geopolitical: '#F97316',
    whale: '#06B6D4',
    default: '#5A7080',
};

const s = {
    panel: {
        position: 'absolute',
        top: 0,
        right: 0,
        width: 340,
        height: '100%',
        background: 'rgba(13, 17, 23, 0.92)',
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        borderLeft: '1px solid rgba(30, 42, 58, 0.6)',
        display: 'flex',
        flexDirection: 'column',
        zIndex: 40,
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    header: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '12px 14px',
        borderBottom: '1px solid rgba(30, 42, 58, 0.6)',
        background: 'linear-gradient(135deg, rgba(239, 68, 68, 0.06) 0%, transparent 100%)',
    },
    headerTitle: {
        fontSize: 13,
        fontWeight: 600,
        color: '#C8D8E8',
        display: 'flex',
        alignItems: 'center',
        gap: 8,
    },
    liveDot: {
        width: 7,
        height: 7,
        borderRadius: '50%',
        background: '#EF4444',
        boxShadow: '0 0 8px rgba(239, 68, 68, 0.6)',
        animation: 'feedPulse 2s ease-in-out infinite',
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
    filterBar: {
        display: 'flex',
        gap: 4,
        padding: '8px 12px',
        borderBottom: '1px solid rgba(30, 42, 58, 0.4)',
        overflowX: 'auto',
    },
    filterBtn: (active) => ({
        padding: '3px 8px',
        fontSize: 10,
        fontWeight: 600,
        borderRadius: 4,
        border: '1px solid',
        borderColor: active ? '#3B82F6' : '#1E2A3A',
        background: active ? 'rgba(59, 130, 246, 0.15)' : 'transparent',
        color: active ? '#7CB3F0' : '#5A7080',
        cursor: 'pointer',
        whiteSpace: 'nowrap',
    }),
    list: {
        flex: 1,
        overflowY: 'auto',
        padding: '4px 0',
    },
    card: (highlighted) => ({
        margin: '3px 8px',
        padding: '10px 12px',
        background: highlighted
            ? 'linear-gradient(135deg, rgba(59, 130, 246, 0.08) 0%, rgba(13, 17, 23, 0.95) 100%)'
            : 'rgba(22, 27, 34, 0.8)',
        border: `1px solid ${highlighted ? 'rgba(59, 130, 246, 0.3)' : 'rgba(30, 42, 58, 0.5)'}`,
        borderRadius: 8,
        cursor: 'default',
        transition: 'all 0.2s ease',
    }),
    cardHeader: {
        display: 'flex',
        alignItems: 'center',
        gap: 6,
        marginBottom: 4,
    },
    typeBadge: (color) => ({
        display: 'inline-flex',
        alignItems: 'center',
        gap: 3,
        padding: '2px 6px',
        borderRadius: 4,
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: '0.5px',
        background: `rgba(${hexToRgb(color)}, 0.15)`,
        color: color,
        border: `1px solid rgba(${hexToRgb(color)}, 0.25)`,
    }),
    headline: {
        fontSize: 12,
        fontWeight: 500,
        color: '#C8D8E8',
        lineHeight: '1.4',
        marginBottom: 4,
    },
    entityPills: {
        display: 'flex',
        flexWrap: 'wrap',
        gap: 3,
        marginTop: 4,
    },
    entityPill: (onBoard) => ({
        fontSize: 9,
        padding: '1px 5px',
        borderRadius: 3,
        background: onBoard ? 'rgba(16, 185, 129, 0.15)' : 'rgba(90, 120, 144, 0.1)',
        color: onBoard ? '#10B981' : '#5A7A90',
        border: `1px solid ${onBoard ? 'rgba(16, 185, 129, 0.3)' : 'rgba(90, 120, 144, 0.15)'}`,
        fontWeight: onBoard ? 600 : 400,
        boxShadow: onBoard ? '0 0 6px rgba(16, 185, 129, 0.2)' : 'none',
    }),
    time: {
        fontSize: 10,
        color: '#4A5A6A',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    addBtn: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: 3,
        padding: '3px 8px',
        fontSize: 10,
        fontWeight: 600,
        color: '#5A7080',
        background: 'transparent',
        border: '1px solid rgba(30, 42, 58, 0.5)',
        borderRadius: 4,
        cursor: 'pointer',
        transition: 'all 0.2s',
    },
    footer: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
    },
    empty: {
        textAlign: 'center',
        padding: '40px 16px',
        color: '#4A5A6A',
        fontSize: 12,
    },
    refreshBtn: {
        background: 'none',
        border: 'none',
        color: '#5A7080',
        cursor: 'pointer',
        padding: 4,
        display: 'flex',
        alignItems: 'center',
    },
};

function hexToRgb(hex) {
    const h = (hex || '#6B7280').replace('#', '');
    return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)].join(',');
}

function timeAgo(dateStr) {
    const mins = Math.floor((Date.now() - new Date(dateStr).getTime()) / 60000);
    if (mins < 1) return 'now';
    if (mins < 60) return `${mins}m`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h`;
    return `${Math.floor(hrs / 24)}d`;
}

const FILTERS = ['all', 'breaking_news', 'insider', 'congressional', 'dark_pool', 'social', 'geopolitical'];

function IntelFeed({ onClose, onAddToCanvas, boardEntityNames = [] }) {
    const [items, setItems] = useState([]);
    const [loading, setLoading] = useState(false);
    const [filter, setFilter] = useState('all');
    const timerRef = useRef(null);

    const boardNameSet = new Set(boardEntityNames.map(n => n.toLowerCase()));

    const fetchFeed = useCallback(async () => {
        setLoading(true);
        try {
            const entityParam = boardEntityNames.length > 0
                ? `&entities=${encodeURIComponent(boardEntityNames.join(','))}`
                : '';
            const res = await api.get(`/api/v1/feed/live?limit=60${entityParam}`);
            setItems(res.items || []);
        } catch (err) {
            console.error('Feed fetch failed:', err);
        } finally {
            setLoading(false);
        }
    }, [boardEntityNames.join(',')]);

    useEffect(() => {
        fetchFeed();
        timerRef.current = setInterval(fetchFeed, POLL_INTERVAL);
        return () => clearInterval(timerRef.current);
    }, [fetchFeed]);

    const filtered = filter === 'all'
        ? items
        : items.filter(i => i.signal_type === filter);

    const isOnBoard = (entityName) => boardNameSet.has(entityName?.toLowerCase());

    const hasAnyOnBoard = (item) => {
        const entities = item.entities || [];
        if (entities.some(e => isOnBoard(e))) return true;
        if (isOnBoard(item.ticker)) return true;
        if (isOnBoard(item.actor)) return true;
        return false;
    };

    const handleAdd = (item) => {
        if (!onAddToCanvas) return;
        const isNews = item.signal_type === 'breaking_news' || item.signal_type === 'news';
        onAddToCanvas({
            type: isNews ? 'news' : 'signal',
            id: item.id || `feed-${Date.now()}`,
            label: item.description || item.headline || `${item.signal_type}: ${item.ticker || ''}`,
            data: {
                signal_type: item.signal_type,
                ticker: item.ticker,
                actor: item.actor,
                direction: item.direction,
                magnitude: item.magnitude,
                confidence: item.confidence,
                headline: item.description || item.headline,
                summary: item.description,
                source: item.signal_type,
                sentiment: item.direction === 'buy' || item.direction === 'bullish' ? 'bullish'
                    : item.direction === 'sell' || item.direction === 'bearish' ? 'bearish' : 'neutral',
                urgency: item.magnitude > 7 ? 'breaking' : item.magnitude > 4 ? 'high' : 'normal',
                entities: item.entities || [item.ticker, item.actor].filter(Boolean),
                published_at: item.signal_date || item.created_at,
            },
        });
    };

    // Sort: board-relevant items first, then by date
    const sorted = [...filtered].sort((a, b) => {
        const aOnBoard = hasAnyOnBoard(a) ? 1 : 0;
        const bOnBoard = hasAnyOnBoard(b) ? 1 : 0;
        if (aOnBoard !== bOnBoard) return bOnBoard - aOnBoard;
        return new Date(b.signal_date || b.created_at || 0) - new Date(a.signal_date || a.created_at || 0);
    });

    return (
        <div style={s.panel}>
            <div style={s.header}>
                <span style={s.headerTitle}>
                    <div style={s.liveDot} />
                    <Radio size={13} />
                    LIVE INTEL
                </span>
                <div style={{ display: 'flex', gap: 4 }}>
                    <button style={s.refreshBtn} onClick={fetchFeed} title="Refresh">
                        <RefreshCw size={13} style={loading ? { animation: 'spin 1s linear infinite' } : {}} />
                    </button>
                    <button style={s.closeBtn} onClick={onClose} title="Close feed">
                        <X size={16} />
                    </button>
                </div>
            </div>

            <div style={s.filterBar}>
                {FILTERS.map(f => (
                    <button
                        key={f}
                        style={s.filterBtn(filter === f)}
                        onClick={() => setFilter(f)}
                    >
                        {f === 'all' ? 'ALL' : f.replace('_', ' ').toUpperCase()}
                    </button>
                ))}
            </div>

            <div style={s.list}>
                {sorted.length === 0 && !loading && (
                    <div style={s.empty}>No signals in feed yet</div>
                )}
                {sorted.map((item, idx) => {
                    const highlighted = hasAnyOnBoard(item);
                    const typeColor = TYPE_COLORS[item.signal_type] || TYPE_COLORS.default;
                    const Icon = TYPE_ICONS[item.signal_type] || TYPE_ICONS.default;
                    const entities = item.entities || [item.ticker, item.actor].filter(Boolean);

                    return (
                        <div
                            key={item.id || idx}
                            style={s.card(highlighted)}
                            onMouseEnter={e => { e.currentTarget.style.borderColor = typeColor; }}
                            onMouseLeave={e => {
                                e.currentTarget.style.borderColor = highlighted
                                    ? 'rgba(59, 130, 246, 0.3)'
                                    : 'rgba(30, 42, 58, 0.5)';
                            }}
                        >
                            <div style={s.cardHeader}>
                                <span style={s.typeBadge(typeColor)}>
                                    <Icon size={9} />
                                    {(item.signal_type || 'signal').replace('_', ' ')}
                                </span>
                                {item.direction && (
                                    <span style={{
                                        fontSize: 9,
                                        fontWeight: 700,
                                        color: item.direction === 'buy' || item.direction === 'bullish' ? '#10B981' : item.direction === 'sell' || item.direction === 'bearish' ? '#EF4444' : '#5A7080',
                                    }}>
                                        {item.direction.toUpperCase()}
                                    </span>
                                )}
                                <span style={{ flex: 1 }} />
                                <span style={s.time}>
                                    {timeAgo(item.signal_date || item.created_at)}
                                </span>
                            </div>

                            <div style={s.headline}>
                                {item.description || `${item.signal_type}: ${item.ticker || item.actor || ''}`}
                            </div>

                            {entities.length > 0 && (
                                <div style={s.entityPills}>
                                    {entities.slice(0, 6).map((ent, i) => (
                                        <span key={i} style={s.entityPill(isOnBoard(ent))}>
                                            {ent}
                                        </span>
                                    ))}
                                </div>
                            )}

                            <div style={{ ...s.footer, marginTop: 6 }}>
                                {item.magnitude != null && (
                                    <span style={{ ...s.time, color: typeColor }}>
                                        mag {Number(item.magnitude).toFixed(1)}
                                    </span>
                                )}
                                <button
                                    style={s.addBtn}
                                    onClick={() => handleAdd(item)}
                                    onMouseEnter={e => {
                                        e.currentTarget.style.background = 'rgba(59, 130, 246, 0.1)';
                                        e.currentTarget.style.borderColor = '#3B82F6';
                                        e.currentTarget.style.color = '#7CB3F0';
                                    }}
                                    onMouseLeave={e => {
                                        e.currentTarget.style.background = 'transparent';
                                        e.currentTarget.style.borderColor = 'rgba(30, 42, 58, 0.5)';
                                        e.currentTarget.style.color = '#5A7080';
                                    }}
                                    title="Add to canvas"
                                >
                                    <Plus size={10} /> Add
                                </button>
                            </div>
                        </div>
                    );
                })}
            </div>

            <style>{`
                @keyframes feedPulse {
                    0%, 100% { opacity: 1; }
                    50% { opacity: 0.4; }
                }
                @keyframes spin {
                    from { transform: rotate(0deg); }
                    to { transform: rotate(360deg); }
                }
            `}</style>
        </div>
    );
}

export default IntelFeed;
