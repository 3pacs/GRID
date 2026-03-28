import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
    Search, TrendingUp, Activity, Users, FlaskConical, Layout,
    Radio, Command, RefreshCw, Scan, ShieldCheck, BarChart3, Clock, X,
    ArrowUp, ArrowDown, CornerDownLeft,
} from 'lucide-react';
import { api } from '../api.js';

// ── Constants ────────────────────────────────────────────────────

const LS_KEY = 'grid_recent_searches';
const MAX_RECENT = 5;
const DEBOUNCE_MS = 200;

const TYPE_ICONS = {
    ticker: TrendingUp,
    feature: Activity,
    actor: Users,
    hypothesis: FlaskConical,
    view: Layout,
    source: Radio,
    command: Command,
};

const TYPE_COLORS = {
    ticker: '#22C55E',
    feature: '#1A6EBF',
    actor: '#F59E0B',
    hypothesis: '#A78BFA',
    view: '#38BDF8',
    source: '#6B7280',
    command: '#F472B6',
};

const TYPE_LABELS = {
    ticker: 'Tickers',
    feature: 'Features',
    actor: 'Actors',
    hypothesis: 'Hypotheses',
    view: 'Views',
    source: 'Sources',
    command: 'Commands',
};

const QUICK_COMMANDS = [
    { id: 'refresh', title: 'Refresh watchlist prices', subtitle: 'Reload latest prices for all watchlist tickers', icon: RefreshCw },
    { id: 'scan', title: 'Trigger options scan', subtitle: 'Run the options mispricing scanner', icon: Scan },
    { id: 'audit', title: 'Trigger source audit', subtitle: 'Audit data source accuracy', icon: ShieldCheck },
    { id: 'regime', title: 'Show current regime', subtitle: 'Navigate to the regime view', icon: BarChart3 },
];

// ── Helpers ──────────────────────────────────────────────────────

function loadRecent() {
    try {
        return JSON.parse(localStorage.getItem(LS_KEY) || '[]').slice(0, MAX_RECENT);
    } catch {
        return [];
    }
}

function saveRecent(list) {
    try {
        localStorage.setItem(LS_KEY, JSON.stringify(list.slice(0, MAX_RECENT)));
    } catch { /* quota exceeded — ignore */ }
}

function addRecent(query) {
    if (!query || query.startsWith('>')) return;
    const prev = loadRecent().filter(q => q !== query);
    saveRecent([query, ...prev]);
}

function groupResults(results) {
    const order = ['ticker', 'view', 'feature', 'actor', 'hypothesis', 'source', 'command'];
    const groups = {};
    for (const r of results) {
        const t = r.type || 'view';
        if (!groups[t]) groups[t] = [];
        groups[t].push(r);
    }
    return order.filter(t => groups[t]).map(t => ({ type: t, items: groups[t] }));
}

// ── Styles ───────────────────────────────────────────────────────

const s = {
    backdrop: {
        position: 'fixed', inset: 0, zIndex: 9999,
        background: 'rgba(0, 0, 0, 0.65)',
        backdropFilter: 'blur(4px)', WebkitBackdropFilter: 'blur(4px)',
        display: 'flex', alignItems: 'flex-start', justifyContent: 'center',
        paddingTop: '15vh',
    },
    modal: {
        width: '100%', maxWidth: '600px', margin: '0 16px',
        background: '#0D1520',
        border: '1px solid #1A2840',
        borderRadius: '14px',
        boxShadow: '0 24px 64px rgba(0,0,0,0.7), 0 0 0 1px rgba(26,110,191,0.1)',
        overflow: 'hidden',
        display: 'flex', flexDirection: 'column',
        maxHeight: '70vh',
    },
    inputRow: {
        display: 'flex', alignItems: 'center', gap: '10px',
        padding: '14px 16px',
        borderBottom: '1px solid #1A2840',
    },
    input: {
        flex: 1, background: 'none', border: 'none', outline: 'none',
        color: '#E8F0F8', fontSize: '16px',
        fontFamily: "'IBM Plex Sans', sans-serif",
        caretColor: '#1A6EBF',
    },
    kbd: {
        fontSize: '11px', fontFamily: "'JetBrains Mono', monospace",
        color: '#5A7080', background: '#111B2A',
        padding: '2px 6px', borderRadius: '4px',
        border: '1px solid #1A2840',
    },
    results: {
        overflowY: 'auto', flex: 1,
        WebkitOverflowScrolling: 'touch',
    },
    groupLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: '#5A7080', padding: '10px 16px 4px 16px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    item: {
        display: 'flex', alignItems: 'center', gap: '12px',
        padding: '10px 16px', cursor: 'pointer',
        transition: 'background 0.1s',
    },
    itemActive: {
        background: '#1A6EBF15',
    },
    iconWrap: {
        width: '32px', height: '32px', borderRadius: '8px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        flexShrink: 0,
    },
    title: {
        fontSize: '14px', fontWeight: 600, color: '#E8F0F8',
        fontFamily: "'IBM Plex Sans', sans-serif",
        whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
    },
    subtitle: {
        fontSize: '12px', color: '#5A7080', marginTop: '1px',
        fontFamily: "'IBM Plex Sans', sans-serif",
        whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
    },
    footer: {
        display: 'flex', alignItems: 'center', gap: '16px',
        padding: '8px 16px', borderTop: '1px solid #1A2840',
        fontSize: '11px', color: '#5A7080',
        fontFamily: "'JetBrains Mono', monospace",
    },
    footerHint: {
        display: 'flex', alignItems: 'center', gap: '4px',
    },
    emptyState: {
        padding: '32px 16px', textAlign: 'center',
        color: '#5A7080', fontSize: '13px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    recentLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: '#5A7080', padding: '12px 16px 4px 16px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    recentItem: {
        display: 'flex', alignItems: 'center', gap: '10px',
        padding: '8px 16px', cursor: 'pointer',
        transition: 'background 0.1s',
        fontSize: '13px', color: '#8AA0B8',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
};

// ── Component ────────────────────────────────────────────────────

export default function CommandPalette({ open, onClose, onNavigate }) {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [activeIndex, setActiveIndex] = useState(0);
    const inputRef = useRef(null);
    const debounceRef = useRef(null);
    const listRef = useRef(null);

    // Flatten grouped results for keyboard nav
    const grouped = groupResults(results);
    const flatItems = grouped.flatMap(g => g.items);

    // ── Focus input on open ──
    useEffect(() => {
        if (open) {
            setQuery('');
            setResults([]);
            setActiveIndex(0);
            setTimeout(() => inputRef.current?.focus(), 50);
        }
    }, [open]);

    // ── Debounced search ──
    const doSearch = useCallback(async (q) => {
        if (!q.trim()) {
            setResults([]);
            setLoading(false);
            return;
        }

        // Quick commands mode
        if (q.startsWith('>')) {
            const cmdQ = q.slice(1).trim().toLowerCase();
            const cmds = QUICK_COMMANDS
                .filter(c => !cmdQ || c.id.includes(cmdQ) || c.title.toLowerCase().includes(cmdQ))
                .map(c => ({
                    type: 'command',
                    title: `>${c.id}`,
                    subtitle: c.subtitle,
                    action: c.id,
                    param: null,
                    _cmdIcon: c.icon,
                }));
            setResults(cmds);
            setActiveIndex(0);
            setLoading(false);
            return;
        }

        setLoading(true);
        try {
            const data = await api.searchEverything(q.trim());
            if (data && !data.error) {
                setResults(data.results || []);
            } else {
                setResults([]);
            }
        } catch {
            setResults([]);
        }
        setActiveIndex(0);
        setLoading(false);
    }, []);

    useEffect(() => {
        if (debounceRef.current) clearTimeout(debounceRef.current);
        debounceRef.current = setTimeout(() => doSearch(query), DEBOUNCE_MS);
        return () => clearTimeout(debounceRef.current);
    }, [query, doSearch]);

    // ── Scroll active item into view ──
    useEffect(() => {
        if (!listRef.current) return;
        const active = listRef.current.querySelector('[data-active="true"]');
        if (active) {
            active.scrollIntoView({ block: 'nearest' });
        }
    }, [activeIndex]);

    // ── Execute a result ──
    const executeResult = useCallback((item) => {
        if (!item) return;
        addRecent(query);

        // Quick commands
        if (item.type === 'command') {
            const cmdId = item.action;
            if (cmdId === 'refresh') {
                api.refreshWatchlistPrices?.() || api._fetch('/api/v1/watchlist/refresh', { method: 'POST' });
            } else if (cmdId === 'scan') {
                api._fetch('/api/v1/options/scan', { method: 'POST' });
            } else if (cmdId === 'audit') {
                api._fetch('/api/v1/intelligence/source-audit', { method: 'POST' });
            } else if (cmdId === 'regime') {
                onNavigate('regime');
            }
            onClose();
            return;
        }

        // Navigation results
        if (item.action === 'watchlist-analysis' && item.param) {
            onNavigate('watchlist-analysis', item.param);
        } else if (item.action) {
            onNavigate(item.action, item.param || undefined);
        }
        onClose();
    }, [query, onClose, onNavigate]);

    // ── Keyboard handler ──
    const handleKeyDown = useCallback((e) => {
        if (e.key === 'Escape') {
            e.preventDefault();
            onClose();
            return;
        }
        if (e.key === 'ArrowDown') {
            e.preventDefault();
            setActiveIndex(i => Math.min(i + 1, flatItems.length - 1));
            return;
        }
        if (e.key === 'ArrowUp') {
            e.preventDefault();
            setActiveIndex(i => Math.max(i - 1, 0));
            return;
        }
        if (e.key === 'Enter') {
            e.preventDefault();
            if (flatItems[activeIndex]) {
                executeResult(flatItems[activeIndex]);
            }
            return;
        }
    }, [flatItems, activeIndex, executeResult, onClose]);

    if (!open) return null;

    const recent = loadRecent();
    const showRecent = !query.trim() && recent.length > 0;
    const showEmpty = !query.trim() && recent.length === 0;

    // Build flat index counter for active highlighting
    let flatIdx = 0;

    return (
        <div style={s.backdrop} onClick={onClose}>
            <div style={s.modal} onClick={e => e.stopPropagation()}>
                {/* Input row */}
                <div style={s.inputRow}>
                    <Search size={18} color="#5A7080" />
                    <input
                        ref={inputRef}
                        style={s.input}
                        value={query}
                        onChange={e => setQuery(e.target.value)}
                        onKeyDown={handleKeyDown}
                        placeholder='Search tickers, features, views... or type ">" for commands'
                        spellCheck={false}
                        autoComplete="off"
                    />
                    {loading && (
                        <span style={{ color: '#5A7080', fontSize: '12px', fontFamily: "'JetBrains Mono', monospace" }}>...</span>
                    )}
                    <span style={s.kbd}>ESC</span>
                </div>

                {/* Results */}
                <div style={s.results} ref={listRef}>
                    {showEmpty && (
                        <div style={s.emptyState}>
                            Type to search across all of GRID.<br />
                            <span style={{ color: '#3A5060', fontSize: '12px' }}>
                                Tickers, features, actors, hypotheses, views, and sources
                            </span>
                        </div>
                    )}

                    {showRecent && (
                        <>
                            <div style={s.recentLabel}>RECENT SEARCHES</div>
                            {recent.map((q, i) => (
                                <div
                                    key={q}
                                    style={{
                                        ...s.recentItem,
                                        background: i === activeIndex ? '#1A6EBF15' : 'transparent',
                                    }}
                                    onClick={() => {
                                        setQuery(q);
                                        doSearch(q);
                                    }}
                                    onMouseEnter={() => setActiveIndex(i)}
                                >
                                    <Clock size={14} color="#3A5060" />
                                    {q}
                                </div>
                            ))}
                        </>
                    )}

                    {query.trim() && flatItems.length === 0 && !loading && (
                        <div style={s.emptyState}>
                            No results for "{query}"
                        </div>
                    )}

                    {grouped.map(group => (
                        <div key={group.type}>
                            <div style={s.groupLabel}>
                                {TYPE_LABELS[group.type] || group.type.toUpperCase()}
                            </div>
                            {group.items.map((item, itemIdx) => {
                                const globalIdx = flatIdx++;
                                const isActive = globalIdx === activeIndex;
                                const IconComp = item._cmdIcon || TYPE_ICONS[item.type] || Layout;
                                const iconColor = TYPE_COLORS[item.type] || '#5A7080';
                                return (
                                    <div
                                        key={`${item.type}-${item.title}-${itemIdx}`}
                                        data-active={isActive ? 'true' : 'false'}
                                        style={{
                                            ...s.item,
                                            ...(isActive ? s.itemActive : {}),
                                        }}
                                        onClick={() => executeResult(item)}
                                        onMouseEnter={() => setActiveIndex(globalIdx)}
                                    >
                                        <div style={{
                                            ...s.iconWrap,
                                            background: `${iconColor}18`,
                                        }}>
                                            <IconComp size={16} color={iconColor} />
                                        </div>
                                        <div style={{ flex: 1, minWidth: 0 }}>
                                            <div style={s.title}>{item.title}</div>
                                            {item.subtitle && (
                                                <div style={s.subtitle}>{item.subtitle}</div>
                                            )}
                                        </div>
                                        {isActive && (
                                            <CornerDownLeft size={14} color="#5A7080" />
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    ))}
                </div>

                {/* Footer hints */}
                <div style={s.footer}>
                    <span style={s.footerHint}>
                        <ArrowUp size={12} /> <ArrowDown size={12} /> navigate
                    </span>
                    <span style={s.footerHint}>
                        <CornerDownLeft size={12} /> select
                    </span>
                    <span style={s.footerHint}>
                        esc close
                    </span>
                    <span style={{ marginLeft: 'auto', color: '#3A5060' }}>
                        &gt; commands
                    </span>
                </div>
            </div>
        </div>
    );
}
