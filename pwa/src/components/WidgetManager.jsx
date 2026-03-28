import React, { useState } from 'react';
import { colors } from '../styles/shared.js';

/**
 * Widget catalog — every dashboard module available for toggling.
 * Each entry defines an id, display label, category, default visibility,
 * and optional description.
 */
const WIDGET_CATALOG = [
    // Market Overview
    { id: 'regime-thermo', label: 'Regime Thermometer', category: 'Market Overview', defaultOn: true, desc: 'Horizontal CRISIS↔GROWTH bar' },
    { id: 'market-pulse', label: 'Heatmap', category: 'Market Overview', defaultOn: true, desc: 'Z-score tile grid by asset class' },
    { id: 'momentum-sparks', label: 'Momentum Sparklines', category: 'Market Overview', defaultOn: true, desc: 'Key asset mini-charts' },
    { id: 'fear-greed', label: 'Fear & Greed Gauge', category: 'Market Overview', defaultOn: true, desc: 'Composite sentiment arc' },

    // Intelligence
    { id: 'regime-card', label: 'Regime Card', category: 'Intelligence', defaultOn: true, desc: 'Current regime details' },
    { id: 'briefing-preview', label: 'Latest Briefing', category: 'Intelligence', defaultOn: true, desc: 'Most recent LLM briefing' },
    { id: 'capital-flow', label: 'Capital Flow Analysis', category: 'Intelligence', defaultOn: true, desc: 'Deep sector capital flow research' },

    // Portfolio
    { id: 'watchlist', label: 'Watchlist', category: 'Portfolio', defaultOn: true, desc: 'Tracked tickers' },
    { id: 'journal', label: 'Recent Journal', category: 'Portfolio', defaultOn: true, desc: 'Latest decision entries' },

    // System
    { id: 'agent-progress', label: 'Agent Progress', category: 'System', defaultOn: true, desc: 'Live agent run status' },
    { id: 'quick-actions', label: 'Quick Actions', category: 'System', defaultOn: true, desc: 'Navigation grid' },
    { id: 'status-metrics', label: 'Status Metrics', category: 'System', defaultOn: true, desc: 'Features, hypotheses, journal counts' },
];

const STORAGE_KEY = 'grid_widget_prefs';

/**
 * Load widget visibility preferences from localStorage.
 * Returns a map of widget id → boolean.
 */
export function loadWidgetPrefs() {
    try {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved) {
            const parsed = JSON.parse(saved);
            // Merge with catalog defaults (new widgets default on)
            const merged = {};
            for (const w of WIDGET_CATALOG) {
                merged[w.id] = w.id in parsed ? parsed[w.id] : w.defaultOn;
            }
            return merged;
        }
    } catch { /* ignore */ }
    // Default: everything on
    const defaults = {};
    for (const w of WIDGET_CATALOG) {
        defaults[w.id] = w.defaultOn;
    }
    return defaults;
}

/**
 * Save widget visibility preferences to localStorage.
 */
function saveWidgetPrefs(prefs) {
    try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
    } catch { /* ignore */ }
}

/**
 * Check if a widget is visible.
 */
export function isWidgetVisible(prefs, widgetId) {
    return prefs[widgetId] !== false;
}

/**
 * Get the widget catalog for external use.
 */
export function getWidgetCatalog() {
    return WIDGET_CATALOG;
}

/**
 * WidgetManager — slide-out panel with toggle switches for every dashboard widget.
 * Grouped by category with toggle all / toggle none per group.
 */
export default function WidgetManager({ prefs, onPrefsChange, open, onClose }) {
    const [filter, setFilter] = useState('');

    if (!open) return null;

    const categories = [...new Set(WIDGET_CATALOG.map(w => w.category))];

    const toggle = (id) => {
        const next = { ...prefs, [id]: !prefs[id] };
        saveWidgetPrefs(next);
        onPrefsChange(next);
    };

    const toggleCategory = (category, on) => {
        const next = { ...prefs };
        for (const w of WIDGET_CATALOG) {
            if (w.category === category) next[w.id] = on;
        }
        saveWidgetPrefs(next);
        onPrefsChange(next);
    };

    const toggleAll = (on) => {
        const next = {};
        for (const w of WIDGET_CATALOG) next[w.id] = on;
        saveWidgetPrefs(next);
        onPrefsChange(next);
    };

    const filtered = filter
        ? WIDGET_CATALOG.filter(w =>
            w.label.toLowerCase().includes(filter.toLowerCase()) ||
            w.desc.toLowerCase().includes(filter.toLowerCase()) ||
            w.category.toLowerCase().includes(filter.toLowerCase())
        )
        : WIDGET_CATALOG;

    const activeCount = Object.values(prefs).filter(Boolean).length;

    return (
        <div style={{
            position: 'fixed', inset: 0, zIndex: 1000,
            background: 'rgba(0,0,0,0.6)', backdropFilter: 'blur(4px)',
        }} onClick={onClose}>
            <div style={{
                position: 'absolute', right: 0, top: 0, bottom: 0,
                width: '340px', maxWidth: '90vw', background: colors.bg,
                borderLeft: `1px solid ${colors.border}`,
                display: 'flex', flexDirection: 'column',
                overflowY: 'auto',
            }} onClick={e => e.stopPropagation()}>
                {/* Header */}
                <div style={{
                    padding: '16px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                }}>
                    <div>
                        <div style={{
                            fontSize: '14px', fontWeight: 700, color: colors.text,
                            fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px',
                        }}>WIDGETS</div>
                        <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                            {activeCount} / {WIDGET_CATALOG.length} active
                        </div>
                    </div>
                    <button onClick={onClose} style={{
                        background: 'none', border: 'none', color: colors.textMuted,
                        fontSize: '20px', cursor: 'pointer', padding: '4px 8px',
                    }}>×</button>
                </div>

                {/* Search */}
                <div style={{ padding: '12px 16px 8px' }}>
                    <input
                        type="text"
                        value={filter}
                        onChange={e => setFilter(e.target.value)}
                        placeholder="Search widgets..."
                        style={{
                            width: '100%', background: colors.card, border: `1px solid ${colors.border}`,
                            borderRadius: '6px', padding: '8px 12px', fontSize: '13px',
                            color: colors.text, outline: 'none', boxSizing: 'border-box',
                            fontFamily: "'JetBrains Mono', monospace",
                        }}
                    />
                </div>

                {/* Bulk actions */}
                <div style={{ padding: '4px 16px 8px', display: 'flex', gap: '8px' }}>
                    <button onClick={() => toggleAll(true)} style={bulkBtn}>Show All</button>
                    <button onClick={() => toggleAll(false)} style={bulkBtn}>Hide All</button>
                </div>

                {/* Widget groups */}
                <div style={{ flex: 1, overflowY: 'auto', padding: '0 16px 16px' }}>
                    {categories.map(cat => {
                        const catWidgets = filtered.filter(w => w.category === cat);
                        if (catWidgets.length === 0) return null;
                        const allOn = catWidgets.every(w => prefs[w.id]);
                        const allOff = catWidgets.every(w => !prefs[w.id]);

                        return (
                            <div key={cat} style={{ marginTop: '12px' }}>
                                <div style={{
                                    display: 'flex', justifyContent: 'space-between',
                                    alignItems: 'center', marginBottom: '6px',
                                }}>
                                    <span style={{
                                        fontSize: '10px', fontWeight: 700, color: colors.accent,
                                        letterSpacing: '1.5px', textTransform: 'uppercase',
                                        fontFamily: "'JetBrains Mono', monospace",
                                    }}>{cat}</span>
                                    <button
                                        onClick={() => toggleCategory(cat, !allOn)}
                                        style={{
                                            ...bulkBtn, fontSize: '10px', padding: '2px 8px',
                                        }}
                                    >{allOn ? 'Hide All' : 'Show All'}</button>
                                </div>

                                {catWidgets.map(w => (
                                    <div key={w.id} style={{
                                        display: 'flex', alignItems: 'center', gap: '10px',
                                        padding: '8px 10px', borderRadius: '8px',
                                        background: prefs[w.id] ? colors.card : 'transparent',
                                        border: `1px solid ${prefs[w.id] ? colors.border : 'transparent'}`,
                                        marginBottom: '4px', cursor: 'pointer',
                                        transition: 'background 0.15s',
                                    }} onClick={() => toggle(w.id)}>
                                        {/* Toggle switch */}
                                        <div style={{
                                            width: '36px', height: '20px', borderRadius: '10px',
                                            background: prefs[w.id] ? colors.accent : '#2A3A50',
                                            position: 'relative', flexShrink: 0,
                                            transition: 'background 0.2s',
                                        }}>
                                            <div style={{
                                                width: '16px', height: '16px', borderRadius: '50%',
                                                background: '#fff', position: 'absolute', top: '2px',
                                                left: prefs[w.id] ? '18px' : '2px',
                                                transition: 'left 0.2s',
                                            }} />
                                        </div>
                                        <div style={{ flex: 1, minWidth: 0 }}>
                                            <div style={{
                                                fontSize: '13px', fontWeight: 600,
                                                color: prefs[w.id] ? colors.text : colors.textMuted,
                                            }}>{w.label}</div>
                                            <div title={w.desc} style={{
                                                fontSize: '10px', color: colors.textMuted,
                                                whiteSpace: 'nowrap', overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                            }}>{w.desc}</div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        );
                    })}
                </div>
            </div>
        </div>
    );
}

const bulkBtn = {
    background: colors.card, border: `1px solid ${colors.border}`,
    borderRadius: '4px', padding: '4px 10px', fontSize: '11px',
    color: colors.textMuted, cursor: 'pointer',
    fontFamily: "'JetBrains Mono', monospace",
};
