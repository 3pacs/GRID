import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { colors, shared } from '../styles/shared.js';

const CATEGORIES = [
    'clustering', 'orthogonality', 'regime_detection', 'feature_engineering',
    'feature_importance', 'options_scan', 'conflict_resolution', 'pipeline_summary',
];

const fmt = (v) => {
    if (v == null) return '-';
    if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toFixed(4);
    return String(v);
};

const fmtDate = (d) => d ? d.substring(0, 19).replace('T', ' ') : '-';

export default function Snapshots() {
    const [category, setCategory] = useState(CATEGORIES[0]);
    const [latest, setLatest] = useState(null);
    const [history, setHistory] = useState([]);
    const [compareMode, setCompareMode] = useState(false);
    const [dateA, setDateA] = useState('');
    const [dateB, setDateB] = useState('');
    const [comparison, setComparison] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [expandedId, setExpandedId] = useState(null);

    useEffect(() => {
        loadData();
    }, [category]);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        setComparison(null);
        try {
            const [latestRes, historyRes] = await Promise.all([
                api._fetch('/api/v1/snapshots/latest/' + category + '?n=1').catch(() => null),
                api._fetch('/api/v1/snapshots/history/' + category + '?start_date=&end_date=').catch(() => null),
            ]);
            setLatest(latestRes?.snapshots?.[0] || latestRes || null);
            setHistory(historyRes?.snapshots || historyRes || []);
        } catch (e) {
            setError(e.message || 'Failed to load snapshots');
        }
        setLoading(false);
    };

    const runCompare = async () => {
        if (!dateA || !dateB) return;
        setLoading(true);
        setError(null);
        try {
            const res = await api._fetch(
                '/api/v1/snapshots/compare/' + category + '?date_a=' + dateA + '&date_b=' + dateB
            );
            setComparison(res);
        } catch (e) {
            setError(e.message || 'Comparison failed');
        }
        setLoading(false);
    };

    const extractMetrics = (snap) => {
        if (!snap) return {};
        const payload = snap.payload || snap;
        const out = {};
        for (const [k, v] of Object.entries(payload)) {
            if (typeof v === 'number') out[k] = v;
        }
        if (snap.metrics && typeof snap.metrics === 'object') {
            for (const [k, v] of Object.entries(snap.metrics)) {
                if (typeof v === 'number') out[k] = v;
            }
        }
        return out;
    };

    const allMetricKeys = () => {
        const keys = new Set();
        history.forEach(s => {
            Object.keys(extractMetrics(s)).forEach(k => keys.add(k));
        });
        return [...keys].slice(0, 6);
    };

    const renderMetricBar = (values, key) => {
        if (!values.length) return null;
        const nums = values.map(v => v[key]).filter(n => n != null);
        if (!nums.length) return null;
        const max = Math.max(...nums);
        const min = Math.min(...nums);
        const range = max - min || 1;
        return (
            <div key={key} style={{ marginBottom: '12px' }}>
                <div style={{ fontSize: '11px', color: colors.textMuted, marginBottom: '4px' }}>{key}</div>
                <div style={{ display: 'flex', gap: '2px', alignItems: 'flex-end', height: '32px' }}>
                    {nums.slice(-20).map((v, i) => {
                        const h = Math.max(4, ((v - min) / range) * 28);
                        return (
                            <div key={i} style={{
                                width: '8px', height: `${h}px`, borderRadius: '2px',
                                background: i === nums.length - 1 ? colors.accent : colors.textMuted + '66',
                            }} title={fmt(v)} />
                        );
                    })}
                </div>
            </div>
        );
    };

    const renderDelta = (a, b) => {
        if (a == null || b == null) return <span style={{ color: colors.textMuted }}>-</span>;
        const delta = b - a;
        if (delta === 0) return <span style={{ color: colors.textMuted }}>0</span>;
        const color = delta > 0 ? colors.green : colors.red;
        return <span style={{ color, fontFamily: colors.mono, fontSize: '12px' }}>
            {delta > 0 ? '+' : ''}{fmt(delta)}
        </span>;
    };

    return (
        <div style={shared.container}>
            <div style={shared.header}>Snapshots</div>

            <div style={{ ...shared.tabs, flexWrap: 'wrap' }}>
                {CATEGORIES.map(c => (
                    <button key={c} style={shared.tab(category === c)}
                        onClick={() => { setCategory(c); setCompareMode(false); }}>
                        {c.replace(/_/g, ' ')}
                    </button>
                ))}
            </div>

            {error && <div style={shared.error}>{error}</div>}
            {loading && <div style={{ color: colors.textMuted, fontSize: '13px', padding: '12px' }}>Loading...</div>}

            {/* Latest Snapshot */}
            {latest && !loading && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>LATEST SNAPSHOT</div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                        <span style={{ fontSize: '12px', color: colors.textMuted }}>
                            {fmtDate(latest.created_at || latest.snapshot_date)}
                        </span>
                        <span style={shared.badge(colors.accent)}>{category.replace(/_/g, ' ')}</span>
                    </div>
                    <div style={shared.metricGrid}>
                        {Object.entries(extractMetrics(latest)).slice(0, 8).map(([k, v]) => (
                            <div key={k} style={shared.metric}>
                                <div style={shared.metricValue}>{fmt(v)}</div>
                                <div style={shared.metricLabel}>{k.replace(/_/g, ' ')}</div>
                            </div>
                        ))}
                    </div>
                    {latest.payload && typeof latest.payload === 'object' && (
                        <div style={{ ...shared.prose, marginTop: '10px', maxHeight: '200px' }}>
                            {JSON.stringify(latest.payload, null, 2)}
                        </div>
                    )}
                </div>
            )}

            {/* Controls row */}
            {!loading && (
                <div style={{ display: 'flex', gap: '8px', marginBottom: '12px' }}>
                    <button style={{
                        ...shared.buttonSmall,
                        ...(compareMode ? shared.buttonSuccess : {}),
                    }} onClick={() => setCompareMode(!compareMode)}>
                        {compareMode ? 'Compare: ON' : 'Compare'}
                    </button>
                    <button style={shared.buttonSmall} onClick={loadData}>Refresh</button>
                </div>
            )}

            {/* Compare Mode */}
            {compareMode && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>COMPARE SNAPSHOTS</div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: '8px', alignItems: 'end' }}>
                        <div>
                            <div style={shared.label}>Date A</div>
                            <input type="date" style={shared.input} value={dateA}
                                onChange={e => setDateA(e.target.value)} />
                        </div>
                        <div>
                            <div style={shared.label}>Date B</div>
                            <input type="date" style={shared.input} value={dateB}
                                onChange={e => setDateB(e.target.value)} />
                        </div>
                        <button style={shared.buttonSmall} onClick={runCompare}
                            disabled={!dateA || !dateB}>Compare</button>
                    </div>
                    {comparison && (
                        <div style={{ marginTop: '12px' }}>
                            <div style={{
                                display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr',
                                gap: '4px', fontSize: '12px', padding: '6px 0',
                                borderBottom: `1px solid ${colors.border}`,
                            }}>
                                <span style={{ color: colors.textMuted, fontWeight: 600 }}>Metric</span>
                                <span style={{ color: colors.textMuted, fontWeight: 600 }}>Date A</span>
                                <span style={{ color: colors.textMuted, fontWeight: 600 }}>Date B</span>
                                <span style={{ color: colors.textMuted, fontWeight: 600 }}>Delta</span>
                            </div>
                            {Object.entries(comparison.metrics_a || comparison.a || {}).map(([k, valA]) => {
                                const valB = (comparison.metrics_b || comparison.b || {})[k];
                                return (
                                    <div key={k} style={{
                                        display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr',
                                        gap: '4px', fontSize: '12px', padding: '6px 0',
                                        borderBottom: `1px solid ${colors.border}`,
                                    }}>
                                        <span style={{ color: colors.textDim }}>{k.replace(/_/g, ' ')}</span>
                                        <span style={shared.value}>{fmt(valA)}</span>
                                        <span style={shared.value}>{fmt(valB)}</span>
                                        {renderDelta(
                                            typeof valA === 'number' ? valA : null,
                                            typeof valB === 'number' ? valB : null
                                        )}
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </div>
            )}

            {/* Metrics Trending */}
            {history.length > 1 && !loading && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>METRICS TRENDING</div>
                    {allMetricKeys().map(key => {
                        const values = history.map(s => extractMetrics(s));
                        return renderMetricBar(values, key);
                    })}
                    {allMetricKeys().length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '13px' }}>No numeric metrics to trend</div>
                    )}
                </div>
            )}

            {/* History Timeline */}
            {!loading && (
                <div style={{ marginTop: '4px' }}>
                    <div style={shared.sectionTitle}>HISTORY</div>
                    {history.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '13px', padding: '12px', textAlign: 'center' }}>
                            No snapshots found for {category.replace(/_/g, ' ')}
                        </div>
                    )}
                    {history.map((snap, i) => {
                        const metrics = extractMetrics(snap);
                        const id = snap.id || i;
                        const expanded = expandedId === id;
                        return (
                            <div key={id} style={{
                                ...shared.card, cursor: 'pointer', marginBottom: '6px',
                            }} onClick={() => setExpandedId(expanded ? null : id)}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span style={{ fontSize: '13px', color: colors.text, fontFamily: colors.mono }}>
                                        {fmtDate(snap.created_at || snap.snapshot_date)}
                                    </span>
                                    <div style={{ display: 'flex', gap: '8px' }}>
                                        {Object.entries(metrics).slice(0, 3).map(([k, v]) => (
                                            <span key={k} style={{ fontSize: '11px', color: colors.textDim }}>
                                                {k.replace(/_/g, ' ')}: <span style={{ color: colors.text }}>{fmt(v)}</span>
                                            </span>
                                        ))}
                                    </div>
                                </div>
                                {expanded && snap.payload && (
                                    <div style={{ ...shared.prose, marginTop: '8px', maxHeight: '300px' }}>
                                        {JSON.stringify(snap.payload, null, 2)}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
