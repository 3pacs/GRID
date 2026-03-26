import React, { useState, useEffect, useRef } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';

export default function SystemLogs() {
    const [logs, setLogs] = useState([]);
    const [source, setSource] = useState('api');
    const [autoRefresh, setAutoRefresh] = useState(false);
    const [config, setConfig] = useState(null);
    const [sources, setSources] = useState([]);
    const [activeTab, setActiveTab] = useState('logs');
    const intervalRef = useRef(null);

    useEffect(() => {
        loadLogs();
        return () => clearInterval(intervalRef.current);
    }, [source]);

    useEffect(() => {
        if (autoRefresh) {
            intervalRef.current = setInterval(loadLogs, 5000);
        } else {
            clearInterval(intervalRef.current);
        }
        return () => clearInterval(intervalRef.current);
    }, [autoRefresh, source]);

    const loadLogs = async () => {
        try {
            const result = await api.getLogs(source, 100);
            setLogs(result.logs || result || []);
        } catch (e) { console.warn('[GRID] System:', e.message); }
    };

    const loadConfig = async () => {
        try {
            const [cfg, src] = await Promise.all([
                api.getConfig().catch(() => null),
                api.getSources().catch(() => []),
            ]);
            if (cfg) setConfig(cfg);
            if (Array.isArray(src)) setSources(src);
            else if (src?.sources) setSources(src.sources);
        } catch (e) { console.warn('[GRID] System:', e.message); }
    };

    const toggleSource = async (id, active) => {
        try {
            await api.updateSource(id, { active: !active });
            loadConfig();
        } catch (e) { console.warn('[GRID] System:', e.message); }
    };

    return (
        <div style={shared.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={shared.header}>System</div>
                <ViewHelp id="system" />
            </div>

            <div style={shared.tabs}>
                {['logs', 'config', 'sources'].map(t => (
                    <button key={t} style={shared.tab(activeTab === t)}
                        onClick={() => {
                            setActiveTab(t);
                            if (t === 'config' && !config) loadConfig();
                            if (t === 'sources' && sources.length === 0) loadConfig();
                        }}>
                        {t.charAt(0).toUpperCase() + t.slice(1)}
                    </button>
                ))}
            </div>

            {activeTab === 'logs' && (
                <>
                    <div style={shared.card}>
                        <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
                            {['api', 'hyperspace', 'system'].map(s => (
                                <button key={s} style={shared.tab(source === s)}
                                    onClick={() => setSource(s)}>
                                    {s}
                                </button>
                            ))}
                            <div style={{ marginLeft: 'auto', display: 'flex', gap: '8px', alignItems: 'center' }}>
                                <button style={shared.buttonSmall} onClick={loadLogs}>Refresh</button>
                                <button
                                    style={{
                                        ...shared.buttonSmall,
                                        ...(autoRefresh ? shared.buttonSuccess : {}),
                                    }}
                                    onClick={() => setAutoRefresh(!autoRefresh)}
                                >
                                    {autoRefresh ? 'Auto: ON' : 'Auto: OFF'}
                                </button>
                            </div>
                        </div>
                    </div>

                    <div style={{
                        ...shared.prose,
                        maxHeight: '600px',
                        fontSize: '11px',
                        lineHeight: '1.5',
                    }}>
                        {Array.isArray(logs)
                            ? logs.map((line, i) => (
                                <div key={i} style={{
                                    color: typeof line === 'string' && line.includes('ERROR') ? colors.red
                                        : typeof line === 'string' && line.includes('WARNING') ? colors.yellow
                                        : colors.textDim,
                                }}>
                                    {typeof line === 'string' ? line : JSON.stringify(line)}
                                </div>
                            ))
                            : <div>{JSON.stringify(logs, null, 2)}</div>
                        }
                        {logs.length === 0 && (
                            <div style={{ color: colors.textMuted }}>No logs available</div>
                        )}
                    </div>
                </>
            )}

            {activeTab === 'config' && config && (
                <div style={shared.card}>
                    {Object.entries(config).map(([key, value]) => (
                        <div key={key} style={shared.row}>
                            <span style={{ fontSize: '13px', color: colors.text, fontWeight: 600 }}>{key}</span>
                            <span style={{ fontSize: '12px', color: colors.textDim, fontFamily: colors.mono }}>
                                {typeof value === 'object' ? JSON.stringify(value) : String(value)}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            {activeTab === 'sources' && (
                <div style={shared.card}>
                    {sources.length === 0 ? (
                        <div style={{ color: colors.textMuted, fontSize: '13px' }}>No sources found</div>
                    ) : (
                        sources.map((s, i) => (
                            <div key={s.id || i} style={shared.row}>
                                <div>
                                    <span style={{ fontSize: '13px', fontWeight: 600, color: colors.text }}>
                                        {s.name || s.source_name || `Source ${s.id}`}
                                    </span>
                                    {s.trust_score != null && (
                                        <span style={{ fontSize: '11px', color: colors.textMuted, marginLeft: '8px' }}>
                                            trust: {s.trust_score}
                                        </span>
                                    )}
                                </div>
                                <button
                                    style={{
                                        ...shared.buttonSmall,
                                        ...(s.active ? shared.buttonDanger : shared.buttonSuccess),
                                    }}
                                    onClick={() => toggleSource(s.id, s.active)}
                                >
                                    {s.active ? 'Disable' : 'Enable'}
                                </button>
                            </div>
                        ))
                    )}
                </div>
            )}
        </div>
    );
}
