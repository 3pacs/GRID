import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

const groupColors = {
    ingestion: '#1A6EBF',
    features: '#B8922A',
    discovery: '#8B5CF6',
    physics: '#22C55E',
    validation: '#EF4444',
    governance: '#F59E0B',
};

export default function Workflows() {
    const [workflows, setWorkflows] = useState([]);
    const [waves, setWaves] = useState(null);
    const [schedule, setSchedule] = useState(null);
    const [running, setRunning] = useState({});
    const [messages, setMessages] = useState({});
    const [activeTab, setActiveTab] = useState('all');

    useEffect(() => {
        loadWorkflows();
        api.getWorkflowWaves().then(setWaves).catch(() => {});
        api.getWorkflowSchedule().then(setSchedule).catch(() => {});
    }, []);

    const loadWorkflows = async () => {
        try {
            const result = await api.getWorkflows();
            setWorkflows(result.workflows || []);
        } catch (e) { console.warn('[GRID] Workflows:', e.message); }
    };

    const toggleWorkflow = async (name, enabled) => {
        try {
            if (enabled) {
                await api.disableWorkflow(name);
            } else {
                await api.enableWorkflow(name);
            }
            loadWorkflows();
        } catch (e) { console.warn('[GRID] Workflows:', e.message); }
    };

    const runWorkflow = async (name) => {
        setRunning(prev => ({ ...prev, [name]: true }));
        try {
            const result = await api.runWorkflow(name);
            setMessages(prev => ({ ...prev, [name]: result.message || 'Triggered' }));
        } catch (e) {
            setMessages(prev => ({ ...prev, [name]: `Error: ${e.message}` }));
        }
        setRunning(prev => ({ ...prev, [name]: false }));
    };

    const groups = [...new Set(workflows.map(w => w.group))];
    const filtered = activeTab === 'all'
        ? workflows
        : workflows.filter(w => w.group === activeTab);

    const enabledCount = workflows.filter(w => w.enabled).length;

    return (
        <div style={shared.container}>
            <div style={shared.header}>Workflows</div>

            {/* Summary */}
            <div style={shared.metricGrid}>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{workflows.length}</div>
                    <div style={shared.metricLabel}>Total</div>
                </div>
                <div style={shared.metric}>
                    <div style={{ ...shared.metricValue, color: colors.green }}>{enabledCount}</div>
                    <div style={shared.metricLabel}>Enabled</div>
                </div>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{waves?.total_waves || 0}</div>
                    <div style={shared.metricLabel}>Waves</div>
                </div>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{schedule?.total || 0}</div>
                    <div style={shared.metricLabel}>Scheduled</div>
                </div>
            </div>

            {/* Group Tabs */}
            <div style={{ ...shared.tabs, marginTop: '16px' }}>
                <button style={shared.tab(activeTab === 'all')} onClick={() => setActiveTab('all')}>
                    All
                </button>
                {groups.map(g => (
                    <button key={g} style={shared.tab(activeTab === g)} onClick={() => setActiveTab(g)}>
                        {g}
                    </button>
                ))}
            </div>

            {/* Workflow List */}
            <div style={shared.card}>
                {filtered.map(wf => (
                    <div key={wf.name} style={{ ...shared.row, flexWrap: 'wrap', gap: '8px' }}>
                        <div style={{ flex: 1, minWidth: '200px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <span style={shared.badge(groupColors[wf.group] || colors.border)}>
                                    {wf.group}
                                </span>
                                <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                                    {wf.name}
                                </span>
                            </div>
                            <div style={{ fontSize: '12px', color: colors.textMuted, marginTop: '4px' }}>
                                {wf.description}
                            </div>
                            {wf.schedule && (
                                <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                                    Schedule: {wf.schedule}
                                </div>
                            )}
                            {messages[wf.name] && (
                                <div style={{ fontSize: '11px', color: colors.accent, marginTop: '4px' }}>
                                    {messages[wf.name]}
                                </div>
                            )}
                        </div>
                        <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                            <button
                                style={{
                                    ...shared.buttonSmall,
                                    ...(wf.enabled ? shared.buttonDanger : shared.buttonSuccess),
                                }}
                                onClick={() => toggleWorkflow(wf.name, wf.enabled)}
                            >
                                {wf.enabled ? 'Disable' : 'Enable'}
                            </button>
                            <button
                                style={{
                                    ...shared.buttonSmall,
                                    ...(running[wf.name] ? shared.buttonDisabled : {}),
                                }}
                                onClick={() => runWorkflow(wf.name)}
                                disabled={running[wf.name]}
                            >
                                {running[wf.name] ? 'Running...' : 'Run'}
                            </button>
                        </div>
                    </div>
                ))}
                {filtered.length === 0 && (
                    <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                        No workflows found
                    </div>
                )}
            </div>

            {/* Wave Plan */}
            {waves?.waves?.length > 0 && (
                <>
                    <div style={shared.sectionTitle}>Execution Wave Plan</div>
                    <div style={shared.card}>
                        {waves.waves.map((w, i) => (
                            <div key={i} style={{ ...shared.row }}>
                                <div>
                                    <span style={shared.badge(w.parallel ? '#8B5CF6' : colors.border)}>
                                        Wave {w.wave}
                                    </span>
                                    <span style={{ fontSize: '11px', color: colors.textMuted, marginLeft: '8px' }}>
                                        {w.parallel ? 'parallel' : 'sequential'}
                                    </span>
                                </div>
                                <div style={{ fontSize: '12px', color: colors.text }}>
                                    {w.tasks.join(', ')}
                                </div>
                            </div>
                        ))}
                    </div>
                </>
            )}

            {/* Schedule */}
            {schedule?.schedules?.length > 0 && (
                <>
                    <div style={shared.sectionTitle}>Schedule</div>
                    <div style={shared.card}>
                        {schedule.schedules.map((s, i) => (
                            <div key={i} style={shared.row}>
                                <div>
                                    <span style={{ fontSize: '13px', color: colors.text }}>{s.name}</span>
                                    <span style={{ fontSize: '11px', color: colors.textMuted, marginLeft: '8px' }}>
                                        {s.group}
                                    </span>
                                </div>
                                <span style={{ fontSize: '12px', color: colors.textDim, fontFamily: colors.mono }}>
                                    {s.cron || s.frequency || 'manual'}
                                </span>
                            </div>
                        ))}
                    </div>
                </>
            )}
        </div>
    );
}
