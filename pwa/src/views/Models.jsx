import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import DecisionModal from '../components/DecisionModal.jsx';
import KillSwitch from '../components/KillSwitch.jsx';

const stateColors = {
    PRODUCTION: '#1A7A4A', STAGING: '#1A6EBF', SHADOW: '#8A6000',
    CANDIDATE: '#5A7080', RETIRED: '#333', FLAGGED: '#8B1F1F',
};

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    prodCards: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px', marginBottom: '20px' },
    prodCard: {
        background: '#0D1520', borderRadius: '8px', padding: '12px',
        border: '1px solid #1A2840', textAlign: 'center',
    },
    layerLabel: {
        fontSize: '10px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        marginBottom: '6px',
    },
    modelName: {
        fontSize: '13px', color: '#C8D8E8', fontFamily: "'JetBrains Mono', monospace",
        fontWeight: 500,
    },
    group: { marginBottom: '16px' },
    groupTitle: {
        fontSize: '11px', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '8px',
    },
    modelRow: {
        background: '#0D1520', borderRadius: '8px', padding: '12px 14px',
        border: '1px solid #1A2840', marginBottom: '6px', cursor: 'pointer',
    },
    chip: {
        fontSize: '10px', fontWeight: 600, padding: '2px 8px', borderRadius: '4px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    detail: {
        marginTop: '10px', paddingTop: '10px', borderTop: '1px solid #1A2840',
    },
    actionBtn: {
        padding: '8px 16px', borderRadius: '6px', border: '1px solid #1A2840',
        background: 'transparent', color: '#1A6EBF', fontSize: '12px',
        fontFamily: "'JetBrains Mono', monospace", cursor: 'pointer',
        marginRight: '8px', marginTop: '8px', minHeight: '36px',
    },
};

const TRANSITIONS = {
    CANDIDATE: [{ label: 'Begin Shadow', target: 'SHADOW' }],
    SHADOW: [{ label: 'Promote to Staging', target: 'STAGING' }],
    STAGING: [{ label: 'Promote to Production', target: 'PRODUCTION', confirm: true }],
    PRODUCTION: [],
    FLAGGED: [],
};

export default function Models() {
    const { allModels, productionModels, setAllModels, setProductionModels, addNotification } = useStore();
    const [expanded, setExpanded] = useState(null);
    const [confirmAction, setConfirmAction] = useState(null);

    useEffect(() => {
        loadModels();
    }, []);

    const loadModels = async () => {
        try {
            const [all, prod] = await Promise.all([
                api.getModels(),
                api.getProductionModels(),
            ]);
            setAllModels(all.models || []);
            setProductionModels(prod.models || {});
        } catch (e) { console.warn('[GRID] Models:', e.message); }
    };

    const handleTransition = async (modelId, newState) => {
        try {
            await api.transitionModel(modelId, { new_state: newState, reason: 'PWA operator' });
            addNotification('success', `Model transitioned to ${newState}`);
            loadModels();
        } catch (err) {
            addNotification('error', err.message || 'Transition failed');
        }
        setConfirmAction(null);
    };

    const handleKill = async (modelId) => {
        try {
            await api.transitionModel(modelId, { new_state: 'RETIRED', reason: 'Kill switch' });
            addNotification('success', 'Model retired');
            loadModels();
        } catch (err) {
            addNotification('error', err.message || 'Kill failed');
        }
    };

    const grouped = {};
    (allModels || []).forEach(m => {
        const state = m.state || 'UNKNOWN';
        if (!grouped[state]) grouped[state] = [];
        grouped[state].push(m);
    });

    const stateOrder = ['PRODUCTION', 'STAGING', 'SHADOW', 'CANDIDATE', 'FLAGGED', 'RETIRED'];

    return (
        <div style={styles.container}>
            <div style={styles.title}>MODEL REGISTRY</div>

            <div style={styles.prodCards}>
                {['REGIME', 'TACTICAL', 'EXECUTION'].map(layer => {
                    const model = productionModels?.[layer];
                    return (
                        <div key={layer} style={styles.prodCard}>
                            <div style={styles.layerLabel}>{layer}</div>
                            <div style={styles.modelName}>
                                {model ? `${model.name}` : 'None'}
                            </div>
                            {model && (
                                <div style={{ fontSize: '10px', color: '#5A7080', marginTop: '2px' }}>
                                    v{model.version}
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>

            {stateOrder.map(state => {
                const models = grouped[state];
                if (!models?.length) return null;
                const color = stateColors[state] || '#5A7080';
                return (
                    <div key={state} style={styles.group}>
                        <div style={{ ...styles.groupTitle, color }}>{state}</div>
                        {models.map(m => (
                            <div key={m.id} style={styles.modelRow}
                                onClick={() => setExpanded(expanded === m.id ? null : m.id)}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <div>
                                        <span style={{ fontSize: '14px', fontFamily: "'JetBrains Mono', monospace" }}>
                                            {m.name}
                                        </span>
                                        <span style={{ fontSize: '12px', color: '#5A7080', marginLeft: '8px' }}>
                                            v{m.version}
                                        </span>
                                    </div>
                                    <span style={{
                                        ...styles.chip,
                                        background: `${color}22`, color,
                                    }}>
                                        {m.layer}
                                    </span>
                                </div>
                                {expanded === m.id && (
                                    <div style={styles.detail}>
                                        <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '8px' }}>
                                            Created: {m.created_at?.substring(0, 10)}
                                        </div>
                                        {(TRANSITIONS[state] || []).map(t => (
                                            <button key={t.target} style={styles.actionBtn}
                                                onClick={e => {
                                                    e.stopPropagation();
                                                    if (t.confirm) {
                                                        setConfirmAction({ modelId: m.id, target: t.target, label: t.label });
                                                    } else {
                                                        handleTransition(m.id, t.target);
                                                    }
                                                }}>
                                                {t.label}
                                            </button>
                                        ))}
                                        <button style={{ ...styles.actionBtn, color: '#8B1F1F', borderColor: '#8B1F1F44' }}
                                            onClick={e => { e.stopPropagation(); setConfirmAction({ modelId: m.id, target: 'RETIRED', label: 'Retire' }); }}>
                                            Retire
                                        </button>
                                        {state === 'PRODUCTION' && (
                                            <div style={{ marginTop: '12px' }}>
                                                <KillSwitch modelId={m.id} modelName={m.name} onKill={handleKill} />
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                );
            })}

            {confirmAction && (
                <DecisionModal
                    title={confirmAction.label}
                    body={`This will transition the model to ${confirmAction.target}. Are you sure?`}
                    confirmLabel="CONFIRM"
                    confirmColor={confirmAction.target === 'RETIRED' ? '#8B1F1F' : '#1A6EBF'}
                    onConfirm={() => handleTransition(confirmAction.modelId, confirmAction.target)}
                    onCancel={() => setConfirmAction(null)}
                />
            )}
        </div>
    );
}
