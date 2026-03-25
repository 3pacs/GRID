import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, tokens, shared } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

const hypoStateColors = {
    CANDIDATE: { bg: '#1A6EBF22', color: '#1A6EBF' },
    TESTING: { bg: '#F59E0B22', color: '#F59E0B' },
    PASSED: { bg: '#22C55E22', color: '#22C55E' },
    FAILED: { bg: '#EF444422', color: '#EF4444' },
    KILLED: { bg: '#5A708022', color: '#5A7080' },
};

const jobStatusColors = {
    queued: { bg: '#5A708033', color: '#5A7080' },
    running: { bg: '#1A6EBF33', color: '#1A6EBF' },
    complete: { bg: '#1A7A4A33', color: '#1A7A4A' },
    failed: { bg: '#8B1F1F33', color: '#8B1F1F' },
};

const HYPO_STATES = ['ALL', 'CANDIDATE', 'TESTING', 'PASSED', 'FAILED', 'KILLED'];

export default function Discovery() {
    const { jobs, hypotheses, setJobs, setHypotheses, addNotification } = useStore();
    const [orthoResult, setOrthoResult] = useState(null);
    const [clusterResult, setClusterResult] = useState(null);
    const [nComponents, setNComponents] = useState(3);
    const [hypoFilter, setHypoFilter] = useState('ALL');
    const [running, setRunning] = useState({});
    const { isMobile } = useDevice();

    useEffect(() => { loadData(); }, []);
    useEffect(() => { loadHypotheses(); }, [hypoFilter]);

    const loadData = async () => {
        try {
            const [j, ortho, cluster] = await Promise.all([
                api.getJobs().catch(() => ({ jobs: [] })),
                api.getResults('orthogonality').catch(() => null),
                api.getResults('clustering').catch(() => null),
            ]);
            setJobs(j.jobs || []);
            if (ortho?.result) setOrthoResult(ortho.result);
            if (cluster?.result) setClusterResult(cluster.result);
        } catch (err) {
            addNotification('error', 'Failed to load discovery data');
        }
        loadHypotheses();
    };

    const loadHypotheses = async () => {
        const params = {};
        if (hypoFilter !== 'ALL') params.state = hypoFilter;
        try {
            const data = await api.getHypotheses(params);
            setHypotheses(data.hypotheses || []);
        } catch (err) {
            addNotification('error', 'Failed to load hypotheses');
        }
    };

    const triggerOrtho = async () => {
        setRunning(r => ({ ...r, ortho: true }));
        try {
            await api.triggerOrthogonality();
            addNotification('info', 'Orthogonality audit started');
        } catch (err) {
            addNotification('error', err.message);
        }
        setRunning(r => ({ ...r, ortho: false }));
    };

    const triggerCluster = async () => {
        setRunning(r => ({ ...r, cluster: true }));
        try {
            await api.triggerClustering(nComponents);
            addNotification('info', 'Cluster discovery started');
        } catch (err) {
            addNotification('error', err.message);
        }
        setRunning(r => ({ ...r, cluster: false }));
    };

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            <div style={{
                fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg,
                color: colors.textMuted, letterSpacing: '2px', marginBottom: tokens.space.lg,
            }}>
                DISCOVERY
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr',
                gap: tokens.space.md, marginBottom: tokens.space.xl,
            }}>
                <button style={{
                    padding: tokens.space.lg, borderRadius: tokens.radius.md,
                    border: `1px solid ${colors.accent}`,
                    background: `linear-gradient(135deg, ${colors.accentGlow} 0%, transparent 100%)`,
                    color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    fontSize: tokens.fontSize.md, fontWeight: 600, cursor: 'pointer',
                    minHeight: tokens.minTouch, transition: `all ${tokens.transition.fast}`,
                }} onClick={triggerOrtho} disabled={running.ortho}>
                    {running.ortho ? 'Running...' : 'ORTHOGONALITY AUDIT'}
                </button>
                <button style={{
                    padding: tokens.space.lg, borderRadius: tokens.radius.md,
                    border: `1px solid ${colors.accent}`,
                    background: `linear-gradient(135deg, ${colors.accentGlow} 0%, transparent 100%)`,
                    color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    fontSize: tokens.fontSize.md, fontWeight: 600, cursor: 'pointer',
                    minHeight: tokens.minTouch, transition: `all ${tokens.transition.fast}`,
                }} onClick={triggerCluster} disabled={running.cluster}>
                    {running.cluster ? 'Running...' : 'CLUSTER DISCOVERY'}
                </button>
            </div>

            {jobs.length > 0 && (
                <div style={{ marginBottom: tokens.space.xl }}>
                    <div style={shared.sectionTitle}>JOBS</div>
                    {jobs.slice(0, 5).map(j => {
                        const sc = jobStatusColors[j.status] || jobStatusColors.queued;
                        return (
                            <div key={j.id} style={{
                                ...shared.card, display: 'flex',
                                justifyContent: 'space-between', alignItems: 'center',
                                minHeight: tokens.minTouch,
                            }}>
                                <div>
                                    <span style={{
                                        fontSize: tokens.fontSize.md,
                                        fontFamily: "'JetBrains Mono', monospace",
                                        color: colors.text,
                                    }}>
                                        {j.type}
                                    </span>
                                    <span style={{
                                        fontSize: tokens.fontSize.xs, color: colors.textMuted,
                                        marginLeft: tokens.space.sm,
                                    }}>
                                        {j.started?.substring(11, 19)}
                                    </span>
                                </div>
                                <span style={{
                                    fontSize: tokens.fontSize.xs, fontWeight: 600,
                                    padding: '3px 10px', borderRadius: tokens.radius.sm,
                                    fontFamily: "'JetBrains Mono', monospace",
                                    background: sc.bg, color: sc.color,
                                }}>
                                    {j.status?.toUpperCase()}
                                </span>
                            </div>
                        );
                    })}
                </div>
            )}

            {orthoResult && !orthoResult.error && (
                <div style={shared.cardGradient}>
                    <div style={shared.sectionTitle}>ORTHOGONALITY</div>
                    {[
                        { label: 'Features analyzed', value: orthoResult.n_features_analyzed },
                        { label: 'True dimensionality', value: orthoResult.true_dimensionality, accent: true },
                        { label: 'Correlated pairs', value: orthoResult.highly_correlated_pairs?.length || 0 },
                    ].map((m, i) => (
                        <div key={i} style={{
                            display: 'flex', justifyContent: 'space-between',
                            alignItems: 'center', padding: '10px 0',
                            borderBottom: i < 2 ? `1px solid ${colors.borderSubtle}` : 'none',
                            minHeight: '40px',
                        }}>
                            <span style={{ color: colors.textMuted, fontSize: tokens.fontSize.md }}>{m.label}</span>
                            <span style={{
                                fontFamily: "'JetBrains Mono', monospace",
                                fontSize: '14px',
                                color: m.accent ? colors.yellow : colors.text,
                                fontWeight: m.accent ? 700 : 400,
                            }}>
                                {m.value}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            {clusterResult && !clusterResult.error && (
                <div style={shared.cardGradient}>
                    <div style={shared.sectionTitle}>CLUSTERING</div>
                    {[
                        { label: 'Best k', value: clusterResult.best_k, accent: true },
                        { label: 'PCA components', value: clusterResult.pca_components_used },
                        { label: 'Variance explained', value: `${(clusterResult.variance_explained * 100).toFixed(1)}%` },
                    ].map((m, i) => (
                        <div key={i} style={{
                            display: 'flex', justifyContent: 'space-between',
                            alignItems: 'center', padding: '10px 0',
                            borderBottom: i < 2 ? `1px solid ${colors.borderSubtle}` : 'none',
                            minHeight: '40px',
                        }}>
                            <span style={{ color: colors.textMuted, fontSize: tokens.fontSize.md }}>{m.label}</span>
                            <span style={{
                                fontFamily: "'JetBrains Mono', monospace",
                                fontSize: '14px',
                                color: m.accent ? colors.yellow : colors.text,
                                fontWeight: m.accent ? 700 : 400,
                            }}>
                                {m.value}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            <div style={{ marginBottom: tokens.space.xl }}>
                <div style={shared.sectionTitle}>HYPOTHESES</div>
                <div style={{
                    ...shared.tabs, marginBottom: tokens.space.md,
                }}>
                    {HYPO_STATES.map(st => {
                        const isActive = hypoFilter === st;
                        const sc = st !== 'ALL' ? hypoStateColors[st] : null;
                        return (
                            <button key={st} onClick={() => setHypoFilter(st)}
                                style={{
                                    padding: '8px 14px', borderRadius: tokens.radius.sm,
                                    border: `1px solid ${isActive ? (sc?.color || colors.accent) : colors.border}`,
                                    background: isActive ? (sc?.bg || colors.accentGlow) : 'transparent',
                                    color: isActive ? (sc?.color || colors.accent) : colors.textMuted,
                                    fontSize: tokens.fontSize.sm,
                                    fontFamily: "'JetBrains Mono', monospace",
                                    cursor: 'pointer', whiteSpace: 'nowrap',
                                    minHeight: '36px', transition: `all ${tokens.transition.fast}`,
                                }}>
                                {st}
                            </button>
                        );
                    })}
                </div>
                {hypotheses.map((h, i) => {
                    const sc = hypoStateColors[h.state] || hypoStateColors.KILLED;
                    return (
                        <div key={h.id || i} style={{
                            ...shared.card, minHeight: '52px',
                        }}>
                            <div style={{
                                fontSize: tokens.fontSize.md, color: colors.text,
                                marginBottom: tokens.space.xs,
                                display: '-webkit-box', WebkitLineClamp: 2,
                                WebkitBoxOrient: 'vertical', overflow: 'hidden',
                                lineHeight: '1.4',
                            }}>
                                {h.statement}
                            </div>
                            <div style={{ display: 'flex', gap: tokens.space.sm, alignItems: 'center' }}>
                                <span style={{
                                    fontSize: tokens.fontSize.xs, fontWeight: 600,
                                    padding: '3px 10px', borderRadius: tokens.radius.sm,
                                    fontFamily: "'JetBrains Mono', monospace",
                                    background: sc.bg, color: sc.color,
                                }}>
                                    {h.state}
                                </span>
                                <span style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>
                                    {h.created_at?.substring(0, 10)}
                                </span>
                            </div>
                        </div>
                    );
                })}
                {hypotheses.length === 0 && (
                    <div style={{
                        color: colors.textMuted, textAlign: 'center',
                        padding: tokens.space.xl, fontSize: tokens.fontSize.md,
                    }}>
                        No hypotheses found
                    </div>
                )}
            </div>
        </div>
    );
}
