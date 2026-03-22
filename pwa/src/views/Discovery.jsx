import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    controls: {
        display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', marginBottom: '20px',
    },
    runBtn: {
        padding: '14px', borderRadius: '8px', border: '1px solid #1A6EBF',
        background: '#1A6EBF22', color: '#1A6EBF', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '12px', fontWeight: 600, cursor: 'pointer', minHeight: '44px',
    },
    section: { marginBottom: '20px' },
    sectionTitle: {
        fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '10px',
    },
    jobRow: {
        background: '#0D1520', borderRadius: '8px', padding: '10px 14px',
        border: '1px solid #1A2840', marginBottom: '6px',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    },
    statusChip: {
        fontSize: '10px', fontWeight: 600, padding: '2px 8px', borderRadius: '4px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    resultCard: {
        background: '#0D1520', borderRadius: '10px', padding: '16px',
        border: '1px solid #1A2840', marginBottom: '12px',
    },
    metricRow: {
        display: 'flex', justifyContent: 'space-between', padding: '4px 0',
        fontSize: '13px',
    },
    hypothesisRow: {
        background: '#0D1520', borderRadius: '8px', padding: '10px 14px',
        border: '1px solid #1A2840', marginBottom: '6px',
    },
    filterTabs: {
        display: 'flex', gap: '4px', marginBottom: '12px', overflowX: 'auto',
    },
    filterTab: {
        padding: '4px 12px', borderRadius: '4px', border: '1px solid #1A2840',
        background: 'transparent', color: '#5A7080', fontSize: '11px',
        fontFamily: "'JetBrains Mono', monospace", cursor: 'pointer',
        whiteSpace: 'nowrap',
    },
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

    useEffect(() => {
        loadData();
    }, []);

    useEffect(() => {
        loadHypotheses();
    }, [hypoFilter]);

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
        <div style={styles.container}>
            <div style={styles.title}>DISCOVERY</div>

            <div style={styles.controls}>
                <button style={styles.runBtn} onClick={triggerOrtho} disabled={running.ortho}>
                    {running.ortho ? '...' : 'ORTHOGONALITY AUDIT'}
                </button>
                <button style={styles.runBtn} onClick={triggerCluster} disabled={running.cluster}>
                    {running.cluster ? '...' : 'CLUSTER DISCOVERY'}
                </button>
            </div>

            {jobs.length > 0 && (
                <div style={styles.section}>
                    <div style={styles.sectionTitle}>JOBS</div>
                    {jobs.slice(0, 5).map(j => {
                        const sc = jobStatusColors[j.status] || jobStatusColors.queued;
                        return (
                            <div key={j.id} style={styles.jobRow}>
                                <div>
                                    <span style={{ fontSize: '13px', fontFamily: "'JetBrains Mono', monospace" }}>
                                        {j.type}
                                    </span>
                                    <span style={{ fontSize: '11px', color: '#5A7080', marginLeft: '8px' }}>
                                        {j.started?.substring(11, 19)}
                                    </span>
                                </div>
                                <span style={{ ...styles.statusChip, background: sc.bg, color: sc.color }}>
                                    {j.status?.toUpperCase()}
                                </span>
                            </div>
                        );
                    })}
                </div>
            )}

            {orthoResult && !orthoResult.error && (
                <div style={styles.resultCard}>
                    <div style={styles.sectionTitle}>ORTHOGONALITY</div>
                    <div style={styles.metricRow}>
                        <span style={{ color: '#5A7080' }}>Features analyzed</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>
                            {orthoResult.n_features_analyzed}
                        </span>
                    </div>
                    <div style={styles.metricRow}>
                        <span style={{ color: '#5A7080' }}>True dimensionality</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", color: '#B8922A' }}>
                            {orthoResult.true_dimensionality}
                        </span>
                    </div>
                    <div style={styles.metricRow}>
                        <span style={{ color: '#5A7080' }}>Correlated pairs</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>
                            {orthoResult.highly_correlated_pairs?.length || 0}
                        </span>
                    </div>
                </div>
            )}

            {clusterResult && !clusterResult.error && (
                <div style={styles.resultCard}>
                    <div style={styles.sectionTitle}>CLUSTERING</div>
                    <div style={styles.metricRow}>
                        <span style={{ color: '#5A7080' }}>Best k</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", color: '#B8922A' }}>
                            {clusterResult.best_k}
                        </span>
                    </div>
                    <div style={styles.metricRow}>
                        <span style={{ color: '#5A7080' }}>PCA components</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>
                            {clusterResult.pca_components_used}
                        </span>
                    </div>
                    <div style={styles.metricRow}>
                        <span style={{ color: '#5A7080' }}>Variance explained</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>
                            {(clusterResult.variance_explained * 100).toFixed(1)}%
                        </span>
                    </div>
                </div>
            )}

            <div style={styles.section}>
                <div style={styles.sectionTitle}>HYPOTHESES</div>
                <div style={styles.filterTabs}>
                    {HYPO_STATES.map(s => (
                        <button key={s} onClick={() => setHypoFilter(s)}
                            style={{
                                ...styles.filterTab,
                                ...(hypoFilter === s ? { borderColor: '#1A6EBF', color: '#1A6EBF' } : {}),
                            }}>
                            {s}
                        </button>
                    ))}
                </div>
                {hypotheses.map((h, i) => (
                    <div key={h.id || i} style={styles.hypothesisRow}>
                        <div style={{
                            fontSize: '13px', color: '#C8D8E8', marginBottom: '4px',
                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        }}>
                            {h.statement}
                        </div>
                        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                            <span style={{
                                ...styles.statusChip,
                                background: '#1A284044', color: '#5A7080',
                            }}>
                                {h.state}
                            </span>
                            <span style={{ fontSize: '11px', color: '#5A7080' }}>
                                {h.created_at?.substring(0, 10)}
                            </span>
                        </div>
                    </div>
                ))}
                {hypotheses.length === 0 && (
                    <div style={{ color: '#5A7080', textAlign: 'center', padding: '20px', fontSize: '13px' }}>
                        No hypotheses found
                    </div>
                )}
            </div>
        </div>
    );
}
