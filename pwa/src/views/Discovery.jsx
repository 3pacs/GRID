import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, tokens, shared } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';
import ViewHelp from '../components/ViewHelp.jsx';
import { useAsyncData } from '../hooks/useAsyncData.js';
import LoadingSkeleton from '../components/LoadingSkeleton.jsx';
import ErrorState from '../components/ErrorState.jsx';

const hypoStateColors = {
    CANDIDATE: { bg: '#1A6EBF22', color: '#1A6EBF' },
    TESTING: { bg: '#F59E0B22', color: '#F59E0B' },
    PASSED: { bg: '#22C55E22', color: '#22C55E' },
    FAILED: { bg: '#EF444422', color: '#EF4444' },
    KILLED: { bg: '#5A708022', color: '#5A7080' },
    PROMOTED: { bg: '#A855F722', color: '#A855F7' },
};

const jobStatusColors = {
    queued: { bg: '#5A708033', color: '#5A7080' },
    running: { bg: '#1A6EBF33', color: '#1A6EBF' },
    complete: { bg: '#1A7A4A33', color: '#1A7A4A' },
    failed: { bg: '#8B1F1F33', color: '#8B1F1F' },
};

const HYPO_STATES = ['ALL', 'CANDIDATE', 'TESTING', 'PASSED', 'FAILED', 'KILLED', 'PROMOTED'];

async function loadAuditResult(type) {
    try {
        const response = await api.getResults(type);
        if (!response || response.error || !Object.hasOwn(response, 'result')) {
            return { status: 'unavailable', result: null };
        }
        if (response.result === null) return { status: 'empty', result: null };
        if (typeof response.result !== 'object' || response.result.error) {
            return { status: 'unavailable', result: null };
        }
        return { status: 'available', result: response.result };
    } catch {
        return { status: 'unavailable', result: null };
    }
}

function resultTime(result) {
    if (result?.as_of_date) return `As of ${result.as_of_date}`;
    const timestamp = result?.completed_at || result?.generated_at || result?.timestamp;
    return timestamp ? `Result time: ${timestamp}` : 'Result time unknown';
}

const researchRunStatusColors = {
    started: { bg: '#1A6EBF22', color: '#1A6EBF' },
    running: { bg: '#1A6EBF22', color: '#1A6EBF' },
    ok: { bg: '#22C55E22', color: '#22C55E' },
    failed: { bg: '#EF444422', color: '#EF4444' },
    timeout: { bg: '#F59E0B22', color: '#F59E0B' },
    abandoned: { bg: '#5A708022', color: '#5A7080' },
};

// GRID W4c — reads GET /api/v1/snapshots/research/latest (api/routers/snapshots.py),
// which wraps scripts/research_status.py::latest_research_run_result. Self-contained
// fetch/render, same pattern as TestedHypotheses() below: it must not block or be
// blocked by the main Discovery data load, since a research-run event may not exist
// at all (a healthy "no data yet" state, not an error).
//
// Uses api.get() — the existing generic request helper other views already call for
// ad-hoc endpoints with no dedicated api.js method (see AttentionRadar.jsx,
// GeoFlows.jsx, InfluenceNetwork.jsx, Surfacer.jsx, TPS.jsx, Timeline.jsx,
// Valuation.jsx) — rather than adding a new named method to api.js, which is claimed
// by another lane in this branch.
export function ResearchRunPanel() {
    const [data, setData] = useState(null);
    const [loaded, setLoaded] = useState(false);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            let result;
            try {
                result = await api.get('/api/v1/snapshots/research/latest');
            } catch (err) {
                result = { status: 'unavailable', reason: err?.message || 'request failed' };
            }
            if (!cancelled) {
                setData(result);
                setLoaded(true);
            }
        })();
        return () => { cancelled = true; };
    }, []);

    if (!loaded) return null;

    if (!data || data.status === 'no_runs') {
        return (
            <div style={{ ...shared.cardGradient, marginBottom: tokens.space.xl }}>
                <div style={shared.sectionTitle}>RESEARCH RUN</div>
                <div style={{ color: colors.textMuted, fontSize: tokens.fontSize.sm }}>
                    No research run recorded yet.
                </div>
            </div>
        );
    }

    if (data.status === 'unavailable') {
        return (
            <div style={{ ...shared.cardGradient, marginBottom: tokens.space.xl }}>
                <div style={shared.sectionTitle}>RESEARCH RUN</div>
                <div style={{ color: colors.textMuted, fontSize: tokens.fontSize.sm }}>
                    Research status unavailable: {data.reason || 'unknown reason'}
                </div>
            </div>
        );
    }

    const sc = researchRunStatusColors[data.status] || researchRunStatusColors.abandoned;
    const reasons = [
        ...(data.skip_reasons || []),
        ...(data.failure_reasons || []),
    ];

    return (
        <div style={{ ...shared.cardGradient, marginBottom: tokens.space.xl }}>
            <div style={shared.sectionTitle}>RESEARCH RUN</div>
            <div style={{ display: 'flex', gap: tokens.space.sm, alignItems: 'center', flexWrap: 'wrap', marginBottom: tokens.space.sm }}>
                <span style={{
                    fontSize: tokens.fontSize.xs, fontWeight: 600,
                    padding: '3px 10px', borderRadius: tokens.radius.sm,
                    fontFamily: "'JetBrains Mono', monospace",
                    background: sc.bg, color: sc.color,
                }}>
                    {String(data.status || '').toUpperCase()}
                </span>
                {data.phase && (
                    <span style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>
                        phase: {data.phase}
                    </span>
                )}
                {data.run_id && (
                    <span style={{
                        fontSize: tokens.fontSize.xs, color: colors.textDim,
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        {data.run_id}
                    </span>
                )}
            </div>
            <div style={{
                display: 'flex', gap: tokens.space.md, flexWrap: 'wrap',
                fontSize: tokens.fontSize.xs, color: colors.textMuted, marginBottom: tokens.space.xs,
            }}>
                <span>iterations: {data.iterations ?? data.iteration ?? '—'}</span>
                <span>duration: {data.duration_s != null ? `${data.duration_s}s` : '—'}</span>
                <span>eval version: {data.inputs?.evaluation_version ?? '—'}</span>
                <span>sha: {data.code_sha ? data.code_sha.substring(0, 8) : '—'}</span>
            </div>
            {data.error && (
                <div style={{ fontSize: tokens.fontSize.xs, color: colors.red, marginBottom: tokens.space.xs }}>
                    {data.error}
                </div>
            )}
            {reasons.length > 0 && (
                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>
                    reasons: {reasons.join(', ')}
                </div>
            )}
            {data.latest_hypothesis && (
                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textDim, marginTop: tokens.space.xs }}>
                    latest hypothesis ({data.latest_hypothesis.state}): {data.latest_hypothesis.statement}
                </div>
            )}
        </div>
    );
}

// Item #20 (Wave 3 triage report): hypothesis_registry/validation_results
// are noise-generator output (41,521 rows, 2,040 "PASSED", no trial
// ledger, FDR correction or holdout — autoresearch is paused and Hermes
// hypothesis scoring/discovery/review are held). This must never render a
// PASSED verdict or a correlation/Sharpe number as a validated finding, or
// offer to act on one (the previous version showed "Strong relationship
// confirmed — consider promoting to feature" and a Promote button driven
// purely by |correlation| > 0.4). Show the honest "research lane off"
// state instead — count only, no per-hypothesis metrics, no promote action.
function TestedHypotheses() {
    const [summary, setSummary] = useState(null);
    const [loaded, setLoaded] = useState(false);

    useEffect(() => {
        (async () => {
            try {
                const data = await api.getHypothesisResults({});
                setSummary({ count: data.count || 0, note: data.note || null });
            } catch { /* fallback: no results endpoint yet */ }
            setLoaded(true);
        })();
    }, []);

    if (!loaded || !summary) return null;

    return (
        <div style={{ marginBottom: tokens.space.xl }}>
            <div style={shared.sectionTitle}>TESTED HYPOTHESES</div>
            <div style={{
                ...shared.card, borderLeft: `3px solid ${colors.textMuted}`,
                color: colors.textMuted, fontSize: tokens.fontSize.sm, lineHeight: 1.6,
            }}>
                <div style={{ fontWeight: 700, marginBottom: '4px', color: colors.text }}>
                    RESEARCH LANE OFF
                </div>
                <div>
                    {summary.count} tested hypotheses on record, but autoresearch is paused and
                    Hermes hypothesis scoring/discovery/review are held. There is no trial ledger,
                    FDR correction or out-of-sample holdout behind any PASSED verdict — none of
                    these are validated findings, and this view will not act on them.
                </div>
            </div>
        </div>
    );
}

export default function Discovery({ focusHypothesis = '' }) {
    const { jobs, hypotheses, setJobs, setHypotheses, addNotification } = useStore();
    const [orthoResult, setOrthoResult] = useState(null);
    const [clusterResult, setClusterResult] = useState(null);
    const [orthoStatus, setOrthoStatus] = useState('loading');
    const [clusterStatus, setClusterStatus] = useState('loading');
    const [nComponents, setNComponents] = useState(3);
    const [hypoFilter, setHypoFilter] = useState('ALL');
    const [hypoQuery, setHypoQuery] = useState(focusHypothesis || '');
    const [running, setRunning] = useState({});
    const { isMobile } = useDevice();

    useEffect(() => { loadHypotheses(); }, [hypoFilter]);
    useEffect(() => { setHypoQuery(focusHypothesis || ''); }, [focusHypothesis]);

    const { loading, error, refetch: loadData } = useAsyncData(async () => {
        setOrthoStatus('loading');
        setClusterStatus('loading');
        setOrthoResult(null);
        setClusterResult(null);
        try {
            await Promise.all([
                api.getJobs().then((j) => setJobs(j.jobs || [])),
                loadAuditResult('orthogonality').then((ortho) => {
                    setOrthoResult(ortho.result);
                    setOrthoStatus(ortho.status);
                }),
                loadAuditResult('clustering').then((cluster) => {
                    setClusterResult(cluster.result);
                    setClusterStatus(cluster.status);
                }),
            ]);
        } finally {
            await loadHypotheses();
        }
    }, { fallback: null });

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

    const normalizedHypoQuery = hypoQuery.trim().toLowerCase();
    const visibleHypotheses = normalizedHypoQuery
        ? hypotheses.filter((h) => (
            String(h.id || '').toLowerCase() === normalizedHypoQuery
            || (h.statement || '').toLowerCase().includes(normalizedHypoQuery)
        ))
        : hypotheses;

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.lg }}>
                <div style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg,
                    color: colors.textMuted, letterSpacing: '2px',
                }}>
                    DISCOVERY
                </div>
                <ViewHelp id="discovery" />
            </div>

            <ResearchRunPanel />

            {loading && !jobs.length && orthoStatus === 'loading' && clusterStatus === 'loading' ? (
                <LoadingSkeleton variant="card" count={3} />
            ) : error ? (
                <ErrorState error={error} onRetry={loadData} title="Discovery data unavailable" />
            ) : (
            <>
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
                                        {j.started ? `Started: ${j.started}` : 'Start time unknown'}
                                        {j.finished ? ` · Finished: ${j.finished}` : ''}
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

            <div style={shared.cardGradient}>
                <div style={shared.sectionTitle}>ORTHOGONALITY</div>
                {orthoStatus === 'loading' && <div>Loading orthogonality result...</div>}
                {orthoStatus === 'empty' && <div>No completed orthogonality audit found.</div>}
                {orthoStatus === 'unavailable' && <div>Orthogonality result unavailable.</div>}
                {orthoStatus === 'available' && orthoResult && <>
                <div style={{ color: colors.textMuted, fontSize: tokens.fontSize.xs }}>{resultTime(orthoResult)}</div>
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
                </>}
            </div>

            <div style={shared.cardGradient}>
                <div style={shared.sectionTitle}>CLUSTERING</div>
                {clusterStatus === 'loading' && <div>Loading clustering result...</div>}
                {clusterStatus === 'empty' && <div>No completed clustering run found.</div>}
                {clusterStatus === 'unavailable' && <div>Clustering result unavailable.</div>}
                {clusterStatus === 'available' && clusterResult && <>
                <div style={{ color: colors.textMuted, fontSize: tokens.fontSize.xs }}>{resultTime(clusterResult)}</div>
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
                </>}
            </div>

            {/* ═══ TESTED HYPOTHESES (RESULTS) ═══ */}
            <TestedHypotheses />

            <div style={{ marginBottom: tokens.space.xl }}>
                <div style={shared.sectionTitle}>HYPOTHESES</div>
                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textDim, marginBottom: tokens.space.sm }}>
                    Raw registry browse — research lane off (see above); state badges reflect
                    registry rows only, not validated findings.
                </div>
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
                <div style={{
                    ...shared.card,
                    display: 'flex',
                    gap: tokens.space.sm,
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    marginBottom: tokens.space.md,
                }}>
                    <input
                        type="text"
                        value={hypoQuery}
                        onChange={(e) => setHypoQuery(e.target.value)}
                        placeholder="Filter by hypothesis id or text..."
                        style={{
                            flex: '1 1 240px',
                            minWidth: 0,
                            background: colors.bg,
                            border: `1px solid ${colors.border}`,
                            borderRadius: tokens.radius.sm,
                            color: colors.text,
                            padding: '10px 12px',
                            fontSize: tokens.fontSize.sm,
                        }}
                    />
                    {hypoQuery ? (
                        <button style={shared.buttonSmall} onClick={() => setHypoQuery('')}>
                            Clear
                        </button>
                    ) : null}
                </div>
                {visibleHypotheses.map((h, i) => {
                    const sc = hypoStateColors[h.state] || hypoStateColors.KILLED;
                    return (
                        <div key={h.id || i} style={{
                            ...shared.card, minHeight: '52px',
                        }}>
                            <div title={h.statement} style={{
                                fontSize: tokens.fontSize.md, color: colors.text,
                                marginBottom: tokens.space.xs,
                                display: '-webkit-box', WebkitLineClamp: 2,
                                WebkitBoxOrient: 'vertical', overflow: 'hidden',
                                lineHeight: '1.5', wordBreak: 'break-word',
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
                                    {h.created_at ? `Created: ${h.created_at}` : 'Creation time unknown'}
                                </span>
                            </div>
                        </div>
                    );
                })}
                {visibleHypotheses.length === 0 && (
                    <div style={{
                        color: colors.textMuted, textAlign: 'center',
                        padding: tokens.space.xl, fontSize: tokens.fontSize.md,
                    }}>
                        {hypoQuery ? `No hypotheses match "${hypoQuery}".` : 'No hypotheses found'}
                    </div>
                )}
            </div>
            </>
            )}
        </div>
    );
}
