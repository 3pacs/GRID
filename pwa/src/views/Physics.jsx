import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';

// Energy level color coding
const energyColor = (level) => {
    if (level === 'high') return colors.red;
    if (level === 'building') return colors.yellow;
    return colors.green;
};

const energyBg = (level) => {
    if (level === 'high') return colors.redBg;
    if (level === 'building') return colors.yellowBg;
    return colors.greenBg;
};

// Horizontal energy bar
function EnergyBar({ value, max, level, label }) {
    const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0;
    return (
        <div style={{ marginBottom: '8px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', marginBottom: '3px' }}>
                <span style={{ color: colors.text, fontFamily: colors.mono }}>{label}</span>
                <span style={{ color: energyColor(level), fontWeight: 600 }}>{value.toFixed(2)}</span>
            </div>
            <div style={{ background: colors.bg, borderRadius: '4px', height: '8px', overflow: 'hidden' }}>
                <div style={{
                    width: `${pct}%`, height: '100%', borderRadius: '4px',
                    background: energyColor(level), transition: 'width 0.3s ease',
                }} />
            </div>
        </div>
    );
}

// Coherence meter (arc gauge)
function CoherenceMeter({ coherence, direction }) {
    const pct = Math.round((coherence || 0) * 100);
    const col = pct > 75 ? colors.red : pct > 50 ? colors.yellow : colors.green;
    return (
        <div style={{ textAlign: 'center', padding: '12px' }}>
            <div style={{
                fontSize: '36px', fontWeight: 700, color: col, fontFamily: colors.mono,
            }}>{pct}%</div>
            <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                Narrative Coherence
            </div>
            <div style={{ fontSize: '12px', color: colors.textDim, marginTop: '4px' }}>
                {direction === 'increasing' ? 'Sources aligned UPWARD' :
                 direction === 'decreasing' ? 'Sources aligned DOWNWARD' :
                 'Mixed signals'}
            </div>
        </div>
    );
}

// Force vector arrow
function ForceArrow({ item, maxEnergy }) {
    const pct = maxEnergy > 0 ? (item.energy / maxEnergy) * 100 : 0;
    const isUp = item.direction > 0;
    const arrow = isUp ? '\u2191' : item.direction < 0 ? '\u2193' : '\u2194';
    const arrowColor = isUp ? colors.green : item.direction < 0 ? colors.red : colors.textMuted;
    return (
        <div style={{
            display: 'flex', alignItems: 'center', gap: '8px',
            padding: '6px 0', borderBottom: `1px solid ${colors.border}`,
        }}>
            <span style={{ fontSize: '18px', color: arrowColor, width: '24px', textAlign: 'center' }}>
                {arrow}
            </span>
            <span style={{ flex: 1, fontSize: '12px', color: colors.text, fontFamily: colors.mono }}>
                {item.feature.replace('crucix_', '').replace('gdelt_', '')}
            </span>
            <div style={{ width: '120px' }}>
                <div style={{ background: colors.bg, borderRadius: '3px', height: '6px', overflow: 'hidden' }}>
                    <div style={{
                        width: `${pct}%`, height: '100%', borderRadius: '3px',
                        background: energyColor(item.energy_level),
                    }} />
                </div>
            </div>
            <span style={{
                fontSize: '11px', fontWeight: 600, fontFamily: colors.mono, width: '50px', textAlign: 'right',
                color: energyColor(item.energy_level),
            }}>
                {item.energy.toFixed(2)}
            </span>
        </div>
    );
}

// Dashboard tab
function DashboardTab({ dashboard, loading, onLoad }) {
    if (loading) return <div style={{ color: colors.textMuted, padding: '20px', textAlign: 'center' }}>Loading dashboard...</div>;
    if (!dashboard) return (
        <div style={shared.card}>
            <button style={shared.button} onClick={onLoad}>Load Physics Dashboard</button>
        </div>
    );
    if (dashboard.error) return <div style={{ ...shared.card, borderColor: colors.red }}><div style={shared.error}>{dashboard.error}</div></div>;

    const { market_energy, news_energy, hurst_exponents, ou_parameters, energy_conservation, summary } = dashboard;

    return (
        <>
            {/* Summary */}
            <div style={{ ...shared.card, borderColor: colors.accent }}>
                <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Physics Summary</div>
                <div style={{ fontSize: '13px', color: colors.textDim, lineHeight: '1.6' }}>{summary}</div>
                <div style={{ marginTop: '8px' }}>
                    <span style={shared.badge(
                        energy_conservation?.state === 'equilibrium' ? '#1A7A4A' :
                        energy_conservation?.state === 'stressed' ? '#5A3A00' : '#8B1F1F'
                    )}>
                        {(energy_conservation?.state || 'unknown').toUpperCase()}
                    </span>
                </div>
            </div>

            {/* Market Energy */}
            <div style={shared.card}>
                <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Market Energy State</div>
                <div style={shared.metricGrid}>
                    {Object.entries(market_energy || {}).map(([name, data]) => (
                        <div key={name} style={shared.metric}>
                            <div style={{ fontSize: '11px', color: colors.textMuted, marginBottom: '4px' }}>{name}</div>
                            {data.status === 'ok' ? (
                                <>
                                    <div style={{ fontSize: '13px', fontFamily: colors.mono, color: colors.text }}>
                                        KE: {data.kinetic_energy?.toFixed(4) ?? 'N/A'}
                                    </div>
                                    <div style={{ fontSize: '13px', fontFamily: colors.mono, color: colors.text }}>
                                        PE: {data.potential_energy?.toFixed(4) ?? 'N/A'}
                                    </div>
                                    <div style={{ fontSize: '14px', fontWeight: 700, fontFamily: colors.mono, color: '#E8F0F8', marginTop: '4px' }}>
                                        {data.total_energy?.toFixed(4) ?? 'N/A'}
                                    </div>
                                    <div style={{ fontSize: '10px', color: colors.textMuted }}>Total</div>
                                </>
                            ) : (
                                <div style={{ fontSize: '12px', color: colors.textMuted }}>No data</div>
                            )}
                        </div>
                    ))}
                </div>
            </div>

            {/* Hurst & OU side by side */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                <div style={shared.card}>
                    <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Hurst Exponents</div>
                    {Object.entries(hurst_exponents || {}).map(([name, data]) => (
                        <div key={name} style={shared.row}>
                            <span style={{ fontSize: '12px', color: colors.text }}>{name}</span>
                            <span style={{
                                fontSize: '13px', fontFamily: colors.mono, fontWeight: 600,
                                color: data.interpretation === 'mean-reverting' ? colors.green :
                                       data.interpretation === 'trending' ? colors.red : colors.textDim,
                            }}>
                                {data.hurst?.toFixed(3) ?? 'N/A'} ({data.interpretation})
                            </span>
                        </div>
                    ))}
                </div>
                <div style={shared.card}>
                    <div style={{ ...shared.sectionTitle, marginTop: 0 }}>OU Mean Reversion</div>
                    {Object.entries(ou_parameters || {}).map(([name, data]) => (
                        <div key={name} style={shared.row}>
                            <span style={{ fontSize: '12px', color: colors.text }}>{name}</span>
                            <span style={{ fontSize: '12px', fontFamily: colors.mono, color: colors.textDim }}>
                                {data.mean_reverting
                                    ? `t\u00BD=${data.half_life_days?.toFixed(0) ?? '?'}d`
                                    : 'No reversion'}
                            </span>
                        </div>
                    ))}
                </div>
            </div>

            {/* News energy summary in dashboard */}
            {news_energy && news_energy.n_sources > 0 && (
                <div style={shared.card}>
                    <div style={{ ...shared.sectionTitle, marginTop: 0 }}>
                        News Energy ({news_energy.n_sources} sources)
                    </div>
                    <div style={shared.metricGrid}>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{news_energy.total_news_energy?.toFixed(2) ?? '0'}</div>
                            <div style={shared.metricLabel}>Total Energy</div>
                        </div>
                        <div style={shared.metric}>
                            <CoherenceMeter
                                coherence={news_energy.coherence?.coherence}
                                direction={news_energy.coherence?.dominant_direction}
                            />
                        </div>
                    </div>
                </div>
            )}
        </>
    );
}

// News Energy tab
function NewsEnergyTab({ data, loading, onLoad }) {
    if (loading) return <div style={{ color: colors.textMuted, padding: '20px', textAlign: 'center' }}>Loading news energy...</div>;
    if (!data) return (
        <div style={shared.card}>
            <button style={shared.button} onClick={onLoad}>Load News Energy</button>
        </div>
    );
    if (data.error) return <div style={{ ...shared.card, borderColor: colors.red }}><div style={shared.error}>{data.error}</div></div>;

    const { energy_by_source, total_news_energy, coherence, force_vector, regime_signal, summary } = data;
    const maxEnergy = Math.max(...(energy_by_source || []).map(s => s.total_energy), 1);
    const maxForce = Math.max(...(force_vector || []).map(f => f.energy), 1);

    return (
        <>
            {/* Summary */}
            <div style={{ ...shared.card, borderColor: colors.accent }}>
                <div style={{ fontSize: '13px', color: colors.textDim, lineHeight: '1.6' }}>{summary}</div>
            </div>

            {/* Overview metrics */}
            <div style={shared.card}>
                <div style={shared.metricGrid}>
                    <div style={shared.metric}>
                        <div style={{
                            ...shared.metricValue,
                            color: total_news_energy > 10 ? colors.red : total_news_energy > 3 ? colors.yellow : colors.green,
                        }}>
                            {(total_news_energy || 0).toFixed(2)}
                        </div>
                        <div style={shared.metricLabel}>Total News Energy</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{(energy_by_source || []).length}</div>
                        <div style={shared.metricLabel}>Active Sources</div>
                    </div>
                    <div style={shared.metric}>
                        <CoherenceMeter
                            coherence={coherence?.coherence}
                            direction={coherence?.dominant_direction}
                        />
                    </div>
                    <div style={shared.metric}>
                        <div style={{
                            ...shared.metricValue,
                            color: regime_signal?.equilibrium ? colors.green : colors.red,
                        }}>
                            {regime_signal?.equilibrium ? 'STABLE' : 'SHIFTING'}
                        </div>
                        <div style={shared.metricLabel}>Regime</div>
                    </div>
                </div>
            </div>

            {/* Energy by source */}
            <div style={shared.card}>
                <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Energy by Source</div>
                {(energy_by_source || []).sort((a, b) => b.total_energy - a.total_energy).map((src) => (
                    <EnergyBar
                        key={src.feature}
                        value={src.total_energy}
                        max={maxEnergy}
                        level={src.energy_level}
                        label={src.feature.replace('crucix_', '').replace('gdelt_', '')}
                    />
                ))}
                {(!energy_by_source || energy_by_source.length === 0) && (
                    <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                        No news energy data available.
                    </div>
                )}
            </div>

            {/* Force vector */}
            {force_vector && force_vector.length > 0 && (
                <div style={shared.card}>
                    <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Force Vector (energy injection by source)</div>
                    {force_vector.slice(0, 15).map((item) => (
                        <ForceArrow key={item.feature} item={item} maxEnergy={maxForce} />
                    ))}
                </div>
            )}

            {/* Regime violations */}
            {regime_signal && !regime_signal.equilibrium && (
                <div style={{ ...shared.card, borderColor: colors.red }}>
                    <div style={{ ...shared.sectionTitle, marginTop: 0, color: colors.red }}>
                        Energy Conservation Violations
                    </div>
                    <div style={{ fontSize: '13px', color: colors.textDim, marginBottom: '8px' }}>
                        {regime_signal.interpretation}
                    </div>
                    {(regime_signal.violating_sources || []).map((src) => (
                        <div key={src} style={{ fontSize: '12px', color: colors.red, fontFamily: colors.mono, padding: '2px 0' }}>
                            {src}
                        </div>
                    ))}
                </div>
            )}
        </>
    );
}

export default function Physics() {
    const [verification, setVerification] = useState(null);
    const [verifying, setVerifying] = useState(false);
    const [conventions, setConventions] = useState(null);
    const [featureInput, setFeatureInput] = useState('');
    const [featureAnalysis, setFeatureAnalysis] = useState(null);
    const [analysing, setAnalysing] = useState(false);
    const [activeTab, setActiveTab] = useState('dashboard');
    const [dashboard, setDashboard] = useState(null);
    const [dashboardLoading, setDashboardLoading] = useState(false);
    const [newsEnergy, setNewsEnergy] = useState(null);
    const [newsEnergyLoading, setNewsEnergyLoading] = useState(false);

    const runVerify = async () => {
        setVerifying(true);
        try {
            const result = await api.runPhysicsVerification();
            setVerification(result);
        } catch (e) {
            setVerification({ error: e.message });
        }
        setVerifying(false);
    };

    const loadConventions = async () => {
        try {
            const result = await api.getConventions();
            setConventions(result.conventions || []);
        } catch (e) { console.warn('[GRID] Physics:', e.message); }
    };

    const analyseFeature = async () => {
        if (!featureInput.trim()) return;
        setAnalysing(true);
        setFeatureAnalysis(null);
        try {
            const [ou, hurst, energy] = await Promise.all([
                api.getOUParams(featureInput).catch(() => null),
                api.getHurst(featureInput).catch(() => null),
                api.getEnergy(featureInput).catch(() => null),
            ]);
            setFeatureAnalysis({ ou, hurst, energy, feature: featureInput });
        } catch (e) { console.warn('[GRID] Physics:', e.message); }
        setAnalysing(false);
    };

    const loadDashboard = async () => {
        setDashboardLoading(true);
        try {
            const result = await api.getPhysicsDashboard();
            setDashboard(result);
        } catch (e) {
            setDashboard({ error: e.message });
        }
        setDashboardLoading(false);
    };

    const loadNewsEnergy = async () => {
        setNewsEnergyLoading(true);
        try {
            const result = await api.getNewsEnergy(30);
            setNewsEnergy(result);
        } catch (e) {
            setNewsEnergy({ error: e.message });
        }
        setNewsEnergyLoading(false);
    };

    return (
        <div style={shared.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={shared.header}>Market Physics</div>
                <ViewHelp id="physics" />
            </div>

            <div style={shared.tabs}>
                {['dashboard', 'news-energy', 'verify', 'analyse', 'conventions'].map(t => (
                    <button key={t} style={shared.tab(activeTab === t)}
                        onClick={() => {
                            setActiveTab(t);
                            if (t === 'conventions' && !conventions) loadConventions();
                        }}>
                        {t === 'news-energy' ? 'News Energy' : t.charAt(0).toUpperCase() + t.slice(1)}
                    </button>
                ))}
            </div>

            {activeTab === 'dashboard' && (
                <DashboardTab
                    dashboard={dashboard}
                    loading={dashboardLoading}
                    onLoad={loadDashboard}
                />
            )}

            {activeTab === 'news-energy' && (
                <NewsEnergyTab
                    data={newsEnergy}
                    loading={newsEnergyLoading}
                    onLoad={loadNewsEnergy}
                />
            )}

            {activeTab === 'verify' && (
                <>
                    <div style={shared.card}>
                        <button
                            style={{ ...shared.button, ...(verifying ? shared.buttonDisabled : {}) }}
                            onClick={runVerify}
                            disabled={verifying}
                        >
                            {verifying ? 'Running Verification...' : 'Run Physics Verification'}
                        </button>
                    </div>

                    {verification?.error && (
                        <div style={{ ...shared.card, borderColor: colors.red }}>
                            <div style={shared.error}>{verification.error}</div>
                        </div>
                    )}

                    {verification && !verification.error && (
                        <div style={shared.card}>
                            <div style={shared.metricGrid}>
                                <div style={shared.metric}>
                                    <div style={{
                                        ...shared.metricValue,
                                        color: verification.passed ? colors.green : colors.red,
                                    }}>
                                        {verification.passed ? 'PASS' : 'FAIL'}
                                    </div>
                                    <div style={shared.metricLabel}>Overall</div>
                                </div>
                                {verification.checks_passed != null && (
                                    <div style={shared.metric}>
                                        <div style={shared.metricValue}>
                                            {verification.checks_passed}/{verification.checks_total || verification.checks_passed}
                                        </div>
                                        <div style={shared.metricLabel}>Checks</div>
                                    </div>
                                )}
                            </div>

                            {verification.results && (
                                <div style={{ marginTop: '12px' }}>
                                    {(Array.isArray(verification.results) ? verification.results : Object.entries(verification.results).map(([k, v]) => ({ name: k, ...v }))).map((r, i) => (
                                        <div key={i} style={shared.row}>
                                            <span style={{ fontSize: '13px', color: colors.text }}>
                                                {r.name || r.check || `Check ${i + 1}`}
                                            </span>
                                            <span style={shared.badge(
                                                r.passed || r.status === 'PASS' ? '#1A7A4A' : '#8B1F1F'
                                            )}>
                                                {r.passed || r.status === 'PASS' ? 'PASS' : 'FAIL'}
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            )}

                            {verification.details && (
                                <div style={{ ...shared.prose, marginTop: '12px' }}>
                                    {JSON.stringify(verification.details, null, 2)}
                                </div>
                            )}
                        </div>
                    )}
                </>
            )}

            {activeTab === 'analyse' && (
                <>
                    <div style={shared.card}>
                        <span style={shared.label}>Feature Name</span>
                        <div style={{ display: 'flex', gap: '8px' }}>
                            <input
                                style={{ ...shared.input, width: '250px' }}
                                value={featureInput}
                                onChange={(e) => setFeatureInput(e.target.value)}
                                placeholder="e.g. yield_curve_2s10s"
                                onKeyDown={(e) => e.key === 'Enter' && analyseFeature()}
                            />
                            <button
                                style={{ ...shared.buttonSmall, ...(analysing ? shared.buttonDisabled : {}) }}
                                onClick={analyseFeature}
                                disabled={analysing}
                            >
                                {analysing ? 'Analysing...' : 'Analyse'}
                            </button>
                        </div>
                    </div>

                    {featureAnalysis && (
                        <div style={shared.card}>
                            <div style={{ fontSize: '16px', fontWeight: 600, color: colors.text, marginBottom: '12px' }}>
                                {featureAnalysis.feature}
                            </div>

                            <div style={shared.metricGrid}>
                                {/* Hurst */}
                                {featureAnalysis.hurst && (
                                    <>
                                        <div style={shared.metric}>
                                            <div style={{
                                                ...shared.metricValue,
                                                color: featureAnalysis.hurst.hurst_exponent < 0.45 ? colors.green
                                                    : featureAnalysis.hurst.hurst_exponent > 0.55 ? colors.red
                                                    : colors.textDim,
                                            }}>
                                                {featureAnalysis.hurst.hurst_exponent?.toFixed(3) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Hurst Exponent</div>
                                        </div>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.hurst.interpretation}
                                            </div>
                                            <div style={shared.metricLabel}>Behaviour</div>
                                        </div>
                                    </>
                                )}

                                {/* OU Parameters */}
                                {featureAnalysis.ou && (
                                    <>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.ou.theta?.toFixed(4) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Theta (Speed)</div>
                                        </div>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.ou.half_life?.toFixed(1) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Half-Life (days)</div>
                                        </div>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.ou.mu?.toFixed(4) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Equilibrium</div>
                                        </div>
                                    </>
                                )}

                                {/* Energy */}
                                {featureAnalysis.energy && (
                                    <>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.energy.kinetic_energy?.toFixed(4) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Kinetic Energy</div>
                                        </div>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.energy.potential_energy?.toFixed(4) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Potential Energy</div>
                                        </div>
                                        <div style={shared.metric}>
                                            <div style={shared.metricValue}>
                                                {featureAnalysis.energy.total_energy?.toFixed(4) || 'N/A'}
                                            </div>
                                            <div style={shared.metricLabel}>Total Energy</div>
                                        </div>
                                    </>
                                )}
                            </div>

                            {!featureAnalysis.hurst && !featureAnalysis.ou && !featureAnalysis.energy && (
                                <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                                    No data found for this feature. Check the feature name.
                                </div>
                            )}
                        </div>
                    )}
                </>
            )}

            {activeTab === 'conventions' && (
                <div style={shared.card}>
                    {!conventions ? (
                        <div style={{ color: colors.textMuted, fontSize: '13px' }}>Loading...</div>
                    ) : conventions.length === 0 ? (
                        <div style={{ color: colors.textMuted, fontSize: '13px' }}>No conventions found</div>
                    ) : (
                        conventions.map((c, i) => (
                            <div key={i} style={{ ...shared.row, flexWrap: 'wrap', gap: '8px' }}>
                                <div>
                                    <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                                        {c.domain}
                                    </span>
                                    {c.notes && (
                                        <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                                            {c.notes}
                                        </div>
                                    )}
                                </div>
                                <div style={{ display: 'flex', gap: '12px', fontSize: '12px', fontFamily: colors.mono }}>
                                    <span style={{ color: colors.textDim }}>Unit: {c.unit}</span>
                                    <span style={{ color: colors.textDim }}>Method: {c.method}</span>
                                    <span style={{ color: colors.textDim }}>Days: {c.trading_days}</span>
                                </div>
                            </div>
                        ))
                    )}
                </div>
            )}
        </div>
    );
}
