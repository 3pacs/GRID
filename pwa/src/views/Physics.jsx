import React, { useState } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

export default function Physics() {
    const [verification, setVerification] = useState(null);
    const [verifying, setVerifying] = useState(false);
    const [conventions, setConventions] = useState(null);
    const [featureInput, setFeatureInput] = useState('');
    const [featureAnalysis, setFeatureAnalysis] = useState(null);
    const [analysing, setAnalysing] = useState(false);
    const [activeTab, setActiveTab] = useState('verify');

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

    return (
        <div style={shared.container}>
            <div style={shared.header}>Market Physics</div>

            <div style={shared.tabs}>
                {['verify', 'analyse', 'conventions'].map(t => (
                    <button key={t} style={shared.tab(activeTab === t)}
                        onClick={() => {
                            setActiveTab(t);
                            if (t === 'conventions' && !conventions) loadConventions();
                        }}>
                        {t.charAt(0).toUpperCase() + t.slice(1)}
                    </button>
                ))}
            </div>

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
