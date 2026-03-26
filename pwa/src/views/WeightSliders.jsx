import React, { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';

// Feature descriptions and category groupings
const FEATURE_META = {
    vix:               { category: 'vol',         desc: 'CBOE Volatility Index — primary fear gauge' },
    move_index:        { category: 'vol',         desc: 'Bond market volatility (Merrill Lynch MOVE)' },
    vxn:               { category: 'vol',         desc: 'NASDAQ volatility index' },
    hy_spread:         { category: 'credit',      desc: 'High-yield credit spread — stress transmission' },
    chicago_fed:       { category: 'credit',      desc: 'Chicago Fed financial conditions index' },
    yield_curve_10y2y: { category: 'rates',       desc: 'Treasury 10Y-2Y spread — recession signal' },
    treasury_10y:      { category: 'rates',       desc: '10Y Treasury yield — rate conditions' },
    breakeven_10y:     { category: 'rates',       desc: '10Y breakeven inflation — deflation fear proxy' },
    sp500:             { category: 'risk_assets',  desc: 'S&P 500 — primary equity benchmark' },
    copper:            { category: 'risk_assets',  desc: 'Copper — growth / industrial demand proxy' },
    crude_oil:         { category: 'risk_assets',  desc: 'Crude oil — energy & inflation proxy' },
    gold:              { category: 'safe_havens',  desc: 'Gold — safe haven / uncertainty hedge' },
    dollar_index:      { category: 'safe_havens',  desc: 'DXY dollar index — flight to safety' },
    spy_rsi:           { category: 'sentiment',    desc: 'SPY RSI — oversold = stress signal' },
    put_call_ratio:    { category: 'sentiment',    desc: 'Put/call ratio — high = fear' },
};

const CATEGORY_LABELS = {
    vol: 'Volatility',
    credit: 'Credit',
    rates: 'Rates',
    risk_assets: 'Risk Assets',
    safe_havens: 'Safe Havens',
    sentiment: 'Sentiment',
};

const CATEGORY_ORDER = ['vol', 'credit', 'rates', 'risk_assets', 'safe_havens', 'sentiment'];

const REGIME_COLORS = {
    GROWTH: colors.green,
    NEUTRAL: colors.accent,
    FRAGILE: colors.yellow,
    CRISIS: colors.red,
    UNKNOWN: colors.textMuted,
    UNCALIBRATED: colors.textMuted,
};

const styles = {
    container: { ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    headerRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: tokens.space.lg,
    },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px',
    },
    regimeCard: {
        ...shared.card,
        display: 'flex', gap: tokens.space.lg, flexWrap: 'wrap',
        alignItems: 'center',
    },
    regimeBlock: {
        flex: '1 1 140px', minWidth: '120px',
    },
    regimeLabel: {
        fontSize: tokens.fontSize.xs, color: colors.textMuted,
        fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px',
        marginBottom: '4px',
    },
    regimeValue: {
        fontSize: tokens.fontSize.xl, fontWeight: 700,
        fontFamily: "'JetBrains Mono', monospace",
    },
    previewTag: {
        fontSize: tokens.fontSize.xs, fontWeight: 600,
        padding: '2px 8px', borderRadius: tokens.radius.sm,
        background: colors.yellowBg, color: colors.yellow,
        marginLeft: '8px', verticalAlign: 'middle',
    },
    categoryHeader: {
        fontSize: tokens.fontSize.xs, fontWeight: 700, letterSpacing: '2px',
        color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace",
        marginTop: tokens.space.lg, marginBottom: tokens.space.sm,
        textTransform: 'uppercase',
    },
    sliderCard: {
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: '12px 16px',
        marginBottom: '6px',
    },
    sliderTopRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '4px',
    },
    featureName: {
        fontSize: tokens.fontSize.md, fontWeight: 600, color: colors.text,
        fontFamily: "'JetBrains Mono', monospace",
    },
    weightValue: {
        fontSize: tokens.fontSize.md, fontWeight: 700,
        fontFamily: "'JetBrains Mono', monospace",
        minWidth: '52px', textAlign: 'right',
    },
    featureDesc: {
        fontSize: tokens.fontSize.xs, color: colors.textMuted,
        marginBottom: '8px',
    },
    sliderRow: {
        display: 'flex', alignItems: 'center', gap: '8px',
    },
    rangeLabel: {
        fontSize: '10px', color: colors.textMuted,
        fontFamily: "'JetBrains Mono', monospace",
        minWidth: '36px',
    },
    slider: {
        flex: 1, height: '6px', WebkitAppearance: 'none', appearance: 'none',
        background: colors.border, borderRadius: '3px', outline: 'none',
        cursor: 'pointer',
    },
    buttonRow: {
        display: 'flex', gap: tokens.space.sm, marginTop: tokens.space.lg,
        marginBottom: tokens.space.lg, flexWrap: 'wrap',
    },
    modifiedDot: {
        width: '6px', height: '6px', borderRadius: '50%',
        background: colors.yellow, display: 'inline-block',
        marginRight: '6px', verticalAlign: 'middle',
    },
};

function SliderInput({ name, value, defaultValue, desc, onChange }) {
    const isModified = Math.abs(value - defaultValue) > 0.001;
    const weightColor = value > 0.001 ? colors.red : value < -0.001 ? colors.green : colors.textDim;

    return (
        <div style={{
            ...styles.sliderCard,
            borderLeftColor: isModified ? colors.yellow : colors.border,
            borderLeftWidth: isModified ? '3px' : '1px',
        }}>
            <div style={styles.sliderTopRow}>
                <span style={styles.featureName}>
                    {isModified && <span style={styles.modifiedDot} />}
                    {name}
                </span>
                <span style={{ ...styles.weightValue, color: weightColor }}>
                    {value >= 0 ? '+' : ''}{value.toFixed(2)}
                </span>
            </div>
            <div style={styles.featureDesc}>{desc}</div>
            <div style={styles.sliderRow}>
                <span style={styles.rangeLabel}>-0.30</span>
                <input
                    type="range"
                    min={-0.30}
                    max={0.30}
                    step={0.01}
                    value={value}
                    onChange={(e) => onChange(name, parseFloat(e.target.value))}
                    style={styles.slider}
                />
                <span style={styles.rangeLabel}>+0.30</span>
            </div>
        </div>
    );
}

export default function WeightSliders() {
    const [weights, setWeights] = useState(null);
    const [defaults, setDefaults] = useState(null);
    const [currentRegime, setCurrentRegime] = useState(null);
    const [preview, setPreview] = useState(null);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [simulating, setSimulating] = useState(false);
    const [error, setError] = useState(null);
    const debounceRef = useRef(null);

    useEffect(() => {
        loadWeights();
    }, []);

    const loadWeights = async () => {
        try {
            setLoading(true);
            const data = await api.getRegimeWeights();
            setWeights(data.weights || {});
            setDefaults(data.defaults || {});
            setCurrentRegime({
                state: data.regime,
                confidence: data.confidence,
                stress_index: data.stress_index,
            });
            setPreview(null);
            setError(null);
        } catch (err) {
            setError(err.message || 'Failed to load weights');
        } finally {
            setLoading(false);
        }
    };

    const handleWeightChange = useCallback((name, value) => {
        setWeights(prev => {
            const next = { ...prev, [name]: value };

            // Debounced simulate
            if (debounceRef.current) clearTimeout(debounceRef.current);
            debounceRef.current = setTimeout(async () => {
                try {
                    setSimulating(true);
                    const res = await api.simulateRegimeWeights(next);
                    setPreview(res.result);
                } catch (err) {
                    // Silent — simulation is best-effort
                } finally {
                    setSimulating(false);
                }
            }, 500);

            return next;
        });
    }, []);

    const handleSave = async () => {
        try {
            setSaving(true);
            const res = await api.updateRegimeWeights(weights);
            setCurrentRegime({
                state: res.result?.regime,
                confidence: res.result?.confidence,
                stress_index: res.result?.stress_index,
            });
            setPreview(null);
            setError(null);
        } catch (err) {
            setError(err.message || 'Failed to save weights');
        } finally {
            setSaving(false);
        }
    };

    const handleReset = () => {
        if (defaults) {
            setWeights({ ...defaults });
            setPreview(null);
            // Trigger simulate with defaults
            if (debounceRef.current) clearTimeout(debounceRef.current);
            debounceRef.current = setTimeout(async () => {
                try {
                    setSimulating(true);
                    const res = await api.simulateRegimeWeights({ ...defaults });
                    setPreview(res.result);
                } catch (err) {
                    // Silent
                } finally {
                    setSimulating(false);
                }
            }, 300);
        }
    };

    if (loading) {
        return (
            <div style={styles.container}>
                <div style={styles.title}>WEIGHT TUNING</div>
                <div style={{ ...shared.card, textAlign: 'center', color: colors.textMuted }}>
                    Loading weights...
                </div>
            </div>
        );
    }

    if (error && !weights) {
        return (
            <div style={styles.container}>
                <div style={styles.title}>WEIGHT TUNING</div>
                <div style={{ ...shared.card, color: colors.red }}>{error}</div>
            </div>
        );
    }

    // Group features by category
    const grouped = {};
    for (const cat of CATEGORY_ORDER) {
        grouped[cat] = [];
    }
    for (const [name, val] of Object.entries(weights || {})) {
        const meta = FEATURE_META[name];
        const cat = meta?.category || 'sentiment';
        if (!grouped[cat]) grouped[cat] = [];
        grouped[cat].push({ name, value: val, desc: meta?.desc || name });
    }

    const hasChanges = defaults && weights && Object.keys(weights).some(
        k => Math.abs((weights[k] || 0) - (defaults[k] || 0)) > 0.001
    );

    return (
        <div style={styles.container}>
            <div style={styles.headerRow}>
                <div style={styles.title}>WEIGHT TUNING</div>
                <ViewHelp id="weights" />
            </div>

            {/* Current + Preview regime display */}
            <div style={styles.regimeCard}>
                <div style={styles.regimeBlock}>
                    <div style={styles.regimeLabel}>CURRENT REGIME</div>
                    <div style={{
                        ...styles.regimeValue,
                        color: REGIME_COLORS[currentRegime?.state] || colors.textMuted,
                    }}>
                        {currentRegime?.state || '---'}
                    </div>
                </div>
                <div style={styles.regimeBlock}>
                    <div style={styles.regimeLabel}>CONFIDENCE</div>
                    <div style={{ ...styles.regimeValue, color: colors.text }}>
                        {currentRegime?.confidence != null
                            ? `${(currentRegime.confidence * 100).toFixed(0)}%`
                            : '---'}
                    </div>
                </div>
                <div style={styles.regimeBlock}>
                    <div style={styles.regimeLabel}>STRESS INDEX</div>
                    <div style={{ ...styles.regimeValue, color: colors.text }}>
                        {currentRegime?.stress_index != null
                            ? currentRegime.stress_index.toFixed(3)
                            : '---'}
                    </div>
                </div>

                {/* Preview column */}
                {preview && (
                    <>
                        <div style={{ width: '1px', height: '48px', background: colors.border }} />
                        <div style={styles.regimeBlock}>
                            <div style={styles.regimeLabel}>
                                PREVIEW
                                <span style={styles.previewTag}>
                                    {simulating ? 'SIM...' : 'SIMULATED'}
                                </span>
                            </div>
                            <div style={{
                                ...styles.regimeValue,
                                color: REGIME_COLORS[preview.regime] || colors.textMuted,
                            }}>
                                {preview.regime}
                            </div>
                        </div>
                        <div style={styles.regimeBlock}>
                            <div style={styles.regimeLabel}>SIM CONFIDENCE</div>
                            <div style={{ ...styles.regimeValue, color: colors.text }}>
                                {(preview.confidence * 100).toFixed(0)}%
                            </div>
                        </div>
                        <div style={styles.regimeBlock}>
                            <div style={styles.regimeLabel}>SIM STRESS</div>
                            <div style={{ ...styles.regimeValue, color: colors.text }}>
                                {preview.stress_index?.toFixed(3) ?? '---'}
                            </div>
                        </div>
                    </>
                )}
            </div>

            {/* Action buttons */}
            <div style={styles.buttonRow}>
                <button
                    onClick={handleSave}
                    disabled={saving || !hasChanges}
                    style={{
                        ...shared.button,
                        ...(saving || !hasChanges ? shared.buttonDisabled : {}),
                        flex: '1 1 auto',
                    }}
                >
                    {saving ? 'Saving...' : 'Save Weights'}
                </button>
                <button
                    onClick={handleReset}
                    disabled={!hasChanges}
                    style={{
                        ...shared.button,
                        background: 'transparent',
                        border: `1px solid ${colors.border}`,
                        color: colors.textDim,
                        ...(!hasChanges ? shared.buttonDisabled : {}),
                        flex: '1 1 auto',
                    }}
                >
                    Reset to Defaults
                </button>
            </div>

            {error && <div style={shared.error}>{error}</div>}

            {/* Sliders grouped by category */}
            {CATEGORY_ORDER.map(cat => {
                const items = grouped[cat];
                if (!items || items.length === 0) return null;
                return (
                    <div key={cat}>
                        <div style={styles.categoryHeader}>
                            {CATEGORY_LABELS[cat] || cat}
                        </div>
                        {items.map(item => (
                            <SliderInput
                                key={item.name}
                                name={item.name}
                                value={item.value}
                                defaultValue={defaults?.[item.name] ?? 0}
                                desc={item.desc}
                                onChange={handleWeightChange}
                            />
                        ))}
                    </div>
                );
            })}

            {/* Bottom spacing */}
            <div style={{ height: '24px' }} />
        </div>
    );
}
