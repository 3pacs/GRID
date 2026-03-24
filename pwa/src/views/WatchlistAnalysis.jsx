import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

export default function WatchlistAnalysis({ ticker, onBack }) {
    const [analysis, setAnalysis] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (ticker) loadAnalysis();
    }, [ticker]);

    const loadAnalysis = async () => {
        setLoading(true);
        setError(null);
        try {
            const data = await api.getTickerAnalysis(ticker);
            setAnalysis(data);
        } catch (err) {
            setError(err.message || 'Failed to load analysis');
        }
        setLoading(false);
    };

    if (loading) {
        return (
            <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>
                    Loading analysis for {ticker}...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
                <button
                    onClick={onBack}
                    style={{ ...shared.buttonSmall, marginBottom: '16px', background: colors.card }}
                >
                    Back
                </button>
                <div style={shared.error}>{error}</div>
            </div>
        );
    }

    const item = analysis?.watchlist_item;
    const journal = analysis?.latest_journal;
    const options = analysis?.options_signals;

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '20px' }}>
                <button
                    onClick={onBack}
                    style={{
                        background: colors.card, border: `1px solid ${colors.border}`,
                        borderRadius: '8px', color: colors.textDim, padding: '8px 14px',
                        fontSize: '13px', cursor: 'pointer', fontFamily: colors.sans,
                    }}
                >
                    Back
                </button>
                <div>
                    <div style={{
                        fontSize: '20px', fontWeight: 700, color: '#E8F0F8',
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        {ticker}
                    </div>
                    <div style={{ fontSize: '12px', color: colors.textMuted }}>
                        {item?.display_name || ticker} - {item?.asset_type?.toUpperCase()}
                    </div>
                </div>
            </div>

            {/* Notes */}
            {item?.notes && (
                <div style={{ ...shared.card }}>
                    <div style={shared.label}>Notes</div>
                    <div style={{ fontSize: '13px', color: colors.textDim, lineHeight: '1.5' }}>
                        {item.notes}
                    </div>
                </div>
            )}

            {/* Latest Journal Context */}
            <div style={shared.sectionTitle}>Latest Journal Context</div>
            {journal ? (
                <div style={shared.card}>
                    <div style={shared.metricGrid}>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{journal.inferred_state || '--'}</div>
                            <div style={shared.metricLabel}>State</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={{ ...shared.metricValue, fontSize: '12px' }}>
                                {journal.action_taken?.substring(0, 30) || '--'}
                            </div>
                            <div style={shared.metricLabel}>Action</div>
                        </div>
                    </div>
                    <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '8px', fontFamily: colors.mono }}>
                        {journal.decision_timestamp}
                    </div>
                </div>
            ) : (
                <div style={{ ...shared.card, color: colors.textMuted, fontSize: '13px' }}>
                    No journal entries available
                </div>
            )}

            {/* Options Signals */}
            {options && (
                <>
                    <div style={shared.sectionTitle}>Options Signals</div>
                    <div style={shared.card}>
                        <div style={shared.metricGrid}>
                            <div style={shared.metric}>
                                <div style={shared.metricValue}>
                                    {options.put_call_ratio != null ? options.put_call_ratio.toFixed(2) : '--'}
                                </div>
                                <div style={shared.metricLabel}>P/C Ratio</div>
                            </div>
                            <div style={shared.metric}>
                                <div style={shared.metricValue}>
                                    {options.iv_atm != null ? (options.iv_atm * 100).toFixed(1) + '%' : '--'}
                                </div>
                                <div style={shared.metricLabel}>IV ATM</div>
                            </div>
                            <div style={shared.metric}>
                                <div style={shared.metricValue}>
                                    {options.max_pain != null ? '$' + options.max_pain.toFixed(0) : '--'}
                                </div>
                                <div style={shared.metricLabel}>Max Pain</div>
                            </div>
                            <div style={shared.metric}>
                                <div style={shared.metricValue}>
                                    {options.iv_skew != null ? options.iv_skew.toFixed(3) : '--'}
                                </div>
                                <div style={shared.metricLabel}>IV Skew</div>
                            </div>
                        </div>
                        <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '8px', fontFamily: colors.mono }}>
                            Signal date: {options.signal_date}
                        </div>
                    </div>
                </>
            )}

            {/* Signal Context */}
            <div style={shared.sectionTitle}>Signal Context</div>
            <div style={shared.card}>
                {analysis?.signals?.error ? (
                    <div style={{ fontSize: '12px', color: colors.textMuted }}>
                        Signals unavailable: {analysis.signals.error}
                    </div>
                ) : analysis?.signals?.layers ? (
                    <div style={shared.prose}>
                        {JSON.stringify(analysis.signals.layers, null, 2)}
                    </div>
                ) : (
                    <div style={{ fontSize: '12px', color: colors.textMuted }}>
                        No signal data available
                    </div>
                )}
            </div>

            {/* Metadata */}
            <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '16px', fontFamily: colors.mono }}>
                Added: {item?.added_at}
            </div>
        </div>
    );
}
