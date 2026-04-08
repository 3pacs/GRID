import React, { useState, useMemo, useEffect, useRef } from 'react';
import { Target, X, TrendingUp, TrendingDown } from 'lucide-react';

const TIMEFRAME_OPTIONS = [
    { value: 7, label: '7 days' },
    { value: 14, label: '14 days' },
    { value: 30, label: '30 days' },
    { value: 60, label: '60 days' },
    { value: 90, label: '90 days' },
];

const font = "'IBM Plex Sans', sans-serif";

const overlayStyle = {
    position: 'fixed',
    inset: 0,
    zIndex: 200,
    background: 'rgba(0,0,0,0.7)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontFamily: font,
};

const modalStyle = {
    background: '#0D1117',
    border: '1px solid #1E2A3A',
    borderRadius: 12,
    width: 480,
    maxWidth: '95vw',
    maxHeight: '90vh',
    overflowY: 'auto',
    boxShadow: '0 16px 48px rgba(0,0,0,0.7)',
    padding: 0,
};

const headerStyle = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '16px 20px',
    borderBottom: '1px solid #1E2A3A',
};

const bodyStyle = {
    padding: '16px 20px',
    display: 'flex',
    flexDirection: 'column',
    gap: 14,
};

const labelStyle = {
    fontSize: 11,
    color: '#5A7080',
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
    marginBottom: 4,
    display: 'block',
};

const inputStyle = {
    width: '100%',
    background: '#161B22',
    border: '1px solid #1E2A3A',
    borderRadius: 6,
    color: '#C8D8E8',
    fontSize: 13,
    padding: '8px 10px',
    fontFamily: font,
    outline: 'none',
    boxSizing: 'border-box',
};

const textareaStyle = {
    ...inputStyle,
    resize: 'vertical',
    minHeight: 70,
};

const selectStyle = {
    ...inputStyle,
    cursor: 'pointer',
};

const btnBase = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 6,
    padding: '8px 16px',
    fontSize: 13,
    fontFamily: font,
    borderRadius: 6,
    cursor: 'pointer',
    border: 'none',
    fontWeight: 600,
};

const dirToggleStyle = (active, isBullish) => ({
    ...btnBase,
    flex: 1,
    background: active
        ? (isBullish ? 'rgba(16,185,129,0.15)' : 'rgba(239,68,68,0.15)')
        : '#161B22',
    color: active
        ? (isBullish ? '#10B981' : '#EF4444')
        : '#3A4A5A',
    border: `1px solid ${active
        ? (isBullish ? '#10B981' : '#EF4444')
        : '#1E2A3A'}`,
});

const checkboxRowStyle = {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    fontSize: 12,
    color: '#C8D8E8',
    padding: '3px 0',
};

function PredictionModal({ selectedNodes, boardId, onSubmit, onClose }) {
    const modalRef = useRef(null);

    // Pre-fill thesis from selected node labels
    const defaultThesis = useMemo(
        () => selectedNodes.map((n) => n.data?.label || '').filter(Boolean).join('; '),
        [selectedNodes],
    );

    const [thesis, setThesis] = useState(defaultThesis);
    const [ticker, setTicker] = useState('');
    const [direction, setDirection] = useState('bullish');
    const [timeframeDays, setTimeframeDays] = useState(30);
    const [confidence, setConfidence] = useState(50);
    const [leverNodeId, setLeverNodeId] = useState('');
    const [conditionNodeIds, setConditionNodeIds] = useState([]);
    const [submitting, setSubmitting] = useState(false);
    const [error, setError] = useState('');

    // Close on Escape
    useEffect(() => {
        const handler = (e) => {
            if (e.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', handler);
        return () => document.removeEventListener('keydown', handler);
    }, [onClose]);

    // Close on backdrop click
    const handleBackdropClick = (e) => {
        if (modalRef.current && !modalRef.current.contains(e.target)) {
            onClose();
        }
    };

    // Nodes available for lever/condition selection
    const nodeOptions = useMemo(
        () => selectedNodes.map((n) => ({
            id: n.id,
            label: n.data?.label || n.id,
            type: n.type || 'note',
        })),
        [selectedNodes],
    );

    // Remaining nodes (not lever) for condition checkboxes
    const conditionOptions = useMemo(
        () => nodeOptions.filter((n) => n.id !== leverNodeId),
        [nodeOptions, leverNodeId],
    );

    const toggleCondition = (nodeId) => {
        setConditionNodeIds((prev) =>
            prev.includes(nodeId)
                ? prev.filter((id) => id !== nodeId)
                : [...prev, nodeId]
        );
    };

    const handleSubmit = async () => {
        if (!thesis.trim()) { setError('Thesis text is required'); return; }
        if (!ticker.trim()) { setError('Ticker is required'); return; }
        setError('');
        setSubmitting(true);
        try {
            await onSubmit({
                board_id: boardId,
                thesis_text: thesis.trim(),
                ticker: ticker.trim().toUpperCase(),
                direction,
                timeframe_days: timeframeDays,
                lever_node_id: leverNodeId || null,
                condition_node_ids: conditionNodeIds,
                confidence: confidence / 100,
            });
        } catch (err) {
            setError(err?.message || 'Failed to create prediction');
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div style={overlayStyle} onClick={handleBackdropClick}>
            <div ref={modalRef} style={modalStyle}>
                {/* Header */}
                <div style={headerStyle}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: '#10B981', fontSize: 15, fontWeight: 700 }}>
                        <Target size={18} />
                        Create Prediction
                    </div>
                    <button
                        onClick={onClose}
                        style={{ background: 'none', border: 'none', color: '#5A7080', cursor: 'pointer', padding: 4 }}
                    >
                        <X size={16} />
                    </button>
                </div>

                {/* Body */}
                <div style={bodyStyle}>
                    {/* Thesis text */}
                    <div>
                        <label style={labelStyle}>Thesis</label>
                        <textarea
                            style={textareaStyle}
                            value={thesis}
                            onChange={(e) => setThesis(e.target.value)}
                            placeholder="Describe your thesis..."
                        />
                    </div>

                    {/* Ticker + Direction row */}
                    <div style={{ display: 'flex', gap: 12 }}>
                        <div style={{ flex: 1 }}>
                            <label style={labelStyle}>Ticker</label>
                            <input
                                style={inputStyle}
                                value={ticker}
                                onChange={(e) => setTicker(e.target.value.toUpperCase())}
                                placeholder="e.g. AAPL"
                                maxLength={10}
                            />
                        </div>
                        <div style={{ flex: 1 }}>
                            <label style={labelStyle}>Direction</label>
                            <div style={{ display: 'flex', gap: 6 }}>
                                <button
                                    style={dirToggleStyle(direction === 'bullish', true)}
                                    onClick={() => setDirection('bullish')}
                                    type="button"
                                >
                                    <TrendingUp size={14} /> Bullish
                                </button>
                                <button
                                    style={dirToggleStyle(direction === 'bearish', false)}
                                    onClick={() => setDirection('bearish')}
                                    type="button"
                                >
                                    <TrendingDown size={14} /> Bearish
                                </button>
                            </div>
                        </div>
                    </div>

                    {/* Timeframe + Confidence row */}
                    <div style={{ display: 'flex', gap: 12 }}>
                        <div style={{ flex: 1 }}>
                            <label style={labelStyle}>Timeframe</label>
                            <select
                                style={selectStyle}
                                value={timeframeDays}
                                onChange={(e) => setTimeframeDays(Number(e.target.value))}
                            >
                                {TIMEFRAME_OPTIONS.map((o) => (
                                    <option key={o.value} value={o.value}>{o.label}</option>
                                ))}
                            </select>
                        </div>
                        <div style={{ flex: 1 }}>
                            <label style={labelStyle}>Confidence: {confidence}%</label>
                            <input
                                type="range"
                                min={1}
                                max={99}
                                value={confidence}
                                onChange={(e) => setConfidence(Number(e.target.value))}
                                style={{ width: '100%', accentColor: '#3B82F6' }}
                            />
                        </div>
                    </div>

                    {/* Lever selection */}
                    {nodeOptions.length > 0 && (
                        <div>
                            <label style={labelStyle}>Lever (causal actor/signal)</label>
                            <select
                                style={selectStyle}
                                value={leverNodeId}
                                onChange={(e) => {
                                    setLeverNodeId(e.target.value);
                                    // Remove from conditions if it was there
                                    setConditionNodeIds((prev) =>
                                        prev.filter((id) => id !== e.target.value)
                                    );
                                }}
                            >
                                <option value="">-- None --</option>
                                {nodeOptions.map((n) => (
                                    <option key={n.id} value={n.id}>
                                        [{n.type}] {n.label}
                                    </option>
                                ))}
                            </select>
                        </div>
                    )}

                    {/* Condition checkboxes */}
                    {conditionOptions.length > 0 && (
                        <div>
                            <label style={labelStyle}>Conditions (environmental amplifiers)</label>
                            {conditionOptions.map((n) => (
                                <div key={n.id} style={checkboxRowStyle}>
                                    <input
                                        type="checkbox"
                                        checked={conditionNodeIds.includes(n.id)}
                                        onChange={() => toggleCondition(n.id)}
                                        style={{ accentColor: '#F59E0B' }}
                                    />
                                    <span style={{ color: '#5A7080', fontSize: 10 }}>[{n.type}]</span>
                                    {n.label}
                                </div>
                            ))}
                        </div>
                    )}

                    {/* Error display */}
                    {error && (
                        <div style={{ fontSize: 12, color: '#EF4444', padding: '6px 0' }}>
                            {error}
                        </div>
                    )}

                    {/* Submit */}
                    <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, paddingTop: 4, paddingBottom: 4 }}>
                        <button
                            onClick={onClose}
                            style={{
                                ...btnBase,
                                background: '#161B22',
                                color: '#5A7080',
                                border: '1px solid #1E2A3A',
                            }}
                        >
                            Cancel
                        </button>
                        <button
                            onClick={handleSubmit}
                            disabled={submitting}
                            style={{
                                ...btnBase,
                                background: submitting ? '#1E2A3A' : '#10B981',
                                color: '#fff',
                                opacity: submitting ? 0.6 : 1,
                            }}
                        >
                            <Target size={14} />
                            {submitting ? 'Creating...' : 'Create Prediction'}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default React.memo(PredictionModal);
