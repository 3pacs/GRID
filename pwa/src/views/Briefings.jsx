import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

export default function Briefings() {
    const [ollamaStatus, setOllamaStatus] = useState(null);
    const [activeTab, setActiveTab] = useState('hourly');
    const [briefing, setBriefing] = useState(null);
    const [briefingList, setBriefingList] = useState([]);
    const [generating, setGenerating] = useState(false);
    const [question, setQuestion] = useState('');
    const [askResult, setAskResult] = useState(null);
    const [asking, setAsking] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        api.getOllamaStatus().then(setOllamaStatus).catch(() => {});
        loadBriefing('hourly');
        loadBriefingList();
    }, []);

    const loadBriefing = async (type) => {
        try {
            const result = await api.getLatestBriefing(type);
            setBriefing(result);
        } catch { setBriefing(null); }
    };

    const loadBriefingList = async () => {
        try {
            const result = await api.listBriefings('', 30);
            setBriefingList(result.briefings || []);
        } catch (e) { console.warn('[GRID] Briefings:', e.message); }
    };

    const generate = async (type) => {
        setGenerating(true);
        setError(null);
        try {
            const result = await api.generateBriefing(type);
            setBriefing(result);
            loadBriefingList();
        } catch (e) {
            setError(e.message || 'Generation failed');
        }
        setGenerating(false);
    };

    const switchTab = (type) => {
        setActiveTab(type);
        loadBriefing(type);
    };

    const readSaved = async (filename) => {
        try {
            const result = await api.readBriefing(filename);
            setBriefing({ content: result.content, type: filename.split('_')[0] });
        } catch (e) { console.warn('[GRID] Briefings:', e.message); }
    };

    const askQuestion = async () => {
        if (!question.trim()) return;
        setAsking(true);
        try {
            const result = await api.askOllama(question);
            setAskResult(result);
        } catch (e) {
            setAskResult({ response: `Error: ${e.message}` });
        }
        setAsking(false);
    };

    const available = ollamaStatus?.available;

    return (
        <div style={shared.container}>
            <div style={shared.header}>Market Briefings</div>

            {/* Ollama Status */}
            <div style={shared.card}>
                <div style={shared.row}>
                    <div>
                        <span style={shared.label}>Ollama</span>
                        <span style={shared.value}>{ollamaStatus?.model || 'unknown'}</span>
                    </div>
                    <span style={shared.badge(available ? '#1A7A4A' : '#8B1F1F')}>
                        {available ? 'ONLINE' : 'OFFLINE'}
                    </span>
                </div>
            </div>

            {/* Generate Briefings */}
            <div style={shared.card}>
                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', alignItems: 'center' }}>
                    <span style={{ ...shared.label, marginBottom: 0 }}>Generate:</span>
                    {['hourly', 'daily', 'weekly'].map(type => (
                        <button
                            key={type}
                            style={{
                                ...shared.buttonSmall,
                                ...(generating ? shared.buttonDisabled : {}),
                            }}
                            onClick={() => generate(type)}
                            disabled={generating || !available}
                        >
                            {generating ? 'Generating...' : type.charAt(0).toUpperCase() + type.slice(1)}
                        </button>
                    ))}
                </div>
                {error && <div style={shared.error}>{error}</div>}
            </div>

            {/* Briefing Tabs */}
            <div style={shared.tabs}>
                {['hourly', 'daily', 'weekly'].map(type => (
                    <button
                        key={type}
                        style={shared.tab(activeTab === type)}
                        onClick={() => switchTab(type)}
                    >
                        {type.charAt(0).toUpperCase() + type.slice(1)}
                    </button>
                ))}
            </div>

            {/* Current Briefing */}
            {briefing?.content ? (
                <div style={shared.card}>
                    <div style={{ ...shared.row, borderBottom: 'none', paddingTop: 0 }}>
                        <span style={shared.badge('#1A2840')}>
                            {briefing.type?.toUpperCase()}
                        </span>
                        {briefing.timestamp && (
                            <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                {new Date(briefing.timestamp).toLocaleString()}
                            </span>
                        )}
                    </div>
                    <div style={shared.prose}>{briefing.content}</div>
                </div>
            ) : (
                <div style={{
                    ...shared.card, textAlign: 'center', color: colors.textMuted,
                    padding: '40px', fontSize: '13px',
                }}>
                    No {activeTab} briefing available. Generate one above.
                </div>
            )}

            {/* Saved Briefings Archive */}
            <div style={shared.sectionTitle}>Archive</div>
            <div style={shared.card}>
                {briefingList.length === 0 ? (
                    <div style={{ color: colors.textMuted, fontSize: '13px' }}>No saved briefings</div>
                ) : (
                    briefingList.map((b, i) => (
                        <div
                            key={i}
                            style={{ ...shared.row, cursor: 'pointer' }}
                            onClick={() => readSaved(b.filename)}
                        >
                            <div>
                                <span style={shared.badge(
                                    b.type === 'weekly' ? '#5A3A00'
                                        : b.type === 'daily' ? '#1A6EBF'
                                        : '#1A2840'
                                )}>
                                    {b.type?.toUpperCase()}
                                </span>
                                <span style={{ fontSize: '12px', color: colors.textMuted, marginLeft: '10px' }}>
                                    {b.filename}
                                </span>
                            </div>
                            <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                {(b.size_bytes / 1024).toFixed(1)}kb
                            </span>
                        </div>
                    ))
                )}
            </div>

            {/* Ask Ollama */}
            <div style={shared.sectionTitle}>Ask GRID Analyst</div>
            <div style={shared.card}>
                <textarea
                    style={shared.textarea}
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ask about market conditions, regime state, feature relationships..."
                />
                <div style={{ marginTop: '8px' }}>
                    <button
                        style={{
                            ...shared.buttonSmall,
                            ...(asking || !available ? shared.buttonDisabled : {}),
                        }}
                        onClick={askQuestion}
                        disabled={asking || !available}
                    >
                        {asking ? 'Thinking...' : 'Ask'}
                    </button>
                </div>
                {askResult?.response && (
                    <div style={{ ...shared.prose, marginTop: '12px' }}>
                        {askResult.response}
                    </div>
                )}
            </div>
        </div>
    );
}
