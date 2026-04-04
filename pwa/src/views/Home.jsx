import React, { useState, useRef, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, tokens } from '../styles/shared.js';

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

function formatAnswer(text) {
    return text.split('\n').map((line, i) => {
        const isData = /^\s*([-*]|\w[\w\s]*:)/.test(line) && line.includes(':');
        return (
            <div key={i} style={isData ? { fontFamily: MONO, fontSize: '12px', color: colors.textDim } : undefined}>
                {line || '\u00A0'}
            </div>
        );
    });
}

export default function Home() {
    const messages = useStore(s => s.chatMessages);
    const addChatMessage = useStore(s => s.addChatMessage);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const messagesRef = useRef(null);
    const inputRef = useRef(null);

    useEffect(() => {
        if (messagesRef.current) {
            messagesRef.current.scrollTop = messagesRef.current.scrollHeight;
        }
    }, [messages, loading]);

    useEffect(() => { inputRef.current?.focus(); }, []);

    const send = useCallback(async (text) => {
        const q = (text || input).trim();
        if (!q || loading) return;
        setInput('');
        addChatMessage({ role: 'user', content: q });
        setLoading(true);
        try {
            const history = messages.map(m => ({ role: m.role, content: m.content }));
            const result = await api.askGRID(q, null, history);
            addChatMessage({
                role: 'assistant',
                content: result.error ? `Error: ${result.message || 'Failed'}` : result.answer,
                sources: result.sources_used || [],
                confidence: result.confidence || 0,
            });
        } catch (err) {
            addChatMessage({ role: 'assistant', content: `Connection error: ${err.message}`, sources: [], confidence: 0 });
        } finally {
            setLoading(false);
        }
    }, [input, loading, messages]);

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
    };

    const hasMessages = messages.length > 0;

    return (
        <div style={S.page}>
            {/* Centered state — logo + input */}
            {!hasMessages && (
                <div style={S.center}>
                    <span style={S.logo}>GRID</span>
                    <div style={S.boxWrap}>
                        <input
                            ref={inputRef}
                            style={S.box}
                            placeholder="Ask anything..."
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={handleKeyDown}
                        />
                        <button onClick={() => send()} disabled={!input.trim() || loading}
                            style={{ ...S.btn, opacity: (!input.trim() || loading) ? 0.35 : 1 }}>
                            {loading ? '\u2026' : '\u2192'}
                        </button>
                    </div>
                </div>
            )}

            {/* Conversation state */}
            {hasMessages && (
                <>
                    <div style={S.messages} ref={messagesRef}>
                        {messages.map((msg, i) => {
                            if (msg.role === 'user') {
                                return <div key={i} style={S.msgUser}>{msg.content}</div>;
                            }
                            return (
                                <div key={i} style={S.msgGrid}>
                                    <div>{formatAnswer(msg.content)}</div>
                                    {msg.sources?.length > 0 && (
                                        <div style={S.sources}>
                                            {msg.sources.map((s, j) => (
                                                <span key={j} style={S.tag}>{s}</span>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            );
                        })}
                        {loading && (
                            <div style={S.thinking}>
                                <span style={S.thinkDot} /><span style={S.thinkDot} /><span style={S.thinkDot} />
                            </div>
                        )}
                    </div>
                    <div style={S.bottomBar}>
                        <input
                            ref={inputRef}
                            style={S.boxSmall}
                            placeholder="Follow up..."
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={handleKeyDown}
                        />
                        <button onClick={() => send()} disabled={!input.trim() || loading}
                            style={{ ...S.btnSmall, opacity: (!input.trim() || loading) ? 0.35 : 1 }}>
                            {'\u2192'}
                        </button>
                    </div>
                </>
            )}
        </div>
    );
}

const S = {
    page: {
        display: 'flex',
        flexDirection: 'column',
        height: '100vh',
        width: '100%',
    },
    center: {
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '32px',
        padding: '20px',
    },
    logo: {
        fontFamily: MONO,
        fontSize: '42px',
        fontWeight: 800,
        letterSpacing: '10px',
        color: colors.accent,
    },
    boxWrap: {
        display: 'flex',
        width: '100%',
        maxWidth: '560px',
        gap: '0',
        border: `1px solid ${colors.border}`,
        borderRadius: '12px',
        overflow: 'hidden',
        background: colors.card,
    },
    box: {
        flex: 1,
        background: 'transparent',
        border: 'none',
        color: colors.text,
        padding: '14px 18px',
        fontSize: '15px',
        fontFamily: SANS,
        outline: 'none',
    },
    btn: {
        width: '52px',
        background: colors.accent,
        border: 'none',
        color: '#fff',
        fontSize: '18px',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        transition: 'opacity 0.15s',
    },
    messages: {
        flex: 1,
        overflowY: 'auto',
        padding: '20px',
        maxWidth: '720px',
        width: '100%',
        margin: '0 auto',
        display: 'flex',
        flexDirection: 'column',
        gap: '14px',
        WebkitOverflowScrolling: 'touch',
    },
    msgUser: {
        alignSelf: 'flex-end',
        background: colors.accent,
        color: '#fff',
        borderRadius: '16px 16px 4px 16px',
        padding: '10px 16px',
        maxWidth: '75%',
        fontSize: '14px',
        fontFamily: SANS,
        lineHeight: 1.5,
    },
    msgGrid: {
        alignSelf: 'flex-start',
        background: colors.card,
        border: `1px solid ${colors.border}`,
        color: colors.text,
        borderRadius: '16px 16px 16px 4px',
        padding: '14px 18px',
        maxWidth: '88%',
        fontSize: '14px',
        fontFamily: SANS,
        lineHeight: 1.65,
    },
    sources: {
        marginTop: '10px',
        display: 'flex',
        gap: '4px',
        flexWrap: 'wrap',
    },
    tag: {
        fontSize: '9px',
        fontFamily: MONO,
        background: colors.bg,
        color: colors.textDim,
        padding: '2px 7px',
        borderRadius: '3px',
        border: `1px solid ${colors.border}`,
    },
    thinking: {
        alignSelf: 'flex-start',
        display: 'flex',
        gap: '5px',
        padding: '14px 18px',
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '16px 16px 16px 4px',
    },
    thinkDot: {
        width: '7px',
        height: '7px',
        borderRadius: '50%',
        background: colors.textMuted,
        animation: 'chatPulse 1.4s ease-in-out infinite',
    },
    bottomBar: {
        display: 'flex',
        gap: '0',
        padding: '10px 20px calc(10px + env(safe-area-inset-bottom, 0px))',
        borderTop: `1px solid ${colors.border}`,
        maxWidth: '720px',
        width: '100%',
        margin: '0 auto',
        background: colors.bg,
    },
    boxSmall: {
        flex: 1,
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '10px 0 0 10px',
        color: colors.text,
        padding: '12px 16px',
        fontSize: '14px',
        fontFamily: SANS,
        outline: 'none',
    },
    btnSmall: {
        width: '48px',
        background: colors.accent,
        border: 'none',
        borderRadius: '0 10px 10px 0',
        color: '#fff',
        fontSize: '16px',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
    },
};
