import React, { useState, useRef, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, tokens } from '../styles/shared.js';

/* ─── Design tokens ─────────────────────────────────────────────────── */

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";
const PANEL_BG = 'rgba(8, 12, 16, 0.97)';
const MSG_USER_BG = colors.accent;
const MSG_GRID_BG = colors.card;
const BORDER = colors.border;
const TRANSITION = 'transform 0.35s cubic-bezier(0.4, 0, 0.2, 1)';

const QUICK_PROMPTS = [
    'What should I watch?',
    'Explain the current regime',
    'Any red flags?',
    'What are the lever pullers doing?',
];

const TIMEFRAMES = [
    { label: '1D', value: '1d' },
    { label: '1W', value: '1w' },
    { label: '1M', value: '1m' },
    { label: '3M', value: '3m' },
    { label: '6M+', value: '6m' },
];

/* ─── Styles ────────────────────────────────────────────────────────── */

const S = {
    fabWrap: {
        position: 'fixed',
        bottom: 'calc(80px + env(safe-area-inset-bottom, 0px))',
        right: '20px',
        zIndex: 9000,
    },
    fab: {
        width: '52px',
        height: '52px',
        borderRadius: '50%',
        background: colors.accent,
        border: 'none',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        boxShadow: '0 4px 16px rgba(26, 110, 191, 0.4)',
        transition: 'transform 0.2s ease, box-shadow 0.2s ease',
        position: 'relative',
    },
    fabIcon: {
        width: '24px',
        height: '24px',
        fill: 'none',
        stroke: '#fff',
        strokeWidth: 2,
        strokeLinecap: 'round',
        strokeLinejoin: 'round',
    },
    badge: {
        position: 'absolute',
        top: '-2px',
        right: '-2px',
        width: '18px',
        height: '18px',
        borderRadius: '50%',
        background: colors.red,
        color: '#fff',
        fontSize: '10px',
        fontWeight: 700,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontFamily: MONO,
    },
    overlay: {
        position: 'fixed',
        inset: 0,
        background: 'rgba(0,0,0,0.5)',
        zIndex: 9500,
        transition: 'opacity 0.3s ease',
    },
    panel: {
        position: 'fixed',
        left: 0,
        right: 0,
        bottom: 0,
        height: '60vh',
        minHeight: '340px',
        maxHeight: '80vh',
        background: PANEL_BG,
        borderTop: `1px solid ${BORDER}`,
        borderRadius: '16px 16px 0 0',
        zIndex: 9600,
        display: 'flex',
        flexDirection: 'column',
        transition: TRANSITION,
        backdropFilter: 'blur(20px)',
        WebkitBackdropFilter: 'blur(20px)',
    },
    header: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '14px 20px 10px',
        borderBottom: `1px solid ${BORDER}`,
        flexShrink: 0,
    },
    headerTitle: {
        fontSize: '15px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: SANS,
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
    },
    headerDot: {
        width: '8px',
        height: '8px',
        borderRadius: '50%',
        background: colors.green,
        display: 'inline-block',
    },
    closeBtn: {
        background: 'none',
        border: 'none',
        color: colors.textMuted,
        cursor: 'pointer',
        fontSize: '20px',
        padding: '4px 8px',
        lineHeight: 1,
    },
    messages: {
        flex: 1,
        overflowY: 'auto',
        padding: '12px 16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '10px',
        WebkitOverflowScrolling: 'touch',
    },
    quickPrompts: {
        display: 'flex',
        gap: '6px',
        flexWrap: 'wrap',
        padding: '8px 16px',
        borderBottom: `1px solid ${BORDER}`,
        flexShrink: 0,
    },
    quickBtn: {
        background: colors.cardElevated,
        border: `1px solid ${BORDER}`,
        borderRadius: tokens.radius.pill,
        padding: '6px 14px',
        fontSize: '12px',
        color: colors.textDim,
        cursor: 'pointer',
        fontFamily: SANS,
        whiteSpace: 'nowrap',
        transition: 'all 0.15s ease',
    },
    timeframeRow: {
        display: 'flex',
        gap: '4px',
        padding: '6px 16px',
        borderTop: `1px solid ${BORDER}`,
        flexShrink: 0,
        alignItems: 'center',
    },
    timeframeLabel: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textMuted,
        marginRight: '4px',
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
    },
    timeframeBtn: (active) => ({
        background: active ? colors.accent : 'transparent',
        border: `1px solid ${active ? colors.accent : BORDER}`,
        borderRadius: tokens.radius.sm,
        padding: '3px 10px',
        fontSize: '11px',
        fontFamily: MONO,
        fontWeight: active ? 700 : 400,
        color: active ? '#fff' : colors.textDim,
        cursor: 'pointer',
        transition: 'all 0.15s ease',
    }),
    inputRow: {
        display: 'flex',
        gap: '8px',
        padding: '10px 16px calc(10px + env(safe-area-inset-bottom, 0px))',
        borderTop: `1px solid ${BORDER}`,
        flexShrink: 0,
        background: PANEL_BG,
    },
    input: {
        flex: 1,
        background: colors.bg,
        border: `1px solid ${BORDER}`,
        borderRadius: tokens.radius.md,
        color: colors.text,
        padding: '10px 14px',
        fontSize: '14px',
        fontFamily: SANS,
        outline: 'none',
        minHeight: '44px',
        resize: 'none',
    },
    sendBtn: {
        width: '44px',
        height: '44px',
        borderRadius: tokens.radius.md,
        background: colors.accent,
        border: 'none',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0,
        transition: 'opacity 0.15s ease',
    },
    msgUser: {
        alignSelf: 'flex-end',
        background: MSG_USER_BG,
        color: '#fff',
        borderRadius: '14px 14px 4px 14px',
        padding: '10px 14px',
        maxWidth: '80%',
        fontSize: '14px',
        fontFamily: SANS,
        lineHeight: 1.5,
        wordBreak: 'break-word',
    },
    msgGrid: {
        alignSelf: 'flex-start',
        background: MSG_GRID_BG,
        border: `1px solid ${BORDER}`,
        color: colors.text,
        borderRadius: '14px 14px 14px 4px',
        padding: '12px 14px',
        maxWidth: '88%',
        fontSize: '14px',
        fontFamily: SANS,
        lineHeight: 1.6,
        wordBreak: 'break-word',
    },
    msgGridData: {
        fontFamily: MONO,
        fontSize: '12px',
        color: colors.textDim,
    },
    sources: {
        marginTop: '8px',
        display: 'flex',
        gap: '4px',
        flexWrap: 'wrap',
    },
    sourceTag: {
        fontSize: '10px',
        fontFamily: MONO,
        background: colors.bg,
        color: colors.textMuted,
        padding: '2px 8px',
        borderRadius: tokens.radius.sm,
        border: `1px solid ${colors.borderSubtle}`,
    },
    confidence: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textMuted,
        marginTop: '4px',
    },
    typing: {
        alignSelf: 'flex-start',
        display: 'flex',
        gap: '4px',
        padding: '10px 14px',
        background: MSG_GRID_BG,
        border: `1px solid ${BORDER}`,
        borderRadius: '14px 14px 14px 4px',
    },
    dot: (delay) => ({
        width: '7px',
        height: '7px',
        borderRadius: '50%',
        background: colors.textMuted,
        animation: `chatPulse 1.4s ease-in-out ${delay}s infinite`,
    }),
    empty: {
        textAlign: 'center',
        color: colors.textMuted,
        fontSize: '13px',
        fontFamily: SANS,
        padding: '40px 20px',
        lineHeight: 1.7,
    },
};

/* ─── Keyframes (injected once) ─────────────────────────────────────── */

let _stylesInjected = false;
function injectKeyframes() {
    if (_stylesInjected) return;
    _stylesInjected = true;
    const style = document.createElement('style');
    style.textContent = `
        @keyframes chatPulse {
            0%, 60%, 100% { opacity: 0.3; transform: scale(0.8); }
            30% { opacity: 1; transform: scale(1); }
        }
        @keyframes chatSlideUp {
            from { transform: translateY(100%); }
            to { transform: translateY(0); }
        }
    `;
    document.head.appendChild(style);
}

/* ─── Typing indicator ──────────────────────────────────────────────── */

function TypingIndicator() {
    return (
        <div style={S.typing}>
            <div style={S.dot(0)} />
            <div style={S.dot(0.2)} />
            <div style={S.dot(0.4)} />
        </div>
    );
}

/* ─── Icons (inline SVG) ────────────────────────────────────────────── */

function ChatIcon() {
    return (
        <svg style={S.fabIcon} viewBox="0 0 24 24">
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
        </svg>
    );
}

function SendIcon() {
    return (
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none"
            stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="22" y1="2" x2="11" y2="13" />
            <polygon points="22 2 15 22 11 13 2 9 22 2" />
        </svg>
    );
}

/* ─── Format response text ──────────────────────────────────────────── */

function formatAnswer(text) {
    // Detect lines that look like data (start with spaces/dashes + colons) and
    // wrap them in mono-styled spans.
    const lines = text.split('\n');
    return lines.map((line, i) => {
        const isData = /^\s*([-*]|\w[\w\s]*:)/.test(line) && line.includes(':');
        return (
            <div key={i} style={isData ? S.msgGridData : undefined}>
                {line || '\u00A0'}
            </div>
        );
    });
}

/* ─── Main component ────────────────────────────────────────────────── */

export default function ChatPanel() {
    const activeView = useStore(s => s.activeView);
    const messages = useStore(s => s.chatMessages);
    const unread = useStore(s => s.chatUnread);
    const addChatMessage = useStore(s => s.addChatMessage);
    const setChatUnread = useStore(s => s.setChatUnread);
    const [open, setOpen] = useState(false);
    const [input, setInput] = useState('');
    const [loading, setLoading] = useState(false);
    const [timeframe, setTimeframe] = useState(null);
    const messagesRef = useRef(null);
    const inputRef = useRef(null);

    useEffect(() => { injectKeyframes(); }, []);

    // Detect context ticker from watchlist-analysis view
    const contextTicker = (() => {
        if (typeof window !== 'undefined') {
            const hash = window.location.hash;
            const match = hash.match(/#\/watchlist\/([A-Za-z0-9.]+)/);
            if (match) return match[1].toUpperCase();
        }
        return null;
    })();

    // Scroll to bottom on new messages
    useEffect(() => {
        if (messagesRef.current) {
            messagesRef.current.scrollTop = messagesRef.current.scrollHeight;
        }
    }, [messages, loading]);

    // Focus input when panel opens
    useEffect(() => {
        if (open && inputRef.current) {
            setTimeout(() => inputRef.current?.focus(), 100);
        }
        if (open) setChatUnread(0);
    }, [open]);

    // Build history for multi-turn
    const buildHistory = useCallback(() => {
        return messages.map(m => ({
            role: m.role,
            content: m.content,
        }));
    }, [messages]);

    const send = useCallback(async (text) => {
        const q = (text || input).trim();
        if (!q || loading) return;

        setInput('');
        const userMsg = { role: 'user', content: q };
        addChatMessage(userMsg);
        setLoading(true);

        try {
            const history = buildHistory();
            const result = await api.askGRID(q, contextTicker, history, timeframe);

            if (result.error) {
                addChatMessage({
                    role: 'assistant',
                    content: `Error: ${result.message || 'Request failed'}`,
                    sources: [],
                    confidence: 0,
                });
            } else {
                const gridMsg = {
                    role: 'assistant',
                    content: result.answer,
                    sources: result.sources_used || [],
                    confidence: result.confidence || 0,
                };
                addChatMessage(gridMsg);
                if (!open) setChatUnread(unread + 1);
            }
        } catch (err) {
            addChatMessage({
                role: 'assistant',
                content: `Connection error: ${err.message}`,
                sources: [],
                confidence: 0,
            });
        } finally {
            setLoading(false);
        }
    }, [input, loading, contextTicker, buildHistory, open]);

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            send();
        }
    };

    const handleQuickPrompt = (prompt) => {
        send(prompt);
    };

    return (
        <>
            {/* Floating action button */}
            <div style={S.fabWrap} data-onboarding="chat-fab">
                <button
                    style={S.fab}
                    onClick={() => setOpen(o => !o)}
                    title="Ask GRID"
                    onMouseEnter={e => { e.currentTarget.style.transform = 'scale(1.08)'; }}
                    onMouseLeave={e => { e.currentTarget.style.transform = 'scale(1)'; }}
                >
                    <ChatIcon />
                    {unread > 0 && <span style={S.badge}>{unread}</span>}
                </button>
            </div>

            {/* Overlay */}
            {open && (
                <div
                    style={{ ...S.overlay, opacity: open ? 1 : 0 }}
                    onClick={() => setOpen(false)}
                />
            )}

            {/* Panel */}
            <div style={{
                ...S.panel,
                transform: open ? 'translateY(0)' : 'translateY(100%)',
                pointerEvents: open ? 'auto' : 'none',
            }}>
                {/* Header */}
                <div style={S.header}>
                    <div style={S.headerTitle}>
                        <span style={S.headerDot} />
                        Ask GRID
                        {contextTicker && (
                            <span style={{
                                fontFamily: MONO,
                                fontSize: '12px',
                                color: colors.accent,
                                marginLeft: '4px',
                            }}>
                                [{contextTicker}]
                            </span>
                        )}
                    </div>
                    <button style={S.closeBtn} onClick={() => setOpen(false)}>
                        &times;
                    </button>
                </div>

                {/* Quick prompts — shown when no messages */}
                {messages.length === 0 && (
                    <div style={S.quickPrompts}>
                        {QUICK_PROMPTS.map(p => (
                            <button
                                key={p}
                                style={S.quickBtn}
                                onClick={() => handleQuickPrompt(p)}
                                onMouseEnter={e => {
                                    e.currentTarget.style.borderColor = colors.accent;
                                    e.currentTarget.style.color = colors.text;
                                }}
                                onMouseLeave={e => {
                                    e.currentTarget.style.borderColor = BORDER;
                                    e.currentTarget.style.color = colors.textDim;
                                }}
                            >
                                {p}
                            </button>
                        ))}
                    </div>
                )}

                {/* Messages */}
                <div style={S.messages} ref={messagesRef}>
                    {messages.length === 0 && (
                        <div style={S.empty}>
                            Ask GRID anything about the market, your portfolio,
                            or the intelligence picture.
                        </div>
                    )}

                    {messages.map((msg, i) => {
                        if (msg.role === 'user') {
                            return <div key={i} style={S.msgUser}>{msg.content}</div>;
                        }
                        return (
                            <div key={i} style={S.msgGrid}>
                                <div>{formatAnswer(msg.content)}</div>
                                {msg.sources && msg.sources.length > 0 && (
                                    <div style={S.sources}>
                                        {msg.sources.map((s, j) => (
                                            <span key={j} style={S.sourceTag}>{s}</span>
                                        ))}
                                    </div>
                                )}
                                {msg.confidence > 0 && (
                                    <div style={S.confidence}>
                                        confidence: {(msg.confidence * 100).toFixed(0)}%
                                    </div>
                                )}
                            </div>
                        );
                    })}

                    {loading && <TypingIndicator />}
                </div>

                {/* Timeframe selector */}
                <div style={S.timeframeRow}>
                    <span style={S.timeframeLabel}>Horizon</span>
                    {TIMEFRAMES.map(tf => (
                        <button
                            key={tf.value}
                            style={S.timeframeBtn(timeframe === tf.value)}
                            onClick={() => setTimeframe(
                                timeframe === tf.value ? null : tf.value
                            )}
                        >
                            {tf.label}
                        </button>
                    ))}
                </div>

                {/* Input row */}
                <div style={S.inputRow}>
                    <input
                        ref={inputRef}
                        style={S.input}
                        placeholder={contextTicker
                            ? `Ask about ${contextTicker} or the market...`
                            : 'Ask about the market, portfolio, intelligence...'}
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        onKeyDown={handleKeyDown}
                    />
                    <button
                        style={{
                            ...S.sendBtn,
                            opacity: (!input.trim() || loading) ? 0.5 : 1,
                        }}
                        onClick={() => send()}
                        disabled={!input.trim() || loading}
                    >
                        <SendIcon />
                    </button>
                </div>
            </div>
        </>
    );
}
