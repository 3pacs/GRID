import React, { useState, useRef, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';
import { WidgetGrid } from '../components/home/widgets.jsx';
import { tickerName } from '../components/home/plain.js';

const MONO = "'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

// Friendly, plain-English example questions (the main way a non-technical
// user learns what they can ask). Phrased the way he'd actually say it.
const SUGGESTIONS = [
    'How are my stocks doing?',
    "What's the market doing right now?",
    'Should I worry about Apple and Tesla this week?',
    'Tell me when Apple hits $250',
];

/**
 * stepdad.finance home page. Plain-language in → live dashboard out.
 * Designed to be legible and forgiving for a non-technical 73-year-old:
 * big text, big buttons, calm copy, no jargon, no dead ends.
 */
export default function Home() {
    const [input, setInput] = useState('');
    const [layout, setLayout] = useState(null); // { spoken, widgets, allocation }
    const [history, setHistory] = useState([]); // [{role, content}] for context
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [gap, setGap] = useState(null); // { message, requestId, answered: null|'yes'|'no' }
    const [alertNote, setAlertNote] = useState(null); // confirmation after creating a price alert
    const [alerts, setAlerts] = useState([]); // active price alerts — what we're watching for him
    const inputRef = useRef(null);
    const topRef = useRef(null);

    const hasLayout = !!layout;

    // Focus the ask box only on the opening screen — never auto-pop the keyboard
    // after an answer arrives (it would cover the answer he just asked for).
    useEffect(() => { if (!hasLayout) inputRef.current?.focus(); }, [hasLayout]);

    // Load the alerts he's already set so he can see (and cancel) them.
    const loadAlerts = useCallback(async () => {
        try {
            const res = await api.listAlerts();
            setAlerts((res?.alerts || []).filter((a) => a.active));
        } catch { /* non-fatal — alerts panel just stays empty */ }
    }, []);
    useEffect(() => { loadAlerts(); }, [loadAlerts]);

    const removeAlert = useCallback(async (id) => {
        setAlerts((list) => list.filter((a) => a.id !== id)); // optimistic
        try { await api.cancelAlert(id); } catch { /* non-fatal */ } finally { loadAlerts(); }
    }, [loadAlerts]);

    const compose = useCallback(async (text) => {
        const q = (text ?? input).trim();
        if (!q || loading) return;
        setInput('');
        setError(null);
        setGap(null);
        setAlertNote(null);
        setLoading(true);
        const nextHistory = [...history, { role: 'user', content: q }];
        try {
            const res = await api.compose(q, history);
            if (res?.alert_created) {
                // He set a price alert — confirm plainly + refresh the watching list.
                setAlertNote(res.spoken_reply);
                loadAlerts();
                topRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } else if (res?.cannot_fulfill) {
                // Honest "can't do that yet" — show the graceful message + ping opt-in.
                setGap({ message: res.spoken_reply, requestId: res.request_id, answered: null });
                topRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            } else if (res?.error) {
                setInput(q); // restore so he doesn't have to retype
                setError('I couldn’t do that just now. Please try again.');
            } else {
                setLayout({
                    spoken: res.spoken_reply || '',
                    widgets: res.widgets || [],
                    allocation: res.allocation || [],
                });
                setHistory([...nextHistory, { role: 'assistant', content: res.spoken_reply || '' }]);
                topRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        } catch (err) {
            setInput(q); // restore on failure
            setError('I couldn’t reach the service. Please try again in a moment.');
        } finally {
            setLoading(false);
        }
    }, [input, loading, history, loadAlerts]);

    // Single-line inputs: any Enter submits.
    const handleKeyDown = (e) => {
        if (e.key === 'Enter') { e.preventDefault(); compose(); }
    };

    const reset = () => { setLayout(null); setHistory([]); setError(null); setInput(''); setGap(null); setAlertNote(null); };

    const answerGap = useCallback(async (wants) => {
        const rid = gap?.requestId;
        setGap((g) => (g ? { ...g, answered: wants ? 'yes' : 'no' } : g));
        if (rid != null) { try { await api.setCapabilityPing(rid, wants); } catch { /* non-fatal */ } }
    }, [gap]);

    const inputProps = {
        value: input,
        onChange: (e) => setInput(e.target.value),
        onKeyDown: handleKeyDown,
        enterKeyHint: 'send',
        autoComplete: 'off',
        autoCapitalize: 'sentences',
    };

    return (
        <div style={S.page}>
            <style>{'@keyframes sdBounce{0%,80%,100%{transform:translateY(0);opacity:.5}40%{transform:translateY(-6px);opacity:1}}'}</style>

            {/* ── Opening screen: brand + big ask box ── */}
            {!hasLayout && (
                <div style={S.center}>
                    <img src="/stepdad-mascot.png" alt="stepdad" style={S.mascot} />
                    <div style={S.brand}>
                        <span style={S.brandStep}>stepdad</span><span style={S.brandDot}>.</span><span style={S.brandFin}>finance</span>
                    </div>

                    {loading ? (
                        <Working />
                    ) : (
                        <>
                            {gap && <GapCard gap={gap} onAnswer={answerGap} />}
                            {alertNote && <AlertCard message={alertNote} />}
                            <div style={S.tagline}>Tap a question below, or type your own.</div>
                            <div style={S.boxWrap}>
                                <input ref={inputRef} style={S.box} placeholder="Ask me anything…" {...inputProps} />
                                <button onClick={() => compose()} disabled={!input.trim()}
                                    style={{ ...S.btn, opacity: input.trim() ? 1 : 0.4 }}>
                                    Show me
                                </button>
                            </div>
                            {error && <div style={S.error}>{error}</div>}
                            <div style={S.suggestions}>
                                {SUGGESTIONS.map((s, i) => (
                                    <button key={i} style={S.chip} onClick={() => compose(s)}>{s}</button>
                                ))}
                            </div>
                            <AlertsPanel alerts={alerts} onCancel={removeAlert} />
                        </>
                    )}
                </div>
            )}

            {/* ── Answer screen: layout + bottom ask bar ── */}
            {hasLayout && (
                <>
                    <div style={S.scroll}>
                        <div ref={topRef} />
                        <div style={S.header}>
                            <button style={S.startOver} onClick={reset}>← Start over</button>
                        </div>
                        {gap && <GapCard gap={gap} onAnswer={answerGap} />}
                        {alertNote && <AlertCard message={alertNote} />}
                        {layout.spoken && <div style={S.spoken}>{layout.spoken}</div>}
                        {layout.allocation?.length > 0 && (
                            <div style={S.allocWrap}>
                                <div style={S.allocLabel}>A sample mix — not advice:</div>
                                <div style={S.alloc}>
                                    {layout.allocation.map((a, i) => (
                                        <span key={i} style={S.allocItem}>
                                            {tickerName(a.ticker)}{a.weight ? ` ${Math.round(a.weight * 100)}%` : ''}
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                        <WidgetGrid widgets={layout.widgets} />
                        <AlertsPanel alerts={alerts} onCancel={removeAlert} />
                        {error && <div style={S.error}>{error}</div>}
                        {loading && <div style={S.workingInline}><Dots /> Working on it…</div>}
                    </div>

                    <div style={S.bottomBar}>
                        <input ref={inputRef} style={S.boxSmall} placeholder="Ask me anything else…" {...inputProps} />
                        <button onClick={() => compose()} disabled={!input.trim() || loading}
                            style={{ ...S.btnSmall, opacity: (!input.trim() || loading) ? 0.4 : 1 }}>
                            {loading ? '…' : 'Ask'}
                        </button>
                    </div>
                </>
            )}
        </div>
    );
}

function Dots() {
    return (
        <span style={S.dots}>
            <i style={S.dot} /><i style={{ ...S.dot, animationDelay: '.2s' }} /><i style={{ ...S.dot, animationDelay: '.4s' }} />
        </span>
    );
}

function Working() {
    return (
        <div style={S.working}>
            <Dots />
            <div style={S.workingText}>Working on it… this takes a few seconds.</div>
        </div>
    );
}

// Shown when dad asks for something we can't do yet: a calm message + a
// ping opt-in. His answer is recorded so he gets told when it ships.
function GapCard({ gap, onAnswer }) {
    if (gap.answered === 'yes') {
        return <div style={S.gap}><div style={S.gapMsg}>👍 Great — I’ll let you know the moment it’s ready.</div></div>;
    }
    if (gap.answered === 'no') {
        return <div style={S.gap}><div style={S.gapMsg}>No problem — I’ll still build it for you.</div></div>;
    }
    return (
        <div style={S.gap}>
            <div style={S.gapMsg}>{gap.message}</div>
            <div style={S.gapBtns}>
                <button style={S.gapYes} onClick={() => onAnswer(true)}>Yes, ping me</button>
                <button style={S.gapNo} onClick={() => onAnswer(false)}>No thanks</button>
            </div>
        </div>
    );
}

// Confirmation shown right after he sets a price alert.
function AlertCard({ message }) {
    return (
        <div style={S.alertCard}>
            <div style={S.alertCardMsg}>🔔 {message}</div>
        </div>
    );
}

// The standing list of price alerts we're watching for him, each cancelable.
function AlertsPanel({ alerts, onCancel }) {
    if (!alerts || alerts.length === 0) return null;
    return (
        <div style={S.alertsPanel}>
            <div style={S.alertsTitle}>🔔 I’m watching these for you</div>
            {alerts.map((a) => (
                <div key={a.id} style={S.alertRow}>
                    <span style={S.alertRowText}>
                        {tickerName(a.ticker)} {a.direction === 'above' ? 'goes above' : 'drops below'}{' '}
                        ${Number(a.threshold).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                    </span>
                    <button style={S.alertCancel} onClick={() => onCancel(a.id)}>Cancel</button>
                </div>
            ))}
            <div style={S.alertsFoot}>I’ll text you the moment it happens.</div>
        </div>
    );
}

const S = {
    page: { display: 'flex', flexDirection: 'column', height: '100vh', width: '100%' },
    center: {
        flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center',
        // `safe center` keeps content centered when it fits but aligns to the
        // top (instead of clipping) once the confirmation + watching list make
        // it taller than the screen — so nothing is ever unreachable.
        justifyContent: 'safe center', overflowY: 'auto', WebkitOverflowScrolling: 'touch',
        gap: '22px', padding: '24px', maxWidth: '660px',
        margin: '0 auto', width: '100%', boxSizing: 'border-box',
    },
    mascot: { width: 'auto', height: '140px', objectFit: 'contain', marginBottom: '-6px', filter: 'drop-shadow(0 6px 16px rgba(0,0,0,0.45))' },
    brand: { fontFamily: MONO, fontSize: '36px', fontWeight: 800, letterSpacing: '-0.5px' },
    brandStep: { color: colors.text },
    brandDot: { color: colors.accent },
    brandFin: { color: colors.accent },
    tagline: { fontFamily: SANS, fontSize: '19px', color: colors.text, textAlign: 'center', marginTop: '-6px' },

    boxWrap: {
        display: 'flex', width: '100%', border: `2px solid ${colors.border}`,
        borderRadius: '16px', overflow: 'hidden', background: colors.card,
    },
    box: {
        flex: 1, background: 'transparent', border: 'none', color: colors.text,
        padding: '18px 20px', fontSize: '18px', fontFamily: SANS, outline: 'none',
    },
    btn: {
        background: colors.accent, border: 'none', color: '#fff', fontSize: '18px',
        fontWeight: 700, fontFamily: SANS, cursor: 'pointer', padding: '0 22px',
        minHeight: '60px', minWidth: '110px',
    },
    suggestions: { display: 'flex', flexDirection: 'column', gap: '12px', width: '100%' },
    chip: {
        fontFamily: SANS, fontSize: '17px', color: colors.text, background: colors.card,
        border: `2px solid ${colors.border}`, borderRadius: '14px', padding: '16px 18px',
        cursor: 'pointer', textAlign: 'left', minHeight: '56px', width: '100%',
    },
    error: { fontFamily: SANS, fontSize: '17px', color: colors.red, textAlign: 'center', lineHeight: 1.5 },

    gap: {
        width: '100%', boxSizing: 'border-box',
        background: colors.card, border: `2px solid ${colors.accent}66`,
        borderRadius: '16px', padding: '20px', display: 'flex',
        flexDirection: 'column', gap: '16px',
    },
    gapMsg: { fontFamily: SANS, fontSize: '19px', lineHeight: 1.5, color: colors.text, fontWeight: 500 },
    gapBtns: { display: 'flex', gap: '12px', flexWrap: 'wrap' },
    gapYes: {
        fontFamily: SANS, fontSize: '17px', fontWeight: 700, color: '#fff',
        background: colors.accent, border: 'none', borderRadius: '12px',
        padding: '14px 22px', minHeight: '52px', cursor: 'pointer', flex: '1 1 auto',
    },
    gapNo: {
        fontFamily: SANS, fontSize: '17px', fontWeight: 600, color: colors.text,
        background: 'transparent', border: `2px solid ${colors.border}`, borderRadius: '12px',
        padding: '14px 22px', minHeight: '52px', cursor: 'pointer', flex: '1 1 auto',
    },

    alertCard: {
        width: '100%', boxSizing: 'border-box', background: `${colors.accent}14`,
        border: `2px solid ${colors.accent}66`, borderRadius: '16px', padding: '20px',
    },
    alertCardMsg: { fontFamily: SANS, fontSize: '19px', lineHeight: 1.5, color: colors.text, fontWeight: 600 },
    alertsPanel: {
        width: '100%', boxSizing: 'border-box', background: colors.card,
        border: `2px solid ${colors.border}`, borderRadius: '16px', padding: '18px',
        display: 'flex', flexDirection: 'column', gap: '12px',
    },
    alertsTitle: { fontFamily: SANS, fontSize: '17px', fontWeight: 700, color: colors.text },
    alertRow: {
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        gap: '12px', padding: '12px 0', borderTop: `1px solid ${colors.border}`,
    },
    alertRowText: { fontFamily: SANS, fontSize: '18px', fontWeight: 600, color: colors.text },
    alertCancel: {
        fontFamily: SANS, fontSize: '15px', fontWeight: 600, color: colors.text,
        background: 'transparent', border: `2px solid ${colors.border}`, borderRadius: '10px',
        padding: '10px 16px', minHeight: '44px', cursor: 'pointer', flexShrink: 0,
    },
    alertsFoot: { fontFamily: SANS, fontSize: '15px', color: colors.textDim || colors.text, opacity: 0.75 },

    working: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '14px', padding: '20px' },
    workingText: { fontFamily: SANS, fontSize: '19px', color: colors.text, fontWeight: 600 },
    workingInline: { display: 'flex', alignItems: 'center', gap: '10px', fontFamily: SANS, fontSize: '17px', color: colors.text, fontWeight: 600 },
    dots: { display: 'inline-flex', gap: '6px' },
    dot: { width: '11px', height: '11px', borderRadius: '50%', background: colors.accentLight || colors.accent, animation: 'sdBounce 1.2s infinite' },

    scroll: {
        flex: 1, overflowY: 'auto', padding: '22px', maxWidth: '1100px', width: '100%',
        margin: '0 auto', display: 'flex', flexDirection: 'column', gap: '18px',
        WebkitOverflowScrolling: 'touch',
    },
    header: { display: 'flex', alignItems: 'center' },
    startOver: {
        fontFamily: SANS, fontSize: '16px', fontWeight: 600, color: colors.text,
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: '12px',
        padding: '12px 18px', minHeight: '48px', cursor: 'pointer',
    },
    spoken: { fontFamily: SANS, fontSize: '20px', color: colors.text, lineHeight: 1.5, fontWeight: 600 },
    allocWrap: { display: 'flex', flexDirection: 'column', gap: '8px' },
    allocLabel: { fontFamily: SANS, fontSize: '15px', color: colors.textDim },
    alloc: { display: 'flex', flexWrap: 'wrap', gap: '10px' },
    allocItem: {
        fontFamily: SANS, fontSize: '16px', fontWeight: 600, color: colors.text,
        background: `${colors.accent}1A`, border: `1px solid ${colors.accent}55`,
        borderRadius: '10px', padding: '8px 14px',
    },
    bottomBar: {
        display: 'flex', gap: '10px',
        padding: '12px 20px calc(12px + env(safe-area-inset-bottom, 0px))',
        borderTop: `1px solid ${colors.border}`, maxWidth: '1100px', width: '100%',
        margin: '0 auto', background: colors.bg,
    },
    boxSmall: {
        flex: 1, background: colors.card, border: `2px solid ${colors.border}`,
        borderRadius: '14px', color: colors.text, padding: '15px 18px',
        fontSize: '17px', fontFamily: SANS, outline: 'none', minHeight: '56px',
        boxSizing: 'border-box',
    },
    btnSmall: {
        background: colors.accent, border: 'none', borderRadius: '14px', color: '#fff',
        fontSize: '17px', fontWeight: 700, fontFamily: SANS, cursor: 'pointer',
        minHeight: '56px', minWidth: '80px',
    },
};
