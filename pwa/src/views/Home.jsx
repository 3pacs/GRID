import React, { useState, useRef, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';
import { WidgetGrid } from '../components/home/widgets.jsx';

const MONO = "'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const SUGGESTIONS = [
    'Show me Apple and Tesla, and should I worry this week?',
    "What's the market doing right now?",
    'My watchlist and where the money is moving',
    "What's happening with gold and bitcoin?",
];

/**
 * stepdad.finance home page. Plain-language in → live dashboard out.
 * The user describes what they want to see; /chat/compose returns a layout
 * of widgets that each fetch their own data. Verdict cards reuse the full
 * GRID synthesis pipeline (with the publishing firewall).
 */
export default function Home() {
    const [input, setInput] = useState('');
    const [layout, setLayout] = useState(null); // { spoken, widgets, allocation }
    const [history, setHistory] = useState([]); // [{role, content}] for context
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const inputRef = useRef(null);
    const topRef = useRef(null);

    useEffect(() => { inputRef.current?.focus(); }, [layout]);

    const compose = useCallback(async (text) => {
        const q = (text ?? input).trim();
        if (!q || loading) return;
        setInput('');
        setError(null);
        setLoading(true);
        const nextHistory = [...history, { role: 'user', content: q }];
        try {
            const res = await api.compose(q, history);
            if (res?.error) {
                setError(res.message || 'Could not build that. Try rewording it.');
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
            setError(`Connection problem: ${err.message}`);
        } finally {
            setLoading(false);
        }
    }, [input, loading, history]);

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); compose(); }
    };

    const reset = () => { setLayout(null); setHistory([]); setError(null); setInput(''); };

    const hasLayout = !!layout;

    return (
        <div style={S.page}>
            {/* ── Empty state: brand + big ask box ── */}
            {!hasLayout && (
                <div style={S.center}>
                    <img src="/stepdad-mascot.png" alt="stepdad" style={S.mascot} />
                    <div style={S.brand}>
                        <span style={S.brandStep}>stepdad</span><span style={S.brandDot}>.</span><span style={S.brandFin}>finance</span>
                    </div>
                    <div style={S.tagline}>Tell me what you want to see.</div>

                    <div style={S.boxWrap}>
                        <input
                            ref={inputRef}
                            style={S.box}
                            placeholder="e.g. Show me Apple, Tesla, and tell me if I should worry…"
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={handleKeyDown}
                        />
                        <button onClick={() => compose()} disabled={!input.trim() || loading}
                            style={{ ...S.btn, opacity: (!input.trim() || loading) ? 0.35 : 1 }}>
                            {loading ? '…' : '→'}
                        </button>
                    </div>

                    {error && <div style={S.error}>{error}</div>}

                    <div style={S.suggestions}>
                        {SUGGESTIONS.map((s, i) => (
                            <button key={i} style={S.chip} onClick={() => compose(s)} disabled={loading}>
                                {s}
                            </button>
                        ))}
                    </div>
                    {loading && <div style={S.building}>Building your page…</div>}
                </div>
            )}

            {/* ── Composed state: layout + bottom ask bar ── */}
            {hasLayout && (
                <>
                    <div style={S.scroll}>
                        <div ref={topRef} />
                        <div style={S.header}>
                            <button style={S.home} onClick={reset} title="Start over">
                                <span style={S.brandStepSm}>stepdad</span><span style={S.brandDotSm}>.</span><span style={S.brandFinSm}>finance</span>
                            </button>
                        </div>
                        {layout.spoken && <div style={S.spoken}>{layout.spoken}</div>}
                        {layout.allocation?.length > 0 && (
                            <div style={S.alloc}>
                                {layout.allocation.map((a, i) => (
                                    <span key={i} style={S.allocItem}>
                                        {a.ticker}{a.weight ? ` ${Math.round(a.weight * 100)}%` : ''}
                                    </span>
                                ))}
                            </div>
                        )}
                        <WidgetGrid widgets={layout.widgets} />
                        {error && <div style={S.error}>{error}</div>}
                    </div>

                    <div style={S.bottomBar}>
                        <input
                            ref={inputRef}
                            style={S.boxSmall}
                            placeholder="Change it — ask for something else…"
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={handleKeyDown}
                        />
                        <button onClick={() => compose()} disabled={!input.trim() || loading}
                            style={{ ...S.btnSmall, opacity: (!input.trim() || loading) ? 0.35 : 1 }}>
                            {loading ? '…' : '→'}
                        </button>
                    </div>
                </>
            )}
        </div>
    );
}

const S = {
    page: { display: 'flex', flexDirection: 'column', height: '100vh', width: '100%' },
    center: {
        flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', gap: '20px', padding: '20px', maxWidth: '640px',
        margin: '0 auto', width: '100%',
    },
    mascot: { width: 'auto', height: '132px', objectFit: 'contain', marginBottom: '-6px', filter: 'drop-shadow(0 6px 16px rgba(0,0,0,0.45))' },
    brand: { fontFamily: MONO, fontSize: '34px', fontWeight: 800, letterSpacing: '-0.5px' },
    brandStep: { color: colors.text },
    brandDot: { color: colors.accent },
    brandFin: { color: colors.accent },
    tagline: { fontFamily: SANS, fontSize: '16px', color: colors.textDim, marginTop: '-8px' },
    boxWrap: {
        display: 'flex', width: '100%', border: `1px solid ${colors.border}`,
        borderRadius: '14px', overflow: 'hidden', background: colors.card,
    },
    box: {
        flex: 1, background: 'transparent', border: 'none', color: colors.text,
        padding: '16px 18px', fontSize: '15px', fontFamily: SANS, outline: 'none',
    },
    btn: {
        width: '56px', background: colors.accent, border: 'none', color: '#fff',
        fontSize: '20px', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
    },
    suggestions: { display: 'flex', flexWrap: 'wrap', gap: '8px', justifyContent: 'center' },
    chip: {
        fontFamily: SANS, fontSize: '13px', color: colors.textDim, background: colors.card,
        border: `1px solid ${colors.border}`, borderRadius: '20px', padding: '8px 14px',
        cursor: 'pointer', textAlign: 'left',
    },
    building: { fontFamily: SANS, fontSize: '14px', color: colors.textMuted },
    error: { fontFamily: SANS, fontSize: '14px', color: colors.red, textAlign: 'center' },

    scroll: {
        flex: 1, overflowY: 'auto', padding: '20px', maxWidth: '1100px', width: '100%',
        margin: '0 auto', display: 'flex', flexDirection: 'column', gap: '14px',
        WebkitOverflowScrolling: 'touch',
    },
    header: { display: 'flex', alignItems: 'center' },
    home: { background: 'none', border: 'none', cursor: 'pointer', padding: 0, fontFamily: MONO, fontSize: '18px', fontWeight: 800 },
    brandStepSm: { color: colors.text },
    brandDotSm: { color: colors.accent },
    brandFinSm: { color: colors.accent },
    spoken: { fontFamily: SANS, fontSize: '17px', color: colors.text, lineHeight: 1.5, fontWeight: 500 },
    alloc: { display: 'flex', flexWrap: 'wrap', gap: '8px' },
    allocItem: {
        fontFamily: MONO, fontSize: '12px', color: colors.accent, background: `${colors.accent}1A`,
        border: `1px solid ${colors.accent}44`, borderRadius: '6px', padding: '3px 9px',
    },
    bottomBar: {
        display: 'flex', padding: '10px 20px calc(10px + env(safe-area-inset-bottom, 0px))',
        borderTop: `1px solid ${colors.border}`, maxWidth: '1100px', width: '100%',
        margin: '0 auto', background: colors.bg, gap: '0',
    },
    boxSmall: {
        flex: 1, background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: '10px 0 0 10px', color: colors.text, padding: '13px 16px',
        fontSize: '14px', fontFamily: SANS, outline: 'none',
    },
    btnSmall: {
        width: '50px', background: colors.accent, border: 'none', borderRadius: '0 10px 10px 0',
        color: '#fff', fontSize: '17px', cursor: 'pointer', display: 'flex',
        alignItems: 'center', justifyContent: 'center',
    },
};
