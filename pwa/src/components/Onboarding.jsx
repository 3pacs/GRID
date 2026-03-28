import React, { useState, useEffect, useCallback, useRef } from 'react';
import { colors, tokens } from '../styles/shared.js';

/* ─── Constants ────────────────────────────────────────────────────── */

const STORAGE_KEY = 'grid_onboarded';
const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";
const ACCENT = '#1A6EBF';
const OVERLAY_BG = 'rgba(4, 6, 10, 0.85)';
const TOOLTIP_BG = '#111B2A';
const SPOTLIGHT_PADDING = 12;
const SPOTLIGHT_RADIUS = 14;
const TRANSITION_MS = 300;

/* ─── Tour Steps ───────────────────────────────────────────────────── */

const STEPS = [
    {
        id: 'welcome',
        target: null,  // full screen
        title: 'Welcome to GRID Intelligence',
        body: 'Your systematic trading intelligence platform. Let us show you around.',
        showLogo: true,
    },
    {
        id: 'watchlist',
        target: '[data-onboarding="watchlist"]',
        title: 'Your Watchlist',
        body: 'Add tickers to track real-time prices, signals, and AI-generated insights for any asset.',
        position: 'bottom',
    },
    {
        id: 'worldviews',
        target: '[data-onboarding="tab-bar"]',
        title: '7 World Views',
        body: 'Flow: follow the money. Power: who moves markets. Truth: verify official data. Globe: macro geography. Risk: threat radar. Signal: AI alerts. Home ties it together.',
        position: 'bottom',
    },
    {
        id: 'chat',
        target: '[data-onboarding="chat-fab"]',
        title: 'Ask GRID',
        body: 'Ask anything about the market. GRID combines live data, regime awareness, and LLM reasoning to answer.',
        position: 'left',
    },
    {
        id: 'palette',
        target: null,  // full screen, show shortcut
        title: 'Command Palette',
        body: 'Press Cmd+K (or Ctrl+K) to instantly jump to any view, ticker, or action. Power-user speed.',
        showShortcut: true,
    },
    {
        id: 'background',
        target: null,  // full screen
        title: 'Intelligence Runs 24/7',
        body: 'Hermes and Qwen work in the background -- ingesting data from 37+ sources, scoring trust, detecting regime shifts, and surfacing opportunities while you sleep.',
        showPulse: true,
    },
    {
        id: 'ready',
        target: null,  // full screen
        title: "You're Ready",
        body: 'Explore freely. You can replay this tour anytime from Settings.',
        showReady: true,
    },
];

/* ─── Styles ───────────────────────────────────────────────────────── */

const S = {
    overlay: {
        position: 'fixed',
        inset: 0,
        zIndex: 10000,
        pointerEvents: 'auto',
    },
    tooltipOuter: {
        position: 'fixed',
        zIndex: 10002,
        transition: `all ${TRANSITION_MS}ms cubic-bezier(0.4, 0, 0.2, 1)`,
        pointerEvents: 'auto',
    },
    tooltip: {
        background: TOOLTIP_BG,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.lg,
        padding: '24px',
        maxWidth: '380px',
        width: 'calc(100vw - 40px)',
        boxShadow: '0 12px 40px rgba(0,0,0,0.6), 0 0 0 1px rgba(26,110,191,0.15)',
    },
    fullCenter: {
        position: 'fixed',
        inset: 0,
        zIndex: 10002,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        padding: '40px 24px',
        textAlign: 'center',
        pointerEvents: 'auto',
    },
    title: {
        fontFamily: SANS,
        fontSize: '20px',
        fontWeight: 700,
        color: '#E8F0F8',
        marginBottom: '10px',
    },
    body: {
        fontFamily: SANS,
        fontSize: '14px',
        lineHeight: '1.65',
        color: colors.textDim,
        marginBottom: '20px',
    },
    btnRow: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: '12px',
    },
    btnNext: {
        background: ACCENT,
        color: '#fff',
        border: 'none',
        borderRadius: tokens.radius.sm,
        padding: '10px 24px',
        fontSize: '13px',
        fontWeight: 600,
        fontFamily: SANS,
        cursor: 'pointer',
        minHeight: '40px',
        transition: `all ${tokens.transition.fast}`,
    },
    btnSkip: {
        background: 'none',
        color: colors.textMuted,
        border: 'none',
        padding: '10px 16px',
        fontSize: '13px',
        fontFamily: SANS,
        cursor: 'pointer',
        minHeight: '40px',
    },
    dots: {
        display: 'flex',
        gap: '6px',
        justifyContent: 'center',
        marginBottom: '20px',
    },
    dot: (active) => ({
        width: active ? '20px' : '8px',
        height: '8px',
        borderRadius: '4px',
        background: active ? ACCENT : colors.border,
        transition: `all ${TRANSITION_MS}ms ease`,
    }),
    logo: {
        fontFamily: MONO,
        fontSize: '42px',
        fontWeight: 800,
        letterSpacing: '12px',
        color: ACCENT,
        marginBottom: '16px',
        textShadow: '0 0 40px rgba(26,110,191,0.4)',
    },
    shortcutBox: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        padding: '10px 20px',
        fontFamily: MONO,
        fontSize: '18px',
        fontWeight: 600,
        color: '#E8F0F8',
        marginBottom: '20px',
        letterSpacing: '2px',
    },
    pulseRing: {
        width: '80px',
        height: '80px',
        borderRadius: '50%',
        border: `2px solid ${ACCENT}`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        marginBottom: '20px',
        position: 'relative',
    },
    pulseInner: {
        width: '12px',
        height: '12px',
        borderRadius: '50%',
        background: colors.green,
        boxShadow: `0 0 12px ${colors.green}`,
    },
    readyCheck: {
        width: '64px',
        height: '64px',
        borderRadius: '50%',
        background: `${colors.green}22`,
        border: `2px solid ${colors.green}`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        marginBottom: '20px',
        fontSize: '28px',
    },
};

/* ─── SVG overlay with spotlight cutout ────────────────────────────── */

function SpotlightOverlay({ rect }) {
    if (!rect) {
        return (
            <div style={{ ...S.overlay, background: OVERLAY_BG }} />
        );
    }

    const x = rect.left - SPOTLIGHT_PADDING;
    const y = rect.top - SPOTLIGHT_PADDING;
    const w = rect.width + SPOTLIGHT_PADDING * 2;
    const h = rect.height + SPOTLIGHT_PADDING * 2;
    const r = SPOTLIGHT_RADIUS;

    return (
        <svg
            style={{ ...S.overlay }}
            width="100%"
            height="100%"
            viewBox={`0 0 ${window.innerWidth} ${window.innerHeight}`}
            preserveAspectRatio="none"
        >
            <defs>
                <mask id="onboarding-mask">
                    <rect x="0" y="0" width="100%" height="100%" fill="white" />
                    <rect
                        x={x} y={y} width={w} height={h}
                        rx={r} ry={r}
                        fill="black"
                    />
                </mask>
            </defs>
            <rect
                x="0" y="0"
                width="100%" height="100%"
                fill={OVERLAY_BG}
                mask="url(#onboarding-mask)"
            />
            {/* Spotlight border glow */}
            <rect
                x={x} y={y} width={w} height={h}
                rx={r} ry={r}
                fill="none"
                stroke={ACCENT}
                strokeWidth="2"
                opacity="0.5"
            />
        </svg>
    );
}

/* ─── Tooltip positioning ──────────────────────────────────────────── */

function getTooltipPosition(targetRect, position) {
    if (!targetRect) return {};

    const margin = 16;
    const tooltipW = Math.min(380, window.innerWidth - 40);

    switch (position) {
        case 'bottom': {
            const left = Math.max(20, Math.min(
                targetRect.left + targetRect.width / 2 - tooltipW / 2,
                window.innerWidth - tooltipW - 20
            ));
            return {
                top: targetRect.bottom + SPOTLIGHT_PADDING + margin,
                left,
            };
        }
        case 'top': {
            const left = Math.max(20, Math.min(
                targetRect.left + targetRect.width / 2 - tooltipW / 2,
                window.innerWidth - tooltipW - 20
            ));
            return {
                bottom: window.innerHeight - targetRect.top + SPOTLIGHT_PADDING + margin,
                left,
            };
        }
        case 'left': {
            return {
                top: Math.max(20, targetRect.top + targetRect.height / 2 - 80),
                right: window.innerWidth - targetRect.left + SPOTLIGHT_PADDING + margin,
            };
        }
        case 'right': {
            return {
                top: Math.max(20, targetRect.top + targetRect.height / 2 - 80),
                left: targetRect.right + SPOTLIGHT_PADDING + margin,
            };
        }
        default:
            return { top: targetRect.bottom + margin, left: 20 };
    }
}

/* ─── Pulse animation via keyframes (injected once) ────────────────── */

let animInjected = false;
function injectAnimations() {
    if (animInjected) return;
    animInjected = true;
    const style = document.createElement('style');
    style.textContent = `
        @keyframes onboarding-pulse {
            0% { box-shadow: 0 0 0 0 rgba(26,110,191,0.4); }
            70% { box-shadow: 0 0 0 20px rgba(26,110,191,0); }
            100% { box-shadow: 0 0 0 0 rgba(26,110,191,0); }
        }
        @keyframes onboarding-fade-in {
            from { opacity: 0; transform: translateY(8px); }
            to { opacity: 1; transform: translateY(0); }
        }
    `;
    document.head.appendChild(style);
}

/* ─── Main Component ───────────────────────────────────────────────── */

export default function Onboarding({ forceShow, onDismiss }) {
    const [visible, setVisible] = useState(false);
    const [step, setStep] = useState(0);
    const [targetRect, setTargetRect] = useState(null);
    const [fadeKey, setFadeKey] = useState(0);
    const rafRef = useRef(null);

    // Check if tour should show
    useEffect(() => {
        injectAnimations();
        if (forceShow) {
            setVisible(true);
            setStep(0);
            return;
        }
        const onboarded = localStorage.getItem(STORAGE_KEY);
        if (!onboarded) {
            // Small delay so the app renders first
            const timer = setTimeout(() => setVisible(true), 800);
            return () => clearTimeout(timer);
        }
    }, [forceShow]);

    // Measure target element for current step
    const measureTarget = useCallback(() => {
        const current = STEPS[step];
        if (!current || !current.target) {
            setTargetRect(null);
            return;
        }
        const el = document.querySelector(current.target);
        if (el) {
            const rect = el.getBoundingClientRect();
            setTargetRect({
                top: rect.top,
                left: rect.left,
                width: rect.width,
                height: rect.height,
                bottom: rect.bottom,
                right: rect.right,
            });
        } else {
            setTargetRect(null);
        }
    }, [step]);

    useEffect(() => {
        if (!visible) return;
        measureTarget();
        // Re-measure on scroll/resize
        const handleUpdate = () => {
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
            rafRef.current = requestAnimationFrame(measureTarget);
        };
        window.addEventListener('resize', handleUpdate);
        window.addEventListener('scroll', handleUpdate, true);
        return () => {
            window.removeEventListener('resize', handleUpdate);
            window.removeEventListener('scroll', handleUpdate, true);
            if (rafRef.current) cancelAnimationFrame(rafRef.current);
        };
    }, [visible, step, measureTarget]);

    const finish = useCallback(() => {
        localStorage.setItem(STORAGE_KEY, 'true');
        setVisible(false);
        setStep(0);
        onDismiss?.();
    }, [onDismiss]);

    const next = useCallback(() => {
        if (step >= STEPS.length - 1) {
            finish();
        } else {
            setStep(s => s + 1);
            setFadeKey(k => k + 1);
        }
    }, [step, finish]);

    const skip = useCallback(() => {
        finish();
    }, [finish]);

    // Keyboard support
    useEffect(() => {
        if (!visible) return;
        const handleKey = (e) => {
            if (e.key === 'Escape') skip();
            if (e.key === 'Enter' || e.key === 'ArrowRight') next();
            if (e.key === 'ArrowLeft' && step > 0) {
                setStep(s => s - 1);
                setFadeKey(k => k + 1);
            }
        };
        window.addEventListener('keydown', handleKey);
        return () => window.removeEventListener('keydown', handleKey);
    }, [visible, next, skip, step]);

    if (!visible) return null;

    const current = STEPS[step];
    const isFullScreen = !current.target;
    const isMac = typeof navigator !== 'undefined' && /Mac/.test(navigator.platform);

    /* ── Step indicator dots ──────────────────────────────── */
    const dotsEl = (
        <div style={S.dots}>
            {STEPS.map((_, i) => (
                <div key={i} style={S.dot(i === step)} />
            ))}
        </div>
    );

    /* ── Button row ───────────────────────────────────────── */
    const isLast = step === STEPS.length - 1;
    const buttonsEl = (
        <div style={S.btnRow}>
            <button onClick={skip} style={S.btnSkip}>
                {isLast ? '' : 'Skip tour'}
            </button>
            <button onClick={next} style={S.btnNext}>
                {isLast ? 'Get Started' : 'Next'}
            </button>
        </div>
    );

    /* ── Full-screen steps (welcome, palette, background, ready) ── */
    if (isFullScreen) {
        return (
            <div onClick={(e) => e.stopPropagation()}>
                <div style={{ ...S.overlay, background: OVERLAY_BG }} />
                <div
                    key={fadeKey}
                    style={{
                        ...S.fullCenter,
                        animation: `onboarding-fade-in ${TRANSITION_MS}ms ease`,
                    }}
                >
                    {current.showLogo && (
                        <div style={S.logo}>GRID</div>
                    )}
                    {current.showShortcut && (
                        <div style={S.shortcutBox}>
                            {isMac ? '\u2318' : 'Ctrl+'} K
                        </div>
                    )}
                    {current.showPulse && (
                        <div style={{
                            ...S.pulseRing,
                            animation: 'onboarding-pulse 2s infinite',
                        }}>
                            <div style={S.pulseInner} />
                        </div>
                    )}
                    {current.showReady && (
                        <div style={S.readyCheck}>
                            <span role="img" aria-label="check" style={{ lineHeight: 1 }}>{'\u2713'}</span>
                        </div>
                    )}
                    <div style={S.title}>{current.title}</div>
                    <div style={{ ...S.body, maxWidth: '340px' }}>{current.body}</div>
                    {dotsEl}
                    {buttonsEl}
                </div>
            </div>
        );
    }

    /* ── Spotlight steps (targeted) ───────────────────────── */
    const tooltipPos = getTooltipPosition(targetRect, current.position);

    return (
        <div onClick={(e) => e.stopPropagation()}>
            <SpotlightOverlay rect={targetRect} />
            <div
                key={fadeKey}
                style={{
                    ...S.tooltipOuter,
                    ...tooltipPos,
                    animation: `onboarding-fade-in ${TRANSITION_MS}ms ease`,
                }}
            >
                <div style={S.tooltip}>
                    <div style={S.title}>{current.title}</div>
                    <div style={S.body}>{current.body}</div>
                    {dotsEl}
                    {buttonsEl}
                </div>
            </div>
        </div>
    );
}

/* ─── Utility: reset onboarding (for "Show Tour Again") ────────────── */

export function resetOnboarding() {
    localStorage.removeItem(STORAGE_KEY);
}

export function isOnboarded() {
    return !!localStorage.getItem(STORAGE_KEY);
}
