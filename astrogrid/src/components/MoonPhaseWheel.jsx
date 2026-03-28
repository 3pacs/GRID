import React from 'react';
import { tokens } from '../styles/tokens.js';

function clamp01(value) {
    return Math.max(0, Math.min(1, value));
}

function phaseLabel(phase) {
    if (phase == null || Number.isNaN(phase)) return 'Unknown';
    if (phase < 0.03) return 'New Moon';
    if (phase < 0.23) return 'Waxing Crescent';
    if (phase < 0.28) return 'First Quarter';
    if (phase < 0.48) return 'Waxing Gibbous';
    if (phase < 0.53) return 'Full Moon';
    if (phase < 0.73) return 'Waning Gibbous';
    if (phase < 0.78) return 'Last Quarter';
    if (phase < 0.98) return 'Waning Crescent';
    return 'New Moon';
}

function MiniStat({ label, value }) {
    return (
        <div style={{
            padding: '12px',
            borderRadius: tokens.radius.md,
            background: 'rgba(10, 18, 35, 0.55)',
            border: `1px solid ${tokens.cardBorder}`,
        }}>
            <div style={metaLabel}>{label}</div>
            <div style={metaValue}>{value}</div>
        </div>
    );
}

export default function MoonPhaseWheel({
    phase = 0,
    illumination = null,
    label,
    regime = null,
    subtitle,
    size = 240,
}) {
    const phaseValue = clamp01(Number(phase));
    const illumValue = illumination == null
        ? Math.round((1 - Math.cos(phaseValue * Math.PI * 2)) * 50)
        : illumination;
    const displayLabel = label || phaseLabel(phaseValue);
    const isWaxing = phaseValue < 0.5;
    const shadowOffset = (phaseValue < 0.5 ? 1 - phaseValue * 2 : (phaseValue - 0.5) * 2) * (size * 0.32);
    const shadowX = isWaxing ? shadowOffset : -shadowOffset;

    const ringSegments = Array.from({ length: 12 }, (_, index) => {
        const angle = (index / 12) * 360;
        const active = phaseValue * 12 >= index;
        return (
            <div
                key={index}
                style={{
                    position: 'absolute',
                    inset: 0,
                    borderRadius: '50%',
                    border: `1px solid ${active ? tokens.accent : tokens.cardBorder}`,
                    transform: `rotate(${angle}deg) scale(${1 - index * 0.017})`,
                    opacity: active ? 0.3 : 0.12,
                    pointerEvents: 'none',
                }}
            />
        );
    });

    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>Moon Phase</div>
                    <div style={headline}>{displayLabel}</div>
                </div>
                <div style={headerStat}>{illumValue}% illumination</div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(180px, 1fr) minmax(0, 1fr)',
                gap: tokens.spacing.lg,
                alignItems: 'center',
            }}>
                <div style={{
                    position: 'relative',
                    width: '100%',
                    maxWidth: `${size}px`,
                    aspectRatio: '1',
                    margin: '0 auto',
                }}>
                    {ringSegments}
                    <div style={{
                        position: 'absolute',
                        inset: '6%',
                        borderRadius: '50%',
                        background: 'radial-gradient(circle at 35% 35%, #E8F0F8 0%, #AAB8C7 24%, #425166 57%, #0A1628 100%)',
                        boxShadow: 'inset 0 0 20px rgba(0,0,0,0.4)',
                        overflow: 'hidden',
                    }}>
                        <div style={{
                            position: 'absolute',
                            inset: 0,
                            background: `radial-gradient(circle at ${50 + shadowX / size * 80}% 50%, rgba(5,8,16,0.05) 0%, rgba(5,8,16,0.55) 52%, rgba(5,8,16,0.9) 100%)`,
                            mixBlendMode: 'multiply',
                        }} />
                        <div style={{
                            position: 'absolute',
                            inset: 0,
                            background: isWaxing
                                ? 'linear-gradient(90deg, rgba(255,255,255,0.88) 0%, rgba(255,255,255,0.18) 100%)'
                                : 'linear-gradient(270deg, rgba(255,255,255,0.88) 0%, rgba(255,255,255,0.18) 100%)',
                            opacity: 0.9,
                        }} />
                    </div>
                    <div style={{
                        position: 'absolute',
                        inset: '-2%',
                        borderRadius: '50%',
                        border: `1px solid ${tokens.cardBorder}`,
                        boxShadow: 'inset 0 0 0 1px rgba(74, 158, 255, 0.08)',
                    }} />
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: tokens.spacing.sm }}>
                    <div style={panelStyle}>
                        <div style={metaLabel}>Current Signal</div>
                        <div style={{ ...metaValue, fontSize: '18px' }}>{displayLabel}</div>
                        <div style={detailText}>
                            {subtitle || `A ${phaseValue < 0.5 ? 'waxing' : 'waning'} lunar cycle with ${illumValue}% illumination.`}
                        </div>
                    </div>

                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
                        gap: tokens.spacing.sm,
                    }}>
                        <MiniStat label="Phase" value={`${Math.round(phaseValue * 100)}%`} />
                        <MiniStat label="Illumination" value={`${illumValue}%`} />
                        <MiniStat label="Regime" value={regime || 'Neutral'} />
                        <MiniStat label="Direction" value={phaseValue < 0.5 ? 'Waxing' : 'Waning'} />
                    </div>
                </div>
            </div>
        </div>
    );
}

const cardStyle = {
    background: tokens.card,
    border: `1px solid ${tokens.cardBorder}`,
    borderRadius: tokens.radius.xl,
    padding: tokens.spacing.lg,
    backdropFilter: 'blur(12px)',
    WebkitBackdropFilter: 'blur(12px)',
    boxShadow: '0 24px 70px rgba(0, 0, 0, 0.35)',
};

const headerRow = {
    display: 'flex',
    justifyContent: 'space-between',
    gap: tokens.spacing.md,
    alignItems: 'baseline',
    marginBottom: tokens.spacing.md,
    flexWrap: 'wrap',
};

const eyebrow = {
    fontSize: '11px',
    color: tokens.accent,
    fontFamily: tokens.fontMono,
    letterSpacing: '2px',
    textTransform: 'uppercase',
};

const headline = {
    fontSize: '20px',
    color: tokens.textBright,
    fontWeight: 700,
    marginTop: '4px',
};

const headerStat = {
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    textAlign: 'right',
};

const panelStyle = {
    padding: '12px 14px',
    borderRadius: tokens.radius.md,
    background: 'rgba(10, 18, 35, 0.62)',
    border: `1px solid ${tokens.cardBorder}`,
};

const metaLabel = {
    fontSize: '10px',
    letterSpacing: '1.2px',
    textTransform: 'uppercase',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
};

const metaValue = {
    fontSize: '16px',
    fontWeight: 700,
    color: tokens.textBright,
    marginTop: '6px',
};

const detailText = {
    marginTop: '4px',
    fontSize: '13px',
    color: tokens.text,
    lineHeight: 1.5,
};
