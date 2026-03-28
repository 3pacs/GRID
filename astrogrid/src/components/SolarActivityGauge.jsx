import React from 'react';
import { tokens } from '../styles/tokens.js';

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function gaugeColor(value) {
    const v = clamp(Number(value) || 0, 0, 9);
    if (v < 3) return tokens.green;
    if (v < 5) return tokens.gold;
    if (v < 7) return tokens.purple;
    return tokens.red;
}

function Metric({ label, value }) {
    return (
        <div style={metricCard}>
            <div style={metaLabel}>{label}</div>
            <div style={metricValue}>{value}</div>
        </div>
    );
}

export default function SolarActivityGauge({
    kpIndex = null,
    sunspotNumber = null,
    solarWindSpeed = null,
    flareClass = null,
    title = 'Solar Activity',
}) {
    const value = clamp(Number(kpIndex) || 0, 0, 9);
    const pct = (value / 9) * 100;
    const color = gaugeColor(value);

    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>Solar Weather</div>
                    <div style={headline}>{title}</div>
                </div>
                <div style={kpLabel}>Kp {kpIndex ?? '--'}</div>
            </div>

            <div style={{
                position: 'relative',
                height: '14px',
                borderRadius: tokens.radius.pill,
                background: 'rgba(10, 18, 35, 0.8)',
                border: `1px solid ${tokens.cardBorder}`,
                overflow: 'hidden',
            }}>
                <div style={{
                    width: `${pct}%`,
                    height: '100%',
                    background: `linear-gradient(90deg, ${tokens.accent}, ${color})`,
                    boxShadow: `0 0 18px ${color}`,
                }} />
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))',
                gap: tokens.spacing.sm,
                marginTop: tokens.spacing.md,
            }}>
                <Metric label="Kp" value={kpIndex ?? '--'} />
                <Metric label="Sunspots" value={sunspotNumber ?? '--'} />
                <Metric label="Wind" value={solarWindSpeed ? `${solarWindSpeed} km/s` : '--'} />
                <Metric label="Flares" value={flareClass || '--'} />
            </div>
        </div>
    );
}

const cardStyle = {
    background: tokens.card,
    border: `1px solid ${tokens.cardBorder}`,
    borderRadius: tokens.radius.xl,
    padding: tokens.spacing.lg,
};

const headerRow = {
    display: 'flex',
    justifyContent: 'space-between',
    gap: tokens.spacing.md,
    flexWrap: 'wrap',
    marginBottom: tokens.spacing.md,
};

const eyebrow = {
    fontSize: '11px',
    color: tokens.accent,
    fontFamily: tokens.fontMono,
    letterSpacing: '2px',
    textTransform: 'uppercase',
};

const headline = {
    marginTop: '4px',
    fontSize: '20px',
    fontWeight: 700,
    color: tokens.textBright,
};

const kpLabel = {
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    fontWeight: 700,
};

const metricCard = {
    padding: '12px',
    borderRadius: tokens.radius.md,
    background: 'rgba(10, 18, 35, 0.6)',
    border: `1px solid ${tokens.cardBorder}`,
};

const metaLabel = {
    fontSize: '10px',
    letterSpacing: '1px',
    textTransform: 'uppercase',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
};

const metricValue = {
    marginTop: '6px',
    fontSize: '14px',
    fontWeight: 700,
    color: tokens.textBright,
    lineHeight: 1.4,
};
