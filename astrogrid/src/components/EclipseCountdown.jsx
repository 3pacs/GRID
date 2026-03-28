import React from 'react';
import { tokens } from '../styles/tokens.js';

function getCountdown(targetDate) {
    const target = targetDate ? new Date(targetDate) : null;
    if (!target || Number.isNaN(target.getTime())) return null;

    const diffMs = target.getTime() - Date.now();
    const future = diffMs >= 0;
    const abs = Math.abs(diffMs);
    const days = Math.floor(abs / 86400000);
    const hours = Math.floor((abs % 86400000) / 3600000);
    const minutes = Math.floor((abs % 3600000) / 60000);

    return { future, days, hours, minutes };
}

function Metric({ label, value, accent }) {
    return (
        <div style={{
            padding: '12px',
            borderRadius: tokens.radius.md,
            background: 'rgba(10, 18, 35, 0.6)',
            border: `1px solid ${tokens.cardBorder}`,
            textAlign: 'center',
        }}>
            <div style={metaLabel}>{label}</div>
            <div style={{ ...metaValue, color: accent }}>{value}</div>
        </div>
    );
}

export default function EclipseCountdown({
    eclipse = null,
    title = 'Next Eclipse',
    accent = tokens.purple,
}) {
    const countdown = getCountdown(eclipse?.date || eclipse?.datetime || eclipse?.target_date);

    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>{title}</div>
                    <div style={headline}>{eclipse?.name || eclipse?.event || 'Eclipse Event'}</div>
                </div>
                <div style={headerStat}>{eclipse?.type || eclipse?.kind || 'event'}</div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(92px, 1fr))',
                gap: tokens.spacing.sm,
            }}>
                <Metric label="Days" value={countdown ? countdown.days : '--'} accent={accent} />
                <Metric label="Hours" value={countdown ? countdown.hours : '--'} accent={accent} />
                <Metric label="Minutes" value={countdown ? countdown.minutes : '--'} accent={accent} />
                <Metric label="State" value={countdown ? (countdown.future ? 'Ahead' : 'Passed') : 'Unknown'} accent={accent} />
            </div>

            {eclipse?.description && (
                <div style={detailText}>{eclipse.description}</div>
            )}
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

const headerStat = {
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
};

const metaLabel = {
    fontSize: '10px',
    letterSpacing: '1px',
    textTransform: 'uppercase',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
};

const metaValue = {
    marginTop: '6px',
    fontSize: '18px',
    fontWeight: 700,
};

const detailText = {
    marginTop: tokens.spacing.md,
    color: tokens.text,
    fontSize: '13px',
    lineHeight: 1.6,
};
