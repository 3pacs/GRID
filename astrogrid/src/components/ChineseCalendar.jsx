import React from 'react';
import { tokens } from '../styles/tokens.js';

function Metric({ label, value }) {
    return (
        <div style={metricCard}>
            <div style={metaLabel}>{label}</div>
            <div style={metricValue}>{value}</div>
        </div>
    );
}

export default function ChineseCalendar({
    animal = 'Unknown',
    element = 'Unknown',
    yinYang = 'Yang',
    flyingStar = null,
    lunarMonth = null,
    hexagram = null,
}) {
    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>Chinese Calendar</div>
                    <div style={headline}>{element} {animal}</div>
                </div>
                <div style={headerStat}>{yinYang}</div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))',
                gap: tokens.spacing.sm,
            }}>
                <Metric label="Animal" value={animal} />
                <Metric label="Element" value={element} />
                <Metric label="Yin/Yang" value={yinYang} />
                <Metric label="Flying Star" value={flyingStar ?? '--'} />
                <Metric label="Lunar Month" value={lunarMonth ?? '--'} />
                <Metric label="Hexagram" value={hexagram ?? '--'} />
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

const headerStat = {
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    alignSelf: 'end',
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
};
