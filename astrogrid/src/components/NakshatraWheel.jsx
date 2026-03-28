import React from 'react';
import { tokens } from '../styles/tokens.js';

const NAKSHATRAS = [
    'Ashwini', 'Bharani', 'Krittika', 'Rohini', 'Mrigashira', 'Ardra', 'Punarvasu', 'Pushya', 'Ashlesha',
    'Magha', 'Purva Phalguni', 'Uttara Phalguni', 'Hasta', 'Chitra', 'Swati', 'Vishakha', 'Anuradha',
    'Jyeshtha', 'Mula', 'Purva Ashadha', 'Uttara Ashadha', 'Shravana', 'Dhanishta', 'Shatabhisha',
    'Purva Bhadrapada', 'Uttara Bhadrapada', 'Revati',
];

function clampIndex(value) {
    const raw = Number(value);
    if (!Number.isFinite(raw)) return 0;
    return Math.max(0, Math.min(26, Math.round(raw)));
}

function Metric({ label, value }) {
    return (
        <div style={metricCard}>
            <div style={metaLabel}>{label}</div>
            <div style={metaValue}>{value}</div>
        </div>
    );
}

export default function NakshatraWheel({
    index = 0,
    name = null,
    quality = null,
    rulingPlanet = null,
    deity = null,
    size = 280,
}) {
    const activeIndex = clampIndex(index);
    const anglePer = 360 / 27;

    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>Nakshatra Wheel</div>
                    <div style={headline}>{name || NAKSHATRAS[activeIndex]}</div>
                </div>
                <div style={headerStat}>27 lunar mansions</div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(220px, 1fr) minmax(0, 1fr)',
                gap: tokens.spacing.lg,
                alignItems: 'center',
            }}>
                <div style={{
                    position: 'relative',
                    width: '100%',
                    maxWidth: `${size}px`,
                    aspectRatio: '1',
                    margin: '0 auto',
                    borderRadius: '50%',
                    background: 'radial-gradient(circle at 50% 50%, rgba(74, 158, 255, 0.15) 0%, rgba(5, 8, 16, 0.96) 68%)',
                    border: `1px solid ${tokens.cardBorder}`,
                    overflow: 'hidden',
                }}>
                    {NAKSHATRAS.map((segment, idx) => {
                        const rotate = idx * anglePer;
                        const selected = idx === activeIndex;
                        return (
                            <div
                                key={segment}
                                style={{
                                    position: 'absolute',
                                    inset: 0,
                                    clipPath: 'polygon(50% 50%, 50% 0%, 100% 0%, 100% 100%)',
                                    transform: `rotate(${rotate}deg)`,
                                    transformOrigin: '50% 50%',
                                    background: selected
                                        ? 'linear-gradient(135deg, rgba(74, 158, 255, 0.55), rgba(124, 58, 237, 0.28))'
                                        : 'linear-gradient(135deg, rgba(10, 18, 35, 0.85), rgba(10, 18, 35, 0.55))',
                                    opacity: selected ? 1 : 0.72,
                                    borderRight: `1px solid ${selected ? tokens.accent : tokens.cardBorder}`,
                                }}
                            />
                        );
                    })}
                    <div style={centerDisk}>
                        <div>
                            <div style={metaLabel}>Active Mansion</div>
                            <div style={{ ...metaValue, fontSize: '18px' }}>{name || NAKSHATRAS[activeIndex]}</div>
                            <div style={detailText}>
                                {deity ? `Deity: ${deity}` : 'Vedic lunar mansion overlay'}
                            </div>
                        </div>
                    </div>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: tokens.spacing.sm }}>
                    <Metric label="Ruling Planet" value={rulingPlanet || 'Unknown'} />
                    <Metric label="Quality" value={quality || 'Unknown'} />
                    <Metric label="Segment" value={`${activeIndex + 1} of 27`} />
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
    fontSize: '16px',
    fontWeight: 700,
    color: tokens.textBright,
    marginTop: '4px',
};

const detailText = {
    marginTop: '6px',
    fontSize: '12px',
    color: tokens.textMuted,
    lineHeight: 1.5,
};

const metricCard = {
    padding: '12px 14px',
    borderRadius: tokens.radius.md,
    border: `1px solid ${tokens.cardBorder}`,
    background: 'rgba(10, 18, 35, 0.6)',
};

const centerDisk = {
    position: 'absolute',
    inset: '12%',
    borderRadius: '50%',
    background: 'rgba(5, 8, 16, 0.9)',
    border: `1px solid ${tokens.cardBorder}`,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    textAlign: 'center',
    padding: tokens.spacing.md,
    boxShadow: 'inset 0 0 24px rgba(0,0,0,0.25)',
};
