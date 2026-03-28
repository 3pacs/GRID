import React from 'react';
import { tokens } from '../styles/tokens.js';

const TYPE_COLORS = {
    retrograde: tokens.red,
    eclipse: tokens.purple,
    conjunction: tokens.gold,
    ingress: tokens.accent,
    solar: tokens.gold,
    lunar: tokens.textBright,
    default: tokens.textMuted,
};

export default function CelestialTimeline({
    events = [],
    title = 'Celestial Timeline',
    subtitle = 'Upcoming events and state changes',
    compact = false,
}) {
    const safeEvents = events.slice(0, 30);

    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>Timeline</div>
                    <div style={headline}>{title}</div>
                </div>
                <div style={subhead}>{subtitle}</div>
            </div>

            <div style={{
                display: 'flex',
                gap: tokens.spacing.md,
                overflowX: 'auto',
                paddingBottom: '4px',
            }}>
                {safeEvents.length > 0 ? safeEvents.map((event, index) => {
                    const color = TYPE_COLORS[event.type] || TYPE_COLORS.default;
                    return (
                        <div
                            key={`${event.date || event.datetime || index}-${index}`}
                            style={{
                                minWidth: compact ? '160px' : '210px',
                                flex: '0 0 auto',
                                padding: '12px',
                                borderRadius: tokens.radius.md,
                                background: 'rgba(10, 18, 35, 0.72)',
                                border: `1px solid ${tokens.cardBorder}`,
                                position: 'relative',
                                overflow: 'hidden',
                            }}
                        >
                            <div style={{
                                position: 'absolute',
                                top: 0,
                                left: 0,
                                right: 0,
                                height: '3px',
                                background: color,
                            }} />
                            <div style={typeLabel(color)}>{event.type || 'event'}</div>
                            <div style={eventName}>
                                {event.name || event.event || event.label || 'Celestial Event'}
                            </div>
                            <div style={eventDate}>
                                {event.date || event.datetime || 'TBD'}
                            </div>
                            {(event.description || event.detail) && (
                                <div style={eventDetail}>
                                    {event.description || event.detail}
                                </div>
                            )}
                        </div>
                    );
                }) : (
                    <div style={emptyState}>
                        No celestial events to display yet.
                    </div>
                )}
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

const subhead = {
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    alignSelf: 'end',
};

const typeLabel = (color) => ({
    fontSize: '10px',
    color,
    fontFamily: tokens.fontMono,
    letterSpacing: '1px',
    textTransform: 'uppercase',
});

const eventName = {
    marginTop: '6px',
    fontSize: '14px',
    fontWeight: 700,
    color: tokens.textBright,
    lineHeight: 1.4,
};

const eventDate = {
    marginTop: '8px',
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
};

const eventDetail = {
    marginTop: '8px',
    fontSize: '12px',
    color: tokens.text,
    lineHeight: 1.6,
};

const emptyState = {
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    fontSize: '13px',
    padding: '20px 4px',
};
