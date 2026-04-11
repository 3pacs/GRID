/**
 * TemporalScrubber -- Timeline slider for canvas temporal filtering.
 * Full width, compact 32px height.
 * Range buttons (7d/30d/90d/365d), draggable slider, play/pause/speed.
 */
import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Play, Pause } from 'lucide-react';
import { colors, tokens } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const RANGES = [
    { key: '7d', label: '7d', days: 7 },
    { key: '30d', label: '30d', days: 30 },
    { key: '90d', label: '90d', days: 90 },
    { key: '365d', label: '1y', days: 365 },
];

const SPEEDS = [1, 5, 10];

/* ── Styles ──────────────────────────────────────────────────── */

const S = {
    container: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        height: '32px',
        padding: '0 12px',
        width: '100%',
        boxSizing: 'border-box',
    },
    rangeGroup: {
        display: 'flex',
        gap: '2px',
        flexShrink: 0,
    },
    rangePill: (active) => ({
        padding: '2px 8px',
        borderRadius: tokens.radius.pill,
        fontSize: '10px',
        fontWeight: 600,
        fontFamily: MONO,
        cursor: 'pointer',
        border: 'none',
        background: active ? colors.accent : 'transparent',
        color: active ? '#fff' : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
        lineHeight: '20px',
    }),
    sliderWrap: {
        flex: 1,
        position: 'relative',
        height: '32px',
        display: 'flex',
        alignItems: 'center',
        cursor: 'pointer',
        userSelect: 'none',
    },
    track: {
        width: '100%',
        height: '2px',
        background: colors.border,
        borderRadius: '1px',
        position: 'relative',
    },
    fill: (pct) => ({
        position: 'absolute',
        left: 0,
        top: 0,
        width: `${pct}%`,
        height: '2px',
        background: colors.accent,
        borderRadius: '1px',
        pointerEvents: 'none',
    }),
    handle: (pct) => ({
        position: 'absolute',
        left: `${pct}%`,
        top: '50%',
        transform: 'translate(-50%, -50%)',
        width: '14px',
        height: '14px',
        borderRadius: '50%',
        background: colors.accent,
        border: `2px solid ${colors.card}`,
        boxShadow: '0 1px 6px rgba(26,110,191,0.4)',
        cursor: 'grab',
        zIndex: 2,
    }),
    tickContainer: {
        position: 'absolute',
        width: '100%',
        height: '2px',
        top: 0,
        left: 0,
        pointerEvents: 'none',
    },
    tick: (pct) => ({
        position: 'absolute',
        left: `${pct}%`,
        top: '-3px',
        width: '1px',
        height: '8px',
        background: `${colors.textMuted}40`,
    }),
    tooltip: (pct) => ({
        position: 'absolute',
        left: `${pct}%`,
        top: '-24px',
        transform: 'translateX(-50%)',
        padding: '2px 6px',
        borderRadius: '4px',
        background: colors.cardElevated,
        border: `1px solid ${colors.border}`,
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textDim,
        whiteSpace: 'nowrap',
        pointerEvents: 'none',
        zIndex: 3,
    }),
    dateLabel: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textDim,
        whiteSpace: 'nowrap',
        flexShrink: 0,
    },
    playBtn: {
        width: '24px',
        height: '24px',
        borderRadius: '50%',
        border: `1px solid ${colors.border}`,
        background: 'transparent',
        color: colors.textDim,
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0,
        padding: 0,
        transition: `all ${tokens.transition.fast}`,
    },
    speedBtn: {
        fontSize: '10px',
        fontFamily: MONO,
        color: colors.textMuted,
        background: 'transparent',
        border: 'none',
        cursor: 'pointer',
        padding: '2px 4px',
        flexShrink: 0,
        transition: `color ${tokens.transition.fast}`,
    },
};

/* ── Helpers ──────────────────────────────────────────────────── */

function formatDate(d) {
    if (!d) return '--';
    const date = d instanceof Date ? d : new Date(d);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' });
}

function addDays(date, days) {
    const d = new Date(date);
    d.setDate(d.getDate() + days);
    return d;
}

function daysBetween(a, b) {
    return Math.round((b - a) / 86400000);
}

function getMonthTicks(start, end) {
    const ticks = [];
    const totalDays = daysBetween(start, end);
    if (totalDays <= 0) return ticks;

    const current = new Date(start);
    current.setDate(1);
    current.setMonth(current.getMonth() + 1);

    while (current < end) {
        const pct = (daysBetween(start, current) / totalDays) * 100;
        if (pct > 0 && pct < 100) {
            ticks.push(pct);
        }
        current.setMonth(current.getMonth() + 1);
    }
    return ticks;
}

/* ── Component ───────────────────────────────────────────────── */

export default function TemporalScrubber({ timeRange, onTimeRangeChange }) {
    const [activeRange, setActiveRange] = useState('90d');
    const [playing, setPlaying] = useState(false);
    const [speedIdx, setSpeedIdx] = useState(0);
    const [dragging, setDragging] = useState(false);
    const [hoverPct, setHoverPct] = useState(null);
    const trackRef = useRef(null);
    const playRef = useRef(null);

    // Derive date range
    const rangeDays = RANGES.find(r => r.key === activeRange)?.days || 90;
    const endDate = new Date();
    const startDate = addDays(endDate, -rangeDays);

    // Current position from timeRange prop
    // Supports both { currentDate } and { start, end } from CanvasStore
    const currentDate = timeRange?.currentDate
        ? new Date(timeRange.currentDate)
        : timeRange?.end
            ? new Date(timeRange.end)
            : endDate;
    const totalDays = daysBetween(startDate, endDate);
    const currentPct = totalDays > 0
        ? Math.max(0, Math.min(100, (daysBetween(startDate, currentDate) / totalDays) * 100))
        : 100;

    const monthTicks = getMonthTicks(startDate, endDate);

    // Convert % to date
    const pctToDate = useCallback((pct) => {
        const days = (pct / 100) * totalDays;
        return addDays(startDate, Math.round(days));
    }, [startDate, totalDays]);

    // Emit change
    const emitChange = useCallback((date) => {
        onTimeRangeChange?.({
            ...timeRange,
            currentDate: date.toISOString(),
            startDate: startDate.toISOString(),
            endDate: endDate.toISOString(),
            rangeDays,
        });
    }, [onTimeRangeChange, timeRange, startDate, endDate, rangeDays]);

    // Mouse drag handling
    const getTrackPct = useCallback((clientX) => {
        if (!trackRef.current) return 0;
        const rect = trackRef.current.getBoundingClientRect();
        return Math.max(0, Math.min(100, ((clientX - rect.left) / rect.width) * 100));
    }, []);

    const handleMouseDown = useCallback((e) => {
        e.preventDefault();
        setDragging(true);
        const pct = getTrackPct(e.clientX);
        emitChange(pctToDate(pct));
    }, [getTrackPct, pctToDate, emitChange]);

    useEffect(() => {
        if (!dragging) return;
        const handleMouseMove = (e) => {
            const pct = getTrackPct(e.clientX);
            emitChange(pctToDate(pct));
        };
        const handleMouseUp = () => setDragging(false);
        window.addEventListener('mousemove', handleMouseMove);
        window.addEventListener('mouseup', handleMouseUp);
        return () => {
            window.removeEventListener('mousemove', handleMouseMove);
            window.removeEventListener('mouseup', handleMouseUp);
        };
    }, [dragging, getTrackPct, pctToDate, emitChange]);

    // Hover tooltip
    const handleHover = useCallback((e) => {
        if (dragging) return;
        const pct = getTrackPct(e.clientX);
        setHoverPct(pct);
    }, [dragging, getTrackPct]);

    // Playback
    useEffect(() => {
        if (!playing) return;
        const speed = SPEEDS[speedIdx] || 1;
        const intervalMs = 200; // tick every 200ms
        const timer = setInterval(() => {
            const next = addDays(currentDate, speed);
            if (next >= endDate) {
                setPlaying(false);
                emitChange(endDate);
            } else {
                emitChange(next);
            }
        }, intervalMs);
        playRef.current = timer;
        return () => clearInterval(timer);
    }, [playing, speedIdx, currentDate, endDate, emitChange]);

    // Range change
    const handleRangeChange = useCallback((rangeKey) => {
        setActiveRange(rangeKey);
        setPlaying(false);
        const days = RANGES.find(r => r.key === rangeKey)?.days || 90;
        const newStart = addDays(new Date(), -days);
        onTimeRangeChange?.({
            ...timeRange,
            currentDate: new Date().toISOString(),
            startDate: newStart.toISOString(),
            endDate: new Date().toISOString(),
            rangeDays: days,
        });
    }, [onTimeRangeChange, timeRange]);

    const togglePlay = useCallback(() => {
        if (!playing && currentPct >= 99) {
            // Reset to start before playing
            emitChange(startDate);
        }
        setPlaying(!playing);
    }, [playing, currentPct, startDate, emitChange]);

    const cycleSpeed = useCallback(() => {
        setSpeedIdx((speedIdx + 1) % SPEEDS.length);
    }, [speedIdx]);

    return (
        <div style={S.container}>
            {/* Range buttons */}
            <div style={S.rangeGroup}>
                {RANGES.map(r => (
                    <button
                        key={r.key}
                        style={S.rangePill(activeRange === r.key)}
                        onClick={() => handleRangeChange(r.key)}
                        onMouseEnter={(e) => {
                            if (activeRange !== r.key) e.currentTarget.style.color = colors.text;
                        }}
                        onMouseLeave={(e) => {
                            if (activeRange !== r.key) e.currentTarget.style.color = colors.textMuted;
                        }}
                    >
                        {r.label}
                    </button>
                ))}
            </div>

            {/* Slider */}
            <div
                ref={trackRef}
                style={S.sliderWrap}
                onMouseDown={handleMouseDown}
                onMouseMove={handleHover}
                onMouseLeave={() => setHoverPct(null)}
            >
                <div style={S.track}>
                    <div style={S.fill(currentPct)} />
                    {/* Month ticks */}
                    <div style={S.tickContainer}>
                        {monthTicks.map((pct, i) => (
                            <div key={i} style={S.tick(pct)} />
                        ))}
                    </div>
                </div>
                {/* Handle */}
                <div style={S.handle(currentPct)} />
                {/* Hover tooltip */}
                {hoverPct != null && !dragging && (
                    <div style={S.tooltip(hoverPct)}>
                        {formatDate(pctToDate(hoverPct))}
                    </div>
                )}
            </div>

            {/* Date label */}
            <span style={S.dateLabel}>{formatDate(currentDate)}</span>

            {/* Play/Pause */}
            <button
                style={S.playBtn}
                onClick={togglePlay}
                onMouseEnter={(e) => {
                    e.currentTarget.style.borderColor = colors.accent;
                    e.currentTarget.style.color = colors.text;
                }}
                onMouseLeave={(e) => {
                    e.currentTarget.style.borderColor = colors.border;
                    e.currentTarget.style.color = colors.textDim;
                }}
                title={playing ? 'Pause' : 'Play'}
            >
                {playing
                    ? <Pause size={12} />
                    : <Play size={12} style={{ marginLeft: '1px' }} />
                }
            </button>

            {/* Speed */}
            <button
                style={S.speedBtn}
                onClick={cycleSpeed}
                onMouseEnter={(e) => { e.currentTarget.style.color = colors.text; }}
                onMouseLeave={(e) => { e.currentTarget.style.color = colors.textMuted; }}
                title="Cycle playback speed"
            >
                {SPEEDS[speedIdx]}x
            </button>
        </div>
    );
}
