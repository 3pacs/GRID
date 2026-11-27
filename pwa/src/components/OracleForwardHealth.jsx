import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

/**
 * Did the Oracle forward pass produce a usable forecast this session?
 *
 * From 2026-11-23 to 2026-11-25 every candidate abstained at creation and this
 * page showed nothing about it. The line below is read from
 * /api/v1/oracle/forward-health (oracle/forward_health.py): green when every
 * cohort ticker has a forecast, yellow when one does not or when the next pass
 * is minutes away (an admin response says so), red when the session has none. If the status itself cannot be read, it says so instead of
 * showing nothing.
 */
const LEVEL_COLOR = {
    ok: colors.green, degraded: colors.yellow, retrying: colors.yellow, failing: colors.red,
    pending: colors.textMuted, info: colors.textMuted,
};
const LEVEL_LABEL = {
    ok: 'FORWARD PASS OK', degraded: 'FORWARD PASS DEGRADED', failing: 'NO USABLE FORECAST',
    retrying: 'WAITING FOR A QUALIFIED QUOTE',
    pending: 'FORWARD PASS PENDING', info: 'FORWARD PASS',
};

export default function OracleForwardHealth() {
    const [health, setHealth] = useState(null);
    const [unavailable, setUnavailable] = useState(false);

    useEffect(() => {
        let live = true;
        (async () => {
            try {
                const res = await api.getOracleForwardHealth();
                if (!live) return;
                if (!res || res.error || !res.status || !res.message) setUnavailable(true);
                else setHealth(res);
            } catch {
                if (live) setUnavailable(true);
            }
        })();
        return () => { live = false; };
    }, []);

    const box = (color) => ({
        border: `1px solid ${color}55`, borderLeft: `3px solid ${color}`, borderRadius: '6px',
        padding: '8px 10px', marginBottom: '12px', fontSize: '11px', lineHeight: 1.45,
        color: colors.text, background: `${color}10`,
    });

    if (unavailable) {
        return (
            <div data-testid="forward-health-unavailable" style={box(colors.textMuted)}>
                Forward-pass status unavailable. This page cannot say whether today&apos;s pass produced a forecast.
            </div>
        );
    }
    if (!health) return null;

    const color = LEVEL_COLOR[health.level] || colors.textMuted;
    const failing = health.level === 'failing';
    const tickers = Object.entries(health.tickers || {});
    const notes = Object.entries(health.operator_notes || {});
    return (
        <div data-testid="forward-health" data-level={health.level} role={failing ? 'alert' : 'status'} style={box(color)}>
            <div style={{ fontWeight: 700, letterSpacing: '0.5px', fontSize: '10px', color, marginBottom: '3px' }}>
                {LEVEL_LABEL[health.level] || 'FORWARD PASS'}
            </div>
            <div>{health.message}</div>
            {health.level !== 'ok' && tickers.length > 0 && (
                <div style={{ marginTop: '4px', color: colors.textMuted }}>
                    {tickers.map(([name, row]) => (
                        <div key={name}>
                            {name}: {row.qualified} forecast(s), {row.abstained} abstention(s)
                            {row.top_reason ? ` (${row.top_reason})` : ''}
                        </div>
                    ))}
                </div>
            )}
            {notes.map(([name, note]) => (
                <div key={name} style={{ marginTop: '4px', color: colors.textMuted }}>{name}: {note}</div>
            ))}
        </div>
    );
}
