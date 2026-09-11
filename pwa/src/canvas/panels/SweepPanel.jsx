/**
 * SweepPanel — the latest persisted universe sweep, as a ranked list.
 *
 * Shown in the detail-panel slot when the 'verdicts' layer is on and no node
 * is selected. Rows come straight from universe_ranking_history.top_k (the
 * Sunday 05:00 long-horizon job in intelligence/scheduler.py); nothing here
 * runs the decision stack. Clicking a row searches the canvas for that ticker.
 */
import React from 'react';
import { colors, tokens, glassMorphism } from '../../styles/shared.js';
import { VERDICT_COLORS, normalizeVerdict } from '../CanvasStore.js';

const MONO = "'JetBrains Mono', monospace";

export function formatSweepMeta(sweep) {
    if (!sweep) return { title: 'No sweep yet', subtitle: '', generated: '' };
    const horizon = sweep.horizon_days ? `${sweep.horizon_days}d` : '';
    const universe = sweep.universe_name || '';
    const generated = sweep.generated_at ? String(sweep.generated_at).slice(0, 10) : '';
    const scored = `${sweep.tickers_succeeded ?? 0}/${sweep.tickers_attempted ?? 0} scored`;
    return {
        title: [horizon, universe].filter(Boolean).join(' · ') || 'Sweep',
        subtitle: [sweep.regime_signature, scored].filter(Boolean).join(' · '),
        generated,
    };
}

const S = {
    overlay: {
        position: 'absolute',
        top: 0, right: 0, bottom: 0,
        width: '360px',
        background: colors.card,
        borderLeft: `1px solid ${colors.border}`,
        zIndex: 90,
        display: 'flex',
        flexDirection: 'column',
        boxShadow: '-4px 0 24px rgba(0,0,0,0.4)',
        overflow: 'hidden',
    },
    header: {
        ...glassMorphism,
        padding: '14px 16px',
        borderBottom: `1px solid ${colors.border}`,
        flexShrink: 0,
    },
    eyebrow: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: '#F97316',
        fontFamily: MONO,
        textTransform: 'uppercase',
    },
    title: { fontSize: '15px', fontWeight: 700, color: colors.text, marginTop: '4px' },
    subtitle: { fontSize: '11px', color: colors.textMuted, fontFamily: MONO, marginTop: '2px' },
    close: {
        position: 'absolute', top: '10px', right: '10px',
        background: 'transparent', border: 'none', color: colors.textMuted,
        cursor: 'pointer', fontSize: '16px', lineHeight: 1,
    },
    list: { flex: 1, overflowY: 'auto', padding: '6px 0' },
    row: (active) => ({
        display: 'grid',
        gridTemplateColumns: '28px 1fr auto auto',
        alignItems: 'center',
        gap: '8px',
        padding: '7px 16px',
        cursor: 'pointer',
        background: active ? `${colors.primary}22` : 'transparent',
        borderLeft: active ? `2px solid ${colors.primary}` : '2px solid transparent',
    }),
    rank: { fontSize: '10px', color: colors.textMuted, fontFamily: MONO },
    ticker: { fontSize: '13px', fontWeight: 700, color: colors.text, fontFamily: MONO },
    sector: { fontSize: '10px', color: colors.textMuted, marginLeft: '6px' },
    chip: (verdict) => ({
        fontSize: '9px',
        fontWeight: 700,
        letterSpacing: '0.8px',
        fontFamily: MONO,
        padding: '2px 6px',
        borderRadius: tokens.radius.pill,
        background: `${VERDICT_COLORS[verdict]}33`,
        color: VERDICT_COLORS[verdict],
        textTransform: 'uppercase',
    }),
    score: { fontSize: '11px', color: colors.text, fontFamily: MONO, minWidth: '34px', textAlign: 'right' },
    narrative: {
        padding: '10px 16px',
        borderTop: `1px solid ${colors.border}`,
        fontSize: '11px',
        color: colors.textMuted,
        lineHeight: 1.45,
        flexShrink: 0,
        maxHeight: '96px',
        overflowY: 'auto',
    },
    empty: { padding: '24px 16px', fontSize: '12px', color: colors.textMuted, lineHeight: 1.5 },
};

export default function SweepPanel({ sweep, onPick, onClose, activeTicker }) {
    const meta = formatSweepMeta(sweep);
    const rows = Array.isArray(sweep?.top_k) ? sweep.top_k : [];
    const active = String(activeTicker || '').toUpperCase();

    return (
        <div style={S.overlay} data-testid="sweep-panel">
            <div style={S.header}>
                <div style={S.eyebrow}>Sweep verdicts</div>
                <div style={S.title}>{meta.title}</div>
                <div style={S.subtitle}>
                    {meta.subtitle}{meta.generated ? ` · ${meta.generated}` : ''}
                </div>
                {onClose && (
                    <button style={S.close} onClick={onClose} aria-label="Close sweep panel">×</button>
                )}
            </div>

            {rows.length === 0 ? (
                <div style={S.empty}>
                    No persisted sweep at this horizon yet. The long-horizon job runs
                    Sunday 05:00 and writes here; run it by hand with
                    {' '}<code>rank_universe(..., horizon_days=90)</code> to fill this.
                </div>
            ) : (
                <div style={S.list}>
                    {rows.map((row, i) => {
                        const ticker = String(row.ticker || '').toUpperCase();
                        const verdict = normalizeVerdict(row.error ? 'error' : row.verdict);
                        return (
                            <div
                                key={`${ticker}-${i}`}
                                style={S.row(ticker === active)}
                                onClick={() => onPick?.(ticker)}
                                data-testid={`sweep-row-${ticker}`}
                                title={row.error || `${ticker}: ${verdict}, composite ${Number(row.composite_score ?? 0).toFixed(2)}`}
                            >
                                <span style={S.rank}>{i + 1}</span>
                                <span>
                                    <span style={S.ticker}>{ticker}</span>
                                    {row.sector && <span style={S.sector}>{row.sector}</span>}
                                </span>
                                <span style={S.chip(verdict)}>{verdict.replace('_', ' ')}</span>
                                <span style={S.score}>{Number(row.composite_score ?? 0).toFixed(2)}</span>
                            </div>
                        );
                    })}
                </div>
            )}

            {sweep?.narrative && <div style={S.narrative}>{sweep.narrative}</div>}
        </div>
    );
}
