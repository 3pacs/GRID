import React from 'react';
import { colors } from '../../styles/shared.js';

const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

/**
 * Parse a GRID /chat/ask answer into its mandated sections
 * (VERDICT / WHY / CONFLICTS / ACTION CALLS / BREAKING) so the front end can
 * give the plain-English verdict real visual hierarchy instead of burying it.
 *
 * Returns { verdict, why[], conflicts, actions[], breaking, rest[] }. Any
 * section may be empty; unlabelled prose lands in `rest` so nothing is lost.
 */
export function parseAnswer(text) {
    const out = { verdict: '', why: [], conflicts: '', actions: [], breaking: '', rest: [] };
    if (!text || typeof text !== 'string') return out;

    const lines = text.split('\n');
    let section = null; // verdict | why | conflicts | actions | breaking

    const labelMatch = (line) => {
        const m = line.match(/^\s*(?:\d+[.)]\s*)?\**\s*([A-Za-z][A-Za-z ]{2,20}?)\s*\**\s*:\s*(.*)$/);
        if (!m) return null;
        const key = m[1].trim().toLowerCase();
        const map = {
            verdict: 'verdict', 'bottom line': 'verdict', call: 'verdict',
            why: 'why', reasons: 'why', because: 'why',
            conflict: 'conflicts', conflicts: 'conflicts', disagreement: 'conflicts',
            action: 'actions', 'action calls': 'actions', 'what to do': 'actions',
            'action call': 'actions', moves: 'actions',
            breaking: 'breaking', 'breaking events': 'breaking',
        };
        return map[key] ? { sec: map[key], rest: m[2].trim() } : null;
    };

    const stripBullet = (s) => s.replace(/^\s*[-*•]\s*/, '').trim();

    for (const raw of lines) {
        const line = raw.replace(/\s+$/, '');
        if (!line.trim()) continue;

        const lbl = labelMatch(line);
        if (lbl) {
            section = lbl.sec;
            const val = lbl.rest;
            if (!val) continue;
            if (section === 'verdict') out.verdict = out.verdict || val;
            else if (section === 'conflicts') out.conflicts = out.conflicts ? `${out.conflicts} ${val}` : val;
            else if (section === 'breaking') out.breaking = out.breaking ? `${out.breaking} ${val}` : val;
            else if (section === 'why') out.why.push(stripBullet(val));
            else if (section === 'actions') out.actions.push(stripBullet(val));
            continue;
        }

        const isBullet = /^\s*[-*•]/.test(line);
        if (section === 'why') out.why.push(stripBullet(line));
        else if (section === 'actions') out.actions.push(stripBullet(line));
        else if (section === 'conflicts') out.conflicts = `${out.conflicts} ${line.trim()}`.trim();
        else if (section === 'breaking') out.breaking = `${out.breaking} ${line.trim()}`.trim();
        else if (section === 'verdict' && !out.verdict) out.verdict = line.trim();
        else if (isBullet) out.why.push(stripBullet(line));
        else out.rest.push(line.trim());
    }
    return out;
}

/** Render a parsed answer with the verdict as a prominent headline. */
export function AnswerView({ text }) {
    const a = parseAnswer(text);
    const headline = a.verdict || a.rest[0] || text || '';
    const restAfterHeadline = a.verdict ? a.rest : a.rest.slice(1);

    return (
        <div style={S.wrap}>
            {a.breaking && (
                <div style={S.breaking}>⚡ {a.breaking}</div>
            )}
            {headline && <div style={S.headline}>{headline}</div>}

            {a.why.length > 0 && (
                <ul style={S.list}>
                    {a.why.map((w, i) => <li key={i} style={S.li}>{w}</li>)}
                </ul>
            )}

            {restAfterHeadline.length > 0 && (
                <div style={S.body}>
                    {restAfterHeadline.map((p, i) => <p key={i} style={S.p}>{p}</p>)}
                </div>
            )}

            {a.conflicts && (
                <div style={S.conflicts}><span style={S.conflictsTag}>But</span> {a.conflicts}</div>
            )}

            {a.actions.length > 0 && (
                <div style={S.actions}>
                    <div style={S.actionsTitle}>What to do</div>
                    <ul style={S.list}>
                        {a.actions.map((act, i) => <li key={i} style={S.liAction}>{act}</li>)}
                    </ul>
                </div>
            )}
        </div>
    );
}

const S = {
    wrap: { display: 'flex', flexDirection: 'column', gap: '14px', fontFamily: SANS },
    breaking: {
        background: colors.redBg, color: colors.red, fontWeight: 700,
        padding: '8px 12px', borderRadius: '8px', fontSize: '14px',
    },
    headline: {
        fontSize: '20px', fontWeight: 700, lineHeight: 1.35, color: colors.text,
        letterSpacing: '-0.01em',
    },
    list: { margin: 0, paddingLeft: '20px', display: 'flex', flexDirection: 'column', gap: '7px' },
    li: { fontSize: '15px', lineHeight: 1.5, color: colors.textDim },
    liAction: { fontSize: '15px', lineHeight: 1.5, color: colors.text, fontWeight: 500 },
    body: { display: 'flex', flexDirection: 'column', gap: '8px' },
    p: { margin: 0, fontSize: '15px', lineHeight: 1.6, color: colors.textDim },
    conflicts: {
        fontSize: '14px', lineHeight: 1.5, color: colors.textDim,
        borderLeft: `3px solid ${colors.yellow}`, paddingLeft: '12px',
    },
    conflictsTag: { color: colors.yellow, fontWeight: 700, marginRight: '4px' },
    actions: {
        background: colors.greenBg, borderRadius: '10px', padding: '12px 14px',
        border: `1px solid ${colors.green}33`,
    },
    actionsTitle: {
        fontSize: '11px', fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.08em', color: colors.green, marginBottom: '8px',
    },
};
