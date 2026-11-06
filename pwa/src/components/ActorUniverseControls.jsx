// Shared controls for the actor-network / actor-universe views: how many
// actors to request (N), which bulk groups to opt into, and the freshness
// of the precomputed graph. Visual style follows the existing filter bars.
import React from 'react';
import { colors } from '../styles/shared.js';
import { BULK_GROUPS, NODE_COUNT_OPTIONS, formatComputedAt } from '../utils/actorUniverse.js';

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const btn = (active) => ({
    background: active ? `${colors.accent}22` : 'transparent',
    border: `1px solid ${active ? colors.accent : colors.border}`,
    color: active ? colors.accent : colors.textMuted,
    borderRadius: '4px',
    padding: '2px 6px',
    fontSize: '10px',
    fontFamily: MONO,
    cursor: 'pointer',
});

export default function ActorUniverseControls({ nodeCount, onNodeCount, groups, onGroups, metadata }) {
    const fresh = formatComputedAt(metadata?.computed_at);
    const toggle = (id) => {
        const next = groups.includes(id) ? groups.filter(g => g !== id) : [...groups, id];
        onGroups(next);
    };
    return (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, letterSpacing: '1px' }}>ACTORS</span>
            <select
                aria-label="Number of actors"
                value={nodeCount}
                onChange={e => onNodeCount(Number(e.target.value))}
                style={{ ...btn(false), padding: '2px 4px' }}
            >
                {NODE_COUNT_OPTIONS.map(n => <option key={n} value={n}>{n.toLocaleString()}</option>)}
            </select>
            <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, letterSpacing: '1px' }}>+ LISTS</span>
            {BULK_GROUPS.map(g => (
                <button key={g.id} type="button" onClick={() => toggle(g.id)} style={btn(groups.includes(g.id))}
                    aria-pressed={groups.includes(g.id)}>
                    {g.label}
                </button>
            ))}
            {fresh && (
                <span title={metadata?.graph_version || ''}
                    style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, whiteSpace: 'nowrap' }}>
                    {fresh}
                    {metadata?.total_actors ? ` · ${Number(metadata.total_actors).toLocaleString()} actors` : ''}
                </span>
            )}
        </div>
    );
}
