/**
 * Actor network / universe: overview request params (N + opt-in bulk lists),
 * merging an "expand neighborhood" ego response into the graph, the
 * computed_at freshness label, and the shared controls.
 */
import React from 'react';
import { render, screen, cleanup, fireEvent } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ActorUniverseControls from '../components/ActorUniverseControls.jsx';
import {
    DEFAULT_NODE_COUNT, buildActorNetworkParams, formatComputedAt, mergeEgoIntoGraph,
} from '../utils/actorUniverse.js';

afterEach(() => cleanup());

describe('buildActorNetworkParams', () => {
    it('keeps the default URL (preload cache key) for the default request', () => {
        expect(buildActorNetworkParams()).toEqual({});
        expect(buildActorNetworkParams({ n: DEFAULT_NODE_COUNT, groups: [] })).toEqual({});
    });

    it('sends N and opts bulk groups in on top of market', () => {
        expect(buildActorNetworkParams({ n: 20000, groups: ['pep', 'icij'] }))
            .toEqual({ limit: 20000, groups: 'market,pep,icij' });
    });

    it('drops unknown groups', () => {
        expect(buildActorNetworkParams({ groups: ['bogus'] })).toEqual({});
    });
});

describe('mergeEgoIntoGraph', () => {
    const base = {
        nodes: [{ id: 'a', label: 'A', x: 10, y: 20 }, { id: 'b', label: 'B' }],
        links: [{ source: 'a', target: 'b', strength: 0.5 }],
        metadata: { computed_at: '2026-11-06T03:40:00Z' },
    };

    it('adds new neighbours and edges without duplicating existing ones', () => {
        const ego = {
            nodes: [{ id: 'a', name: 'A' }, { id: 'c', name: 'C', importance: 0.7 }],
            edges: [
                { source: 'a', target: 'c', strength: 0.9, relationship: 'insider_trade' },
                { source: 'b', target: 'a', strength: 0.4 },          // reverse duplicate
                { source: 'a', target: 'zzz', strength: 0.4 },        // unknown endpoint
            ],
        };
        const out = mergeEgoIntoGraph(base, ego, 'a');
        expect(out.nodes.map(n => n.id)).toEqual(['a', 'b', 'c']);
        const c = out.nodes.find(n => n.id === 'c');
        expect(c.label).toBe('C');
        expect(c.influence).toBe(0.7);
        expect(c.expanded).toBe(true);
        expect(Number.isFinite(c.x)).toBe(true);
        expect(out.links).toHaveLength(2);
        expect(out.lastExpansionAdded).toBe(1);
        expect(out.metadata).toBe(base.metadata);
        expect(base.nodes).toHaveLength(2); // input untouched
    });

    it('returns the graph unchanged for an empty or failed expansion', () => {
        expect(mergeEgoIntoGraph(base, null, 'a')).toBe(base);
        expect(mergeEgoIntoGraph(base, { error: 'x' }, 'a')).toBe(base);
    });
});

describe('formatComputedAt', () => {
    const now = Date.parse('2026-11-06T12:00:00Z');
    it('formats minutes, hours and days', () => {
        expect(formatComputedAt('2026-11-06T11:45:00Z', now)).toBe('computed 15m ago');
        expect(formatComputedAt('2026-11-06T03:40:00Z', now)).toBe('computed 8h ago');
        expect(formatComputedAt('2026-11-02T12:00:00Z', now)).toBe('computed 4d ago');
    });
    it('is null when unknown', () => {
        expect(formatComputedAt(null, now)).toBeNull();
        expect(formatComputedAt('not a date', now)).toBeNull();
    });
});

describe('ActorUniverseControls', () => {
    it('changes N, toggles bulk lists and shows freshness', () => {
        const onNodeCount = vi.fn();
        const onGroups = vi.fn();
        render(
            <ActorUniverseControls
                nodeCount={2000}
                onNodeCount={onNodeCount}
                groups={['pep']}
                onGroups={onGroups}
                metadata={{ computed_at: new Date(Date.now() - 2 * 3600e3).toISOString(), total_actors: 3493149 }}
            />,
        );
        fireEvent.change(screen.getByLabelText('Number of actors'), { target: { value: '20000' } });
        expect(onNodeCount).toHaveBeenCalledWith(20000);
        fireEvent.click(screen.getByText('ICIJ'));
        expect(onGroups).toHaveBeenCalledWith(['pep', 'icij']);
        fireEvent.click(screen.getByText('PEP'));
        expect(onGroups).toHaveBeenCalledWith([]);
        expect(screen.getByText(/computed 2h ago/)).toBeTruthy();
        expect(screen.getByText(/3,493,149 actors/)).toBeTruthy();
    });
});
