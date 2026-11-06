/**
 * FreshnessIndicator renders the states docs/reference/AVAILABILITY_CONTRACT.md
 * defines. Regressions covered (issue #959 M1 reconciliation):
 *   - `confirmed` was labelled "LIVE" (a literal the contract lists as a defect);
 *   - a null / unknown confidence fell through to "EST", so an unknown read as
 *     an estimate.
 */
import React from 'react';
import fs from 'node:fs';
import path from 'node:path';
import { render, screen, cleanup, fireEvent } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import FreshnessIndicator, {
    describeFreshness, confidenceOpacity, formatAsOf,
} from '../components/flows/FreshnessIndicator.jsx';
import JunctionCard from '../components/flows/JunctionCard.jsx';
import FlowTooltip from '../components/flows/FlowTooltip.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getJunctionPoints: vi.fn(),
        getFlowWaterfall: vi.fn(),
    },
}));

afterEach(() => {
    cleanup();
    vi.clearAllMocks();
});

function badge(container) {
    return container.querySelector('[data-freshness]');
}

describe('describeFreshness', () => {
    it('maps confirmed to OBSERVED with the data as-of, never LIVE', () => {
        const d = describeFreshness({ confidence: 'confirmed', asOf: '2026-10-16T21:05:11Z' });
        expect(d.state).toBe('observed');
        expect(d.label).toBe('OBSERVED 2026-10-16');
        expect(d.title).toBe('Observed; as of 2026-10-16');
        expect(d.label).not.toMatch(/LIVE/);
    });

    it('says the as-of is not reported when a confirmed row has none', () => {
        const d = describeFreshness({ confidence: 'confirmed' });
        expect(d.label).toBe('OBSERVED');
        expect(d.title).toMatch(/as-of date not reported/);
    });

    it.each([null, undefined, '', '   '])('renders %p confidence as UNKNOWN, not an estimate', (c) => {
        const d = describeFreshness({ confidence: c });
        expect(d.state).toBe('unknown');
        expect(d.label).toBe('UNKNOWN');
        expect(d.title).toBe('Confidence unknown');
    });

    it('renders an unrecognised confidence as UNKNOWN and names the value', () => {
        const d = describeFreshness({ confidence: 'hard_data' });
        expect(d.state).toBe('unknown');
        expect(d.title).toContain('"hard_data"');
    });

    it('renders unavailable with its reason, overriding any confidence', () => {
        const d = describeFreshness({ confidence: 'confirmed', unavailable: true, reason: 'no WALCL rows' });
        expect(d.state).toBe('unavailable');
        expect(d.label).toBe('UNAVAILABLE');
        expect(d.title).toBe('Unavailable: no WALCL rows');
        expect(describeFreshness({ confidence: 'unavailable' }).title).toBe('Unavailable: no reason reported');
    });

    it('keeps estimated, derived, stale and rumored distinct', () => {
        expect(describeFreshness({ confidence: 'estimated' }).label).toBe('ESTIMATED');
        expect(describeFreshness({ confidence: 'DERIVED' }).label).toBe('DERIVED');
        expect(describeFreshness({ confidence: 'stale', asOf: '2026-10-13' }).title).toBe('Stale (as of 2026-10-13)');
        expect(describeFreshness({ confidence: 'confirmed', stale: true, reason: 'stale' }).state).toBe('stale');
        expect(describeFreshness({ confidence: 'rumored' }).label).toBe('RUMOR');
    });

    it('formats as-of values', () => {
        expect(formatAsOf(null)).toBeNull();
        expect(formatAsOf('')).toBeNull();
        expect(formatAsOf('2026-10-16')).toBe('2026-10-16');
        expect(formatAsOf('week 41')).toBe('week 41');
    });

    it('draws unknown fainter than an estimate', () => {
        expect(confidenceOpacity('confirmed')).toBe(1);
        expect(confidenceOpacity(null)).toBeLessThan(confidenceOpacity('estimated'));
        expect(confidenceOpacity('mystery')).toBeLessThan(confidenceOpacity('estimated'));
    });
});

describe('FreshnessIndicator rendering', () => {
    it('renders UNKNOWN for a missing confidence (badge and compact dot)', () => {
        const { container, rerender } = render(<FreshnessIndicator confidence={null} />);
        expect(screen.getByText('UNKNOWN')).toBeInTheDocument();
        expect(screen.queryByText(/EST/)).not.toBeInTheDocument();
        expect(badge(container)).toHaveAttribute('data-freshness', 'unknown');

        rerender(<FreshnessIndicator compact />);
        expect(badge(container)).toHaveAttribute('aria-label', 'UNKNOWN. Confidence unknown');
    });

    it('renders unavailable with the reason in its tooltip', () => {
        const { container } = render(<FreshnessIndicator unavailable reason="probe timeout" />);
        expect(screen.getByText('UNAVAILABLE')).toBeInTheDocument();
        expect(badge(container)).toHaveAttribute('title', 'Unavailable: probe timeout');
    });

    it('never renders the literal LIVE for any known state', () => {
        for (const c of ['confirmed', 'derived', 'estimated', 'stale', 'rumored', 'unavailable', null]) {
            const { container } = render(<FreshnessIndicator confidence={c} asOf="2026-10-16" />);
            expect(container.textContent).not.toMatch(/LIVE/);
            cleanup();
        }
    });

    it('source no longer contains the LIVE / EST labels', () => {
        const src = fs.readFileSync(
            path.resolve(__dirname, '../components/flows/FreshnessIndicator.jsx'), 'utf8',
        );
        expect(src).not.toMatch(/label:\s*'LIVE'/);
        expect(src).not.toMatch(/label:\s*'EST'/);
        expect(src).not.toMatch(/\|\|\s*'estimated'/);
    });
});

describe('FreshnessIndicator consumers', () => {
    it('JunctionCard shows UNKNOWN for a point without confidence and OBSERVED with as-of when reported', () => {
        const { container, rerender } = render(<JunctionCard point={{ id: 'walcl', label: 'Fed balance sheet', value: 7e12 }} />);
        expect(badge(container)).toHaveAttribute('data-freshness', 'unknown');

        rerender(<JunctionCard point={{ id: 'walcl', label: 'Fed balance sheet', value: 7e12, confidence: 'confirmed', as_of: '2026-10-15' }} />);
        expect(screen.getByText('OBSERVED 2026-10-15')).toBeInTheDocument();

        rerender(<JunctionCard point={{ id: 'walcl', label: 'Fed balance sheet', value: null, confidence: 'confirmed', available: false, reason: 'no rows' }} />);
        expect(badge(container)).toHaveAttribute('data-freshness', 'unavailable');
    });

    it('FlowTooltip renders node and edge confidence honestly', () => {
        const { rerender } = render(<FlowTooltip visible x={0} y={0} node={{ id: 'n', label: 'Node', value: 1 }} />);
        expect(screen.getByText('UNKNOWN')).toBeInTheDocument();
        rerender(<FlowTooltip visible x={0} y={0} edge={{ source_layer: 'monetary', target_layer: 'credit', value_usd: 1, confidence: 'estimated' }} />);
        expect(screen.getByText('ESTIMATED')).toBeInTheDocument();
    });

    it('JunctionDashboard shows readings-table provenance and unknown layer confidence', async () => {
        const { default: JunctionDashboard } = await import('../components/flows/JunctionDashboard.jsx');
        api.getJunctionPoints.mockResolvedValue({
            junction_points: [{ id: 'walcl', layer: 'monetary', label: 'Fed BS', value: 7e12, confidence: 'confirmed' }],
            layer_summaries: [{ id: 'monetary', label: 'Monetary', order: 1, aggregate_value: 7e12, dominant_confidence: null, node_count: 1 }],
            total_junction_points: 1,
            provenance: {
                junction_point_readings: {
                    table: 'junction_point_readings', max_obs_date: '2026-10-13', age_days: 6,
                    stale_after_days: 3, unavailable: true, reason: 'stale',
                },
            },
        });
        render(<JunctionDashboard />);
        const prov = await screen.findByTestId('readings-provenance');
        expect(prov).toHaveTextContent('STALE');
        expect(prov).toHaveTextContent('newest obs 2026-10-13 (6d old, threshold 3d)');
        expect(screen.getByText('regime unknown')).toBeInTheDocument();
        expect(screen.queryByText('neutral')).not.toBeInTheDocument();
        // layer card badge + compact dot for the null dominant_confidence
        expect(document.querySelectorAll('[data-freshness="unknown"]').length).toBeGreaterThanOrEqual(2);
        expect(document.body.textContent).not.toMatch(/LIVE|\bEST\b/);
    });

    it('JunctionDashboard says provenance is not reported when the envelope is missing', async () => {
        const { default: JunctionDashboard } = await import('../components/flows/JunctionDashboard.jsx');
        api.getJunctionPoints.mockResolvedValue({ junction_points: [], layer_summaries: [], total_junction_points: 0 });
        // useJunctionPoints keeps a 5-minute module cache; step past it so this
        // render fetches instead of reusing the previous test's payload.
        const realNow = Date.now();
        const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(realNow + 10 * 60 * 1000);
        render(<JunctionDashboard />);
        const prov = await screen.findByTestId('readings-provenance');
        expect(prov).toHaveTextContent('freshness not reported');
        expect(prov).toHaveTextContent('UNKNOWN');
        nowSpy.mockRestore();
    });

    it('FlowWaterfall lists per-step confidence', async () => {
        if (typeof globalThis.ResizeObserver !== 'function') {
            globalThis.ResizeObserver = class { observe() {} disconnect() {} };
        }
        const { default: FlowWaterfall } = await import('../components/flows/FlowWaterfall.jsx');
        api.getFlowWaterfall.mockResolvedValue({
            source: 'fed', starting_value: 100,
            chain: [
                { layer: 'monetary', node: 'fed', label: 'Fed', value: 100, attenuation: 0, confidence: 'confirmed' },
                { layer: 'credit', node: 'banks', label: 'Banks', value: 30, attenuation: 0.7, confidence: null },
            ],
        });
        render(<FlowWaterfall />);
        const strip = await screen.findByTestId('waterfall-step-confidence');
        expect(strip).toHaveTextContent('OBSERVED');
        expect(strip).toHaveTextContent('UNKNOWN');
        fireEvent.click(screen.getByText('Reverse Repo'));
        expect(api.getFlowWaterfall).toHaveBeenCalled();
    });
});
