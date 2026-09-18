import React from 'react';
import { cleanup, fireEvent, render, waitFor } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import CatalystTimeline, { formatConfidence, nodeRadius, statedConfidence } from '../views/CatalystTimeline.jsx';

// docs/reference/CONFIDENCE_POLICY.md, applied to the catalyst timeline
// (api.getCatalystTimeline, outside the oracle_predictions reader work).
//
// The view used to size every node from
//     e.confidence || e.probability || 0.5
// which invented a 50% confidence for an event that stated none, and
// rewrote a measured 0 to that same 0.5. The tooltip had the same `||`
// chain and printed '--' for a measured 0. These tests pin:
//
//   null  -> "unscored", minimum radius, dashed ring
//   0     -> "0%",       minimum radius, solid ring
//   0.5   -> "50%",      the radius an actual 50% event gets
//
// and that the unscored node is never drawn at the 50% radius.

const getCatalystTimeline = vi.fn();

vi.mock('../api.js', () => ({
    api: {
        get getCatalystTimeline() {
            return getCatalystTimeline;
        },
    },
}));

beforeAll(() => {
    if (!globalThis.ResizeObserver) {
        globalThis.ResizeObserver = class {
            observe() {}
            unobserve() {}
            disconnect() {}
        };
    }
});

const UNSCORED = {
    id: 'pred-1', type: 'prediction', subtype: 'price_target', date: '2026-11-15',
    label: 'UNSCORED EVENT', confidence: null, probability: null,
    invalidation: 'x',
};
const ZERO = {
    id: 'pred-2', type: 'prediction', subtype: 'price_target', date: '2026-11-20',
    label: 'ZERO EVENT', confidence: 0, invalidation: 'x',
};
const HALF = {
    id: 'ms-3', type: 'milestone', subtype: 'EARNINGS_GUIDANCE', date: '2026-11-25',
    label: 'HALF EVENT', probability: 0.5, invalidation: 'x',
};

const PAYLOAD = {
    status: 'ok', ticker: 'ACME', today: '2026-09-18', event_count: 3,
    events: [UNSCORED, ZERO, HALF], valuation: {},
};

async function renderTimeline() {
    getCatalystTimeline.mockResolvedValue(PAYLOAD);
    const utils = render(<CatalystTimeline selectedTicker="ACME" />);
    await waitFor(() => expect(utils.container.querySelectorAll('circle').length).toBe(3));
    return utils;
}

function circleFor(container, label) {
    // Nodes are drawn in event order; each one's short label <text> is
    // drawn in that same order, so the nth label belongs to the nth circle.
    const texts = Array.from(container.querySelectorAll('svg text'))
        .filter(t => ['UNSCORED EVENT', 'ZERO EVENT', 'HALF EVENT'].includes(t.textContent));
    const circles = Array.from(container.querySelectorAll('circle'));
    const idx = texts.findIndex(t => t.textContent === label);
    return circles[idx];
}

describe('CatalystTimeline unscored confidence', () => {
    afterEach(() => {
        cleanup();
        getCatalystTimeline.mockReset();
    });

    it('exports helpers that treat null as unscored and 0 as a measurement', () => {
        expect(statedConfidence({ confidence: null, probability: null })).toBeNull();
        expect(statedConfidence({})).toBeNull();
        expect(statedConfidence({ confidence: 0 })).toBe(0);
        expect(statedConfidence({ confidence: 0, probability: 0.9 })).toBe(0);
        expect(statedConfidence({ probability: 0 })).toBe(0);
        expect(statedConfidence({ confidence: null, probability: 0.5 })).toBe(0.5);

        expect(formatConfidence(null)).toBe('unscored');
        expect(formatConfidence(undefined)).toBe('unscored');
        expect(formatConfidence(0)).toBe('0%');
        expect(formatConfidence(0.5)).toBe('50%');

        expect(nodeRadius(null)).toBe(nodeRadius(0));
        expect(nodeRadius(null)).toBeLessThan(nodeRadius(0.5));
    });

    it('draws the unscored node at the minimum radius with a dashed ring, not as a 50% event', async () => {
        const { container } = await renderTimeline();

        const unscored = circleFor(container, 'UNSCORED EVENT');
        const zero = circleFor(container, 'ZERO EVENT');
        const half = circleFor(container, 'HALF EVENT');

        expect(unscored.getAttribute('data-confidence')).toBe('unscored');
        expect(zero.getAttribute('data-confidence')).toBe('scored');
        expect(half.getAttribute('data-confidence')).toBe('scored');

        const r = el => Number(el.getAttribute('r'));
        expect(r(unscored)).toBe(r(zero));
        expect(r(unscored)).toBeLessThan(r(half));
        // The old `|| 0.5` fallback sized both the null and the 0 event at 11.
        expect(r(unscored)).not.toBe(r(half));
        expect(r(zero)).not.toBe(r(half));

        expect(unscored.getAttribute('stroke-dasharray')).toBe('2,2');
        expect(zero.hasAttribute('stroke-dasharray')).toBe(false);
    });

    it('tooltip reads "unscored" for null and "0%" for a measured zero', async () => {
        const { container } = await renderTimeline();
        const text = () => container.textContent;

        fireEvent.mouseEnter(circleFor(container, 'UNSCORED EVENT'));
        expect(text()).toContain('Confidence: unscored');
        expect(text()).not.toContain('Confidence: 50%');
        expect(text()).not.toContain('Confidence: --');

        fireEvent.mouseEnter(circleFor(container, 'ZERO EVENT'));
        expect(text()).toContain('Confidence: 0%');
        expect(text()).not.toContain('Confidence: --');

        fireEvent.mouseEnter(circleFor(container, 'HALF EVENT'));
        expect(text()).toContain('Confidence: 50%');
    });

    it('detail panel shows an unscored row for null and 0% for a measured zero', async () => {
        const { container, getByText } = await renderTimeline();

        fireEvent.click(circleFor(container, 'UNSCORED EVENT'));
        await waitFor(() => expect(getByText('unscored')).toBeInTheDocument());
        expect(container.textContent).not.toContain('50%');

        fireEvent.click(circleFor(container, 'ZERO EVENT'));
        await waitFor(() => expect(getByText('0%')).toBeInTheDocument());
    });
});
