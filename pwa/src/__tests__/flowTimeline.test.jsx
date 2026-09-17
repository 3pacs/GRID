import React from 'react';
import { render } from '@testing-library/react';
import { describe, expect, it, beforeAll } from 'vitest';
import FlowTimeline from '../components/FlowTimeline.jsx';

// jsdom implements neither ResizeObserver (the component measures itself)
// nor SVG path geometry (the GEX line animates over its own length).
beforeAll(() => {
    if (!globalThis.ResizeObserver) {
        globalThis.ResizeObserver = class {
            observe() {}
            unobserve() {}
            disconnect() {}
        };
    }
    if (!globalThis.SVGElement.prototype.getTotalLength) {
        globalThis.SVGElement.prototype.getTotalLength = () => 0;
    }
});

// /api/v1/derivatives/flow-timeline now reports a bar it could not compute as
// net_gex: null / regime: null instead of 0 / "neutral" (audit C-M5). The
// chart must show a gap for those days, not a zero-GEX neutral session.
const TIMELINE = {
    ticker: 'SPY',
    days: 30,
    history: [
        { date: '2026-03-02', net_gex: 2_000_000_000, regime: 'long_gamma',
          chain_snap_date: '2026-03-02', spot: 500 },
        { date: '2026-03-03', net_gex: null, regime: null,
          chain_snap_date: null, spot: 501 },
        { date: '2026-03-04', net_gex: 1_000_000_000, regime: 'long_gamma',
          chain_snap_date: '2026-03-04', spot: 502 },
    ],
    opex_calendar: [],
    catalysts: [],
    gamma_flip_crossings: [],
};

describe('FlowTimeline null bars', () => {
    it('does not plot a null bar as a zero-GEX neutral day', () => {
        // The chart draws one regime band per gap between plotted bars. If
        // the null day were plotted (as the old net_gex: 0 / "neutral" bar
        // was), there would be one more band than with it left out.
        const countBands = data => {
            const { container, unmount } = render(
                <FlowTimeline ticker="SPY" timelineData={data} />
            );
            const n = container.querySelectorAll('svg g > rect').length;
            unmount();
            return n;
        };

        const asIfMeasured = {
            ...TIMELINE,
            history: TIMELINE.history.map(bar => (
                bar.net_gex == null
                    ? { ...bar, net_gex: 0, regime: 'neutral' }
                    : bar
            )),
        };

        expect(countBands(TIMELINE)).toBe(countBands(asIfMeasured) - 1);
    });

    it('reports the latest measured GEX, not the null bar', () => {
        const { container } = render(
            <FlowTimeline ticker="SPY" timelineData={TIMELINE} />
        );

        expect(container.textContent).toContain('$1.00B');
        expect(container.textContent).not.toContain('n/a');
    });

    it('shows n/a when no bar could be computed at all', () => {
        const allNull = {
            ...TIMELINE,
            history: TIMELINE.history.map(bar => ({
                ...bar, net_gex: null, regime: null, chain_snap_date: null,
            })),
        };
        const { container } = render(
            <FlowTimeline ticker="SPY" timelineData={allNull} />
        );

        expect(container.textContent).toContain('n/a');
        expect(container.textContent).not.toContain('$0');
    });
});
