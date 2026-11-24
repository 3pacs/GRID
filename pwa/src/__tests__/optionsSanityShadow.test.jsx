import React from 'react';
import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { TickerRecommendations } from '../views/Options.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: { getOptionsRecommendations: vi.fn() } }));

// #1400: CROSS_ASSET is a shadow check. Its verdict is persisted with
// "shadow": true and never gates, so the card must not draw it as a failed gate.
function recWith(sanityStatus) {
    return {
        scan_summary: { source: 'persisted' },
        recommendations: [{ ticker: 'AAPL', direction: 'CALL', strike: 200,
            entry_price: 10, target_price: 12, stop_loss: 8, sanity_status: sanityStatus }],
    };
}

describe('Options sanity dots: shadow verdicts', () => {
    beforeEach(() => api.getOptionsRecommendations.mockReset());

    it('draws a shadow FAIL as a labelled hollow dot, not a red one', async () => {
        api.getOptionsRecommendations.mockResolvedValue(recWith({
            DATA_QUALITY: { status: 'PASS' }, DEALER_FLOW: { status: 'PASS' },
            CROSS_ASSET: { status: 'FAIL', reason: 'Momentum (-9.00%) and regime both contradict CALL', shadow: true },
            LLM_REVIEW: { status: 'SKIP' }, HISTORICAL_ANALOG: { status: 'SKIP' },
        }));
        render(<TickerRecommendations ticker="AAPL" />);
        const dot = await screen.findByTitle('Cross asset: FAIL (shadow, recorded, not a gate)');
        expect(dot).toHaveAttribute('data-shadow', 'true');
        expect(dot.style.background).toBe('transparent');
        expect(dot.style.border).toContain('dashed');
        expect(screen.queryByTitle('Cross asset: FAIL')).toBeNull();
    });

    it('draws shadow PASS and SKIP the same hollow way', async () => {
        for (const status of ['PASS', 'SKIP']) {
            api.getOptionsRecommendations.mockResolvedValue(recWith({
                DATA_QUALITY: { status: 'PASS' }, CROSS_ASSET: { status, shadow: true },
            }));
            const view = render(<TickerRecommendations ticker="AAPL" />);
            const dot = await screen.findByTitle(`Cross asset: ${status} (shadow, recorded, not a gate)`);
            expect(dot.style.background).toBe('transparent');
            view.unmount();
        }
    });

    it('still draws a gating FAIL as filled, beside a shadow verdict', async () => {
        api.getOptionsRecommendations.mockResolvedValue(recWith({
            DATA_QUALITY: { status: 'PASS' }, DEALER_FLOW: { status: 'FAIL' },
            CROSS_ASSET: { status: 'PASS', shadow: true },
        }));
        render(<TickerRecommendations ticker="AAPL" />);
        const gate = await screen.findByTitle('Dealer flow: FAIL');
        expect(gate).not.toHaveAttribute('data-shadow');
        expect(gate.style.background).not.toBe('transparent');
        expect(gate.style.background).not.toBe('');
    });

    it('leaves a row without the flag as it was (CROSS_ASSET gated before #1400)', async () => {
        api.getOptionsRecommendations.mockResolvedValue(recWith({
            DATA_QUALITY: { status: 'PASS' }, CROSS_ASSET: { status: 'PASS' },
        }));
        render(<TickerRecommendations ticker="AAPL" />);
        const dot = await screen.findByTitle('Cross asset: PASS');
        expect(dot).not.toHaveAttribute('data-shadow');
        expect(dot.style.background).not.toBe('transparent');
    });
});
