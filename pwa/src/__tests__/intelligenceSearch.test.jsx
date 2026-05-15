import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import IntelligenceSearch, { getIntelligenceSearchOpenTarget } from '../components/IntelligenceSearch.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

describe('IntelligenceSearch navigation', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('maps results to canonical open targets', () => {
        expect(getIntelligenceSearchOpenTarget({
            source_type: 'actor',
            title: 'Warren Buffett',
            source_id: '1',
        })).toEqual({
            view: 'actor-network',
            param: 'Warren Buffett',
        });

        expect(getIntelligenceSearchOpenTarget({
            source_type: 'hypothesis',
            source_id: '42',
            title: 'Semiconductor thesis',
        })).toEqual({
            view: 'discovery',
            param: '42',
        });

        expect(getIntelligenceSearchOpenTarget({
            source_type: 'signal',
            title: 'INSIDER_BUY: NVDA',
            source_id: '88',
        })).toEqual({
            view: 'watchlist-analysis',
            param: 'NVDA',
        });

        expect(getIntelligenceSearchOpenTarget({
            source_type: 'snapshot',
            title: 'Semiconductor Deep Dive',
            source_id: '7',
        })).toEqual({
            view: 'snapshots',
        });
    });

    it('sanitizes snippet HTML so script tags and event handlers cannot execute', async () => {
        api.get.mockResolvedValue({
            total: 1,
            results: [
                {
                    source_type: 'actor',
                    source_id: '1',
                    title: 'Warren Buffett',
                    snippet: 'safe text <mark>buffett</mark> <script>window.__pwned=1;</script> <img src=x onerror="window.__pwned=2">',
                    relevance: 0.91,
                },
            ],
        });

        const { container } = render(
            <IntelligenceSearch
                onClose={vi.fn()}
                onAddToCanvas={vi.fn()}
                onOpenResult={vi.fn()}
            />
        );

        fireEvent.change(
            screen.getByPlaceholderText('Search actors, signals, hypotheses...'),
            { target: { value: 'buffett' } }
        );

        await screen.findByText('Warren Buffett');

        // <mark> is preserved so the highlight still renders.
        expect(container.querySelector('mark')).not.toBeNull();
        // <script> and event-handler-bearing <img> must be stripped.
        expect(container.querySelector('script')).toBeNull();
        expect(container.querySelector('img')).toBeNull();
        expect(window.__pwned).toBeUndefined();
    });

    it('renders an Open action that routes matching results', async () => {
        api.get.mockResolvedValue({
            total: 1,
            results: [
                {
                    source_type: 'actor',
                    source_id: '1',
                    title: 'Warren Buffett',
                    snippet: 'Investor and Berkshire Hathaway chairman',
                    relevance: 0.91,
                },
            ],
        });

        const handleOpen = vi.fn();
        render(
            <IntelligenceSearch
                onClose={vi.fn()}
                onAddToCanvas={vi.fn()}
                onOpenResult={handleOpen}
            />
        );

        fireEvent.change(
            screen.getByPlaceholderText('Search actors, signals, hypotheses...'),
            { target: { value: 'buffett' } }
        );

        expect(await screen.findByText('Warren Buffett')).toBeInTheDocument();

        fireEvent.click(screen.getByRole('button', { name: 'Open Warren Buffett' }));

        expect(handleOpen).toHaveBeenCalledWith(
            {
                view: 'actor-network',
                param: 'Warren Buffett',
            },
            expect.objectContaining({
                source_type: 'actor',
                source_id: '1',
            })
        );
    });
});
