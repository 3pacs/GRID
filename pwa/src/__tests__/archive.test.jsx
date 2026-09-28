import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Archive from '../views/Archive.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getResearchArchive: vi.fn(),
        triggerDeepDive: vi.fn(),
        loadFlowBriefingAudio: vi.fn(async () => 'blob:https://example.test/audio'),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Archive view async data', () => {
    beforeEach(() => {
        api.getResearchArchive.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the archive tab content', async () => {
        const gate = deferred();
        api.getResearchArchive.mockImplementation(() => gate.promise);

        render(<Archive />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            deep_dive_count: 1,
            deep_dives: [{ id: 7, thesis_direction: 'BULLISH', generated_at: '2026-09-10T00:00:00Z', model_used: 'qwen3.8-27b', provider_used: 'local', duration_ms: 1200 }],
            audio_count: 0, postmortem_count: 0, diary_count: 0, thesis_count: 0,
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText(/Deep Dive #7/)).toBeInTheDocument();
        // Wave 3 W3.4: on-demand writers must read "generated on request",
        // never imply a live/scheduled feed (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md #4).
        expect(screen.getByText(/generated on request/i)).toBeInTheDocument();
        expect(screen.getByText(/qwen3\.8-27b \(local\)/i)).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getResearchArchive.mockResolvedValueOnce({ error: true, message: 'research archive unavailable' });

        render(<Archive />);

        expect(await screen.findByText('research archive unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getResearchArchive.mockResolvedValueOnce({
            deep_dive_count: 0, deep_dives: [], audio_count: 0, postmortem_count: 0, diary_count: 0, thesis_count: 0,
        });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getResearchArchive).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('research archive unavailable')).not.toBeInTheDocument();
    });

    // Item #16 (Wave 3 triage report): thesis scoring/postmortems are held
    // (look-ahead bug in intelligence.thesis_tracker.score_old_theses) —
    // neither tab may render as if it were a live, ongoing feed.
    it('shows a scoring-held banner on the postmortems tab, with the as_of date from the API', async () => {
        api.getResearchArchive.mockResolvedValueOnce({
            deep_dive_count: 0, deep_dives: [],
            audio_count: 0,
            postmortem_count: 1,
            postmortems: [{
                thesis_direction: 'bullish', actual_direction: 'bearish',
                root_cause: 'external_shock', what_we_missed: 'a shock',
                lesson: 'diversify', generated_at: '2026-04-17T00:00:00Z',
            }],
            postmortem_as_of: '2026-04-17T00:00:00Z',
            diary_count: 0, thesis_count: 0,
        });

        render(<Archive />);
        await waitFor(() => expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument());

        fireEvent.click(screen.getByRole('button', { name: /Post-Mortems/ }));

        expect(await screen.findByText(/SCORING HELD/)).toBeInTheDocument();
        expect(screen.getByText(/2026-04-17T00:00:00Z/)).toBeInTheDocument();
    });

    it('shows a scoring-held banner on the theses tab even when snapshots exist', async () => {
        api.getResearchArchive.mockResolvedValueOnce({
            deep_dive_count: 0, deep_dives: [],
            audio_count: 0, postmortem_count: 0, diary_count: 0,
            thesis_count: 1,
            thesis_snapshots: [{
                timestamp: '2026-09-20T14:00:00Z', overall_direction: 'bullish',
                conviction: 0.6, outcome: null, actual_market_move: null,
            }],
        });

        render(<Archive />);
        await waitFor(() => expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument());

        fireEvent.click(screen.getByRole('button', { name: /Thesis History/ }));

        expect(await screen.findByText(/SCORING HELD/)).toBeInTheDocument();
    });
});
