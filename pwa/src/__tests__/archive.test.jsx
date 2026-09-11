import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Archive from '../views/Archive.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getResearchArchive: vi.fn(),
        triggerDeepDive: vi.fn(),
        getFlowBriefingAudioUrl: vi.fn(() => 'https://example.test/audio.mp3'),
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
});
