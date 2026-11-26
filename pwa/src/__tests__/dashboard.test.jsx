import React from 'react';
import { render, screen, waitFor, fireEvent, within, act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Dashboard from '../views/Dashboard.jsx';
import { api } from '../api.js';

const storeState = {
    currentRegime: null,
    systemStatus: null,
    setCurrentRegime: vi.fn(),
    setSystemStatus: vi.fn(),
    setLoading: vi.fn(),
    addNotification: vi.fn(),
    livePriceUpdates: {},
};

vi.mock('../api.js', () => ({
    api: {
        getCurrent: vi.fn(),
        getStatus: vi.fn(),
        getThesis: vi.fn(),
        getIntelDashboard: vi.fn(),
        getAggregatedFlows: vi.fn(),
        getWatchlistPrices: vi.fn(),
        refreshWatchlistPrices: vi.fn(),
        getWatchlistEnriched: vi.fn(),
        listFlowBriefings: vi.fn(),
        loadFlowBriefingAudio: vi.fn(async (name) => `blob:/audio/${name}`),
        getPostmortemLessons: vi.fn(),
        generateFlowBriefing: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

vi.mock('../hooks/useDevice.js', () => ({
    useDevice: vi.fn(() => ({ isMobile: false })),
}));

vi.mock('../hooks/useWebSocket.js', () => ({
    useWebSocket: vi.fn(() => ({ connected: false, prices: {} })),
}));

vi.mock('../components/StatusDot.jsx', () => ({
    default: () => <div data-testid="status-dot" />,
}));

vi.mock('../components/DashboardFlows.jsx', () => ({
    default: () => <div data-testid="dashboard-flows" />,
}));

describe('Dashboard watchlist loading', () => {
    beforeEach(() => {
        storeState.currentRegime = null;
        storeState.systemStatus = null;
        storeState.livePriceUpdates = {};
        storeState.setCurrentRegime.mockReset();
        storeState.setSystemStatus.mockReset();
        storeState.setLoading.mockReset();
        storeState.addNotification.mockReset();

        api.getCurrent.mockReset();
        api.getStatus.mockReset();
        api.getThesis.mockReset();
        api.getIntelDashboard.mockReset();
        api.getAggregatedFlows.mockReset();
        api.getWatchlistPrices.mockReset();
        api.refreshWatchlistPrices.mockReset();
        api.getWatchlistEnriched.mockReset();
        api.listFlowBriefings.mockReset();
        api.loadFlowBriefingAudio.mockClear();

        api.getCurrent.mockResolvedValue({ state: 'NEUTRAL', confidence: 0.5 });
        api.getStatus.mockResolvedValue({ database: { connected: true } });
        api.getThesis.mockResolvedValue({});
        api.getIntelDashboard.mockResolvedValue({});
        api.getAggregatedFlows.mockResolvedValue({});
        api.getWatchlistPrices.mockResolvedValue({ prices: { SPY: { price: 500, pct_1d: 0.01 } }, fresh: true, cached: true });
        api.refreshWatchlistPrices.mockResolvedValue({ prices: { SPY: { price: 501, pct_1d: 0.02 } } });
        api.getWatchlistEnriched.mockResolvedValue({ items: [] });
        api.listFlowBriefings.mockResolvedValue({ briefings: [] });
        api.getPostmortemLessons.mockReset();
        api.getPostmortemLessons.mockResolvedValue({ lessons: [], generated_at: null });
        api.generateFlowBriefing.mockReset();
    });

    it('never stores a failed regime read and never calls a missing reading NEUTRAL (#959)', async () => {
        api.getCurrent.mockResolvedValue({ error: true, status: 503, message: 'down' });
        render(<Dashboard onNavigate={vi.fn()} />);

        await waitFor(() => expect(api.getCurrent).toHaveBeenCalled());
        expect(storeState.setCurrentRegime).not.toHaveBeenCalled();
        expect(screen.getByText('UNAVAILABLE')).toBeInTheDocument();
        expect(screen.queryByText('NEUTRAL')).not.toBeInTheDocument();
    });

    it('uses cached watchlist prices on mount and avoids the refresh endpoint', async () => {
        render(<Dashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(api.getWatchlistPrices).toHaveBeenCalledTimes(1);
        });

        expect(api.refreshWatchlistPrices).not.toHaveBeenCalled();
    });

    it('labels the PULSE row with the live price source (Wave 3 #11)', async () => {
        api.getWatchlistPrices.mockResolvedValue({
            prices: { SPY: { price: 500, pct_1d: 0.01, source: 'yfinance', updated_at: '2026-09-28T12:00:00Z' } },
            fresh: true, cached: true, source: 'yfinance',
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText('PULSE')).toBeInTheDocument();
        });
        const pulseLabel = screen.getByText('PULSE');
        // The strip is not a live feed and names no source of its own: each quote carries its own.
        expect(pulseLabel.title).not.toMatch(/\blive quotes\b/i);
        expect(pulseLabel.title).toMatch(/not a live feed/i);
        expect(screen.getByTitle(/^yfinance · /)).toBeInTheDocument();
    });

    it('does not attribute a quote with no source to yfinance (#959)', async () => {
        api.getWatchlistPrices.mockResolvedValue({
            prices: { SPY: { price: 500, pct_1d: 0.01 } }, fresh: true, cached: true,
        });
        render(<Dashboard onNavigate={vi.fn()} />);
        await waitFor(() => expect(screen.getAllByTitle('source not reported · time not reported').length).toBeGreaterThan(0));
        expect(screen.queryByTitle(/yfinance/i)).not.toBeInTheDocument();
    });

    it('says why the PULSE strip is empty when the quote cache is empty (#959)', async () => {
        api.getWatchlistPrices.mockResolvedValue({ prices: {}, fresh: false, cached: false, source: 'yfinance' });
        render(<Dashboard onNavigate={vi.fn()} />);

        const note = await screen.findByTestId('pulse-empty-note');
        expect(note).toHaveTextContent(/No cached quotes yet/i);
        expect(note).toHaveTextContent(/not a live feed/i);
        expect(note).not.toHaveTextContent(/failed/i);
    });

    it('does not claim an empty cache when the quote read failed (#959)', async () => {
        api.getWatchlistPrices.mockResolvedValue({ error: true, status: 503, message: 'down' });
        render(<Dashboard onNavigate={vi.fn()} />);

        const note = await screen.findByTestId('pulse-empty-note');
        expect(note).toHaveTextContent(/Quotes unavailable: the request failed/i);
        expect(note).not.toHaveTextContent(/No cached quotes/i);
    });

    it('shows no empty-strip note while the first quote read is pending or when a quote exists (#959)', async () => {
        let resolvePrices;
        api.getWatchlistPrices.mockReturnValue(new Promise(r => { resolvePrices = r; }));
        render(<Dashboard onNavigate={vi.fn()} />);
        await waitFor(() => expect(api.getWatchlistPrices).toHaveBeenCalled());
        expect(screen.queryByTestId('pulse-empty-note')).not.toBeInTheDocument();

        await act(async () => { resolvePrices({ prices: { SPY: { price: 500, pct_1d: 0.01 } }, fresh: true, cached: true }); });
        await waitFor(() => expect(screen.getAllByText('500.00').length).toBeGreaterThan(0));
        expect(screen.queryByTestId('pulse-empty-note')).not.toBeInTheDocument();
    });

    it('fills the strip only when the user presses Refresh quotes, and says so if that fails (#959)', async () => {
        api.getWatchlistPrices.mockResolvedValue({ prices: {}, fresh: false, cached: false });
        api.refreshWatchlistPrices.mockResolvedValueOnce({ error: true, status: 502, message: 'upstream' });
        render(<Dashboard onNavigate={vi.fn()} />);

        const button = await screen.findByTestId('pulse-refresh');
        expect(api.refreshWatchlistPrices).not.toHaveBeenCalled();
        fireEvent.click(button);
        expect(await screen.findByText(/Quote refresh failed, so there are still no quotes/i)).toBeInTheDocument();

        api.refreshWatchlistPrices.mockResolvedValueOnce({ prices: { SPY: { price: 501, pct_1d: 0.02 } } });
        fireEvent.click(screen.getByTestId('pulse-refresh'));
        await waitFor(() => expect(screen.queryByTestId('pulse-empty-note')).not.toBeInTheDocument());
        expect(screen.getAllByText('501.00').length).toBeGreaterThan(0);
    });

    it('shows "time not reported" for a change-feed event with no time, never the brief time (#959)', async () => {
        const generated = new Date(Date.now() - 5 * 60000).toISOString();
        api.getIntelDashboard.mockResolvedValue({
            generated_at: generated,
            cross_ref: { red_flags: [{ category: 'Macro', description: 'divergence X' }] },
            trust: { convergence_events: [{ description: 'sources agree Y', detected_at: '2020-01-01T00:00:00Z' }] },
        });
        render(<Dashboard onNavigate={vi.fn()} />);
        await screen.findByText(/divergence X/);
        expect(screen.getByText('time not reported')).toBeInTheDocument();
        expect(screen.queryByText('5m ago')).not.toBeInTheDocument();
        // an event that carries its own time still shows it
        expect(screen.getByText(/\dd ago/)).toBeInTheDocument();
    });

    it('does not read a failed intel request as "no changes" (#959)', async () => {
        api.getIntelDashboard.mockResolvedValue({ error: true, status: 503, message: 'down' });
        render(<Dashboard onNavigate={vi.fn()} />);
        await screen.findByText(/Change feed unavailable/);
        expect(screen.queryByText(/No significant changes/)).not.toBeInTheDocument();
    });

    it('keeps pending, available-empty and failed change-feed states apart (#959, Codex hold at 0cd00fc2)', async () => {
        let release;
        api.getIntelDashboard.mockReturnValue(new Promise((res) => { release = res; }));
        render(<Dashboard onNavigate={vi.fn()} />);
        // First request still in flight: neither "no changes" nor "unavailable".
        await screen.findByText('Loading change feed...');
        expect(screen.queryByText(/No significant changes/)).not.toBeInTheDocument();
        expect(screen.queryByText(/Change feed unavailable/)).not.toBeInTheDocument();
        // It succeeds with nothing to show: now, and only now, the feed is empty.
        await act(async () => { release({ generated_at: '2026-11-24T12:00:00Z' }); });
        await screen.findByText(/No significant changes/);
        expect(screen.queryByText('Loading change feed...')).not.toBeInTheDocument();
    });

    it('a rejected first intel request reads unavailable, not empty (#959)', async () => {
        api.getIntelDashboard.mockRejectedValue(new Error('network'));
        render(<Dashboard onNavigate={vi.fn()} />);
        await screen.findByText(/Change feed unavailable/);
        expect(screen.queryByText(/No significant changes/)).not.toBeInTheDocument();
        expect(screen.queryByText('Loading change feed...')).not.toBeInTheDocument();
    });

    it('renders a text-only on-demand briefing when no audio was made (paid TTS disabled)', async () => {
        // Wave 3 W3.4 (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md #5): with
        // GRID_ALLOW_PAID_LLM unset, audio_briefing.py returns script_text
        // with no audio_path. The Dashboard button must show that text
        // instead of silently reporting a failure.
        api.generateFlowBriefing.mockResolvedValue({
            status: 'SUCCESS',
            briefing: {
                script_text: 'Good morning. GRID Intelligence briefing for today.',
                audio_path: null,
                provider: 'local',
                briefing_date: '2026-09-28',
                generated_at: '2026-09-28T12:00:00Z',
            },
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        const btn = await screen.findByRole('button', { name: 'Generate on request' });
        fireEvent.click(btn);

        await waitFor(() => {
            expect(screen.getByTestId('text-briefing')).toBeInTheDocument();
        });
        const textBriefing = screen.getByTestId('text-briefing');
        expect(within(textBriefing).getByText(/GRID Intelligence briefing for today/)).toBeInTheDocument();
        expect(within(textBriefing).getByText(/\(local\)/)).toBeInTheDocument();
    });

    it('says the local TTS is unavailable when Kokoro did not answer (never a fake success)', async () => {
        api.generateFlowBriefing.mockResolvedValue({
            status: 'SUCCESS',
            briefing: {
                script_text: 'Good morning. GRID Intelligence briefing for today.',
                audio_path: null,
                audio_status: 'unavailable',
                audio_note: 'Local TTS unavailable: Kokoro at http://gridz4:8880 is unreachable (ConnectionError). Text-only briefing; no audio was made and no paid TTS was tried.',
                provider: 'local',
                briefing_date: '2026-10-01',
                generated_at: '2026-10-01T12:00:00Z',
            },
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        fireEvent.click(await screen.findByRole('button', { name: 'Generate on request' }));

        await waitFor(() => {
            expect(screen.getByText(/Local TTS unavailable: Kokoro/)).toBeInTheDocument();
        });
        expect(screen.getByTestId('text-briefing')).toBeInTheDocument();
        expect(screen.queryByRole('button', { name: /play briefing/i })).not.toBeInTheDocument();
    });

    it('labels an older saved recording as previous when the new briefing has no audio', async () => {
        api.listFlowBriefings.mockResolvedValue({
            briefings: [{ filename: 'briefing_2026-04-07_20260407_060040.mp3', briefing_date: '2026-04-07' }],
        });
        api.generateFlowBriefing.mockResolvedValue({
            status: 'SUCCESS',
            briefing: {
                script_text: 'Good morning.',
                audio_path: null,
                audio_status: 'unavailable',
                audio_note: 'Local TTS unavailable: Kokoro TTS is unreachable (ConnectionError).',
                provider: 'local',
            },
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        await screen.findByRole('button', { name: 'Load and play briefing' });
        expect(screen.queryByTestId('previous-recording')).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole('button', { name: 'Generate on request' }));

        await waitFor(() => {
            expect(screen.getByTestId('previous-recording')).toHaveTextContent(/Previous recording \(2026-04-07\)/);
        });
    });

    it('shows the not-configured note inside a text-only briefing', async () => {
        api.generateFlowBriefing.mockResolvedValue({
            status: 'SUCCESS',
            briefing: {
                script_text: 'Good morning.',
                audio_path: null,
                audio_status: 'not_configured',
                audio_note: 'Text-only briefing: local TTS is not configured (set GRID_KOKORO_URL to a Kokoro server). Paid TTS stays off.',
                provider: 'local',
            },
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        fireEvent.click(await screen.findByRole('button', { name: 'Generate on request' }));

        await waitFor(() => {
            expect(screen.getByTestId('text-briefing-note')).toHaveTextContent(/GRID_KOKORO_URL/);
        });
    });
});
