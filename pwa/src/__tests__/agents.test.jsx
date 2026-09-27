import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import Agents from '../views/Agents.jsx';
import { api } from '../api.js';

const storeState = {
    agentProgress: null,
    agentLastComplete: null,
    wsConnected: false,
    addNotification: vi.fn(),
};

vi.mock('../api.js', () => ({
    api: {
        getAgentStatus: vi.fn(),
        getAgentRuns: vi.fn(),
        getBacktestSummary: vi.fn(),
        getAgentRun: vi.fn(),
        triggerAgentRun: vi.fn(),
        runAgentBacktest: vi.fn(),
        startAgentSchedule: vi.fn(),
        stopAgentSchedule: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn((selector) => (selector ? selector(storeState) : storeState)),
}));

describe('Agents view — honest empty state', () => {
    it('says a run has not been generated instead of implying the schedule has run', async () => {
        api.getAgentStatus.mockResolvedValue({
            enabled: true,
            llm_provider: 'local',
            llm_model: 'qwen3.8-27b',
            debate_rounds: 2,
            tradingagents_installed: true,
            schedule: { running: false, cron: 'weekdays 17:00', next_run: null, scheduled_jobs: 0 },
        });
        api.getAgentRuns.mockResolvedValue([]);
        api.getBacktestSummary.mockResolvedValue({ has_data: false, message: 'No data yet' });

        render(<Agents />);

        await waitFor(() => {
            expect(screen.getByText(/Not generated — no agent run has completed yet/)).toBeInTheDocument();
        });
        // The stale copy this replaces must not reappear.
        expect(screen.queryByText('No agent runs yet')).not.toBeInTheDocument();
    });

    it('still lists real runs when they exist, without the empty-state copy', async () => {
        api.getAgentStatus.mockResolvedValue({
            enabled: true,
            schedule: { running: true, cron: 'weekdays 17:00', next_run: null, scheduled_jobs: 1 },
        });
        api.getAgentRuns.mockResolvedValue([
            { id: 1, ticker: 'SPY', as_of_date: '2026-09-25', final_decision: 'BUY', duration_seconds: 12.3 },
        ]);
        api.getBacktestSummary.mockResolvedValue({ has_data: false });

        render(<Agents />);

        await waitFor(() => {
            expect(screen.getByText('SPY')).toBeInTheDocument();
        });
        expect(screen.queryByText(/Not generated — no agent run/)).not.toBeInTheDocument();
    });
});
