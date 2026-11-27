import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import OracleForwardHealth from '../components/OracleForwardHealth.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: { getOracleForwardHealth: vi.fn() } }));

const REASON = 'quote is older than the 20-minute reference limit';
// 2026-11-25 as the server reports it: every candidate abstained.
const INCIDENT = {
    status: 'all_abstained', level: 'failing', session_date: '2026-11-25', attempts: 6,
    message: 'No usable Oracle forecast in the 2026-11-25 session: all 610 candidates abstained over 6 attempt(s). '
        + `Top reason: ${REASON}. Last real forecast: 2026-11-19T15:42:44+00:00.`,
    tickers: {
        SPY: { qualified: 0, abstained: 305, top_reason: REASON },
        QQQ: { qualified: 0, abstained: 305, top_reason: REASON },
    },
};

describe('Oracle forward-pass status on the predictions page', () => {
    beforeEach(() => { api.getOracleForwardHealth.mockReset(); });

    it('says so, as an alert, when every candidate abstained', async () => {
        api.getOracleForwardHealth.mockResolvedValue(INCIDENT);
        render(<OracleForwardHealth />);
        const banner = await screen.findByRole('alert');
        expect(banner).toHaveAttribute('data-level', 'failing');
        expect(banner).toHaveTextContent('NO USABLE FORECAST');
        expect(banner).toHaveTextContent('all 610 candidates abstained over 6 attempt(s)');
        expect(banner).toHaveTextContent(`SPY: 0 forecast(s), 305 abstention(s) (${REASON})`);
    });

    it('shows the operator note an admin response carries', async () => {
        api.getOracleForwardHealth.mockResolvedValue({
            ...INCIDENT, operator_notes: { SPY: 'the newest SPY capture is a private-use (Schwab) batch' },
        });
        render(<OracleForwardHealth />);
        expect(await screen.findByRole('alert')).toHaveTextContent('SPY: the newest SPY capture is a private-use (Schwab) batch');
    });

    it('names the cohort ticker that has no forecast', async () => {
        api.getOracleForwardHealth.mockResolvedValue({
            status: 'partial', level: 'degraded', session_date: '2026-11-25',
            message: 'Oracle forward pass has no forecast for SPY in the 2026-11-25 session.',
            tickers: { SPY: { qualified: 0, abstained: 0, top_reason: null }, QQQ: { qualified: 111, abstained: 0, top_reason: null } },
        });
        render(<OracleForwardHealth />);
        const banner = await screen.findByRole('status');
        expect(banner).toHaveAttribute('data-level', 'degraded');
        expect(banner).toHaveTextContent('FORWARD PASS DEGRADED');
        expect(banner).toHaveTextContent('no forecast for SPY');
    });

    it('is a quiet line, not an alert, when the session has its forecasts', async () => {
        api.getOracleForwardHealth.mockResolvedValue({
            status: 'ok', level: 'ok', session_date: '2026-11-25',
            message: 'Oracle forward pass has a forecast for every cohort ticker in the 2026-11-25 session.',
            tickers: { SPY: { qualified: 40, abstained: 0 }, QQQ: { qualified: 111, abstained: 0 } },
        });
        render(<OracleForwardHealth />);
        const banner = await screen.findByRole('status');
        expect(banner).toHaveTextContent('FORWARD PASS OK');
        expect(banner).not.toHaveTextContent('abstention(s)');
        expect(screen.queryByRole('alert')).toBeNull();
    });

    it.each([
        ['an API error', () => Promise.resolve({ error: true, message: 'HTTP 500' })],
        ['an incomplete response', () => Promise.resolve({ level: 'ok' })],
        ['a rejected request', () => Promise.reject(new Error('network'))],
    ])('says the status is unavailable after %s, never nothing', async (_name, reply) => {
        api.getOracleForwardHealth.mockImplementation(reply);
        render(<OracleForwardHealth />);
        await waitFor(() => expect(screen.getByTestId('forward-health-unavailable')).toBeInTheDocument());
        expect(screen.getByTestId('forward-health-unavailable')).toHaveTextContent('Forward-pass status unavailable');
    });
});
