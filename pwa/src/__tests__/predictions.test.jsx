import React from 'react';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Predictions, { PredictionCard, EvidencePanel } from '../views/Predictions.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getOracleScoreboard: vi.fn(),
        getOracleEvidence: vi.fn(),
        getOracleLatest: vi.fn(),
        getOraclePredictions: vi.fn(),
    },
}));

const EVIDENCE = {
    method: { min_windows: 30, min_outcomes_shown: 5 },
    summary: {
        outcomes: 21814, windows: 102, official_last_expiry: '2026-05-15', corrected_outcomes: 15743,
        corrected_last_expiry: '2026-11-03', excluded_implausible: 26, models_hidden_small: 15,
        models_tested: 1, models_with_relative_advantage: 0,
    },
    models: [
        { name: 'timeseries_enhanced', outcomes: 8064, windows: 94, raw_mean_pct: -0.145, excess_mean_pct: -0.083, lower_bound_pct: -0.259, verdict: 'no_relative_advantage' },
        { name: 'options_flow', outcomes: 351, windows: 2, raw_mean_pct: 2.64, excess_mean_pct: 0.4, lower_bound_pct: null, verdict: 'insufficient_windows' },
    ],
};

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Predictions view async data', () => {
    beforeEach(() => {
        api.getOracleScoreboard.mockReset();
        api.getOracleLatest.mockReset();
        api.getOracleEvidence.mockReset();
        api.getOraclePredictions.mockReset();
        api.getOracleEvidence.mockResolvedValue(EVIDENCE);
        api.getOracleLatest.mockResolvedValue({ streak: {}, recent_scored: [] });
        api.getOraclePredictions.mockResolvedValue({ predictions: [], total: 0 });
    });

    it('shows a loading skeleton while fetching, then renders the scoreboard', async () => {
        const gate = deferred();
        api.getOracleScoreboard.mockImplementation(() => gate.promise);

        render(<Predictions />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            overall: { accuracy: 0.62, scored: 40, total_pnl: 5.1 },
            models: [{ name: 'oracle_v1', accuracy: 0.62, cumulative_pnl: 5.1 }],
            by_ticker: [], calibration: {},
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.queryByText('MODEL TOURNAMENT')).not.toBeInTheDocument();
        expect(await screen.findByText('EXPLORATORY MODEL COMPARISON')).toBeInTheDocument();
        expect(screen.getByTestId('evidence-headline')).toHaveTextContent('0 of 1 testable models show a relative advantage. Financial edge is not demonstrated.');
        expect(screen.getByText('NO RELATIVE ADVANTAGE')).toBeInTheDocument();
        expect(screen.getByText('TOO FEW CLUSTERS')).toBeInTheDocument();
        expect(screen.getByText('View 2 model comparisons').closest('details')).not.toHaveAttribute('open');
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getOracleScoreboard.mockResolvedValueOnce({ error: true, message: 'oracle scoreboard unavailable' });

        render(<Predictions />);

        expect(await screen.findByText('oracle scoreboard unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getOracleScoreboard.mockResolvedValueOnce({
            overall: {}, models: [], by_ticker: [], calibration: {},
        });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getOracleScoreboard).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('oracle scoreboard unavailable')).not.toBeInTheDocument();
    });
});

describe('Predictions evidence panel', () => {
    beforeEach(() => {
        api.getOracleScoreboard.mockResolvedValue({ overall: {}, models: [], by_ticker: [], calibration: {} });
        api.getOracleLatest.mockResolvedValue({ streak: {}, recent_scored: [] });
        api.getOraclePredictions.mockResolvedValue({ predictions: [], total: 0 });
    });

    it('keeps the scoreboard but makes evidence failure visible and retryable', async () => {
        api.getOracleEvidence.mockResolvedValue({ error: true, message: 'evidence unavailable' });
        render(<Predictions />);
        expect(await screen.findByText('WEIGHTED OUTCOME SCORE')).toBeInTheDocument();
        expect(screen.getByText('evidence unavailable')).toBeInTheDocument();
        api.getOracleEvidence.mockResolvedValue(EVIDENCE);
        fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
        expect(await screen.findByText('EXPLORATORY MODEL COMPARISON')).toBeInTheDocument();
        expect(screen.queryByText('evidence unavailable')).not.toBeInTheDocument();
    });

    it('never calls a losing hindsight relative advantage financial edge', () => {
        const evidence = {
            ...EVIDENCE,
            summary: { ...EVIDENCE.summary, models_with_relative_advantage: 1 },
            models: [{ name: 'losing_model', outcomes: 30, windows: 30, raw_mean_pct: -1, excess_mean_pct: 1, lower_bound_pct: 1, verdict: 'relative_advantage' }],
        };
        render(<EvidencePanel evidence={evidence} />);
        expect(screen.getByText('RELATIVE ADVANTAGE')).toBeInTheDocument();
        expect(screen.getByTestId('evidence-headline')).toHaveTextContent('Financial edge is not demonstrated');
        expect(screen.getByText(/relative advantage can still lose money/)).toBeInTheDocument();
        expect(screen.queryByText('EDGE')).not.toBeInTheDocument();
    });
});

describe('Prediction operator lifecycle', () => {
    const base = { id: 'synthetic', ticker: 'SYNTH', model_name: 'synthetic', direction: 'CALL', signals: [], anti_signals: [], confidence: null };
    beforeEach(() => {
        api.getOracleScoreboard.mockReset();
        api.getOracleLatest.mockReset();
        api.getOracleEvidence.mockReset();
        api.getOraclePredictions.mockReset();
        api.getOracleScoreboard.mockResolvedValue({
            overall: { total_predictions: 10, active: 1, awaiting_score: 2, unscorable: 3, scored: 3, other: 1, hits: 1, partials: 1, misses: 1, accuracy: .5, mean_signed_return_pct: 3, total_pnl: 9 },
            models: [], by_ticker: [], calibration: {},
        });
        api.getOracleLatest.mockResolvedValue({ streak: {}, recent_scored: [] });
        api.getOracleEvidence.mockResolvedValue(EVIDENCE);
        api.getOraclePredictions.mockResolvedValue({ predictions: [], total: 0, has_more: false });
    });

    it('shows all lifecycle totals and reaches overdue, no-data and unknown rows', async () => {
        api.getOraclePredictions.mockImplementation(async ({ status }) => ({
            predictions: status === 'unscorable' ? [{ ...base, verdict: 'no_data', score_notes: 'No entry price recorded' }] : [],
            total: status === 'unscorable' ? 1 : 0, has_more: false,
        }));
        render(<Predictions />);
        await screen.findByText('MEAN SIGNED FORECAST RETURN');
        for (const [label, count] of Object.entries({ total: 10, active: 1, 'awaiting-score': 2, unscorable: 3, scored: 3, other: 1 })) {
            expect(within(screen.getByTestId(`lifecycle-${label}`)).getByText(String(count))).toBeInTheDocument();
        }
        expect(screen.getByText('+3.00%')).toBeInTheDocument();
        expect(screen.queryByText('CUMULATIVE P&L')).not.toBeInTheDocument();
        expect(screen.queryByText(/per trade/)).not.toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Awaiting score' }));
        await waitFor(() => expect(api.getOraclePredictions).toHaveBeenLastCalledWith({ status: 'expired', cohort: 'prospective', limit: '100', offset: '0' }));
        fireEvent.click(screen.getByRole('button', { name: 'Unscorable' }));
        expect(await screen.findByText('Unscorable: No entry price recorded')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Void / other' }));
        await waitFor(() => expect(api.getOraclePredictions).toHaveBeenLastCalledWith({ status: 'other', cohort: 'prospective', limit: '100', offset: '0' }));
    });

    it('reports list transport failures instead of claiming no predictions exist', async () => {
        api.getOraclePredictions.mockResolvedValue({ error: true, message: 'scoring records unavailable' });
        render(<Predictions />);
        expect(await screen.findByText('scoring records unavailable')).toBeInTheDocument();
        expect(screen.queryByText('No active predictions found.')).not.toBeInTheDocument();
        api.getOraclePredictions.mockResolvedValue({ predictions: [], total: 0 });
        fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
        expect(await screen.findByText('No active predictions found.')).toBeInTheDocument();
    });

    it('ignores an old filter response and allows paging beyond the first 100', async () => {
        const old = deferred();
        api.getOraclePredictions.mockImplementation(({ ticker, offset }) => {
            if (!ticker) return old.promise;
            return Promise.resolve({ predictions: [{ ...base, id: offset, ticker, verdict: 'pending', days_left: 2 }], total: 101, has_more: offset === '0' });
        });
        render(<Predictions />);
        const filter = await screen.findByPlaceholderText('Ticker...');
        fireEvent.change(filter, { target: { value: 'NEW' } });
        expect(await screen.findByText(/NEW/)).toBeInTheDocument();
        old.resolve({ predictions: [{ ...base, ticker: 'OLD' }], total: 1 });
        await waitFor(() => expect(screen.queryByText(/OLD/)).not.toBeInTheDocument());
        fireEvent.click(screen.getByRole('button', { name: 'Next' }));
        await waitFor(() => expect(api.getOraclePredictions).toHaveBeenLastCalledWith({ status: 'active', cohort: 'prospective', limit: '100', offset: '100', ticker: 'NEW' }));
        fireEvent.click(screen.getByRole('button', { name: 'Unscorable' }));
        await waitFor(() => expect(api.getOraclePredictions).toHaveBeenLastCalledWith({ status: 'unscorable', cohort: 'prospective', limit: '100', offset: '0', ticker: 'NEW' }));
    });

    it('puts bearish evidence under confirming signals for a PUT', () => {
        render(<PredictionCard pred={{ ...base, direction: 'PUT', signals: [
            { name: 'bear-evidence', direction: 'bearish', z_score: -2 },
            { name: 'bull-evidence', direction: 'bullish', z_score: 2 },
        ] }} />);
        fireEvent.click(screen.getByText(/SYNTH/));
        const confirming = screen.getByText('CONFIRMING SIGNALS (1)').nextElementSibling;
        const opposing = screen.getByText('OPPOSING SIGNALS (1)').nextElementSibling;
        expect(confirming).toHaveTextContent('bear-evidence');
        expect(opposing).toHaveTextContent('bull-evidence');
    });

    it('starts with prospective records and keeps legacy outcomes out of the new count', async () => {
        api.getOracleScoreboard.mockResolvedValue({
            overall: {}, models: [], by_ticker: [], calibration: { target_event: 'strict_hit' },
            recording_cohorts: {
                oracle_forward_v2: { total: 2, pending: 1, unscorable: 1, scored: 0 },
                legacy: { total: 900 },
            },
        });
        render(<Predictions />);
        expect(await screen.findByText('2 recorded · 1 pending · 1 abstained / unscorable · 0 scored')).toBeInTheDocument();
        expect(screen.getByText(/900 legacy records remain separate/)).toBeInTheDocument();
        expect(screen.getByText('HISTORICAL STRICT-HIT CALIBRATION')).toBeInTheDocument();
        expect(screen.getByText(/Partial and miss count as 0/)).toBeInTheDocument();
        await waitFor(() => expect(api.getOraclePredictions).toHaveBeenLastCalledWith(expect.objectContaining({ cohort: 'prospective' })));
        fireEvent.change(screen.getByRole('combobox', { name: 'Recording cohort' }), { target: { value: 'legacy' } });
        await waitFor(() => expect(api.getOraclePredictions).toHaveBeenLastCalledWith(expect.objectContaining({ cohort: 'legacy', offset: '0' })));
    });

    it('shows exact delayed quote provenance and an unavailable tracking reason without a return', () => {
        render(<PredictionCard pred={{ ...base, verdict: 'pending', recording_policy: 'oracle_forward_v2',
            entry_price: 100, days_left: 1, tracking_pnl: null,
            tracking_pnl_basis: 'unavailable: quote is older than the 20-minute reference limit',
            entry_price_basis: { status: 'measured', provider: 'cboe', market_at: '2026-11-10T15:05:00Z',
                registered_at: '2026-11-10T15:09:01Z', read_observed_at: '2026-11-10T15:09:02Z' },
        }} />);
        expect(screen.getByText('PROSPECTIVE OBSERVED REFERENCE')).toBeInTheDocument();
        expect(screen.getByText(/Entry quote: cboe/)).toHaveTextContent('observed 2026-11-10T15:05:00Z');
        expect(screen.getByText(/Entry quote: cboe/)).toHaveTextContent('not an executable fill');
        expect(screen.getByText(/older than the 20-minute reference limit/)).toBeInTheDocument();
        expect(screen.queryByText('REFERENCE MOVE')).not.toBeInTheDocument();
    });

    it('does not turn an abstention into a bearish forecast', () => {
        render(<PredictionCard pred={{ ...base, direction: 'NONE', verdict: 'no_data' }} />);
        expect(screen.getByText('— SYNTH')).toBeInTheDocument();
        expect(screen.queryByText('▼ SYNTH')).not.toBeInTheDocument();
    });

    it('does not display an empty calibration sample as a perfect zero Brier score', async () => {
        api.getOracleScoreboard.mockResolvedValue({ overall: {}, models: [], by_ticker: [],
            calibration: { label: 'insufficient_data', total_predictions: 0, brier_score: 0, buckets: [] } });
        render(<Predictions />);
        expect(await screen.findByText('No measured strict-hit outcomes for calibration.')).toBeInTheDocument();
        expect(screen.queryByText('Brier: 0.000')).not.toBeInTheDocument();
    });
});
