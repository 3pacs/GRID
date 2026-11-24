import fs from 'node:fs';
import path from 'node:path';
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import IntelDashboard, { describeBriefing } from '../views/IntelDashboard.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getTrustScores: vi.fn(),
        getConvergenceAlerts: vi.fn(),
        getCrossReference: vi.fn(),
        getLatestBriefing: vi.fn(),
        getSpiderStats: vi.fn(),
    },
}));

describe('IntelDashboard truthfulness', () => {
    beforeEach(() => {
        api.getTrustScores.mockReset();
        api.getConvergenceAlerts.mockReset();
        api.getCrossReference.mockReset();
        api.getLatestBriefing.mockReset();
        api.getSpiderStats.mockReset();
    });

    it('renders honest empty states and no fabricated sources/alerts when the API returns nothing', async () => {
        api.getTrustScores.mockResolvedValue(null);
        api.getConvergenceAlerts.mockResolvedValue(null);
        api.getCrossReference.mockResolvedValue(null);
        api.getLatestBriefing.mockResolvedValue(null);
        api.getSpiderStats.mockResolvedValue(null);

        render(<IntelDashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText('No trust-scored sources available yet')).toBeInTheDocument();
        });
        expect(screen.getByText('No active convergence alerts')).toBeInTheDocument();

        // None of the previously hardcoded fixture names should appear anywhere.
        const fabricatedNames = [
            'FRED', 'BLS', 'Unusual Whales', 'Congressional Trades',
            'Dark Pool (FINRA)', 'Polymarket', 'Satellite/Alt Data', 'Reddit (Trust-Scored)',
            'Congressional buy + unusual call flow + dark pool accumulation',
            'Fed liquidity expanding but bond prices falling',
        ];
        for (const name of fabricatedNames) {
            expect(screen.queryByText(name, { exact: false })).not.toBeInTheDocument();
        }
    });

    it('renders real API data when sources and alerts are wired', async () => {
        api.getTrustScores.mockResolvedValue({
            sources: [{ name: 'FRED', trust_score: 0.9, accuracy_30d: 0.9, signals: 10, category: 'macro' }],
        });
        api.getConvergenceAlerts.mockResolvedValue({
            alerts: [{ ticker: 'NVDA', type: 'multi-signal', message: 'real signal', severity: 'high', timestamp: new Date().toISOString() }],
        });
        api.getCrossReference.mockResolvedValue(null);
        api.getLatestBriefing.mockResolvedValue(null);
        api.getSpiderStats.mockResolvedValue(null);

        render(<IntelDashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText('FRED')).toBeInTheDocument();
        });
        expect(screen.getByText('real signal')).toBeInTheDocument();
    });
});

describe('IntelDashboard source', () => {
    it('contains no Math.random or placeholder data-fabrication paths', () => {
        const src = fs.readFileSync(
            path.resolve(process.cwd(), 'src/views/IntelDashboard.jsx'),
            'utf8',
        );
        expect(src).not.toMatch(/Math\.random/i);
        expect(src).not.toMatch(/placeholder/i);
    });
});

describe('IntelDashboard briefing tile never claims LIVE', () => {
    beforeEach(() => {
        for (const fn of Object.values(api)) fn.mockReset();
        api.getTrustScores.mockResolvedValue(null);
        api.getConvergenceAlerts.mockResolvedValue(null);
        api.getCrossReference.mockResolvedValue(null);
        api.getSpiderStats.mockResolvedValue(null);
    });

    // The three shapes /api/v1/ollama/briefing/latest and the api wrapper return.
    // All are truthy objects; the old tile showed a green LIVE for each of them.
    const cases = [
        ['a failed request', { error: true, status: 500, message: 'boom' }, 'UNAVAILABLE'],
        ['no saved briefing', { content: null, message: 'No hourly briefings found' }, 'NONE'],
        ['a saved briefing', { content: 'Markets were mixed.', type: 'hourly' }, 'SAVED'],
    ];

    it.each(cases)('renders %s honestly', async (_name, payload, label) => {
        api.getLatestBriefing.mockResolvedValue(payload);
        render(<IntelDashboard onNavigate={vi.fn()} />);
        await waitFor(() => {
            expect(screen.getByTestId('briefing-state')).toHaveTextContent(label);
        });
        expect(screen.queryByText('LIVE')).not.toBeInTheDocument();
    });

    it('marks a saved briefing as age unknown, since the payload carries no generation time', () => {
        const saved = describeBriefing({ content: 'x', type: 'hourly' });
        expect(saved.saved).toBe(true);
        expect(saved.title).toMatch(/age is unknown/);
        expect(describeBriefing(null).label).toBe('UNAVAILABLE');
        expect(describeBriefing({ content: '   ' }).label).toBe('NONE');
    });
});
