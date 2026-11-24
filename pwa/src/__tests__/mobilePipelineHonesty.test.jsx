/**
 * #959 M1: the mobile Pipeline card used to print a green "OK" whenever the
 * database was connected, even with zero sources or a pipeline-health read
 * that the route itself marked availability: "unavailable".
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import MobileDashboard from '../views/mobile/MobileDashboard.jsx';

vi.mock('../api.js', () => ({
    api: {
        getStatus: vi.fn(), getCurrent: vi.fn(), getPipelineHealth: vi.fn(),
        get100xOpportunities: vi.fn(), getSources: vi.fn(), getJournal: vi.fn(),
        getOperatorIssues: vi.fn(), getSnapshotLatest: vi.fn(),
    },
}));
vi.mock('../store.js', () => ({ default: vi.fn((sel) => (sel ? sel({ userRole: 'admin' }) : { userRole: 'admin' })) }));

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    api.getStatus.mockResolvedValue({ database: { connected: true } });
    api.getCurrent.mockResolvedValue({ state: 'NEUTRAL', confidence: 0.5 });
    api.get100xOpportunities.mockResolvedValue({ opportunities: [] });
    api.getSources.mockResolvedValue({ sources: [] });
    api.getJournal.mockResolvedValue({ entries: [] });
    api.getOperatorIssues.mockResolvedValue({ issues: [] });
    api.getSnapshotLatest.mockResolvedValue({ snapshots: [] });
});

describe('MobileDashboard pipeline card', () => {
    it('does not say OK when pipeline health is unavailable, and shows the route reason', async () => {
        api.getPipelineHealth.mockResolvedValue({
            availability: 'unavailable', stale_reason: 'pipeline query failed',
            summary: { total_sources: 0, healthy: 0, broken: 0, stale: 0 },
        });
        render(<MobileDashboard onNavigate={vi.fn()} />);
        expect(await screen.findByText('pipeline query failed')).toBeInTheDocument();
        expect(screen.getByText('N/A')).toBeInTheDocument();
        expect(screen.queryByText('OK')).not.toBeInTheDocument();
    });

    it('does not say OK with zero sources just because the database is up', async () => {
        api.getPipelineHealth.mockResolvedValue({
            availability: 'available', summary: { total_sources: 0, healthy: 0, broken: 0, stale: 0 },
        });
        render(<MobileDashboard onNavigate={vi.fn()} />);
        await waitFor(() => expect(screen.getByText('Pipeline')).toBeInTheDocument());
        expect(screen.queryByText('OK')).not.toBeInTheDocument();
    });
});
