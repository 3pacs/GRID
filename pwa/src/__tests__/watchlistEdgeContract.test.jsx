import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { InsiderEdgePanel, TrustBar } from '../views/WatchlistAnalysis.jsx';

describe('watchlist edge contract', () => {
    it('labels an absent confidence as unscored instead of rendering a numeric bar', () => {
        render(<TrustBar score={null} />);

        expect(screen.getByText('unscored')).toBeInTheDocument();
        expect(screen.queryByTestId('trustbar-fill')).not.toBeInTheDocument();
    });

    it('preserves a measured zero as a numeric result', () => {
        render(<TrustBar score={0} />);

        expect(screen.getByText('0')).toBeInTheDocument();
        expect(screen.getByTestId('trustbar-fill')).toBeInTheDocument();
    });

    it('shows an unavailable persisted-data state instead of hiding an empty edge response', () => {
        render(<InsiderEdgePanel edgeData={{ status: 'unavailable' }} loading={false} />);

        expect(screen.getByText('INSIDER EDGE UNAVAILABLE')).toBeInTheDocument();
        expect(screen.getByText('Persisted intelligence data is unavailable.')).toBeInTheDocument();
    });

    it('labels intentionally unavailable enrichment on a partial persisted response', () => {
        render(<InsiderEdgePanel edgeData={{
            status: 'partial', congressional: [{ member: 'A', action: 'BUY' }],
            availability: { lever_pullers: { status: 'unavailable' } },
        }} loading={false} />);

        expect(screen.getByText('Lever and actor enrichment is unavailable in this read-only view.')).toBeInTheDocument();
    });

    it('distinguishes unscored convergence from a measured zero in the edge panel', () => {
        const edgeData = {
            status: 'partial',
            congressional: [{ member: 'A', action: 'BUY', trust_score: null }],
            convergence: {
                status: 'detected', signal_type: 'BUY', direction: 'bullish',
                source_count: 3, scored_source_count: 0, confidence: null,
                confidence_basis: 'unscored',
            },
        };
        const { rerender } = render(<InsiderEdgePanel edgeData={edgeData} loading={false} />);
        expect(screen.getByText(/3 sources bullish.*unscored/)).toBeInTheDocument();
        expect(screen.getAllByText('unscored')).toHaveLength(1);
        expect(screen.queryByText(/50%/)).not.toBeInTheDocument();

        rerender(<InsiderEdgePanel edgeData={{
            ...edgeData,
            convergence: {
                ...edgeData.convergence, scored_source_count: 3, confidence: 0,
                confidence_basis: 'mean_trust_of_scored_sources',
            },
        }} loading={false} />);
        expect(screen.getByText(/3 sources bullish.*0%/)).toBeInTheDocument();
        expect(screen.getByText('0')).toBeInTheDocument();
        expect(screen.queryByText('unscored')).not.toBeInTheDocument();
    });
});
