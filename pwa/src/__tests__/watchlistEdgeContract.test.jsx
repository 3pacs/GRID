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

    it('presents persisted trust without implying calibrated confidence', () => {
        const edgeData = {
            status: 'partial',
            congressional: [{ member: 'A', action: 'BUY', trust_score: null }],
            convergence: {
                status: 'detected', signal_type: 'BUY', direction: 'bullish',
                source_count: 3, non_null_trust_score_count: 0, confidence: null,
                persisted_trust_mean: null, confidence_basis: 'unverified_score_provenance',
            },
        };
        const { rerender } = render(<InsiderEdgePanel edgeData={edgeData} loading={false} />);
        expect(screen.getByText('3 sources bullish')).toBeInTheDocument();
        expect(screen.getByText('unscored · score provenance unverified')).toBeInTheDocument();
        expect(screen.queryByText(/50%/)).not.toBeInTheDocument();

        rerender(<InsiderEdgePanel edgeData={{
            ...edgeData,
            convergence: {
                ...edgeData.convergence, non_null_trust_score_count: 3,
                persisted_trust_mean: 0,
            },
        }} loading={false} />);
        expect(screen.getByText('0.00 · score provenance unverified')).toBeInTheDocument();
        expect(screen.queryByText(/0%/)).not.toBeInTheDocument();

        rerender(<InsiderEdgePanel edgeData={{
            ...edgeData,
            convergence: {
                ...edgeData.convergence, non_null_trust_score_count: 3,
                persisted_trust_mean: 0.5,
            },
        }} loading={false} />);
        expect(screen.getByText('0.50 · score provenance unverified')).toBeInTheDocument();
        expect(screen.queryByText(/50%/)).not.toBeInTheDocument();

        // During a mixed-version cutover, old workers may send only the legacy
        // numeric confidence. Never turn that unverified number into a percent.
        rerender(<InsiderEdgePanel edgeData={{
            ...edgeData,
            convergence: { status: 'detected', source_count: 3, direction: 'bullish', confidence: 0.5 },
        }} loading={false} />);
        expect(screen.getByText('unavailable · score provenance unverified')).toBeInTheDocument();
        expect(screen.queryByText(/50%/)).not.toBeInTheDocument();
    });
});
