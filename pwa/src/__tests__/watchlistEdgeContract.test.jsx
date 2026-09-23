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
        }} loading={false} />);

        expect(screen.getByText('Lever and actor enrichment is unavailable in this read-only view.')).toBeInTheDocument();
    });
});
