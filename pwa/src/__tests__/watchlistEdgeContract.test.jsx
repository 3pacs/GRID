import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { TrustBar } from '../views/WatchlistAnalysis.jsx';

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
});
