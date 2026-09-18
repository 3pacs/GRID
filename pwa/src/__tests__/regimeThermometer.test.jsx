import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import RegimeThermometer from '../components/RegimeThermometer';

describe('RegimeThermometer with an unscored regime', () => {
    it('draws no needle and says the confidence is unscored', () => {
        render(<RegimeThermometer regime={{ state: 'expansion', confidence: null, transition_probability: 0.1 }} />);
        expect(screen.queryByTestId('regime-needle')).toBeNull();
        expect(screen.getByText(/confidence unscored/)).toBeTruthy();
        expect(screen.queryByText(/%$/)).toBeNull();
    });

    it('still places the needle for a measured confidence', () => {
        render(<RegimeThermometer regime={{ state: 'expansion', confidence: 0.8, transition_probability: 0 }} />);
        expect(screen.getByTestId('regime-needle')).toBeTruthy();
        expect(screen.getByText('80%')).toBeTruthy();
    });
});
