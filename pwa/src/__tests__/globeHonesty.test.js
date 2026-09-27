import { describe, expect, it } from 'vitest';
import { activityColorSafe, fmtActivity, fmtGdpSignal, NO_DATA_COLOR } from '../views/Globe.jsx';

// Before this change a country with no measurable inputs still rendered a
// fake 50% "activity" reading (the backend's `0.5` placeholder), colored the
// same neutral grey as a genuinely neutral country. These helpers must treat
// a missing score as its own state, never as a number.
describe('Globe activity honesty helpers', () => {
    it('fmtActivity renders N/A for a null score, never 0% or a computed value', () => {
        expect(fmtActivity(null)).toBe('N/A');
        expect(fmtActivity(undefined)).toBe('N/A');
        expect(fmtActivity(0.5)).toBe('50%');
        expect(fmtActivity(0)).toBe('0%');
    });

    it('activityColorSafe uses the no-data color for a null score, not the mid-scale color', () => {
        expect(activityColorSafe(null)).toBe(NO_DATA_COLOR);
        expect(activityColorSafe(undefined)).toBe(NO_DATA_COLOR);
        // A real 0.5 reading still gets the normal scale color, and it must
        // differ from the no-data color so the two are visually distinct.
        expect(activityColorSafe(0.5)).not.toBe(NO_DATA_COLOR);
    });

    it('fmtGdpSignal renders a readable "no data" instead of the raw no_data token', () => {
        expect(fmtGdpSignal('no_data')).toBe('no data');
        expect(fmtGdpSignal(null)).toBe('no data');
        expect(fmtGdpSignal('growth')).toBe('growth');
        expect(fmtGdpSignal('stable')).toBe('stable');
    });
});
