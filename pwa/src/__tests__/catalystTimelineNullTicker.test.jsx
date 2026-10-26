import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import CatalystTimeline from '../views/CatalystTimeline.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getCatalystTimeline: vi.fn(),
    },
}));

// jsdom has no ResizeObserver; the view observes its container for the D3 canvas size.
class ResizeObserverStub {
    observe() {}
    unobserve() {}
    disconnect() {}
}

// app.jsx initialises selectedTicker as null (useState(null)) and passes it explicitly, so the
// prop default '' never applies. Opening #/catalyst-timeline before any ticker is chosen used to
// throw "Cannot read properties of null (reading 'trim')" before the first request.
describe('CatalystTimeline with no selected ticker', () => {
    beforeEach(() => {
        globalThis.ResizeObserver = ResizeObserverStub;
        api.getCatalystTimeline.mockReset();
        api.getCatalystTimeline.mockResolvedValue({ status: 'ok', ticker: 'ACME', events: [], today: '2026-09-18', valuation: null });
    });

    it('renders the empty search state when selectedTicker is null and makes no request', () => {
        expect(() => render(<CatalystTimeline selectedTicker={null} />)).not.toThrow();
        expect(api.getCatalystTimeline).not.toHaveBeenCalled();
        expect(screen.getByPlaceholderText(/ticker/i)).toBeTruthy();
    });

    it('renders the empty search state when selectedTicker is undefined', () => {
        expect(() => render(<CatalystTimeline />)).not.toThrow();
        expect(api.getCatalystTimeline).not.toHaveBeenCalled();
    });

    it('still loads the timeline for a real ticker, upper-cased and trimmed', async () => {
        render(<CatalystTimeline selectedTicker="  acme " />);
        await waitFor(() => expect(api.getCatalystTimeline).toHaveBeenCalledWith('ACME'));
    });
});
