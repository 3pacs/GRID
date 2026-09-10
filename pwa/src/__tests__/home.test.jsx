/**
 * Tests for the stepdad.finance home page (pwa/src/views/Home.jsx): the
 * plain-language composer that turns a typed request into either a rendered
 * dashboard, a price-alert confirmation, a "can't do that yet" gap card, or
 * a calm error message.
 */
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import Home from '../views/Home.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        compose: vi.fn(),
        listAlerts: vi.fn(),
        cancelAlert: vi.fn(),
        setCapabilityPing: vi.fn(),
    },
}));

// WidgetGrid pulls its own data through further api calls (getTickerQuote,
// getWatchlist, ...) — irrelevant to Home's own submit/branch logic, so it's
// stubbed at the component boundary, same as widgets.jsx gets its own suite.
vi.mock('../components/home/widgets.jsx', () => ({
    WidgetGrid: ({ widgets }) => (
        <div data-testid="widget-grid">{JSON.stringify(widgets)}</div>
    ),
}));

beforeEach(() => {
    window.HTMLElement.prototype.scrollIntoView = vi.fn();
    api.compose.mockReset();
    api.listAlerts.mockReset();
    api.cancelAlert.mockReset();
    api.setCapabilityPing.mockReset();
    api.listAlerts.mockResolvedValue({ alerts: [] });
    api.cancelAlert.mockResolvedValue({});
    api.setCapabilityPing.mockResolvedValue({});
});

afterEach(() => {
    vi.clearAllMocks();
});

function ask(text) {
    const input = screen.getByPlaceholderText('Ask me anything…');
    fireEvent.change(input, { target: { value: text } });
    fireEvent.click(screen.getByText('Show me'));
}

describe('Home — normal compose flow', () => {
    it('renders the returned widgets and spoken reply on a normal response', async () => {
        api.compose.mockResolvedValue({
            spoken_reply: 'Apple is up today.',
            widgets: [{ type: 'ticker_pulse', title: 'Apple', props: { ticker: 'AAPL' } }],
            allocation: [{ ticker: 'AAPL', weight: 0.5 }],
        });

        render(<Home />);
        ask('How is Apple doing?');

        await waitFor(() => {
            expect(screen.getByText('Apple is up today.')).toBeInTheDocument();
        });
        expect(api.compose).toHaveBeenCalledWith('How is Apple doing?', []);
        expect(screen.getByTestId('widget-grid')).toHaveTextContent('ticker_pulse');
        expect(screen.getByText('Apple 50%')).toBeInTheDocument();
        expect(screen.getByText('← Start over')).toBeInTheDocument();
    });

    it('carries prior turns as history on a follow-up question', async () => {
        api.compose.mockResolvedValue({ spoken_reply: 'First answer.', widgets: [], allocation: [] });
        render(<Home />);
        ask('First question?');
        await waitFor(() => expect(screen.getByText('First answer.')).toBeInTheDocument());

        api.compose.mockResolvedValue({ spoken_reply: 'Second answer.', widgets: [], allocation: [] });
        const bottomInput = screen.getByPlaceholderText('Ask me anything else…');
        fireEvent.change(bottomInput, { target: { value: 'Second question?' } });
        fireEvent.click(screen.getByText('Ask'));

        await waitFor(() => expect(screen.getByText('Second answer.')).toBeInTheDocument());
        expect(api.compose).toHaveBeenLastCalledWith('Second question?', [
            { role: 'user', content: 'First question?' },
            { role: 'assistant', content: 'First answer.' },
        ]);
    });
});

describe('Home — alert_created branch', () => {
    it('shows the confirmation banner and refreshes the alerts list', async () => {
        api.compose.mockResolvedValue({
            alert_created: true,
            spoken_reply: "I'll text you when Apple hits $250.",
        });

        render(<Home />);
        await waitFor(() => expect(api.listAlerts).toHaveBeenCalledTimes(1));
        ask('Tell me when Apple hits $250');

        await waitFor(() => {
            expect(screen.getByText(/I'll text you when Apple hits \$250\./)).toBeInTheDocument();
        });
        await waitFor(() => expect(api.listAlerts).toHaveBeenCalledTimes(2));
    });
});

describe('Home — cannot_fulfill branch', () => {
    it('shows the gap card and records the ping opt-in answer', async () => {
        api.compose.mockResolvedValue({
            cannot_fulfill: true,
            spoken_reply: "I can't do that yet.",
            request_id: 'req-1',
        });

        render(<Home />);
        ask('Can you trade options for me?');

        await waitFor(() => expect(screen.getByText("I can't do that yet.")).toBeInTheDocument());
        expect(screen.getByText('Yes, ping me')).toBeInTheDocument();
        expect(screen.getByText('No thanks')).toBeInTheDocument();

        fireEvent.click(screen.getByText('Yes, ping me'));

        await waitFor(() => {
            expect(api.setCapabilityPing).toHaveBeenCalledWith('req-1', true);
        });
        expect(screen.getByText(/I’ll let you know the moment it’s ready/)).toBeInTheDocument();
    });

    it('records a "no" answer without pinging', async () => {
        api.compose.mockResolvedValue({
            cannot_fulfill: true,
            spoken_reply: "I can't do that yet.",
            request_id: 'req-2',
        });

        render(<Home />);
        ask('Can you day-trade for me?');
        await waitFor(() => expect(screen.getByText('No thanks')).toBeInTheDocument());

        fireEvent.click(screen.getByText('No thanks'));

        await waitFor(() => {
            expect(api.setCapabilityPing).toHaveBeenCalledWith('req-2', false);
        });
        expect(screen.getByText(/No problem — I’ll still build it for you\./)).toBeInTheDocument();
    });
});

describe('Home — error branches', () => {
    it('shows a calm message and restores the input when the response carries res.error', async () => {
        api.compose.mockResolvedValue({ error: true, status: 500, message: 'boom' });

        render(<Home />);
        ask('What is going on with the market?');

        await waitFor(() => {
            expect(screen.getByText(/I couldn’t do that just now\. Please try again\./)).toBeInTheDocument();
        });
        expect(screen.getByPlaceholderText('Ask me anything…').value).toBe('What is going on with the market?');
    });

    it('shows a calm message and restores the input when compose() throws', async () => {
        api.compose.mockRejectedValue(new Error('network down'));

        render(<Home />);
        ask('Is Tesla crashing?');

        await waitFor(() => {
            expect(screen.getByText(/I couldn’t reach the service\. Please try again in a moment\./)).toBeInTheDocument();
        });
        expect(screen.getByPlaceholderText('Ask me anything…').value).toBe('Is Tesla crashing?');
    });
});

describe('Home — suggestion chips', () => {
    it('submits the tapped suggestion verbatim', async () => {
        api.compose.mockResolvedValue({ spoken_reply: 'Sure.', widgets: [], allocation: [] });

        render(<Home />);
        fireEvent.click(screen.getByText('How are my stocks doing?'));

        await waitFor(() => {
            expect(api.compose).toHaveBeenCalledWith('How are my stocks doing?', []);
        });
    });
});

describe('Home — alerts panel', () => {
    it('lists active alerts and cancels one on request', async () => {
        api.listAlerts.mockResolvedValue({
            alerts: [
                { id: 'a1', ticker: 'AAPL', direction: 'above', threshold: 250, active: true },
                { id: 'a2', ticker: 'TSLA', direction: 'below', threshold: 180, active: false },
            ],
        });

        render(<Home />);

        await waitFor(() => {
            expect(screen.getByText(/Apple goes above \$250/)).toBeInTheDocument();
        });
        // Inactive alert is filtered out of the panel.
        expect(screen.queryByText(/Tesla drops below/)).not.toBeInTheDocument();

        api.listAlerts.mockResolvedValue({ alerts: [] });
        fireEvent.click(screen.getByText('Cancel'));

        expect(api.cancelAlert).toHaveBeenCalledWith('a1');
        await waitFor(() => {
            expect(screen.queryByText(/Apple goes above/)).not.toBeInTheDocument();
        });
    });

    it('renders nothing when there are no active alerts', async () => {
        api.listAlerts.mockResolvedValue({ alerts: [] });
        render(<Home />);
        await waitFor(() => expect(api.listAlerts).toHaveBeenCalled());
        expect(screen.queryByText(/I’m watching these for you/)).not.toBeInTheDocument();
    });
});
