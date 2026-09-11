/**
 * Tests for the stepdad.finance widget registry (pwa/src/components/home/widgets.jsx).
 * Only WidgetGrid is exported, so each card is exercised through it by
 * passing a single-widget layout and asserting on what renders.
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { WidgetGrid } from '../components/home/widgets.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        askStream: vi.fn(),
        getTickerQuote: vi.fn(),
        getWatchlist: vi.fn(),
        getCurrent: vi.fn(),
        getNewsMomentum: vi.fn(),
        getSectorFlows: vi.fn(),
    },
}));

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
});

afterEach(() => {
    vi.clearAllMocks();
});

describe('WidgetGrid', () => {
    it('renders nothing for an empty or missing widget list', () => {
        const { container: empty } = render(<WidgetGrid widgets={[]} />);
        expect(empty.firstChild).toBeNull();
        const { container: missing } = render(<WidgetGrid widgets={null} />);
        expect(missing.firstChild).toBeNull();
    });

    it('renders one card per known registry id and silently ignores an unknown id', async () => {
        api.getWatchlist.mockResolvedValue({ items: [] });
        api.askStream.mockResolvedValue(undefined);
        render(
            <WidgetGrid
                widgets={[
                    { type: 'verdict', title: 'Verdict card', props: { question: '' } },
                    { type: 'watchlist', title: 'Watchlist card' },
                    { type: 'not_a_real_widget', title: 'Should not render' },
                ]}
            />
        );

        expect(screen.getByText('Verdict card')).toBeInTheDocument();
        await waitFor(() => expect(screen.getByText('Watchlist card')).toBeInTheDocument());
        expect(screen.queryByText('Should not render')).not.toBeInTheDocument();
    });
});

describe('VerdictCard (via WidgetGrid)', () => {
    it('streams tokens from askStream and renders the accumulated text', async () => {
        api.askStream.mockImplementation((question, { onDelta }) => {
            onDelta('Apple');
            onDelta('Apple looks steady this week.');
            return Promise.resolve();
        });

        render(<WidgetGrid widgets={[{ type: 'verdict', title: 'Your read', props: { question: 'How is Apple?' } }]} />);

        await waitFor(() => {
            expect(screen.getByText('Apple looks steady this week.')).toBeInTheDocument();
        });
        expect(api.askStream).toHaveBeenCalledWith('How is Apple?', expect.objectContaining({ history: [] }));
    });

    it('shows a calm error with a retry option when the stream fails', async () => {
        api.askStream.mockRejectedValue(new Error('down'));
        render(<WidgetGrid widgets={[{ type: 'verdict', props: { question: 'Anything happening?' } }]} />);

        await waitFor(() => {
            expect(screen.getByText(/couldn.t get an answer/)).toBeInTheDocument();
        });
        expect(screen.getByText('Try again')).toBeInTheDocument();
    });
});

describe('TickerPulseCard (via WidgetGrid)', () => {
    it('renders price, change and sentiment fields for a normal quote', async () => {
        api.getTickerQuote.mockResolvedValue({ price: 254.32, change_pct: 1.2, sentiment: 'bullish' });
        render(<WidgetGrid widgets={[{ type: 'ticker_pulse', props: { ticker: 'aapl' } }]} />);

        await waitFor(() => expect(screen.getByText('$254.32')).toBeInTheDocument());
        expect(screen.getByText('+1.2%')).toBeInTheDocument();
        expect(screen.getByText('Looking up')).toBeInTheDocument();
        expect(screen.getByText(/Apple is up today/)).toBeInTheDocument();
        expect(api.getTickerQuote).toHaveBeenCalledWith('AAPL');
    });

    it('shows the "market may be closed" stale state when the quote has no numeric price', async () => {
        api.getTickerQuote.mockResolvedValue({ sentiment: 'neutral' });
        render(<WidgetGrid widgets={[{ type: 'ticker_pulse', props: { ticker: 'TSLA' } }]} />);

        await waitFor(() => {
            expect(screen.getByText(/Tesla.s price isn.t updating right now/)).toBeInTheDocument();
        });
    });

    it('shows the empty state when no ticker is given, without fetching a quote', async () => {
        render(<WidgetGrid widgets={[{ type: 'ticker_pulse', props: {} }]} />);
        expect(await screen.findByText('No stock picked.')).toBeInTheDocument();
        expect(api.getTickerQuote).not.toHaveBeenCalled();
    });
});

describe('WatchlistCard (via WidgetGrid)', () => {
    it('renders saved tickers by their plain names', async () => {
        api.getWatchlist.mockResolvedValue({ items: [{ ticker: 'AAPL' }, { ticker: 'MSFT' }] });
        render(<WidgetGrid widgets={[{ type: 'watchlist' }]} />);

        await waitFor(() => expect(screen.getByText('Apple')).toBeInTheDocument());
        expect(screen.getByText('Microsoft')).toBeInTheDocument();
    });

    it('shows the empty state with no saved stocks', async () => {
        api.getWatchlist.mockResolvedValue({ items: [] });
        render(<WidgetGrid widgets={[{ type: 'watchlist' }]} />);

        await waitFor(() => expect(screen.getByText('No stocks saved yet.')).toBeInTheDocument());
    });
});

describe('MacroRegimeCard (via WidgetGrid)', () => {
    it('renders the plain-English regime sentence', async () => {
        api.getCurrent.mockResolvedValue({ regime: 'risk_off' });
        render(<WidgetGrid widgets={[{ type: 'macro_regime' }]} />);

        await waitFor(() => {
            expect(screen.getByText('Investors are nervous and playing it safe.')).toBeInTheDocument();
        });
    });

    it('shows the empty state when there is no regime read yet', async () => {
        api.getCurrent.mockResolvedValue({});
        render(<WidgetGrid widgets={[{ type: 'macro_regime' }]} />);

        await waitFor(() => {
            expect(screen.getByText(/market read isn.t ready yet/)).toBeInTheDocument();
        });
    });

    // /api/v1/regime/current returns `state`; `regime` is the older shape the
    // card was written against, so both have to render.
    it('reads the endpoint\'s own `state` field', async () => {
        api.getCurrent.mockResolvedValue({ state: 'GROWTH', data_staleness_days: 0 });
        render(<WidgetGrid widgets={[{ type: 'macro_regime' }]} />);

        await waitFor(() => {
            expect(screen.getByText('Investors are feeling confident.')).toBeInTheDocument();
        });
    });

    it('names the date the data is really from when the inputs are stale', async () => {
        api.getCurrent.mockResolvedValue({
            state: 'NEUTRAL',
            as_of_date: '2026-09-10',
            staleness_days: 0,
            data_as_of: '2026-04-03',
            data_staleness_days: 160,
        });
        render(<WidgetGrid widgets={[{ type: 'macro_regime' }]} />);

        await waitFor(() => {
            expect(screen.getByText(/Apr 3, 2026/)).toBeInTheDocument();
        });
        expect(screen.getByText(/160 days ago, not today/)).toBeInTheDocument();
    });

    it('says nothing about dates when the data is current', async () => {
        api.getCurrent.mockResolvedValue({
            state: 'NEUTRAL',
            data_as_of: '2026-09-10',
            data_staleness_days: 1,
        });
        render(<WidgetGrid widgets={[{ type: 'macro_regime' }]} />);

        await waitFor(() => {
            expect(screen.getByText(/calm and mixed/)).toBeInTheDocument();
        });
        expect(screen.queryByText(/not today/)).not.toBeInTheDocument();
    });

    it('still flags age when the data date itself is missing', async () => {
        api.getCurrent.mockResolvedValue({ state: 'NEUTRAL', data_staleness_days: 160 });
        render(<WidgetGrid widgets={[{ type: 'macro_regime' }]} />);

        await waitFor(() => {
            expect(screen.getByText(/160 days old/)).toBeInTheDocument();
        });
    });
});

describe('NewsCard (via WidgetGrid)', () => {
    it('renders a mood badge and the summary', async () => {
        api.getNewsMomentum.mockResolvedValue({ direction: 'bullish', summary: 'Tech stocks rallying.' });
        render(<WidgetGrid widgets={[{ type: 'news' }]} />);

        await waitFor(() => expect(screen.getByText('Tech stocks rallying.')).toBeInTheDocument());
        expect(screen.getByText('Looking up')).toBeInTheDocument();
    });

    it('shows the quiet/empty state with no direction or summary', async () => {
        api.getNewsMomentum.mockResolvedValue({});
        render(<WidgetGrid widgets={[{ type: 'news' }]} />);

        await waitFor(() => {
            expect(screen.getByText(/It.s quiet — no big news right now\./)).toBeInTheDocument();
        });
    });
});

describe('MoneyFlowCard (via WidgetGrid)', () => {
    it('ranks sectors by absolute stress and labels the flow direction', async () => {
        api.getSectorFlows.mockResolvedValue({
            sectors: [
                { name: 'tech', sector_stress: 0.8 },
                { name: 'energy', sector_stress: -0.3 },
            ],
        });
        render(<WidgetGrid widgets={[{ type: 'money_flow' }]} />);

        await waitFor(() => expect(screen.getByText('tech')).toBeInTheDocument());
        expect(screen.getByText('energy')).toBeInTheDocument();
        expect(screen.getByText('▲ money coming in')).toBeInTheDocument();
        expect(screen.getByText('▼ money pulling out')).toBeInTheDocument();
    });

    it('shows the empty state when nothing is moving', async () => {
        api.getSectorFlows.mockResolvedValue({ sectors: [] });
        render(<WidgetGrid widgets={[{ type: 'money_flow' }]} />);

        await waitFor(() => {
            expect(screen.getByText('Nothing notable moving right now.')).toBeInTheDocument();
        });
    });
});
