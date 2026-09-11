import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { WidgetGrid } from '../components/home/widgets.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        askStream: vi.fn(),
    },
}));

describe('VerdictCard question handling', () => {
    beforeEach(() => {
        api.askStream.mockReset();
        api.askStream.mockResolvedValue('answer');
    });

    it('streams an answer when props.question is set', async () => {
        render(<WidgetGrid widgets={[{ type: 'verdict', title: 'Your read', props: { question: 'Should dad sell Apple?' } }]} />);

        await waitFor(() => {
            expect(api.askStream).toHaveBeenCalledTimes(1);
        });
        expect(api.askStream).toHaveBeenCalledWith('Should dad sell Apple?', expect.any(Object));
    });

    it('does not stream the title when only a title is provided', async () => {
        render(<WidgetGrid widgets={[{ type: 'verdict', title: 'Should dad sell Apple?', props: {} }]} />);

        expect(await screen.findByText('Ask a question to get a read on it.')).toBeInTheDocument();
        expect(api.askStream).not.toHaveBeenCalled();
    });

    it('does not stream and shows the empty state when neither title nor question is provided', async () => {
        render(<WidgetGrid widgets={[{ type: 'verdict', props: {} }]} />);

        expect(await screen.findByText('Ask a question to get a read on it.')).toBeInTheDocument();
        expect(api.askStream).not.toHaveBeenCalled();
    });

    it('treats a blank question the same as a missing one', async () => {
        render(<WidgetGrid widgets={[{ type: 'verdict', title: 'Your read', props: { question: '   ' } }]} />);

        expect(await screen.findByText('Ask a question to get a read on it.')).toBeInTheDocument();
        expect(api.askStream).not.toHaveBeenCalled();
    });
});
