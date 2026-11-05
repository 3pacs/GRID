import React from 'react';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Surfacer from '../views/Surfacer.jsx';
import { api } from '../api.js';
vi.mock('../api.js', () => ({ api: { get: vi.fn() } }));
afterEach(cleanup);
beforeEach(() => api.get.mockReset());
describe('Surfacer manual recovery', () => {
    it('shows pending, disables repeat requests, reports timeout and recovers on Retry', async () => {
        let settle;
        api.get.mockImplementationOnce(() => new Promise(resolve => { settle = resolve; }));
        render(<Surfacer />);
        const loading = screen.getByRole('button', { name: 'Loading' });
        expect(loading).toBeDisabled();
        fireEvent.click(loading);
        expect(api.get).toHaveBeenCalledTimes(1);
        settle({ error: true, code: 'request_timeout', message: 'Surfacer took too long to load. Please retry.' });
        const retry = await screen.findByRole('button', { name: 'Retry' });
        expect(screen.getByText(/took too long/)).toBeInTheDocument();
        api.get.mockResolvedValueOnce({ candidates: [], meta: { calibration_note: 'Synthetic stale/calibration warning retained' } });
        fireEvent.click(retry);
        await screen.findByRole('button', { name: 'Refresh' });
        expect(screen.queryByText(/took too long/)).not.toBeInTheDocument();
        expect(screen.getByText('Synthetic stale/calibration warning retained')).toBeInTheDocument();
        expect(api.get).toHaveBeenCalledTimes(2);
        expect(api.get).toHaveBeenLastCalledWith('/api/v1/surfacer/candidates?limit=18&fresh_only=false&queue_missing_data=false');
    });
    it('clears loading after unexpected rejection and supports manual retry', async () => {
        api.get.mockRejectedValueOnce(new Error('synthetic rejection'));
        render(<Surfacer />);
        const retry = await screen.findByRole('button', { name: 'Retry' });
        expect(screen.getByText('Surfacer failed to load. Please retry.')).toBeInTheDocument();
        api.get.mockResolvedValueOnce({ candidates: [], meta: {} });
        fireEvent.click(retry);
        await waitFor(() => expect(screen.getByRole('button', { name: 'Refresh' })).toBeEnabled());
        expect(api.get).toHaveBeenCalledTimes(2);
    });
});
