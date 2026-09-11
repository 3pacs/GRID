import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { useAsyncData } from '../hooks/useAsyncData.js';

function Harness({ fetcher, options }) {
    const { data, loading, error, refetch, stale } = useAsyncData(fetcher, options);
    return (
        <div>
            <div data-testid="loading">{String(loading)}</div>
            <div data-testid="stale">{String(stale)}</div>
            <div data-testid="error">{error ? error.message : ''}</div>
            <div data-testid="status">{error?.status ?? ''}</div>
            <div data-testid="data">{JSON.stringify(data)}</div>
            <button onClick={refetch}>refetch</button>
        </div>
    );
}

describe('useAsyncData', () => {
    it('treats a resolved { error: true } marker as a failure without setting data', async () => {
        const fetcher = vi.fn().mockResolvedValue({ error: true, status: 503, message: 'endpoint down' });

        render(<Harness fetcher={fetcher} options={{ fallback: [] }} />);

        await waitFor(() => {
            expect(screen.getByTestId('error').textContent).toBe('endpoint down');
        });
        expect(screen.getByTestId('status').textContent).toBe('503');
        expect(screen.getByTestId('data').textContent).toBe('[]');
    });

    it('falls back to a default message when the marker has none', async () => {
        const fetcher = vi.fn().mockResolvedValue({ error: true, status: 0 });

        render(<Harness fetcher={fetcher} options={{ fallback: null }} />);

        await waitFor(() => {
            expect(screen.getByTestId('error').textContent).toBe('Request failed');
        });
    });

    it('still routes a thrown error to the error state unchanged', async () => {
        const fetcher = vi.fn().mockRejectedValue(new Error('network exploded'));

        render(<Harness fetcher={fetcher} options={{ fallback: [] }} />);

        await waitFor(() => {
            expect(screen.getByTestId('error').textContent).toBe('network exploded');
        });
        expect(screen.getByTestId('data').textContent).toBe('[]');
    });

    it('sets data normally for a successful payload', async () => {
        const fetcher = vi.fn().mockResolvedValue({ value: 42 });

        render(<Harness fetcher={fetcher} options={{ fallback: null }} />);

        await waitFor(() => {
            expect(screen.getByTestId('data').textContent).toBe('{"value":42}');
        });
        expect(screen.getByTestId('error').textContent).toBe('');
    });

    it('clears the error on refetch after a marker failure', async () => {
        const fetcher = vi.fn()
            .mockResolvedValueOnce({ error: true, status: 500, message: 'boom' })
            .mockResolvedValueOnce({ value: 1 });

        render(<Harness fetcher={fetcher} options={{ fallback: null }} />);

        await waitFor(() => {
            expect(screen.getByTestId('error').textContent).toBe('boom');
        });

        fireEvent.click(screen.getByText('refetch'));

        await waitFor(() => {
            expect(screen.getByTestId('data').textContent).toBe('{"value":1}');
        });
        expect(screen.getByTestId('error').textContent).toBe('');
    });

    it('never calls the fetcher when skip is true', async () => {
        const fetcher = vi.fn().mockResolvedValue({ value: 1 });

        render(<Harness fetcher={fetcher} options={{ fallback: null, skip: true }} />);

        expect(screen.getByTestId('loading').textContent).toBe('false');
        expect(fetcher).not.toHaveBeenCalled();
    });
});
