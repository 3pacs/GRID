/**
 * Tests for the stepdad.finance composer's API surface:
 *   - api.compose()   POST /api/v1/chat/compose
 *   - api.askStream() SSE parsing for the streaming verdict
 *   - 401 handling    dispatches grid:auth-expired (see api.test.js for the
 *                      broader _fetch error-handling suite)
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { api } from '../api.js';

describe('api.compose / api.askStream / auth expiry', () => {
    beforeEach(() => {
        localStorage.clear();
        api.token = null;
        api.clearCache();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    describe('compose', () => {
        it('POSTs the question and history to /api/v1/chat/compose with JSON headers', async () => {
            const mockResponse = { spoken_reply: 'Hi', widgets: [], allocation: [] };
            const fetchMock = vi.fn().mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve(mockResponse),
            });
            vi.stubGlobal('fetch', fetchMock);

            const history = [{ role: 'user', content: 'prior question' }];
            const result = await api.compose('How are my stocks?', history);

            expect(result).toEqual(mockResponse);
            expect(fetchMock).toHaveBeenCalledTimes(1);
            const [url, opts] = fetchMock.mock.calls[0];
            expect(url).toContain('/api/v1/chat/compose');
            expect(opts.method).toBe('POST');
            expect(JSON.parse(opts.body)).toEqual({ question: 'How are my stocks?', history });
            expect(opts.headers['Content-Type']).toBe('application/json');
        });

        it('defaults history to an empty array', async () => {
            const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: () => Promise.resolve({}) });
            vi.stubGlobal('fetch', fetchMock);

            await api.compose('Should I worry about Tesla?');

            const [, opts] = fetchMock.mock.calls[0];
            expect(JSON.parse(opts.body)).toEqual({ question: 'Should I worry about Tesla?', history: [] });
        });

        it('includes the bearer token when one is set', async () => {
            api.token = 'my-token';
            const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: () => Promise.resolve({}) });
            vi.stubGlobal('fetch', fetchMock);

            await api.compose('question');

            const [, opts] = fetchMock.mock.calls[0];
            expect(opts.headers['Authorization']).toBe('Bearer my-token');
        });
    });

    describe('askStream', () => {
        function makeStreamingFetch(fullText, { split = true } = {}) {
            const bytes = new TextEncoder().encode(fullText);
            const chunks = split
                ? [bytes.slice(0, Math.floor(bytes.length * 0.6)), bytes.slice(Math.floor(bytes.length * 0.6))]
                : [bytes];
            let idx = 0;
            const reader = {
                read: vi.fn(() => {
                    if (idx < chunks.length) {
                        const value = chunks[idx];
                        idx += 1;
                        return Promise.resolve({ done: false, value });
                    }
                    return Promise.resolve({ done: true, value: undefined });
                }),
            };
            const fetchMock = vi.fn().mockResolvedValue({
                ok: true,
                status: 200,
                body: { getReader: () => reader },
            });
            return { fetchMock, reader };
        }

        it('POSTs the question/history and parses SSE delta frames incrementally into the full text', async () => {
            const sse = 'data: {"delta":"Hello"}\n\ndata: {"delta":" world"}\n\ndata: {"done":true}\n\n';
            const { fetchMock, reader } = makeStreamingFetch(sse, { split: true });
            vi.stubGlobal('fetch', fetchMock);

            const onDelta = vi.fn();
            const result = await api.askStream('What about Apple?', { history: [{ role: 'user', content: 'hi' }], onDelta });

            expect(result).toBe('Hello world');
            expect(onDelta).toHaveBeenNthCalledWith(1, 'Hello');
            expect(onDelta).toHaveBeenNthCalledWith(2, 'Hello world');
            expect(onDelta).toHaveBeenCalledTimes(2); // the final "done" frame carries no delta

            const [url, opts] = fetchMock.mock.calls[0];
            expect(url).toContain('/api/v1/chat/ask/stream');
            expect(JSON.parse(opts.body)).toEqual({ question: 'What about Apple?', history: [{ role: 'user', content: 'hi' }] });

            // Reading stops once the underlying stream reports done — no further reads happen.
            expect(reader.read).toHaveBeenCalledTimes(3);
        });

        it('stops consuming once the stream ends after the done frame, even split across chunk boundaries', async () => {
            const sse = 'data: {"delta":"A"}\n\ndata: {"delta":"B"}\n\ndata: {"done":true}\n\n';
            const { fetchMock, reader } = makeStreamingFetch(sse, { split: false });
            vi.stubGlobal('fetch', fetchMock);

            const result = await api.askStream('Q');
            expect(result).toBe('AB');
            expect(reader.read).toHaveBeenCalledTimes(2);
        });

        it('throws when the stream carries an error frame', async () => {
            const sse = 'data: {"delta":"partial"}\n\ndata: {"error":true,"message":"stream error"}\n\n';
            const { fetchMock } = makeStreamingFetch(sse, { split: false });
            vi.stubGlobal('fetch', fetchMock);

            await expect(api.askStream('Q')).rejects.toThrow('stream error');
        });

        it('throws an HTTP error when the response is not ok', async () => {
            const fetchMock = vi.fn().mockResolvedValue({ ok: false, status: 503, body: null });
            vi.stubGlobal('fetch', fetchMock);

            await expect(api.askStream('Q')).rejects.toThrow('HTTP 503');
        });
    });

    describe('401 handling', () => {
        it('clears the token and dispatches grid:auth-expired for a 401 on a non-auth endpoint', async () => {
            api.token = 'expiring-token';
            const fetchMock = vi.fn().mockResolvedValue({
                ok: false,
                status: 401,
                statusText: 'Unauthorized',
                text: () => Promise.resolve(JSON.stringify({ detail: 'Session expired' })),
            });
            vi.stubGlobal('fetch', fetchMock);

            const listener = vi.fn();
            window.addEventListener('grid:auth-expired', listener);
            try {
                const result = await api.get('/api/v1/watchlist/');

                expect(result).toEqual({ error: true, status: 401, message: 'Session expired' });
                expect(api.token).toBeNull();
                expect(listener).toHaveBeenCalledTimes(1);
            } finally {
                window.removeEventListener('grid:auth-expired', listener);
            }
        });

        it('does not dispatch grid:auth-expired for a 401 on the login endpoint', async () => {
            const fetchMock = vi.fn().mockResolvedValue({
                ok: false,
                status: 401,
                statusText: 'Unauthorized',
                text: () => Promise.resolve(JSON.stringify({ detail: 'Bad password' })),
            });
            vi.stubGlobal('fetch', fetchMock);

            const listener = vi.fn();
            window.addEventListener('grid:auth-expired', listener);
            try {
                await api._fetch('/api/v1/auth/login', { method: 'POST', body: JSON.stringify({ password: 'x' }) });
                expect(listener).not.toHaveBeenCalled();
            } finally {
                window.removeEventListener('grid:auth-expired', listener);
            }
        });
    });
});
