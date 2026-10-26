/**
 * D7: the session JWT must never appear in a URL. EventSource streams use a
 * single-use ticket from POST /api/v1/auth/stream-ticket; briefing audio is
 * fetched with the Authorization header and played from an object URL.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const SESSION = 'session.jwt.value';

const localStorageMock = (() => {
    let store = {};
    return {
        getItem: vi.fn((key) => store[key] || null),
        setItem: vi.fn((key, val) => { store[key] = val; }),
        removeItem: vi.fn((key) => { delete store[key]; }),
        clear: vi.fn(() => { store = {}; }),
    };
})();
Object.defineProperty(global, 'localStorage', { value: localStorageMock });
Object.defineProperty(global, 'window', {
    value: {
        location: { origin: 'http://localhost:8000', protocol: 'http:', host: 'localhost:8000', hash: '' },
        localStorage: localStorageMock,
    },
});

const eventSources = [];
class MockEventSource {
    constructor(url) {
        this.url = url;
        this.listeners = {};
        this.close = vi.fn();
        eventSources.push(this);
    }
    addEventListener(name, fn) { this.listeners[name] = fn; }
}
global.EventSource = MockEventSource;
global.fetch = vi.fn();

const { api } = await import('../api.js');

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

function ticketResponse(ticket = 'one-use-ticket') {
    return { ok: true, json: () => Promise.resolve({ ticket, expires_in: 60, path: 'x' }) };
}

describe('stream and audio auth never put the session token in a URL', () => {
    beforeEach(() => {
        localStorageMock.clear();
        global.fetch.mockReset();
        eventSources.length = 0;
        api.token = SESSION;
    });

    it('gold stream POSTs for a ticket with the Bearer header, then opens with ?ticket=', async () => {
        global.fetch.mockResolvedValueOnce(ticketResponse('t-gold'));
        const handle = api.streamDadTickerGold('AAPL', { refreshFinviz: true });
        expect(typeof handle.close).toBe('function');
        await flush();

        const [url, init] = global.fetch.mock.calls[0];
        expect(url).toBe('http://localhost:8000/api/v1/auth/stream-ticket');
        expect(init.method).toBe('POST');
        expect(init.headers.Authorization).toBe(`Bearer ${SESSION}`);
        expect(JSON.parse(init.body)).toEqual({ path: '/api/v1/dad/ticker/AAPL/gold/stream' });

        expect(eventSources).toHaveLength(1);
        const streamUrl = eventSources[0].url;
        expect(streamUrl).toContain('/api/v1/dad/ticker/AAPL/gold/stream?');
        expect(streamUrl).toContain('ticket=t-gold');
        expect(streamUrl).toContain('refresh_finviz=true');
        expect(streamUrl).not.toContain(SESSION);
        expect(streamUrl).not.toMatch(/[?&]token=/);
    });

    it('falls back via onError without opening a stream when no ticket is issued', async () => {
        global.fetch.mockResolvedValueOnce({ ok: false, status: 401, json: () => Promise.resolve({}) });
        const onError = vi.fn();
        api.streamDadTickerGold('AAPL', { onError });
        await flush();
        expect(onError).toHaveBeenCalledWith(expect.objectContaining({ type: 'ticket' }));
        expect(eventSources).toHaveLength(0);
    });

    it('closing before the ticket arrives never opens the stream', async () => {
        global.fetch.mockResolvedValueOnce(ticketResponse());
        const handle = api.streamDadTickerGold('AAPL');
        handle.close();
        await flush();
        expect(eventSources).toHaveLength(0);
    });

    it('briefing audio is fetched with the Authorization header, not a URL token', async () => {
        const blob = { size: 3 };
        global.fetch.mockResolvedValueOnce({
            ok: true,
            headers: { get: () => 'audio/mpeg' },
            blob: () => Promise.resolve(blob),
        });
        const createObjectURL = vi.fn(() => 'blob:briefing');
        global.URL.createObjectURL = createObjectURL;

        const url = await api.loadFlowBriefingAudio('briefing_2026-09-25.mp3');
        expect(url).toBe('blob:briefing');
        const [requested, init] = global.fetch.mock.calls[0];
        expect(requested).toBe('http://localhost:8000/api/v1/flows/briefing/audio/briefing_2026-09-25.mp3');
        expect(requested).not.toContain('token');
        expect(init.headers.Authorization).toBe(`Bearer ${SESSION}`);
        expect(createObjectURL).toHaveBeenCalledWith(blob);
        expect(api.getFlowBriefingAudioUrl).toBeUndefined();
    });

    it('briefing GET is read-only and generation is an explicit POST', async () => {
        global.fetch.mockResolvedValue({ ok: true, json: () => Promise.resolve({ status: 'not_generated' }) });
        await api.getFlowBriefing();
        await api.generateFlowBriefing(true);
        const [getUrl, getInit] = global.fetch.mock.calls[0];
        const [postUrl, postInit] = global.fetch.mock.calls[1];
        expect(getUrl).toBe('http://localhost:8000/api/v1/flows/briefing');
        expect(getInit.method ?? 'GET').toBe('GET');
        expect(postUrl).toBe('http://localhost:8000/api/v1/flows/briefing?audio=true');
        expect(postInit.method).toBe('POST');
    });
});
