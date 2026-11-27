/**
 * #959 probe finding: the Technology sector page showed Cloudflare's whole 524 HTML page as its error text.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { httpErrorMessage, looksLikeMarkup } from '../utils/httpError.js';

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
global.fetch = vi.fn();
const { api } = await import('../api.js');

const CLOUDFLARE_524 = '<!DOCTYPE html>\n<!--[if lt IE 7]> <html class="no-js ie6 oldie" lang="en-US"> <![endif]-->\n<head><title>stepdad.finance | 524: A timeout occurred</title></head><body><span class="code-label">Error code 524</span></body></html>';

function failing(status, body, contentType) {
    return Promise.resolve({
        ok: false, status, statusText: 'x',
        headers: { get: (k) => (k.toLowerCase() === 'content-type' ? contentType : null) },
        text: async () => body,
    });
}

describe('httpErrorMessage', () => {
    it('never returns markup: a gateway HTML page becomes a sentence', () => {
        const m = httpErrorMessage(524, 'Unknown', CLOUDFLARE_524, 'text/html; charset=UTF-8');
        expect(m).toBe('The server took too long to answer (HTTP 524). The request may still be running; try again shortly.');
        expect(m).not.toMatch(/[<>]|DOCTYPE/i);
    });

    it('classifies timeouts and outages by status, and any other markup generically', () => {
        for (const s of [504, 522, 523, 524]) expect(httpErrorMessage(s, '', '<html></html>')).toMatch(/took too long.*HTTP/);
        for (const s of [502, 503, 521, 530]) expect(httpErrorMessage(s, '', '<html></html>')).toMatch(/unavailable.*HTTP/);
        expect(httpErrorMessage(418, '', '<!doctype html><p>x</p>')).toBe('The server returned an error page instead of data (HTTP 418).');
    });

    it('recognises markup by content type even without a doctype, and by leading comment or xml prolog', () => {
        expect(looksLikeMarkup('anything', 'text/html')).toBe(true);
        expect(looksLikeMarkup('<?xml version="1.0"?><e/>')).toBe(true);
        expect(looksLikeMarkup('  <!-- x --><p/>')).toBe(true);
        expect(looksLikeMarkup('plain text with a < sign')).toBe(false);
        expect(looksLikeMarkup('')).toBe(false);
    });

    it('keeps a plain-text body as before, and falls back to the status text or code', () => {
        expect(httpErrorMessage(500, 'Server Error', 'Internal failure while computing')).toBe('Internal failure while computing');
        expect(httpErrorMessage(500, 'Server Error', '')).toBe('Server Error');
        expect(httpErrorMessage(500, '', '')).toBe('HTTP 500');
    });
});

describe('api fetch wrapper uses it', () => {
    beforeEach(() => { global.fetch.mockReset(); api.clearCache(); });

    it('a 524 HTML page is a readable error, not markup', async () => {
        global.fetch.mockReturnValue(failing(524, CLOUDFLARE_524, 'text/html'));
        const r = await api.getSectorDetail('Technology');
        expect(r.error).toBe(true);
        expect(r.status).toBe(524);
        expect(r.message).toMatch(/took too long to answer \(HTTP 524\)/);
        expect(r.message).not.toMatch(/[<>]/);
    });

    it('a JSON error keeps its detail and a plain-text error keeps its text', async () => {
        global.fetch.mockReturnValueOnce(failing(404, JSON.stringify({ detail: "Sector 'x' not found" }), 'application/json'));
        expect((await api.getSectorDetail('x')).message).toBe("Sector 'x' not found");
        global.fetch.mockReturnValueOnce(failing(500, 'plain failure', 'text/plain'));
        api.clearCache();
        expect((await api.getSectorDetail('y')).message).toBe('plain failure');
    });
});
