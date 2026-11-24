/**
 * #959 / reviews 6033672549 and 6033672651: getCurrent({force: true}) is a fresh
 * network read. A cached success must not clear a "last known" mark.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

global.fetch = vi.fn();
const { api } = await import('../api.js');

const reply = (body) => ({ ok: true, status: 200, json: () => Promise.resolve(body) });

describe('api.getCurrent force read', () => {
    beforeEach(() => {
        global.fetch.mockReset();
        api.clearCache();
    });

    it('control: without force, a second read within the TTL comes from the cache', async () => {
        global.fetch.mockResolvedValue(reply({ state: 'GROWTH' }));
        await api.getCurrent();
        await api.getCurrent();
        expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    it('with force, every read goes to the network even when a fresh cached success exists', async () => {
        global.fetch.mockResolvedValueOnce(reply({ state: 'GROWTH' }))
            .mockResolvedValueOnce({ ok: false, status: 503, statusText: 'Service Unavailable', text: () => Promise.resolve('{"detail": "down"}') });
        expect(await api.getCurrent()).toEqual({ state: 'GROWTH' });
        const second = await api.getCurrent({ force: true });
        expect(global.fetch).toHaveBeenCalledTimes(2);
        expect(second.error).toBe(true);
    });
});
