import useStore from './store.js';
import {
    mockBriefing,
    mockCompare,
    mockCorrelations,
    mockEclipses,
    mockEphemeris,
    mockLunar,
    mockNakshatra,
    mockOverview,
    mockRetrogrades,
    mockSolar,
    mockTimeline,
} from './mockData.js';

class AstroGridApiError extends Error {
    constructor(status, message, detail) {
        super(message);
        this.status = status;
        this.detail = detail;
    }
}

class AstroGridApi {
    get config() {
        return useStore.getState();
    }

    get baseUrl() {
        return (this.config.apiBaseUrl || '').replace(/\/$/, '');
    }

    get token() {
        return this.config.apiToken;
    }

    get mode() {
        return this.config.apiMode;
    }

    get mockResponses() {
        return {
            '/api/v1/astrogrid/overview': mockOverview,
            '/api/v1/astrogrid/ephemeris': mockEphemeris,
            '/api/v1/astrogrid/correlations': mockCorrelations,
            '/api/v1/astrogrid/timeline': mockTimeline,
            '/api/v1/astrogrid/briefing': mockBriefing,
            '/api/v1/astrogrid/retrogrades': mockRetrogrades,
            '/api/v1/astrogrid/eclipses': mockEclipses,
            '/api/v1/astrogrid/nakshatra': mockNakshatra,
            '/api/v1/astrogrid/lunar': mockLunar,
            '/api/v1/astrogrid/solar': mockSolar,
            '/api/v1/astrogrid/compare': mockCompare,
        };
    }

    async _fetch(path, options = {}) {
        if (this.mode !== 'live') {
            useStore.getState().setConnectionState('demo', 'Using bundled celestial demo data.');
            const normalizedPath = path.split('?')[0];
            return this.mockResponses[normalizedPath] || {};
        }

        const headers = { 'Content-Type': 'application/json', ...options.headers };
        if (this.token) {
            headers['Authorization'] = `Bearer ${this.token}`;
        }

        let response;

        try {
            response = await fetch(`${this.baseUrl}${path}`, {
                ...options,
                headers,
            });
        } catch (error) {
            useStore.getState().setConnectionState('offline', 'Could not reach the live AstroGrid backend.');
            throw new AstroGridApiError(0, 'Network error while contacting AstroGrid backend.', error);
        }

        if (!response.ok) {
            const body = await response.json().catch(() => ({}));
            const message = body.detail || response.statusText;

            if (response.status === 401) {
                useStore.getState().setConnectionState('unauthorized', 'Backend rejected the current AstroGrid token.');
            } else {
                useStore.getState().setConnectionState('error', `Backend error: ${message}`);
            }

            throw new AstroGridApiError(response.status, message, body);
        }

        useStore.getState().setConnectionState('connected', `Connected to ${this.baseUrl || 'AstroGrid backend'}.`);
        return response.json();
    }

    async ping() {
        return this._fetch('/api/v1/astrogrid/overview');
    }

    // Celestial overview — current state of all bodies
    async getCelestialOverview() {
        return this._fetch('/api/v1/astrogrid/overview');
    }

    // Ephemeris for a specific date
    async getEphemeris(date) {
        const params = date ? `?date=${date}` : '';
        return this._fetch(`/api/v1/astrogrid/ephemeris${params}`);
    }

    // Correlations between celestial events and market data
    async getCorrelations(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/astrogrid/correlations${qs ? '?' + qs : ''}`);
    }

    // Timeline of upcoming celestial events
    async getTimeline(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/astrogrid/timeline${qs ? '?' + qs : ''}`);
    }

    // AI-generated celestial briefing
    async getBriefing() {
        return this._fetch('/api/v1/astrogrid/briefing');
    }

    // Current and upcoming retrogrades
    async getRetrogrades() {
        return this._fetch('/api/v1/astrogrid/retrogrades');
    }

    // Upcoming eclipses
    async getEclipses() {
        return this._fetch('/api/v1/astrogrid/eclipses');
    }

    // Nakshatra (lunar mansion) data
    async getNakshatra() {
        return this._fetch('/api/v1/astrogrid/nakshatra');
    }

    // Lunar calendar and phase data
    async getLunarCalendar() {
        return this._fetch('/api/v1/astrogrid/lunar');
    }

    // Solar activity (sunspots, flares, etc.)
    async getSolarActivity() {
        return this._fetch('/api/v1/astrogrid/solar');
    }

    // Compare celestial state between two dates
    async compareDates(date1, date2) {
        return this._fetch(`/api/v1/astrogrid/compare?date1=${date1}&date2=${date2}`);
    }
}

export const api = new AstroGridApi();
export default api;
