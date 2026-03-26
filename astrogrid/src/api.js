/**
 * AstroGrid API client.
 * All fetch calls to /api/v1/astrogrid/* go through here.
 */

class AstroGridApiError extends Error {
    constructor(status, message, detail) {
        super(message);
        this.status = status;
        this.detail = detail;
    }
}

class AstroGridApi {
    constructor() {
        this.baseUrl = window.location.origin;
    }

    get token() {
        return localStorage.getItem('grid_token');
    }

    async _fetch(path, options = {}) {
        const headers = { 'Content-Type': 'application/json', ...options.headers };
        if (this.token) {
            headers['Authorization'] = `Bearer ${this.token}`;
        }

        const response = await fetch(`${this.baseUrl}${path}`, {
            ...options,
            headers,
        });

        if (!response.ok) {
            const body = await response.json().catch(() => ({}));
            const message = body.detail || response.statusText;

            if (response.status === 401) {
                localStorage.removeItem('grid_token');
                window.location.href = '/';
            }

            throw new AstroGridApiError(response.status, message, body);
        }

        return response.json();
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
