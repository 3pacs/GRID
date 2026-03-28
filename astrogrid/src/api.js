import useStore from './store.js';
import {
    ASTROGRID_ENDPOINTS,
    buildAstrogridBriefingCandidates,
    buildAstrogridCompareCandidates,
    buildAstrogridCorrelationsCandidates,
    buildAstrogridEphemerisPath,
    buildAstrogridLunarCalendarCandidates,
    buildAstrogridRetrogradeCandidates,
    buildAstrogridSnapshotPath,
    buildAstrogridSolarActivityCandidates,
    buildAstrogridTimelineCandidates,
    fetchFirstAstrogridCandidate,
} from './lib/endpoints.js';
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
            [ASTROGRID_ENDPOINTS.overview]: mockOverview,
            [ASTROGRID_ENDPOINTS.snapshot]: mockOverview,
            [ASTROGRID_ENDPOINTS.signals]: mockOverview,
            [ASTROGRID_ENDPOINTS.signalsBriefing]: mockBriefing,
            [ASTROGRID_ENDPOINTS.ephemeris]: mockEphemeris,
            [ASTROGRID_ENDPOINTS.correlations]: mockCorrelations,
            [ASTROGRID_ENDPOINTS.timeline]: mockTimeline,
            [ASTROGRID_ENDPOINTS.briefing]: mockBriefing,
            [ASTROGRID_ENDPOINTS.narrative]: mockBriefing,
            [ASTROGRID_ENDPOINTS.retrograde]: mockRetrogrades,
            [ASTROGRID_ENDPOINTS.retrogrades]: mockRetrogrades,
            [ASTROGRID_ENDPOINTS.eclipses]: mockEclipses,
            [ASTROGRID_ENDPOINTS.nakshatra]: mockNakshatra,
            [ASTROGRID_ENDPOINTS.lunar]: mockLunar,
            [ASTROGRID_ENDPOINTS.lunarCalendar]: mockLunar,
            [ASTROGRID_ENDPOINTS.solar]: mockSolar,
            [ASTROGRID_ENDPOINTS.solarActivity]: mockSolar,
            [ASTROGRID_ENDPOINTS.compare]: mockCompare,
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
            headers.Authorization = `Bearer ${this.token}`;
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

    async _fetchFirst(candidates, options = {}) {
        return fetchFirstAstrogridCandidate((path, requestOptions) => this._fetch(path, requestOptions), candidates, {
            requestOptions: options,
            fallbackError: new AstroGridApiError(500, 'No AstroGrid endpoint candidates succeeded', {}),
        });
    }

    async ping() {
        return this._fetch(ASTROGRID_ENDPOINTS.overview);
    }

    // Celestial overview — current state of all bodies
    async getCelestialOverview() {
        return this._fetch(ASTROGRID_ENDPOINTS.overview);
    }

    // Snapshot contract for AstroGrid surfaces
    async getSnapshot(date) {
        return this._fetch(buildAstrogridSnapshotPath(date));
    }

    // Celestial signal feed (contract-safe primary data source)
    async getCelestialSignals() {
        return this._fetch(ASTROGRID_ENDPOINTS.signals);
    }

    // Celestial briefing from signal feed
    async getCelestialBriefing() {
        return this._fetch(ASTROGRID_ENDPOINTS.signalsBriefing);
    }

    // Ephemeris for a specific date
    async getEphemeris(date) {
        return this._fetch(buildAstrogridEphemerisPath(date));
    }

    // Correlations between celestial events and market data
    async getCorrelations(params = {}) {
        return this._fetchFirst(buildAstrogridCorrelationsCandidates(params));
    }

    // Timeline of upcoming celestial events
    async getTimeline(params = {}) {
        return this._fetchFirst(buildAstrogridTimelineCandidates(params));
    }

    // AI-generated celestial briefing
    async getBriefing() {
        return this._fetchFirst(buildAstrogridBriefingCandidates());
    }

    // Current and upcoming retrogrades
    async getRetrogrades() {
        return this._fetchFirst(buildAstrogridRetrogradeCandidates());
    }

    // Upcoming eclipses
    async getEclipses() {
        return this._fetch(ASTROGRID_ENDPOINTS.eclipses);
    }

    // Nakshatra (lunar mansion) data
    async getNakshatra() {
        return this._fetch(ASTROGRID_ENDPOINTS.nakshatra);
    }

    // Lunar calendar and phase data
    async getLunarCalendar(year, month) {
        return this._fetchFirst(buildAstrogridLunarCalendarCandidates(year, month));
    }

    // Solar activity (sunspots, flares, etc.)
    async getSolarActivity() {
        return this._fetchFirst(buildAstrogridSolarActivityCandidates());
    }

    // Compare celestial state between two dates
    async compareDates(date1, date2) {
        return this._fetchFirst(buildAstrogridCompareCandidates(date1, date2));
    }
}

export const api = new AstroGridApi();
export default api;
