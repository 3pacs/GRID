/**
 * DerivativesGrid API client.
 * All fetch calls to /api/v1/derivatives/* go through here.
 */

class DerivativesApiError extends Error {
    constructor(status, message, detail) {
        super(message);
        this.status = status;
        this.detail = detail;
    }
}

class DerivativesApi {
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

            throw new DerivativesApiError(response.status, message, body);
        }

        return response.json();
    }

    // Market-wide overview
    async getOverview() {
        return this._fetch('/api/v1/derivatives/overview');
    }

    // Full GEX profile for a ticker
    async getGEX(ticker) {
        return this._fetch(`/api/v1/derivatives/gex/${ticker}`);
    }

    // Current dealer regime
    async getRegime() {
        return this._fetch('/api/v1/derivatives/regime');
    }

    // Support/resistance walls
    async getWalls(ticker) {
        return this._fetch(`/api/v1/derivatives/walls/${ticker}`);
    }

    // Vanna and charm decomposition
    async getVannaCharm(ticker) {
        return this._fetch(`/api/v1/derivatives/vanna-charm/${ticker}`);
    }

    // Vol surface data
    async getVolSurface(ticker) {
        return this._fetch(`/api/v1/derivatives/vol-surface/${ticker}`);
    }

    // Skew curves
    async getSkew(ticker) {
        return this._fetch(`/api/v1/derivatives/skew/${ticker}`);
    }

    // Term structure
    async getTermStructure(ticker) {
        return this._fetch(`/api/v1/derivatives/term-structure/${ticker}`);
    }

    // OI heatmap
    async getOIHeatmap(ticker) {
        return this._fetch(`/api/v1/derivatives/oi-heatmap/${ticker}`);
    }

    // Flow narrative / LLM briefing
    async getFlowNarrative() {
        return this._fetch('/api/v1/derivatives/flow-narrative');
    }

    // Latest signals
    async getSignals() {
        return this._fetch('/api/v1/derivatives/signals');
    }

    // Scanner results
    async getScan() {
        return this._fetch('/api/v1/derivatives/scan');
    }

    // Historical GEX data
    async getHistory(ticker) {
        return this._fetch(`/api/v1/derivatives/history/${ticker}`);
    }
}

export const api = new DerivativesApi();
export default api;
