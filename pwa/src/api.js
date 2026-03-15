/**
 * GRID API client module.
 * All fetch calls go through here.
 */

class GRIDApiError extends Error {
    constructor(status, message, detail) {
        super(message);
        this.status = status;
        this.detail = detail;
    }
}

class GRIDApi {
    constructor() {
        this.baseUrl = window.location.origin;
        this._ws = null;
        this._wsReconnectDelay = 1000;
        this._wsMaxDelay = 30000;
    }

    get token() {
        return localStorage.getItem('grid_token');
    }

    set token(val) {
        if (val) {
            localStorage.setItem('grid_token', val);
        } else {
            localStorage.removeItem('grid_token');
        }
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

        if (response.status === 401) {
            this.token = null;
            window.location.hash = '#/login';
            throw new GRIDApiError(401, 'Unauthorized', 'Session expired');
        }

        if (!response.ok) {
            const body = await response.json().catch(() => ({}));
            throw new GRIDApiError(
                response.status,
                body.detail || response.statusText,
                body
            );
        }

        return response.json();
    }

    // Auth
    async login(password) {
        const data = await this._fetch('/api/v1/auth/login', {
            method: 'POST',
            body: JSON.stringify({ password }),
        });
        this.token = data.token;
        return data;
    }

    async logout() {
        await this._fetch('/api/v1/auth/logout', { method: 'POST' });
        this.token = null;
    }

    async verify() {
        return this._fetch('/api/v1/auth/verify');
    }

    // System
    async getStatus() { return this._fetch('/api/v1/system/status'); }
    async getLogs(source = 'api', lines = 50) {
        return this._fetch(`/api/v1/system/logs?source=${source}&lines=${lines}`);
    }
    async restartHyperspace() {
        return this._fetch('/api/v1/system/restart-hyperspace', { method: 'POST' });
    }

    // Regime
    async getCurrent() { return this._fetch('/api/v1/regime/current'); }
    async getHistory(days = 90) { return this._fetch(`/api/v1/regime/history?days=${days}`); }
    async getTransitions() { return this._fetch('/api/v1/regime/transitions'); }

    // Journal
    async getJournal(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/journal?${qs}`);
    }
    async getJournalEntry(id) { return this._fetch(`/api/v1/journal/${id}`); }
    async createJournalEntry(data) {
        return this._fetch('/api/v1/journal', { method: 'POST', body: JSON.stringify(data) });
    }
    async recordOutcome(id, data) {
        return this._fetch(`/api/v1/journal/${id}/outcome`, {
            method: 'PUT',
            body: JSON.stringify(data),
        });
    }
    async getJournalStats() { return this._fetch('/api/v1/journal/stats'); }

    // Models
    async getModels(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/models?${qs}`);
    }
    async getModel(id) { return this._fetch(`/api/v1/models/${id}`); }
    async transitionModel(id, data) {
        return this._fetch(`/api/v1/models/${id}/transition`, {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }
    async rollbackModel(id) {
        return this._fetch(`/api/v1/models/${id}/rollback`, { method: 'POST' });
    }
    async getProductionModels() { return this._fetch('/api/v1/models/production'); }

    // Discovery
    async triggerOrthogonality() {
        return this._fetch('/api/v1/discovery/orthogonality', { method: 'POST' });
    }
    async triggerClustering(n = 3) {
        return this._fetch(`/api/v1/discovery/clustering?n_components=${n}`, { method: 'POST' });
    }
    async getJobs() { return this._fetch('/api/v1/discovery/jobs'); }
    async getResults(type) { return this._fetch(`/api/v1/discovery/results/${type}`); }
    async getHypotheses(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/discovery/hypotheses?${qs}`);
    }

    // Config
    async getConfig() { return this._fetch('/api/v1/config'); }
    async updateConfig(data) {
        return this._fetch('/api/v1/config', { method: 'PUT', body: JSON.stringify(data) });
    }
    async getSources() { return this._fetch('/api/v1/config/sources'); }
    async updateSource(id, data) {
        return this._fetch(`/api/v1/config/sources/${id}`, {
            method: 'PUT',
            body: JSON.stringify(data),
        });
    }

    // WebSocket
    connectWebSocket(onMessage) {
        if (this._ws) {
            this._ws.close();
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${protocol}//${window.location.host}/ws?token=${this.token}`;

        this._ws = new WebSocket(url);
        this._wsReconnectDelay = 1000;

        this._ws.onopen = () => {
            console.log('WebSocket connected');
            this._wsReconnectDelay = 1000;
        };

        this._ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                onMessage(data);
            } catch (e) {
                console.error('WS parse error:', e);
            }
        };

        this._ws.onclose = () => {
            console.log('WebSocket disconnected, reconnecting...');
            setTimeout(() => {
                this._wsReconnectDelay = Math.min(this._wsReconnectDelay * 2, this._wsMaxDelay);
                if (this.token) {
                    this.connectWebSocket(onMessage);
                }
            }, this._wsReconnectDelay);
        };

        this._ws.onerror = (err) => {
            console.error('WebSocket error:', err);
        };
    }

    disconnectWebSocket() {
        if (this._ws) {
            this._ws.close();
            this._ws = null;
        }
    }
}

export const api = new GRIDApi();
export { GRIDApiError };
