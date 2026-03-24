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

    // Agents
    async getAgentStatus() { return this._fetch('/api/v1/agents/status'); }
    async triggerAgentRun(data) {
        return this._fetch('/api/v1/agents/run', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }
    async getAgentRuns(limit = 20) {
        return this._fetch(`/api/v1/agents/runs?limit=${limit}`);
    }
    async getAgentRun(id) { return this._fetch(`/api/v1/agents/runs/${id}`); }
    async runAgentBacktest(data = {}) {
        return this._fetch('/api/v1/agents/backtest', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }
    async getBacktestSummary(days = 90) {
        return this._fetch(`/api/v1/agents/backtest/summary?days_back=${days}`);
    }
    async getAgentSchedule() { return this._fetch('/api/v1/agents/schedule'); }
    async startAgentSchedule() {
        return this._fetch('/api/v1/agents/schedule/start', { method: 'POST' });
    }
    async stopAgentSchedule() {
        return this._fetch('/api/v1/agents/schedule/stop', { method: 'POST' });
    }

    // Signals
    async getSignals() { return this._fetch('/api/v1/signals'); }
    async getSignalSnapshot() { return this._fetch('/api/v1/signals/snapshot'); }

    // Features
    async getFeatures() { return this._fetch('/api/v1/config/features'); }
    async updateFeature(id, data) {
        return this._fetch(`/api/v1/config/features/${id}`, {
            method: 'PUT', body: JSON.stringify(data),
        });
    }

    // Workflows
    async getWorkflows() { return this._fetch('/api/v1/workflows'); }
    async getEnabledWorkflows() { return this._fetch('/api/v1/workflows/enabled'); }
    async enableWorkflow(name) {
        return this._fetch(`/api/v1/workflows/${name}/enable`, { method: 'POST' });
    }
    async disableWorkflow(name) {
        return this._fetch(`/api/v1/workflows/${name}/disable`, { method: 'POST' });
    }
    async runWorkflow(name) {
        return this._fetch(`/api/v1/workflows/${name}/run`, { method: 'POST' });
    }
    async validateWorkflow(name) {
        return this._fetch(`/api/v1/workflows/${name}/validate`);
    }
    async getWorkflowWaves() { return this._fetch('/api/v1/workflows/waves'); }
    async getWorkflowSchedule() { return this._fetch('/api/v1/workflows/schedule'); }

    // Physics
    async runPhysicsVerification(asOf) {
        const qs = asOf ? `?as_of=${asOf}` : '';
        return this._fetch(`/api/v1/physics/verify${qs}`);
    }
    async getConventions() { return this._fetch('/api/v1/physics/conventions'); }
    async getConvention(domain) { return this._fetch(`/api/v1/physics/conventions/${domain}`); }
    async getOUParams(feature, window = 252) {
        return this._fetch(`/api/v1/physics/ou/${feature}?window=${window}`);
    }
    async getHurst(feature) { return this._fetch(`/api/v1/physics/hurst/${feature}`); }
    async getEnergy(feature) { return this._fetch(`/api/v1/physics/energy/${feature}`); }

    // Ollama
    async getOllamaStatus() { return this._fetch('/api/v1/ollama/status'); }
    async generateBriefing(type = 'hourly') {
        return this._fetch('/api/v1/ollama/briefing', {
            method: 'POST', body: JSON.stringify({ briefing_type: type }),
        });
    }
    async getLatestBriefing(type = 'hourly') {
        return this._fetch(`/api/v1/ollama/briefing/latest?briefing_type=${type}`);
    }
    async listBriefings(type = '', limit = 20) {
        return this._fetch(`/api/v1/ollama/briefings?briefing_type=${type}&limit=${limit}`);
    }
    async readBriefing(filename) {
        return this._fetch(`/api/v1/ollama/briefings/${filename}`);
    }
    async askOllama(question, context = '') {
        return this._fetch('/api/v1/ollama/ask', {
            method: 'POST', body: JSON.stringify({ question, context }),
        });
    }
    async explainRelationship(featureA, featureB, pattern) {
        return this._fetch('/api/v1/ollama/explain', {
            method: 'POST',
            body: JSON.stringify({ feature_a: featureA, feature_b: featureB, observed_pattern: pattern }),
        });
    }
    async generateHypotheses(pattern, n = 3) {
        return this._fetch('/api/v1/ollama/hypotheses', {
            method: 'POST', body: JSON.stringify({ pattern_description: pattern, n_candidates: n }),
        });
    }
    async analyzeRegimeTransition(fromRegime, toRegime, changes = {}) {
        return this._fetch('/api/v1/ollama/regime-analysis', {
            method: 'POST',
            body: JSON.stringify({ from_regime: fromRegime, to_regime: toRegime, feature_changes: changes }),
        });
    }

    // Associations
    async getCorrelationMatrix(days = 252) {
        return this._fetch(`/api/v1/associations/correlation-matrix?days=${days}`);
    }
    async getLagAnalysis(featureA, featureB, maxLag = 10) {
        return this._fetch(
            `/api/v1/associations/lag-analysis?feature_a=${encodeURIComponent(featureA)}&feature_b=${encodeURIComponent(featureB)}&max_lag=${maxLag}`
        );
    }
    async getAssociationClusters() {
        return this._fetch('/api/v1/associations/clusters');
    }
    async getRegimeFeatures(days = 504) {
        return this._fetch(`/api/v1/associations/regime-features?days=${days}`);
    }
    async getAnomalies(sigma = 2.5) {
        return this._fetch(`/api/v1/associations/anomalies?sigma_threshold=${sigma}`);
    }

    // Backtest
    async runBacktest(startDate = '2015-01-01', capital = 100000, costBps = 10) {
        return this._fetch('/api/v1/backtest/run', {
            method: 'POST',
            body: JSON.stringify({ start_date: startDate, initial_capital: capital, cost_bps: costBps }),
        });
    }
    async getBacktestResults() { return this._fetch('/api/v1/backtest/results'); }
    async getBacktestSummaryPitch() { return this._fetch('/api/v1/backtest/summary'); }
    async generateCharts() {
        return this._fetch('/api/v1/backtest/charts', { method: 'POST' });
    }
    getChartUrl(name) { return `${this.baseUrl}/api/v1/backtest/charts/${name}`; }

    // Paper Trades
    async createPaperTrade() {
        return this._fetch('/api/v1/backtest/paper-trade', { method: 'POST' });
    }
    async listPaperTrades() { return this._fetch('/api/v1/backtest/paper-trades'); }
    async getPaperTrade(filename) {
        return this._fetch(`/api/v1/backtest/paper-trades/${filename}`);
    }
    async scorePredictions() {
        return this._fetch('/api/v1/backtest/paper-trade/score', { method: 'POST' });
    }

    // WebSocket (first-message auth pattern)
    connectWebSocket(onMessage) {
        if (this._ws) {
            this._ws.close();
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${protocol}//${window.location.host}/ws`;

        this._ws = new WebSocket(url);
        this._wsReconnectDelay = 1000;

        this._ws.onopen = () => {
            // Send auth token as first message instead of query param
            this._ws.send(JSON.stringify({ type: 'auth', token: this.token }));
            console.log('WebSocket connected, auth sent');
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
