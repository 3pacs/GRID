/**
 * GRID API client — TypeScript version.
 *
 * Full superset of api.js. Every method from the JS version is preserved
 * with identical signatures. New code should import from here; existing
 * .jsx consumers can continue importing from './api.js' (Vite resolves both).
 */

// ── Error class ───────────────────────────────────────────────

export class GRIDApiError extends Error {
    status: number;
    detail?: string;

    constructor(status: number, message: string, detail?: string) {
        super(message);
        this.status = status;
        this.detail = detail;
    }
}

// ── Fetch options ─────────────────────────────────────────────

interface FetchOptions extends Omit<RequestInit, 'headers'> {
    headers?: Record<string, string>;
}

// ── API client class ──────────────────────────────────────────

class GRIDApi {
    baseUrl: string;
    private _ws: WebSocket | null;
    private _wsReconnectDelay: number;
    private _wsMaxDelay: number;

    constructor() {
        this.baseUrl = typeof window !== 'undefined' ? window.location.origin : '';
        this._ws = null;
        this._wsReconnectDelay = 1000;
        this._wsMaxDelay = 30000;
    }

    get token(): string | null {
        return localStorage.getItem('grid_token');
    }

    set token(val: string | null) {
        if (val) {
            localStorage.setItem('grid_token', val);
        } else {
            localStorage.removeItem('grid_token');
        }
    }

    // ── Generic helpers ───────────────────────────────────────

    /** Public GET helper — delegates to _fetch. */
    async get<T = unknown>(path: string): Promise<T> {
        return this._fetch<T>(path);
    }

    /** Public POST helper — delegates to _fetch. */
    async post<T = unknown>(path: string, body: unknown = {}): Promise<T> {
        return this._fetch<T>(path, { method: 'POST', body: JSON.stringify(body) });
    }

    async _fetch<T = unknown>(path: string, options: FetchOptions = {}): Promise<T> {
        const headers: Record<string, string> = { 'Content-Type': 'application/json', ...options.headers };
        if (this.token) {
            headers['Authorization'] = `Bearer ${this.token}`;
        }

        const response = await fetch(`${this.baseUrl}${path}`, {
            ...options,
            headers,
        });

        if (!response.ok) {
            const body = await response.text().catch(() => '');
            let message = response.statusText;
            try {
                const parsed = JSON.parse(body);
                message = parsed.detail || parsed.message || message;
            } catch (_) {
                if (body) message = body;
            }

            // Only treat 401 as session expiry for non-auth endpoints
            if (response.status === 401 && !path.startsWith('/api/v1/auth/login') && !path.startsWith('/api/v1/auth/register')) {
                this.token = null;
                if (typeof window.dispatchEvent === 'function') {
                    window.dispatchEvent(new Event('grid:auth-expired'));
                }
                window.location.hash = '#/login';
            }

            throw new GRIDApiError(response.status, message);
        }

        return await response.json();
    }

    // ── Auth ──────────────────────────────────────────────────

    async login(password: string, username: string | null = null) {
        const body = username
            ? { password, username }
            : { password };
        const data = await this._fetch<{ token: string; role?: string; username?: string }>('/api/v1/auth/login', {
            method: 'POST',
            body: JSON.stringify(body),
        });
        this.token = data.token;
        return data;
    }

    async register(username: string, password: string) {
        const data = await this._fetch<{ token: string; role?: string; username?: string }>('/api/v1/auth/register', {
            method: 'POST',
            body: JSON.stringify({ username, password }),
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

    // ── User management (admin only) ─────────────────────────

    async listUsers() {
        return this._fetch('/api/v1/auth/users');
    }

    async createUser(username: string, password: string, role: string = 'contributor') {
        return this._fetch('/api/v1/auth/users', {
            method: 'POST',
            body: JSON.stringify({ username, password, role }),
        });
    }

    async deleteUser(username: string) {
        return this._fetch(`/api/v1/auth/users/${encodeURIComponent(username)}`, {
            method: 'DELETE',
        });
    }

    // ── System ────────────────────────────────────────────────

    async getStatus() { return this._fetch('/api/v1/system/status'); }
    async getLogs(source: string = 'api', lines: number = 50) {
        return this._fetch(`/api/v1/system/logs?source=${source}&lines=${lines}`);
    }
    async restartHyperspace() {
        return this._fetch('/api/v1/system/restart-hyperspace', { method: 'POST' });
    }

    // ── Regime ────────────────────────────────────────────────

    async getCurrent() { return this._fetch('/api/v1/regime/current'); }
    async getHistory(days: number = 90) { return this._fetch(`/api/v1/regime/history?days=${days}`); }
    async getTransitions() { return this._fetch('/api/v1/regime/transitions'); }
    async getAllActiveRegimes() { return this._fetch('/api/v1/regime/all-active'); }
    async getRegimeSynthesis() { return this._fetch('/api/v1/regime/synthesis'); }
    async getRegimeWeights() { return this._fetch('/api/v1/regime/weights'); }
    async updateRegimeWeights(weights: unknown) { return this._fetch('/api/v1/regime/weights', { method: 'PUT', body: JSON.stringify({ weights }) }); }
    async simulateRegimeWeights(weights: unknown) { return this._fetch('/api/v1/regime/simulate', { method: 'POST', body: JSON.stringify({ weights }) }); }

    // ── Strategy ──────────────────────────────────────────────

    async getActiveStrategies() { return this._fetch('/api/v1/strategy/active'); }
    async getStrategyForRegime(state: string) { return this._fetch(`/api/v1/strategy/for-regime/${encodeURIComponent(state)}`); }
    async assignStrategy(data: unknown) {
        return this._fetch('/api/v1/strategy/assign', {
            method: 'POST', body: JSON.stringify(data),
        });
    }

    // ── Journal ───────────────────────────────────────────────

    async getJournal(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/journal?${qs}`);
    }
    async getJournalEntry(id: string | number) { return this._fetch(`/api/v1/journal/${id}`); }
    async createJournalEntry(data: unknown) {
        return this._fetch('/api/v1/journal', { method: 'POST', body: JSON.stringify(data) });
    }
    async recordOutcome(id: string | number, data: unknown) {
        return this._fetch(`/api/v1/journal/${id}/outcome`, {
            method: 'PUT',
            body: JSON.stringify(data),
        });
    }
    async getJournalStats() { return this._fetch('/api/v1/journal/stats'); }

    // ── Models ────────────────────────────────────────────────

    async getModels(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/models?${qs}`);
    }
    async getModel(id: string | number) { return this._fetch(`/api/v1/models/${id}`); }
    async transitionModel(id: string | number, data: unknown) {
        return this._fetch(`/api/v1/models/${id}/transition`, {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }
    async rollbackModel(id: string | number) {
        return this._fetch(`/api/v1/models/${id}/rollback`, { method: 'POST' });
    }
    async getProductionModels() { return this._fetch('/api/v1/models/production'); }

    // ── Discovery ─────────────────────────────────────────────

    async triggerOrthogonality() {
        return this._fetch('/api/v1/discovery/orthogonality', { method: 'POST' });
    }
    async triggerClustering(n: number = 3) {
        return this._fetch(`/api/v1/discovery/clustering?n_components=${n}`, { method: 'POST' });
    }
    async getDiscoveryCorrelationMatrix(period: number = 90, regime: string = 'all') {
        return this._fetch(`/api/v1/discovery/correlation-matrix?period=${period}&regime=${encodeURIComponent(regime)}`);
    }
    async getJobs() { return this._fetch('/api/v1/discovery/jobs'); }
    async getResults(type: string) { return this._fetch(`/api/v1/discovery/results/${type}`); }
    async getHypotheses(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/discovery/hypotheses?${qs}`);
    }

    // ── Config ────────────────────────────────────────────────

    async getConfig() { return this._fetch('/api/v1/config'); }
    async updateConfig(data: unknown) {
        return this._fetch('/api/v1/config', { method: 'PUT', body: JSON.stringify(data) });
    }
    async getSources() { return this._fetch('/api/v1/config/sources'); }
    async updateSource(id: string | number, data: unknown) {
        return this._fetch(`/api/v1/config/sources/${id}`, {
            method: 'PUT',
            body: JSON.stringify(data),
        });
    }

    // ── Agents ────────────────────────────────────────────────

    async getAgentStatus() { return this._fetch('/api/v1/agents/status'); }
    async triggerAgentRun(data: unknown) {
        return this._fetch('/api/v1/agents/run', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }
    async getAgentRuns(limit: number = 20) {
        return this._fetch(`/api/v1/agents/runs?limit=${limit}`);
    }
    async getAgentRun(id: string | number) { return this._fetch(`/api/v1/agents/runs/${id}`); }
    async runAgentBacktest(data: unknown = {}) {
        return this._fetch('/api/v1/agents/backtest', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }
    async getBacktestSummary(days: number = 90) {
        return this._fetch(`/api/v1/agents/backtest/summary?days_back=${days}`);
    }
    async getAgentSchedule() { return this._fetch('/api/v1/agents/schedule'); }
    async startAgentSchedule() {
        return this._fetch('/api/v1/agents/schedule/start', { method: 'POST' });
    }
    async stopAgentSchedule() {
        return this._fetch('/api/v1/agents/schedule/stop', { method: 'POST' });
    }

    // ── Watchlist ─────────────────────────────────────────────

    async getWatchlist(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/watchlist/?${qs}`);
    }
    async addToWatchlist(data: unknown) {
        return this._fetch('/api/v1/watchlist/', {
            method: 'POST', body: JSON.stringify(data),
        });
    }
    async removeFromWatchlist(ticker: string) {
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}`, {
            method: 'DELETE',
        });
    }
    async getTickerAnalysis(ticker: string, period: string = '3M') {
        const qs = period && period !== '3M' ? `?period=${encodeURIComponent(period)}` : '';
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}/analysis${qs}`);
    }
    async getWatchlistEnriched(limit: number = 20) {
        return this._fetch(`/api/v1/watchlist/enriched?limit=${limit}`);
    }
    async searchWatchlistTickers(query: string) {
        return this._fetch(`/api/v1/watchlist/search?q=${encodeURIComponent(query)}`);
    }
    async getTickerOverview(ticker: string) {
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}/overview`);
    }
    async refreshWatchlistPrices() {
        return this._fetch('/api/v1/watchlist/refresh-prices', { method: 'POST' });
    }
    async getWatchlistPrices() {
        return this._fetch('/api/v1/watchlist/prices');
    }
    async getPortfolio() {
        return this._fetch('/api/v1/watchlist/portfolio');
    }
    async preloadWatchlist() {
        return this._fetch('/api/v1/watchlist/preload');
    }
    async getTickerEdge(ticker: string) {
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}/edge`);
    }
    async getFeatureTimeframes(feature: string, periods: string = '5d,5w,3m,1y,5y') {
        return this._fetch(`/api/v1/signals/timeframes?feature=${encodeURIComponent(feature)}&periods=${encodeURIComponent(periods)}`);
    }
    async promoteHypothesis(hypothesisId: string | number) {
        return this._fetch(`/api/v1/discovery/hypotheses/${hypothesisId}/promote`, { method: 'POST' });
    }
    async getHypothesisResults(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/discovery/hypotheses/results?${qs}`);
    }

    // ── Settings ──────────────────────────────────────────────

    async getSettings() { return this._fetch('/api/v1/system/settings'); }
    async updateSettings(data: unknown) {
        return this._fetch('/api/v1/system/settings', { method: 'POST', body: JSON.stringify(data) });
    }
    async getApiKeys() { return this._fetch('/api/v1/system/api-keys'); }
    async getServices() { return this._fetch('/api/v1/system/services'); }
    async getHermesStatus(limit: number = 20) { return this._fetch(`/api/v1/system/hermes-status?limit=${limit}`); }
    async getFreshness() { return this._fetch('/api/v1/system/freshness'); }
    async getHealth() { return this._fetch('/api/v1/system/health'); }
    async getPipelineHealth() { return this._fetch('/api/v1/system/pipeline-health'); }
    async getArchitecture() { return this._fetch('/api/v1/system/architecture'); }

    // ── Signals ───────────────────────────────────────────────

    async getSignals() { return this._fetch('/api/v1/signals'); }
    async getSignalSnapshot() { return this._fetch('/api/v1/signals/snapshot'); }
    async getCelestialSignals() { return this._fetch('/api/v1/signals/celestial'); }
    async getCrucixSignals() { return this._fetch('/api/v1/signals/crucix'); }
    async getConvictionScores(minScore: number = 20) { return this._fetch(`/api/v1/signals/conviction?min_score=${minScore}`); }
    async getConvictionTicker(ticker: string) { return this._fetch(`/api/v1/signals/conviction/${encodeURIComponent(ticker)}`); }

    // ── Options ───────────────────────────────────────────────

    async getOptionsSignals(ticker: string = '', limit: number = 50) {
        const qs = new URLSearchParams({ ...(ticker && { ticker }), limit: String(limit) }).toString();
        return this._fetch(`/api/v1/options/signals?${qs}`);
    }
    async scanMispricing(minScore: number = 5.0) {
        return this._fetch(`/api/v1/options/scan?min_score=${minScore}`);
    }
    async get100xOpportunities() { return this._fetch('/api/v1/options/100x'); }
    async getGEXProfile(ticker: string) {
        return this._fetch(`/api/v1/derivatives/gex/${encodeURIComponent(ticker)}`);
    }
    async getVannaCharm(ticker: string) {
        return this._fetch(`/api/v1/derivatives/vanna-charm/${encodeURIComponent(ticker)}`);
    }
    async getFlowTimeline(ticker: string, days: number = 90) {
        return this._fetch(`/api/v1/derivatives/flow-timeline/${encodeURIComponent(ticker)}?days=${days}`);
    }

    // ── Options Recommendations ───────────────────────────────

    async getOptionsRecommendations(ticker: string = '') {
        const qs = ticker ? `?ticker=${encodeURIComponent(ticker)}` : '';
        return this._fetch(`/api/v1/options/recommendations${qs}`);
    }
    async refreshOptionsRecommendations() {
        return this._fetch('/api/v1/options/recommendations/refresh', { method: 'POST' });
    }
    async getOptionsRecommendationHistory(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/options/recommendations/history?${qs}`);
    }
    async getOptionsHistory(ticker: string = '', days: number = 30, only100x: boolean = false, limit: number = 50) {
        const params = new URLSearchParams({ days: String(days), limit: String(limit) });
        if (ticker) params.set('ticker', ticker);
        if (only100x) params.set('only_100x', 'true');
        return this._fetch(`/api/v1/options/history?${params}`);
    }

    // ── Physics ───────────────────────────────────────────────

    async getNewsMomentum(lookbackDays: number = 63) {
        return this._fetch(`/api/v1/physics/momentum?lookback_days=${lookbackDays}`);
    }

    // ── Features ──────────────────────────────────────────────

    async getFeatures() { return this._fetch('/api/v1/config/features'); }
    async updateFeature(id: string | number, data: unknown) {
        return this._fetch(`/api/v1/config/features/${id}`, {
            method: 'PUT', body: JSON.stringify(data),
        });
    }

    // ── Workflows ─────────────────────────────────────────────

    async getWorkflows() { return this._fetch('/api/v1/workflows'); }
    async getEnabledWorkflows() { return this._fetch('/api/v1/workflows/enabled'); }
    async enableWorkflow(name: string) {
        return this._fetch(`/api/v1/workflows/${name}/enable`, { method: 'POST' });
    }
    async disableWorkflow(name: string) {
        return this._fetch(`/api/v1/workflows/${name}/disable`, { method: 'POST' });
    }
    async runWorkflow(name: string) {
        return this._fetch(`/api/v1/workflows/${name}/run`, { method: 'POST' });
    }
    async runOrthogonality() {
        return this._fetch('/api/v1/discovery/orthogonality', { method: 'POST' });
    }
    async getSectorFlows() { return this._fetch('/api/v1/flows/sectors'); }
    async getSankeyData(asOf: string | null = null) {
        const qs = asOf ? `?as_of=${asOf}` : '';
        return this._fetch(`/api/v1/flows/sankey${qs}`);
    }
    async getSectorDetail(sectorName: string) { return this._fetch(`/api/v1/flows/sectors/${encodeURIComponent(sectorName)}/detail`); }
    async getActorSupplyChain(actorId: string, direction: string = 'both', depth: number = 2) {
        return this._fetch(`/api/v1/actors/${encodeURIComponent(actorId)}/supply_chain?direction=${direction}&depth=${depth}`);
    }
    async getActorCapitalFlow(actorId: string, periods: number = 4, periodType: string = 'annual') {
        return this._fetch(`/api/v1/actors/${encodeURIComponent(actorId)}/capital_flow?periods=${periods}&period_type=${periodType}`);
    }
    async getActorNews(actorId: string, limit: number = 20) {
        return this._fetch(`/api/v1/actors/${encodeURIComponent(actorId)}/news?limit=${limit}`);
    }
    async getActorTrustCog(actorId: string) {
        return this._fetch(`/api/v1/actors/${encodeURIComponent(actorId)}/trust-cog`);
    }
    async getOraclePredictLive(ticker: string, horizon: number = 7) {
        return this._fetch(`/api/v1/oracle/predict-live/${encodeURIComponent(ticker)}?horizon=${horizon}`);
    }
    async getMoneyMap() { return this._fetch('/api/v1/flows/money-map'); }
    async getCompanyDrill(ticker: string) { return this._fetch(`/api/v1/flows/company/${encodeURIComponent(ticker)}`); }
    async getAggregatedFlows(sector: string | null = null, period: string = 'weekly', days: number = 30) {
        const params = new URLSearchParams({ period, days: String(days) });
        if (sector) params.set('sector', sector);
        return this._fetch(`/api/v1/flows/aggregated?${params}`);
    }
    async getFlowMomentum(ticker: string) {
        return this._fetch(`/api/v1/flows/momentum/${encodeURIComponent(ticker)}`);
    }

    // ── Flow Engine v2 ────────────────────────────────────────

    async getFlowMapV2() { return this._fetch('/api/v1/flows/flow-map-v2'); }
    async getJunctionPoints() { return this._fetch('/api/v1/flows/junction-points'); }
    async getFlowLayers() { return this._fetch('/api/v1/flows/layers'); }
    async getFlowLayerDetail(layerId: string) { return this._fetch(`/api/v1/flows/layers/${encodeURIComponent(layerId)}`); }
    async getFlowWaterfall(source: string = 'fed') { return this._fetch(`/api/v1/flows/waterfall?source=${encodeURIComponent(source)}`); }
    async getFlowOrthogonality() { return this._fetch('/api/v1/flows/orthogonality'); }
    async generateFlowImage(type: string, style: string = 'dark') {
        return this._fetch(`/api/v1/flows/generate-image/${encodeURIComponent(type)}?style=${style}`);
    }
    async getCdsDashboard() { return this._fetch('/api/v1/flows/cds'); }
    async getCdsHistory(seriesKey: string, days: number = 365) {
        return this._fetch(`/api/v1/flows/cds/history/${encodeURIComponent(seriesKey)}?days=${days}`);
    }

    // ── Audio Briefing ────────────────────────────────────────

    async getFlowBriefing(audio: boolean = true) {
        return this._fetch(`/api/v1/flows/briefing?audio=${audio}`);
    }
    getFlowBriefingAudioUrl(filename: string | null = null): string {
        const path = filename
            ? `/api/v1/flows/briefing/audio/${encodeURIComponent(filename)}`
            : '/api/v1/flows/briefing/audio';
        return `${this.baseUrl}${path}?token=${encodeURIComponent(this.token || '')}`;
    }
    async listFlowBriefings() {
        return this._fetch('/api/v1/flows/briefing/list');
    }
    async getFlowBriefingDetail(filename: string) {
        return this._fetch(`/api/v1/flows/briefing/detail/${encodeURIComponent(filename)}`);
    }

    // ── Deep Dives ────────────────────────────────────────────

    async getDeepDives(days: number = 90) {
        return this._fetch(`/api/v1/intelligence/deep-dives?days=${days}`);
    }
    async getDeepDive(id: string | number) {
        return this._fetch(`/api/v1/intelligence/deep-dives/${id}`);
    }
    async triggerDeepDive() {
        return this._fetch('/api/v1/intelligence/deep-dives/generate', { method: 'POST' });
    }

    // ── Research Archive ──────────────────────────────────────

    async getResearchArchive(days: number = 365) {
        return this._fetch(`/api/v1/intelligence/archive?days=${days}`);
    }

    async validateWorkflow(name: string) {
        return this._fetch(`/api/v1/workflows/${name}/validate`);
    }
    async getWorkflowWaves() { return this._fetch('/api/v1/workflows/waves'); }
    async getWorkflowSchedule() { return this._fetch('/api/v1/workflows/schedule'); }

    // ── Physics (extended) ────────────────────────────────────

    async runPhysicsVerification(asOf?: string) {
        const qs = asOf ? `?as_of=${asOf}` : '';
        return this._fetch(`/api/v1/physics/verify${qs}`);
    }
    async getConventions() { return this._fetch('/api/v1/physics/conventions'); }
    async getConvention(domain: string) { return this._fetch(`/api/v1/physics/conventions/${domain}`); }
    async getOUParams(feature: string, window: number = 252) {
        return this._fetch(`/api/v1/physics/ou/${feature}?window=${window}`);
    }
    async getHurst(feature: string) { return this._fetch(`/api/v1/physics/hurst/${feature}`); }
    async getEnergy(feature: string) { return this._fetch(`/api/v1/physics/energy/${feature}`); }
    async getNewsEnergy(lookbackDays: number = 30, asOf?: string) {
        const params = new URLSearchParams({ lookback_days: String(lookbackDays) });
        if (asOf) params.set('as_of', asOf);
        return this._fetch(`/api/v1/physics/news-energy?${params}`);
    }
    async getPhysicsDashboard(asOf?: string) {
        const qs = asOf ? `?as_of=${asOf}` : '';
        return this._fetch(`/api/v1/physics/dashboard${qs}`);
    }

    // ── Ollama ────────────────────────────────────────────────

    async getOllamaStatus() { return this._fetch('/api/v1/ollama/status'); }
    async generateBriefing(type: string = 'hourly') {
        return this._fetch('/api/v1/ollama/briefing', {
            method: 'POST', body: JSON.stringify({ briefing_type: type }),
        });
    }
    async getLatestBriefing(type: string = 'hourly') {
        return this._fetch(`/api/v1/ollama/briefing/latest?briefing_type=${type}`);
    }
    async listBriefings(type: string = '', limit: number = 20) {
        return this._fetch(`/api/v1/ollama/briefings?briefing_type=${type}&limit=${limit}`);
    }
    async readBriefing(filename: string) {
        return this._fetch(`/api/v1/ollama/briefings/${filename}`);
    }
    async askOllama(question: string, context: string = '') {
        return this._fetch('/api/v1/ollama/ask', {
            method: 'POST', body: JSON.stringify({ question, context }),
        });
    }
    async explainRelationship(featureA: string, featureB: string, pattern: string) {
        return this._fetch('/api/v1/ollama/explain', {
            method: 'POST',
            body: JSON.stringify({ feature_a: featureA, feature_b: featureB, observed_pattern: pattern }),
        });
    }
    async generateHypotheses(pattern: string, n: number = 3) {
        return this._fetch('/api/v1/ollama/hypotheses', {
            method: 'POST', body: JSON.stringify({ pattern_description: pattern, n_candidates: n }),
        });
    }
    async analyzeRegimeTransition(fromRegime: string, toRegime: string, changes: unknown = {}) {
        return this._fetch('/api/v1/ollama/regime-analysis', {
            method: 'POST',
            body: JSON.stringify({ from_regime: fromRegime, to_regime: toRegime, feature_changes: changes }),
        });
    }
    async getCapitalFlowResearch(sectors: string[] | null = null, asOf: string | null = null, force: boolean = false) {
        return this._fetch('/api/v1/ollama/capital-flows', {
            method: 'POST',
            body: JSON.stringify({ sectors, as_of: asOf, force }),
        });
    }

    // ── Ask GRID (Chat) ───────────────────────────────────────

    async askGRID(question: string, contextTicker: string | null = null, history: unknown[] = [], timeframe: string | null = null) {
        return this._fetch('/api/v1/chat/ask', {
            method: 'POST',
            body: JSON.stringify({
                question,
                context_ticker: contextTicker,
                timeframe,
                history,
            }),
        });
    }

    // ── Actor Network ─────────────────────────────────────────

    async getActorNetwork() { return this._fetch('/api/v1/intelligence/actor-network'); }
    async getActorDetail(id: string) { return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(id)}`); }
    async getActorNeighborhood(id: string, depth: number = 3, maxNodes: number = 2000) {
        return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(id)}/neighborhood?depth=${depth}&max_nodes=${maxNodes}`);
    }
    async getActorPath(fromId: string, toId: string) {
        return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(fromId)}/path/${encodeURIComponent(toId)}`);
    }
    async getActorConnections(id: string) {
        return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(id)}/connections`);
    }
    async getSpiderStats() {
        return this._fetch('/api/v1/intelligence/spider/stats');
    }

    // ── Intelligence Dashboard (unified) ──────────────────────

    async getIntelDashboard() { return this._fetch('/api/v1/intelligence/dashboard'); }

    async getTrustScores() {
        const data = await this._fetch<{ error?: string; trust?: { top_sources?: unknown[] } }>('/api/v1/intelligence/dashboard');
        if (data?.error) return data;
        return { sources: data?.trust?.top_sources ?? [] };
    }

    async getConvergenceAlerts() {
        const data = await this._fetch<{ error?: string; trust?: { convergence_events?: unknown[] } }>('/api/v1/intelligence/dashboard');
        if (data?.error) return data;
        return { alerts: data?.trust?.convergence_events ?? [] };
    }

    // ── Cross-Reference (Lie Detector) ────────────────────────

    async getCrossReference() { return this._fetch('/api/v1/intelligence/cross-reference'); }
    async getCrossRefHistory() { return this._fetch('/api/v1/intelligence/cross-reference/history'); }

    // ── Regime Analog Engine ──────────────────────────────────

    async getRegimeAnalog() { return this._fetch('/api/v1/intelligence/regime'); }
    async getRegimeAnalogs(n: number = 20) { return this._fetch(`/api/v1/intelligence/regime/analogs?n=${n}&include_timesfm=true`); }
    async getRegimeHistory(days: number = 365) { return this._fetch(`/api/v1/intelligence/regime/history?days=${days}`); }

    // ── Globe ─────────────────────────────────────────────────

    async getGlobeData() { return this._fetch('/api/v1/intelligence/globe'); }

    // ── Risk Map ──────────────────────────────────────────────

    async getRiskMap() { return this._fetch('/api/v1/intelligence/risk-map'); }

    // ── Unified Thesis ────────────────────────────────────────

    async getThesis() { return this._fetch('/api/v1/intelligence/thesis'); }

    // ── Earnings Calendar ─────────────────────────────────────

    async getEarningsCalendar(daysAhead: number = 30) {
        return this._fetch(`/api/v1/earnings/calendar?days_ahead=${daysAhead}`);
    }
    async getRecentEarnings(daysBack: number = 30) {
        return this._fetch(`/api/v1/earnings/recent?days_back=${daysBack}`);
    }
    async getEarningsSurprise(ticker: string) {
        return this._fetch(`/api/v1/earnings/surprise/${encodeURIComponent(ticker)}`);
    }
    async predictEarnings(ticker: string) {
        return this._fetch(`/api/v1/earnings/predict/${encodeURIComponent(ticker)}`, { method: 'POST' });
    }
    async getEarningsScorecard() {
        return this._fetch('/api/v1/earnings/scorecard');
    }
    async getEarningsHistory(ticker: string, limit: number = 20) {
        return this._fetch(`/api/v1/earnings/history/${encodeURIComponent(ticker)}?limit=${limit}`);
    }
    async runEarningsCycle() {
        return this._fetch('/api/v1/earnings/cycle', { method: 'POST' });
    }

    // ── Trend Tracker ─────────────────────────────────────────

    async getTrends(days: number = 90) { return this._fetch(`/api/v1/intelligence/trends?days=${days}`); }

    // ── Associations ──────────────────────────────────────────

    async getCorrelationMatrix(days: number = 252) {
        return this._fetch(`/api/v1/associations/correlation-matrix?days=${days}`);
    }
    async getSmartHeatmap(family: string | null = null, orthogonalOnly: boolean = true) {
        let url = `/api/v1/discovery/smart-heatmap?orthogonal_only=${orthogonalOnly}`;
        if (family) url += `&family=${encodeURIComponent(family)}`;
        return this._fetch(url);
    }
    async getTimeseries(featureNames: string[], days: number = 30) {
        return this._fetch(`/api/v1/signals/timeseries?features=${encodeURIComponent(featureNames.join(','))}&days=${days}`);
    }
    async getLagAnalysis(featureA: string, featureB: string, maxLag: number = 10) {
        return this._fetch(
            `/api/v1/associations/lag-analysis?feature_a=${encodeURIComponent(featureA)}&feature_b=${encodeURIComponent(featureB)}&max_lag=${maxLag}`
        );
    }
    async getAssociationClusters() {
        return this._fetch('/api/v1/associations/clusters');
    }
    async getRegimeFeatures(days: number = 504) {
        return this._fetch(`/api/v1/associations/regime-features?days=${days}`);
    }
    async getAnomalies(sigma: number = 2.5) {
        return this._fetch(`/api/v1/associations/anomalies?sigma_threshold=${sigma}`);
    }

    // ── Snapshots ─────────────────────────────────────────────

    async getSnapshotLatest(category: string, n: number = 1) {
        return this._fetch(`/api/v1/snapshots/latest/${encodeURIComponent(category)}?n=${n}`);
    }
    async getSnapshotHistory(category: string, startDate: string | null = null, endDate: string | null = null) {
        const params = new URLSearchParams();
        if (startDate) params.set('start_date', startDate);
        if (endDate) params.set('end_date', endDate);
        const qs = params.toString();
        return this._fetch(`/api/v1/snapshots/history/${encodeURIComponent(category)}${qs ? '?' + qs : ''}`);
    }
    async compareSnapshots(category: string, dateA: string, dateB: string) {
        return this._fetch(
            `/api/v1/snapshots/compare/${encodeURIComponent(category)}?date_a=${encodeURIComponent(dateA)}&date_b=${encodeURIComponent(dateB)}`
        );
    }
    async getOperatorIssues(daysBack: number = 30, category: string | null = null, severity: string | null = null) {
        const params = new URLSearchParams({ days_back: String(daysBack) });
        if (category) params.set('category', category);
        if (severity) params.set('severity', severity);
        return this._fetch(`/api/v1/snapshots/issues?${params}`);
    }

    // ── Backtest ──────────────────────────────────────────────

    async runBacktest(startDate: string = '2015-01-01', capital: number = 100000, costBps: number = 10) {
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
    getChartUrl(name: string): string { return `${this.baseUrl}/api/v1/backtest/charts/${name}`; }

    // ── Paper Trading Strategies ──────────────────────────────

    async getPaperStrategies() { return this._fetch('/api/v1/trading/strategies'); }
    async getStrategyHistory(strategyId: string) {
        return this._fetch(`/api/v1/trading/strategies/${encodeURIComponent(strategyId)}/history`);
    }
    async promoteToStrategy(data: unknown) {
        return this._fetch('/api/v1/trading/strategies/promote', {
            method: 'POST', body: JSON.stringify(data),
        });
    }
    async killStrategy(strategyId: string) {
        return this._fetch(`/api/v1/trading/strategies/${encodeURIComponent(strategyId)}/kill`, { method: 'POST' });
    }
    async getBacktestWinners(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/discovery/backtest-results?${qs}`);
    }
    async getTradingDashboard() { return this._fetch('/api/v1/trading/dashboard'); }

    // ── Paper Trades ──────────────────────────────────────────

    async createPaperTrade() {
        return this._fetch('/api/v1/backtest/paper-trade', { method: 'POST' });
    }
    async listPaperTrades() { return this._fetch('/api/v1/backtest/paper-trades'); }
    async getPaperTrade(filename: string) {
        return this._fetch(`/api/v1/backtest/paper-trades/${filename}`);
    }
    async scorePredictions() {
        return this._fetch('/api/v1/backtest/paper-trade/score', { method: 'POST' });
    }

    // ── Push Notifications ────────────────────────────────────

    async getVapidKey() { return this._fetch('/api/v1/notifications/vapid-key'); }
    async subscribePush(subscription: PushSubscription, userAgent: string = '') {
        const sub = subscription.toJSON();
        return this._fetch('/api/v1/notifications/subscribe', {
            method: 'POST',
            body: JSON.stringify({
                endpoint: sub.endpoint,
                keys: sub.keys,
                user_agent: userAgent,
            }),
        });
    }
    async unsubscribePush(endpoint: string) {
        return this._fetch('/api/v1/notifications/unsubscribe', {
            method: 'DELETE',
            body: JSON.stringify({ endpoint }),
        });
    }
    async getNotificationPreferences(endpoint: string) {
        return this._fetch(`/api/v1/notifications/preferences?endpoint=${encodeURIComponent(endpoint)}`);
    }
    async updateNotificationPreferences(endpoint: string, prefs: unknown) {
        return this._fetch('/api/v1/notifications/preferences', {
            method: 'PUT',
            body: JSON.stringify({ endpoint, ...(prefs as Record<string, unknown>) }),
        });
    }
    async testPush(subscription: PushSubscription) {
        const sub = subscription.toJSON();
        return this._fetch('/api/v1/notifications/test', {
            method: 'POST',
            body: JSON.stringify({
                endpoint: sub.endpoint,
                keys: sub.keys,
            }),
        });
    }

    // ── Intelligence — Event Timeline ─────────────────────────

    async getEventTimeline(ticker: string, days: number = 90) {
        return this._fetch(`/api/v1/intelligence/events?ticker=${encodeURIComponent(ticker)}&days=${days}&include_lead_times=true`);
    }
    async getRecurringPatterns(minOccurrences: number = 3) {
        return this._fetch(`/api/v1/intelligence/patterns?min_occurrences=${minOccurrences}`);
    }

    // ── Intelligence — Forensics ──────────────────────────────

    async getForensicReports(ticker: string, days: number = 90) {
        return this._fetch(`/api/v1/intelligence/forensics/${encodeURIComponent(ticker)}?days=${days}`);
    }
    async analyzeForensicMove(ticker: string, date: string) {
        return this._fetch(`/api/v1/intelligence/forensics/${encodeURIComponent(ticker)}/analyze?date=${encodeURIComponent(date)}`, { method: 'POST' });
    }

    // ── Intelligence — Causation ──────────────────────────────

    async getCausalLinks(ticker: string) {
        return this._fetch(`/api/v1/intelligence/causation?ticker=${encodeURIComponent(ticker)}`);
    }

    // ── Oracle ────────────────────────────────────────────────

    async getOracleScoreboard() { return this._fetch('/api/v1/oracle/scoreboard'); }
    async getOraclePredictions(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/oracle/predictions?${qs}`);
    }
    async getOracleLatest() { return this._fetch('/api/v1/oracle/latest'); }
    async publishOraclePrediction(data: unknown) {
        return this._fetch('/api/v1/oracle/publish', {
            method: 'POST', body: JSON.stringify(data),
        });
    }

    // ── Signal Registry & Ensemble ────────────────────────────

    async getSignalRegistry(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/signals/registry${qs ? '?' + qs : ''}`);
    }
    async getSignalRegistryStats() { return this._fetch('/api/v1/signals/registry/stats'); }
    async getSignalRegistryForTicker(ticker: string) {
        return this._fetch(`/api/v1/signals/registry/ticker/${encodeURIComponent(ticker)}`);
    }
    async refreshSignalRegistry() {
        return this._fetch('/api/v1/signals/registry/refresh', { method: 'POST' });
    }
    async getModelFactory() { return this._fetch('/api/v1/models/factory'); }
    async getModelFactoryEntry(modelName: string) {
        return this._fetch(`/api/v1/models/factory/${encodeURIComponent(modelName)}`);
    }
    async ensemblePredict(ticker: string, regime: string) {
        return this._fetch('/api/v1/ensemble/predict', {
            method: 'POST',
            body: JSON.stringify({ ticker, regime }),
        });
    }

    // ── Universal search ──────────────────────────────────────

    async searchEverything(query: string) {
        return this._fetch(`/api/v1/search?q=${encodeURIComponent(query)}`);
    }

    // ── Vault ─────────────────────────────────────────────────

    async vaultNotes(params: Record<string, string> = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/vault/notes?${qs}`);
    }

    async vaultNote(id: string | number) {
        return this._fetch(`/api/v1/vault/notes/${id}`);
    }

    async vaultSearch(q: string, domain: string = '') {
        const qs = new URLSearchParams({ q, ...(domain && { domain }) }).toString();
        return this._fetch(`/api/v1/vault/search?${qs}`);
    }

    async vaultDashboard() {
        return this._fetch('/api/v1/vault/dashboard');
    }

    async vaultChangeStatus(id: string | number, status: string) {
        return this._fetch(`/api/v1/vault/notes/${id}/status`, {
            method: 'PATCH',
            body: JSON.stringify({ status }),
        });
    }

    async vaultCreateNote(data: unknown) {
        return this._fetch('/api/v1/vault/notes', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }

    async vaultSync() {
        return this._fetch('/api/v1/vault/sync', { method: 'POST' });
    }

    async vaultActions(noteId: string | number | null = null, limit: number = 50) {
        const qs = new URLSearchParams({ limit: String(limit), ...(noteId != null && { note_id: String(noteId) }) }).toString();
        return this._fetch(`/api/v1/vault/actions?${qs}`);
    }

    // ── WebSocket (first-message auth pattern) ────────────────

    connectWebSocket(onMessage: (data: unknown) => void): void {
        if (this._ws) {
            this._ws.close();
        }

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${protocol}//${window.location.host}/ws`;

        this._ws = new WebSocket(url);
        this._wsReconnectDelay = 1000;

        this._ws.onopen = () => {
            // Send auth token as first message instead of query param
            this._ws!.send(JSON.stringify({ type: 'auth', token: this.token }));
            console.log('WebSocket connected, auth sent');
            this._wsReconnectDelay = 1000;
        };

        this._ws.onmessage = (event: MessageEvent) => {
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

        this._ws.onerror = (err: Event) => {
            console.error('WebSocket error:', err);
        };
    }

    disconnectWebSocket(): void {
        if (this._ws) {
            this._ws.close();
            this._ws = null;
        }
    }

    // ── Trial Gem Hunter ──────────────────────────────────────

    async getTrialGems() { return this._fetch('/api/v1/trials/gems'); }
    async getTrialSignals(limit: number = 50, signalType: string | null = null) {
        let url = `/api/v1/trials/signals?limit=${limit}`;
        if (signalType) url += `&signal_type=${signalType}`;
        return this._fetch(url);
    }
    async getTrialCatalysts() { return this._fetch('/api/v1/trials/catalysts'); }
    async getTrialSponsors(limit: number = 20) { return this._fetch(`/api/v1/trials/sponsors?limit=${limit}`); }
    async getTrialStats() { return this._fetch('/api/v1/trials/stats'); }

    // ── Knowledge Base ────────────────────────────────────────

    async getKnowledgeSummary() { return this._fetch('/api/v1/knowledge/summary'); }
    async getKnowledge(params: string = '') { return this._fetch(`/api/v1/knowledge?${params}`); }
    async getKnowledgeItem(id: string | number) { return this._fetch(`/api/v1/knowledge/${id}`); }

    // ── Lever Map ─────────────────────────────────────────────

    async getLevers() { return this._fetch('/api/v1/intelligence/levers'); }
    async getLeverChain(event: string) { return this._fetch(`/api/v1/intelligence/levers/chain/${encodeURIComponent(event)}`); }
    async getLeverReport() { return this._fetch('/api/v1/intelligence/levers/report'); }

    // ── Market Diary ──────────────────────────────────────────

    async getDiaryList(limit: number = 365) { return this._fetch(`/api/v1/intelligence/diary/list?limit=${limit}`); }
    async getDiaryEntry(date: string) { return this._fetch(`/api/v1/intelligence/diary?date=${date}`); }
    async searchDiary(q: string) { return this._fetch(`/api/v1/intelligence/diary/search?q=${encodeURIComponent(q)}`); }
    async generateDiary() { return this._fetch('/api/v1/intelligence/diary/generate', { method: 'POST' }); }

    // ── Milestones ────────────────────────────────────────────

    async getMilestoneScorecard() { return this._fetch('/api/v1/intelligence/milestones/scorecard'); }
    async getTickerMilestones(ticker: string) { return this._fetch(`/api/v1/intelligence/milestones/${encodeURIComponent(ticker)}`); }

    // ── Canvas ────────────────────────────────────────────────

    async getCanvasBoards() { return this.get('/api/v1/canvas/boards'); }
    async createCanvasBoard(name: string, description: string = '') { return this.post('/api/v1/canvas/boards', { name, description }); }
    async getCanvasBoard(boardId: string) { return this.get(`/api/v1/canvas/boards/${boardId}`); }
    async updateCanvasBoard(boardId: string, updates: unknown) { return this._fetch(`/api/v1/canvas/boards/${boardId}`, { method: 'PUT', body: JSON.stringify(updates) }); }
    async deleteCanvasBoard(boardId: string) { return this._fetch(`/api/v1/canvas/boards/${boardId}`, { method: 'DELETE' }); }
    async addCanvasNode(boardId: string, node: unknown) { return this.post(`/api/v1/canvas/boards/${boardId}/nodes`, node); }
    async updateCanvasNode(boardId: string, nodeId: string, updates: unknown) { return this._fetch(`/api/v1/canvas/boards/${boardId}/nodes/${nodeId}`, { method: 'PUT', body: JSON.stringify(updates) }); }
    async deleteCanvasNode(boardId: string, nodeId: string) { return this._fetch(`/api/v1/canvas/boards/${boardId}/nodes/${nodeId}`, { method: 'DELETE' }); }
    async addCanvasEdge(boardId: string, edge: unknown) { return this.post(`/api/v1/canvas/boards/${boardId}/edges`, edge); }
    async deleteCanvasEdge(boardId: string, edgeId: string) { return this._fetch(`/api/v1/canvas/boards/${boardId}/edges/${edgeId}`, { method: 'DELETE' }); }
    async saveCanvasGraph(boardId: string, graph: unknown) { return this._fetch(`/api/v1/canvas/boards/${boardId}/graph`, { method: 'PUT', body: JSON.stringify(graph) }); }

    // ── Canvas Expansion ──────────────────────────────────────

    async expandCanvasNode(boardId: string, nodeId: string) { return this.post(`/api/v1/canvas/boards/${boardId}/expand/${nodeId}`); }
    async suggestCanvasConnections(boardId: string) { return this.post(`/api/v1/canvas/boards/${boardId}/suggest-connections`); }
    async findCanvasPath(boardId: string, sourceId: string, targetId: string) { return this.post(`/api/v1/canvas/boards/${boardId}/path`, { source_node_id: sourceId, target_node_id: targetId }); }

    // ── Canvas LLM ────────────────────────────────────────────

    async explainCanvasConnection(boardId: string, sourceNodeId: string, targetNodeId: string) {
        return this.post('/api/v1/canvas/explain', {
            source_node_id: sourceNodeId,
            target_node_id: targetNodeId,
            board_id: boardId,
        });
    }

    // ── Canvas Prediction ─────────────────────────────────────

    async createCanvasPrediction(payload: unknown) {
        return this.post('/api/v1/canvas/predict', payload);
    }

    // ── Canvas Data Helpers ───────────────────────────────────

    /** Fetch price history for a ticker, normalized to {date, close} for ChartNode. */
    async getCanvasChartPrices(ticker: string, period: string = '3M'): Promise<Array<{ date: string; close: number }>> {
        const qs = period ? `?period=${encodeURIComponent(period)}` : '';
        const res = await this._fetch<{ price_history?: Array<{ date: string; value?: number; close?: number }> }>(`/api/v1/watchlist/${encodeURIComponent(ticker)}/analysis${qs}`);
        const history = res.price_history || [];
        return history.map(p => ({ date: p.date, close: p.value ?? p.close ?? 0 }));
    }

    /** Fetch intelligence events for a ticker, normalized for TimelineNode. */
    async getCanvasTimelineEvents(ticker: string, days: number = 90): Promise<Array<{ date: string; type: string; description: string }>> {
        const res = await this.getEventTimeline(ticker, days) as { events?: Array<{ event_date?: string; date?: string; event_type?: string; type?: string; description?: string; title?: string }> } | Array<{ event_date?: string; date?: string; event_type?: string; type?: string; description?: string; title?: string }>;
        const events = (Array.isArray(res) ? res : (res as { events?: unknown[] }).events) || [];
        return (events as Array<{ event_date?: string; date?: string; event_type?: string; type?: string; description?: string; title?: string }>).map(e => ({
            date: e.event_date || e.date || '',
            type: e.event_type || e.type || 'default',
            description: e.description || e.title || '',
        }));
    }
}

export const api = new GRIDApi();
export type { GRIDApi, FetchOptions };
