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

    /** Public GET helper — delegates to _fetch. */
    async get(path) {
        return this._fetch(path);
    }

    /** Public POST helper — delegates to _fetch. */
    async post(path, body = {}) {
        return this._fetch(path, { method: 'POST', body: JSON.stringify(body) });
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
            const body = await response.text().catch(() => '');
            let message = response.statusText;
            try {
                const parsed = JSON.parse(body);
                message = parsed.detail || parsed.message || message;
            } catch (_) {
                if (body) message = body;
            }

            // Only treat 401 as session expiry for non-auth endpoints
            // (login/register 401s mean wrong credentials, not expired session)
            if (response.status === 401 && !path.startsWith('/api/v1/auth/login') && !path.startsWith('/api/v1/auth/register')) {
                this.token = null;
                window.location.hash = '#/login';
            }

            throw new GRIDApiError(response.status, message);
        }

        return await response.json();
    }

    // Auth
    async login(password, username = null) {
        const body = username
            ? { password, username }
            : { password };
        const data = await this._fetch('/api/v1/auth/login', {
            method: 'POST',
            body: JSON.stringify(body),
        });
        this.token = data.token;
        return data;
    }

    async register(username, password) {
        const data = await this._fetch('/api/v1/auth/register', {
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

    // User management (admin only)
    async listUsers() {
        return this._fetch('/api/v1/auth/users');
    }

    async createUser(username, password, role = 'contributor') {
        return this._fetch('/api/v1/auth/users', {
            method: 'POST',
            body: JSON.stringify({ username, password, role }),
        });
    }

    async deleteUser(username) {
        return this._fetch(`/api/v1/auth/users/${encodeURIComponent(username)}`, {
            method: 'DELETE',
        });
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
    async getAllActiveRegimes() { return this._fetch('/api/v1/regime/all-active'); }
    async getRegimeSynthesis() { return this._fetch('/api/v1/regime/synthesis'); }
    async getRegimeWeights() { return this._fetch('/api/v1/regime/weights'); }
    async updateRegimeWeights(weights) { return this._fetch('/api/v1/regime/weights', { method: 'PUT', body: JSON.stringify({ weights }) }); }
    async simulateRegimeWeights(weights) { return this._fetch('/api/v1/regime/simulate', { method: 'POST', body: JSON.stringify({ weights }) }); }

    // Strategy
    async getActiveStrategies() { return this._fetch('/api/v1/strategy/active'); }
    async getStrategyForRegime(state) { return this._fetch(`/api/v1/strategy/for-regime/${encodeURIComponent(state)}`); }
    async assignStrategy(data) {
        return this._fetch('/api/v1/strategy/assign', {
            method: 'POST', body: JSON.stringify(data),
        });
    }

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
    async getDiscoveryCorrelationMatrix(period = 90, regime = 'all') {
        return this._fetch(`/api/v1/discovery/correlation-matrix?period=${period}&regime=${encodeURIComponent(regime)}`);
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

    // Watchlist
    async getWatchlist(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/watchlist/?${qs}`);
    }
    async addToWatchlist(data) {
        return this._fetch('/api/v1/watchlist/', {
            method: 'POST', body: JSON.stringify(data),
        });
    }
    async removeFromWatchlist(ticker) {
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}`, {
            method: 'DELETE',
        });
    }
    async getTickerAnalysis(ticker, period = '3M') {
        const qs = period && period !== '3M' ? `?period=${encodeURIComponent(period)}` : '';
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}/analysis${qs}`);
    }
    async getWatchlistEnriched(limit = 20) {
        return this._fetch(`/api/v1/watchlist/enriched?limit=${limit}`);
    }
    async searchWatchlistTickers(query) {
        return this._fetch(`/api/v1/watchlist/search?q=${encodeURIComponent(query)}`);
    }
    async getTickerOverview(ticker) {
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
    async getTickerEdge(ticker) {
        return this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}/edge`);
    }
    async getFeatureTimeframes(feature, periods = '5d,5w,3m,1y,5y') {
        return this._fetch(`/api/v1/signals/timeframes?feature=${encodeURIComponent(feature)}&periods=${encodeURIComponent(periods)}`);
    }
    async promoteHypothesis(hypothesisId) {
        return this._fetch(`/api/v1/discovery/hypotheses/${hypothesisId}/promote`, { method: 'POST' });
    }
    async getHypothesisResults(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/discovery/hypotheses/results?${qs}`);
    }

    // Settings
    async getSettings() { return this._fetch('/api/v1/system/settings'); }
    async updateSettings(data) {
        return this._fetch('/api/v1/system/settings', { method: 'POST', body: JSON.stringify(data) });
    }
    async getApiKeys() { return this._fetch('/api/v1/system/api-keys'); }
    async getServices() { return this._fetch('/api/v1/system/services'); }
    async getHermesStatus(limit = 20) { return this._fetch(`/api/v1/system/hermes-status?limit=${limit}`); }
    async getFreshness() { return this._fetch('/api/v1/system/freshness'); }
    async getHealth() { return this._fetch('/api/v1/system/health'); }
    async getPipelineHealth() { return this._fetch('/api/v1/system/pipeline-health'); }
    async getArchitecture() { return this._fetch('/api/v1/system/architecture'); }

    // Signals
    async getSignals() { return this._fetch('/api/v1/signals'); }
    async getSignalSnapshot() { return this._fetch('/api/v1/signals/snapshot'); }
    async getCelestialSignals() { return this._fetch('/api/v1/signals/celestial'); }
    async getCrucixSignals() { return this._fetch('/api/v1/signals/crucix'); }
    async getConvictionScores(minScore = 20) { return this._fetch(`/api/v1/signals/conviction?min_score=${minScore}`); }
    async getConvictionTicker(ticker) { return this._fetch(`/api/v1/signals/conviction/${encodeURIComponent(ticker)}`); }

    // Options
    async getOptionsSignals(ticker = '', limit = 50) {
        const qs = new URLSearchParams({ ...(ticker && { ticker }), limit: String(limit) }).toString();
        return this._fetch(`/api/v1/options/signals?${qs}`);
    }
    async scanMispricing(minScore = 5.0) {
        return this._fetch(`/api/v1/options/scan?min_score=${minScore}`);
    }
    async get100xOpportunities() { return this._fetch('/api/v1/options/100x'); }
    async getGEXProfile(ticker) {
        return this._fetch(`/api/v1/derivatives/gex/${encodeURIComponent(ticker)}`);
    }
    async getVannaCharm(ticker) {
        return this._fetch(`/api/v1/derivatives/vanna-charm/${encodeURIComponent(ticker)}`);
    }
    async getFlowTimeline(ticker, days = 90) {
        return this._fetch(`/api/v1/derivatives/flow-timeline/${encodeURIComponent(ticker)}?days=${days}`);
    }

    // Options Recommendations
    async getOptionsRecommendations(ticker = '') {
        const qs = ticker ? `?ticker=${encodeURIComponent(ticker)}` : '';
        return this._fetch(`/api/v1/options/recommendations${qs}`);
    }
    async refreshOptionsRecommendations() {
        return this._fetch('/api/v1/options/recommendations/refresh', { method: 'POST' });
    }
    async getOptionsRecommendationHistory(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/options/recommendations/history?${qs}`);
    }

    async getOptionsHistory(ticker = '', days = 30, only100x = false, limit = 50) {
        const params = new URLSearchParams({ days: String(days), limit: String(limit) });
        if (ticker) params.set('ticker', ticker);
        if (only100x) params.set('only_100x', 'true');
        return this._fetch(`/api/v1/options/history?${params}`);
    }

    // Physics — news momentum
    async getNewsMomentum(lookbackDays = 63) {
        return this._fetch(`/api/v1/physics/momentum?lookback_days=${lookbackDays}`);
    }

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
    async runOrthogonality() {
        return this._fetch('/api/v1/discovery/orthogonality', { method: 'POST' });
    }
    async getSectorFlows() { return this._fetch('/api/v1/flows/sectors'); }
    async getSankeyData(asOf = null) {
        const qs = asOf ? `?as_of=${asOf}` : '';
        return this._fetch(`/api/v1/flows/sankey${qs}`);
    }
    async getSectorDetail(sectorName) { return this._fetch(`/api/v1/flows/sectors/${encodeURIComponent(sectorName)}/detail`); }
    async getMoneyMap() { return this._fetch('/api/v1/flows/money-map'); }
    async getSectorDrill(sectorName) { return this._fetch(`/api/v1/flows/sector/${encodeURIComponent(sectorName)}`); }
    async getCompanyDrill(ticker) { return this._fetch(`/api/v1/flows/company/${encodeURIComponent(ticker)}`); }
    async getAggregatedFlows(sector = null, period = 'weekly', days = 30) {
        const params = new URLSearchParams({ period, days });
        if (sector) params.set('sector', sector);
        return this._fetch(`/api/v1/flows/aggregated?${params}`);
    }
    async getFlowMomentum(ticker) {
        return this._fetch(`/api/v1/flows/momentum/${encodeURIComponent(ticker)}`);
    }

    // Flow Engine v2
    async getFlowMapV2() { return this._fetch('/api/v1/flows/flow-map-v2'); }
    async getJunctionPoints() { return this._fetch('/api/v1/flows/junction-points'); }
    async getFlowLayers() { return this._fetch('/api/v1/flows/layers'); }
    async getFlowLayerDetail(layerId) { return this._fetch(`/api/v1/flows/layers/${encodeURIComponent(layerId)}`); }
    async getFlowWaterfall(source = 'fed') { return this._fetch(`/api/v1/flows/waterfall?source=${encodeURIComponent(source)}`); }
    async getFlowOrthogonality() { return this._fetch('/api/v1/flows/orthogonality'); }
    async generateFlowImage(type, style = 'dark') {
        return this._fetch(`/api/v1/flows/generate-image/${encodeURIComponent(type)}?style=${style}`);
    }
    async getCdsDashboard() { return this._fetch('/api/v1/flows/cds'); }
    async getCdsHistory(seriesKey, days = 365) {
        return this._fetch(`/api/v1/flows/cds/history/${encodeURIComponent(seriesKey)}?days=${days}`);
    }

    // Audio Briefing (flow-engine powered, OpenAI TTS)
    async getFlowBriefing(audio = true) {
        return this._fetch(`/api/v1/flows/briefing?audio=${audio}`);
    }
    getFlowBriefingAudioUrl(filename = null) {
        const path = filename
            ? `/api/v1/flows/briefing/audio/${encodeURIComponent(filename)}`
            : '/api/v1/flows/briefing/audio';
        return `${this.baseUrl}${path}?token=${encodeURIComponent(this.token || '')}`;
    }
    async listFlowBriefings() {
        return this._fetch('/api/v1/flows/briefing/list');
    }
    async getFlowBriefingDetail(filename) {
        return this._fetch(`/api/v1/flows/briefing/detail/${encodeURIComponent(filename)}`);
    }

    // Deep Dives
    async getDeepDives(days = 90) {
        return this._fetch(`/api/v1/intelligence/deep-dives?days=${days}`);
    }
    async getDeepDive(id) {
        return this._fetch(`/api/v1/intelligence/deep-dives/${id}`);
    }
    async triggerDeepDive() {
        return this._fetch('/api/v1/intelligence/deep-dives/generate', { method: 'POST' });
    }

    // Research Archive
    async getResearchArchive(days = 365) {
        return this._fetch(`/api/v1/intelligence/archive?days=${days}`);
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
    async getNewsEnergy(lookbackDays = 30, asOf) {
        const params = new URLSearchParams({ lookback_days: lookbackDays });
        if (asOf) params.set('as_of', asOf);
        return this._fetch(`/api/v1/physics/news-energy?${params}`);
    }
    async getPhysicsDashboard(asOf) {
        const qs = asOf ? `?as_of=${asOf}` : '';
        return this._fetch(`/api/v1/physics/dashboard${qs}`);
    }

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
    async getCapitalFlowResearch(sectors = null, asOf = null, force = false) {
        return this._fetch('/api/v1/ollama/capital-flows', {
            method: 'POST',
            body: JSON.stringify({ sectors, as_of: asOf, force }),
        });
    }

    // Ask GRID (Chat)
    async askGRID(question, contextTicker = null, history = [], timeframe = null) {
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

    // Actor Network
    async getActorNetwork() { return this._fetch('/api/v1/intelligence/actor-network'); }
    async getPowerMap(sectorName) { return this._fetch(`/api/v1/intelligence/power-map/${encodeURIComponent(sectorName)}`); }
    async getEgoGraphSearch(q) { return this._fetch(`/api/v1/intelligence/ego-graph/search?q=${encodeURIComponent(q)}`); }
    async getEgoGraph(actorId, depth = 2, maxNodes = 80) { return this._fetch(`/api/v1/intelligence/ego-graph/${encodeURIComponent(actorId)}?depth=${depth}&max_nodes=${maxNodes}`); }
    async getGrandPowerMap(limit = 50) { return this._fetch(`/api/v1/intelligence/grand-power-map?limit=${limit}`); }
    async getActorDetail(id) { return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(id)}`); }
    async getActorNeighborhood(id, depth = 3, maxNodes = 2000) {
        return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(id)}/neighborhood?depth=${depth}&max_nodes=${maxNodes}`);
    }
    async getActorPath(fromId, toId) {
        return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(fromId)}/path/${encodeURIComponent(toId)}`);
    }
    async getActorConnections(id) {
        return this._fetch(`/api/v1/intelligence/actor/${encodeURIComponent(id)}/connections`);
    }
    async getSpiderStats() {
        return this._fetch('/api/v1/intelligence/spider/stats');
    }

    // Intelligence Dashboard (unified)
    async getIntelDashboard() { return this._fetch('/api/v1/intelligence/dashboard'); }

    async getTrustScores() {
        const data = await this._fetch('/api/v1/intelligence/dashboard');
        if (data?.error) return data;
        return { sources: data?.trust?.top_sources ?? [] };
    }

    async getConvergenceAlerts() {
        const data = await this._fetch('/api/v1/intelligence/dashboard');
        if (data?.error) return data;
        return { alerts: data?.trust?.convergence_events ?? [] };
    }

    // Cross-Reference (Lie Detector)
    async getCrossReference() { return this._fetch('/api/v1/intelligence/cross-reference'); }
    async getCrossRefHistory() { return this._fetch('/api/v1/intelligence/cross-reference/history'); }

    // Regime Analog Engine
    async getRegimeAnalog() { return this._fetch('/api/v1/intelligence/regime'); }
    async getRegimeAnalogs(n = 20) { return this._fetch(`/api/v1/intelligence/regime/analogs?n=${n}&include_timesfm=true`); }
    async getRegimeHistory(days = 365) { return this._fetch(`/api/v1/intelligence/regime/history?days=${days}`); }

    // Globe
    async getGlobeData() { return this._fetch('/api/v1/intelligence/globe'); }

    // Risk Map
    async getRiskMap() { return this._fetch('/api/v1/intelligence/risk-map'); }

    // Unified Thesis
    async getThesis() { return this._fetch('/api/v1/intelligence/thesis'); }

    // Earnings Calendar
    async getEarningsCalendar(daysAhead = 30) {
        return this._fetch(`/api/v1/earnings/calendar?days_ahead=${daysAhead}`);
    }
    async getRecentEarnings(daysBack = 30) {
        return this._fetch(`/api/v1/earnings/recent?days_back=${daysBack}`);
    }
    async getEarningsSurprise(ticker) {
        return this._fetch(`/api/v1/earnings/surprise/${encodeURIComponent(ticker)}`);
    }
    async predictEarnings(ticker) {
        return this._fetch(`/api/v1/earnings/predict/${encodeURIComponent(ticker)}`, { method: 'POST' });
    }
    async getEarningsScorecard() {
        return this._fetch('/api/v1/earnings/scorecard');
    }
    async getEarningsHistory(ticker, limit = 20) {
        return this._fetch(`/api/v1/earnings/history/${encodeURIComponent(ticker)}?limit=${limit}`);
    }
    async runEarningsCycle() {
        return this._fetch('/api/v1/earnings/cycle', { method: 'POST' });
    }

    // Trend Tracker
    async getTrends(days = 90) { return this._fetch(`/api/v1/intelligence/trends?days=${days}`); }

    // Associations
    async getCorrelationMatrix(days = 252) {
        return this._fetch(`/api/v1/associations/correlation-matrix?days=${days}`);
    }
    async getSmartHeatmap(family = null, orthogonalOnly = true) {
        let url = `/api/v1/discovery/smart-heatmap?orthogonal_only=${orthogonalOnly}`;
        if (family) url += `&family=${encodeURIComponent(family)}`;
        return this._fetch(url);
    }
    async getTimeseries(featureNames, days = 30) {
        return this._fetch(`/api/v1/signals/timeseries?features=${encodeURIComponent(featureNames.join(','))}&days=${days}`);
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

    // Snapshots
    async getSnapshotLatest(category, n = 1) {
        return this._fetch(`/api/v1/snapshots/latest/${encodeURIComponent(category)}?n=${n}`);
    }
    async getSnapshotHistory(category, startDate = null, endDate = null) {
        const params = new URLSearchParams();
        if (startDate) params.set('start_date', startDate);
        if (endDate) params.set('end_date', endDate);
        const qs = params.toString();
        return this._fetch(`/api/v1/snapshots/history/${encodeURIComponent(category)}${qs ? '?' + qs : ''}`);
    }
    async compareSnapshots(category, dateA, dateB) {
        return this._fetch(
            `/api/v1/snapshots/compare/${encodeURIComponent(category)}?date_a=${encodeURIComponent(dateA)}&date_b=${encodeURIComponent(dateB)}`
        );
    }
    async getOperatorIssues(daysBack = 30, category = null, severity = null) {
        const params = new URLSearchParams({ days_back: String(daysBack) });
        if (category) params.set('category', category);
        if (severity) params.set('severity', severity);
        return this._fetch(`/api/v1/snapshots/issues?${params}`);
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

    // Paper Trading Strategies
    async getPaperStrategies() { return this._fetch('/api/v1/trading/strategies'); }
    async getStrategyHistory(strategyId) {
        return this._fetch(`/api/v1/trading/strategies/${encodeURIComponent(strategyId)}/history`);
    }
    async promoteToStrategy(data) {
        return this._fetch('/api/v1/trading/strategies/promote', {
            method: 'POST', body: JSON.stringify(data),
        });
    }
    async killStrategy(strategyId) {
        return this._fetch(`/api/v1/trading/strategies/${encodeURIComponent(strategyId)}/kill`, { method: 'POST' });
    }
    async getBacktestWinners(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/discovery/backtest-results?${qs}`);
    }
    async getTradingDashboard() { return this._fetch('/api/v1/trading/dashboard'); }

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

    // Push Notifications
    async getVapidKey() { return this._fetch('/api/v1/notifications/vapid-key'); }
    async subscribePush(subscription, userAgent = '') {
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
    async unsubscribePush(endpoint) {
        return this._fetch('/api/v1/notifications/unsubscribe', {
            method: 'DELETE',
            body: JSON.stringify({ endpoint }),
        });
    }
    async getNotificationPreferences(endpoint) {
        return this._fetch(`/api/v1/notifications/preferences?endpoint=${encodeURIComponent(endpoint)}`);
    }
    async updateNotificationPreferences(endpoint, prefs) {
        return this._fetch('/api/v1/notifications/preferences', {
            method: 'PUT',
            body: JSON.stringify({ endpoint, ...prefs }),
        });
    }
    async testPush(subscription) {
        const sub = subscription.toJSON();
        return this._fetch('/api/v1/notifications/test', {
            method: 'POST',
            body: JSON.stringify({
                endpoint: sub.endpoint,
                keys: sub.keys,
            }),
        });
    }

    // Intelligence — Event Timeline
    async getEventTimeline(ticker, days = 90) {
        return this._fetch(`/api/v1/intelligence/events?ticker=${encodeURIComponent(ticker)}&days=${days}&include_lead_times=true`);
    }
    async getRecurringPatterns(minOccurrences = 3) {
        return this._fetch(`/api/v1/intelligence/patterns?min_occurrences=${minOccurrences}`);
    }

    // Intelligence — Forensics ("Why did this move?")
    async getForensicReports(ticker, days = 90) {
        return this._fetch(`/api/v1/intelligence/forensics/${encodeURIComponent(ticker)}?days=${days}`);
    }
    async analyzeForensicMove(ticker, date) {
        return this._fetch(`/api/v1/intelligence/forensics/${encodeURIComponent(ticker)}/analyze?date=${encodeURIComponent(date)}`, { method: 'POST' });
    }

    // Intelligence — Causation
    async getCausalLinks(ticker) {
        return this._fetch(`/api/v1/intelligence/causation?ticker=${encodeURIComponent(ticker)}`);
    }

    // Oracle
    async getOracleScoreboard() { return this._fetch('/api/v1/oracle/scoreboard'); }
    async getOraclePredictions(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/oracle/predictions?${qs}`);
    }
    async getOracleLatest() { return this._fetch('/api/v1/oracle/latest'); }
    async publishOraclePrediction(data) {
        return this._fetch('/api/v1/oracle/publish', {
            method: 'POST', body: JSON.stringify(data),
        });
    }

    // Signal Registry & Ensemble
    async getSignalRegistry(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/signals/registry${qs ? '?' + qs : ''}`);
    }
    async getSignalRegistryStats() { return this._fetch('/api/v1/signals/registry/stats'); }
    async getSignalRegistryForTicker(ticker) {
        return this._fetch(`/api/v1/signals/registry/ticker/${encodeURIComponent(ticker)}`);
    }
    async refreshSignalRegistry() {
        return this._fetch('/api/v1/signals/registry/refresh', { method: 'POST' });
    }
    async getModelFactory() { return this._fetch('/api/v1/models/factory'); }
    async getModelFactoryEntry(modelName) {
        return this._fetch(`/api/v1/models/factory/${encodeURIComponent(modelName)}`);
    }
    async ensemblePredict(ticker, regime) {
        return this._fetch('/api/v1/ensemble/predict', {
            method: 'POST',
            body: JSON.stringify({ ticker, regime }),
        });
    }

    // Universal search
    async searchEverything(query) {
        return this._fetch(`/api/v1/search?q=${encodeURIComponent(query)}`);
    }

    // ── Vault ────────────────────────────────────────────────────
    async vaultNotes(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this._fetch(`/api/v1/vault/notes?${qs}`);
    }

    async vaultNote(id) {
        return this._fetch(`/api/v1/vault/notes/${id}`);
    }

    async vaultSearch(q, domain = '') {
        const qs = new URLSearchParams({ q, ...(domain && { domain }) }).toString();
        return this._fetch(`/api/v1/vault/search?${qs}`);
    }

    async vaultDashboard() {
        return this._fetch('/api/v1/vault/dashboard');
    }

    async vaultChangeStatus(id, status) {
        return this._fetch(`/api/v1/vault/notes/${id}/status`, {
            method: 'PATCH',
            body: JSON.stringify({ status }),
        });
    }

    async vaultCreateNote(data) {
        return this._fetch('/api/v1/vault/notes', {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }

    async vaultSync() {
        return this._fetch('/api/v1/vault/sync', { method: 'POST' });
    }

    async vaultActions(noteId = null, limit = 50) {
        const qs = new URLSearchParams({ limit, ...(noteId && { note_id: noteId }) }).toString();
        return this._fetch(`/api/v1/vault/actions?${qs}`);
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
        this._wsMaxDelay = 30000;

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
            const delay = this._wsReconnectDelay;
            // Jitter: ±25% to prevent thundering herd
            const jitter = delay * (0.75 + Math.random() * 0.5);
            this._wsReconnectDelay = Math.min(delay * 2, this._wsMaxDelay);
            console.log(`WebSocket disconnected, reconnecting in ${Math.round(jitter)}ms...`);
            setTimeout(() => {
                if (this.token) {
                    this.connectWebSocket(onMessage);
                }
            }, jitter);
        };

        this._ws.onerror = () => {
            // onclose fires after onerror — reconnect handled there
        };
    }

    disconnectWebSocket() {
        if (this._ws) {
            this._ws.close();
            this._ws = null;
        }
    }

    // ── Trial Gem Hunter ────────────────────────────────────────────────────
    async getTrialGems() { return this._fetch('/api/v1/trials/gems'); }
    async getTrialSignals(limit = 50, signalType = null) {
        let url = `/api/v1/trials/signals?limit=${limit}`;
        if (signalType) url += `&signal_type=${signalType}`;
        return this._fetch(url);
    }
    async getTrialCatalysts() { return this._fetch('/api/v1/trials/catalysts'); }
    async getTrialSponsors(limit = 20) { return this._fetch(`/api/v1/trials/sponsors?limit=${limit}`); }
    async getTrialStats() { return this._fetch('/api/v1/trials/stats'); }

    // ── Knowledge Base ──────────────────────────────────────────────────────
    async getKnowledgeSummary() { return this._fetch('/api/v1/knowledge/summary'); }
    async getKnowledge(params = '') { return this._fetch(`/api/v1/knowledge?${params}`); }
    async getKnowledgeItem(id) { return this._fetch(`/api/v1/knowledge/${id}`); }

    // ── Lever Map ───────────────────────────────────────────────────────────
    async getLevers() { return this._fetch('/api/v1/intelligence/levers'); }
    async getLeverChain(event) { return this._fetch(`/api/v1/intelligence/levers/chain/${encodeURIComponent(event)}`); }
    async getLeverReport() { return this._fetch('/api/v1/intelligence/levers/report'); }

    // ── Market Diary ────────────────────────────────────────────────────────
    async getDiaryList(limit = 365) { return this._fetch(`/api/v1/intelligence/diary/list?limit=${limit}`); }
    async getDiaryEntry(date) { return this._fetch(`/api/v1/intelligence/diary?date=${date}`); }
    async searchDiary(q) { return this._fetch(`/api/v1/intelligence/diary/search?q=${encodeURIComponent(q)}`); }
    async generateDiary() { return this._fetch('/api/v1/intelligence/diary/generate', { method: 'POST' }); }

    // ── Milestones ──────────────────────────────────────────────────────────
    async getMilestoneScorecard() { return this._fetch('/api/v1/intelligence/milestones/scorecard'); }
    async getTickerMilestones(ticker) { return this._fetch(`/api/v1/intelligence/milestones/${encodeURIComponent(ticker)}`); }
    async getCatalystTimeline(ticker, monthsForward = 12, monthsBack = 6) {
        return this._fetch(`/api/v1/valuation/catalyst-timeline/${encodeURIComponent(ticker)}?months_forward=${monthsForward}&months_back=${monthsBack}`);
    }

    // ── Canvas ─────────────────────────────────────────────────────────────
    async getCanvasBoards() { return this.get('/api/v1/canvas/boards'); }
    async createCanvasBoard(name, description = '') { return this.post('/api/v1/canvas/boards', { name, description }); }
    async getCanvasBoard(boardId) { return this.get(`/api/v1/canvas/boards/${boardId}`); }
    async updateCanvasBoard(boardId, updates) { return this._fetch(`/api/v1/canvas/boards/${boardId}`, { method: 'PUT', body: JSON.stringify(updates) }); }
    async deleteCanvasBoard(boardId) { return this._fetch(`/api/v1/canvas/boards/${boardId}`, { method: 'DELETE' }); }
    async addCanvasNode(boardId, node) { return this.post(`/api/v1/canvas/boards/${boardId}/nodes`, node); }
    async updateCanvasNode(boardId, nodeId, updates) { return this._fetch(`/api/v1/canvas/boards/${boardId}/nodes/${nodeId}`, { method: 'PUT', body: JSON.stringify(updates) }); }
    async deleteCanvasNode(boardId, nodeId) { return this._fetch(`/api/v1/canvas/boards/${boardId}/nodes/${nodeId}`, { method: 'DELETE' }); }
    async addCanvasEdge(boardId, edge) { return this.post(`/api/v1/canvas/boards/${boardId}/edges`, edge); }
    async deleteCanvasEdge(boardId, edgeId) { return this._fetch(`/api/v1/canvas/boards/${boardId}/edges/${edgeId}`, { method: 'DELETE' }); }
    async saveCanvasGraph(boardId, graph) { return this._fetch(`/api/v1/canvas/boards/${boardId}/graph`, { method: 'PUT', body: JSON.stringify(graph) }); }

    // ── Canvas Expansion ──────────────────────────────────────────────────
    async expandCanvasNode(boardId, nodeId, depth = 1) { return this.post(`/api/v1/canvas/boards/${boardId}/expand/${nodeId}?depth=${depth}`); }
    async suggestCanvasConnections(boardId) { return this.post(`/api/v1/canvas/boards/${boardId}/suggest-connections`); }
    async findCanvasPath(boardId, sourceId, targetId) { return this.post(`/api/v1/canvas/boards/${boardId}/path`, { source_node_id: sourceId, target_node_id: targetId }); }

    // ── Canvas LLM ───────────────────────────────────────────────────────
    async explainCanvasConnection(boardId, sourceNodeId, targetNodeId) {
        return this.post('/api/v1/canvas/explain', {
            source_node_id: sourceNodeId,
            target_node_id: targetNodeId,
            board_id: boardId,
        });
    }

    // ── Canvas Prediction ─────────────────────────────────────────────
    async createCanvasPrediction(payload) {
        return this.post('/api/v1/canvas/predict', payload);
    }

    // ── Canvas Data Helpers ──────────────────────────────────────────────
    /** Fetch price history for a ticker, normalized to {date, close} for ChartNode. */
    async getCanvasChartPrices(ticker, period = '3M') {
        const qs = period ? `?period=${encodeURIComponent(period)}` : '';
        const res = await this._fetch(`/api/v1/watchlist/${encodeURIComponent(ticker)}/analysis${qs}`);
        const history = res.price_history || [];
        return history.map(p => ({ date: p.date, close: p.value ?? p.close ?? 0 }));
    }

    /** Fetch intelligence events for a ticker, normalized for TimelineNode. */
    async getCanvasTimelineEvents(ticker, days = 90) {
        const res = await this.getEventTimeline(ticker, days);
        const events = res.events || res || [];
        return events.map(e => ({
            date: e.event_date || e.date,
            type: e.event_type || e.type || 'default',
            description: e.description || e.title || '',
        }));
    }
}

export const api = new GRIDApi();
export { GRIDApiError };
