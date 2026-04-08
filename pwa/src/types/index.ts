// Core domain types used across the GRID application

export interface Actor {
    id: string;
    name: string;
    category?: string;
    tier?: string;
    influence_score?: number;
    trust_score?: number;
    net_worth_estimate?: number;
    metadata?: Record<string, unknown>;
}

export interface Signal {
    id: string;
    signal_type: string;
    signal_date: string;
    ticker?: string;
    actor?: string;
    direction?: string;
    magnitude?: number;
    description?: string;
    confidence?: string;
}

export interface Hypothesis {
    id: string;
    thesis: string;
    pattern_type: string;
    confidence: number;
    status: string;
    direction?: string;
    ticker?: string;
    role?: string;
    pair_id?: string;
    evidence?: Record<string, unknown>;
    test_criteria?: Record<string, unknown>;
}

export interface CanvasBoard {
    id: string;
    name: string;
    description?: string;
    created_at: string;
    updated_at: string;
}

export interface CanvasNode {
    id: string;
    board_id: string;
    node_type: string;
    label?: string;
    position_x: number;
    position_y: number;
    entity_id?: string;
    data?: Record<string, unknown>;
}

export interface CanvasEdge {
    id: string;
    board_id: string;
    source_node_id: string;
    target_node_id: string;
    edge_type: string;
    label?: string;
    data?: Record<string, unknown>;
}

export interface SearchResult {
    source_type: string;
    source_id: string;
    title: string;
    snippet?: string;
    relevance: number;
}

export interface GeoFlow {
    from_lat: number;
    from_lng: number;
    from_name: string;
    to_lat: number;
    to_lng: number;
    to_name: string;
    amount: number;
    confidence: string;
    date: string;
    type: string;
}

export interface CausalLink {
    id: string;
    cause_type: string;
    cause_date: string;
    cause_description: string;
    effect_date: string;
    effect_description: string;
    probability: number;
    lead_time_days: number;
    lever_actor?: string;
}

export interface RegimeState {
    state: string;
    confidence: number;
    timestamp?: string;
}

export interface JournalEntry {
    id: string;
    model_version_id?: string;
    ticker?: string;
    direction?: string;
    entry_price?: number;
    target_price?: number;
    stop_loss?: number;
    confidence?: number;
    thesis?: string;
    outcome?: string;
    outcome_recorded_at?: string;
    created_at: string;
}

export interface PricePoint {
    date: string;
    close: number;
    value?: number;
}

export interface TimelineEvent {
    date: string;
    type: string;
    description: string;
}

export interface Notification {
    id: number;
    type: string;
    message: string;
}

export interface LiveAlert {
    id: number;
    severity: string;
    timestamp: string;
    [key: string]: unknown;
}

export interface LiveRecommendation {
    id: number;
    ticker?: string;
    direction?: string;
    strike?: string | number;
    timestamp: string;
    [key: string]: unknown;
}

export interface PushPreferences {
    trade_recommendations: boolean;
    convergence_alerts: boolean;
    regime_changes: boolean;
    red_flags: boolean;
    price_alerts: boolean;
    price_alert_threshold: number;
}

// Confidence levels used throughout
export type Confidence = 'confirmed' | 'derived' | 'estimated' | 'rumored' | 'inferred';

// Node types for canvas
export type NodeType = 'actor' | 'company' | 'hypothesis' | 'signal' | 'note' | 'evidence' | 'chart' | 'timeline';

// Theme names
export type ThemeName = 'dark' | 'midnight' | 'terminal';

// Store slice types for typed Zustand stores

export interface AuthState {
    token: string | null;
    isAuthenticated: boolean;
    userRole: string;
    username: string;
    setAuth: (token: string, role?: string, username?: string) => void;
    clearAuth: () => void;
}

export interface UiState {
    theme: ThemeName | string;
    activeView: string;
    loading: Record<string, boolean>;
    errors: Record<string, string | null>;
    notifications: Notification[];
    setTheme: (name: string) => void;
    setActiveView: (view: string) => void;
    setLoading: (key: string, value: boolean) => void;
    setError: (key: string, error: string | null) => void;
    addNotification: (type: string, message: string) => void;
    removeNotification: (id: number) => void;
}

export interface DomainState {
    systemStatus: unknown;
    latestSignals: unknown;
    currentRegime: RegimeState | null;
    regimeHistory: unknown[];
    journalEntries: JournalEntry[];
    journalStats: unknown;
    productionModels: Record<string, unknown>;
    allModels: unknown[];
    jobs: unknown[];
    hypotheses: Hypothesis[];
    agentProgress: unknown;
    agentLastComplete: unknown;
    setSystemStatus: (status: unknown) => void;
    setCurrentRegime: (regime: RegimeState | null) => void;
    setRegimeHistory: (history: unknown[]) => void;
    setJournalEntries: (entries: JournalEntry[]) => void;
    setJournalStats: (stats: unknown) => void;
    setProductionModels: (models: Record<string, unknown>) => void;
    setAllModels: (models: unknown[]) => void;
    setJobs: (jobs: unknown[]) => void;
    setHypotheses: (hypotheses: Hypothesis[]) => void;
}

export interface RealtimeState {
    wsConnected: boolean;
    livePriceUpdates: Record<string, unknown>;
    liveAlerts: LiveAlert[];
    liveRecommendations: LiveRecommendation[];
    lastRegimeChange: unknown;
    pushSupported: boolean;
    pushPermission: string;
    pushSubscription: PushSubscription | null;
    pushPreferences: PushPreferences;
    chatMessages: unknown[];
    chatUnread: number;
    setWsConnected: (connected: boolean) => void;
    setLivePriceUpdates: (prices: Record<string, unknown>) => void;
    pushAlert: (alert: Partial<LiveAlert>) => void;
    pushRecommendation: (rec: Partial<LiveRecommendation>) => void;
    dismissAlert: (id: number) => void;
    dismissRecommendation: (id: number) => void;
    setPushPermission: (perm: string) => void;
    setPushSubscription: (sub: PushSubscription | null) => void;
    setPushPreferences: (prefs: PushPreferences) => void;
    addChatMessage: (msg: unknown) => void;
    clearChat: () => void;
    setChatUnread: (n: number) => void;
    handleWsMessage: (event: { type: string; data?: unknown; severity?: string; timestamp?: string }) => void;
}

export interface CanvasStoreState {
    boards: CanvasBoard[];
    currentBoardId: string | null;
    nodes: unknown[];
    edges: unknown[];
    selectedNodeId: string | null;
    selectedEdgeId: string | null;
    loading: boolean;
    setBoards: (boards: CanvasBoard[]) => void;
    setCurrentBoardId: (id: string | null) => void;
    setNodes: (nodes: unknown[]) => void;
    setEdges: (edges: unknown[]) => void;
    setSelectedNodeId: (id: string | null) => void;
    setSelectedEdgeId: (id: string | null) => void;
    setLoading: (loading: boolean) => void;
    updateNodePosition: (nodeId: string, position: { x: number; y: number }) => void;
    addNode: (node: unknown) => void;
    removeNode: (nodeId: string) => void;
    addEdge: (edge: unknown) => void;
    removeEdge: (edgeId: string) => void;
    loadGraph: (graph: { nodes: unknown[]; edges: unknown[] }) => void;
    toDbFormat: () => { nodes: unknown[]; edges: unknown[] };
}

// Merged store type (backwards compat layer from store.js)
export type GridStore = AuthState & UiState & DomainState & RealtimeState;

// Theme color palette
export interface ThemeColors {
    bg: string;
    card: string;
    cardHover: string;
    cardElevated: string;
    border: string;
    borderSubtle: string;
    text: string;
    textDim: string;
    textMuted: string;
    textDimAlt: string;
    accent: string;
    green: string;
    greenBg: string;
    red: string;
    redBg: string;
    yellow: string;
    yellowBg: string;
    mono: string;
    sans: string;
    glassOverlay: string;
    gradientCard: string;
    accentGlow: string;
    accentLight: string;
    shadow: {
        sm: string;
        md: string;
        lg: string;
    };
}

// Confidence color map
export interface ConfidenceColors {
    confirmed: string;
    derived: string;
    estimated: string;
    rumored: string;
    inferred: string;
    contradicted: string;
}
