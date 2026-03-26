/**
 * DerivativesGrid Zustand global state store.
 */

import { create } from 'zustand';

const TICKER_LIST = [
    'SPY', 'QQQ', 'IWM', 'DIA', 'NVDA', 'TSLA', 'AAPL', 'MSFT', 'AMZN', 'GOOG',
    'META', 'AMD', 'NFLX', 'AVGO', 'CRM', 'ORCL', 'INTC', 'MU', 'SMCI', 'ARM',
    'JPM', 'GS', 'BAC', 'MS', 'V', 'MA', 'XLF', 'XLE', 'XLK', 'XLV',
    'GLD', 'SLV', 'TLT', 'HYG', 'VIX', 'UVXY', 'SQQQ', 'TQQQ', 'COIN', 'MSTR',
];

const useStore = create((set, get) => ({
    // Navigation
    activeView: 'dealer-flow',

    // Ticker
    selectedTicker: 'SPY',
    tickerList: TICKER_LIST,

    // Data
    gexData: null,
    volSurface: null,
    regime: null,
    signals: [],
    briefing: '',

    // UI
    loading: false,

    // Auth — shares JWT with GRID PWA
    token: localStorage.getItem('grid_token') || null,
    isAuthenticated: !!localStorage.getItem('grid_token'),

    // Actions
    setActiveView: (view) => {
        window.location.hash = `#/${view}`;
        set({ activeView: view });
    },

    setSelectedTicker: (ticker) => set({ selectedTicker: ticker }),
    setGexData: (data) => set({ gexData: data }),
    setVolSurface: (data) => set({ volSurface: data }),
    setRegime: (regime) => set({ regime }),
    setSignals: (signals) => set({ signals }),
    setBriefing: (briefing) => set({ briefing }),
    setLoading: (loading) => set({ loading }),

    setAuth: (token) => {
        localStorage.setItem('grid_token', token);
        set({ token, isAuthenticated: true });
    },

    clearAuth: () => {
        localStorage.removeItem('grid_token');
        set({ token: null, isAuthenticated: false });
    },
}));

export default useStore;
