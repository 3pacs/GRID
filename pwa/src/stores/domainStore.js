/**
 * Domain store slice — regime, journal, models, discovery, signals.
 */
import { create } from 'zustand';

const useDomainStore = create((set) => ({
    // System
    systemStatus: null,

    // Signals
    latestSignals: null,

    // Regime
    currentRegime: null,
    regimeHistory: [],

    // Journal
    journalEntries: [],
    journalStats: null,

    // Models
    productionModels: {},
    allModels: [],

    // Discovery
    jobs: [],
    hypotheses: [],

    // Agents
    agentProgress: null,
    agentLastComplete: null,

    // Setters
    setSystemStatus: (status) => set({ systemStatus: status }),
    setCurrentRegime: (regime) => set({ currentRegime: regime }),
    setRegimeHistory: (history) => set({ regimeHistory: history }),
    setJournalEntries: (entries) => set({ journalEntries: entries }),
    setJournalStats: (stats) => set({ journalStats: stats }),
    setProductionModels: (models) => set({ productionModels: models }),
    setAllModels: (models) => set({ allModels: models }),
    setJobs: (jobs) => set({ jobs }),
    setHypotheses: (hypotheses) => set({ hypotheses }),
}));

export default useDomainStore;
