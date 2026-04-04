/**
 * MoneyFlow — 5-tab orchestrator for the 8-layer flow engine.
 *
 * Tabs: Sankey | Dashboard | Bubbles | Waterfall | Orthogonality
 * Thin wrapper (<300 lines). All heavy rendering in components/flows/.
 */
import React, { useState, useCallback, lazy, Suspense } from 'react';
import { colors, tokens } from '../styles/shared.js';
import {
  GitBranch, LayoutGrid, Circle, BarChart2, Waypoints, ShieldAlert,
} from 'lucide-react';

// Lazy load visualization components for code splitting
const FlowSankey8 = lazy(() => import('../components/flows/FlowSankey8.jsx'));
const JunctionDashboard = lazy(() => import('../components/flows/JunctionDashboard.jsx'));
const BubbleUniverse = lazy(() => import('../components/flows/BubbleUniverse.jsx'));
const FlowWaterfall = lazy(() => import('../components/flows/FlowWaterfall.jsx'));
const OrthogonalityView = lazy(() => import('../components/flows/OrthogonalityView.jsx'));
const CDSDashboard = lazy(() => import('../components/flows/CDSDashboard.jsx'));

const FONT = colors.mono || "'IBM Plex Mono', monospace";

const TABS = [
  { id: 'sankey', label: 'Sankey', Icon: GitBranch, desc: '8-layer capital flow' },
  { id: 'dashboard', label: 'Dashboard', Icon: LayoutGrid, desc: '23 junction points' },
  { id: 'cds', label: 'Credit', Icon: ShieldAlert, desc: 'CDS spreads & credit risk' },
  { id: 'bubbles', label: 'Bubbles', Icon: Circle, desc: 'Force-directed layers' },
  { id: 'waterfall', label: 'Waterfall', Icon: BarChart2, desc: 'Fed → layers cascade' },
  { id: 'orthogonality', label: 'PCA', Icon: Waypoints, desc: 'Signal independence' },
];

function LoadingFallback() {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      height: '100%', color: colors.textDim, fontFamily: FONT, fontSize: '12px',
    }}>
      Loading visualization...
    </div>
  );
}

export default function MoneyFlow() {
  const [activeTab, setActiveTab] = useState('sankey');

  const renderContent = useCallback(() => {
    switch (activeTab) {
      case 'sankey': return <FlowSankey8 />;
      case 'dashboard': return <JunctionDashboard />;
      case 'bubbles': return <BubbleUniverse />;
      case 'waterfall': return <FlowWaterfall />;
      case 'orthogonality': return <OrthogonalityView />;
      case 'cds': return <CDSDashboard />;
      default: return <FlowSankey8 />;
    }
  }, [activeTab]);

  return (
    <div style={sty.page}>
      {/* Header */}
      <div style={sty.header}>
        <div style={sty.titleRow}>
          <h1 style={sty.title}>CAPITAL FLOWS</h1>
          <span style={sty.subtitle}>8-Layer Junction Point Model</span>
        </div>
      </div>

      {/* Tab bar */}
      <div style={sty.tabBar}>
        {TABS.map(tab => {
          const active = activeTab === tab.id;
          const Icon = tab.Icon;
          return (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              style={sty.tab(active)}
              title={tab.desc}
            >
              <Icon size={14} strokeWidth={active ? 2.5 : 1.5} />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </div>

      {/* Content */}
      <div style={sty.content}>
        <Suspense fallback={<LoadingFallback />}>
          {renderContent()}
        </Suspense>
      </div>
    </div>
  );
}

// ── Styles ──────────────────────────────────────────────────────
const sty = {
  page: {
    display: 'flex', flexDirection: 'column', height: '100%',
    background: colors.bg, color: colors.text, fontFamily: FONT,
    overflow: 'hidden',
  },
  header: {
    padding: '12px 16px 0',
  },
  titleRow: {
    display: 'flex', alignItems: 'baseline', gap: 12,
  },
  title: {
    fontSize: '16px', fontWeight: 700, margin: 0,
    letterSpacing: '2px', color: colors.text, fontFamily: FONT,
  },
  subtitle: {
    fontSize: '10px', color: colors.textDim, fontWeight: 400,
  },
  tabBar: {
    display: 'flex', gap: 2, padding: '8px 16px 0',
    borderBottom: `1px solid ${colors.border}`,
  },
  tab: (active) => ({
    display: 'flex', alignItems: 'center', gap: 6,
    background: active ? colors.accent + '15' : 'transparent',
    border: 'none',
    borderBottom: active ? `2px solid ${colors.accent}` : '2px solid transparent',
    borderRadius: '6px 6px 0 0',
    padding: '8px 14px',
    cursor: 'pointer',
    fontSize: '11px', fontWeight: active ? 700 : 400,
    fontFamily: FONT,
    color: active ? colors.accent : colors.textDim,
    transition: 'all 0.15s ease',
  }),
  content: {
    flex: '1 1 auto', minHeight: 0, overflow: 'hidden',
    margin: '0 12px 12px',
    background: colors.card,
    border: `1px solid ${colors.border}`,
    borderRadius: tokens.radius.md,
  },
};
