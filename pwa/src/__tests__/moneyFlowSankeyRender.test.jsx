import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import MoneyFlow from '../views/MoneyFlow.jsx';

const fixture = vi.hoisted(() => ({ data: null }));
vi.mock('../components/flows/useFlowData.js', async (importOriginal) => ({
  ...await importOriginal(),
  useFlowLayers: () => ({ data: fixture.data, loading: false, error: null }),
}));

class RenderBoundary extends React.Component {
  state = { error: null };
  static getDerivedStateFromError(error) { return { error }; }
  render() {
    return this.state.error
      ? <div role="alert">Render failed: {this.state.error.message}</div>
      : this.props.children;
  }
}

const node = (id) => ({ id, label: id, value: 2e12, change_1m: 0.02, unit: 'USD' });
const cases = [
  {
    name: 'one populated node',
    data: { layers: [{ id: 'monetary', label: 'Monetary', nodes: [node('Fed')] }], edges: [] },
    labels: ['Fed'], edges: 0,
  },
  {
    name: 'multiple populated layers and edges',
    data: {
      layers: [
        { id: 'retail', label: 'Retail', nodes: [node('Savings')] },
        { id: 'monetary', label: 'Monetary', nodes: [node('Fed'), node('Reserves')] },
        { id: 'credit', label: 'Credit', nodes: [node('Loans')] },
      ],
      edges: [
        { source_layer: 'monetary', source_node: 'Fed', target_layer: 'credit', target_node: 'Loans' },
        { source_layer: 'credit', source_node: 'Loans', target_layer: 'retail', target_node: 'Savings' },
      ],
    },
    labels: ['Fed', 'Reserves', 'Loans', 'Savings'], edges: 2,
  },
];

beforeEach(() => {
  // A local layout observer, not a browser or network request.
  vi.stubGlobal('ResizeObserver', class {
    constructor(callback) { this.callback = callback; }
    observe(target) { this.callback([{ target, contentRect: { width: 900, height: 500 } }]); }
    disconnect() {}
  });
  vi.stubGlobal('fetch', vi.fn(() => { throw new Error('Unexpected network call'); }));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe('MoneyFlow default Sankey populated render', () => {
  it.each(cases)('renders $name without entering the error boundary', async ({ data, labels, edges }) => {
    fixture.data = data;
    const { container } = render(<RenderBoundary><MoneyFlow /></RenderBoundary>);
    // Never click a tab: this exercises MoneyFlow's actual default lazy Sankey.
    expect(screen.getByRole('button', { name: 'Sankey' })).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
      expect(container.querySelectorAll('svg g.nodes > g')).toHaveLength(labels.length);
    });
    const graph = container.querySelector('svg:has(g.nodes)');
    expect(graph).not.toBeNull();
    expect(graph.querySelectorAll('g.nodes > g > circle')).toHaveLength(labels.length * 2);
    expect(graph.querySelectorAll('g.edges > path')).toHaveLength(edges);
    for (const label of labels) expect(graph.textContent).toContain(label);
    for (const circle of graph.querySelectorAll('g.nodes circle')) {
      expect(Number.isFinite(Number(circle.getAttribute('cx')))).toBe(true);
      expect(Number.isFinite(Number(circle.getAttribute('cy')))).toBe(true);
    }
    expect(globalThis.fetch).not.toHaveBeenCalled();
  });
});
