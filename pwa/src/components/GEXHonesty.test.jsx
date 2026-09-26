import React from 'react';
import { render, screen, cleanup } from '@testing-library/react';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import GEXProfile from './GEXProfile.jsx';
import VannaCharmViz from './VannaCharmViz.jsx';

beforeAll(() => { global.ResizeObserver = class { observe() {} disconnect() {} }; });
afterEach(cleanup);
const provenance = { basis: 'assumed positions', chain_snap_date: '2026-09-25', chain_capture_completed_at: '2026-09-25T18:00:00Z', spot_source: 'spy_close_receipt', spot_basis: 'prior_completed_unadjusted_close', spot_obs_date: '2026-09-24', spot_available_at: '2026-09-25T13:31:00Z' };
describe('modeled GEX presentation', () => {
  it('labels model and actual source timestamps separately', () => {
    render(<GEXProfile ticker="SPY" gexData={{ ...provenance, regime: 'LONG_GAMMA', gex_aggregate: 0 }} />);
    expect(screen.getByText('MODELED GEX PROFILE')).toBeTruthy();
    expect(screen.getByText('Estimated LONG GAMMA')).toBeTruthy();
    expect(screen.getByText('$0')).toBeTruthy();
    expect(screen.getByText(/Reference price source: spy_close_receipt/)).toBeTruthy();
    expect(screen.getByText(/2026-09-25T18:00:00Z/)).toBeTruthy();
    expect(screen.getByText(/Reference price date: 2026-09-24/)).toBeTruthy();
  });
  it('does not fabricate missing aggregate or provenance', () => {
    render(<GEXProfile ticker="SPY" gexData={{ gex_aggregate: null }} />);
    expect(screen.queryByText('$0')).toBeNull();
    expect(screen.getByText(/Model basis: Unavailable/)).toBeTruthy();
  });
  it('suppresses stale numeric data', () => {
    render(<GEXProfile ticker="SPY" gexData={{ stale: true, gex_aggregate: 123 }} />);
    expect(screen.getByText('Stale GEX profile unavailable')).toBeTruthy();
    expect(screen.queryByText('MODELED GEX PROFILE')).toBeNull();
  });
  it.each([null, undefined, NaN, Infinity])('shows unavailable net delta for %s', value => {
    const { container } = render(<VannaCharmViz ticker="SPY" vannaCharmData={{ ...provenance, vanna_exposure: 1, charm_exposure: 2, net_dealer_delta_change: value }} />);
    expect(container.querySelector('svg').textContent).toContain('Unavailable');
    expect(container.querySelector('svg').textContent).not.toMatch(/dealers buy|dealers sell|BUY|SELL/);
  });
  it('suppresses stale vanna estimates', () => {
    render(<VannaCharmViz ticker="SPY" vannaCharmData={{ stale: true, net_dealer_delta_change: 0 }} />);
    expect(screen.getByText('Stale vanna/charm data unavailable')).toBeTruthy();
    expect(screen.queryByText('$0')).toBeNull();
  });
  it('preserves a real zero net delta', () => {
    const { container } = render(<VannaCharmViz ticker="SPY" vannaCharmData={{ net_dealer_delta_change: 0 }} />);
    expect(container.querySelector('svg').textContent).toContain('$0');
  });
});
