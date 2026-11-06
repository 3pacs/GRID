// Live test 2026-11-06: these views showed little or nothing within 10 s.
// Each must show loading, then data or an explicit unavailable reason; a
// failure must never read as an empty dataset, and a stuck request must end.
import React from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import { VIEW_REQUEST_TIMEOUT_MS } from '../utils/apiResult.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(), post: vi.fn(),
        getThesis: vi.fn(), getRegimeAnalogs: vi.fn(), getGlobeData: vi.fn(),
        getSignalSnapshot: vi.fn(), getSmartHeatmap: vi.fn(), getCelestialSignals: vi.fn(), getCurrent: vi.fn(),
        getLogs: vi.fn(), getConfig: vi.fn(), getSources: vi.fn(), updateSource: vi.fn(),
        getConventions: vi.fn(), runPhysicsVerification: vi.fn(), getPhysicsDashboard: vi.fn(), getNewsEnergy: vi.fn(),
        getOUParams: vi.fn(), getHurst: vi.fn(), getEnergy: vi.fn(),
        getResearchArchive: vi.fn(), triggerDeepDive: vi.fn(), loadFlowBriefingAudio: vi.fn(),
        getAnomalies: vi.fn(), getCorrelationMatrix: vi.fn(), getRegimeFeatures: vi.fn(), getTimeseries: vi.fn(),
    },
}));

const storeState = { addNotification: vi.fn(), setLoading: vi.fn() };
vi.mock('../store.js', () => ({ default: vi.fn(() => storeState) }));

const { default: Thesis } = await import('../views/Thesis.jsx');
const { default: RegimeAnalog } = await import('../views/RegimeAnalog.jsx');
const { default: Valuation } = await import('../views/Valuation.jsx');
const { default: Signals } = await import('../views/Signals.jsx');
const { default: Heatmap } = await import('../views/Heatmap.jsx');
const { default: SystemLogs } = await import('../views/SystemLogs.jsx');
const { default: Physics } = await import('../views/Physics.jsx');
const { default: Archive } = await import('../views/Archive.jsx');
const { default: Associations } = await import('../views/Associations.jsx');
const { default: Globe } = await import('../views/Globe.jsx');

const fail = (message, status = 503) => Promise.resolve({ error: true, status, message });
const never = () => new Promise(() => {});

beforeAll(() => {
    if (typeof globalThis.ResizeObserver === 'undefined') {
        globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    }
    // Plain function: vi.resetAllMocks() in beforeEach would clear a vi.fn() return value.
    window.matchMedia = () => ({ matches: false, addEventListener() {}, removeEventListener() {} });
});

beforeEach(() => { vi.resetAllMocks(); });
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });

describe('slow endpoints end in an explicit timeout, not endless loading', () => {
    it('Thesis', async () => {
        vi.useFakeTimers();
        api.getThesis.mockImplementation(never);
        render(<Thesis />);
        expect(screen.getByText('Loading unified thesis...')).toBeInTheDocument();
        await act(async () => { await vi.advanceTimersByTimeAsync(VIEW_REQUEST_TIMEOUT_MS); });
        expect(screen.getByText(/Unified thesis did not respond within 45 s/)).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
    });

    it('Research archive', async () => {
        vi.useFakeTimers();
        api.getResearchArchive.mockImplementation(never);
        render(<Archive />);
        await act(async () => { await vi.advanceTimersByTimeAsync(VIEW_REQUEST_TIMEOUT_MS); });
        expect(screen.getByText(/Research archive did not respond within 45 s/)).toBeInTheDocument();
    });

    it('Regime analog', async () => {
        vi.useFakeTimers();
        api.getRegimeAnalogs.mockImplementation(never);
        render(<RegimeAnalog />);
        await act(async () => { await vi.advanceTimersByTimeAsync(VIEW_REQUEST_TIMEOUT_MS); });
        expect(screen.getByText(/Regime analogs did not respond within 45 s/)).toBeInTheDocument();
    });
});

describe('failures render their reason instead of an empty dataset', () => {
    it('Regime analog shows the marker message, not "Error: " + boolean', async () => {
        api.getRegimeAnalogs.mockImplementation(() => fail('analog engine cold'));
        render(<RegimeAnalog />);
        expect(await screen.findByText('Error: analog engine cold')).toBeInTheDocument();
    });

    it('Valuation shows the failure after Analyze instead of rendering nothing', async () => {
        api.get.mockImplementation((path) => (path.includes('/prompt/') ? fail('valuation backend down') : Promise.resolve({ predictions: [] })));
        render(<Valuation />);
        fireEvent.change(screen.getByPlaceholderText(/Enter ticker/), { target: { value: 'nvda' } });
        fireEvent.click(screen.getByRole('button', { name: /Analyze/ }));
        expect(await screen.findByText('valuation backend down')).toBeInTheDocument();
        expect(api.get).toHaveBeenCalledWith('/api/v1/valuation/prompt/NVDA');
    });

    it('Signals snapshot, drivers and celestial tabs say unavailable', async () => {
        api.getSignalSnapshot.mockImplementation(() => fail('snapshot table empty'));
        api.getCurrent.mockImplementation(() => fail('regime offline'));
        api.getCelestialSignals.mockImplementation(() => fail('celestial puller failed'));
        render(<Signals />);
        expect(await screen.findByText('snapshot table empty')).toBeInTheDocument();
        expect(screen.queryByText('No features available')).toBeNull();
        fireEvent.click(screen.getByRole('button', { name: 'Drivers' }));
        expect(await screen.findByText('regime offline')).toBeInTheDocument();
        expect(screen.queryByText('No active signals')).toBeNull();
        fireEvent.click(screen.getByRole('button', { name: 'Celestial' }));
        expect(await screen.findByText(/celestial puller failed/)).toBeInTheDocument();
        expect(screen.queryByText(/No celestial features registered/)).toBeNull();
    });

    it('Heatmap says the snapshot is unavailable instead of "Run inference"', async () => {
        api.getSignalSnapshot.mockImplementation(() => fail('snapshot read timed out'));
        api.getSmartHeatmap.mockResolvedValue({ features: [], matrix: [], families: [] });
        render(<Heatmap />);
        expect(await screen.findByText('Z-score data unavailable: snapshot read timed out')).toBeInTheDocument();
        expect(screen.queryByText(/Run inference to generate a snapshot/)).toBeNull();
    });

    it('Heatmap with both inputs down shows both reasons', async () => {
        api.getSignalSnapshot.mockImplementation(() => fail('A down'));
        api.getSmartHeatmap.mockImplementation(() => fail('B down'));
        render(<Heatmap />);
        expect(await screen.findByText('A down | B down')).toBeInTheDocument();
    });

    it('System config and sources tabs report failures', async () => {
        api.getLogs.mockResolvedValue({ logs: [] });
        api.getConfig.mockImplementation(() => fail('config forbidden', 403));
        api.getSources.mockImplementation(() => fail('sources query failed'));
        render(<SystemLogs />);
        fireEvent.click(screen.getByRole('button', { name: 'Config' }));
        expect(await screen.findByText('config forbidden')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Sources' }));
        expect(await screen.findByText('sources query failed')).toBeInTheDocument();
        expect(screen.queryByText('No sources found')).toBeNull();
    });

    it('Physics conventions and verification show the reason', async () => {
        api.getConventions.mockImplementation(() => fail('conventions missing'));
        api.runPhysicsVerification.mockImplementation(() => fail('verifier crashed', 500));
        render(<Physics />);
        fireEvent.click(screen.getByRole('button', { name: 'Conventions' }));
        expect(await screen.findByText('conventions missing')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Verify' }));
        fireEvent.click(screen.getByRole('button', { name: 'Run Physics Verification' }));
        expect(await screen.findByText('verifier crashed')).toBeInTheDocument();
    });

    it('Associations reports missing secondary inputs', async () => {
        api.getAnomalies.mockResolvedValue({ anomalies: [] });
        api.getCorrelationMatrix.mockImplementation(() => fail('matrix cold'));
        api.getRegimeFeatures.mockResolvedValue(null);
        api.getSmartHeatmap.mockResolvedValue(null);
        render(<Associations />);
        expect(await screen.findByText('Partial feed — unavailable inputs: matrix cold')).toBeInTheDocument();
    });

    it('Globe shows the data failure instead of an empty map', async () => {
        vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => Promise.resolve({ type: 'Topology', objects: {} }) })));
        api.getGlobeData.mockImplementation(() => fail('globe aggregate stale'));
        render(<Globe />);
        expect(await screen.findByText('globe aggregate stale')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
    });

    it('Globe names the world-outline failure', async () => {
        vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: false, status: 404 })));
        api.getGlobeData.mockResolvedValue({ countries: [], hotspots: [] });
        render(<Globe />);
        expect(await screen.findByText(/World map outline unavailable: World map outline HTTP 404/)).toBeInTheDocument();
    });
});
