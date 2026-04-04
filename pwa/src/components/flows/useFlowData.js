/**
 * useFlowData — Custom hook for flow engine v2 data.
 *
 * Calls /api/v1/flows/flow-map-v2 and caches for 5 minutes.
 * Provides layers, junctions, edges, waterfall, orthogonality.
 */
import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../../api.js';

const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

function makeCache() {
  return { data: null, ts: 0 };
}

const _cache = {
  layers: makeCache(),
  junctions: makeCache(),
  waterfall: makeCache(),
  orthogonality: makeCache(),
};

function isFresh(key) {
  return _cache[key].data && (Date.now() - _cache[key].ts) < CACHE_TTL;
}

function setCache(key, data) {
  _cache[key] = { data, ts: Date.now() };
}

export function useFlowLayers() {
  const [data, setData] = useState(_cache.layers.data);
  const [loading, setLoading] = useState(!_cache.layers.data);
  const [error, setError] = useState(null);

  const fetch = useCallback(async (force = false) => {
    if (!force && isFresh('layers')) {
      setData(_cache.layers.data);
      return;
    }
    setLoading(true);
    setError(null);
    const res = await api.getFlowLayers();
    if (res.error) {
      setError(res.message);
    } else {
      setCache('layers', res);
      setData(res);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetch(); }, [fetch]);

  return { data, loading, error, refetch: () => fetch(true) };
}

export function useJunctionPoints() {
  const [data, setData] = useState(_cache.junctions.data);
  const [loading, setLoading] = useState(!_cache.junctions.data);
  const [error, setError] = useState(null);

  const fetch = useCallback(async (force = false) => {
    if (!force && isFresh('junctions')) {
      setData(_cache.junctions.data);
      return;
    }
    setLoading(true);
    setError(null);
    const res = await api.getJunctionPoints();
    if (res.error) {
      setError(res.message);
    } else {
      setCache('junctions', res);
      setData(res);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetch(); }, [fetch]);

  return { data, loading, error, refetch: () => fetch(true) };
}

export function useFlowWaterfall(source = 'fed') {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const srcRef = useRef(source);

  const fetch = useCallback(async (src) => {
    setLoading(true);
    setError(null);
    const res = await api.getFlowWaterfall(src);
    if (res.error) {
      setError(res.message);
    } else {
      setData(res);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetch(source); srcRef.current = source; }, [source, fetch]);

  return { data, loading, error, refetch: () => fetch(srcRef.current) };
}

export function useFlowOrthogonality() {
  const [data, setData] = useState(_cache.orthogonality.data);
  const [loading, setLoading] = useState(!_cache.orthogonality.data);
  const [error, setError] = useState(null);

  const fetch = useCallback(async (force = false) => {
    if (!force && isFresh('orthogonality')) {
      setData(_cache.orthogonality.data);
      return;
    }
    setLoading(true);
    setError(null);
    const res = await api.getFlowOrthogonality();
    if (res.error) {
      setError(res.message);
    } else {
      setCache('orthogonality', res);
      setData(res);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetch(); }, [fetch]);

  return { data, loading, error, refetch: () => fetch(true) };
}

/** Helper: format dollar values */
export function fmtDollar(val) {
  if (val == null || isNaN(val)) return '--';
  const abs = Math.abs(val);
  const sign = val < 0 ? '-' : '';
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(1)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
}

/** Helper: format percentage */
export function fmtPct(val) {
  if (val == null || isNaN(val)) return '--';
  return `${val >= 0 ? '+' : ''}${(val * 100).toFixed(1)}%`;
}

/** Helper: trend arrow */
export function trendArrow(trend) {
  if (trend === 'accelerating') return '⬆⬆';
  if (trend === 'expanding') return '⬆';
  if (trend === 'decelerating') return '⬇⬇';
  if (trend === 'contracting') return '⬇';
  if (trend === 'stable') return '→';
  return '?';
}
