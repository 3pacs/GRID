/**
 * GeoFlows -- Geo-spatial capital flow visualization.
 *
 * Shows money flows as animated arcs between financial centers,
 * actor locations as sized scatter points, and signal density as a heatmap.
 *
 * Uses deck.gl for WebGL-accelerated map layers on top of MapLibre GL.
 */
import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Map as MapLibreMap } from 'maplibre-gl';
import DeckGL from '@deck.gl/react';
import { ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { HeatmapLayer } from '@deck.gl/aggregation-layers';
import 'maplibre-gl/dist/maplibre-gl.css';
import { api } from '../api.js';

// Dark map style (free, no API key needed)
const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

const INITIAL_VIEW = {
    longitude: 0,
    latitude: 20,
    zoom: 2,
    pitch: 45,
    bearing: 0,
};

const FLOW_TYPES = [
    { label: 'Capital', value: 'capital' },
    { label: 'Commodity', value: 'commodity' },
    { label: 'Military', value: 'military' },
];

const PERIODS = [
    { label: '30D', days: 30 },
    { label: '90D', days: 90 },
    { label: '180D', days: 180 },
    { label: '1Y', days: 365 },
];

// Confidence -> opacity mapping
const CONF_OPACITY = {
    confirmed: 255,
    derived: 180,
    estimated: 120,
    rumored: 80,
    inferred: 60,
};

const panelStyle = {
    position: 'absolute',
    top: 16,
    left: 16,
    zIndex: 10,
    background: 'rgba(13,17,23,0.92)',
    border: '1px solid #1E2A3A',
    borderRadius: 8,
    padding: 16,
    minWidth: 200,
    fontFamily: "'IBM Plex Sans', sans-serif",
    color: '#C8D8E8',
};

const labelStyle = { fontSize: 11, color: '#5A7080', marginBottom: 4 };

const btnRow = { display: 'flex', gap: 4 };

function makeBtn(isActive) {
    return {
        padding: '4px 10px',
        borderRadius: 4,
        border: 'none',
        cursor: 'pointer',
        background: isActive ? '#3B82F6' : '#1E2A3A',
        color: '#C8D8E8',
        fontSize: 11,
        fontWeight: 600,
    };
}

function GeoFlows() {
    const [viewState, setViewState] = useState(INITIAL_VIEW);
    const [flows, setFlows] = useState([]);
    const [actors, setActors] = useState([]);
    const [density, setDensity] = useState([]);
    const [flowType, setFlowType] = useState('capital');
    const [days, setDays] = useState(90);
    const [loading, setLoading] = useState(true);
    const [showFlows, setShowFlows] = useState(true);
    const [showActors, setShowActors] = useState(true);
    const [showHeatmap, setShowHeatmap] = useState(false);
    const [hoveredItem, setHoveredItem] = useState(null);
    const [webglSupported, setWebglSupported] = useState(true);
    const mapContainer = useRef(null);
    const mapRef = useRef(null);

    // Check WebGL support on mount
    useEffect(() => {
        try {
            const canvas = document.createElement('canvas');
            const gl = canvas.getContext('webgl2') || canvas.getContext('webgl');
            if (!gl) {
                setWebglSupported(false);
            }
        } catch (_) {
            setWebglSupported(false);
        }
    }, []);

    // Initialize MapLibre base map
    useEffect(() => {
        if (!webglSupported || !mapContainer.current || mapRef.current) return;

        const map = new MapLibreMap({
            container: mapContainer.current,
            style: MAP_STYLE,
            center: [INITIAL_VIEW.longitude, INITIAL_VIEW.latitude],
            zoom: INITIAL_VIEW.zoom,
            pitch: INITIAL_VIEW.pitch,
            bearing: INITIAL_VIEW.bearing,
            interactive: false, // deck.gl handles interaction
            attributionControl: false,
        });

        mapRef.current = map;

        return () => {
            if (mapRef.current) {
                mapRef.current.remove();
                mapRef.current = null;
            }
        };
    }, [webglSupported]);

    // Sync MapLibre viewState when deck.gl viewState changes
    useEffect(() => {
        if (!mapRef.current) return;
        const map = mapRef.current;
        map.jumpTo({
            center: [viewState.longitude, viewState.latitude],
            zoom: viewState.zoom,
            pitch: viewState.pitch,
            bearing: viewState.bearing,
        });
    }, [viewState]);

    // Fetch data
    useEffect(() => {
        if (!webglSupported) return;

        setLoading(true);
        const params = `flow_type=${flowType}&days=${days}`;

        Promise.all([
            api.get(`/api/v1/geo/flows?${params}`),
            api.get(`/api/v1/geo/actors?min_influence=0.3&limit=300`),
            api.get(`/api/v1/geo/signals/density?days=${days}`),
        ]).then(([flowRes, actorRes, densityRes]) => {
            setFlows(flowRes?.flows || []);
            setActors(actorRes?.actors || []);
            setDensity(densityRes?.density || []);
        }).catch(err => {
            console.error('GeoFlows fetch error:', err);
        }).finally(() => setLoading(false));
    }, [flowType, days, webglSupported]);

    // Arc layer -- capital flows between locations
    const arcLayer = useMemo(() => {
        if (!showFlows || flows.length === 0) return null;
        return new ArcLayer({
            id: 'flow-arcs',
            data: flows,
            getSourcePosition: d => [d.from_lng, d.from_lat],
            getTargetPosition: d => [d.to_lng, d.to_lat],
            getSourceColor: d => [59, 130, 246, CONF_OPACITY[d.confidence] || 120],
            getTargetColor: d => [16, 185, 129, CONF_OPACITY[d.confidence] || 120],
            getWidth: d => Math.max(1, Math.min(8, Math.log10(Math.max(d.amount || 1, 1)))),
            greatCircle: true,
            pickable: true,
            autoHighlight: true,
            highlightColor: [255, 255, 255, 180],
            onHover: info => setHoveredItem(info.object ? {
                type: 'flow',
                text: `${info.object.from_name} -> ${info.object.to_name}`,
                amount: info.object.amount,
                confidence: info.object.confidence,
                x: info.x,
                y: info.y,
            } : null),
        });
    }, [flows, showFlows]);

    // Scatter layer -- actor locations
    const scatterLayer = useMemo(() => {
        if (!showActors || actors.length === 0) return null;
        return new ScatterplotLayer({
            id: 'actor-scatter',
            data: actors,
            getPosition: d => [d.lng, d.lat],
            getRadius: d => Math.max(20000, (d.influence || 0) * 200000),
            getFillColor: d => {
                const cat = (d.category || '').toLowerCase();
                if (cat.includes('sovereign')) return [139, 92, 246, 200];
                if (cat.includes('institutional')) return [59, 130, 246, 200];
                if (cat.includes('individual')) return [245, 158, 11, 200];
                return [107, 114, 128, 160];
            },
            pickable: true,
            autoHighlight: true,
            onHover: info => setHoveredItem(info.object ? {
                type: 'actor',
                text: info.object.name,
                category: info.object.category,
                influence: info.object.influence,
                x: info.x,
                y: info.y,
            } : null),
            radiusMinPixels: 4,
            radiusMaxPixels: 40,
        });
    }, [actors, showActors]);

    // Heatmap layer -- signal density
    const heatmapLayer = useMemo(() => {
        if (!showHeatmap || density.length === 0) return null;
        return new HeatmapLayer({
            id: 'signal-heatmap',
            data: density,
            getPosition: d => [d.lng, d.lat],
            getWeight: d => d.weight,
            radiusPixels: 60,
            intensity: 1,
            threshold: 0.05,
            colorRange: [
                [0, 0, 0, 0],
                [16, 185, 129, 80],
                [245, 158, 11, 150],
                [239, 68, 68, 220],
            ],
        });
    }, [density, showHeatmap]);

    const layers = [heatmapLayer, arcLayer, scatterLayer].filter(Boolean);

    if (!webglSupported) {
        return (
            <div style={{
                width: '100%', height: 'calc(100vh - 64px)', background: '#080C10',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                color: '#5A7080', fontFamily: "'IBM Plex Sans', sans-serif", fontSize: 14,
                padding: 32, textAlign: 'center',
            }}>
                <div>
                    <div style={{ fontSize: 18, fontWeight: 700, color: '#C8D8E8', marginBottom: 12 }}>
                        WebGL Not Available
                    </div>
                    <div>
                        Geo-spatial visualization requires WebGL support in your browser.
                        Please use a modern browser with hardware acceleration enabled.
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div style={{ position: 'relative', width: '100%', height: 'calc(100vh - 64px)', background: '#080C10' }}>
            {/* MapLibre base map */}
            <div
                ref={mapContainer}
                style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0 }}
            />

            {/* deck.gl overlay */}
            <DeckGL
                viewState={viewState}
                onViewStateChange={({ viewState: vs }) => setViewState(vs)}
                controller={true}
                layers={layers}
                style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0 }}
                getCursor={({ isHovering }) => isHovering ? 'pointer' : 'grab'}
            />

            {/* Controls panel */}
            <div style={panelStyle}>
                <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 12 }}>GEO FLOWS</div>

                {/* Flow type */}
                <div style={{ marginBottom: 12 }}>
                    <div style={labelStyle}>FLOW TYPE</div>
                    <div style={btnRow}>
                        {FLOW_TYPES.map(ft => (
                            <button key={ft.value} onClick={() => setFlowType(ft.value)}
                                style={makeBtn(flowType === ft.value)}>
                                {ft.label}
                            </button>
                        ))}
                    </div>
                </div>

                {/* Time range */}
                <div style={{ marginBottom: 12 }}>
                    <div style={labelStyle}>TIME RANGE</div>
                    <div style={btnRow}>
                        {PERIODS.map(p => (
                            <button key={p.days} onClick={() => setDays(p.days)}
                                style={makeBtn(days === p.days)}>
                                {p.label}
                            </button>
                        ))}
                    </div>
                </div>

                {/* Layer toggles */}
                <div style={{ marginBottom: 8 }}>
                    <div style={labelStyle}>LAYERS</div>
                    {[
                        { label: 'Flow Arcs', value: showFlows, set: setShowFlows },
                        { label: 'Actor Locations', value: showActors, set: setShowActors },
                        { label: 'Signal Heatmap', value: showHeatmap, set: setShowHeatmap },
                    ].map(toggle => (
                        <label key={toggle.label} style={{
                            display: 'flex', alignItems: 'center', gap: 6,
                            cursor: 'pointer', marginBottom: 4,
                        }}>
                            <input type="checkbox" checked={toggle.value}
                                onChange={() => toggle.set(!toggle.value)}
                                style={{ accentColor: '#3B82F6' }} />
                            <span style={{ fontSize: 12 }}>{toggle.label}</span>
                        </label>
                    ))}
                </div>

                {/* Stats */}
                <div style={{ borderTop: '1px solid #1E2A3A', paddingTop: 8, marginTop: 8 }}>
                    <div style={{ fontSize: 11, color: '#5A7080' }}>
                        {flows.length} flows &middot; {actors.length} actors &middot; {density.length} signals
                    </div>
                </div>
            </div>

            {/* Hover tooltip */}
            {hoveredItem && (
                <div style={{
                    position: 'absolute',
                    left: hoveredItem.x + 12,
                    top: hoveredItem.y + 12,
                    zIndex: 20,
                    background: 'rgba(13,17,23,0.95)',
                    border: '1px solid #1E2A3A',
                    borderRadius: 6,
                    padding: '8px 12px',
                    pointerEvents: 'none',
                    fontSize: 12,
                    color: '#C8D8E8',
                    maxWidth: 300,
                    fontFamily: "'IBM Plex Sans', sans-serif",
                }}>
                    <div style={{ fontWeight: 600, marginBottom: 4 }}>{hoveredItem.text}</div>
                    {hoveredItem.amount != null && hoveredItem.amount > 0 && (
                        <div style={{ color: '#10B981' }}>
                            ${(hoveredItem.amount / 1e6).toFixed(1)}M
                        </div>
                    )}
                    {hoveredItem.confidence && (
                        <div style={{ fontSize: 10, color: '#5A7080' }}>
                            Confidence: {hoveredItem.confidence}
                        </div>
                    )}
                    {hoveredItem.influence != null && (
                        <div style={{ fontSize: 10, color: '#5A7080' }}>
                            Influence: {(hoveredItem.influence * 100).toFixed(0)}%
                        </div>
                    )}
                </div>
            )}

            {/* Loading indicator */}
            {loading && (
                <div style={{
                    position: 'absolute', top: '50%', left: '50%',
                    transform: 'translate(-50%, -50%)',
                    color: '#5A7080', fontSize: 14, zIndex: 10,
                    fontFamily: "'IBM Plex Mono', monospace",
                }}>
                    Loading geo data...
                </div>
            )}
        </div>
    );
}

export default GeoFlows;
