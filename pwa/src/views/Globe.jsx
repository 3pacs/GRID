/**
 * Globe — D3 world map showing global capital flows, trade imbalances,
 * and economic activity. Countries colored by activity score, curved arc
 * flows between nations, pulsing hotspot markers, and an FX ticker strip.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ChartControls from '../components/ChartControls.jsx';
import useFullScreen from '../hooks/useFullScreen.js';

// Inline TopoJSON feature extraction (avoids topojson-client dependency).
// Converts a TopoJSON topology + object into a GeoJSON FeatureCollection.
function topoFeature(topology, obj) {
    const tf = topology.transform;
    const arcs = topology.arcs;

    function decodeArc(arcIdx) {
        const arc = arcs[arcIdx < 0 ? ~arcIdx : arcIdx];
        const coords = [];
        let x = 0, y = 0;
        for (const pt of arc) {
            x += pt[0];
            y += pt[1];
            coords.push(tf ? [x * tf.scale[0] + tf.translate[0], y * tf.scale[1] + tf.translate[1]] : [x, y]);
        }
        if (arcIdx < 0) coords.reverse();
        return coords;
    }

    function decodeRing(indices) {
        let coords = [];
        for (const idx of indices) {
            const decoded = decodeArc(idx);
            // Skip the first point of subsequent arcs (shared with previous)
            coords = coords.concat(coords.length ? decoded.slice(1) : decoded);
        }
        return coords;
    }

    function decodeGeometry(geom) {
        if (geom.type === 'Polygon') {
            return { type: 'Polygon', coordinates: geom.arcs.map(decodeRing) };
        } else if (geom.type === 'MultiPolygon') {
            return { type: 'MultiPolygon', coordinates: geom.arcs.map(poly => poly.map(decodeRing)) };
        } else if (geom.type === 'Point') {
            const c = geom.coordinates;
            return { type: 'Point', coordinates: tf ? [c[0] * tf.scale[0] + tf.translate[0], c[1] * tf.scale[1] + tf.translate[1]] : c };
        }
        return geom;
    }

    const features = obj.geometries.map(geom => ({
        type: 'Feature',
        id: geom.id,
        properties: geom.properties || {},
        geometry: decodeGeometry(geom),
    }));
    return { type: 'FeatureCollection', features };
}

const WORLD_TOPO_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json';

// ISO 3166-1 alpha-3 to numeric mapping for TopoJSON matching
const ISO_ALPHA3_TO_NUM = {
    USA: '840', CHN: '156', JPN: '392', DEU: '276', GBR: '826',
    FRA: '250', IND: '356', BRA: '076', KOR: '410', AUS: '036',
    CAN: '124', MEX: '484', RUS: '643', IDN: '360', SAU: '682',
    ZAF: '710', TUR: '792', ARG: '032', ITA: '380', ESP: '724',
    NLD: '528', CHE: '756', SWE: '752', NOR: '578', SGP: '702',
    THA: '764', VNM: '704', MYS: '458', PHL: '608', TWN: '158',
    HKG: '344', NZL: '554', CHL: '152', COL: '170', PER: '604',
    EGY: '818', NGA: '566', POL: '616', ISR: '376',
};

// Flag emoji helper
const countryFlag = (id) => {
    if (!id || id.length < 2) return '';
    const cc = id.slice(0, 2);
    return String.fromCodePoint(...[...cc.toUpperCase()].map(c => 0x1F1E6 + c.charCodeAt(0) - 65));
};

const LAYER_DEFS = [
    { id: 'trade', label: 'Trade Flows', color: '#06B6D4' },
    { id: 'capital', label: 'Capital Flows', color: '#8B5CF6' },
    { id: 'fx', label: 'FX Moves', color: '#F59E0B' },
    { id: 'gdp', label: 'GDP Activity', color: '#22C55E' },
    { id: 'lights', label: 'Night Lights', color: '#FF6B6B' },
];

const activityColor = d3.scaleLinear()
    .domain([0, 0.35, 0.5, 0.65, 1])
    .range(['#EF4444', '#F59E0B', '#5A7080', '#22C55E', '#10B981'])
    .clamp(true);

const fmtB = (v) => {
    if (v == null) return 'N/A';
    if (v >= 1e12) return `$${(v / 1e12).toFixed(1)}T`;
    if (v >= 1e9) return `$${(v / 1e9).toFixed(0)}B`;
    if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
    return `$${v.toFixed(0)}`;
};

const fmtPct = (v) => {
    if (v == null) return 'N/A';
    return `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`;
};

export default function Globe() {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const fullScreenRef = useRef(null);
    const zoomRef = useRef(null);
    const [data, setData] = useState(null);
    const [world, setWorld] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [layers, setLayers] = useState({ trade: true, capital: true, fx: false, gdp: true, lights: false });
    const [selectedCountry, setSelectedCountry] = useState(null);
    const [hoveredCountry, setHoveredCountry] = useState(null);
    const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });
    const [dims, setDims] = useState({ width: 960, height: 500 });
    const [countrySearch, setCountrySearch] = useState('');
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

    // Load data
    useEffect(() => {
        const load = async () => {
            setLoading(true);
            setError(null);
            try {
                const [globeData, topoResp] = await Promise.all([
                    api.getGlobeData(),
                    fetch(WORLD_TOPO_URL).then(r => r.json()),
                ]);
                setData(globeData);
                setWorld(topoResp);
            } catch (err) {
                setError(err.message || 'Failed to load globe data');
            }
            setLoading(false);
        };
        load();
    }, []);

    // Responsive sizing
    useEffect(() => {
        if (!containerRef.current) return;
        const ro = new ResizeObserver((entries) => {
            const { width } = entries[0].contentRect;
            const w = Math.max(600, width);
            setDims({ width: w, height: Math.max(400, Math.min(600, w * 0.52)) });
        });
        ro.observe(containerRef.current);
        return () => ro.disconnect();
    }, []);

    // Country data lookup
    const countryMap = {};
    if (data?.countries) {
        data.countries.forEach(c => { countryMap[c.id] = c; });
    }
    const hotspotSet = new Set((data?.hotspots || []).map(h => h.country));

    // Country centroids for arcs
    const centroids = useRef({});

    // Render map
    useEffect(() => {
        if (!world || !data || !svgRef.current) return;
        const { width, height } = dims;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const projection = d3.geoNaturalEarth1()
            .fitSize([width - 40, height - 40], topoFeature(world, world.objects.countries))
            .translate([width / 2, height / 2]);
        const path = d3.geoPath().projection(projection);

        const g = svg.append('g');

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([1, 8])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);
        zoomRef.current = { zoom, svg };

        // Graticule
        g.append('path')
            .datum(d3.geoGraticule()())
            .attr('d', path)
            .attr('fill', 'none')
            .attr('stroke', '#0D1520')
            .attr('stroke-width', 0.3);

        // Countries
        const countriesGeo = topoFeature(world, world.objects.countries);
        const numToAlpha = {};
        Object.entries(ISO_ALPHA3_TO_NUM).forEach(([a, n]) => { numToAlpha[n] = a; });

        // Compute centroids
        countriesGeo.features.forEach(f => {
            const alpha3 = numToAlpha[f.id];
            if (alpha3) {
                const c = path.centroid(f);
                if (c && !isNaN(c[0])) {
                    centroids.current[alpha3] = c;
                }
            }
        });

        g.selectAll('path.country')
            .data(countriesGeo.features)
            .join('path')
            .attr('class', 'country')
            .attr('d', path)
            .attr('fill', (d) => {
                const alpha3 = numToAlpha[d.id];
                const cd = alpha3 ? countryMap[alpha3] : null;
                if (!cd || !layers.gdp) return '#14203A';
                return activityColor(cd.activity_score);
            })
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.5)
            .style('cursor', 'pointer')
            .on('mouseenter', function (event, d) {
                const alpha3 = numToAlpha[d.id];
                const cd = alpha3 ? countryMap[alpha3] : null;
                if (cd) {
                    setHoveredCountry(cd);
                    setTooltipPos({ x: event.offsetX, y: event.offsetY });
                }
                d3.select(this).attr('stroke', '#E8F0F8').attr('stroke-width', 1.5);
            })
            .on('mousemove', function (event) {
                setTooltipPos({ x: event.offsetX, y: event.offsetY });
            })
            .on('mouseleave', function () {
                setHoveredCountry(null);
                d3.select(this).attr('stroke', colors.border).attr('stroke-width', 0.5);
            })
            .on('click', function (event, d) {
                const alpha3 = numToAlpha[d.id];
                const cd = alpha3 ? countryMap[alpha3] : null;
                setSelectedCountry(cd || null);
            });

        // Flow arcs
        if (data.flows) {
            const flowG = g.append('g').attr('class', 'flows');
            const maxVol = Math.max(...data.flows.map(f => f.volume || 0), 1);

            data.flows.forEach(flow => {
                if (!layers[flow.type]) return;
                const from = centroids.current[flow.from];
                const to = centroids.current[flow.to];
                if (!from || !to) return;

                const dx = to[0] - from[0];
                const dy = to[1] - from[1];
                const dr = Math.sqrt(dx * dx + dy * dy) * 1.5;
                const thickness = Math.max(0.8, Math.min(4, (flow.volume / maxVol) * 5));
                const flowColor = flow.type === 'trade' ? '#06B6D4' : '#8B5CF6';

                flowG.append('path')
                    .attr('d', `M${from[0]},${from[1]} A${dr},${dr} 0 0,1 ${to[0]},${to[1]}`)
                    .attr('fill', 'none')
                    .attr('stroke', flowColor)
                    .attr('stroke-width', thickness)
                    .attr('stroke-opacity', 0.4)
                    .attr('stroke-dasharray', flow.type === 'capital' ? '4,3' : 'none')
                    .style('pointer-events', 'none');

                // Arrow head
                const angle = Math.atan2(to[1] - from[1], to[0] - from[0]);
                const headLen = 4 + thickness;
                const tipX = to[0] - Math.cos(angle) * 2;
                const tipY = to[1] - Math.sin(angle) * 2;
                flowG.append('polygon')
                    .attr('points', [
                        [tipX, tipY],
                        [tipX - headLen * Math.cos(angle - 0.4), tipY - headLen * Math.sin(angle - 0.4)],
                        [tipX - headLen * Math.cos(angle + 0.4), tipY - headLen * Math.sin(angle + 0.4)],
                    ].map(p => p.join(',')).join(' '))
                    .attr('fill', flowColor)
                    .attr('opacity', 0.6)
                    .style('pointer-events', 'none');
            });
        }

        // Hotspot pulsing markers
        if (data.hotspots && data.hotspots.length > 0) {
            const hotG = g.append('g').attr('class', 'hotspots');
            data.hotspots.forEach(h => {
                const pos = centroids.current[h.country];
                if (!pos) return;
                const sev = h.severity === 'high' ? '#EF4444' : '#F59E0B';

                // Pulsing ring
                const pulse = hotG.append('circle')
                    .attr('cx', pos[0]).attr('cy', pos[1]).attr('r', 6)
                    .attr('fill', 'none').attr('stroke', sev).attr('stroke-width', 1.5);

                function animatePulse() {
                    pulse.attr('r', 6).attr('stroke-opacity', 0.8)
                        .transition().duration(1200).ease(d3.easeQuadOut)
                        .attr('r', 16).attr('stroke-opacity', 0)
                        .on('end', animatePulse);
                }
                animatePulse();

                // Center dot
                hotG.append('circle')
                    .attr('cx', pos[0]).attr('cy', pos[1]).attr('r', 3)
                    .attr('fill', sev).attr('stroke', '#080C10').attr('stroke-width', 1);
            });
        }

        // Night lights overlay (subtle glow on countries with data)
        if (layers.lights && data.countries) {
            data.countries.forEach(c => {
                if (c.night_lights_change == null) return;
                const pos = centroids.current[c.id];
                if (!pos) return;
                const intensity = Math.max(0, Math.min(1, 0.5 + c.night_lights_change * 8));
                g.append('circle')
                    .attr('cx', pos[0]).attr('cy', pos[1])
                    .attr('r', 8 + intensity * 10)
                    .attr('fill', `rgba(255, 200, 50, ${intensity * 0.3})`)
                    .attr('stroke', 'none')
                    .style('pointer-events', 'none')
                    .attr('filter', 'blur(3px)');
            });
        }

    }, [world, data, layers, dims]);

    // Detail panel helpers
    const getCountryFlows = useCallback((countryId) => {
        if (!data?.flows) return [];
        return data.flows
            .filter(f => f.from === countryId || f.to === countryId)
            .sort((a, b) => (b.volume || 0) - (a.volume || 0))
            .slice(0, 5);
    }, [data]);

    const getCountryHotspots = useCallback((countryId) => {
        if (!data?.hotspots) return [];
        return data.hotspots.filter(h => h.country === countryId);
    }, [data]);

    const toggleLayer = (id) => {
        setLayers(prev => ({ ...prev, [id]: !prev[id] }));
    };

    // Search-by-country: highlight matching countries in the SVG
    useEffect(() => {
        if (!svgRef.current || !world || !data) return;
        const q = countrySearch.toLowerCase().trim();
        const svg = d3.select(svgRef.current);
        svg.selectAll('path.country')
            .attr('stroke-width', function (d) {
                if (!q) return 0.5;
                const numToAlpha = {};
                Object.entries(ISO_ALPHA3_TO_NUM).forEach(([a, n]) => { numToAlpha[n] = a; });
                const alpha3 = numToAlpha[d.id];
                const cd = alpha3 ? countryMap[alpha3] : null;
                const name = (cd?.name || '').toLowerCase();
                const id = (alpha3 || '').toLowerCase();
                return (name.includes(q) || id.includes(q)) ? 2.5 : 0.3;
            })
            .attr('stroke', function (d) {
                if (!q) return colors.border;
                const numToAlpha = {};
                Object.entries(ISO_ALPHA3_TO_NUM).forEach(([a, n]) => { numToAlpha[n] = a; });
                const alpha3 = numToAlpha[d.id];
                const cd = alpha3 ? countryMap[alpha3] : null;
                const name = (cd?.name || '').toLowerCase();
                const id = (alpha3 || '').toLowerCase();
                return (name.includes(q) || id.includes(q)) ? '#FFD700' : colors.border;
            });
    }, [countrySearch, world, data]);

    // Zoom control handlers
    const handleZoomIn = useCallback(() => {
        if (!zoomRef.current) return;
        const { zoom, svg } = zoomRef.current;
        svg.transition().duration(300).call(zoom.scaleBy, 1.4);
    }, []);

    const handleZoomOut = useCallback(() => {
        if (!zoomRef.current) return;
        const { zoom, svg } = zoomRef.current;
        svg.transition().duration(300).call(zoom.scaleBy, 0.7);
    }, []);

    const handleFitScreen = useCallback(() => {
        if (!zoomRef.current) return;
        const { zoom, svg } = zoomRef.current;
        svg.transition().duration(500).call(zoom.transform, d3.zoomIdentity);
    }, []);

    const handleSearch = useCallback((query) => {
        setCountrySearch(query);
        // If an exact match is found, select the country
        if (query && data?.countries) {
            const q = query.toLowerCase().trim();
            const match = data.countries.find(c =>
                c.name?.toLowerCase() === q || c.id?.toLowerCase() === q
            );
            if (match) setSelectedCountry(match);
        }
    }, [data]);

    return (
        <div ref={fullScreenRef} style={{ padding: tokens.space.lg, maxWidth: '1400px', margin: '0 auto', background: isFullScreen ? colors.bg : undefined }}>
            {/* Header */}
            <div style={{ ...shared.header, display: 'flex', alignItems: 'center', gap: '12px', marginBottom: tokens.space.md }}>
                THE GLOBE
                <span style={{ fontSize: tokens.fontSize.sm, color: colors.textMuted, fontWeight: 400, fontFamily: colors.mono }}>
                    Global Capital Flows & Economic Activity
                </span>
            </div>

            {/* Layer toggles */}
            <div style={{
                display: 'flex', gap: '6px', marginBottom: tokens.space.md,
                flexWrap: 'wrap',
            }}>
                {LAYER_DEFS.map(l => (
                    <button key={l.id} onClick={() => toggleLayer(l.id)} style={{
                        background: layers[l.id] ? `${l.color}20` : 'transparent',
                        border: `1px solid ${layers[l.id] ? l.color : colors.border}`,
                        borderRadius: tokens.radius.sm, padding: '6px 14px',
                        fontSize: tokens.fontSize.sm, fontWeight: 600,
                        color: layers[l.id] ? l.color : colors.textMuted,
                        cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                        transition: `all ${tokens.transition.fast}`,
                    }}>
                        {l.label}
                    </button>
                ))}
            </div>

            {/* Main layout: map + detail panel */}
            <div style={{ display: 'flex', gap: tokens.space.lg }}>
                {/* Map area */}
                <div ref={containerRef} style={{
                    flex: 1, minWidth: 0,
                    ...shared.card, padding: 0, position: 'relative', overflow: 'hidden',
                }}>
                    {loading ? (
                        <div style={{ padding: '80px 20px', textAlign: 'center', color: colors.textMuted, fontFamily: colors.mono, fontSize: '13px' }}>
                            Loading globe data...
                        </div>
                    ) : error ? (
                        <div style={{ padding: '20px', color: colors.red, fontSize: '12px' }}>{error}</div>
                    ) : (
                        <>
                            <ChartControls
                                onZoomIn={handleZoomIn}
                                onZoomOut={handleZoomOut}
                                onFitScreen={handleFitScreen}
                                onFullScreen={toggleFullScreen}
                                isFullScreen={isFullScreen}
                                onSearch={handleSearch}
                                searchPlaceholder="Search country..."
                            />
                            <svg
                                ref={svgRef}
                                width={dims.width}
                                height={dims.height}
                                style={{ background: colors.bg, display: 'block', width: '100%', height: 'auto' }}
                                viewBox={`0 0 ${dims.width} ${dims.height}`}
                            />
                            {/* Tooltip */}
                            {hoveredCountry && (
                                <div style={{
                                    position: 'absolute',
                                    left: Math.min(tooltipPos.x + 12, dims.width - 220),
                                    top: tooltipPos.y - 10,
                                    background: colors.glassOverlay,
                                    backdropFilter: 'blur(12px)',
                                    border: `1px solid ${colors.border}`,
                                    borderRadius: tokens.radius.sm,
                                    padding: '10px 14px',
                                    pointerEvents: 'none',
                                    zIndex: 10,
                                    minWidth: '180px',
                                    fontFamily: colors.mono,
                                    fontSize: '11px',
                                }}>
                                    <div style={{ fontWeight: 700, color: '#E8F0F8', fontSize: '13px', marginBottom: '6px' }}>
                                        {countryFlag(hoveredCountry.id)} {hoveredCountry.name}
                                    </div>
                                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 12px' }}>
                                        <span style={{ color: colors.textMuted }}>GDP Signal</span>
                                        <span style={{
                                            color: hoveredCountry.gdp_signal === 'growth' ? colors.green
                                                : hoveredCountry.gdp_signal === 'slowing' ? colors.red : colors.text,
                                            fontWeight: 600,
                                        }}>{hoveredCountry.gdp_signal}</span>
                                        <span style={{ color: colors.textMuted }}>FX 1m</span>
                                        <span style={{
                                            color: (hoveredCountry.fx_change_1m || 0) >= 0 ? colors.green : colors.red,
                                        }}>{fmtPct(hoveredCountry.fx_change_1m)}</span>
                                        <span style={{ color: colors.textMuted }}>Night Lights</span>
                                        <span style={{
                                            color: (hoveredCountry.night_lights_change || 0) >= 0 ? colors.green : colors.red,
                                        }}>{fmtPct(hoveredCountry.night_lights_change)}</span>
                                        <span style={{ color: colors.textMuted }}>Activity</span>
                                        <span style={{
                                            color: activityColor(hoveredCountry.activity_score),
                                            fontWeight: 600,
                                        }}>{(hoveredCountry.activity_score * 100).toFixed(0)}%</span>
                                    </div>
                                    {hotspotSet.has(hoveredCountry.id) && (
                                        <div style={{
                                            marginTop: '6px', padding: '4px 8px', borderRadius: '4px',
                                            background: '#EF444420', color: '#EF4444', fontSize: '10px', fontWeight: 600,
                                        }}>
                                            RED FLAG DETECTED
                                        </div>
                                    )}
                                </div>
                            )}
                            {/* Map instructions */}
                            <div style={{
                                position: 'absolute', bottom: '8px', left: '12px',
                                fontSize: '9px', color: colors.textMuted, fontFamily: colors.mono,
                            }}>
                                Scroll to zoom -- Drag to pan -- Click country for detail
                            </div>
                        </>
                    )}
                </div>

                {/* Detail panel (right sidebar) */}
                {selectedCountry && (
                    <div style={{
                        width: '300px', flexShrink: 0,
                        ...shared.card, padding: 0, overflowY: 'auto', maxHeight: `${dims.height + 60}px`,
                    }}>
                        {/* Country header */}
                        <div style={{
                            padding: '14px 16px', borderBottom: `1px solid ${colors.border}`,
                            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                        }}>
                            <div>
                                <div style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8' }}>
                                    {countryFlag(selectedCountry.id)} {selectedCountry.name}
                                </div>
                                <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: colors.mono, marginTop: '2px' }}>
                                    {selectedCountry.id}
                                </div>
                            </div>
                            <button onClick={() => setSelectedCountry(null)} style={{
                                background: 'none', border: `1px solid ${colors.border}`,
                                borderRadius: '4px', padding: '4px 8px', color: colors.textMuted,
                                cursor: 'pointer', fontSize: '10px',
                            }}>Close</button>
                        </div>

                        {/* Key metrics */}
                        <div style={{ padding: '12px 16px', borderBottom: `1px solid ${colors.border}` }}>
                            <div style={shared.sectionTitle}>KEY METRICS</div>
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
                                <MetricBox label="GDP Signal" value={selectedCountry.gdp_signal}
                                    color={selectedCountry.gdp_signal === 'growth' ? colors.green
                                        : selectedCountry.gdp_signal === 'slowing' ? colors.red : colors.text} />
                                <MetricBox label="FX Change 1m" value={fmtPct(selectedCountry.fx_change_1m)}
                                    color={(selectedCountry.fx_change_1m || 0) >= 0 ? colors.green : colors.red} />
                                <MetricBox label="Night Lights" value={fmtPct(selectedCountry.night_lights_change)}
                                    color={(selectedCountry.night_lights_change || 0) >= 0 ? colors.green : colors.red} />
                                <MetricBox label="Activity Score"
                                    value={`${(selectedCountry.activity_score * 100).toFixed(0)}%`}
                                    color={activityColor(selectedCountry.activity_score)} />
                            </div>
                        </div>

                        {/* Trade partners */}
                        <div style={{ padding: '12px 16px', borderBottom: `1px solid ${colors.border}` }}>
                            <div style={shared.sectionTitle}>TOP TRADE PARTNERS</div>
                            {getCountryFlows(selectedCountry.id).length === 0 ? (
                                <div style={{ fontSize: '11px', color: colors.textMuted }}>No flow data available</div>
                            ) : (
                                getCountryFlows(selectedCountry.id).map((f, i) => {
                                    const partner = f.from === selectedCountry.id ? f.to : f.from;
                                    const isExport = f.from === selectedCountry.id;
                                    const arrow = isExport ? '\u2192' : '\u2190';
                                    const flowColor = f.type === 'trade' ? '#06B6D4' : '#8B5CF6';
                                    return (
                                        <div key={i} style={{
                                            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                                            padding: '6px 0', borderBottom: i < 4 ? `1px solid ${colors.borderSubtle}` : 'none',
                                            fontSize: '11px',
                                        }}>
                                            <span style={{ color: colors.text, fontFamily: colors.mono }}>
                                                {countryFlag(partner)} {partner}
                                                <span style={{ color: flowColor, margin: '0 4px' }}>{arrow}</span>
                                                <span style={{
                                                    fontSize: '9px', padding: '1px 4px', borderRadius: '3px',
                                                    background: `${flowColor}20`, color: flowColor,
                                                }}>{f.type}</span>
                                            </span>
                                            <span style={{ color: colors.text, fontFamily: colors.mono, fontWeight: 600 }}>
                                                {fmtB(f.volume)}
                                            </span>
                                        </div>
                                    );
                                })
                            )}
                        </div>

                        {/* Cross-reference status */}
                        <div style={{ padding: '12px 16px' }}>
                            <div style={shared.sectionTitle}>CROSS-REFERENCE STATUS</div>
                            {getCountryHotspots(selectedCountry.id).length === 0 ? (
                                <div style={{
                                    padding: '8px 12px', borderRadius: tokens.radius.sm,
                                    background: '#22C55E10', color: colors.green,
                                    fontSize: '11px', fontFamily: colors.mono,
                                }}>
                                    No divergences detected
                                </div>
                            ) : (
                                getCountryHotspots(selectedCountry.id).map((h, i) => (
                                    <div key={i} style={{
                                        padding: '8px 12px', borderRadius: tokens.radius.sm,
                                        background: h.severity === 'high' ? '#EF444415' : '#F59E0B15',
                                        border: `1px solid ${h.severity === 'high' ? '#EF444440' : '#F59E0B40'}`,
                                        marginBottom: '6px',
                                    }}>
                                        <div style={{
                                            fontSize: '10px', fontWeight: 700,
                                            color: h.severity === 'high' ? '#EF4444' : '#F59E0B',
                                            marginBottom: '3px',
                                        }}>
                                            {h.severity === 'high' ? 'RED FLAG' : 'WARNING'}
                                        </div>
                                        <div style={{ fontSize: '11px', color: colors.text, lineHeight: '1.4' }}>
                                            {h.reason}
                                        </div>
                                    </div>
                                ))
                            )}
                        </div>
                    </div>
                )}
            </div>

            {/* FX Ticker Strip */}
            {data?.fx_map && (
                <div style={{
                    ...shared.card, marginTop: tokens.space.md, padding: '10px 16px',
                    display: 'flex', gap: '20px', overflowX: 'auto',
                    WebkitOverflowScrolling: 'touch', scrollbarWidth: 'none',
                }}>
                    <div style={{
                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                        whiteSpace: 'nowrap', alignSelf: 'center',
                    }}>FX</div>
                    {Object.entries(data.fx_map).map(([pair, val]) => (
                        <div key={pair} style={{
                            whiteSpace: 'nowrap', fontFamily: colors.mono,
                            fontSize: '11px', display: 'flex', gap: '6px', alignItems: 'center',
                        }}>
                            <span style={{ color: colors.textMuted, fontWeight: 600 }}>{pair}</span>
                            <span style={{ color: val != null ? colors.text : colors.textMuted }}>
                                {val != null ? val.toFixed(pair === 'USDJPY' || pair === 'USDKRW' ? 1 : 4) : '--'}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            {/* Legend */}
            <div style={{
                display: 'flex', gap: '16px', marginTop: tokens.space.sm,
                fontSize: '9px', color: colors.textMuted, fontFamily: colors.mono,
                flexWrap: 'wrap',
            }}>
                <span>
                    <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '2px', background: '#22C55E', marginRight: '4px', verticalAlign: 'middle' }} />
                    High Activity
                </span>
                <span>
                    <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '2px', background: '#5A7080', marginRight: '4px', verticalAlign: 'middle' }} />
                    Neutral
                </span>
                <span>
                    <span style={{ display: 'inline-block', width: '10px', height: '10px', borderRadius: '2px', background: '#EF4444', marginRight: '4px', verticalAlign: 'middle' }} />
                    Declining
                </span>
                <span>
                    <span style={{ display: 'inline-block', width: '8px', height: '2px', background: '#06B6D4', marginRight: '4px', verticalAlign: 'middle' }} />
                    Trade Flow
                </span>
                <span>
                    <span style={{ display: 'inline-block', width: '8px', height: '2px', background: '#8B5CF6', marginRight: '4px', verticalAlign: 'middle', borderTop: '1px dashed #8B5CF6' }} />
                    Capital Flow
                </span>
                <span>
                    <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', background: '#EF4444', marginRight: '4px', verticalAlign: 'middle' }} />
                    Hotspot
                </span>
                {data && (
                    <span style={{ marginLeft: 'auto' }}>
                        {data.countries?.length || 0} countries -- {data.flows?.length || 0} flows -- {data.hotspots?.length || 0} hotspots
                    </span>
                )}
            </div>
        </div>
    );
}

function MetricBox({ label, value, color }) {
    return (
        <div style={{
            background: colors.bg, borderRadius: tokens.radius.sm,
            padding: '8px 10px', textAlign: 'center',
        }}>
            <div style={{ fontSize: '13px', fontWeight: 700, color, fontFamily: colors.mono }}>
                {value}
            </div>
            <div style={{ fontSize: '9px', color: colors.textMuted, marginTop: '2px' }}>
                {label}
            </div>
        </div>
    );
}
