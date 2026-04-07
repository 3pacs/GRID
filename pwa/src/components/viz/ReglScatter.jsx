/**
 * ReglScatter — WebGL scatter plot for 20M+ data points.
 *
 * Uses regl-scatterplot for GPU-accelerated rendering with lasso selection.
 *
 * Props:
 *   points        — Float32Array or array of [x, y] pairs
 *   colors        — optional color array (hex strings or RGBA arrays)
 *   sizes         — optional per-point size array
 *   opacity       — global opacity (default: 0.6)
 *   pointSize     — global point size (default: 3)
 *   height        — container height (default: 400)
 *   performanceMode — enable for >2M points (default: auto)
 *   showLasso     — enable lasso selection (default: true)
 *   onSelect      — callback(selectedIndices) on lasso selection
 *   onHover       — callback(pointIndex) on hover
 *   colorBy       — optional category array for automatic coloring
 */
import { useRef, useEffect, useState } from 'react';
import createScatterplot from 'regl-scatterplot';

const GRID_PALETTE = [
    [0.102, 0.431, 0.749, 0.8],  // #1A6EBF
    [0.063, 0.725, 0.506, 0.8],  // #10B981
    [0.937, 0.267, 0.267, 0.8],  // #EF4444
    [0.961, 0.620, 0.043, 0.8],  // #F59E0B
    [0.545, 0.361, 0.965, 0.8],  // #8B5CF6
    [0.925, 0.286, 0.600, 0.8],  // #EC4899
    [0.024, 0.714, 0.831, 0.8],  // #06B6D4
    [0.518, 0.800, 0.086, 0.8],  // #84CC16
];

export default function ReglScatter({
    points = [],
    colors,
    sizes,
    opacity = 0.6,
    pointSize = 3,
    height = 400,
    performanceMode,
    showLasso = true,
    onSelect,
    onHover,
    colorBy,
}) {
    const containerRef = useRef(null);
    const canvasRef = useRef(null);
    const scatterRef = useRef(null);
    const [info, setInfo] = useState('');

    useEffect(() => {
        if (!containerRef.current) return;

        const container = containerRef.current;
        const width = container.clientWidth;

        // Create canvas
        let canvas = canvasRef.current;
        if (!canvas) {
            canvas = document.createElement('canvas');
            canvas.style.width = '100%';
            canvas.style.height = `${height}px`;
            container.innerHTML = '';
            container.appendChild(canvas);
            canvasRef.current = canvas;
        }
        canvas.width = width * (window.devicePixelRatio || 1);
        canvas.height = height * (window.devicePixelRatio || 1);
        canvas.style.height = `${height}px`;

        // Destroy previous
        if (scatterRef.current) {
            scatterRef.current.destroy();
            scatterRef.current = null;
        }

        const numPoints = Array.isArray(points) ? points.length : points.length / 2;
        const autoPerf = performanceMode ?? numPoints > 2_000_000;

        try {
            const scatterplot = createScatterplot({
                canvas,
                width,
                height,
                pointSize: autoPerf ? Math.min(pointSize, 1) : pointSize,
                opacity: autoPerf ? 0.3 : opacity,
                backgroundColor: [0.051, 0.082, 0.125, 1], // #0D1520
                lassoColor: [0.102, 0.431, 0.749, 0.5],
                lassoMinDelay: 10,
                lassoMinDist: 2,
                showReticle: !autoPerf,
                reticleColor: [0.102, 0.431, 0.749, 0.8],
                performanceMode: autoPerf,
            });

            scatterRef.current = scatterplot;

            // Build draw config
            const drawConfig = {};

            // Handle points format
            if (points instanceof Float32Array) {
                drawConfig.x = new Float32Array(numPoints);
                drawConfig.y = new Float32Array(numPoints);
                for (let i = 0; i < numPoints; i++) {
                    drawConfig.x[i] = points[i * 2];
                    drawConfig.y[i] = points[i * 2 + 1];
                }
            } else if (Array.isArray(points) && points.length > 0) {
                if (Array.isArray(points[0])) {
                    drawConfig.x = new Float32Array(points.map(p => p[0]));
                    drawConfig.y = new Float32Array(points.map(p => p[1]));
                } else {
                    // Flat interleaved
                    drawConfig.x = new Float32Array(numPoints);
                    drawConfig.y = new Float32Array(numPoints);
                    for (let i = 0; i < numPoints; i++) {
                        drawConfig.x[i] = points[i * 2];
                        drawConfig.y[i] = points[i * 2 + 1];
                    }
                }
            }

            // Category coloring
            if (colorBy) {
                const categories = [...new Set(colorBy)];
                const catMap = new Map(categories.map((c, i) => [c, i]));
                scatterplot.set({
                    pointColor: GRID_PALETTE.slice(0, categories.length),
                });
                drawConfig.category = new Uint8Array(colorBy.map(c => catMap.get(c)));
            }

            if (drawConfig.x && drawConfig.x.length > 0) {
                scatterplot.draw(drawConfig);
                setInfo(`${numPoints.toLocaleString()} points${autoPerf ? ' (perf mode)' : ''}`);
            }

            // Events
            if (onSelect) {
                scatterplot.subscribe('select', ({ points: sel }) => {
                    onSelect(sel);
                });
            }
            if (onHover) {
                scatterplot.subscribe('pointOver', (idx) => onHover(idx));
                scatterplot.subscribe('pointOut', () => onHover(null));
            }
        } catch (err) {
            console.error('regl-scatterplot init error:', err);
            setInfo(`Error: ${err.message}`);
        }

        return () => {
            if (scatterRef.current) {
                scatterRef.current.destroy();
                scatterRef.current = null;
            }
        };
    }, [points, colors, sizes, opacity, pointSize, height, performanceMode, colorBy]);

    return (
        <div style={{ width: '100%', position: 'relative' }}>
            <div
                ref={containerRef}
                style={{
                    width: '100%',
                    height,
                    background: '#0D1520',
                    borderRadius: 6,
                    overflow: 'hidden',
                }}
            />
            {info && (
                <div style={{
                    position: 'absolute',
                    bottom: 8,
                    right: 12,
                    color: '#5A7080',
                    fontSize: 11,
                    fontFamily: 'IBM Plex Mono',
                }}>
                    {info}
                </div>
            )}
        </div>
    );
}
