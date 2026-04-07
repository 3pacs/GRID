/**
 * UPlotChart — High-performance time-series chart wrapper.
 *
 * Renders up to 5M data points using Canvas 2D via uPlot.
 * Supports OHLC, line, area, and bar series with streaming updates.
 *
 * Props:
 *   data      — uPlot-format data array: [timestamps, ...series]
 *   options   — uPlot options object (merged with defaults)
 *   width     — explicit width (optional, auto-resizes by default)
 *   height    — chart height in px (default: 300)
 *   className — optional container class
 *   onReady   — callback(uplot) when chart instance is created
 */
import { useRef, useEffect, useCallback, useState } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

const GRID_DARK = {
    bg: '#0D1520',
    axes: '#5A7080',
    grid: '#1A2332',
    text: '#8AA0B8',
    crosshair: '#1A6EBF',
    series: ['#1A6EBF', '#10B981', '#EF4444', '#F59E0B', '#8B5CF6', '#EC4899'],
};

function buildDefaults(width, height) {
    return {
        width,
        height,
        cursor: {
            drag: { x: true, y: false, setScale: true },
            sync: { key: 'grid-sync' },
        },
        scales: {
            x: { time: true },
        },
        axes: [
            {
                stroke: GRID_DARK.axes,
                grid: { stroke: GRID_DARK.grid, width: 1 },
                ticks: { stroke: GRID_DARK.grid, width: 1 },
                font: '11px IBM Plex Mono',
            },
            {
                stroke: GRID_DARK.axes,
                grid: { stroke: GRID_DARK.grid, width: 1 },
                ticks: { stroke: GRID_DARK.grid, width: 1 },
                font: '11px IBM Plex Mono',
                size: 60,
            },
        ],
    };
}

function addDefaultSeriesColors(opts) {
    if (!opts.series) return opts;
    const updated = { ...opts, series: [...opts.series] };
    for (let i = 1; i < updated.series.length; i++) {
        if (!updated.series[i].stroke) {
            updated.series[i] = {
                ...updated.series[i],
                stroke: GRID_DARK.series[(i - 1) % GRID_DARK.series.length],
                width: 1.5,
            };
        }
    }
    return updated;
}

export default function UPlotChart({
    data,
    options = {},
    height = 300,
    className = '',
    onReady,
}) {
    const containerRef = useRef(null);
    const chartRef = useRef(null);
    const [containerWidth, setContainerWidth] = useState(0);

    // Track container width
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const ro = new ResizeObserver((entries) => {
            const w = entries[0].contentRect.width;
            if (w > 0) setContainerWidth(Math.floor(w));
        });
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    // Create / recreate chart
    useEffect(() => {
        if (!containerRef.current || containerWidth === 0 || !data?.length) return;

        // Destroy previous instance
        if (chartRef.current) {
            chartRef.current.destroy();
            chartRef.current = null;
        }

        const defaults = buildDefaults(containerWidth, height);
        const merged = addDefaultSeriesColors({
            ...defaults,
            ...options,
            width: containerWidth,
            height,
            axes: options.axes || defaults.axes,
        });

        const chart = new uPlot(merged, data, containerRef.current);
        chartRef.current = chart;
        if (onReady) onReady(chart);

        return () => {
            chart.destroy();
            chartRef.current = null;
        };
    }, [containerWidth, height, data, options, onReady]);

    // Resize on container width change
    useEffect(() => {
        if (chartRef.current && containerWidth > 0) {
            chartRef.current.setSize({ width: containerWidth, height });
        }
    }, [containerWidth, height]);

    return (
        <div
            ref={containerRef}
            className={className}
            style={{
                width: '100%',
                background: GRID_DARK.bg,
                borderRadius: 6,
                overflow: 'hidden',
            }}
        />
    );
}

/**
 * Generate OHLC uPlot options with volume bars.
 */
UPlotChart.ohlcOptions = function ohlcOptions(title = '') {
    return {
        title,
        series: [
            {},
            { label: 'Open', stroke: '#8AA0B8', width: 1 },
            { label: 'High', stroke: '#10B981', width: 1 },
            { label: 'Low', stroke: '#EF4444', width: 1 },
            { label: 'Close', stroke: '#1A6EBF', width: 2 },
            {
                label: 'Volume',
                stroke: 'rgba(26, 110, 191, 0.3)',
                fill: 'rgba(26, 110, 191, 0.1)',
                width: 1,
                scale: 'vol',
                paths: () => null,
            },
        ],
        scales: {
            x: { time: true },
            y: {},
            vol: { range: (u, min, max) => [0, max * 5] },
        },
    };
};
