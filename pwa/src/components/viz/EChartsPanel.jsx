/**
 * EChartsPanel — Apache ECharts wrapper for GRID.
 *
 * Supports 20+ chart types with progressive rendering for millions of points.
 * Auto-applies GRID dark theme and handles resize.
 *
 * Props:
 *   option    — ECharts option object (required)
 *   height    — container height (default: 400)
 *   theme     — 'grid-dark' (default) or custom theme name
 *   loading   — show loading spinner
 *   onReady   — callback(echartInstance)
 *   onEvents  — { eventName: handler } map
 *   className — optional container class
 */
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts/core';

// GRID dark theme for ECharts
const GRID_ECHARTS_THEME = {
    backgroundColor: '#0D1520',
    textStyle: {
        color: '#8AA0B8',
        fontFamily: 'IBM Plex Mono, monospace',
    },
    title: {
        textStyle: { color: '#E2E8F0', fontSize: 14, fontWeight: 500 },
        subtextStyle: { color: '#5A7080' },
    },
    legend: {
        textStyle: { color: '#8AA0B8' },
    },
    tooltip: {
        backgroundColor: 'rgba(13, 21, 32, 0.95)',
        borderColor: '#1A2332',
        textStyle: { color: '#E2E8F0', fontFamily: 'IBM Plex Mono' },
    },
    xAxis: {
        axisLine: { lineStyle: { color: '#1A2332' } },
        splitLine: { lineStyle: { color: '#1A2332', type: 'dashed' } },
        axisLabel: { color: '#5A7080' },
    },
    yAxis: {
        axisLine: { lineStyle: { color: '#1A2332' } },
        splitLine: { lineStyle: { color: '#1A2332', type: 'dashed' } },
        axisLabel: { color: '#5A7080' },
    },
    color: ['#1A6EBF', '#10B981', '#EF4444', '#F59E0B', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'],
    categoryAxis: {
        axisLine: { lineStyle: { color: '#1A2332' } },
        splitLine: { show: false },
    },
    valueAxis: {
        axisLine: { lineStyle: { color: '#1A2332' } },
        splitLine: { lineStyle: { color: '#1A2332', type: 'dashed' } },
    },
    dataZoom: [
        {
            type: 'inside',
            textStyle: { color: '#8AA0B8' },
        },
        {
            type: 'slider',
            backgroundColor: '#111B2A',
            borderColor: '#1A2332',
            fillerColor: 'rgba(26, 110, 191, 0.15)',
            handleStyle: { color: '#1A6EBF' },
            textStyle: { color: '#8AA0B8' },
        },
    ],
};

// Register theme once
let themeRegistered = false;
function ensureTheme() {
    if (!themeRegistered) {
        echarts.registerTheme('grid-dark', GRID_ECHARTS_THEME);
        themeRegistered = true;
    }
}

export default function EChartsPanel({
    option,
    height = 400,
    theme = 'grid-dark',
    loading = false,
    onReady,
    onEvents = {},
    className = '',
}) {
    ensureTheme();

    return (
        <div className={className} style={{ width: '100%' }}>
            <ReactECharts
                option={option}
                theme={theme}
                style={{ height, width: '100%' }}
                opts={{ renderer: 'canvas' }}
                showLoading={loading}
                loadingOption={{
                    text: 'Loading...',
                    color: '#1A6EBF',
                    textColor: '#8AA0B8',
                    maskColor: 'rgba(8, 12, 16, 0.8)',
                }}
                onChartReady={onReady}
                onEvents={onEvents}
                notMerge={true}
                lazyUpdate={true}
            />
        </div>
    );
}

/**
 * Pre-built option generators for common GRID chart patterns.
 */
EChartsPanel.heatmapOption = function heatmapOption({ data, xLabels, yLabels, title = '' }) {
    return {
        title: { text: title, left: 'center' },
        tooltip: {
            position: 'top',
            formatter: (p) => `${xLabels[p.data[0]]} / ${yLabels[p.data[1]]}: ${p.data[2].toFixed(2)}`,
        },
        grid: { top: 40, right: 20, bottom: 60, left: 100 },
        xAxis: { type: 'category', data: xLabels, splitArea: { show: true } },
        yAxis: { type: 'category', data: yLabels, splitArea: { show: true } },
        visualMap: {
            min: 0,
            max: 1,
            calculable: true,
            orient: 'horizontal',
            left: 'center',
            bottom: 0,
            inRange: { color: ['#0D3320', '#10B981', '#F59E0B', '#EF4444'] },
            textStyle: { color: '#8AA0B8' },
        },
        series: [{
            type: 'heatmap',
            data,
            label: { show: true, color: '#E2E8F0', fontSize: 10 },
            emphasis: {
                itemStyle: { shadowBlur: 10, shadowColor: 'rgba(26, 110, 191, 0.5)' },
            },
        }],
    };
};

EChartsPanel.sankeyOption = function sankeyOption({ nodes, links, title = '' }) {
    return {
        title: { text: title, left: 'center' },
        tooltip: { trigger: 'item', triggerOn: 'mousemove' },
        series: [{
            type: 'sankey',
            data: nodes,
            links,
            emphasis: { focus: 'adjacency' },
            lineStyle: { color: 'gradient', curveness: 0.5 },
            label: { color: '#E2E8F0', fontSize: 11 },
            itemStyle: { borderWidth: 0 },
            nodeWidth: 20,
            nodeGap: 12,
        }],
    };
};

EChartsPanel.radarOption = function radarOption({ indicators, values, title = '' }) {
    return {
        title: { text: title, left: 'center' },
        radar: {
            indicator: indicators,
            shape: 'polygon',
            splitNumber: 5,
            axisLine: { lineStyle: { color: '#1A2332' } },
            splitLine: { lineStyle: { color: '#1A2332' } },
            splitArea: { areaStyle: { color: ['rgba(26, 110, 191, 0.02)', 'rgba(26, 110, 191, 0.05)'] } },
            axisName: { color: '#8AA0B8', fontSize: 11 },
        },
        series: [{
            type: 'radar',
            data: values.map((v, i) => ({
                value: v.data,
                name: v.name,
                areaStyle: { opacity: 0.15 },
            })),
        }],
    };
};

EChartsPanel.treemapOption = function treemapOption({ data, title = '' }) {
    return {
        title: { text: title, left: 'center' },
        tooltip: {
            formatter: (p) => `${p.name}: ${p.value}`,
        },
        series: [{
            type: 'treemap',
            data,
            roam: false,
            nodeClick: 'zoomToNode',
            breadcrumb: { itemStyle: { color: '#1A2332', textStyle: { color: '#8AA0B8' } } },
            label: { show: true, color: '#E2E8F0', fontSize: 12 },
            itemStyle: { borderColor: '#0D1520', borderWidth: 2 },
            levels: [
                { itemStyle: { borderColor: '#1A2332', borderWidth: 3 } },
                { itemStyle: { borderColor: '#1A2332', borderWidth: 1 }, colorSaturation: [0.3, 0.7] },
            ],
        }],
    };
};
