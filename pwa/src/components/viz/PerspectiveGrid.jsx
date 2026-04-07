/**
 * PerspectiveGrid — FINOS Perspective streaming data grid.
 *
 * High-performance pivot table and chart viewer powered by WebAssembly.
 * Handles millions of rows with real-time streaming updates.
 *
 * Props:
 *   data        — array of objects or Arrow table
 *   columns     — array of column names to display (optional, shows all)
 *   groupBy     — array of column names to group by
 *   splitBy     — array of column names to split by
 *   sort        — array of [column, 'asc'|'desc'] pairs
 *   aggregates  — { column: 'sum'|'avg'|'count'|... }
 *   plugin      — 'Datagrid' | 'X/Y Scatter' | 'Y Line' | 'Heatmap' etc.
 *   height      — container height (default: 400)
 *   editable    — allow inline editing (default: false)
 *   onConfigChange — callback when user changes config
 */
import { useRef, useEffect, useState } from 'react';

// Perspective requires dynamic imports (WASM)
let perspectivePromise = null;
function loadPerspective() {
    if (!perspectivePromise) {
        perspectivePromise = import('@finos/perspective');
    }
    return perspectivePromise;
}

const GRID_STYLES = `
    :host {
        --background-color: #0D1520;
        --inactive-color: #5A7080;
        --active-color: #1A6EBF;
        --error-color: #EF4444;
        --plugin--font-family: 'IBM Plex Mono', monospace;
        --plugin--background-color: #0D1520;
        --plugin--border-color: #1A2332;
    }
    perspective-viewer {
        font-family: 'IBM Plex Mono', monospace;
    }
`;

export default function PerspectiveGrid({
    data = [],
    columns,
    groupBy,
    splitBy,
    sort,
    aggregates,
    plugin = 'Datagrid',
    height = 400,
    editable = false,
    onConfigChange,
}) {
    const containerRef = useRef(null);
    const viewerRef = useRef(null);
    const tableRef = useRef(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let cancelled = false;

        async function init() {
            try {
                const perspective = await loadPerspective();

                if (cancelled || !containerRef.current) return;

                // Import custom elements
                await import('@finos/perspective-viewer');
                await import('@finos/perspective-viewer-datagrid');
                await import('@finos/perspective-viewer-d3fc');

                if (cancelled || !containerRef.current) return;

                // Create viewer if not exists
                if (!viewerRef.current) {
                    const viewer = document.createElement('perspective-viewer');
                    viewer.setAttribute('theme', 'Pro Dark');
                    containerRef.current.innerHTML = '';
                    containerRef.current.appendChild(viewer);
                    viewerRef.current = viewer;

                    // Inject GRID styles
                    const style = document.createElement('style');
                    style.textContent = GRID_STYLES;
                    viewer.shadowRoot?.appendChild(style);
                }

                const viewer = viewerRef.current;

                // Create table with data
                if (data.length > 0) {
                    const worker = await perspective.default.worker();
                    const table = await worker.table(data);
                    tableRef.current = table;
                    await viewer.load(table);

                    // Apply configuration
                    const config = { plugin };
                    if (columns) config.columns = columns;
                    if (groupBy) config.group_by = groupBy;
                    if (splitBy) config.split_by = splitBy;
                    if (sort) config.sort = sort;
                    if (aggregates) config.aggregates = aggregates;

                    await viewer.restore(config);
                }

                setLoading(false);
            } catch (err) {
                if (!cancelled) {
                    console.error('Perspective init error:', err);
                    setError(err.message);
                    setLoading(false);
                }
            }
        }

        init();

        return () => {
            cancelled = true;
            if (tableRef.current) {
                tableRef.current.delete();
                tableRef.current = null;
            }
        };
    }, [data, columns, groupBy, splitBy, sort, aggregates, plugin]);

    if (error) {
        return (
            <div style={{
                height,
                background: '#0D1520',
                borderRadius: 6,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                color: '#EF4444',
                fontFamily: 'IBM Plex Mono',
                fontSize: 13,
                padding: 20,
            }}>
                Perspective failed to load: {error}
            </div>
        );
    }

    return (
        <div style={{ width: '100%', position: 'relative' }}>
            {loading && (
                <div style={{
                    position: 'absolute',
                    inset: 0,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    background: '#0D1520',
                    color: '#8AA0B8',
                    fontFamily: 'IBM Plex Mono',
                    fontSize: 13,
                    zIndex: 10,
                    borderRadius: 6,
                }}>
                    Loading Perspective...
                </div>
            )}
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
        </div>
    );
}

/**
 * Stream new rows into an existing Perspective table.
 */
PerspectiveGrid.update = function update(tableRef, newRows) {
    if (tableRef.current) {
        tableRef.current.update(newRows);
    }
};
