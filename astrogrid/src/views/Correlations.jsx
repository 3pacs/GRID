import React, { useEffect, useMemo, useState } from 'react';
import api from '../api.js';
import CorrelationHeatmap from '../components/CorrelationHeatmap.jsx';
import { buildAstrogridCorrelationMatrix, normalizeAstrogridCorrelations } from '../lib/contract.js';
import { buildCorrelationMatrix } from '../lib/mockData.js';
import useStore from '../store.js';
import { tokens, styles } from '../styles/tokens.js';

export default function Correlations() {
    const { celestialData, setCorrelationData } = useStore();
    const [hovered, setHovered] = useState(null);
    const [error, setError] = useState(null);
    const [status, setStatus] = useState('loading');
    const [statusNote, setStatusNote] = useState('Waiting for live correlation data.');
    const [loading, setLoading] = useState(false);

    const fallback = useMemo(() => buildCorrelationMatrix(celestialData), [celestialData]);
    const [heatmapData, setHeatmapData] = useState(fallback);

    useEffect(() => {
        setHeatmapData(fallback);
    }, [fallback]);

    useEffect(() => {
        let cancelled = false;

        setError(null);
        setLoading(true);
        setStatus('loading');
        setStatusNote('Waiting for live correlation data.');
        api.getCorrelations({ market: 'spy', feature: 'lunar_phase', period: '1Y' })
            .then((data) => {
                if (cancelled) return;

                const normalizedRows = normalizeAstrogridCorrelations(data);
                if (!normalizedRows.length) {
                    setHeatmapData(fallback);
                    setStatus('demo');
                    setStatusNote('Correlation endpoint returned an unexpected shape, so the deterministic demo matrix is shown.');
                    setCorrelationData([]);
                    return;
                }

                const normalized = buildAstrogridCorrelationMatrix(data);
                if (normalized) {
                    setCorrelationData(normalizedRows);
                    setHeatmapData(normalized);
                    setStatus('live');
                    setStatusNote('Live backend correlations loaded.');
                    return;
                }

                setHeatmapData(fallback);
                setStatus('demo');
                setStatusNote('Correlation payload was present but not normalized, so the deterministic demo matrix is shown.');
                setCorrelationData(normalizedRows);
            })
            .catch((e) => {
                if (cancelled) return;
                setError(e.message);
                setHeatmapData(fallback);
                setStatus('demo');
                setStatusNote('Correlation endpoint unavailable, so the deterministic demo matrix is shown.');
                setCorrelationData([]);
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });

        return () => {
            cancelled = true;
        };
    }, [setCorrelationData]);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Celestial Correlations</div>
            <div style={styles.subheader}>Market-Astro Patterns</div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Analyzing correlations...</div>}
            <div style={styles.card}>
                <div style={styles.subheader}>Data Source</div>
                <div style={styles.value}>
                    {status === 'live'
                        ? 'Live backend correlations'
                        : status === 'loading'
                            ? 'Checking live correlations...'
                            : 'Generated demo matrix'}
                </div>
                <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                    {statusNote}
                </div>
            </div>

            <CorrelationHeatmap
                rows={heatmapData.rows.map((row) => row.label || row)}
                columns={heatmapData.columns.map((column) => column.label || column)}
                matrix={
                    heatmapData.matrix
                    || heatmapData.rows.map((row) =>
                        heatmapData.columns.map((column) => {
                            const cell = heatmapData.cells.find((item) =>
                                item.rowKey === row.key && item.columnKey === column.key
                            );
                            return cell?.value ?? 0;
                        })
                    )
                }
                title="Celestial x Market Correlations"
                subtitle="Stable signal feed plus deterministic fallback matrix"
                onCellHover={setHovered}
            />

            <div style={{ marginTop: tokens.spacing.lg }} />
            <div style={styles.card}>
                <div style={styles.subheader}>Hovered Cell</div>
                <div style={styles.value}>
                    {hovered
                        ? `${hovered.row} vs ${hovered.column}: ${hovered.value.toFixed(2)}`
                        : 'Hover a heatmap cell to inspect a relationship.'}
                </div>
            </div>
        </div>
    );
}
