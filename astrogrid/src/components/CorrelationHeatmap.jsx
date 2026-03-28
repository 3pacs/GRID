import React from 'react';
import { tokens } from '../styles/tokens.js';

function clamp(value) {
    if (!Number.isFinite(value)) return 0;
    return Math.max(-1, Math.min(1, value));
}

function cellColor(value) {
    const v = clamp(value);
    if (v >= 0) {
        const mix = Math.round(72 + (34 - 72) * v);
        return `rgba(${mix}, ${Math.round(158 + (197 - 158) * v)}, ${Math.round(217 + (94 - 217) * v)}, ${0.2 + Math.abs(v) * 0.65})`;
    }
    const t = Math.abs(v);
    return `rgba(${Math.round(239 + (245 - 239) * t)}, ${Math.round(68 + (158 - 68) * t)}, ${Math.round(68 + (11 - 68) * t)}, ${0.2 + t * 0.65})`;
}

export default function CorrelationHeatmap({
    rows = [],
    columns = [],
    matrix = [],
    title = 'Correlation Heatmap',
    subtitle = 'Celestial x market features',
    onCellHover = null,
}) {
    const safeRows = rows.slice(0, 12);
    const safeColumns = columns.slice(0, 12);

    return (
        <div style={cardStyle}>
            <div style={headerRow}>
                <div>
                    <div style={eyebrow}>Heatmap</div>
                    <div style={headline}>{title}</div>
                </div>
                <div style={subhead}>{subtitle}</div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: `160px repeat(${safeColumns.length || 1}, minmax(52px, 1fr))`,
                gap: '6px',
                alignItems: 'stretch',
                minWidth: '100%',
                overflowX: 'auto',
            }}>
                <div />
                {safeColumns.map((column) => (
                    <div key={column} style={columnHeader} title={column}>{column}</div>
                ))}

                {safeRows.map((row, rowIndex) => (
                    <React.Fragment key={row}>
                        <div style={rowHeader} title={row}>{row}</div>
                        {safeColumns.map((column, columnIndex) => {
                            const value = matrix[rowIndex]?.[columnIndex] ?? 0;
                            const valueStr = Number.isFinite(value) ? value.toFixed(2) : '--';
                            return (
                                <button
                                    key={`${row}-${column}`}
                                    type="button"
                                    onMouseEnter={() => onCellHover?.({ row, column, value })}
                                    onFocus={() => onCellHover?.({ row, column, value })}
                                    title={`${row} x ${column}: ${valueStr}`}
                                    style={{
                                        appearance: 'none',
                                        border: `1px solid ${tokens.cardBorder}`,
                                        borderRadius: tokens.radius.sm,
                                        minHeight: '44px',
                                        background: cellColor(value),
                                        color: Math.abs(clamp(value)) > 0.65 ? '#08111D' : tokens.textBright,
                                        fontFamily: tokens.fontMono,
                                        fontSize: '12px',
                                        fontWeight: 700,
                                        cursor: 'pointer',
                                        transition: 'transform 0.15s ease, filter 0.15s ease',
                                    }}
                                >
                                    {valueStr}
                                </button>
                            );
                        })}
                    </React.Fragment>
                ))}
            </div>
        </div>
    );
}

const cardStyle = {
    background: tokens.card,
    border: `1px solid ${tokens.cardBorder}`,
    borderRadius: tokens.radius.xl,
    padding: tokens.spacing.lg,
    overflow: 'hidden',
};

const headerRow = {
    display: 'flex',
    justifyContent: 'space-between',
    gap: tokens.spacing.md,
    flexWrap: 'wrap',
    marginBottom: tokens.spacing.md,
};

const eyebrow = {
    fontSize: '11px',
    color: tokens.accent,
    fontFamily: tokens.fontMono,
    letterSpacing: '2px',
    textTransform: 'uppercase',
};

const headline = {
    marginTop: '4px',
    fontSize: '20px',
    fontWeight: 700,
    color: tokens.textBright,
};

const subhead = {
    fontSize: '12px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    alignSelf: 'end',
};

const columnHeader = {
    fontSize: '10px',
    color: tokens.textMuted,
    fontFamily: tokens.fontMono,
    textTransform: 'uppercase',
    letterSpacing: '1px',
    textAlign: 'center',
    padding: '4px 2px',
};

const rowHeader = {
    fontSize: '11px',
    color: tokens.textBright,
    fontFamily: tokens.fontMono,
    padding: '12px 8px 12px 0',
    textAlign: 'right',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
};
