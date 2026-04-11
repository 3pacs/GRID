/**
 * LayerControls -- Compact layer toggle pills for the canvas command bar.
 * Each pill represents an intelligence layer that can be toggled on/off.
 */
import React, { useCallback } from 'react';
import { colors, tokens } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";

/* ── Layer definitions ───────────────────────────────────────── */

const LAYERS = [
    { key: 'financial', label: 'Financial', abbr: 'FIN', color: '#3B82F6' },
    { key: 'insider', label: 'Insider', abbr: 'INS', color: '#8B5CF6' },
    { key: 'political', label: 'Political', abbr: 'POL', color: '#F59E0B' },
    { key: 'news', label: 'News', abbr: 'NEWS', color: '#06B6D4' },
    { key: 'options', label: 'Options', abbr: 'OPT', color: '#10B981' },
    { key: 'macro', label: 'Macro', abbr: 'MAC', color: '#FFD700' },
    { key: 'offshore', label: 'Offshore', abbr: 'OFF', color: '#EF4444' },
    { key: 'predictions', label: 'Predictions', abbr: 'PRED', color: '#EC4899' },
];

/* ── Styles ──────────────────────────────────────────────────── */

const S = {
    container: {
        display: 'flex',
        alignItems: 'center',
        gap: '3px',
        flexWrap: 'nowrap',
        overflowX: 'auto',
        scrollbarWidth: 'none',
        msOverflowStyle: 'none',
    },
    pill: (active, layerColor) => ({
        display: 'inline-flex',
        alignItems: 'center',
        gap: '4px',
        height: '26px',
        padding: '0 8px',
        borderRadius: tokens.radius.pill,
        fontSize: '10px',
        fontWeight: 700,
        fontFamily: MONO,
        letterSpacing: '0.5px',
        cursor: 'pointer',
        border: 'none',
        background: active ? `${layerColor}33` : 'transparent',
        color: active ? layerColor : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
        flexShrink: 0,
        whiteSpace: 'nowrap',
    }),
    dot: (layerColor) => ({
        width: '5px',
        height: '5px',
        borderRadius: '50%',
        background: layerColor,
        flexShrink: 0,
    }),
};

/* ── Component ───────────────────────────────────────────────── */

export default function LayerControls({ activeLayers, onToggleLayer }) {
    // activeLayers can be a Set (from CanvasStore) or an object
    const isLayerActive = useCallback((key) => {
        if (!activeLayers) return true; // default all active
        if (activeLayers instanceof Set) return activeLayers.has(key);
        return activeLayers[key] !== false;
    }, [activeLayers]);

    const handleToggle = useCallback((key) => {
        onToggleLayer?.(key);
    }, [onToggleLayer]);

    return (
        <div style={S.container}>
            {LAYERS.map(layer => {
                const isActive = isLayerActive(layer.key);

                return (
                    <button
                        key={layer.key}
                        style={S.pill(isActive, layer.color)}
                        onClick={() => handleToggle(layer.key)}
                        onMouseEnter={(e) => {
                            if (!isActive) {
                                e.currentTarget.style.color = layer.color;
                                e.currentTarget.style.background = `${layer.color}15`;
                            }
                        }}
                        onMouseLeave={(e) => {
                            if (!isActive) {
                                e.currentTarget.style.color = colors.textMuted;
                                e.currentTarget.style.background = 'transparent';
                            }
                        }}
                        title={`${isActive ? 'Hide' : 'Show'} ${layer.label} layer`}
                    >
                        {isActive && <span style={S.dot(layer.color)} />}
                        {layer.abbr}
                    </button>
                );
            })}
        </div>
    );
}

export { LAYERS };
