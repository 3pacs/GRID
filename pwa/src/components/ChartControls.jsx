/**
 * ChartControls -- Reusable chart controls overlay for all D3 visualizations.
 *
 * Provides: Zoom +/-, Fit-to-screen, Full-screen toggle, Search box.
 * Positioned in the top-right corner of the chart container.
 *
 * Props:
 *   onZoomIn       - callback to zoom in
 *   onZoomOut      - callback to zoom out
 *   onFitScreen    - callback to fit/reset zoom
 *   onFullScreen   - callback to toggle full-screen
 *   isFullScreen   - boolean, current full-screen state
 *   onSearch       - callback(query) for search filtering/highlighting
 *   searchPlaceholder - placeholder text for search input
 *   showSearch     - boolean, whether to show search (default true)
 *   showZoom       - boolean, whether to show zoom buttons (default true)
 *   compact        - boolean, smaller layout for tight spaces
 */
import React, { useState } from 'react';

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const baseBtn = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    width: '28px',
    height: '28px',
    background: 'rgba(13, 21, 32, 0.85)',
    backdropFilter: 'blur(8px)',
    border: '1px solid rgba(26, 35, 50, 0.8)',
    borderRadius: '5px',
    color: '#8AA0B8',
    cursor: 'pointer',
    fontSize: '14px',
    fontFamily: MONO,
    fontWeight: 700,
    padding: 0,
    transition: 'all 0.15s ease',
    lineHeight: 1,
};

const baseBtnHover = {
    background: 'rgba(26, 110, 191, 0.25)',
    borderColor: 'rgba(26, 110, 191, 0.5)',
    color: '#E2E8F0',
};

const activeBtn = {
    background: 'rgba(26, 110, 191, 0.35)',
    borderColor: '#1A6EBF',
    color: '#E2E8F0',
};

export default function ChartControls({
    onZoomIn,
    onZoomOut,
    onFitScreen,
    onFullScreen,
    isFullScreen = false,
    onSearch,
    searchPlaceholder = 'Search...',
    showSearch = true,
    showZoom = true,
    compact = false,
}) {
    const [hovered, setHovered] = useState(null);
    const [searchValue, setSearchValue] = useState('');

    const handleSearch = (val) => {
        setSearchValue(val);
        if (onSearch) onSearch(val);
    };

    const btn = (key, label, title, onClick, isActive = false) => (
        <button
            key={key}
            onClick={onClick}
            title={title}
            onMouseEnter={() => setHovered(key)}
            onMouseLeave={() => setHovered(null)}
            style={{
                ...baseBtn,
                ...(compact ? { width: '24px', height: '24px', fontSize: '12px' } : {}),
                ...(hovered === key ? baseBtnHover : {}),
                ...(isActive ? activeBtn : {}),
            }}
        >
            {label}
        </button>
    );

    return (
        <div
            style={{
                position: 'absolute',
                top: compact ? '6px' : '10px',
                right: compact ? '6px' : '10px',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'flex-end',
                gap: '4px',
                zIndex: 20,
                pointerEvents: 'none',
            }}
        >
            {/* Search row */}
            {showSearch && onSearch && (
                <div style={{ pointerEvents: 'auto' }}>
                    <input
                        type="text"
                        value={searchValue}
                        onChange={(e) => handleSearch(e.target.value)}
                        placeholder={searchPlaceholder}
                        style={{
                            background: 'rgba(13, 21, 32, 0.85)',
                            backdropFilter: 'blur(8px)',
                            border: '1px solid rgba(26, 35, 50, 0.8)',
                            borderRadius: '5px',
                            padding: compact ? '3px 8px' : '4px 10px',
                            fontSize: compact ? '10px' : '11px',
                            fontFamily: MONO,
                            color: '#E2E8F0',
                            width: compact ? '120px' : '160px',
                            outline: 'none',
                            transition: 'border-color 0.15s ease',
                        }}
                        onFocus={(e) => {
                            e.target.style.borderColor = 'rgba(26, 110, 191, 0.5)';
                        }}
                        onBlur={(e) => {
                            e.target.style.borderColor = 'rgba(26, 35, 50, 0.8)';
                        }}
                    />
                </div>
            )}

            {/* Button row */}
            <div style={{ display: 'flex', gap: '3px', pointerEvents: 'auto' }}>
                {showZoom && onZoomIn && btn('zin', '+', 'Zoom in', onZoomIn)}
                {showZoom && onZoomOut && btn('zout', '\u2212', 'Zoom out', onZoomOut)}
                {onFitScreen && btn('fit', '\u2B1C', 'Fit to screen', onFitScreen)}
                {onFullScreen && btn(
                    'fs',
                    isFullScreen ? '\u2716' : '\u26F6',
                    isFullScreen ? 'Exit full-screen' : 'Full-screen',
                    onFullScreen,
                    isFullScreen,
                )}
            </div>
        </div>
    );
}
