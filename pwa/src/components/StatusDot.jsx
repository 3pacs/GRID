import React from 'react';

const colors = {
    online: '#1A7A4A',
    offline: '#8B1F1F',
    warning: '#8A6000',
    unknown: '#5A7080',
};

export default function StatusDot({ status = 'unknown', size = 8, label }) {
    const color = colors[status] || colors.unknown;
    return (
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px' }}>
            <span style={{
                width: size,
                height: size,
                borderRadius: '50%',
                background: color,
                display: 'inline-block',
                boxShadow: status === 'online' ? `0 0 6px ${color}` : 'none',
            }} />
            {label && (
                <span style={{ fontSize: '12px', color: '#5A7080', fontFamily: "'IBM Plex Sans', sans-serif" }}>
                    {label}
                </span>
            )}
        </span>
    );
}
