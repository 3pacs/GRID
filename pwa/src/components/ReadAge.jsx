import React from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

export default function ReadAge({ value, retained = false }) {
    const info = api.getResponseInfo?.(value);
    return <div role="status" style={{ color: colors.textMuted, fontSize: '11px', margin: '8px 0' }}>
        {retained ? 'Previous response retained; refresh has not succeeded. ' : ''}
        {info ? `Response received ${new Date(info.receivedAt).toISOString()} (client cache age at display: ${Math.floor(info.ageMs / 1000)}s). ` : 'Response receipt time unknown. '}
        Data freshness follows the stored observation/result dates, not the request time.
    </div>;
}
