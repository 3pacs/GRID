import React from 'react';
import { colors } from '../styles/shared.js';

export default function GEXProvenance({ data }) {
    const value = (v) => typeof v === 'string' && v.trim() ? v : 'Unavailable';
    return <div style={{ padding: '8px 12px', fontSize: '11px', lineHeight: 1.5, color: colors.textMuted, overflowWrap: 'anywhere' }}>
        <div>Estimated exposure under assumed dealer positions, not measured holdings or trades.</div>
        <div>Model basis: {value(data.basis)}</div>
        <div>Chain session: {value(data.chain_snap_date)} · Capture completed: {value(data.chain_capture_completed_at)}</div>
        <div>Provider underlying quote times: {value(data.chain_provider_regular_market_at_min)} to {value(data.chain_provider_regular_market_at_max)}</div>
        <div>Underlying quote times do not establish option-chain or open-interest freshness.</div>
        <div>Reference price source: {value(data.spot_source)} · Basis: {value(data.spot_basis)}</div>
        <div>Reference price date: {value(data.spot_obs_date)} · Available at: {value(data.spot_available_at)}</div>
        <div>Capture time records ingestion, not the age of every option quote.</div>
    </div>;
}
