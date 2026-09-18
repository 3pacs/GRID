import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { colors, shared } from '../styles/shared.js';

/**
 * God View pillars — W6 first slice.
 *
 * Only the CFTC positioning pillar has a materializer + route. Every other
 * pillar renders the honest "not built yet — no data" card rather than
 * silently omitting it or fabricating a value — see
 * docs/reference/GODVIEW_PILLAR_CONTRACT.md.
 */

const NOT_BUILT_PILLARS = [
    { key: 'finra_short_volume', label: 'FINRA Short Volume' },
    { key: 'sec_regsho_ftd', label: 'SEC Reg SHO — FTD' },
    { key: 'commodity_warehouses', label: 'Commodity Warehouses' },
    { key: 'fed_net_liquidity', label: 'Fed Net Liquidity' },
    { key: 'buyback_blackouts', label: 'Corporate Buyback Blackouts' },
    { key: 'dealer_gex', label: 'Dealer Gamma Exposure' },
];

function fmtNum(v, digits = 2) {
    if (v == null) return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return n.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function fmtDateTime(v) {
    if (!v) return '—';
    return String(v).replace('T', ' ').substring(0, 19);
}

function CoverageBar({ fraction }) {
    const pct = fraction == null ? 0 : Math.round(fraction * 100);
    return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div
                style={{
                    flex: 1,
                    height: 6,
                    borderRadius: 3,
                    background: colors.border,
                    overflow: 'hidden',
                }}
                data-testid="coverage-bar-track"
            >
                <div
                    style={{
                        width: `${pct}%`,
                        height: '100%',
                        background: fraction == null ? colors.textMuted : fraction >= 1 ? colors.green : colors.yellow,
                    }}
                />
            </div>
            <span style={{ fontSize: 12, color: colors.textDim, fontFamily: colors.mono }}>
                {fraction == null ? 'unknown' : `${pct}%`}
            </span>
        </div>
    );
}

function ProvenanceBadge({ provenance, availability }) {
    if (availability !== 'available') {
        return (
            <span style={{ fontSize: 11, color: colors.textMuted, border: `1px solid ${colors.border}`, borderRadius: 4, padding: '1px 6px' }}>
                unavailable
            </span>
        );
    }
    const color = provenance === 'measured' ? colors.green : provenance === 'derived' ? colors.accent : colors.yellow;
    return (
        <span style={{ fontSize: 11, color, border: `1px solid ${color}`, borderRadius: 4, padding: '1px 6px' }}>
            {provenance || 'unknown'}
        </span>
    );
}

function InferredBadge({ basis, note }) {
    if (!basis || basis === 'observed_acquisition') return null;
    const label = basis === 'inferred_schedule' ? 'inferred' : 'basis unknown';
    return (
        <span
            title={note || ''}
            data-testid="inferred-badge"
            style={{
                fontSize: 10,
                color: colors.yellow,
                border: `1px solid ${colors.yellow}`,
                borderRadius: 4,
                padding: '1px 5px',
                marginLeft: 4,
            }}
        >
            {label}
        </span>
    );
}

function FieldRow({ name, field }) {
    if (!field) return null;
    return (
        <div
            style={{
                display: 'grid',
                gridTemplateColumns: '1fr auto auto',
                gap: 8,
                alignItems: 'center',
                padding: '6px 0',
                borderBottom: `1px solid ${colors.borderSubtle}`,
            }}
        >
            <div>
                <div style={{ fontSize: 12, color: colors.textDim }}>{name}</div>
                <div style={{ fontSize: 15, color: colors.text, fontFamily: colors.mono }}>
                    {field.availability === 'available' ? fmtNum(field.value) : '—'}
                    {field.unit ? <span style={{ fontSize: 11, color: colors.textMuted, marginLeft: 4 }}>{field.unit}</span> : null}
                </div>
            </div>
            <div>
                <ProvenanceBadge provenance={field.provenance} availability={field.availability} />
                <InferredBadge basis={field.availability_basis} note={field.availability_basis_note} />
            </div>
            <div style={{ fontSize: 10, color: colors.textMuted, textAlign: 'right' }}>
                <div>pub {fmtDateTime(field.published_at)}</div>
                <div>avail {fmtDateTime(field.available_at)}</div>
                <div>ingest {fmtDateTime(field.ingested_at)}</div>
            </div>
        </div>
    );
}

function CftcPillarCard({ data, error }) {
    const cardStyle = {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: 10,
        padding: 16,
        marginBottom: 16,
    };

    if (error) {
        return (
            <div style={cardStyle} data-testid="cftc-pillar-card" data-state="error">
                <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>CFTC Positioning</div>
                <div style={{ color: colors.red }}>Failed to load: {error}</div>
            </div>
        );
    }

    if (!data) {
        return (
            <div style={cardStyle} data-testid="cftc-pillar-card" data-state="loading">
                <div style={{ color: colors.text, fontWeight: 600 }}>CFTC Positioning</div>
                <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
            </div>
        );
    }

    if (data.available === false) {
        return (
            <div style={cardStyle} data-testid="cftc-pillar-card" data-state="unavailable">
                <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>CFTC Positioning</div>
                <div style={{ color: colors.textMuted, fontSize: 13 }}>UNAVAILABLE</div>
                <div style={{ color: colors.textDim, marginTop: 4 }}>{data.reason || 'no reason given'}</div>
            </div>
        );
    }

    const contracts = data.contracts || {};
    const fieldsByContract = data.fields || {};

    return (
        <div style={cardStyle} data-testid="cftc-pillar-card" data-state="available">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 8 }}>
                <div style={{ color: colors.text, fontWeight: 600, fontSize: 16 }}>CFTC Positioning</div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>as of {data.as_of}</div>
            </div>

            <div style={{ marginBottom: 10 }}>
                <CoverageBar fraction={data.coverage} />
                <div style={{ fontSize: 11, color: colors.textDim, marginTop: 2 }}>
                    {data.contracts_with_data}/{data.contracts_expected} contracts
                    {data.stale_reason ? (
                        <span style={{ color: colors.yellow, marginLeft: 8 }}>stale: {data.stale_reason}</span>
                    ) : null}
                </div>
            </div>

            {Object.keys(contracts).length === 0 ? (
                <div style={{ color: colors.textMuted, fontSize: 13 }}>no qualifying rows as of this date</div>
            ) : (
                Object.entries(contracts).map(([key, code]) => {
                    const fields = fieldsByContract[code];
                    if (!fields) {
                        return (
                            <div key={key} style={{ marginBottom: 12 }}>
                                <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>{key} ({code})</div>
                                <div style={{ color: colors.textMuted, fontSize: 12 }}>no data for this contract as of this date</div>
                            </div>
                        );
                    }
                    return (
                        <div key={key} style={{ marginBottom: 12 }}>
                            <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>{key} ({code})</div>
                            <FieldRow name="Noncommercial net" field={fields.noncommercial_net} />
                            <FieldRow name="Total open interest" field={fields.total_open_interest} />
                            <FieldRow name="Spec net % OI" field={fields.spec_net_pct_oi} />
                            <FieldRow name="Z-score (1y)" field={fields.z_score_1y} />
                            <FieldRow name="Z-score (3y)" field={fields.z_score_3y} />
                            <FieldRow name="Percentile (3y)" field={fields.percentile_3y} />
                        </div>
                    );
                })
            )}

            <div style={{ fontSize: 10, color: colors.textDimAlt, marginTop: 8 }}>
                generation {data.generation_id || '—'} published {fmtDateTime(data.generation_published_at)}
            </div>
        </div>
    );
}

function NotBuiltCard({ label }) {
    return (
        <div
            style={{
                background: colors.card,
                border: `1px dashed ${colors.border}`,
                borderRadius: 10,
                padding: 16,
                marginBottom: 16,
                opacity: 0.7,
            }}
            data-testid="pillar-card-not-built"
        >
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 4 }}>{label}</div>
            <div style={{ color: colors.textMuted, fontSize: 13 }}>not built yet — no data</div>
        </div>
    );
}

export default function GodViewPillars() {
    const [asOf, setAsOf] = useState(() => new Date().toISOString().substring(0, 10));
    const [includeInferred, setIncludeInferred] = useState(false);
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        setError(null);
        const qs = `as_of=${encodeURIComponent(asOf)}&include_inferred=${includeInferred ? 'true' : 'false'}`;
        api.get(`/api/v1/godview/pillars/cftc?${qs}`)
            .then((res) => {
                if (!cancelled) setData(res);
            })
            .catch((e) => {
                if (!cancelled) setError(e.message || 'request failed');
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });
        return () => {
            cancelled = true;
        };
    }, [asOf, includeInferred]);

    return (
        <div style={{ padding: 20, maxWidth: 720 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 16, gap: 12, flexWrap: 'wrap' }}>
                <h2 style={{ color: colors.text, margin: 0 }}>God View — Institutional Pillars</h2>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
                    <label style={{ fontSize: 12, color: colors.textDim }}>
                        <input
                            type="checkbox"
                            checked={includeInferred}
                            onChange={(e) => setIncludeInferred(e.target.checked)}
                            data-testid="include-inferred-toggle"
                            style={{ marginRight: 4 }}
                        />
                        include inferred (backfilled/revised)
                    </label>
                    <label style={{ fontSize: 12, color: colors.textDim }}>
                        as of{' '}
                        <input
                            type="date"
                            value={asOf}
                            onChange={(e) => setAsOf(e.target.value)}
                            style={{ background: colors.card, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: 4, padding: '2px 6px' }}
                        />
                    </label>
                </div>
            </div>

            {loading ? <CftcPillarCard data={null} error={null} /> : <CftcPillarCard data={data} error={error} />}

            {NOT_BUILT_PILLARS.map((p) => (
                <NotBuiltCard key={p.key} label={p.label} />
            ))}
        </div>
    );
}
