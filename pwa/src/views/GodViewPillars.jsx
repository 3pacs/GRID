import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { colors, shared } from '../styles/shared.js';

/**
 * God View pillars.
 *
 * Built: CFTC positioning, Fed net liquidity, commodity warehouses (LME leg
 * only — Cushing is permanently unavailable(never_configured), rendered
 * honestly rather than omitted), FINRA short volume, SEC Reg SHO FTD,
 * corporate buyback blackout windows, and dealer gamma exposure (2026-09-18
 * — all four remaining pillars per operator direction). Any future
 * not-yet-built pillar renders "not built yet" with the SPECIFIC reason it
 * is blocked, never a silent omission or a fabricated value — see
 * docs/reference/GODVIEW_PILLAR_CONTRACT.md.
 */

const NOT_BUILT_PILLARS = [];

function fmtValue(v, digits = 2) {
    if (v == null) return '—';
    if (typeof v === 'boolean') return v ? 'true' : 'false';
    if (typeof v === 'string') return v;
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
                    {field.availability === 'available' ? fmtValue(field.value) : '—'}
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

function UnavailablePanel({ label, payload }) {
    return (
        <div>
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 4 }}>{label}</div>
            <div style={{ color: colors.textMuted, fontSize: 13 }}>UNAVAILABLE</div>
            <div style={{ color: colors.textDim, marginTop: 4, fontSize: 12 }}>{payload?.reason || 'no reason given'}</div>
        </div>
    );
}

function FedLiquidityCard({ data, error }) {
    const cardStyle = {
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 16,
    };

    if (error) {
        return <div style={cardStyle} data-testid="fed-liquidity-card" data-state="error">
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>Fed Net Liquidity</div>
            <div style={{ color: colors.red }}>Failed to load: {error}</div>
        </div>;
    }
    if (!data) {
        return <div style={cardStyle} data-testid="fed-liquidity-card" data-state="loading">
            <div style={{ color: colors.text, fontWeight: 600 }}>Fed Net Liquidity</div>
            <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
        </div>;
    }
    if (data.available === false) {
        return <div style={cardStyle} data-testid="fed-liquidity-card" data-state="unavailable">
            <UnavailablePanel label="Fed Net Liquidity" payload={data} />
        </div>;
    }

    const fields = data.fields || {};
    const hasFields = Object.keys(fields).length > 0;

    return (
        <div style={cardStyle} data-testid="fed-liquidity-card" data-state="available">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 8 }}>
                <div style={{ color: colors.text, fontWeight: 600, fontSize: 16 }}>Fed Net Liquidity</div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>as of {data.as_of}</div>
            </div>
            {data.stale_reason ? (
                <div style={{ color: colors.yellow, fontSize: 11, marginBottom: 8 }}>stale: {data.stale_reason}</div>
            ) : null}
            {!hasFields ? (
                <div style={{ color: colors.textMuted, fontSize: 13 }}>no qualifying observation as of this date</div>
            ) : (
                <>
                    <FieldRow name="Net liquidity (WALCL − TGA − RRP, $M)" field={fields.net_liquidity_usd_m} />
                    <FieldRow name="WALCL ($M)" field={fields.fed_assets_walcl} />
                    <FieldRow name="TGA / WTREGEN ($M)" field={fields.treasury_tga_wtregen} />
                    <FieldRow name="Reverse repo / RRPONTSYD ($B)" field={fields.reverse_repo_rrp} />
                    <FieldRow name="RRP % of peak" field={fields.rrp_as_pct_of_peak} />
                    <FieldRow name="Δ5d ($M)" field={fields.delta_5d_m} />
                    <FieldRow name="Δ30d ($M)" field={fields.delta_30d_m} />
                    <FieldRow name="Liquidity regime" field={fields.liquidity_regime} />
                </>
            )}
            <div style={{ fontSize: 10, color: colors.textDimAlt, marginTop: 8 }}>
                generation {data.generation_id || '—'} published {fmtDateTime(data.generation_published_at)}
            </div>
        </div>
    );
}

function CommodityWarehouseCard({ data, error }) {
    const cardStyle = {
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 16,
    };

    if (error) {
        return <div style={cardStyle} data-testid="commodity-warehouse-card" data-state="error">
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>Commodity Warehouses</div>
            <div style={{ color: colors.red }}>Failed to load: {error}</div>
        </div>;
    }
    if (!data) {
        return <div style={cardStyle} data-testid="commodity-warehouse-card" data-state="loading">
            <div style={{ color: colors.text, fontWeight: 600 }}>Commodity Warehouses</div>
            <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
        </div>;
    }

    const lme = data.lme || {};
    const cushing = data.cushing_crude_stocks || {};
    const lmeFields = lme.fields || {};

    return (
        <div style={cardStyle} data-testid="commodity-warehouse-card" data-state="available">
            <div style={{ color: colors.text, fontWeight: 600, fontSize: 16, marginBottom: 8 }}>Commodity Warehouses</div>

            <div style={{ marginBottom: 10 }} data-testid="lme-section">
                <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>LME cancelled-warrant ratio</div>
                {lme.available === false ? (
                    <UnavailablePanel label="" payload={lme} />
                ) : Object.keys(lmeFields).length === 0 ? (
                    <div style={{ color: colors.textMuted, fontSize: 12 }}>no qualifying rows as of this date</div>
                ) : (
                    Object.entries(lmeFields).map(([metal, fields]) => (
                        <div key={metal} style={{ marginBottom: 8 }}>
                            <div style={{ fontSize: 12, color: colors.textDim, marginBottom: 2, textTransform: 'capitalize' }}>{metal}</div>
                            <FieldRow name="Cancelled ratio" field={fields.canceled_ratio} />
                            <FieldRow name="Total inventory (mt)" field={fields.total_inventory} />
                            <FieldRow name="Physical tightness flag" field={fields.physical_tightness_flag} />
                        </div>
                    ))
                )}
            </div>

            <div data-testid="cushing-section">
                <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>Cushing, OK crude stocks</div>
                <UnavailablePanel label="" payload={cushing} />
            </div>
        </div>
    );
}

function FinraShortVolumeCard({ data, error }) {
    const cardStyle = {
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 16,
    };

    if (error) {
        return <div style={cardStyle} data-testid="finra-short-volume-card" data-state="error">
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>FINRA Short Sale Volume</div>
            <div style={{ color: colors.red }}>Failed to load: {error}</div>
        </div>;
    }
    if (!data) {
        return <div style={cardStyle} data-testid="finra-short-volume-card" data-state="loading">
            <div style={{ color: colors.text, fontWeight: 600 }}>FINRA Short Sale Volume</div>
            <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
        </div>;
    }
    if (data.available === false) {
        return <div style={cardStyle} data-testid="finra-short-volume-card" data-state="unavailable">
            <UnavailablePanel label="FINRA Short Sale Volume" payload={data} />
        </div>;
    }

    const fields = data.fields || {};
    const tickers = Object.keys(fields);

    return (
        <div style={cardStyle} data-testid="finra-short-volume-card" data-state="available">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
                <div style={{ color: colors.text, fontWeight: 600, fontSize: 16 }}>FINRA Short Sale Volume</div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>as of {data.as_of}</div>
            </div>
            <div style={{ fontSize: 11, color: colors.yellow, marginBottom: 10 }} data-testid="not-short-interest-note">
                {data.note}
            </div>
            {tickers.length === 0 ? (
                <div style={{ color: colors.textMuted, fontSize: 13 }}>no qualifying rows as of this date</div>
            ) : (
                tickers.map((ticker) => (
                    <div key={ticker} style={{ marginBottom: 12 }}>
                        <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>{ticker}</div>
                        <FieldRow name="Short ratio" field={fields[ticker].short_ratio} />
                        <FieldRow name="Short ratio (20d avg)" field={fields[ticker].short_ratio_20d_ma} />
                        <FieldRow name="Short volume (shares)" field={fields[ticker].short_volume} />
                        <FieldRow name="Total volume (shares)" field={fields[ticker].total_volume} />
                        <FieldRow name="Spike flag" field={fields[ticker].is_spike} />
                    </div>
                ))
            )}
            <div style={{ fontSize: 10, color: colors.textDimAlt, marginTop: 8 }}>
                generation {data.generation_id || '—'} published {fmtDateTime(data.generation_published_at)}
            </div>
        </div>
    );
}

function SecFtdCard({ data, error }) {
    const cardStyle = {
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 16,
    };

    if (error) {
        return <div style={cardStyle} data-testid="sec-ftd-card" data-state="error">
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>SEC Fails-to-Deliver</div>
            <div style={{ color: colors.red }}>Failed to load: {error}</div>
        </div>;
    }
    if (!data) {
        return <div style={cardStyle} data-testid="sec-ftd-card" data-state="loading">
            <div style={{ color: colors.text, fontWeight: 600 }}>SEC Fails-to-Deliver</div>
            <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
        </div>;
    }
    if (data.available === false) {
        return <div style={cardStyle} data-testid="sec-ftd-card" data-state="unavailable">
            <UnavailablePanel label="SEC Fails-to-Deliver" payload={data} />
        </div>;
    }

    const fields = data.fields || {};
    const cusips = Object.keys(fields);

    return (
        <div style={cardStyle} data-testid="sec-ftd-card" data-state="available">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
                <div style={{ color: colors.text, fontWeight: 600, fontSize: 16 }}>SEC Fails-to-Deliver</div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>as of {data.as_of}</div>
            </div>
            <div style={{ fontSize: 11, color: colors.yellow, marginBottom: 10 }} data-testid="not-a-timeline-note">
                {data.note}
            </div>
            {cusips.length === 0 ? (
                <div style={{ color: colors.textMuted, fontSize: 13 }}>no qualifying rows as of this date</div>
            ) : (
                cusips.map((cusip) => (
                    <div key={cusip} style={{ marginBottom: 12 }}>
                        <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>{cusip}</div>
                        <FieldRow name="Outstanding balance (shares)" field={fields[cusip].failed_shares} />
                        <FieldRow name="Closing price" field={fields[cusip].closing_price} />
                        <FieldRow name="Balance value ($)" field={fields[cusip].total_failed_usd} />
                        <FieldRow name="Observation age (days)" field={fields[cusip].observation_age_days} />
                    </div>
                ))
            )}
            <div style={{ fontSize: 10, color: colors.textDimAlt, marginTop: 8 }}>
                generation {data.generation_id || '—'} published {fmtDateTime(data.generation_published_at)}
            </div>
        </div>
    );
}

function BuybackBlackoutCard({ data, error }) {
    const cardStyle = {
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 16,
    };

    if (error) {
        return <div style={cardStyle} data-testid="buyback-card" data-state="error">
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>Corporate Buyback Blackouts</div>
            <div style={{ color: colors.red }}>Failed to load: {error}</div>
        </div>;
    }
    if (!data) {
        return <div style={cardStyle} data-testid="buyback-card" data-state="loading">
            <div style={{ color: colors.text, fontWeight: 600 }}>Corporate Buyback Blackouts</div>
            <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
        </div>;
    }
    if (data.available === false) {
        return <div style={cardStyle} data-testid="buyback-card" data-state="unavailable">
            <UnavailablePanel label="Corporate Buyback Blackouts" payload={data} />
        </div>;
    }

    const issuers = data.issuers || {};
    const tickers = Object.keys(issuers);

    return (
        <div style={cardStyle} data-testid="buyback-card" data-state="available">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
                <div style={{ color: colors.text, fontWeight: 600, fontSize: 16 }}>Corporate Buyback Blackouts</div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>as of {data.as_of}</div>
            </div>
            <div style={{ fontSize: 11, color: colors.yellow, marginBottom: 4 }} data-testid="modeling-assumption-note">
                {data.note}
            </div>
            <div style={{ fontSize: 11, color: colors.textMuted, marginBottom: 10 }} data-testid="missing-input-note">
                {data.missing_input}
            </div>
            {tickers.length === 0 ? (
                <div style={{ color: colors.textMuted, fontSize: 13 }}>no issuer is in a modeled quiet window as of this date</div>
            ) : (
                tickers.map((ticker) => {
                    const field = issuers[ticker];
                    return (
                        <div key={ticker} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', borderBottom: `1px solid ${colors.borderSubtle}` }}>
                            <span style={{ color: colors.text }}>{ticker}</span>
                            <span style={{ color: colors.yellow, fontSize: 12 }}>{fmtValue(field.value)}</span>
                            <ProvenanceBadge provenance={field.provenance} availability={field.availability} />
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

function DealerGexCard({ data, error }) {
    const cardStyle = {
        background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 16,
    };

    if (error) {
        return <div style={cardStyle} data-testid="dealer-gex-card" data-state="error">
            <div style={{ color: colors.text, fontWeight: 600, marginBottom: 8 }}>Dealer Gamma Exposure</div>
            <div style={{ color: colors.red }}>Failed to load: {error}</div>
        </div>;
    }
    if (!data) {
        return <div style={cardStyle} data-testid="dealer-gex-card" data-state="loading">
            <div style={{ color: colors.text, fontWeight: 600 }}>Dealer Gamma Exposure</div>
            <div style={{ color: colors.textDim, marginTop: 8 }}>Loading…</div>
        </div>;
    }
    if (data.available === false) {
        return <div style={cardStyle} data-testid="dealer-gex-card" data-state="unavailable">
            <UnavailablePanel label="Dealer Gamma Exposure" payload={data} />
        </div>;
    }

    const fields = data.fields || {};
    const tickers = Object.keys(fields);

    return (
        <div style={cardStyle} data-testid="dealer-gex-card" data-state="available">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
                <div style={{ color: colors.text, fontWeight: 600, fontSize: 16 }}>Dealer Gamma Exposure</div>
                <div style={{ fontSize: 11, color: colors.textMuted }}>as of {data.as_of}</div>
            </div>
            <div style={{ fontSize: 11, color: colors.yellow, marginBottom: 4 }} data-testid="sign-convention-note">
                {data.sign_convention_note}
            </div>
            <div style={{ fontSize: 11, color: colors.textMuted, marginBottom: 10 }} data-testid="gex-missing-input-note">
                {data.missing_input}
            </div>
            {tickers.length === 0 ? (
                <div style={{ color: colors.textMuted, fontSize: 13 }}>no qualifying rows as of this date</div>
            ) : (
                tickers.map((ticker) => (
                    <div key={ticker} style={{ marginBottom: 12 }}>
                        <div style={{ fontSize: 13, color: colors.textDim, marginBottom: 4 }}>{ticker}</div>
                        <FieldRow name="Spot price" field={fields[ticker].spot_price} />
                        <FieldRow name="Net GEX ($M / 1% move)" field={fields[ticker].net_gex_usd_m} />
                        <FieldRow name="Gamma flip strike" field={fields[ticker].gamma_flip_strike} />
                        <FieldRow name="Spot to flip (%)" field={fields[ticker].spot_to_flip_pct} />
                        <FieldRow name="Regime" field={fields[ticker].gex_regime} />
                        <FieldRow name="Max pain strike" field={fields[ticker].max_pain_strike} />
                        <FieldRow name="Put/call OI ratio" field={fields[ticker].put_call_oi_ratio} />
                        <FieldRow name="ATM IV" field={fields[ticker].atm_iv} />
                    </div>
                ))
            )}
            <div style={{ fontSize: 10, color: colors.textDimAlt, marginTop: 8 }}>
                generation {data.generation_id || '—'} published {fmtDateTime(data.generation_published_at)}
            </div>
        </div>
    );
}

function NotBuiltCard({ label, reason }) {
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
            <div style={{ color: colors.textMuted, fontSize: 13 }}>not built yet — {reason || 'no data'}</div>
        </div>
    );
}

export default function GodViewPillars() {
    const [asOf, setAsOf] = useState(() => new Date().toISOString().substring(0, 10));
    const [includeInferred, setIncludeInferred] = useState(false);
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(true);
    const [fedData, setFedData] = useState(null);
    const [fedError, setFedError] = useState(null);
    const [fedLoading, setFedLoading] = useState(true);
    const [cmdtyData, setCmdtyData] = useState(null);
    const [cmdtyError, setCmdtyError] = useState(null);
    const [cmdtyLoading, setCmdtyLoading] = useState(true);
    const [finraData, setFinraData] = useState(null);
    const [finraError, setFinraError] = useState(null);
    const [finraLoading, setFinraLoading] = useState(true);
    const [ftdData, setFtdData] = useState(null);
    const [ftdError, setFtdError] = useState(null);
    const [ftdLoading, setFtdLoading] = useState(true);
    const [buybackData, setBuybackData] = useState(null);
    const [buybackError, setBuybackError] = useState(null);
    const [buybackLoading, setBuybackLoading] = useState(true);
    const [gexData, setGexData] = useState(null);
    const [gexError, setGexError] = useState(null);
    const [gexLoading, setGexLoading] = useState(true);

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

    useEffect(() => {
        let cancelled = false;
        setFedLoading(true);
        setFedError(null);
        const qs = `as_of=${encodeURIComponent(asOf)}&include_inferred=${includeInferred ? 'true' : 'false'}`;
        api.get(`/api/v1/godview/pillars/fed_net_liquidity?${qs}`)
            .then((res) => { if (!cancelled) setFedData(res); })
            .catch((e) => { if (!cancelled) setFedError(e.message || 'request failed'); })
            .finally(() => { if (!cancelled) setFedLoading(false); });
        return () => { cancelled = true; };
    }, [asOf, includeInferred]);

    useEffect(() => {
        let cancelled = false;
        setCmdtyLoading(true);
        setCmdtyError(null);
        api.get(`/api/v1/godview/pillars/commodity_warehouses?as_of=${encodeURIComponent(asOf)}`)
            .then((res) => { if (!cancelled) setCmdtyData(res); })
            .catch((e) => { if (!cancelled) setCmdtyError(e.message || 'request failed'); })
            .finally(() => { if (!cancelled) setCmdtyLoading(false); });
        return () => { cancelled = true; };
    }, [asOf]);

    useEffect(() => {
        let cancelled = false;
        setFinraLoading(true);
        setFinraError(null);
        const qs = `as_of=${encodeURIComponent(asOf)}&include_inferred=${includeInferred ? 'true' : 'false'}`;
        api.get(`/api/v1/godview/pillars/finra_short_volume?${qs}`)
            .then((res) => { if (!cancelled) setFinraData(res); })
            .catch((e) => { if (!cancelled) setFinraError(e.message || 'request failed'); })
            .finally(() => { if (!cancelled) setFinraLoading(false); });
        return () => { cancelled = true; };
    }, [asOf, includeInferred]);

    useEffect(() => {
        let cancelled = false;
        setFtdLoading(true);
        setFtdError(null);
        const qs = `as_of=${encodeURIComponent(asOf)}&include_inferred=${includeInferred ? 'true' : 'false'}`;
        api.get(`/api/v1/godview/pillars/sec_regsho_ftd?${qs}`)
            .then((res) => { if (!cancelled) setFtdData(res); })
            .catch((e) => { if (!cancelled) setFtdError(e.message || 'request failed'); })
            .finally(() => { if (!cancelled) setFtdLoading(false); });
        return () => { cancelled = true; };
    }, [asOf, includeInferred]);

    useEffect(() => {
        let cancelled = false;
        setBuybackLoading(true);
        setBuybackError(null);
        api.get(`/api/v1/godview/pillars/buyback_blackouts?as_of=${encodeURIComponent(asOf)}`)
            .then((res) => { if (!cancelled) setBuybackData(res); })
            .catch((e) => { if (!cancelled) setBuybackError(e.message || 'request failed'); })
            .finally(() => { if (!cancelled) setBuybackLoading(false); });
        return () => { cancelled = true; };
    }, [asOf]);

    useEffect(() => {
        let cancelled = false;
        setGexLoading(true);
        setGexError(null);
        api.get(`/api/v1/godview/pillars/dealer_gex?as_of=${encodeURIComponent(asOf)}`)
            .then((res) => { if (!cancelled) setGexData(res); })
            .catch((e) => { if (!cancelled) setGexError(e.message || 'request failed'); })
            .finally(() => { if (!cancelled) setGexLoading(false); });
        return () => { cancelled = true; };
    }, [asOf]);

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
            {fedLoading ? <FedLiquidityCard data={null} error={null} /> : <FedLiquidityCard data={fedData} error={fedError} />}
            {cmdtyLoading ? <CommodityWarehouseCard data={null} error={null} /> : <CommodityWarehouseCard data={cmdtyData} error={cmdtyError} />}
            {finraLoading ? <FinraShortVolumeCard data={null} error={null} /> : <FinraShortVolumeCard data={finraData} error={finraError} />}
            {ftdLoading ? <SecFtdCard data={null} error={null} /> : <SecFtdCard data={ftdData} error={ftdError} />}
            {buybackLoading ? <BuybackBlackoutCard data={null} error={null} /> : <BuybackBlackoutCard data={buybackData} error={buybackError} />}
            {gexLoading ? <DealerGexCard data={null} error={null} /> : <DealerGexCard data={gexData} error={gexError} />}

            {NOT_BUILT_PILLARS.map((p) => (
                <NotBuiltCard key={p.key} label={p.label} reason={p.reason} />
            ))}
        </div>
    );
}
