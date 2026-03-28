import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import SignalCard from '../components/SignalCard.jsx';
import ViewHelp from '../components/ViewHelp.jsx';
import { Moon, Sun, Star, Globe, Sparkles } from 'lucide-react';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)', maxWidth: '900px', margin: '0 auto' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    grid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
        gap: '10px',
    },
    empty: {
        color: '#5A7080', textAlign: 'center', padding: '40px',
        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '14px',
    },
};

// Lunar phase display helpers
const LUNAR_PHASES = [
    { name: 'New Moon', icon: Moon, range: [0, 0.0625] },
    { name: 'Waxing Crescent', icon: Moon, range: [0.0625, 0.1875] },
    { name: 'First Quarter', icon: Moon, range: [0.1875, 0.3125] },
    { name: 'Waxing Gibbous', icon: Moon, range: [0.3125, 0.4375] },
    { name: 'Full Moon', icon: Moon, range: [0.4375, 0.5625] },
    { name: 'Waning Gibbous', icon: Moon, range: [0.5625, 0.6875] },
    { name: 'Last Quarter', icon: Moon, range: [0.6875, 0.8125] },
    { name: 'Waning Crescent', icon: Moon, range: [0.8125, 1.0] },
];

function getLunarPhaseName(value) {
    if (value == null) return 'Unknown';
    const v = value % 1;
    for (const phase of LUNAR_PHASES) {
        if (v >= phase.range[0] && v < phase.range[1]) return phase.name;
    }
    return LUNAR_PHASES[0].name;
}

function getSolarActivityLevel(kpIndex) {
    if (kpIndex == null) return { label: 'Unknown', color: colors.textMuted };
    if (kpIndex < 2) return { label: 'Quiet', color: colors.green };
    if (kpIndex < 4) return { label: 'Unsettled', color: colors.yellow };
    if (kpIndex < 6) return { label: 'Active', color: '#FF8C00' };
    return { label: 'Storm', color: colors.red };
}

const CATEGORY_META = {
    lunar: { label: 'Lunar Phase', Icon: Moon, color: '#8AA0B8' },
    solar: { label: 'Solar Activity', Icon: Sun, color: '#F59E0B' },
    vedic: { label: 'Vedic Nakshatra', Icon: Star, color: '#A78BFA' },
    planetary: { label: 'Planetary Aspects', Icon: Globe, color: '#34D399' },
    chinese: { label: 'Chinese Zodiac', Icon: Sparkles, color: '#F87171' },
    celestial: { label: 'Celestial', Icon: Star, color: colors.accent },
};

function CelestialCategory({ category, features }) {
    const meta = CATEGORY_META[category] || CATEGORY_META.celestial;
    const { Icon } = meta;

    return (
        <div style={shared.card}>
            <div style={{
                display: 'flex', alignItems: 'center', gap: '10px',
                marginBottom: '12px',
            }}>
                <Icon size={18} color={meta.color} />
                <span style={{
                    fontSize: '14px', fontWeight: 600, color: '#E8F0F8',
                    fontFamily: colors.sans,
                }}>
                    {meta.label}
                </span>
            </div>
            {features.length === 0 ? (
                <div style={{
                    color: colors.textMuted, fontSize: '12px',
                    fontStyle: 'italic', padding: '8px 0',
                }}>
                    No data available -- awaiting ingestion module
                </div>
            ) : (
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
                    gap: '8px',
                }}>
                    {features.map((f, i) => (
                        <CelestialFeatureCard key={i} feature={f} category={category} />
                    ))}
                </div>
            )}
        </div>
    );
}

function CelestialFeatureCard({ feature, category }) {
    let displayValue = feature.value != null ? feature.value.toFixed(2) : '--';
    let subLabel = feature.obs_date || '';
    let valueColor = colors.text;

    // Special formatting per category
    if (category === 'lunar') {
        const phaseName = getLunarPhaseName(feature.value);
        displayValue = phaseName;
        subLabel = feature.value != null ? `Phase: ${feature.value.toFixed(3)}` : '';
    } else if (category === 'solar') {
        const activity = getSolarActivityLevel(feature.value);
        subLabel = activity.label;
        valueColor = activity.color;
    }

    return (
        <div style={{
            background: colors.bg, borderRadius: '10px', padding: '12px 14px',
            border: `1px solid ${colors.border}`,
        }}>
            <div title={feature.name.replace(/_/g, ' ')} style={{
                fontSize: '11px', color: colors.textMuted, marginBottom: '6px',
                fontFamily: colors.sans,
                whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                lineHeight: '1.3',
            }}>
                {feature.name.replace(/_/g, ' ')}
            </div>
            <div style={{
                fontFamily: colors.mono,
                fontSize: category === 'lunar' ? '13px' : '16px',
                fontWeight: 500, color: valueColor,
            }}>
                {displayValue}
            </div>
            {subLabel && (
                <div style={{
                    fontSize: '10px', color: colors.textMuted, marginTop: '4px',
                    fontFamily: colors.mono,
                }}>
                    {subLabel}
                </div>
            )}
        </div>
    );
}

function CelestialPanel() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        setLoading(true);
        api.getCelestialSignals()
            .then(d => { setData(d); setError(null); })
            .catch(e => { setError(e.message); setData(null); })
            .finally(() => setLoading(false));
    }, []);

    if (loading) {
        return (
            <div style={styles.empty}>Loading celestial data...</div>
        );
    }

    if (error) {
        return (
            <div style={{ ...shared.card, ...shared.error }}>
                Failed to load celestial signals: {error}
            </div>
        );
    }

    const categories = data?.categories || {};
    const hasData = data?.count > 0;

    // Always show all category sections, even if empty
    const allCategories = ['lunar', 'solar', 'vedic', 'planetary', 'chinese'];

    return (
        <div>
            {/* Summary bar */}
            <div style={shared.card}>
                <div style={shared.row}>
                    <div>
                        <span style={shared.label}>Celestial Features</span>
                        <span style={shared.value}>{data?.count || 0} active</span>
                    </div>
                    <span style={{
                        fontSize: '11px', color: colors.textMuted,
                        fontFamily: colors.mono,
                    }}>
                        as of {data?.as_of || '--'}
                    </span>
                </div>
            </div>

            {!hasData && (
                <div style={{
                    ...shared.card, textAlign: 'center',
                    color: colors.textMuted, padding: '32px', fontSize: '13px',
                }}>
                    No celestial features registered yet.
                    Add ingestion modules for lunar, solar, vedic, planetary,
                    or chinese zodiac data to populate this view.
                </div>
            )}

            {allCategories.map(cat => (
                <CelestialCategory
                    key={cat}
                    category={cat}
                    features={categories[cat] || []}
                />
            ))}
        </div>
    );
}

function SnapshotPanel() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        api.getSignalSnapshot()
            .then(d => setData(d))
            .catch(() => {})
            .finally(() => setLoading(false));
    }, []);

    if (loading) return <div style={styles.empty}>Loading snapshot...</div>;
    if (!data?.features?.length) return <div style={styles.empty}>No features available</div>;

    // Group by family
    const byFamily = {};
    data.features.forEach(f => {
        const fam = f.family || 'other';
        if (!byFamily[fam]) byFamily[fam] = [];
        byFamily[fam].push(f);
    });

    // Sort families by number of extreme signals
    const familyOrder = Object.entries(byFamily)
        .map(([fam, feats]) => ({
            family: fam,
            features: feats.sort((a, b) => Math.abs(b.z_score || 0) - Math.abs(a.z_score || 0)),
            extreme: feats.filter(f => Math.abs(f.z_score || 0) > 2).length,
            avgZ: feats.reduce((s, f) => s + Math.abs(f.z_score || 0), 0) / feats.length,
        }))
        .sort((a, b) => b.extreme - a.extreme || b.avgZ - a.avgZ);

    const familyColors = {
        rates: '#3B82F6', credit: '#EF4444', equity: '#22C55E', vol: '#F59E0B',
        macro: '#8B5CF6', commodity: '#F97316', crypto: '#06B6D4', sentiment: '#EC4899',
        fx: '#14B8A6', breadth: '#6366F1', earnings: '#A855F7', alternative: '#84CC16',
    };

    return (
        <div>
            {familyOrder.map(({ family, features, extreme }) => {
                const fc = familyColors[family] || colors.textMuted;
                // Generate family summary
                const bullish = features.filter(f => (f.z_score || 0) > 1).length;
                const bearish = features.filter(f => (f.z_score || 0) < -1).length;
                let summary = '';
                if (extreme > 0) summary = `${extreme} extreme signal${extreme > 1 ? 's' : ''} — `;
                if (bullish > bearish * 2) summary += 'Mostly elevated readings, risk-on tilt';
                else if (bearish > bullish * 2) summary += 'Mostly depressed readings, risk-off tilt';
                else if (bullish > 0 || bearish > 0) summary += 'Mixed signals across indicators';
                else summary += 'All readings near normal';

                return (
                    <div key={family} style={{ marginBottom: '12px' }}>
                        <div style={{
                            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                            padding: '8px 0', borderBottom: `2px solid ${fc}`,
                            marginBottom: '6px',
                        }}>
                            <div>
                                <span style={{ fontSize: '12px', fontWeight: 700, color: fc, fontFamily: "'JetBrains Mono', monospace", textTransform: 'uppercase', letterSpacing: '1px' }}>
                                    {family}
                                </span>
                                <span style={{ fontSize: '10px', color: colors.textMuted, marginLeft: '8px' }}>
                                    {features.length} signals
                                </span>
                            </div>
                            {extreme > 0 && (
                                <span style={{ fontSize: '9px', padding: '2px 6px', borderRadius: '3px', background: '#EF444420', color: '#EF4444', fontWeight: 600 }}>
                                    {extreme} EXTREME
                                </span>
                            )}
                        </div>
                        {/* Family summary */}
                        <div style={{ fontSize: '11px', color: colors.textDim, marginBottom: '8px', lineHeight: '1.4' }}>
                            {summary}
                        </div>
                        <div style={styles.grid}>
                            {features.slice(0, 12).map((f, i) => (
                                <SignalCard
                                    key={`${f.name}-${i}`}
                                    name={f.name}
                                    value={f.value}
                                    z_score={f.z_score}
                                    family={family}
                                    direction={f.z_score > 0.5 ? 'up' : f.z_score < -0.5 ? 'down' : undefined}
                                />
                            ))}
                        </div>
                    </div>
                );
            })}
        </div>
    );
}

export default function Signals() {
    const [signals, setSignals] = useState(null);
    const [activeTab, setActiveTab] = useState('snapshot');

    useEffect(() => {
        api.getCurrent().then(setSignals).catch(() => {});
    }, []);

    return (
        <div style={styles.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={styles.title}>SIGNALS</div>
                <ViewHelp id="signals" />
            </div>

            {/* Tab bar */}
            <div style={shared.tabs}>
                <button style={shared.tab(activeTab === 'snapshot')} onClick={() => setActiveTab('snapshot')}>Snapshot</button>
                <button style={shared.tab(activeTab === 'live')} onClick={() => setActiveTab('live')}>Drivers</button>
                <button style={shared.tab(activeTab === 'celestial')} onClick={() => setActiveTab('celestial')}>Celestial</button>
            </div>

            {activeTab === 'snapshot' && <SnapshotPanel />}

            {activeTab === 'live' && (
                <>
                    {signals?.top_drivers?.length > 0 ? (
                        <div style={styles.grid}>
                            {signals.top_drivers.map((d, i) => (
                                <SignalCard
                                    key={i}
                                    name={d.feature}
                                    value={d.magnitude}
                                    direction={d.direction}
                                    magnitude={d.magnitude}
                                />
                            ))}
                        </div>
                    ) : (
                        <div style={styles.empty}>
                            {signals?.state === 'UNCALIBRATED'
                                ? 'No production model -- signals unavailable'
                                : 'No active signals'}
                        </div>
                    )}
                </>
            )}

            {activeTab === 'celestial' && <CelestialPanel />}
        </div>
    );
}
