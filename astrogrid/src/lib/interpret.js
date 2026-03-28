/**
 * AstroGrid interpretation helpers.
 *
 * Frontend-oriented utilities that turn raw celestial payloads into
 * display-ready summaries for the SPA.
 */

const DISPLAY_LABELS = {
    lunar: 'Lunar',
    planetary: 'Planetary',
    solar: 'Solar',
    vedic: 'Vedic',
    chinese: 'Chinese',
};

function toNumber(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

export function normalizeCelestialCategories(payload) {
    const categories = payload?.categories ?? {};
    return {
        lunar: Array.isArray(categories.lunar) ? categories.lunar : [],
        planetary: Array.isArray(categories.planetary) ? categories.planetary : [],
        solar: Array.isArray(categories.solar) ? categories.solar : [],
        vedic: Array.isArray(categories.vedic) ? categories.vedic : [],
        chinese: Array.isArray(categories.chinese) ? categories.chinese : [],
    };
}

export function summarizeCategories(payload) {
    const categories = normalizeCelestialCategories(payload);
    return Object.entries(categories).map(([key, items]) => ({
        key,
        label: DISPLAY_LABELS[key] || key,
        count: items.length,
    }));
}

export function getCategoryHighlights(payload) {
    const categories = normalizeCelestialCategories(payload);
    const highlights = [];

    for (const [key, items] of Object.entries(categories)) {
        const candidate = items.find((item) => item?.value != null) || items[0];
        if (!candidate) continue;

        const value = toNumber(candidate.value);
        highlights.push({
            category: key,
            label: candidate.description || candidate.name || DISPLAY_LABELS[key] || key,
            valueLabel: value != null ? value.toFixed(2) : 'Live',
            feature: candidate.name || candidate.description || key,
        });
    }

    return highlights.slice(0, 5);
}

export function describeAspectTone(aspects) {
    if (!Array.isArray(aspects) || aspects.length === 0) {
        return 'No major aspects detected';
    }

    const challenging = aspects.filter((aspect) =>
        aspect.aspect_type === 'square' || aspect.aspect_type === 'opposition'
    ).length;
    const harmonious = aspects.filter((aspect) =>
        aspect.aspect_type === 'trine' || aspect.aspect_type === 'sextile'
    ).length;

    if (challenging > harmonious) return 'Tense geometry';
    if (harmonious > challenging) return 'Supportive geometry';
    return 'Mixed geometry';
}

export function buildRetrogradeSummary(positions) {
    const retrogrades = Object.values(positions || {}).filter((body) => body?.is_retrograde);
    if (retrogrades.length === 0) {
        return 'All tracked bodies direct';
    }

    if (retrogrades.length === 1) {
        return `${retrogrades[0].planet} retrograde`;
    }

    return `${retrogrades.length} bodies retrograde`;
}
