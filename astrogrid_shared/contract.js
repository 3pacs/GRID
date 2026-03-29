const REMOTE_ASTROGRID_API_BASE = 'https://grid.stepdad.finance';

function isObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function toArray(value) {
    if (Array.isArray(value)) return value;
    if (value == null) return [];
    return [value];
}

function asNumber(value, fallback = 0) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : fallback;
    }
    return fallback;
}

export function getAstrogridDefaultApiBaseUrl(locationLike) {
    const hostname = locationLike?.hostname || '';
    const origin = locationLike?.origin || '';

    if (hostname === 'localhost' || hostname === '127.0.0.1') {
        return REMOTE_ASTROGRID_API_BASE;
    }

    return origin || REMOTE_ASTROGRID_API_BASE;
}

export function normalizeAstrogridSignalMap(payload) {
    if (!isObject(payload)) return {};
    if (!isObject(payload.categories)) return payload;

    const flattened = {};
    for (const [category, values] of Object.entries(payload.categories)) {
        if (!isObject(values)) continue;
        for (const [key, value] of Object.entries(values)) {
            flattened[`${category}_${key}`] = typeof value === 'boolean' ? Number(value) : value;
        }
    }
    return flattened;
}

export function normalizeAstrogridTimeline(payload) {
    if (Array.isArray(payload)) return payload;
    return toArray(payload?.events || payload?.items || []);
}

export function normalizeAstrogridCorrelations(payload) {
    if (Array.isArray(payload)) return payload;
    return toArray(payload?.correlations);
}

export function buildAstrogridCorrelationMatrix(payload) {
    const rows = normalizeAstrogridCorrelations(payload);
    if (!rows.length) return null;

    const rowLabels = [...new Set(rows.map((item) => item.celestial_feature || item.feature || item.name).filter(Boolean))];
    const columnLabels = [...new Set(rows.map((item) => item.market_feature || item.market || item.ticker).filter(Boolean))];
    if (!rowLabels.length || !columnLabels.length) return null;

    return {
        rows: rowLabels,
        columns: columnLabels,
        matrix: rowLabels.map((row) => columnLabels.map((column) => {
            const match = rows.find((item) =>
                (item.celestial_feature || item.feature || item.name) === row
                && (item.market_feature || item.market || item.ticker) === column
            );
            return asNumber(match?.correlation ?? match?.value, 0);
        })),
    };
}

export function normalizeAstrogridBriefing(payload, fallback = '') {
    return {
        briefing: payload?.briefing || payload?.text || payload?.content || fallback,
        generatedAt: payload?.generated_at || payload?.created_at || payload?.briefing_date || null,
        source: payload?.source || null,
        raw: payload || null,
    };
}

export function normalizeAstrogridEphemeris(payload) {
    if (!isObject(payload)) return null;

    const root = payload.ephemeris || payload;
    const rawPlanets = Array.isArray(root.planets)
        ? root.planets
        : Array.isArray(root.positions)
            ? root.positions
            : Array.isArray(root.bodies)
                ? root.bodies
                : isObject(root.positions)
                    ? Object.values(root.positions)
                    : [];
    const rawAspects = Array.isArray(root.aspects) ? root.aspects : [];

    const planets = rawPlanets
        .map((planet, index) => ({
            planet: planet.planet || planet.name || planet.body || `Body ${index + 1}`,
            geocentric_longitude: asNumber(
                planet.geocentric_longitude
                ?? planet.longitude
                ?? planet.lon
                ?? planet.position,
                NaN,
            ),
            right_ascension: asNumber(planet.right_ascension ?? planet.ra ?? planet.ascension, 0),
            zodiac_sign: planet.zodiac_sign || planet.sign || 'Unknown',
            zodiac_degree: asNumber(planet.zodiac_degree ?? planet.degree ?? planet.sign_degree, 0),
            is_retrograde: Boolean(
                planet.is_retrograde
                ?? planet.retrograde
                ?? planet.rx,
            ),
        }))
        .filter((planet) => Number.isFinite(planet.geocentric_longitude));

    const aspects = rawAspects
        .map((aspect, index) => ({
            planet1: aspect.planet1 || aspect.from || aspect.body1 || `Body ${index + 1}`,
            planet2: aspect.planet2 || aspect.to || aspect.body2 || 'Body',
            aspect_type: aspect.aspect_type || aspect.type || 'aspect',
            nature: aspect.nature || aspect.tone || 'variable',
            applying: Boolean(aspect.applying ?? false),
            orb_used: asNumber(aspect.orb_used ?? aspect.orb ?? aspect.distance, 0),
        }))
        .filter((aspect) => aspect.planet1 && aspect.planet2);

    if (!planets.length && !aspects.length) {
        return null;
    }

    return {
        date: root.date || payload.date || null,
        planets,
        aspects,
    };
}

function asString(value, fallback = '') {
    return typeof value === 'string' && value.trim() !== '' ? value : fallback;
}

function normalizeConfidence(value) {
    const numeric = asNumber(value, null);
    if (numeric == null) return 0;
    return numeric > 1 ? Math.max(0, Math.min(1, numeric / 100)) : Math.max(0, Math.min(1, numeric));
}

function normalizeSignedValue(value) {
    return Math.round(asNumber(value, 0) * 100) / 100;
}

function normalizeTopicList(value) {
    return toArray(value)
        .map((item) => {
            if (typeof item === 'string') {
                return { label: item, detail: item };
            }
            if (!isObject(item)) return null;
            return {
                label: item.label || item.name || item.driver || item.thesis || item.factor || item.title || 'item',
                detail: item.detail || item.description || item.summary || item.narrative || '',
                raw: item,
            };
        })
        .filter(Boolean);
}

export function normalizeAstrogridRegime(payload) {
    if (!isObject(payload)) return null;
    return {
        state: asString(payload.state, 'UNCALIBRATED'),
        confidence: normalizeConfidence(payload.confidence),
        transitionProbability: normalizeConfidence(payload.transition_probability),
        drivers: normalizeTopicList(payload.top_drivers || payload.drivers),
        contradictionFlags: toArray(payload.contradiction_flags || payload.red_flags).map((item) => String(item)),
        posture: asString(payload.posture || payload.baseline_comparison || payload.grid_recommendation),
        baselineComparison: asString(payload.baseline_comparison),
        modelVersion: asString(payload.model_version),
        asOf: payload.as_of || payload.timestamp || payload.generated_at || null,
        raw: payload,
    };
}

export function normalizeAstrogridThesis(payload) {
    if (!isObject(payload)) return null;
    const theses = toArray(payload.theses).map((item, index) => ({
        id: item?.id || item?.name || item?.thesis || `thesis_${index + 1}`,
        label: item?.label || item?.name || item?.thesis || item?.title || `Thesis ${index + 1}`,
        direction: asString(item?.direction || item?.stance || payload.overall_direction, 'NEUTRAL'),
        conviction: normalizeConfidence(item?.conviction ?? item?.confidence ?? payload.conviction),
        detail: item?.detail || item?.summary || item?.narrative || item?.description || '',
        raw: item,
    }));
    return {
        overallDirection: asString(payload.overall_direction, 'NEUTRAL'),
        conviction: normalizeConfidence(payload.conviction),
        bullishScore: asNumber(payload.bullish_score, 0),
        bearishScore: asNumber(payload.bearish_score, 0),
        activeTheses: asNumber(payload.active_theses, theses.length),
        keyDrivers: normalizeTopicList(payload.key_drivers),
        riskFactors: normalizeTopicList(payload.risk_factors),
        agreements: normalizeTopicList(payload.agreements),
        contradictions: normalizeTopicList(payload.contradictions),
        narrative: asString(payload.narrative),
        generatedAt: payload.generated_at || null,
        theses,
        raw: payload,
    };
}

function normalizeMoneyMapFlow(flow, index) {
    if (!isObject(flow)) return null;
    const volume = normalizeSignedValue(flow.volume ?? flow.value ?? flow.amount_usd ?? flow.net_flow);
    const direction = asString(
        flow.direction,
        volume > 0 ? 'inflow' : volume < 0 ? 'outflow' : 'neutral',
    );
    return {
        id: flow.id || `flow_${index + 1}`,
        from: asString(flow.from || flow.source),
        to: asString(flow.to || flow.target),
        label: asString(flow.label || flow.description || `${flow.from || flow.source || 'source'} → ${flow.to || flow.target || 'target'}`),
        direction,
        volume,
        currency: flow.currency || 'USD',
        confidence: normalizeConfidence(flow.confidence),
        raw: flow,
    };
}

export function normalizeAstrogridMoneyMap(payload) {
    if (!isObject(payload)) return null;
    const layers = toArray(payload.layers).map((layer, index) => ({
        id: layer?.id || layer?.name || `layer_${index + 1}`,
        label: layer?.label || layer?.name || `Layer ${index + 1}`,
        nodes: toArray(layer?.nodes),
        globalLiquidity: isObject(layer?.global_liquidity) ? layer.global_liquidity : null,
        globalPolicy: isObject(layer?.global_policy) ? layer.global_policy : null,
        raw: layer,
    }));
    const flows = toArray(payload.flows).map(normalizeMoneyMapFlow).filter(Boolean);
    const levers = toArray(payload.levers).map((lever, index) => ({
        id: lever?.id || `lever_${index + 1}`,
        label: lever?.label || lever?.title || lever?.name || `Lever ${index + 1}`,
        impactScore: normalizeConfidence(lever?.impact_score ?? lever?.score ?? lever?.confidence),
        detail: lever?.detail || lever?.summary || lever?.description || '',
        raw: lever,
    }));
    return {
        asOf: payload.as_of || payload.timestamp || null,
        layers,
        flows,
        intelligence: isObject(payload.intelligence) ? payload.intelligence : {},
        levers,
        globalLiquidity: isObject(payload.global_liquidity) ? payload.global_liquidity : {},
        globalPolicy: isObject(payload.global_policy) ? payload.global_policy : {},
        currencyImpacts: toArray(payload.currency_impacts),
        raw: payload,
    };
}

export function normalizeAstrogridAggregatedFlows(payload) {
    if (!isObject(payload)) return null;
    const bySector = Object.entries(isObject(payload.by_sector) ? payload.by_sector : {}).map(([sector, value]) => {
        const record = isObject(value) ? value : { net_flow: value };
        return {
            sector,
            netFlow: normalizeSignedValue(record.net_flow),
            inflow: normalizeSignedValue(record.inflow),
            outflow: normalizeSignedValue(record.outflow),
            direction: asString(record.direction, normalizeSignedValue(record.net_flow) >= 0 ? 'inflow' : 'outflow'),
            acceleration: asString(record.acceleration),
            topActors: toArray(record.top_actors),
            sourceBreakdown: isObject(record.source_breakdown) ? record.source_breakdown : {},
            raw: record,
        };
    }).sort((a, b) => Math.abs(b.netFlow) - Math.abs(a.netFlow));

    const byActorTier = Object.entries(isObject(payload.by_actor_tier) ? payload.by_actor_tier : {}).map(([tier, value]) => {
        const record = isObject(value) ? value : { net_flow: value };
        return {
            tier,
            netFlow: normalizeSignedValue(record.net_flow),
            weeklyRate: normalizeSignedValue(record.weekly_rate),
            direction: asString(record.direction, normalizeSignedValue(record.net_flow) >= 0 ? 'inflow' : 'outflow'),
            sectorBreakdown: isObject(record.sector_breakdown) ? record.sector_breakdown : {},
            topActors: toArray(record.top_actors),
            raw: record,
        };
    }).sort((a, b) => Math.abs(b.netFlow) - Math.abs(a.netFlow));

    return {
        days: asNumber(payload.days, 0),
        period: asString(payload.period),
        bySector,
        byActorTier,
        rotationMatrix: isObject(payload.rotation_matrix) ? payload.rotation_matrix : { sectors: [], matrix: [], signals: [] },
        timeSeries: toArray(payload.time_series),
        raw: payload,
    };
}

export function normalizeAstrogridSectorMap(payload) {
    if (!isObject(payload)) {
        return { sectors: [], byName: {}, tickerIndex: {}, raw: payload };
    }

    const sectorEntries = Object.entries(isObject(payload.sectors) ? payload.sectors : {}).map(([name, sector]) => {
        const actors = toArray(sector?.actors).map((actor, index) => ({
            id: actor?.ticker || actor?.name || `${name}_actor_${index + 1}`,
            name: asString(actor?.name, `Actor ${index + 1}`),
            ticker: asString(actor?.ticker),
            subsector: asString(actor?.subsector),
            type: asString(actor?.type),
            influence: asNumber(actor?.influence, 0),
            avgZ: asNumber(actor?.avg_z ?? actor?.avgZ, null),
            live: toArray(actor?.live),
            description: asString(actor?.description),
            options: isObject(actor?.options) ? actor.options : null,
            raw: actor,
        })).sort((a, b) => (b.influence || 0) - (a.influence || 0));

        return {
            name,
            etf: asString(sector?.etf),
            etfZ: asNumber(sector?.etf_z ?? sector?.etfZ, null),
            etfOptions: isObject(sector?.etf_options ?? sector?.etfOptions) ? (sector.etf_options ?? sector.etfOptions) : null,
            sectorStress: asNumber(sector?.sector_stress ?? sector?.sectorStress, null),
            subsectors: toArray(sector?.subsectors).map((item) => String(item)),
            actors,
            topActor: actors[0] || null,
            raw: sector,
        };
    }).sort((a, b) => Math.abs(b.sectorStress || 0) - Math.abs(a.sectorStress || 0));

    const byName = Object.fromEntries(sectorEntries.map((sector) => [sector.name, sector]));
    const tickerIndex = {};
    for (const sector of sectorEntries) {
        for (const actor of sector.actors) {
            if (!actor.ticker) continue;
            tickerIndex[actor.ticker.toUpperCase()] = {
                sector: sector.name,
                actor,
            };
        }
    }

    return {
        sectors: sectorEntries,
        byName,
        tickerIndex,
        raw: payload,
    };
}

export function normalizeAstrogridSectorDetail(payload) {
    if (!isObject(payload)) return null;
    const subsectors = Object.entries(isObject(payload.subsectors) ? payload.subsectors : {}).map(([name, entry]) => {
        const actors = toArray(entry?.actors).map((actor, index) => ({
            id: actor?.ticker || actor?.name || `${name}_actor_${index + 1}`,
            name: asString(actor?.name, `Actor ${index + 1}`),
            ticker: asString(actor?.ticker),
            type: asString(actor?.type),
            influence: asNumber(actor?.influence, 0),
            avgZ: asNumber(actor?.avg_z ?? actor?.avgZ, null),
            latestPrice: asNumber(actor?.latest_price ?? actor?.latestPrice, null),
            pct30d: asNumber(actor?.pct_30d ?? actor?.pct30d, null),
            relPerfVsEtf: asNumber(actor?.rel_perf_vs_etf ?? actor?.relPerfVsEtf, null),
            insiderSignal: asString(actor?.insider_signal ?? actor?.insiderSignal),
            optionsSignal: asString(actor?.options_signal ?? actor?.optionsSignal),
            description: asString(actor?.description),
            raw: actor,
        }));
        return {
            name,
            weight: asNumber(entry?.weight, 0),
            actors,
            topActor: actors[0] || null,
            raw: entry,
        };
    }).sort((a, b) => Math.abs((b.topActor?.relPerfVsEtf || 0)) - Math.abs((a.topActor?.relPerfVsEtf || 0)));

    return {
        sector: asString(payload.sector),
        etf: asString(payload.etf),
        price: asNumber(payload.price, null),
        change1m: asNumber(payload.change_1m ?? payload.change1m, null),
        subsectors,
        sectorMetrics: isObject(payload.sector_metrics) ? payload.sector_metrics : {},
        intelligence: isObject(payload.intelligence) ? payload.intelligence : {},
        raw: payload,
    };
}

export function normalizeAstrogridSignalsSnapshot(payload) {
    if (!isObject(payload)) return [];
    return toArray(payload.features).map((feature, index) => ({
        id: feature?.id || feature?.name || `feature_${index + 1}`,
        name: asString(feature?.name, `feature_${index + 1}`),
        family: asString(feature?.family),
        value: asNumber(feature?.value, 0),
        obsDate: feature?.obs_date || feature?.date || null,
        zScore: asNumber(feature?.z_score ?? feature?.zscore, 0),
        raw: feature,
    })).sort((a, b) => Math.abs(b.zScore) - Math.abs(a.zScore));
}

export function normalizeAstrogridActivePatterns(payload) {
    const rows = Array.isArray(payload)
        ? payload
        : toArray(payload?.active_patterns || payload?.patterns);
    return rows.map((pattern, index) => ({
        id: pattern?.id || `${pattern?.ticker || 'pattern'}_${index + 1}`,
        pattern: asString(pattern?.pattern || pattern?.name || pattern?.sequence, `Pattern ${index + 1}`),
        ticker: asString(pattern?.ticker),
        nextExpected: pattern?.next_expected || pattern?.expected_at || pattern?.window || null,
        confidence: normalizeConfidence(pattern?.confidence ?? pattern?.hit_rate),
        hitRate: normalizeConfidence(pattern?.hit_rate),
        actionable: Boolean(pattern?.actionable),
        raw: pattern,
    })).sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
}

export function normalizeAstrogridCrossReference(payload) {
    if (!isObject(payload)) return null;
    const checks = toArray(payload.checks).map((item, index) => ({
        id: item?.id || `check_${index + 1}`,
        category: asString(item?.category || item?.group),
        label: item?.label || item?.name || item?.claim || `Check ${index + 1}`,
        detail: item?.detail || item?.description || item?.finding || '',
        raw: item,
    }));
    const redFlags = toArray(payload.red_flags).map((item, index) => ({
        id: item?.id || `red_flag_${index + 1}`,
        category: asString(item?.category || item?.group),
        label: item?.label || item?.name || item?.claim || `Red flag ${index + 1}`,
        detail: item?.detail || item?.description || item?.finding || '',
        raw: item,
    }));
    return {
        checks,
        redFlags,
        narrative: asString(payload.narrative),
        summary: isObject(payload.summary) ? payload.summary : {},
        generatedAt: payload.generated_at || null,
        raw: payload,
    };
}

export function normalizeAstrogridScorecard(payload) {
    if (!isObject(payload)) return null;

    const items = toArray(payload.items).map((item, index) => ({
        id: item?.symbol || `scorecard_${index + 1}`,
        symbol: asString(item?.symbol, `item_${index + 1}`),
        label: asString(item?.label, asString(item?.symbol, `Item ${index + 1}`)),
        group: asString(item?.group),
        lookupTicker: asString(item?.lookup_ticker || item?.lookupTicker),
        featureName: asString(item?.feature_name || item?.featureName),
        latest: asNumber(item?.latest, null),
        latestDate: item?.latest_date || item?.latestDate || null,
        livePrice: asNumber(item?.live_price ?? item?.livePrice, null),
        historyPoints: asNumber(item?.history_points ?? item?.historyPoints, 0),
        change1dPct: asNumber(item?.change_1d_pct ?? item?.change1dPct, null),
        change5dPct: asNumber(item?.change_5d_pct ?? item?.change5dPct, null),
        change20dPct: asNumber(item?.change_20d_pct ?? item?.change20dPct, null),
        momentumScore: asNumber(item?.momentum_score ?? item?.momentumScore, 0),
        bias: asString(item?.bias),
        trend: asString(item?.trend),
        confidence: normalizeConfidence(item?.confidence),
        coverage: isObject(item?.coverage) ? item.coverage : {},
        source: asString(item?.source),
        raw: item,
    }));

    const groups = toArray(payload.groups).map((group, index) => ({
        id: asString(group?.id, `group_${index + 1}`),
        label: asString(group?.label, `Group ${index + 1}`),
        symbols: toArray(group?.symbols).map((symbol) => String(symbol)),
        available: asNumber(group?.available, 0),
        total: asNumber(group?.total, 0),
        compositeScore: asNumber(group?.composite_score ?? group?.compositeScore, 0),
        bias: asString(group?.bias),
        strongest: asString(group?.strongest),
        weakest: asString(group?.weakest),
        raw: group,
    }));

    return {
        generatedAt: payload.generated_at || null,
        source: asString(payload.source),
        universe: isObject(payload.universe) ? payload.universe : {},
        items,
        groups,
        leaders: toArray(payload.leaders).map((item) => ({
            symbol: asString(item?.symbol),
            label: asString(item?.label),
            group: asString(item?.group),
            momentumScore: asNumber(item?.momentum_score ?? item?.momentumScore, 0),
            change5dPct: asNumber(item?.change_5d_pct ?? item?.change5dPct, null),
            trend: asString(item?.trend),
            bias: asString(item?.bias),
            raw: item,
        })),
        laggards: toArray(payload.laggards).map((item) => ({
            symbol: asString(item?.symbol),
            label: asString(item?.label),
            group: asString(item?.group),
            momentumScore: asNumber(item?.momentum_score ?? item?.momentumScore, 0),
            change5dPct: asNumber(item?.change_5d_pct ?? item?.change5dPct, null),
            trend: asString(item?.trend),
            bias: asString(item?.bias),
            raw: item,
        })),
        summary: isObject(payload.summary) ? {
            total: asNumber(payload.summary.total, 0),
            available: asNumber(payload.summary.available, 0),
            coverageRatio: asNumber(payload.summary.coverage_ratio ?? payload.summary.coverageRatio, 0),
            compositeScore: asNumber(payload.summary.composite_score ?? payload.summary.compositeScore, 0),
            bias: asString(payload.summary.bias),
            leaders: toArray(payload.summary.leaders).map((item) => String(item)),
            laggards: toArray(payload.summary.laggards).map((item) => String(item)),
            cryptoScore: asNumber(payload.summary.crypto_score ?? payload.summary.cryptoScore, 0),
            macroScore: asNumber(payload.summary.macro_score ?? payload.summary.macroScore, 0),
            oracleAccuracy: asNumber(payload.summary.oracle_accuracy ?? payload.summary.oracleAccuracy, 0),
        } : {},
        evaluation: isObject(payload.evaluation) ? payload.evaluation : {},
        raw: payload,
    };
}
