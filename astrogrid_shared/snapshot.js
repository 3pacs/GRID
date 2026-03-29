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

function asString(value, fallback = '') {
    return typeof value === 'string' && value.trim() !== '' ? value : fallback;
}

function normalizeId(value) {
    return asString(value)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
}

export function normalizeAstrogridBodies(snapshot) {
    const raw = snapshot?.bodies ?? snapshot?.positions ?? snapshot?.objects ?? snapshot?.planets ?? {};
    if (Array.isArray(raw)) {
        return raw
            .map((body, index) => ({
                id: normalizeId(body?.id || body?.name || body?.planet || `body_${index + 1}`),
                name: asString(body?.name || body?.planet, `Body ${index + 1}`),
                sign: asString(body?.sign || body?.zodiac_sign),
                longitude: asNumber(body?.longitude ?? body?.geocentric_longitude ?? body?.ecliptic_longitude),
                geocentric_longitude: asNumber(body?.geocentric_longitude ?? body?.longitude ?? body?.ecliptic_longitude),
                latitude: asNumber(body?.latitude ?? body?.ecliptic_latitude),
                ecliptic_latitude: asNumber(body?.ecliptic_latitude ?? body?.latitude),
                rightAscension: asNumber(body?.right_ascension ?? body?.rightAscension ?? body?.ra),
                right_ascension: asNumber(body?.right_ascension ?? body?.rightAscension ?? body?.ra),
                declination: asNumber(body?.declination ?? body?.dec),
                distance: asNumber(body?.distance ?? body?.distance_au),
                distance_au: asNumber(body?.distance_au ?? body?.distance),
                speed: asNumber(body?.speed ?? body?.motion ?? body?.daily_motion),
                daily_motion: asNumber(body?.daily_motion ?? body?.speed ?? body?.motion),
                retrograde: Boolean(body?.retrograde ?? body?.is_retrograde),
                is_retrograde: Boolean(body?.is_retrograde ?? body?.retrograde),
                degree: asNumber(body?.degree ?? body?.zodiac_degree ?? body?.degree_in_sign),
                zodiac_degree: asNumber(body?.zodiac_degree ?? body?.degree ?? body?.degree_in_sign),
                precision: asString(body?.precision || body?.source_precision || body?.accuracy, 'medium'),
                planet: asString(body?.planet || body?.name),
                raw: body || null,
            }))
            .filter((body) => body.id);
    }

    if (isObject(raw)) {
        return Object.entries(raw).map(([key, body]) => ({
            id: normalizeId(body?.id || key),
            name: asString(body?.name || body?.planet, key),
            sign: asString(body?.sign || body?.zodiac_sign),
            longitude: asNumber(body?.longitude ?? body?.geocentric_longitude ?? body?.ecliptic_longitude),
            geocentric_longitude: asNumber(body?.geocentric_longitude ?? body?.longitude ?? body?.ecliptic_longitude),
            latitude: asNumber(body?.latitude ?? body?.ecliptic_latitude),
            ecliptic_latitude: asNumber(body?.ecliptic_latitude ?? body?.latitude),
            rightAscension: asNumber(body?.right_ascension ?? body?.rightAscension ?? body?.ra),
            right_ascension: asNumber(body?.right_ascension ?? body?.rightAscension ?? body?.ra),
            declination: asNumber(body?.declination ?? body?.dec),
            distance: asNumber(body?.distance ?? body?.distance_au),
            distance_au: asNumber(body?.distance_au ?? body?.distance),
            speed: asNumber(body?.speed ?? body?.motion ?? body?.daily_motion),
            daily_motion: asNumber(body?.daily_motion ?? body?.speed ?? body?.motion),
            retrograde: Boolean(body?.retrograde ?? body?.is_retrograde),
            is_retrograde: Boolean(body?.is_retrograde ?? body?.retrograde),
            degree: asNumber(body?.degree ?? body?.zodiac_degree ?? body?.degree_in_sign),
            zodiac_degree: asNumber(body?.zodiac_degree ?? body?.degree ?? body?.degree_in_sign),
            precision: asString(body?.precision || body?.source_precision || body?.accuracy, 'medium'),
            planet: asString(body?.planet || body?.name, key),
            raw: body || null,
        }));
    }

    return [];
}

export function normalizeAstrogridAspects(snapshot) {
    const raw = snapshot?.aspects ?? snapshot?.relationships ?? [];
    return toArray(raw)
        .map((aspect, index) => {
            const type = normalizeId(aspect?.aspect_type || aspect?.type || aspect?.name);
            const aspectType = type || 'conjunction';
            const orb = asNumber(aspect?.orb_used ?? aspect?.orb ?? aspect?.distance);
            return {
                id: normalizeId(aspect?.id || `${aspect?.planet1 || aspect?.from || 'a'}_${aspect?.planet2 || aspect?.to || 'b'}_${index}`),
                planet1: asString(aspect?.planet1 || aspect?.from),
                planet2: asString(aspect?.planet2 || aspect?.to),
                aspect_type: aspectType,
                type: aspectType,
                exact_angle: asNumber(aspect?.exact_angle ?? aspect?.angle),
                angle: asNumber(aspect?.exact_angle ?? aspect?.angle),
                angle_between: asNumber(aspect?.angle_between ?? aspect?.separation),
                orb_used: orb,
                orb,
                applying: Boolean(aspect?.applying),
                nature: asString(aspect?.nature),
                raw: aspect || null,
            };
        })
        .filter((aspect) => aspect.id);
}

export function normalizeAstrogridLunar(snapshot) {
    const raw = snapshot?.lunar ?? snapshot?.lunar_phase ?? {};
    const phaseName = asString(raw?.phase_name || raw?.phase || raw?.name || raw?.moon_phase, 'Unknown');
    const daysToNew = asNumber(raw?.days_to_new ?? raw?.days_to_new_moon, 0);
    const daysToFull = asNumber(raw?.days_to_full ?? raw?.days_to_full_moon, 0);
    return {
        phase: asNumber(raw?.phase ?? raw?.phase_fraction ?? raw?.phaseAngle ?? raw?.phase_angle, 0.5),
        phaseName,
        phase_name: phaseName,
        illumination: asNumber(raw?.illumination ?? raw?.percent ?? raw?.illumination_pct, 50),
        daysToNew,
        days_to_new: daysToNew,
        daysToFull,
        days_to_full: daysToFull,
        sign: asString(raw?.sign),
        raw: raw || null,
    };
}

export function normalizeAstrogridNakshatra(snapshot) {
    const raw = snapshot?.nakshatra ?? {};
    const name = asString(raw?.nakshatra_name || raw?.name, 'Unknown');
    const ruler = asString(raw?.ruling_planet || raw?.ruler);
    const index = asNumber(raw?.nakshatra_index, 0);
    return {
        name,
        nakshatra_name: name,
        quality: asString(raw?.quality || raw?.nakshatra_quality_name, 'Dual'),
        ruler,
        ruling_planet: ruler,
        deity: asString(raw?.deity),
        index,
        nakshatra_index: index,
        pada: asNumber(raw?.pada, 1),
        raw: raw || null,
    };
}

export function normalizeAstrogridSignals(rawSignals) {
    if (Array.isArray(rawSignals)) {
        return rawSignals.map((item, index) => ({
            key: normalizeId(item?.key || item?.name || `signal_${index + 1}`),
            name: asString(item?.name || item?.key, `Signal ${index + 1}`),
            value: asNumber(item?.value ?? item?.score ?? item?.strength),
            label: asString(item?.label),
            direction: asString(item?.direction),
            raw: item || null,
        }));
    }

    if (isObject(rawSignals)) {
        return Object.entries(rawSignals).map(([key, value]) => ({
            key: normalizeId(key),
            name: key,
            value: asNumber(value, 0),
            label: asString(value?.label),
            direction: asString(value?.direction),
            raw: value || null,
        }));
    }

    return [];
}

export function getAstrogridDateLabel(snapshot) {
    return snapshot?.date || snapshot?.timestamp || snapshot?.datetime || 'now';
}
