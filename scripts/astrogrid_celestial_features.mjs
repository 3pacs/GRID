// Offline features from the actual shipped twelve engines, never market prices.
// Run: node --experimental-default-type=module scripts/astrogrid_celestial_features.mjs dates.json output.json
import {readFileSync, writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {getFullEphemeris} from '../astrogrid_shared/ephemeris.js';
import {computeEngineOutputs, mergeSeer} from '../astrogrid_shared/prophecy.js';

const dates = JSON.parse(readFileSync(process.argv[2], 'utf8'));
const sun = JSON.parse(readFileSync(process.argv[4], 'utf8'));
const rad = (deg) => deg * Math.PI / 180;
const records = dates.map((day) => {
    const timestamp = `${day}T12:00:00.000Z`;
    // The production Python snapshot computes date-only positions at 00:00Z
    // and labels the day's snapshot at 12:00Z. Preserve that convention.
    const sky = getFullEphemeris(new Date(`${day}T00:00:00.000Z`));
    sky.positions.Sun = sun[day];
    if (!sun[day]) throw new Error('Missing production-formula Sun position');
    const phase = sky.lunar_phase.phase;
    const tithi = Math.min(30, Math.floor(phase * 30) + 1);
    const snapshot = {timestamp, date:day, positions:sky.positions, aspects:sky.aspects,
        lunar:sky.lunar_phase, nakshatra:sky.nakshatra, signals:[],
        local_features:{tithi}};
    const engines = computeEngineOutputs(snapshot, [], 'chorus');
    if (engines.length !== 12) throw new Error('Missing implemented mystical engine');
    const engine = Object.fromEntries(engines.map((e) => [e.engine_id, e.feature_trace.score]));
    const raw = {};
    for (const [name, p] of Object.entries(sky.positions)) {
        raw[`${name}_sin`] = Math.sin(rad(p.geocentric_longitude));
        raw[`${name}_cos`] = Math.cos(rad(p.geocentric_longitude));
        raw[`${name}_retrograde`] = Number(p.is_retrograde);
        const signs = ['Aries','Taurus','Gemini','Cancer','Leo','Virgo','Libra','Scorpio','Sagittarius','Capricorn','Aquarius','Pisces'];
        if (!signs.includes(p.zodiac_sign)) throw new Error('Missing zodiac state');
        for (const sign of signs) raw[`${name}_zodiac_${sign}`] = Number(p.zodiac_sign === sign);
    }
    raw.lunar_sin = Math.sin(phase * 2 * Math.PI);
    raw.lunar_cos = Math.cos(phase * 2 * Math.PI);
    raw.lunar_illumination = sky.lunar_phase.illumination;
    for (let i=0;i<27;i++) raw[`nakshatra_${i}`] = Number(sky.nakshatra.nakshatra_index === i);
    for (let i=1;i<=30;i++) raw[`tithi_${i}`] = Number(tithi === i);
    const angles = [0,60,90,120,180];
    for (const angle of angles) {
        raw[`aspect_${angle}`] = sky.aspects.filter((a) => Number(a.exact_angle) === angle).length;
    }
    const seer = mergeSeer(engines, {});
    const direction = seer.prediction.startsWith('Press') ? 1 : seer.prediction.startsWith('Hedge') ? -1 : 0;
    if (![...Object.values(engine), ...Object.values(raw)].every(Number.isFinite)) {
        throw new Error('Nonfinite celestial feature');
    }
    return {date:day, engine, sky:raw, seer_direction:direction};
});
const code = ['astrogrid_shared/ephemeris.js','astrogrid_shared/prophecy.js','astrogrid_shared/snapshot.js'];
const hashes = Object.fromEntries(code.map((path) => [path,createHash('sha256').update(readFileSync(path)).digest('hex')]));
writeFileSync(process.argv[3], JSON.stringify({records,code_sha256:hashes,
    limitations:['Computed approximate ephemeris, not authoritative astrometry',
        'Sun uses the production Python Earth-opposition formula; annual seasonality is controlled',
        'No fixed-star astrometry is supplied',
        'Measured solar weather and randomly drawn divinations are not included',
        'Twelve implemented stylized engines share inputs; they are not twelve independent experiments']}));
