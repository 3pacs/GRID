/**
 * GRID Ephemeris Engine -- Copernicus Module (JavaScript Port).
 *
 * Pure-math planetary position calculator using VSOP87-simplified orbital elements.
 * Client-side computation for instant UI without API round-trips.
 *
 * Accuracy: ~1 degree for inner planets, ~0.5 degree for outer planets.
 * Sufficient for astrological aspect computation and market correlation research.
 *
 * @module ephemeris
 */

// ============================================================================
// CONSTANTS
// ============================================================================

const J2000_JD = 2451545.0;
const J2000_DATE = new Date(Date.UTC(2000, 0, 1, 12, 0, 0));
const OBLIQUITY_J2000 = 23.439291;
const SYNODIC_MONTH = 29.53059;
const REF_NEW_MOON = new Date(Date.UTC(2000, 0, 6, 18, 14, 0));

// Moon mean elements at J2000
const MOON_L0 = 218.3165;
const MOON_RATE = 13.17640;

// Rahu (North Node)
const RAHU_L0 = 125.0445;
const RAHU_RATE = -0.05295;

// Ayanamsha
const AYANAMSHA_J2000 = 23.85;
const PRECESSION_RATE = 50.3 / 3600.0;

export const ZODIAC_SIGNS = [
  "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
  "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces",
];

export const NAKSHATRAS = [
  { name: "Ashwini",           start:   0.000, ruler: "Ketu",    deity: "Ashwini Kumaras", quality: "movable" },
  { name: "Bharani",           start:  13.333, ruler: "Venus",   deity: "Yama",            quality: "fixed" },
  { name: "Krittika",          start:  26.667, ruler: "Sun",     deity: "Agni",            quality: "dual" },
  { name: "Rohini",            start:  40.000, ruler: "Moon",    deity: "Brahma",          quality: "fixed" },
  { name: "Mrigashira",        start:  53.333, ruler: "Mars",    deity: "Soma",            quality: "dual" },
  { name: "Ardra",             start:  66.667, ruler: "Rahu",    deity: "Rudra",           quality: "fixed" },
  { name: "Punarvasu",         start:  80.000, ruler: "Jupiter", deity: "Aditi",           quality: "dual" },
  { name: "Pushya",            start:  93.333, ruler: "Saturn",  deity: "Brihaspati",      quality: "fixed" },
  { name: "Ashlesha",          start: 106.667, ruler: "Mercury", deity: "Nagas",           quality: "movable" },
  { name: "Magha",             start: 120.000, ruler: "Ketu",    deity: "Pitris",          quality: "movable" },
  { name: "Purva Phalguni",    start: 133.333, ruler: "Venus",   deity: "Bhaga",           quality: "fixed" },
  { name: "Uttara Phalguni",   start: 146.667, ruler: "Sun",     deity: "Aryaman",         quality: "fixed" },
  { name: "Hasta",             start: 160.000, ruler: "Moon",    deity: "Savitar",         quality: "movable" },
  { name: "Chitra",            start: 173.333, ruler: "Mars",    deity: "Vishvakarma",     quality: "dual" },
  { name: "Svati",             start: 186.667, ruler: "Rahu",    deity: "Vayu",            quality: "movable" },
  { name: "Vishakha",          start: 200.000, ruler: "Jupiter", deity: "Indra-Agni",      quality: "dual" },
  { name: "Anuradha",          start: 213.333, ruler: "Saturn",  deity: "Mitra",           quality: "fixed" },
  { name: "Jyeshtha",          start: 226.667, ruler: "Mercury", deity: "Indra",           quality: "fixed" },
  { name: "Mula",              start: 240.000, ruler: "Ketu",    deity: "Nirriti",         quality: "fixed" },
  { name: "Purva Ashadha",     start: 253.333, ruler: "Venus",   deity: "Apas",            quality: "movable" },
  { name: "Uttara Ashadha",    start: 266.667, ruler: "Sun",     deity: "Vishvedevas",     quality: "fixed" },
  { name: "Shravana",          start: 280.000, ruler: "Moon",    deity: "Vishnu",          quality: "movable" },
  { name: "Dhanishta",         start: 293.333, ruler: "Mars",    deity: "Vasus",           quality: "movable" },
  { name: "Shatabhisha",       start: 306.667, ruler: "Rahu",    deity: "Varuna",          quality: "movable" },
  { name: "Purva Bhadrapada",  start: 320.000, ruler: "Jupiter", deity: "Aja Ekapada",     quality: "dual" },
  { name: "Uttara Bhadrapada", start: 333.333, ruler: "Saturn",  deity: "Ahir Budhnya",    quality: "fixed" },
  { name: "Revati",            start: 346.667, ruler: "Mercury", deity: "Pushan",          quality: "movable" },
];

export const ASPECTS = {
  conjunction: { angle: 0, orb: 8, nature: "variable" },
  sextile:     { angle: 60, orb: 6, nature: "harmonious" },
  square:      { angle: 90, orb: 7, nature: "challenging" },
  trine:       { angle: 120, orb: 8, nature: "harmonious" },
  opposition:  { angle: 180, orb: 8, nature: "challenging" },
};

const PHASE_NAMES = [
  "New Moon", "Waxing Crescent", "First Quarter", "Waxing Gibbous",
  "Full Moon", "Waning Gibbous", "Last Quarter", "Waning Crescent",
];

// J2000.0 Keplerian orbital elements [value, rate_per_century]
const PLANETS = {
  Mercury: {
    a: [0.38709927, 0.00000037], e: [0.20563593, 0.00001906],
    i: [7.00497902, -0.00594749], L: [252.25032350, 149472.67411175],
    omega_bar: [77.45779628, 0.16047689], Omega: [48.33076593, -0.12534081],
  },
  Venus: {
    a: [0.72333566, 0.00000390], e: [0.00677672, -0.00004107],
    i: [3.39467605, -0.00078890], L: [181.97909950, 58517.81538729],
    omega_bar: [131.60246718, 0.00268329], Omega: [76.67984255, -0.27769418],
  },
  Earth: {
    a: [1.00000261, 0.00000562], e: [0.01671123, -0.00004392],
    i: [-0.00001531, -0.01294668], L: [100.46457166, 35999.37244981],
    omega_bar: [102.93768193, 0.32327364], Omega: [0.0, 0.0],
  },
  Mars: {
    a: [1.52371034, 0.00001847], e: [0.09339410, 0.00007882],
    i: [1.84969142, -0.00813131], L: [-4.55343205, 19140.30268499],
    omega_bar: [-23.94362959, 0.44441088], Omega: [49.55953891, -0.29257343],
  },
  Jupiter: {
    a: [5.20288700, -0.00011607], e: [0.04838624, -0.00013253],
    i: [1.30439695, -0.00183714], L: [34.39644051, 3034.74612775],
    omega_bar: [14.72847983, 0.21252668], Omega: [100.47390909, 0.20469106],
  },
  Saturn: {
    a: [9.53667594, -0.00125060], e: [0.05386179, -0.00050991],
    i: [2.48599187, 0.00193609], L: [49.95424423, 1222.49362201],
    omega_bar: [92.59887831, -0.41897216], Omega: [113.66242448, -0.28867794],
  },
  Uranus: {
    a: [19.18916464, -0.00196176], e: [0.04725744, -0.00004397],
    i: [0.77263783, -0.00242939], L: [313.23810451, 428.48202785],
    omega_bar: [170.95427630, 0.40805281], Omega: [74.01692503, 0.04240589],
  },
  Neptune: {
    a: [30.06992276, 0.00026291], e: [0.00859048, 0.00005105],
    i: [1.77004347, 0.00035372], L: [-55.12002969, 218.45945325],
    omega_bar: [44.96476227, -0.32241464], Omega: [131.78422574, -0.00508664],
  },
  Pluto: {
    a: [39.48211675, -0.00031596], e: [0.24882730, 0.00005170],
    i: [17.14001206, 0.00004818], L: [238.92903833, 145.20780515],
    omega_bar: [224.06891629, -0.04062942], Omega: [110.30393684, -0.01183482],
  },
};

const COMPUTE_PLANETS = [
  "Mercury", "Venus", "Mars", "Jupiter", "Saturn",
  "Uranus", "Neptune", "Pluto",
];

// ============================================================================
// HELPER MATH
// ============================================================================

function normalizeAngle(deg) {
  return ((deg % 360.0) + 360.0) % 360.0;
}

function degToRad(deg) {
  return deg * Math.PI / 180.0;
}

function radToDeg(rad) {
  return rad * 180.0 / Math.PI;
}

function solveKepler(M_deg, e, tol = 1e-6) {
  const M = degToRad(normalizeAngle(M_deg));
  let E = e < 0.8 ? M + e * Math.sin(M) : Math.PI;
  for (let iter = 0; iter < 50; iter++) {
    const dE = (E - e * Math.sin(E) - M) / (1.0 - e * Math.cos(E));
    E -= dE;
    if (Math.abs(dE) < tol) break;
  }
  return radToDeg(E);
}

function angularSeparation(lon1, lon2) {
  let diff = Math.abs(lon1 - lon2) % 360.0;
  return diff <= 180.0 ? diff : 360.0 - diff;
}

function signedAngularDiff(lon1, lon2) {
  let diff = (lon1 - lon2) % 360.0;
  if (diff > 180.0) diff -= 360.0;
  if (diff < -180.0) diff += 360.0;
  return diff;
}

function eclipticToEquatorial(lonDeg, latDeg, obliquityDeg) {
  const lon = degToRad(lonDeg);
  const lat = degToRad(latDeg);
  const eps = degToRad(obliquityDeg);

  const sinRaCDec = Math.sin(lon) * Math.cos(eps) - Math.tan(lat) * Math.sin(eps);
  const cosRaCDec = Math.cos(lon);
  const ra = ((radToDeg(Math.atan2(sinRaCDec, cosRaCDec))) % 360.0 + 360.0) % 360.0;

  const sinDec = Math.sin(lat) * Math.cos(eps) + Math.cos(lat) * Math.sin(eps) * Math.sin(lon);
  const dec = radToDeg(Math.asin(Math.max(-1.0, Math.min(1.0, sinDec))));

  return { ra, dec };
}

// ============================================================================
// DATE UTILITIES
// ============================================================================

/**
 * Convert a Date to Julian Date.
 * @param {Date} dt
 * @returns {number}
 */
export function julianDate(dt) {
  let y = dt.getUTCFullYear();
  let m = dt.getUTCMonth() + 1;
  const d = dt.getUTCDate() + dt.getUTCHours() / 24.0 + dt.getUTCMinutes() / 1440.0;
  if (m <= 2) { y -= 1; m += 12; }
  const A = Math.floor(y / 100);
  const B = 2 - A + Math.floor(A / 4);
  return Math.floor(365.25 * (y + 4716)) + Math.floor(30.6001 * (m + 1)) + d + B - 1524.5;
}

/**
 * Julian centuries since J2000.0.
 * @param {Date} dt
 * @returns {number}
 */
export function centuriesSinceJ2000(dt) {
  return (julianDate(dt) - J2000_JD) / 36525.0;
}

/**
 * Days since J2000.0 epoch.
 * @param {Date} dt
 * @returns {number}
 */
function daysSinceJ2000(dt) {
  return (dt.getTime() - J2000_DATE.getTime()) / 86400000.0;
}

/**
 * Create a Date shifted by N days.
 * @param {Date} dt
 * @param {number} days
 * @returns {Date}
 */
function addDays(dt, days) {
  return new Date(dt.getTime() + days * 86400000);
}

/**
 * Normalize a Date to noon UTC for consistent calculations.
 * @param {Date} dt
 * @returns {Date}
 */
function toNoonUTC(dt) {
  return new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), 12, 0, 0));
}

// ============================================================================
// CORE ENGINE
// ============================================================================

/**
 * Compute heliocentric position of a planet.
 * @param {string} planet
 * @param {number} T - Julian centuries since J2000
 * @returns {{ lon: number, lat: number, dist: number }}
 */
function heliocentricPosition(planet, T) {
  const elem = PLANETS[planet];

  const a = elem.a[0] + elem.a[1] * T;
  const e = elem.e[0] + elem.e[1] * T;
  const i = elem.i[0] + elem.i[1] * T;
  const L = elem.L[0] + elem.L[1] * T;
  const omegaBar = elem.omega_bar[0] + elem.omega_bar[1] * T;
  const Omega = elem.Omega[0] + elem.Omega[1] * T;

  const omega = omegaBar - Omega;
  const M = normalizeAngle(L - omegaBar);
  const E = solveKepler(M, e);
  const E_rad = degToRad(E);

  const nu = radToDeg(Math.atan2(
    Math.sqrt(1.0 - e * e) * Math.sin(E_rad),
    Math.cos(E_rad) - e
  ));

  const r = a * (1.0 - e * Math.cos(E_rad));

  const u = degToRad(nu + omega);
  const OmegaRad = degToRad(Omega);
  const iRad = degToRad(i);

  const cosU = Math.cos(u);
  const sinU = Math.sin(u);
  const cosOmega = Math.cos(OmegaRad);
  const sinOmega = Math.sin(OmegaRad);
  const cosI = Math.cos(iRad);
  const sinI = Math.sin(iRad);

  const xEcl = r * (cosOmega * cosU - sinOmega * sinU * cosI);
  const yEcl = r * (sinOmega * cosU + cosOmega * sinU * cosI);
  const zEcl = r * sinU * sinI;

  const lon = normalizeAngle(radToDeg(Math.atan2(yEcl, xEcl)));
  const lat = r > 0 ? radToDeg(Math.asin(Math.max(-1.0, Math.min(1.0, zEcl / r)))) : 0;

  return { lon, lat, dist: r };
}

/**
 * Quick geocentric longitude for retrograde checks.
 */
function quickGeoLon(planet, T) {
  const h = heliocentricPosition(planet, T);
  const e = heliocentricPosition("Earth", T);
  const hr = degToRad(h.lon), hlar = degToRad(h.lat);
  const er = degToRad(e.lon), elar = degToRad(e.lat);
  const xp = h.dist * Math.cos(hlar) * Math.cos(hr);
  const yp = h.dist * Math.cos(hlar) * Math.sin(hr);
  const xe = e.dist * Math.cos(elar) * Math.cos(er);
  const ye = e.dist * Math.cos(elar) * Math.sin(er);
  return normalizeAngle(radToDeg(Math.atan2(yp - ye, xp - xe)));
}

/**
 * Check if a planet is retrograde on a given date.
 */
function checkRetrograde(planet, dt) {
  const prev = addDays(dt, -1);
  const next = addDays(dt, 1);
  const Tp = centuriesSinceJ2000(prev);
  const Tn = centuriesSinceJ2000(next);
  const lonPrev = quickGeoLon(planet, Tp);
  const lonNext = quickGeoLon(planet, Tn);
  return signedAngularDiff(lonNext, lonPrev) < 0;
}

/**
 * Compute Moon position (simplified).
 */
function computeMoonPosition(dt) {
  const days = daysSinceJ2000(toNoonUTC(dt));

  let L = normalizeAngle(MOON_L0 + MOON_RATE * days);
  const M_moon = normalizeAngle(134.9634 + 13.06499 * days);
  const M_sun = normalizeAngle(357.5291 + 0.98560 * days);
  const D = normalizeAngle(297.8502 + 12.19075 * days);
  const F = normalizeAngle(93.2720 + 13.22935 * days);

  const dL =
    6.289 * Math.sin(degToRad(M_moon))
    - 1.274 * Math.sin(degToRad(2.0 * D - M_moon))
    + 0.658 * Math.sin(degToRad(2.0 * D))
    - 0.214 * Math.sin(degToRad(2.0 * M_moon))
    - 0.186 * Math.sin(degToRad(M_sun))
    - 0.114 * Math.sin(degToRad(2.0 * F));

  const eclLon = normalizeAngle(L + dL);
  const eclLat =
    5.128 * Math.sin(degToRad(F))
    + 0.281 * Math.sin(degToRad(M_moon + F))
    - 0.278 * Math.sin(degToRad(F - M_moon));

  const distKm = 385001.0 - 20905.0 * Math.cos(degToRad(M_moon));
  const distAu = distKm / 149597870.7;

  const signIdx = Math.floor(eclLon / 30.0) % 12;
  const signDeg = eclLon % 30.0;

  const T = centuriesSinceJ2000(dt);
  const obliquity = OBLIQUITY_J2000 - 0.013004 * T;
  const { ra, dec } = eclipticToEquatorial(eclLon, eclLat, obliquity);

  return {
    planet: "Moon",
    ecliptic_longitude: +eclLon.toFixed(4),
    ecliptic_latitude: +eclLat.toFixed(4),
    heliocentric_longitude: null,
    distance_au: +distAu.toFixed(6),
    geocentric_longitude: +eclLon.toFixed(4),
    zodiac_sign: ZODIAC_SIGNS[signIdx],
    zodiac_degree: +signDeg.toFixed(4),
    is_retrograde: false,
    right_ascension: +ra.toFixed(4),
    declination: +dec.toFixed(4),
  };
}

/**
 * Compute lunar node (Rahu/Ketu) position.
 */
function computeLunarNode(node, dt) {
  const days = daysSinceJ2000(toNoonUTC(dt));
  const rahuLon = normalizeAngle(RAHU_L0 + RAHU_RATE * days);
  const lon = node === "Ketu" ? normalizeAngle(rahuLon + 180.0) : rahuLon;

  const signIdx = Math.floor(lon / 30.0) % 12;
  const signDeg = lon % 30.0;

  return {
    planet: node,
    ecliptic_longitude: +lon.toFixed(4),
    ecliptic_latitude: 0.0,
    heliocentric_longitude: null,
    distance_au: null,
    geocentric_longitude: +lon.toFixed(4),
    zodiac_sign: ZODIAC_SIGNS[signIdx],
    zodiac_degree: +signDeg.toFixed(4),
    is_retrograde: true,
    right_ascension: null,
    declination: null,
  };
}

// ============================================================================
// PUBLIC API
// ============================================================================

/**
 * Compute position for a single planet on a date.
 * @param {string} planet - Planet name
 * @param {Date} dt - Date to compute
 * @returns {Object} Position data
 */
export function computePosition(planet, dt) {
  if (planet === "Rahu" || planet === "Ketu") return computeLunarNode(planet, dt);
  if (planet === "Moon") return computeMoonPosition(dt);

  const dtNoon = toNoonUTC(dt);
  const T = centuriesSinceJ2000(dtNoon);

  const h = heliocentricPosition(planet, T);
  const e = heliocentricPosition("Earth", T);

  const hLonRad = degToRad(h.lon), hLatRad = degToRad(h.lat);
  const eLonRad = degToRad(e.lon), eLatRad = degToRad(e.lat);

  const xp = h.dist * Math.cos(hLatRad) * Math.cos(hLonRad);
  const yp = h.dist * Math.cos(hLatRad) * Math.sin(hLonRad);
  const zp = h.dist * Math.sin(hLatRad);

  const xe = e.dist * Math.cos(eLatRad) * Math.cos(eLonRad);
  const ye = e.dist * Math.cos(eLatRad) * Math.sin(eLonRad);
  const ze = e.dist * Math.sin(eLatRad);

  const xg = xp - xe, yg = yp - ye, zg = zp - ze;
  const geoDist = Math.sqrt(xg * xg + yg * yg + zg * zg);
  const geoLon = normalizeAngle(radToDeg(Math.atan2(yg, xg)));
  const geoLat = geoDist > 0 ? radToDeg(Math.asin(Math.max(-1, Math.min(1, zg / geoDist)))) : 0;

  const signIdx = Math.floor(geoLon / 30.0) % 12;
  const signDeg = geoLon % 30.0;

  const isRetro = checkRetrograde(planet, dtNoon);

  const obliquity = OBLIQUITY_J2000 - 0.013004 * T;
  const { ra, dec } = eclipticToEquatorial(geoLon, geoLat, obliquity);

  return {
    planet,
    ecliptic_longitude: +geoLon.toFixed(4),
    ecliptic_latitude: +geoLat.toFixed(4),
    heliocentric_longitude: +h.lon.toFixed(4),
    distance_au: +h.dist.toFixed(6),
    geocentric_longitude: +geoLon.toFixed(4),
    zodiac_sign: ZODIAC_SIGNS[signIdx],
    zodiac_degree: +signDeg.toFixed(4),
    is_retrograde: isRetro,
    right_ascension: +ra.toFixed(4),
    declination: +dec.toFixed(4),
  };
}

/**
 * Compute positions for all planets, Moon, and lunar nodes.
 * @param {Date} dt
 * @returns {Object} Map of planet name to position data
 */
export function computeAllPositions(dt) {
  const result = {};
  for (const planet of COMPUTE_PLANETS) {
    result[planet] = computePosition(planet, dt);
  }
  result.Moon = computePosition("Moon", dt);
  result.Rahu = computePosition("Rahu", dt);
  result.Ketu = computePosition("Ketu", dt);
  return result;
}

/**
 * Find all aspects between planet pairs.
 * @param {Date} dt
 * @param {number|null} orbOverride - Global orb override
 * @returns {Array} List of aspect objects
 */
export function computeAspects(dt, orbOverride = null) {
  const positions = computeAllPositions(dt);
  const bodies = [...COMPUTE_PLANETS, "Moon"];
  const found = [];

  for (let i = 0; i < bodies.length; i++) {
    for (let j = i + 1; j < bodies.length; j++) {
      const p1 = bodies[i], p2 = bodies[j];
      const lon1 = positions[p1].geocentric_longitude;
      const lon2 = positions[p2].geocentric_longitude;
      const sep = angularSeparation(lon1, lon2);

      for (const [aspName, aspDef] of Object.entries(ASPECTS)) {
        const aspOrb = orbOverride !== null ? orbOverride : aspDef.orb;
        const diff = Math.abs(sep - aspDef.angle);
        if (diff <= aspOrb) {
          // Check if applying
          const posTomorrow = computeAllPositions(addDays(dt, 1));
          const sepTomorrow = angularSeparation(
            posTomorrow[p1].geocentric_longitude,
            posTomorrow[p2].geocentric_longitude
          );
          const diffTomorrow = Math.abs(sepTomorrow - aspDef.angle);
          const applying = diffTomorrow < diff;

          found.push({
            planet1: p1,
            planet2: p2,
            aspect_type: aspName,
            exact_angle: aspDef.angle,
            angle_between: +sep.toFixed(4),
            orb_used: +diff.toFixed(4),
            nature: aspDef.nature,
            applying,
          });
        }
      }
    }
  }

  return found;
}

/**
 * Compute lunar phase information.
 * @param {Date} dt
 * @returns {Object}
 */
export function computeLunarPhase(dt) {
  const dtNoon = toNoonUTC(dt);
  const daysSinceRef = (dtNoon.getTime() - REF_NEW_MOON.getTime()) / 86400000.0;
  const phase = ((daysSinceRef % SYNODIC_MONTH) + SYNODIC_MONTH) % SYNODIC_MONTH / SYNODIC_MONTH;

  const illumination = (1.0 - Math.cos(phase * 2.0 * Math.PI)) / 2.0 * 100.0;
  const phaseIdx = Math.floor(phase * 8.0) % 8;
  const phaseName = PHASE_NAMES[phaseIdx];

  const daysToNew = phase > 0.001 ? (1.0 - phase) * SYNODIC_MONTH : 0.0;
  let phaseToFull = 0.5 - phase;
  if (phaseToFull < 0) phaseToFull += 1.0;
  const daysToFull = phaseToFull * SYNODIC_MONTH;

  return {
    phase: +phase.toFixed(6),
    illumination: +illumination.toFixed(4),
    phase_name: phaseName,
    days_to_new: +daysToNew.toFixed(2),
    days_to_full: +daysToFull.toFixed(2),
    phase_angle: +(phase * 360.0).toFixed(4),
  };
}

/**
 * Compute Moon's nakshatra.
 * @param {Date} dt
 * @returns {Object}
 */
export function computeNakshatra(dt) {
  const moon = computeMoonPosition(dt);
  const moonLon = moon.ecliptic_longitude;

  const days = daysSinceJ2000(toNoonUTC(dt));
  const ayanamsha = AYANAMSHA_J2000 + PRECESSION_RATE * (days / 365.25);
  const siderealLon = normalizeAngle(moonLon - ayanamsha);

  const nakSpan = 360.0 / 27.0;
  const nakIdx = Math.floor(siderealLon / nakSpan) % 27;
  const nak = NAKSHATRAS[nakIdx];

  const degInNak = siderealLon - nakIdx * nakSpan;
  let pada = Math.floor(degInNak / (nakSpan / 4.0)) + 1;
  pada = Math.min(pada, 4);

  return {
    nakshatra_index: nakIdx,
    nakshatra_name: nak.name,
    pada,
    ruling_planet: nak.ruler,
    deity: nak.deity,
    quality: nak.quality,
    moon_longitude: +moonLon.toFixed(4),
    sidereal_longitude: +siderealLon.toFixed(4),
    degree_in_nakshatra: +degInNak.toFixed(4),
  };
}

/**
 * Get full ephemeris snapshot for a date.
 * @param {Date} dt
 * @returns {Object}
 */
export function getFullEphemeris(dt) {
  const positions = computeAllPositions(dt);
  const aspects = computeAspects(dt);
  const lunar = computeLunarPhase(dt);
  const nakshatra = computeNakshatra(dt);

  const retrogrades = Object.entries(positions)
    .filter(([name, pos]) => pos.is_retrograde && name !== "Rahu" && name !== "Ketu")
    .map(([name]) => name);

  const summary = Object.entries(positions).map(([name, pos]) => {
    const r = pos.is_retrograde && name !== "Rahu" && name !== "Ketu" ? " (R)" : "";
    return `${name}: ${pos.zodiac_sign} ${pos.zodiac_degree.toFixed(1)}${r}`;
  });

  return {
    date: dt.toISOString().slice(0, 10),
    positions,
    aspects,
    lunar_phase: lunar,
    nakshatra,
    retrograde_planets: retrogrades,
    summary,
  };
}
