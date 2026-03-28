import {
  getAstrogridDateLabel,
  normalizeAstrogridAspects,
  normalizeAstrogridBodies,
  normalizeAstrogridLunar,
  normalizeAstrogridNakshatra,
  normalizeAstrogridSignals,
} from "./lib/snapshot.js";
import { computePosition } from "./lib/ephemeris.js";

const SIGN_NAMES = [
  "Aries",
  "Taurus",
  "Gemini",
  "Cancer",
  "Leo",
  "Virgo",
  "Libra",
  "Scorpio",
  "Sagittarius",
  "Capricorn",
  "Aquarius",
  "Pisces",
];

const ASPECT_COLORS = {
  conjunction: "#f7d36b",
  opposition: "#ff6b6b",
  trine: "#6ee7b7",
  square: "#fb923c",
  sextile: "#7dd3fc",
  default: "#94a3b8",
};

const BODY_COLORS = {
  sun: "#f7d36b",
  moon: "#7dd3fc",
  mercury: "#a78bfa",
  venus: "#f472b6",
  mars: "#fb923c",
  jupiter: "#6ee7b7",
  saturn: "#c4b5fd",
  uranus: "#38bdf8",
  neptune: "#60a5fa",
  pluto: "#f87171",
  rahu: "#d96b43",
  ketu: "#88b6c9",
  default: "#cbd5e1",
};

const WORLD_EDGE_COLORS = {
  capital: "#d96b43",
  telemetry: "#7dd3fc",
  mass: "#ebe6d6",
  compute: "#79b287",
  policy: "#c4b5fd",
  corridor: "#b8924d",
  default: "#948a72",
};

const WORLD_NODE_COLORS = {
  star: { fill: "rgba(247, 211, 107, 0.2)", stroke: "#f7d36b" },
  planet: { fill: "rgba(125, 211, 252, 0.14)", stroke: "#88b6c9" },
  moon: { fill: "rgba(235, 230, 214, 0.1)", stroke: "#ebe6d6" },
  orbit: { fill: "rgba(184, 146, 77, 0.1)", stroke: "#b8924d" },
  surface: { fill: "rgba(217, 107, 67, 0.1)", stroke: "#d96b43" },
  corridor: { fill: "rgba(121, 178, 135, 0.1)", stroke: "#79b287" },
  satellite: { fill: "rgba(196, 181, 253, 0.12)", stroke: "#c4b5fd" },
  default: { fill: "rgba(148, 138, 114, 0.1)", stroke: "#948a72" },
};

const WORLD_SCALE_BANDS = [
  { id: "heliocentric", label: "Heliocentric", x: 36, y: 52, width: 150, height: 304 },
  { id: "earth_system", label: "Earth System", x: 198, y: 52, width: 316, height: 304 },
  { id: "cislunar", label: "Cislunar", x: 526, y: 52, width: 282, height: 304 },
  { id: "martian", label: "Martian", x: 820, y: 52, width: 224, height: 304 },
];

const CANONICAL_PLANETS = {
  mercury: "Mercury",
  venus: "Venus",
  mars: "Mars",
  jupiter: "Jupiter",
  saturn: "Saturn",
  uranus: "Uranus",
  neptune: "Neptune",
  pluto: "Pluto",
  moon: "Moon",
  rahu: "Rahu",
  ketu: "Ketu",
};

const TRAJECTORY_CACHE = new Map();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function toNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeAngle(value) {
  const n = toNumber(value, 0);
  return ((n % 360) + 360) % 360;
}

function signFromLongitude(lon) {
  const normalized = normalizeAngle(lon);
  const signIndex = Math.floor(normalized / 30) % 12;
  return {
    sign: SIGN_NAMES[signIndex],
    degree: normalized % 30,
    signIndex,
  };
}

function pickBodyLongitude(body) {
  return (
    toNumber(body?.geocentric_longitude) ??
    toNumber(body?.longitude) ??
    toNumber(body?.ecliptic_longitude) ??
    toNumber(body?.lon) ??
    0
  );
}

function pickBodyLatitude(body) {
  return (
    toNumber(body?.ecliptic_latitude) ??
    toNumber(body?.latitude) ??
    toNumber(body?.lat) ??
    0
  );
}

function pickBodySpeed(body) {
  return toNumber(body?.speed) ?? toNumber(body?.daily_motion) ?? toNumber(body?.velocity) ?? null;
}

function pickBodyDistance(body) {
  return toNumber(body?.distance) ?? toNumber(body?.distance_au) ?? null;
}

function pickBodyRa(body) {
  return (
    toNumber(body?.right_ascension) ??
    toNumber(body?.rightAscension) ??
    toNumber(body?.ra) ??
    null
  );
}

function pickBodyDec(body) {
  return (
    toNumber(body?.declination) ??
    toNumber(body?.dec) ??
    null
  );
}

function getBodies(snapshot) {
  return normalizeAstrogridBodies(snapshot).map((body) => ({
    id: body.id,
    name: body.name,
    raw: body,
  }));
}

function getAspects(snapshot) {
  return normalizeAstrogridAspects(snapshot).map((aspect) => ({
    id: aspect.id,
    planet1: aspect.planet1 || "A",
    planet2: aspect.planet2 || "B",
    type: aspect.type || "default",
    orb: toNumber(aspect.orb, null),
    applying: Boolean(aspect.applying),
    angle: toNumber(aspect.angle, null),
    raw: aspect,
  }));
}

function getSignals(snapshot) {
  return normalizeAstrogridSignals(
    snapshot?.signals ?? snapshot?.gridSignals ?? snapshot?.marketSignals ?? snapshot?.signals_state
  );
}

function getLunar(snapshot) {
  return normalizeAstrogridLunar(snapshot);
}

function getNakshatra(snapshot) {
  return normalizeAstrogridNakshatra(snapshot);
}

function getDateLabel(snapshot) {
  return getAstrogridDateLabel(snapshot);
}

function polarToCartesian(cx, cy, radius, angleDeg) {
  const angle = (angleDeg - 90) * (Math.PI / 180);
  return {
    x: cx + radius * Math.cos(angle),
    y: cy + radius * Math.sin(angle),
  };
}

function bodyGlyph(body) {
  const raw = body.raw || {};
  return (
    raw.glyph ||
    raw.symbol ||
    raw.icon ||
    raw.planet?.[0] ||
    body.name?.[0] ||
    "•"
  );
}

function aspectColor(type) {
  return ASPECT_COLORS[type] || ASPECT_COLORS.default;
}

function bodyColor(body) {
  const key = String(body?.id || body?.name || "").toLowerCase();
  return BODY_COLORS[key] || BODY_COLORS.default;
}

function aspectStrength(orbs) {
  if (orbs == null) return 0.45;
  const bounded = Math.max(0, Math.min(10, orbs));
  return 1 - bounded / 10;
}

function bodyPositionMap(bodies) {
  const map = new Map();
  for (const body of bodies) {
    const lon = pickBodyLongitude(body.raw);
    const lat = pickBodyLatitude(body.raw);
    const record = { lon, lat, body };
    map.set(body.id, record);
    map.set(String(body.id).toLowerCase(), record);
    map.set(String(body.name).toLowerCase(), record);
    if (body.raw?.planet) {
      map.set(String(body.raw.planet), record);
      map.set(String(body.raw.planet).toLowerCase(), record);
    }
  }
  return map;
}

function bodyLookupKeys(body) {
  const keys = new Set([
    body.id,
    body.name,
    body.id?.toLowerCase?.(),
    body.name?.toLowerCase?.(),
  ]);
  if (body.raw?.planet) {
    keys.add(String(body.raw.planet));
    keys.add(String(body.raw.planet).toLowerCase());
  }
  return [...keys].filter(Boolean);
}

function findBodyPosition(index, bodyMap) {
  for (const key of bodyLookupKeys(index)) {
    const match = bodyMap.get(key);
    if (match) return match;
  }
  return null;
}

function renderBadge(label, value, tone = "neutral") {
  const bg =
    tone === "warm" ? "rgba(247, 211, 107, 0.16)" :
    tone === "hot" ? "rgba(255, 107, 107, 0.16)" :
    tone === "cool" ? "rgba(125, 211, 252, 0.16)" :
    "rgba(148, 163, 184, 0.12)";
  const fg =
    tone === "warm" ? "#f7d36b" :
    tone === "hot" ? "#ff8a8a" :
    tone === "cool" ? "#7dd3fc" :
    "#cbd5e1";
  return `<span class="ag-badge" style="background:${bg};color:${fg}">${escapeHtml(label)}: ${escapeHtml(value)}</span>`;
}

function canonicalBodyId(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase();
}

function canonicalPlanetName(body) {
  const candidates = [
    body?.raw?.planet,
    body?.name,
    body?.id,
  ];
  for (const candidate of candidates) {
    const key = canonicalBodyId(candidate);
    if (CANONICAL_PLANETS[key]) {
      return CANONICAL_PLANETS[key];
    }
  }
  return null;
}

function parseSnapshotDate(snapshot) {
  const raw = snapshot?.date || snapshot?.datetime || snapshot?.timestamp;
  const parsed = raw ? new Date(raw) : new Date();
  return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
}

function addHours(dt, hours) {
  return new Date(dt.getTime() + hours * 3600000);
}

function formatAxisNumber(value, digits = 2) {
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : "—";
}

function formatOffsetHours(offsetHours) {
  const offsetDays = offsetHours / 24;
  if (Math.abs(offsetHours) < 24) {
    return `${offsetHours > 0 ? "+" : ""}${offsetHours}h`;
  }
  const digits = Number.isInteger(offsetDays) ? 0 : 1;
  return `${offsetDays > 0 ? "+" : ""}${offsetDays.toFixed(digits)}d`;
}

function sampleTrajectory(body, centerDate, horizonDays = 14, stepHours = 12) {
  const planet = canonicalPlanetName(body);
  if (!planet) return [];

  const cacheKey = [
    planet,
    centerDate.toISOString(),
    horizonDays,
    stepHours,
  ].join(":");
  if (TRAJECTORY_CACHE.has(cacheKey)) {
    return TRAJECTORY_CACHE.get(cacheKey);
  }

  const totalHours = Math.max(stepHours, Math.round(horizonDays * 24));
  const samples = [];

  for (let offsetHours = -totalHours; offsetHours <= totalHours; offsetHours += stepHours) {
    const at = addHours(centerDate, offsetHours);
    const position = computePosition(planet, at);
    samples.push({
      at: at.toISOString(),
      offsetHours,
      offsetDays: offsetHours / 24,
      lon: normalizeAngle(pickBodyLongitude(position)),
      lat: pickBodyLatitude(position),
      ra: normalizeAngle(pickBodyRa(position) ?? pickBodyLongitude(position)),
      dec: pickBodyDec(position) ?? pickBodyLatitude(position),
      dist: pickBodyDistance(position),
      speed: pickBodySpeed(position),
      retrograde: Boolean(position.is_retrograde),
    });
  }

  TRAJECTORY_CACHE.set(cacheKey, samples);
  return samples;
}

function sampleKey(bodyId, sample) {
  return `${canonicalBodyId(bodyId)}:${sample.at}`;
}

function trajectoryYDomain(projection, series) {
  if (projection === "ecliptic") {
    const maxLat = series.reduce((maxValue, entry) => {
      const bodyMax = entry.samples.reduce(
        (innerMax, sample) => Math.max(innerMax, Math.abs(toNumber(sample.lat, 0))),
        0
      );
      return Math.max(maxValue, bodyMax);
    }, 0);
    const bound = Math.max(6, Math.min(24, Math.ceil((maxLat + 1.5) / 2) * 2));
    return {
      min: -bound,
      max: bound,
      ticks: [-bound, -bound / 2, 0, bound / 2, bound],
      label: "ecliptic latitude",
    };
  }

  return {
    min: -90,
    max: 90,
    ticks: [-60, -30, 0, 30, 60],
    label: "declination",
  };
}

function trajectoryXTicks(projection, marginLeft, innerWidth, height, marginBottom) {
  if (projection === "ecliptic") {
    return SIGN_NAMES.map((sign, index) => {
      const x = marginLeft + (index / 12) * innerWidth + innerWidth / 24;
      return `
        <g>
          <line x1="${x.toFixed(2)}" y1="28" x2="${x.toFixed(2)}" y2="${(height - marginBottom).toFixed(2)}" class="ag-trj-grid-line" />
          <text x="${x.toFixed(2)}" y="${(height - 10).toFixed(2)}" class="ag-trj-axis-label" text-anchor="middle">${escapeHtml(sign.slice(0, 3))}</text>
        </g>
      `;
    }).join("");
  }

  return Array.from({ length: 8 }, (_, index) => {
    const x = marginLeft + (index / 8) * innerWidth;
    return `
      <g>
        <line x1="${x.toFixed(2)}" y1="28" x2="${x.toFixed(2)}" y2="${(height - marginBottom).toFixed(2)}" class="ag-trj-grid-line" />
        <text x="${x.toFixed(2)}" y="${(height - 10).toFixed(2)}" class="ag-trj-axis-label" text-anchor="middle">${index * 3}h</text>
      </g>
    `;
  }).join("");
}

function projectionXValue(sample, projection) {
  return normalizeAngle(projection === "ecliptic" ? sample.lon : sample.ra);
}

function projectionYValue(sample, projection) {
  return projection === "ecliptic" ? sample.lat : sample.dec;
}

export function createRadialSky(snapshot) {
  const bodies = getBodies(snapshot);
  const bodyMap = bodyPositionMap(bodies);
  const lunar = getLunar(snapshot);
  const nakshatra = getNakshatra(snapshot);
  const signals = getSignals(snapshot);
  const width = 720;
  const height = 720;
  const cx = width / 2;
  const cy = height / 2;
  const outer = 300;
  const bodyRadius = 238;
  const ringRadius = 270;

  const signLabels = SIGN_NAMES.map((sign, index) => {
    const angle = index * 30 + 15;
    const p = polarToCartesian(cx, cy, ringRadius, angle);
    return `<text x="${p.x.toFixed(2)}" y="${p.y.toFixed(2)}" class="ag-sign" text-anchor="middle">${escapeHtml(sign)}</text>`;
  }).join("");

  const spokes = Array.from({ length: 12 }, (_, i) => {
    const angle = i * 30;
    const p1 = polarToCartesian(cx, cy, 56, angle);
    const p2 = polarToCartesian(cx, cy, outer, angle);
    return `<line x1="${p1.x.toFixed(2)}" y1="${p1.y.toFixed(2)}" x2="${p2.x.toFixed(2)}" y2="${p2.y.toFixed(2)}" class="ag-spoke" />`;
  }).join("");

  const ringMarks = Array.from({ length: 72 }, (_, i) => {
    const angle = i * 5;
    const p1 = polarToCartesian(cx, cy, outer - (i % 6 === 0 ? 18 : 8), angle);
    const p2 = polarToCartesian(cx, cy, outer, angle);
    return `<line x1="${p1.x.toFixed(2)}" y1="${p1.y.toFixed(2)}" x2="${p2.x.toFixed(2)}" y2="${p2.y.toFixed(2)}" class="ag-mark" />`;
  }).join("");

  const glyphs = bodies.map((body, index) => {
    const lon = normalizeAngle(pickBodyLongitude(body.raw));
    const lat = pickBodyLatitude(body.raw);
    const radius = bodyRadius - Math.min(24, Math.abs(lat) * 0.4);
    const p = polarToCartesian(cx, cy, radius, lon);
    const { sign, degree } = signFromLongitude(lon);
    const precision = body.raw?.precision || body.raw?.source_precision || body.raw?.accuracy || "";
    return `
      <g class="ag-body" transform="translate(${p.x.toFixed(2)} ${p.y.toFixed(2)})">
        <circle r="${body.raw?.visual_radius || 10}" class="ag-body-dot ${body.raw?.is_retrograde ? "retro" : ""}" />
        <text class="ag-glyph" text-anchor="middle" dy="4">${escapeHtml(bodyGlyph(body))}</text>
        <text class="ag-body-label" text-anchor="middle" dy="22">${escapeHtml(body.name)}</text>
        <title>${escapeHtml(body.name)} ${escapeHtml(sign)} ${degree.toFixed(2)}${precision ? ` · ${precision}` : ""}</title>
      </g>
    `;
  }).join("");

  const lunarText = lunar?.phase_name || lunar?.phase || lunar?.moon_phase || "—";
  const lunarIllum = lunar?.illumination != null ? `${Number(lunar.illumination).toFixed(1)}%` : "";
  const nakText = nakshatra?.nakshatra_name || nakshatra?.name || "—";

  return `
    <section class="ag-radial-sky">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="AstroGrid radial sky">
        <defs>
          <radialGradient id="ag-glow" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stop-color="rgba(125, 211, 252, 0.18)" />
            <stop offset="70%" stop-color="rgba(10, 15, 25, 0)" />
            <stop offset="100%" stop-color="rgba(10, 15, 25, 0)" />
          </radialGradient>
        </defs>
        <circle cx="${cx}" cy="${cy}" r="${outer}" class="ag-ring outer" />
        <circle cx="${cx}" cy="${cy}" r="${bodyRadius}" class="ag-ring inner" />
        <circle cx="${cx}" cy="${cy}" r="120" class="ag-ring core" />
        <circle cx="${cx}" cy="${cy}" r="72" class="ag-ring nucleus" />
        <circle cx="${cx}" cy="${cy}" r="330" fill="url(#ag-glow)" />
        ${spokes}
        ${ringMarks}
        ${signLabels}
        ${glyphs}
        <circle cx="${cx}" cy="${cy}" r="18" class="ag-center" />
        <text x="${cx}" y="${cy - 8}" class="ag-center-label" text-anchor="middle">${escapeHtml(snapshot?.title || "Sky")}</text>
        <text x="${cx}" y="${cy + 14}" class="ag-center-sub" text-anchor="middle">${escapeHtml(getDateLabel(snapshot))}</text>
      </svg>
      <div class="ag-radial-meta">
        ${renderBadge("Moon", lunarText, "cool")}
        ${lunarIllum ? renderBadge("Illum", lunarIllum, "warm") : ""}
        ${renderBadge("Nakshatra", nakText, "neutral")}
        ${signals.length ? renderBadge("Signals", String(signals.length), "hot") : ""}
      </div>
    </section>
  `;
}

export function createAspectField(snapshot) {
  const bodies = getBodies(snapshot);
  const bodyMap = bodyPositionMap(bodies);
  const aspects = getAspects(snapshot);
  const width = 720;
  const height = 480;
  const cx = 360;
  const cy = 240;
  const radius = 168;

  const nodes = bodies.map((body) => {
    const pos = findBodyPosition(body, bodyMap);
    if (!pos) return null;
    const lon = normalizeAngle(pos.lon);
    const p = polarToCartesian(cx, cy, radius, lon);
    return { ...pos, x: p.x, y: p.y };
  }).filter(Boolean);

  const lines = aspects.map((aspect, index) => {
    const p1 = bodyMap.get(aspect.planet1?.toLowerCase?.()) || bodyMap.get(aspect.planet1) || null;
    const p2 = bodyMap.get(aspect.planet2?.toLowerCase?.()) || bodyMap.get(aspect.planet2) || null;
    const n1 = p1 ? polarToCartesian(cx, cy, radius, normalizeAngle(p1.lon)) : null;
    const n2 = p2 ? polarToCartesian(cx, cy, radius, normalizeAngle(p2.lon)) : null;
    if (!n1 || !n2) return null;
    const color = aspectColor(aspect.type);
    const strength = aspectStrength(aspect.orb);
    const opacity = 0.18 + strength * 0.72;
    return `<line x1="${n1.x.toFixed(2)}" y1="${n1.y.toFixed(2)}" x2="${n2.x.toFixed(2)}" y2="${n2.y.toFixed(2)}" stroke="${color}" stroke-width="${(1 + strength * 2.2).toFixed(2)}" stroke-opacity="${opacity.toFixed(3)}" class="ag-aspect ${aspect.applying ? "applying" : "separating"}" data-type="${escapeHtml(aspect.type)}" />`;
  }).filter(Boolean).join("");

  const nodesSvg = nodes.map((node) => {
    const { sign, degree } = signFromLongitude(node.lon);
    return `
      <g transform="translate(${node.x.toFixed(2)} ${node.y.toFixed(2)})" class="ag-aspect-node">
        <circle r="9" class="ag-node-dot ${node.body.raw?.is_retrograde ? "retro" : ""}" />
        <text class="ag-node-label" text-anchor="middle" dy="22">${escapeHtml(node.body.name)}</text>
        <title>${escapeHtml(node.body.name)} ${escapeHtml(sign)} ${degree.toFixed(2)}</title>
      </g>
    `;
  }).join("");

  const rows = aspects.map((aspect) => {
    const color = aspectColor(aspect.type);
    const orb = aspect.orb != null ? `${aspect.orb.toFixed(2)}°` : "—";
    return `
      <div class="ag-aspect-row">
        <span class="ag-aspect-type" style="color:${color}">${escapeHtml(aspect.type)}</span>
        <span class="ag-aspect-bodies">${escapeHtml(aspect.planet1)} ${escapeHtml(aspect.applying ? "→" : "↔")} ${escapeHtml(aspect.planet2)}</span>
        <span class="ag-aspect-orb">${escapeHtml(orb)}</span>
      </div>
    `;
  }).join("");

  return `
    <section class="ag-aspect-field">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="AstroGrid aspect field">
        <circle cx="${cx}" cy="${cy}" r="${radius}" class="ag-aspect-ring" />
        ${lines}
        ${nodesSvg}
      </svg>
      <div class="ag-aspect-list">
        ${rows || `<div class="ag-empty">No aspects loaded.</div>`}
      </div>
    </section>
  `;
}

export function createSpacetimeLattice(snapshot) {
  const bodies = getBodies(snapshot);
  const aspects = getAspects(snapshot);
  const width = 900;
  const height = 460;
  const padX = 58;
  const padY = 34;
  const plotWidth = width - padX * 2;
  const plotHeight = height - padY * 2;

  const plotted = bodies.map((body) => {
    const raw = body.raw || {};
    const ra = normalizeAngle(pickBodyRa(raw) ?? pickBodyLongitude(raw));
    const dec = Math.max(-90, Math.min(90, pickBodyDec(raw) ?? pickBodyLatitude(raw)));
    const speed = pickBodySpeed(raw) ?? 0;
    const dist = pickBodyDistance(raw);
    const x = padX + (ra / 360) * plotWidth;
    const y = padY + ((90 - dec) / 180) * plotHeight;
    const trail = Math.min(82, Math.max(12, Math.abs(speed) * 18));
    const drift = speed >= 0 ? trail : -trail;
    const halo = dist == null ? 18 : Math.max(12, Math.min(38, 28 - Math.log10(Math.max(dist, 0.0001) * 10 + 1) * 10));
    const { sign, degree } = signFromLongitude(pickBodyLongitude(raw));
    return { body, raw, ra, dec, speed, dist, x, y, trail, drift, halo, sign, degree };
  });

  const plotMap = new Map();
  for (const point of plotted) {
    plotMap.set(String(point.body.id).toLowerCase(), point);
    plotMap.set(String(point.body.name).toLowerCase(), point);
    if (point.raw?.planet) {
      plotMap.set(String(point.raw.planet).toLowerCase(), point);
    }
  }

  const raTicks = Array.from({ length: 8 }, (_, index) => {
    const raDeg = index * 45;
    const x = padX + (raDeg / 360) * plotWidth;
    return `
      <g>
        <line x1="${x.toFixed(2)}" y1="${padY}" x2="${x.toFixed(2)}" y2="${(height - padY).toFixed(2)}" class="ag-space-grid-line" />
        <text x="${x.toFixed(2)}" y="20" class="ag-space-axis-label" text-anchor="middle">${index * 3}h</text>
      </g>
    `;
  }).join("");

  const decTicks = [-60, -30, 0, 30, 60].map((dec) => {
    const y = padY + ((90 - dec) / 180) * plotHeight;
    return `
      <g>
        <line x1="${padX}" y1="${y.toFixed(2)}" x2="${(width - padX).toFixed(2)}" y2="${y.toFixed(2)}" class="ag-space-band-line" />
        <text x="16" y="${(y + 4).toFixed(2)}" class="ag-space-axis-label">${dec}°</text>
      </g>
    `;
  }).join("");

  const aspectThreads = aspects.map((aspect, index) => {
    const from = plotMap.get(String(aspect.planet1 || "").toLowerCase());
    const to = plotMap.get(String(aspect.planet2 || "").toLowerCase());
    if (!from || !to) return "";
    const color = aspectColor(aspect.type);
    const strength = aspectStrength(aspect.orb);
    const midX = (from.x + to.x) / 2;
    const midY = Math.min(from.y, to.y) - 18 - (index % 3) * 12;
    return `<path d="M ${from.x.toFixed(2)} ${from.y.toFixed(2)} Q ${midX.toFixed(2)} ${midY.toFixed(2)} ${to.x.toFixed(2)} ${to.y.toFixed(2)}" stroke="${color}" stroke-width="${(0.8 + strength * 1.6).toFixed(2)}" stroke-opacity="${(0.14 + strength * 0.26).toFixed(3)}" fill="none" class="ag-space-thread" />`;
  }).join("");

  const bodyMarks = plotted.map((point) => {
    const retro = point.raw?.is_retrograde ? "retro" : "";
    const klass = escapeHtml(point.raw?.class || point.raw?.body_class || "body");
    const distanceLabel = point.dist != null ? `${Number(point.dist).toFixed(4)} AU` : "distance n/a";
    const speedLabel = point.speed != null ? `${Number(point.speed).toFixed(3)}°/day` : "speed n/a";
    return `
      <g class="ag-space-body ${retro}" data-class="${klass}">
        <line x1="${point.x.toFixed(2)}" y1="${point.y.toFixed(2)}" x2="${(point.x + point.drift).toFixed(2)}" y2="${point.y.toFixed(2)}" class="ag-space-trail ${retro}" />
        <circle cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="${point.halo.toFixed(2)}" class="ag-space-halo ${retro}" />
        <circle cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="7.5" class="ag-space-node ${retro}" />
        <text x="${point.x.toFixed(2)}" y="${(point.y - 14).toFixed(2)}" class="ag-space-glyph" text-anchor="middle">${escapeHtml(bodyGlyph(point.body))}</text>
        <text x="${point.x.toFixed(2)}" y="${(point.y + 26).toFixed(2)}" class="ag-space-body-label" text-anchor="middle">${escapeHtml(point.body.name)}</text>
        <title>${escapeHtml(point.body.name)} · RA ${point.ra.toFixed(2)}° · Dec ${point.dec.toFixed(2)}° · ${escapeHtml(point.sign)} ${point.degree.toFixed(2)} · ${escapeHtml(speedLabel)} · ${escapeHtml(distanceLabel)}</title>
      </g>
    `;
  }).join("");

  const nearest = plotted
    .slice()
    .sort((a, b) => (a.dist ?? 999) - (b.dist ?? 999))
    .slice(0, 3)
    .map((point) => point.body.name)
    .join(", ");

  return `
    <section class="ag-spacetime-lattice">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="AstroGrid spacetime lattice">
        <defs>
          <linearGradient id="ag-space-backdrop" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stop-color="rgba(136, 182, 201, 0.08)" />
            <stop offset="55%" stop-color="rgba(184, 146, 77, 0.05)" />
            <stop offset="100%" stop-color="rgba(217, 107, 67, 0.08)" />
          </linearGradient>
        </defs>
        <rect x="${padX}" y="${padY}" width="${plotWidth}" height="${plotHeight}" class="ag-space-frame" fill="url(#ag-space-backdrop)" />
        ${raTicks}
        ${decTicks}
        <line x1="${padX}" y1="${(padY + plotHeight / 2).toFixed(2)}" x2="${(width - padX).toFixed(2)}" y2="${(padY + plotHeight / 2).toFixed(2)}" class="ag-space-equator" />
        ${aspectThreads}
        ${bodyMarks}
        <text x="${padX}" y="${(height - 8)}" class="ag-space-axis-label">Right ascension</text>
        <text x="${(width - padX).toFixed(2)}" y="${(height - 8)}" class="ag-space-axis-label" text-anchor="end">Declination field</text>
      </svg>
      <div class="ag-radial-meta">
        ${renderBadge("Frame", "RA/Dec", "cool")}
        ${renderBadge("Bodies", String(bodies.length), "warm")}
        ${renderBadge("Aspects", String(aspects.length), "hot")}
        ${nearest ? renderBadge("Nearest", nearest, "neutral") : ""}
      </div>
    </section>
  `;
}

function buildWrappedWorldline(points, width) {
  if (!points.length) return "";
  const segments = [];
  let current = [points[0]];

  for (let i = 1; i < points.length; i += 1) {
    const point = points[i];
    const prev = current[current.length - 1];
    if (Math.abs(point.x - prev.x) > width * 0.45) {
      if (current.length > 1) segments.push(current);
      current = [point];
      continue;
    }
    current.push(point);
  }

  if (current.length > 1) {
    segments.push(current);
  }

  return segments.map((segment) => segment.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ")).join(" ");
}

export function createTrajectoryAtlas(snapshot, options = {}) {
  const bodies = getBodies(snapshot);
  const projection = options.projection === "ecliptic" ? "ecliptic" : "radec";
  const horizonDays = Math.max(3, Math.min(45, toNumber(options.horizonDays, 14)));
  const stepHours = Math.max(6, Math.min(24, toNumber(options.stepHours, 12)));
  const focusedBodyId = canonicalBodyId(options.focusedBodyId || "moon");
  const selectedSampleKey = String(options.selectedSampleKey || "");
  const centerDate = parseSnapshotDate(snapshot);

  const series = bodies
    .map((body) => {
      const bodyId = canonicalBodyId(body.id || body.name);
      const samples = sampleTrajectory(body, centerDate, horizonDays, stepHours);
      if (!samples.length) return null;
      const current =
        samples.find((sample) => sample.offsetHours === 0) ||
        samples[Math.floor(samples.length / 2)] ||
        null;
      return {
        body,
        bodyId,
        samples,
        current,
        color: bodyColor(body),
        active: focusedBodyId === "all" || bodyId === focusedBodyId,
      };
    })
    .filter(Boolean);

  const width = 920;
  const height = 500;
  const margin = { top: 32, right: 22, bottom: 38, left: 64 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const yDomain = trajectoryYDomain(projection, series);
  const xFor = (value) => margin.left + (normalizeAngle(value) / 360) * innerWidth;
  const yFor = (value) => {
    const ratio = (yDomain.max - value) / (yDomain.max - yDomain.min || 1);
    return margin.top + ratio * innerHeight;
  };

  const xTicks = trajectoryXTicks(projection, margin.left, innerWidth, height, margin.bottom);
  const yTicks = yDomain.ticks.map((value) => {
    const y = yFor(value);
    return `
      <g>
        <line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${(width - margin.right).toFixed(2)}" y2="${y.toFixed(2)}" class="ag-trj-band-line ${Math.abs(value) < 0.001 ? "mid" : ""}" />
        <text x="14" y="${(y + 4).toFixed(2)}" class="ag-trj-axis-label">${value > 0 ? "+" : ""}${escapeHtml(formatAxisNumber(value, 1))}°</text>
      </g>
    `;
  }).join("");

  const paths = series.map((entry) => {
    const points = entry.samples.map((sample) => ({
      x: xFor(projectionXValue(sample, projection)),
      y: yFor(projectionYValue(sample, projection)),
    }));
    const d = buildWrappedWorldline(points, innerWidth);
    if (!d) return "";
    const opacity = entry.active ? 0.88 : 0.18;
    const strokeWidth = entry.active ? 2.6 : 1.4;
    return `<path d="${d}" class="ag-trj-path ${entry.active ? "active" : "muted"}" stroke="${entry.color}" stroke-width="${strokeWidth}" stroke-opacity="${opacity.toFixed(2)}" fill="none" />`;
  }).join("");

  const samplesMarkup = series.map((entry) => {
    const cadence = entry.active ? 1 : Math.max(1, Math.round(24 / stepHours));
    return entry.samples.map((sample, index) => {
      if (sample.offsetHours !== 0 && !entry.active && index % cadence !== 0) {
        return "";
      }
      const x = xFor(projectionXValue(sample, projection));
      const y = yFor(projectionYValue(sample, projection));
      const key = sampleKey(entry.bodyId, sample);
      const stateClass =
        sample.offsetHours === 0 ? "current" :
        sample.offsetHours < 0 ? "past" :
        "future";
      const radius = sample.offsetHours === 0 ? 5 : entry.active ? 2.4 : 1.5;
      const speedLabel = sample.speed != null ? `${formatAxisNumber(sample.speed, 3)}°/d` : "—";
      const distanceLabel = sample.dist != null ? `${formatAxisNumber(sample.dist, 4)} AU` : "—";
      return `
        <circle
          cx="${x.toFixed(2)}"
          cy="${y.toFixed(2)}"
          r="${radius}"
          fill="${entry.color}"
          class="ag-trj-sample ${stateClass} ${entry.active ? "active" : "muted"} ${key === selectedSampleKey ? "selected" : ""}"
          data-trajectory-sample="1"
          data-body-id="${escapeHtml(entry.bodyId)}"
          data-body-name="${escapeHtml(entry.body.name)}"
          data-at="${escapeHtml(sample.at)}"
          data-offset-hours="${escapeHtml(String(sample.offsetHours))}"
          data-lon="${escapeHtml(formatAxisNumber(sample.lon, 4))}"
          data-lat="${escapeHtml(formatAxisNumber(sample.lat, 4))}"
          data-ra="${escapeHtml(formatAxisNumber(sample.ra, 4))}"
          data-dec="${escapeHtml(formatAxisNumber(sample.dec, 4))}"
          data-dist="${escapeHtml(sample.dist != null ? formatAxisNumber(sample.dist, 6) : "—")}"
          data-speed="${escapeHtml(sample.speed != null ? formatAxisNumber(sample.speed, 4) : "—")}"
          data-retrograde="${sample.retrograde ? "true" : "false"}"
        >
          <title>${escapeHtml(entry.body.name)} · ${escapeHtml(formatOffsetHours(sample.offsetHours))} · lon ${escapeHtml(formatAxisNumber(sample.lon, 2))}° · lat ${escapeHtml(formatAxisNumber(sample.lat, 2))}° · RA ${escapeHtml(formatAxisNumber(sample.ra, 2))}° · Dec ${escapeHtml(formatAxisNumber(sample.dec, 2))}° · ${escapeHtml(speedLabel)} · ${escapeHtml(distanceLabel)}</title>
        </circle>
      `;
    }).join("");
  }).join("");

  const currentLabels = series.map((entry, index) => {
    if (!entry.current) return "";
    const x = xFor(projectionXValue(entry.current, projection));
    const y = yFor(projectionYValue(entry.current, projection));
    const labelDy = entry.active ? -14 : 18 + (index % 2) * 10;
    return `
      <g class="ag-trj-current ${entry.active ? "active" : "muted"}">
        <circle cx="${x.toFixed(2)}" cy="${y.toFixed(2)}" r="${entry.active ? 8 : 5.5}" class="ag-trj-current-ring" stroke="${entry.color}" />
        <text x="${x.toFixed(2)}" y="${(y + labelDy).toFixed(2)}" class="ag-trj-label" text-anchor="middle">${escapeHtml(entry.body.name)}</text>
      </g>
    `;
  }).join("");

  const activeCount = series.filter((entry) => entry.active).length;

  return `
    <section class="ag-trajectory">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="AstroGrid trajectory atlas">
        <defs>
          <linearGradient id="ag-trj-backdrop" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stop-color="rgba(136, 182, 201, 0.08)" />
            <stop offset="60%" stop-color="rgba(184, 146, 77, 0.05)" />
            <stop offset="100%" stop-color="rgba(217, 107, 67, 0.08)" />
          </linearGradient>
        </defs>
        <rect x="${margin.left}" y="${margin.top}" width="${innerWidth}" height="${innerHeight}" class="ag-trj-frame" fill="url(#ag-trj-backdrop)" />
        ${xTicks}
        ${yTicks}
        ${paths}
        ${samplesMarkup}
        ${currentLabels}
        <text x="${margin.left}" y="18" class="ag-trj-axis-label">${escapeHtml(projection === "ecliptic" ? "ecliptic longitude" : "right ascension")}</text>
        <text x="${(width - margin.right).toFixed(2)}" y="18" class="ag-trj-axis-label" text-anchor="end">${escapeHtml(yDomain.label)}</text>
      </svg>
      <div class="ag-spacetime-meta">
        ${renderBadge("Vector", projection === "ecliptic" ? "lon/lat" : "RA/Dec", "cool")}
        ${renderBadge("Window", `±${horizonDays}d`, "warm")}
        ${renderBadge("Cadence", `${stepHours}h`, "neutral")}
        ${renderBadge("Focus", focusedBodyId === "all" ? `${activeCount}/${series.length}` : focusedBodyId, focusedBodyId === "all" ? "neutral" : "hot")}
      </div>
      <div class="ag-spacetime-note">Sampled ephemeris path. Click a mark for a fixed readout.</div>
    </section>
  `;
}

export function createSpacetimeField(snapshot) {
  const bodies = getBodies(snapshot)
    .map((body) => {
      const raw = body.raw || {};
      return {
        ...body,
        lon: normalizeAngle(pickBodyLongitude(raw)),
        lat: pickBodyLatitude(raw),
        dec: toNumber(raw?.declination, 0),
        speed: pickBodySpeed(raw) ?? 0,
        retrograde: Boolean(raw?.is_retrograde ?? raw?.retrograde),
        cls: raw?.class || "body",
      };
    })
    .filter((body) => Number.isFinite(body.lon));

  const width = 860;
  const height = 430;
  const margin = { top: 28, right: 20, bottom: 42, left: 64 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const offsets = Array.from({ length: 13 }, (_, index) => index - 6);
  const yForOffset = (offset) => margin.top + ((offset + 6) / 12) * innerHeight;
  const xForLongitude = (lon) => margin.left + (normalizeAngle(lon) / 360) * innerWidth;

  const zodiacBands = SIGN_NAMES.map((sign, index) => {
    const x = margin.left + (index / 12) * innerWidth;
    const bandWidth = innerWidth / 12;
    const active = index % 2 === 0;
    return `
      <g>
        <rect x="${x.toFixed(2)}" y="${margin.top}" width="${bandWidth.toFixed(2)}" height="${innerHeight}" class="ag-st-band ${active ? "odd" : "even"}" />
        <text x="${(x + bandWidth / 2).toFixed(2)}" y="${(height - 14).toFixed(2)}" class="ag-st-sign" text-anchor="middle">${escapeHtml(sign.slice(0, 3))}</text>
      </g>
    `;
  }).join("");

  const timeRows = offsets.map((offset) => {
    const y = yForOffset(offset);
    const label = offset === 0 ? "now" : `${offset > 0 ? "+" : ""}${offset}d`;
    return `
      <g>
        <line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${(width - margin.right).toFixed(2)}" y2="${y.toFixed(2)}" class="ag-st-row ${offset === 0 ? "current" : ""}" />
        <text x="${(margin.left - 14).toFixed(2)}" y="${(y + 4).toFixed(2)}" class="ag-st-time" text-anchor="end">${escapeHtml(label)}</text>
      </g>
    `;
  }).join("");

  const meridians = Array.from({ length: 13 }, (_, index) => {
    const x = margin.left + (index / 12) * innerWidth;
    return `<line x1="${x.toFixed(2)}" y1="${margin.top}" x2="${x.toFixed(2)}" y2="${(height - margin.bottom).toFixed(2)}" class="ag-st-meridian" />`;
  }).join("");

  const worldlines = bodies.map((body, index) => {
    const color = bodyColor(body);
    const points = offsets.map((offset) => ({
      x: xForLongitude(body.lon + body.speed * offset),
      y: yForOffset(offset),
    }));
    const path = buildWrappedWorldline(points, innerWidth);
    const currentPoint = points[offsets.indexOf(0)];
    const strokeWidth = body.cls === "luminary" ? 3.2 : body.cls === "node" ? 2.5 : 2.1;
    const opacity = body.retrograde ? 0.84 : 0.72;
    const drift = body.speed >= 0 ? `+${body.speed.toFixed(2)}°/d` : `${body.speed.toFixed(2)}°/d`;
    const labelDy = 16 + (index % 3) * 12;

    return `
      <g class="ag-st-body">
        ${path ? `<path d="${path}" class="ag-st-line ${body.retrograde ? "retro" : ""}" stroke="${color}" stroke-width="${strokeWidth}" stroke-opacity="${opacity.toFixed(2)}" />` : ""}
        <circle cx="${currentPoint.x.toFixed(2)}" cy="${currentPoint.y.toFixed(2)}" r="${body.cls === "luminary" ? 5.2 : 4.2}" fill="${color}" class="ag-st-point ${body.retrograde ? "retro" : ""}" />
        <text x="${currentPoint.x.toFixed(2)}" y="${(currentPoint.y - labelDy).toFixed(2)}" class="ag-st-label" text-anchor="middle">${escapeHtml(body.name)}</text>
        <title>${escapeHtml(body.name)} ${escapeHtml(drift)} · ${escapeHtml(body.retrograde ? "retrograde" : "direct")}</title>
      </g>
    `;
  }).join("");

  const retrogradeCount = bodies.filter((body) => body.retrograde).length;

  return `
    <section class="ag-spacetime">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="AstroGrid spacetime projection">
        <defs>
          <linearGradient id="ag-st-glow" x1="0%" x2="100%" y1="0%" y2="0%">
            <stop offset="0%" stop-color="rgba(125, 211, 252, 0.08)" />
            <stop offset="50%" stop-color="rgba(184, 146, 77, 0.12)" />
            <stop offset="100%" stop-color="rgba(217, 107, 67, 0.08)" />
          </linearGradient>
        </defs>
        <rect x="${margin.left}" y="${margin.top}" width="${innerWidth}" height="${innerHeight}" fill="url(#ag-st-glow)" class="ag-st-frame" />
        ${zodiacBands}
        ${meridians}
        ${timeRows}
        ${worldlines}
      </svg>
      <div class="ag-spacetime-meta">
        ${renderBadge("Projection", "lon x time", "cool")}
        ${renderBadge("Horizon", "±6d", "warm")}
        ${renderBadge("Bodies", String(bodies.length), "neutral")}
        ${renderBadge("Retro", String(retrogradeCount), retrogradeCount ? "hot" : "neutral")}
      </div>
      <div class="ag-spacetime-note">Linear worldline projection from current geocentric longitude and daily motion.</div>
    </section>
  `;
}

function worldNodePaint(node) {
  return WORLD_NODE_COLORS[node?.type] || WORLD_NODE_COLORS.default;
}

function worldEdgeColor(edge) {
  return WORLD_EDGE_COLORS[edge?.type] || WORLD_EDGE_COLORS.default;
}

function worldNodeRadius(node) {
  if (node?.type === "star") return 18;
  if (node?.type === "planet") return 14;
  if (node?.type === "moon") return 12;
  if (node?.type === "orbit") return 11;
  if (node?.type === "surface") return 10;
  if (node?.type === "corridor") return 10;
  return 9;
}

function worldLayoutPointMap() {
  return {
    sun: { x: 110, y: 190, kicker: "source" },
    earth: { x: 278, y: 190, kicker: "anchor" },
    earth_surface: { x: 278, y: 304, kicker: "surface" },
    leo: { x: 420, y: 144, kicker: "orbital shell" },
    geo: { x: 420, y: 244, kicker: "relay shell" },
    cislunar_space: { x: 612, y: 190, kicker: "transfer lane" },
    moon: { x: 744, y: 146, kicker: "body" },
    lunar_surface: { x: 744, y: 304, kicker: "surface" },
    mars: { x: 946, y: 190, kicker: "body" },
    mars_surface: { x: 946, y: 304, kicker: "surface" },
  };
}

function worldEdgePath(edge, source, target, index) {
  const midX = (source.x + target.x) / 2;
  if (edge.type === "telemetry") {
    const controlY = Math.max(source.y, target.y) + 56 + (index % 2) * 18;
    return `M ${source.x} ${source.y} Q ${midX} ${controlY} ${target.x} ${target.y}`;
  }
  if (edge.type === "mass") {
    const controlY = Math.min(source.y, target.y) - 26 - (index % 2) * 10;
    return `M ${source.x} ${source.y} Q ${midX} ${controlY} ${target.x} ${target.y}`;
  }
  const controlY = Math.min(source.y, target.y) - 52 - (index % 3) * 14;
  return `M ${source.x} ${source.y} Q ${midX} ${controlY} ${target.x} ${target.y}`;
}

function worldEdgeLabelPoint(edge, source, target, index) {
  const midX = (source.x + target.x) / 2;
  if (edge.type === "telemetry") {
    return { x: midX, y: Math.max(source.y, target.y) + 44 + (index % 2) * 18 };
  }
  if (edge.type === "mass") {
    return { x: midX, y: Math.min(source.y, target.y) - 18 - (index % 2) * 10 };
  }
  return { x: midX, y: Math.min(source.y, target.y) - 40 - (index % 3) * 14 };
}

export function createWorldAtlas(worldModel, options = {}) {
  const world = worldModel || { nodes: [], edges: [], layerStack: [] };
  const width = 1080;
  const height = 390;
  const points = worldLayoutPointMap();
  const nodes = (world.nodes || []).filter((node) => points[node.id]);
  const selectedNodeId = canonicalBodyId(options.selectedNodeId || "");
  const selectedEdgeKeys = new Set(
    selectedNodeId
      ? (world.edges || [])
          .filter((edge) => canonicalBodyId(edge.source) === selectedNodeId || canonicalBodyId(edge.target) === selectedNodeId)
          .map((edge) => edge.id)
      : []
  );

  const bands = WORLD_SCALE_BANDS.map((band) => `
    <g class="ag-world-band-group">
      <rect
        x="${band.x}"
        y="${band.y}"
        width="${band.width}"
        height="${band.height}"
        class="ag-world-band"
        data-scale="${escapeHtml(band.id)}"
      />
      <text x="${band.x + 14}" y="${band.y + 22}" class="ag-world-band-label">${escapeHtml(band.label)}</text>
    </g>
  `).join("");

  const orbitShells = `
    <ellipse cx="278" cy="190" rx="132" ry="78" class="ag-world-orbit" />
    <ellipse cx="278" cy="190" rx="172" ry="112" class="ag-world-orbit faint" />
    <ellipse cx="744" cy="146" rx="84" ry="46" class="ag-world-orbit moon" />
  `;

  const edges = (world.edges || []).map((edge, index) => {
    const source = points[edge.source];
    const target = points[edge.target];
    if (!source || !target) return "";
    const color = worldEdgeColor(edge);
    const path = worldEdgePath(edge, source, target, index);
    const labelPoint = worldEdgeLabelPoint(edge, source, target, index);
    const label = edge?.meta?.label || edge.type;
    const tail = edge.currency || edge.unit || edge.quantityKind || "";
    const dash =
      edge.type === "telemetry" ? "7 9" :
      edge.type === "mass" ? "2 0" :
      "3 0";
    const active = selectedEdgeKeys.size ? selectedEdgeKeys.has(edge.id) : true;
    return `
      <g class="ag-world-edge-group ${escapeHtml(edge.type || "default")}">
        <path
          d="${path}"
          class="ag-world-edge ${escapeHtml(edge.type || "default")} ${active ? "active" : "muted"}"
          stroke="${color}"
          stroke-dasharray="${dash}"
        >
          <title>${escapeHtml(label)} · ${escapeHtml(edge.metrics?.headline || edge.type)} · ${escapeHtml(edge.metrics?.detail || tail || "unscored")}</title>
        </path>
        <text x="${labelPoint.x}" y="${labelPoint.y}" class="ag-world-edge-label" text-anchor="middle">
          ${escapeHtml(label)}${tail ? ` · ${escapeHtml(tail)}` : ""}
        </text>
      </g>
    `;
  }).join("");

  const nodeMarkup = nodes.map((node) => {
    const point = points[node.id];
    const paint = worldNodePaint(node);
    const radius = worldNodeRadius(node);
    const tags = Array.isArray(node.tags) && node.tags.length ? node.tags.join(" / ") : "No tags";
    const isSelected = selectedNodeId && canonicalBodyId(node.id) === selectedNodeId;
    const headline = node.metrics?.headline || node.type;
    const detail = node.metrics?.detail || tags;
    return `
      <g class="ag-world-node-group ${isSelected ? "selected" : ""}" transform="translate(${point.x} ${point.y})" data-world-node="${escapeHtml(node.id)}">
        <circle r="${radius + 10}" class="ag-world-node-halo ${escapeHtml(node.type || "default")} ${isSelected ? "selected" : ""}" />
        <circle r="${radius}" class="ag-world-node ${escapeHtml(node.type || "default")} ${isSelected ? "selected" : ""}" fill="${paint.fill}" stroke="${paint.stroke}" />
        <text y="${radius + 21}" class="ag-world-label" text-anchor="middle">${escapeHtml(node.name)}</text>
        <text y="${radius + 36}" class="ag-world-kicker" text-anchor="middle">${escapeHtml(point.kicker || node.scale || node.type)}</text>
        <title>${escapeHtml(node.name)} · ${escapeHtml(headline)} · ${escapeHtml(detail)}</title>
      </g>
    `;
  }).join("");

  const capitalEdges = (world.edges || []).filter((edge) => edge.type === "capital").length;

  return `
    <section class="ag-world-atlas">
      <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="AstroGrid world atlas">
        <defs>
          <linearGradient id="ag-world-band-glow" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="rgba(136, 182, 201, 0.05)" />
            <stop offset="50%" stop-color="rgba(184, 146, 77, 0.06)" />
            <stop offset="100%" stop-color="rgba(217, 107, 67, 0.05)" />
          </linearGradient>
        </defs>
        <rect x="20" y="36" width="1040" height="328" class="ag-world-frame" fill="url(#ag-world-band-glow)" />
        ${bands}
        ${orbitShells}
        ${edges}
        ${nodeMarkup}
      </svg>
      <div class="ag-radial-meta">
        ${renderBadge("Nodes", String(nodes.length), "cool")}
        ${renderBadge("Flows", String((world.edges || []).length), "warm")}
        ${renderBadge("Capital", String(capitalEdges), "hot")}
        ${renderBadge("Layers", String((world.layerStack || []).length), "neutral")}
      </div>
      <div class="ag-spacetime-note">Earth surface to orbital shell to cislunar corridor to lunar and martian surface. Capital and telemetry ride separate lines.</div>
    </section>
  `;
}

export function createObjectTable(snapshot) {
  const bodies = getBodies(snapshot);
  const rows = bodies.map((body) => {
    const raw = body.raw || {};
    const lon = pickBodyLongitude(raw);
    const lat = pickBodyLatitude(raw);
    const speed = pickBodySpeed(raw);
    const dist = pickBodyDistance(raw);
    const ra = pickBodyRa(raw);
    const dec = pickBodyDec(raw);
    const { sign, degree } = signFromLongitude(lon);
    const precision = raw.precision || raw.source_precision || raw.accuracy || "—";
    const retro = raw.is_retrograde ? "Rx" : "Direct";
    return `
      <tr>
        <td>${escapeHtml(body.name)}</td>
        <td>${escapeHtml(sign)} ${degree.toFixed(2)}</td>
        <td>${lon.toFixed(4)}</td>
        <td>${lat.toFixed(4)}</td>
        <td>${ra != null ? Number(ra).toFixed(4) : "—"}</td>
        <td>${dec != null ? Number(dec).toFixed(4) : "—"}</td>
        <td>${dist != null ? Number(dist).toFixed(6) : "—"}</td>
        <td>${speed != null ? Number(speed).toFixed(4) : "—"}</td>
        <td>${escapeHtml(retro)}</td>
        <td>${escapeHtml(precision)}</td>
      </tr>
    `;
  }).join("");

  return `
    <section class="ag-object-table">
      <table>
        <thead>
          <tr>
            <th>Body</th>
            <th>Sign</th>
            <th>Lon</th>
            <th>Lat</th>
            <th>RA</th>
            <th>Dec</th>
            <th>Dist</th>
            <th>Speed</th>
            <th>Status</th>
            <th>Prec</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="10" class="ag-empty">No tracked bodies.</td></tr>`}
        </tbody>
      </table>
    </section>
  `;
}

export function summarizeSky(snapshot) {
  const bodies = getBodies(snapshot);
  const aspects = getAspects(snapshot);
  const lunar = getLunar(snapshot);
  const nakshatra = getNakshatra(snapshot);
  const signals = getSignals(snapshot);
  const date = getDateLabel(snapshot);
  const moonLabel = lunar?.phase_name || lunar?.phase || lunar?.moon_phase || "Unknown";
  const illum = lunar?.illumination != null ? `${Number(lunar.illumination).toFixed(1)}%` : "—";
  const nakLabel = nakshatra?.nakshatra_name || nakshatra?.name || "—";
  const retrogradeCount = bodies.filter((body) => body.raw?.is_retrograde).length;
  const activeSignals = signals.length;

  return `
    <section class="ag-summary">
      <div class="ag-summary-head">
        <div class="ag-summary-title">${escapeHtml(snapshot?.title || "AstroGrid Observatory")}</div>
        <div class="ag-summary-date">${escapeHtml(date)}</div>
      </div>
      <div class="ag-summary-grid">
        <div class="ag-summary-card">
          <div class="ag-kicker">Lunar</div>
          <div class="ag-value">${escapeHtml(moonLabel)}</div>
          <div class="ag-sub">${escapeHtml(illum)} illuminated</div>
        </div>
        <div class="ag-summary-card">
          <div class="ag-kicker">Nakshatra</div>
          <div class="ag-value">${escapeHtml(nakLabel)}</div>
          <div class="ag-sub">${nakshatra?.pada != null ? `Pada ${escapeHtml(nakshatra.pada)}` : "—"}</div>
        </div>
        <div class="ag-summary-card">
          <div class="ag-kicker">Bodies</div>
          <div class="ag-value">${bodies.length}</div>
          <div class="ag-sub">${retrogradeCount} retrograde</div>
        </div>
        <div class="ag-summary-card">
          <div class="ag-kicker">Aspects</div>
          <div class="ag-value">${aspects.length}</div>
          <div class="ag-sub">${activeSignals} signals</div>
        </div>
      </div>
    </section>
  `;
}
