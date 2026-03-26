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

function getBodies(snapshot) {
  const raw = snapshot?.bodies ?? snapshot?.positions ?? snapshot?.planets ?? {};
  if (Array.isArray(raw)) {
    return raw.map((body, index) => ({
      id: body?.id || body?.name || `body-${index + 1}`,
      name: body?.name || body?.id || `Body ${index + 1}`,
      raw: body,
    }));
  }

  if (raw && typeof raw === "object") {
    return Object.entries(raw).map(([id, body]) => ({
      id,
      name: body?.name || id,
      raw: body,
    }));
  }

  return [];
}

function getAspects(snapshot) {
  const raw = snapshot?.aspects ?? [];
  if (!Array.isArray(raw)) return [];
  return raw.map((aspect, index) => {
    const type = aspect?.aspect_type || aspect?.type || "default";
    const orb = toNumber(aspect?.orb_used ?? aspect?.orb ?? aspect?.orbUsed, null);
    return {
      id: aspect?.id || `aspect-${index + 1}`,
      planet1: aspect?.planet1 || aspect?.body1 || aspect?.from || aspect?.source || "A",
      planet2: aspect?.planet2 || aspect?.body2 || aspect?.to || aspect?.target || "B",
      type,
      orb,
      applying: Boolean(aspect?.applying),
      angle: toNumber(aspect?.exact_angle ?? aspect?.angle ?? aspect?.exactAngle, null),
      raw: aspect,
    };
  });
}

function getSignals(snapshot) {
  const raw = snapshot?.signals ?? [];
  if (Array.isArray(raw)) return raw;
  if (raw && typeof raw === "object") {
    return Object.entries(raw).map(([key, value]) => ({ key, value }));
  }
  return [];
}

function getLunar(snapshot) {
  return snapshot?.lunar_phase || snapshot?.lunar || {};
}

function getNakshatra(snapshot) {
  return snapshot?.nakshatra || {};
}

function getDateLabel(snapshot) {
  return snapshot?.date || snapshot?.timestamp || snapshot?.datetime || "now";
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

export function createObjectTable(snapshot) {
  const bodies = getBodies(snapshot);
  const rows = bodies.map((body) => {
    const raw = body.raw || {};
    const lon = pickBodyLongitude(raw);
    const lat = pickBodyLatitude(raw);
    const speed = pickBodySpeed(raw);
    const dist = pickBodyDistance(raw);
    const { sign, degree } = signFromLongitude(lon);
    const precision = raw.precision || raw.source_precision || raw.accuracy || "—";
    const retro = raw.is_retrograde ? "Rx" : "Direct";
    return `
      <tr>
        <td>${escapeHtml(body.name)}</td>
        <td>${escapeHtml(sign)} ${degree.toFixed(2)}</td>
        <td>${lon.toFixed(4)}</td>
        <td>${lat.toFixed(4)}</td>
        <td>${raw.right_ascension != null ? Number(raw.right_ascension).toFixed(4) : "—"}</td>
        <td>${raw.declination != null ? Number(raw.declination).toFixed(4) : "—"}</td>
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
