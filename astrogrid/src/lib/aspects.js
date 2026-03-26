/**
 * GRID Aspect Geometry Helpers -- Copernicus Module.
 *
 * Provides aspect visualization utilities for the 3D AstroGrid display.
 * Colors, line geometry, and applying/separating logic for planetary aspects.
 *
 * @module aspects
 */

// ============================================================================
// ASPECT COLORS
// ============================================================================

const ASPECT_COLORS = {
  conjunction: { hex: "#FFD700", rgb: [255, 215, 0],   name: "gold" },
  opposition:  { hex: "#FF4444", rgb: [255, 68, 68],   name: "red" },
  trine:       { hex: "#44FF44", rgb: [68, 255, 68],   name: "green" },
  square:      { hex: "#FF8800", rgb: [255, 136, 0],   name: "orange" },
  sextile:     { hex: "#00CCCC", rgb: [0, 204, 204],   name: "cyan" },
};

// Opacity by aspect strength (tighter orb = more opaque)
const MAX_ORB = 10.0;

/**
 * Get the color for an aspect type.
 *
 * @param {string} aspectType - One of: conjunction, opposition, trine, square, sextile
 * @returns {{ hex: string, rgb: number[], name: string }}
 */
export function getAspectColor(aspectType) {
  return ASPECT_COLORS[aspectType] || ASPECT_COLORS.conjunction;
}

/**
 * Get aspect color with opacity based on orb tightness.
 * Tighter orb = more opaque (stronger aspect).
 *
 * @param {string} aspectType
 * @param {number} orbUsed - Actual orb in degrees
 * @param {number} maxOrb - Maximum orb for this aspect type
 * @returns {{ hex: string, rgb: number[], opacity: number }}
 */
export function getAspectColorWithOpacity(aspectType, orbUsed, maxOrb = MAX_ORB) {
  const color = getAspectColor(aspectType);
  // Linear falloff: exact = 1.0, at maxOrb = 0.2
  const opacity = Math.max(0.2, 1.0 - (orbUsed / maxOrb) * 0.8);
  return {
    ...color,
    opacity: +opacity.toFixed(3),
  };
}

// ============================================================================
// 3D LINE GEOMETRY
// ============================================================================

/**
 * Compute 3D line geometry between two planet positions for aspect visualization.
 *
 * Takes two planet positions (as 3D coordinates or ecliptic longitudes)
 * and returns the vertices for rendering an aspect line.
 *
 * @param {{ x: number, y: number, z: number }} planet1Pos - 3D position of planet 1
 * @param {{ x: number, y: number, z: number }} planet2Pos - 3D position of planet 2
 * @param {Object} [options]
 * @param {number} [options.curveHeight=0.1] - Height of the curve above the ecliptic plane
 * @param {number} [options.segments=32] - Number of line segments for the curve
 * @param {boolean} [options.curved=true] - Whether to curve the line (false = straight)
 * @returns {{ vertices: number[][], midpoint: { x: number, y: number, z: number } }}
 */
export function getAspectLine(planet1Pos, planet2Pos, options = {}) {
  const {
    curveHeight = 0.1,
    segments = 32,
    curved = true,
  } = options;

  const vertices = [];
  const p1 = planet1Pos;
  const p2 = planet2Pos;

  if (!curved) {
    // Straight line
    vertices.push([p1.x, p1.y, p1.z]);
    vertices.push([p2.x, p2.y, p2.z]);
    return {
      vertices,
      midpoint: {
        x: (p1.x + p2.x) / 2,
        y: (p1.y + p2.y) / 2,
        z: (p1.z + p2.z) / 2,
      },
    };
  }

  // Curved line (quadratic Bezier through midpoint raised above ecliptic)
  const mid = {
    x: (p1.x + p2.x) / 2,
    y: (p1.y + p2.y) / 2 + curveHeight,
    z: (p1.z + p2.z) / 2,
  };

  for (let i = 0; i <= segments; i++) {
    const t = i / segments;
    const u = 1 - t;

    // Quadratic Bezier: B(t) = (1-t)^2 * P0 + 2(1-t)t * P1 + t^2 * P2
    const x = u * u * p1.x + 2 * u * t * mid.x + t * t * p2.x;
    const y = u * u * p1.y + 2 * u * t * mid.y + t * t * p2.y;
    const z = u * u * p1.z + 2 * u * t * mid.z + t * t * p2.z;

    vertices.push([x, y, z]);
  }

  return { vertices, midpoint: mid };
}

/**
 * Convert ecliptic longitude to 3D position on a unit circle in the ecliptic plane.
 * Useful for mapping planet positions to the zodiac wheel visualization.
 *
 * @param {number} eclipticLongitude - Degrees (0-360)
 * @param {number} [radius=1.0] - Radius of the orbit circle
 * @param {number} [elevation=0.0] - Height above the ecliptic plane
 * @returns {{ x: number, y: number, z: number }}
 */
export function eclipticTo3D(eclipticLongitude, radius = 1.0, elevation = 0.0) {
  const rad = eclipticLongitude * Math.PI / 180.0;
  return {
    x: radius * Math.cos(rad),
    y: elevation,
    z: radius * Math.sin(rad),
  };
}

/**
 * Get aspect line from ecliptic longitudes (convenience wrapper).
 *
 * @param {number} lon1 - Ecliptic longitude of planet 1 (degrees)
 * @param {number} lon2 - Ecliptic longitude of planet 2 (degrees)
 * @param {number} [radius1=1.0] - Orbital radius for planet 1
 * @param {number} [radius2=1.0] - Orbital radius for planet 2
 * @param {Object} [options] - Line options (see getAspectLine)
 * @returns {{ vertices: number[][], midpoint: Object }}
 */
export function getAspectLineFromLongitudes(lon1, lon2, radius1 = 1.0, radius2 = 1.0, options = {}) {
  const pos1 = eclipticTo3D(lon1, radius1);
  const pos2 = eclipticTo3D(lon2, radius2);
  return getAspectLine(pos1, pos2, options);
}

// ============================================================================
// ASPECT DYNAMICS
// ============================================================================

/**
 * Determine if an aspect is applying (getting closer to exact) or separating.
 *
 * @param {number} planet1Speed - Daily motion of planet 1 (degrees/day)
 * @param {number} planet2Speed - Daily motion of planet 2 (degrees/day)
 * @param {number} currentAngle - Current angular separation (degrees)
 * @param {number} exactAngle - Exact aspect angle (e.g. 0, 60, 90, 120, 180)
 * @returns {boolean} True if applying (getting closer to exact)
 */
export function isApplying(planet1Speed, planet2Speed, currentAngle, exactAngle) {
  // Relative speed: how fast the angle between them is changing
  const relativeSpeed = planet1Speed - planet2Speed;

  // If current angle is less than exact: applying if gap is closing
  // For conjunction (0): if angle > 180, measure the other way
  const diff = currentAngle - exactAngle;

  if (exactAngle === 0) {
    // Conjunction: applying if the faster planet is catching up
    // When current > 180, they're approaching from the other side
    if (currentAngle > 180) {
      return relativeSpeed > 0;
    }
    return relativeSpeed < 0;
  }

  if (exactAngle === 180) {
    // Opposition: applying if getting closer to 180
    if (diff > 0) {
      return relativeSpeed < 0;
    }
    return relativeSpeed > 0;
  }

  // General case: applying if the difference is shrinking
  if (diff > 0) {
    // Current > exact: need speed to decrease angle
    return relativeSpeed < 0;
  }
  // Current < exact: need speed to increase angle
  return relativeSpeed > 0;
}

/**
 * Compute the "strength" of an aspect (0-1) based on orb.
 * Exact aspects = 1.0, at the edge of orb = 0.0.
 *
 * @param {number} orbUsed - The actual orb distance from exact (degrees)
 * @param {number} maxOrb - Maximum allowable orb for this aspect
 * @returns {number} Strength from 0.0 to 1.0
 */
export function aspectStrength(orbUsed, maxOrb) {
  if (orbUsed >= maxOrb) return 0.0;
  return 1.0 - (orbUsed / maxOrb);
}

// ============================================================================
// ASPECT LINE STYLING
// ============================================================================

/**
 * Get complete visual style for an aspect line.
 * Combines color, opacity, and line width based on aspect properties.
 *
 * @param {Object} aspect - Aspect object from computeAspects()
 * @returns {Object} Style object with color, opacity, lineWidth, dashPattern
 */
export function getAspectStyle(aspect) {
  const color = getAspectColor(aspect.aspect_type);
  const maxOrb = {
    conjunction: 8, opposition: 8, trine: 8, square: 7, sextile: 6,
  }[aspect.aspect_type] || 8;

  const strength = aspectStrength(aspect.orb_used, maxOrb);
  const opacity = 0.2 + strength * 0.8;
  const lineWidth = 1 + strength * 3;

  // Applying aspects are solid, separating are dashed
  const dashPattern = aspect.applying ? null : [5, 5];

  return {
    color: color.hex,
    rgb: color.rgb,
    opacity: +opacity.toFixed(3),
    lineWidth: +lineWidth.toFixed(1),
    dashPattern,
    strength: +strength.toFixed(3),
    applying: aspect.applying,
    nature: aspect.nature,
  };
}
