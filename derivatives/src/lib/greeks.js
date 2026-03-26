/**
 * DerivativesGrid -- Client-Side Greeks Engine
 *
 * Full Black-Scholes Greeks calculator for real-time what-if analysis.
 * Matches the server-side Python implementation in physics/dealer_gamma.py.
 *
 * Zero external dependencies. All math is self-contained.
 *
 * Parameters convention:
 *   S     = spot price
 *   K     = strike price
 *   T     = time to expiration in years (e.g. 30 days = 30/365)
 *   r     = risk-free rate (annualized, e.g. 0.05 for 5%)
 *   sigma = implied volatility (annualized, e.g. 0.25 for 25%)
 *   type  = 'call' or 'put'
 *
 * @module greeks
 */

// ============================================================================
// CONSTANTS
// ============================================================================

const SQRT_2PI = Math.sqrt(2 * Math.PI);
const DAYS_PER_YEAR = 365;

// ============================================================================
// STANDARD NORMAL DISTRIBUTION
// ============================================================================

/**
 * Standard normal PDF.
 * @param {number} x
 * @returns {number}
 */
export function normalPDF(x) {
  return Math.exp(-0.5 * x * x) / SQRT_2PI;
}

/**
 * Standard normal CDF using the Abramowitz & Stegun rational approximation.
 * Maximum error ~7.5e-8. No external dependencies.
 * @param {number} x
 * @returns {number}
 */
export function normalCDF(x) {
  // Zelen & Severo (1964) approximation, from A&S 26.2.17
  // Maximum absolute error: 7.5e-8
  //
  // This computes P(X <= x) for X ~ N(0,1).
  // For negative x, use symmetry: CDF(-x) = 1 - CDF(x).
  if (x < -8) return 0;
  if (x > 8) return 1;

  const b0 = 0.2316419;
  const b1 = 0.319381530;
  const b2 = -0.356563782;
  const b3 = 1.781477937;
  const b4 = -1.821255978;
  const b5 = 1.330274429;

  const absX = Math.abs(x);
  const t = 1.0 / (1.0 + b0 * absX);
  const pdf = Math.exp(-0.5 * absX * absX) / SQRT_2PI;
  const poly = t * (b1 + t * (b2 + t * (b3 + t * (b4 + t * b5))));
  const cdfPositive = 1.0 - pdf * poly;

  return x >= 0 ? cdfPositive : 1.0 - cdfPositive;
}

// ============================================================================
// CORE BLACK-SCHOLES: d1, d2, OPTION PRICES
// ============================================================================

/**
 * Black-Scholes d1.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function d1(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0 || K <= 0) return 0.0;
  return (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
}

/**
 * Black-Scholes d2.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function d2(S, K, T, r, sigma) {
  return d1(S, K, T, r, sigma) - sigma * Math.sqrt(T);
}

/**
 * Black-Scholes call price.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function callPrice(S, K, T, r, sigma) {
  if (T <= 0) return Math.max(S - K, 0);
  if (sigma <= 0 || S <= 0 || K <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  return S * normalCDF(d1Val) - K * Math.exp(-r * T) * normalCDF(d2Val);
}

/**
 * Black-Scholes put price.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function putPrice(S, K, T, r, sigma) {
  if (T <= 0) return Math.max(K - S, 0);
  if (sigma <= 0 || S <= 0 || K <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  return K * Math.exp(-r * T) * normalCDF(-d2Val) - S * normalCDF(-d1Val);
}

// ============================================================================
// FIRST-ORDER GREEKS
// ============================================================================

/**
 * Delta: dPrice/dSpot.
 * Call delta is in [0, 1], put delta is in [-1, 0].
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @param {string} type - 'call' or 'put'
 * @returns {number}
 */
export function delta(S, K, T, r, sigma, type = 'call') {
  if (T <= 0 || sigma <= 0) {
    if (type === 'call') return S > K ? 1.0 : 0.0;
    return S < K ? -1.0 : 0.0;
  }
  const d1Val = d1(S, K, T, r, sigma);
  if (type === 'call') return normalCDF(d1Val);
  return normalCDF(d1Val) - 1.0;
}

/**
 * Gamma: dDelta/dSpot. Same for calls and puts.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function gamma(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  return normalPDF(d1Val) / (S * sigma * Math.sqrt(T));
}

/**
 * Vega: dPrice/dSigma. Per 1% IV move (divided by 100).
 * Same for calls and puts.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function vega(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  return S * normalPDF(d1Val) * Math.sqrt(T) / 100;
}

/**
 * Theta: dPrice/dTime. Daily decay (divided by 365).
 * Typically negative for long options.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @param {string} type - 'call' or 'put'
 * @returns {number}
 */
export function theta(S, K, T, r, sigma, type = 'call') {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);

  const term1 = -(S * normalPDF(d1Val) * sigma) / (2 * Math.sqrt(T));

  if (type === 'call') {
    const term2 = -r * K * Math.exp(-r * T) * normalCDF(d2Val);
    return (term1 + term2) / DAYS_PER_YEAR;
  }
  const term2 = r * K * Math.exp(-r * T) * normalCDF(-d2Val);
  return (term1 + term2) / DAYS_PER_YEAR;
}

/**
 * Rho: dPrice/dRate. Per 1% rate move (divided by 100).
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @param {string} type - 'call' or 'put'
 * @returns {number}
 */
export function rho(S, K, T, r, sigma, type = 'call') {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d2Val = d2(S, K, T, r, sigma);

  if (type === 'call') {
    return K * T * Math.exp(-r * T) * normalCDF(d2Val) / 100;
  }
  return -K * T * Math.exp(-r * T) * normalCDF(-d2Val) / 100;
}

// ============================================================================
// SECOND-ORDER GREEKS (THE KARSAN STUFF)
// ============================================================================

/**
 * Vanna: dDelta/dVol = dVega/dSpot.
 * Measures how delta changes when IV moves.
 * KEY for understanding vol-selling doom loops.
 * Same for calls and puts.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function vanna(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  return -normalPDF(d1Val) * d2Val / sigma;
}

/**
 * Charm: dDelta/dTime (delta decay).
 * Tells you how much delta erodes each day -- the invisible rebalancing force.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @param {string} type - 'call' or 'put'
 * @returns {number}
 */
export function charm(S, K, T, r, sigma, type = 'call') {
  if (T <= 1e-6 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  const pdfD1 = normalPDF(d1Val);
  const sqrtT = Math.sqrt(T);

  let charmVal = -pdfD1 * (2 * r * T - d2Val * sigma * sqrtT) / (2 * T * sigma * sqrtT);

  if (type === 'call') {
    charmVal -= r * Math.exp(-r * T) * normalCDF(d2Val);
  } else {
    charmVal += r * Math.exp(-r * T) * normalCDF(-d2Val);
  }

  return charmVal;
}

/**
 * Volga (Vomma): dVega/dVol.
 * Vol convexity -- how much vega itself changes when vol moves.
 * Positive for OTM options, tells you about the vol-of-vol exposure.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function volga(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  const vegaVal = S * normalPDF(d1Val) * Math.sqrt(T);
  return vegaVal * d1Val * d2Val / sigma;
}

/**
 * Veta: dVega/dTime.
 * How vega decays over time -- important for calendar spread positioning.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function veta(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  const sqrtT = Math.sqrt(T);
  const pdfD1 = normalPDF(d1Val);

  return -S * pdfD1 * sqrtT * (
    r * d1Val / (sigma * sqrtT)
    - (1 + d1Val * d2Val) / (2 * T)
  );
}

/**
 * Speed: dGamma/dSpot.
 * How gamma changes as spot moves -- third-order price sensitivity.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function speed(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const gammaVal = gamma(S, K, T, r, sigma);
  return -(gammaVal / S) * (d1Val / (sigma * Math.sqrt(T)) + 1);
}

/**
 * Color: dGamma/dTime.
 * How gamma changes as time passes -- the gamma decay rate.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function color(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  const sqrtT = Math.sqrt(T);
  const pdfD1 = normalPDF(d1Val);

  return -pdfD1 / (2 * S * T * sigma * sqrtT) * (
    2 * r * T - d2Val * sigma * sqrtT
    + (1 - d1Val * d2Val)  // not dividing by T -- already inside the outer division
  );
}

/**
 * Zomma: dGamma/dVol.
 * How gamma changes when vol moves -- tells you about gamma convexity in vol space.
 * @param {number} S - Spot price
 * @param {number} K - Strike price
 * @param {number} T - Time to expiration (years)
 * @param {number} r - Risk-free rate
 * @param {number} sigma - Implied volatility
 * @returns {number}
 */
export function zomma(S, K, T, r, sigma) {
  if (T <= 0 || sigma <= 0 || S <= 0) return 0.0;
  const d1Val = d1(S, K, T, r, sigma);
  const d2Val = d1Val - sigma * Math.sqrt(T);
  const gammaVal = gamma(S, K, T, r, sigma);
  return gammaVal * (d1Val * d2Val - 1) / sigma;
}

// ============================================================================
// AGGREGATE GEX (DEALER GAMMA EXPOSURE)
// ============================================================================

/**
 * Compute time to expiry in years from an expiry date string or Date.
 * @param {string|Date} expiry - Expiry date
 * @returns {number} Time in years
 */
function timeToExpiry(expiry) {
  const expiryDate = typeof expiry === 'string' ? new Date(expiry) : expiry;
  const now = new Date();
  const msPerYear = DAYS_PER_YEAR * 24 * 60 * 60 * 1000;
  return Math.max((expiryDate.getTime() - now.getTime()) / msPerYear, 0);
}

/**
 * Compute aggregate dealer gamma exposure at the current spot level.
 *
 * Dealer position assumption: dealers are NET SHORT options.
 *   - Dealer SHORT a call: gamma is NEGATIVE (short gamma)
 *   - Dealer SHORT a put: gamma is POSITIVE (long gamma)
 *   - GEX = SUM(put_gamma * put_oi * 100 * spot) - SUM(call_gamma * call_oi * 100 * spot)
 *
 * @param {Array<{strike: number, type: string, oi: number, iv: number, expiry: string|Date}>} options
 * @param {number} spot - Current spot price
 * @param {number} [r=0.05] - Risk-free rate
 * @returns {{ aggregate: number, perStrike: Array<{strike: number, callGex: number, putGex: number, netGex: number}>, profile: Array<{spot: number, gex: number}> }}
 */
export function computeGEX(options, spot, r = 0.05) {
  if (!options || options.length === 0 || spot <= 0) {
    return { aggregate: 0, perStrike: [], profile: [] };
  }

  // Group by strike
  const strikeMap = {};
  for (const opt of options) {
    const K = opt.strike;
    if (!strikeMap[K]) {
      strikeMap[K] = { strike: K, calls: [], puts: [] };
    }
    if (opt.type === 'call') {
      strikeMap[K].calls.push(opt);
    } else {
      strikeMap[K].puts.push(opt);
    }
  }

  const perStrike = [];
  let aggregate = 0;

  for (const K of Object.keys(strikeMap).map(Number).sort((a, b) => a - b)) {
    const group = strikeMap[K];
    let callGex = 0;
    let putGex = 0;

    for (const opt of group.calls) {
      const T = timeToExpiry(opt.expiry);
      if (T <= 0) continue;
      const iv = opt.iv > 0 ? opt.iv : 0.25;
      const g = gamma(spot, K, T, r, iv) * opt.oi * 100 * spot;
      callGex -= g; // dealer short calls = short gamma
    }

    for (const opt of group.puts) {
      const T = timeToExpiry(opt.expiry);
      if (T <= 0) continue;
      const iv = opt.iv > 0 ? opt.iv : 0.25;
      const g = gamma(spot, K, T, r, iv) * opt.oi * 100 * spot;
      putGex += g; // dealer short puts = long gamma
    }

    const netGex = callGex + putGex;
    perStrike.push({ strike: K, callGex, putGex, netGex });
    aggregate += netGex;
  }

  // Compute GEX profile curve (spot vs GEX) for charting
  const lo = spot * 0.85;
  const hi = spot * 1.15;
  const nPoints = 50;
  const step = (hi - lo) / (nPoints - 1);
  const profile = [];

  for (let i = 0; i < nPoints; i++) {
    const testSpot = lo + step * i;
    let gex = 0;
    for (const opt of options) {
      const T = timeToExpiry(opt.expiry);
      if (T <= 0) continue;
      const iv = opt.iv > 0 ? opt.iv : 0.25;
      const g = gamma(testSpot, opt.strike, T, r, iv) * opt.oi * 100 * testSpot;
      if (opt.type === 'call') {
        gex -= g;
      } else {
        gex += g;
      }
    }
    profile.push({ spot: Math.round(testSpot * 100) / 100, gex: Math.round(gex) });
  }

  return { aggregate: Math.round(aggregate), perStrike, profile };
}

/**
 * Find the spot price where GEX crosses zero (gamma flip point).
 * Below this: dealers are short gamma (amplifying moves).
 * Above this: dealers are long gamma (dampening moves).
 *
 * @param {Array<{strike: number, type: string, oi: number, iv: number, expiry: string|Date}>} options
 * @param {number} spot - Current spot price
 * @param {number} [r=0.05] - Risk-free rate
 * @returns {number|null} Gamma flip spot price, or null if not found
 */
export function findGammaFlip(options, spot, r = 0.05) {
  if (!options || options.length === 0 || spot <= 0) return null;

  const lo = spot * 0.85;
  const hi = spot * 1.15;
  const nPoints = 100;
  const step = (hi - lo) / (nPoints - 1);

  function gexAtSpot(testSpot) {
    let gex = 0;
    for (const opt of options) {
      const T = timeToExpiry(opt.expiry);
      if (T <= 0) continue;
      const iv = opt.iv > 0 ? opt.iv : 0.25;
      const g = gamma(testSpot, opt.strike, T, r, iv) * opt.oi * 100 * testSpot;
      if (opt.type === 'call') {
        gex -= g;
      } else {
        gex += g;
      }
    }
    return gex;
  }

  let prevGex = null;
  let prevSpot = null;

  for (let i = 0; i < nPoints; i++) {
    const testSpot = lo + step * i;
    const gex = gexAtSpot(testSpot);

    if (prevGex !== null && prevGex * gex < 0) {
      // Linear interpolation for the zero crossing
      const ratio = Math.abs(prevGex) / (Math.abs(prevGex) + Math.abs(gex) + 1e-12);
      return Math.round((prevSpot + ratio * (testSpot - prevSpot)) * 100) / 100;
    }
    prevGex = gex;
    prevSpot = testSpot;
  }

  return null;
}

/**
 * Find put wall and call wall (strikes with maximum gamma).
 * Put wall = max positive GEX strike (support level, gravitational floor).
 * Call wall = max negative GEX strike (resistance level, gravitational ceiling).
 *
 * @param {Array<{strike: number, type: string, oi: number, iv: number, expiry: string|Date}>} options
 * @param {number} spot - Current spot price
 * @param {number} [r=0.05] - Risk-free rate
 * @returns {{ putWall: number|null, callWall: number|null, putWallGex: number, callWallGex: number }}
 */
export function findWalls(options, spot, r = 0.05) {
  if (!options || options.length === 0 || spot <= 0) {
    return { putWall: null, callWall: null, putWallGex: 0, callWallGex: 0 };
  }

  const { perStrike } = computeGEX(options, spot, r);

  let putWall = null;
  let putWallGex = 0;
  let callWall = null;
  let callWallGex = 0;

  for (const s of perStrike) {
    // Put wall: strike with max positive put GEX (dealer long gamma from puts)
    if (s.putGex > putWallGex) {
      putWallGex = s.putGex;
      putWall = s.strike;
    }
    // Call wall: strike with max negative call GEX (dealer short gamma from calls)
    if (s.callGex < callWallGex) {
      callWallGex = s.callGex;
      callWall = s.strike;
    }
  }

  return { putWall, callWall, putWallGex, callWallGex };
}

// ============================================================================
// SCENARIO ANALYSIS
// ============================================================================

/**
 * What-if analysis: compute new price and Greeks after a hypothetical move.
 *
 * @param {{ strike: number, type: string, iv: number, expiry: string|Date, spot: number }} option
 * @param {number} spotChange - Absolute spot change (e.g. +5 means spot goes up $5)
 * @param {number} volChange - Absolute IV change (e.g. +0.05 means IV goes up 5 percentage points)
 * @param {number} daysPassed - Days elapsed (reduces time to expiry)
 * @param {number} [r=0.05] - Risk-free rate
 * @returns {{ priceBefore: number, priceAfter: number, pnl: number, pnlPct: number, greeks: Object }}
 */
export function whatIf(option, spotChange = 0, volChange = 0, daysPassed = 0, r = 0.05) {
  const { strike: K, type: optType, iv: sigma, expiry, spot: S } = option;
  const T = timeToExpiry(expiry);
  const priceFn = optType === 'call' ? callPrice : putPrice;

  const priceBefore = priceFn(S, K, T, r, sigma);

  const newS = S + spotChange;
  const newSigma = Math.max(sigma + volChange, 0.01);
  const newT = Math.max(T - daysPassed / DAYS_PER_YEAR, 0);

  const priceAfter = priceFn(newS, K, newT, r, newSigma);
  const pnl = priceAfter - priceBefore;
  const pnlPct = priceBefore > 0 ? (pnl / priceBefore) * 100 : 0;

  return {
    priceBefore: round4(priceBefore),
    priceAfter: round4(priceAfter),
    pnl: round4(pnl),
    pnlPct: round2(pnlPct),
    greeks: {
      delta: round4(delta(newS, K, newT, r, newSigma, optType)),
      gamma: round6(gamma(newS, K, newT, r, newSigma)),
      theta: round4(theta(newS, K, newT, r, newSigma, optType)),
      vega: round4(vega(newS, K, newT, r, newSigma)),
      vanna: round6(vanna(newS, K, newT, r, newSigma)),
      charm: round6(charm(newS, K, newT, r, newSigma, optType)),
    },
  };
}

/**
 * Grid of P&L across spot x vol scenarios.
 *
 * @param {{ strike: number, type: string, iv: number, expiry: string|Date, spot: number }} option
 * @param {Array<number>} spotRange - Array of spot price offsets (e.g. [-10, -5, 0, 5, 10])
 * @param {Array<number>} volRange - Array of IV offsets (e.g. [-0.10, -0.05, 0, 0.05, 0.10])
 * @param {number} [r=0.05] - Risk-free rate
 * @returns {{ spotAxis: Array<number>, volAxis: Array<number>, grid: Array<Array<number>>, basePrice: number }}
 */
export function pnlScenarios(option, spotRange, volRange, r = 0.05) {
  const { strike: K, type: optType, iv: sigma, expiry, spot: S } = option;
  const T = timeToExpiry(expiry);
  const priceFn = optType === 'call' ? callPrice : putPrice;
  const basePrice = priceFn(S, K, T, r, sigma);

  const spotAxis = spotRange.map(offset => S + offset);
  const volAxis = volRange.map(offset => sigma + offset);

  const grid = [];
  for (const volOffset of volRange) {
    const row = [];
    const newSigma = Math.max(sigma + volOffset, 0.01);
    for (const spotOffset of spotRange) {
      const newS = S + spotOffset;
      const newPrice = priceFn(newS, K, T, r, newSigma);
      row.push(round2(newPrice - basePrice));
    }
    grid.push(row);
  }

  return {
    spotAxis: spotAxis.map(round2),
    volAxis: volAxis.map(v => round4(v)),
    grid,
    basePrice: round4(basePrice),
  };
}

// ============================================================================
// UTILITY
// ============================================================================

function round2(x) { return Math.round(x * 100) / 100; }
function round4(x) { return Math.round(x * 10000) / 10000; }
function round6(x) { return Math.round(x * 1000000) / 1000000; }
