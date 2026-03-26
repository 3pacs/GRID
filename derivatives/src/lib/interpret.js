/**
 * DerivativesGrid -- Dealer Flow Interpretation Engine
 *
 * Converts raw derivatives data into Cem Karsan-style mechanical narratives.
 * Every number tells a story about dealer positioning and flow dynamics.
 *
 * This is the soul of DerivativesGrid. The numbers are physics -- they describe
 * the mechanical forces that move markets. Dealers must hedge. Hedging creates
 * flow. Flow moves price. This module translates that chain into words.
 *
 * @module interpret
 */

// ============================================================================
// FORMATTING HELPERS
// ============================================================================

/**
 * Format a large number with B/M/K suffix.
 * @param {number} value
 * @returns {string}
 */
function fmtDollar(value) {
  const abs = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
}

/**
 * Format a percentage with sign.
 * @param {number} value - As decimal (0.05 = 5%) or percentage
 * @param {boolean} [asDecimal=true] - If true, value is decimal
 * @returns {string}
 */
function fmtPct(value, asDecimal = true) {
  const pct = asDecimal ? value * 100 : value;
  const sign = pct > 0 ? '+' : '';
  return `${sign}${pct.toFixed(1)}%`;
}

/**
 * Calculate percentage distance between two prices.
 * @param {number} from
 * @param {number} to
 * @returns {number}
 */
function pctDistance(from, to) {
  if (from <= 0) return 0;
  return ((to - from) / from) * 100;
}

// ============================================================================
// REGIME INTERPRETATION
// ============================================================================

/**
 * Interpret the current dealer gamma regime.
 *
 * This is THE core Karsan insight: dealer positioning determines market behavior.
 * Long gamma = mean-reversion. Short gamma = trending/acceleration.
 *
 * @param {number} gex - Aggregate GEX value (dollar gamma exposure)
 * @param {number} gammaFlip - Spot price where GEX crosses zero
 * @param {number} spot - Current spot price
 * @returns {string} Mechanical narrative
 */
export function interpretRegime(gex, gammaFlip, spot) {
  if (gex == null || spot == null) return '';

  const gexStr = fmtDollar(gex);
  const aboveFlip = gammaFlip != null && spot > gammaFlip;
  const belowFlip = gammaFlip != null && spot < gammaFlip;
  const flipStr = gammaFlip != null ? gammaFlip.toFixed(0) : 'unknown';

  if (gex > 0) {
    const flipPart = gammaFlip != null
      ? ` Above the flip at ${flipStr}, dealer hedging DAMPENS moves.`
      : ' Dealer hedging is dampening moves.';
    return `Dealers are LONG gamma (${gexStr}).${flipPart} Expect mean-reversion and a grind higher toward the call wall.`;
  }

  if (gex < 0) {
    const flipPart = gammaFlip != null
      ? ` Below the flip at ${flipStr}, dealer hedging AMPLIFIES moves.`
      : ' Dealer hedging is amplifying moves.';
    return `Dealers are SHORT gamma (${gexStr}).${flipPart} Expect trending behavior and potential acceleration.`;
  }

  return `Dealers are NEUTRAL on gamma (${gexStr}). Near the flip point — regime could shift quickly with any positioning change.`;
}

// ============================================================================
// GEX INTERPRETATION
// ============================================================================

/**
 * Interpret the GEX level into a regime classification with color and narrative.
 *
 * @param {number} gexValue - Aggregate GEX in dollars
 * @returns {{ level: string, text: string, color: string }}
 */
export function interpretGEX(gexValue) {
  if (gexValue == null) return { level: 'unknown', text: '', color: '#888' };

  const gexStr = fmtDollar(gexValue);

  if (gexValue > 5e9) {
    return {
      level: 'extreme_long',
      text: `Extreme long gamma (${gexStr}) — market is heavily pinned. Very low vol environment. Watch for gamma unwind at OpEx.`,
      color: '#00c853',
    };
  }

  if (gexValue > 1e9) {
    return {
      level: 'long',
      text: `Long gamma (${gexStr}) — dealers dampening moves. Dips get bought, rallies get sold. Low realized vol.`,
      color: '#4caf50',
    };
  }

  if (gexValue > -1e9) {
    return {
      level: 'neutral',
      text: `Neutral gamma (${gexStr}) — balanced dealer positioning. Market can move freely in either direction.`,
      color: '#ff9800',
    };
  }

  if (gexValue > -5e9) {
    return {
      level: 'short',
      text: `Short gamma (${gexStr}) — dealers amplifying moves. Trends persist, vol is elevated. Hedging flows accelerate directional moves.`,
      color: '#f44336',
    };
  }

  return {
    level: 'extreme_short',
    text: `Extreme short gamma (${gexStr}) — dealers massively amplifying. Risk of cascading moves and forced liquidation. This is the danger zone.`,
    color: '#b71c1c',
  };
}

// ============================================================================
// WALL INTERPRETATION
// ============================================================================

/**
 * Interpret the put wall and call wall relative to spot.
 *
 * Gamma walls are gravitational levels. Price tends to orbit between them.
 * Put wall = support floor (max put OI creates buying pressure on approach).
 * Call wall = resistance ceiling (max call OI creates selling pressure).
 *
 * @param {number} putWall - Put wall strike
 * @param {number} callWall - Call wall strike
 * @param {number} gammaFlip - Gamma flip level
 * @param {number} spot - Current spot price
 * @returns {{ support: string, resistance: string, range: string }}
 */
export function interpretWalls(putWall, callWall, gammaFlip, spot) {
  if (spot == null || spot <= 0) {
    return { support: '', resistance: '', range: '' };
  }

  let support = '';
  let resistance = '';
  let range = '';

  if (putWall != null && putWall > 0) {
    const dist = pctDistance(spot, putWall);
    support = `Put wall at ${putWall.toFixed(0)} (${Math.abs(dist).toFixed(1)}% ${dist < 0 ? 'below' : 'above'}) acts as gravitational support — max put gamma creates buying pressure on approach.`;
  } else {
    support = 'No clear put wall identified — support levels are diffuse.';
  }

  if (callWall != null && callWall > 0) {
    const dist = pctDistance(spot, callWall);
    resistance = `Call wall at ${callWall.toFixed(0)} (${Math.abs(dist).toFixed(1)}% ${dist > 0 ? 'above' : 'below'}) acts as resistance ceiling — max call gamma creates selling pressure.`;
  } else {
    resistance = 'No clear call wall identified — resistance levels are diffuse.';
  }

  if (putWall != null && callWall != null && putWall > 0 && callWall > 0) {
    const rangeWidth = ((callWall - putWall) / spot * 100).toFixed(1);
    const posInRange = putWall < callWall
      ? (((spot - putWall) / (callWall - putWall)) * 100).toFixed(0)
      : 50;

    if (gammaFlip != null) {
      const flipPos = spot > gammaFlip ? 'above' : 'below';
      range = `Trading range: ${putWall.toFixed(0)} to ${callWall.toFixed(0)} (${rangeWidth}% wide). Spot is ${posInRange}% through the range, ${flipPos} the gamma flip at ${gammaFlip.toFixed(0)}.`;
    } else {
      range = `Trading range: ${putWall.toFixed(0)} to ${callWall.toFixed(0)} (${rangeWidth}% wide). Spot is ${posInRange}% through the range.`;
    }
  }

  return { support, resistance, range };
}

// ============================================================================
// VANNA INTERPRETATION
// ============================================================================

/**
 * Interpret vanna exposure -- the vol-delta feedback loop.
 *
 * Vanna is dDelta/dVol. When VIX moves, dealers with vanna exposure must
 * rebalance delta. This creates the mechanical vol-selling doom loop:
 *   VIX spikes -> dealers must sell delta -> selling pushes price down ->
 *   price drop causes more VIX spike -> more delta selling -> loop.
 *
 * @param {number} vannaExposure - Aggregate vanna in dollar terms
 * @param {number} vixLevel - Current VIX level
 * @returns {string}
 */
export function interpretVanna(vannaExposure, vixLevel) {
  if (vannaExposure == null) return '';

  const vannaStr = fmtDollar(vannaExposure);
  const vixStr = vixLevel != null ? vixLevel.toFixed(0) : '?';

  if (vannaExposure < -1e8) {
    // Negative vanna: VIX spike forces delta selling (doom loop)
    const hypotheticalVixSpike = 5;
    const deltaForced = Math.abs(vannaExposure) * hypotheticalVixSpike / 100;
    const deltaStr = fmtDollar(deltaForced);

    return `Vanna exposure of ${vannaStr}: if VIX spikes from ${vixStr} to ${(vixLevel || 15) + hypotheticalVixSpike}, dealers must SELL ~${deltaStr} of delta. This mechanically pushes prices lower — the vol-selling doom loop.`;
  }

  if (vannaExposure > 1e8) {
    return `Positive vanna (${vannaStr}): rising VIX would force dealers to BUY delta — a stabilizing force. Vol spikes are self-dampening in this regime.`;
  }

  return `Vanna exposure is small (${vannaStr}) — vol moves will not trigger significant dealer delta rebalancing. The vol-delta feedback loop is muted.`;
}

// ============================================================================
// CHARM INTERPRETATION
// ============================================================================

/**
 * Interpret charm exposure -- the invisible force of time.
 *
 * Charm is dDelta/dTime. As time passes, option deltas shift. Dealers must
 * rebalance to stay hedged. This creates a predictable daily flow that
 * either supports or pressures the market.
 *
 * @param {number} charmExposure - Aggregate charm in dollar-delta per day
 * @param {number} daysToOpex - Business days until next options expiration
 * @returns {string}
 */
export function interpretCharm(charmExposure, daysToOpex) {
  if (charmExposure == null) return '';

  const charmStr = fmtDollar(Math.abs(charmExposure));
  const direction = charmExposure > 0 ? 'positive' : 'negative';
  const daysStr = daysToOpex != null ? daysToOpex.toFixed(0) : '?';
  const totalCharmToOpex = daysToOpex != null
    ? fmtDollar(Math.abs(charmExposure * daysToOpex))
    : '?';

  if (charmExposure > 1e6) {
    return `Charm flow of +${charmStr}/day: time decay is eroding dealer short gamma. Each passing day reduces the amplification. ${daysStr} days to OpEx — charm will have removed ~${totalCharmToOpex} of delta exposure before expiration.`;
  }

  if (charmExposure < -1e6) {
    return `Charm flow of -${charmStr}/day: time decay is increasing dealer delta exposure. Daily rebalancing adds selling pressure. ${daysStr} days to OpEx — ~${totalCharmToOpex} of delta shift before expiration.`;
  }

  return `Charm flow is minimal (${charmStr}/day) — time decay is not generating significant hedging flows. ${daysStr} days to OpEx.`;
}

// ============================================================================
// OPEX INTERPRETATION
// ============================================================================

/**
 * Interpret proximity to options expiration.
 *
 * OpEx is the great reset. Gamma expires, pins release, new positioning forms.
 * The days before OpEx have distinct mechanical characteristics.
 *
 * @param {number} daysToOpex - Calendar days until next OpEx
 * @param {number} totalGamma - Total gamma expiring
 * @returns {string}
 */
export function interpretOpex(daysToOpex, totalGamma) {
  if (daysToOpex == null) return '';

  const gammaStr = totalGamma != null ? fmtDollar(totalGamma) : 'significant gamma';

  if (daysToOpex <= 0) {
    return `OpEx day — ${gammaStr} of gamma expires today. Expect pin action into the close, then a release of energy into next week as new positioning takes over.`;
  }

  if (daysToOpex <= 2) {
    return `OpEx pinning active — ${gammaStr} of gamma expires in ${daysToOpex} day${daysToOpex === 1 ? '' : 's'}. Expect tight range around max pain until expiry. Dealers will defend key strikes aggressively.`;
  }

  if (daysToOpex <= 7) {
    return `Approaching OpEx (${daysToOpex} days) — gamma decay accelerating. Vol compression into Friday. Charm flows intensifying as near-dated options lose their time value rapidly.`;
  }

  if (daysToOpex <= 14) {
    return `OpEx is ${daysToOpex} days out — gamma positioning is building but still has time to evolve. Watch for large trades that shift the GEX landscape this week.`;
  }

  return `OpEx is ${daysToOpex} days out — gamma positioning has time to evolve. Current GEX levels are less sticky; new flow can reshape the profile significantly.`;
}

// ============================================================================
// PCR INTERPRETATION
// ============================================================================

/**
 * Interpret put/call ratio into sentiment level with narrative.
 *
 * @param {number} pcr - Put/call ratio
 * @returns {{ level: string, text: string, color: string }}
 */
export function interpretPCR(pcr) {
  if (pcr == null) return { level: 'unknown', text: '', color: '#888' };

  if (pcr > 1.5) {
    return {
      level: 'extreme_bearish',
      text: `Extreme bearish positioning (P/C ${pcr.toFixed(2)}) — heavy put buying signals panic or aggressive hedging. Historically a contrarian bullish signal when overdone.`,
      color: '#b71c1c',
    };
  }

  if (pcr > 1.2) {
    return {
      level: 'bearish',
      text: `Bearish tilt (P/C ${pcr.toFixed(2)}) — more puts than calls. Smart money may be hedging, or directional bets on downside are building.`,
      color: '#f44336',
    };
  }

  if (pcr > 0.9) {
    return {
      level: 'neutral',
      text: `Neutral options flow (P/C ${pcr.toFixed(2)}) — balanced put/call activity. No strong directional signal from flow.`,
      color: '#ff9800',
    };
  }

  if (pcr > 0.7) {
    return {
      level: 'bullish',
      text: `Bullish tilt (P/C ${pcr.toFixed(2)}) — more calls than puts. Directional optimism or upside speculation dominating flow.`,
      color: '#4caf50',
    };
  }

  return {
    level: 'extreme_bullish',
    text: `Extreme bullish positioning (P/C ${pcr.toFixed(2)}) — heavy call buying. Could signal greed or a gamma squeeze setup. Contrarian bearish if overdone.`,
    color: '#00c853',
  };
}

// ============================================================================
// IV SKEW INTERPRETATION
// ============================================================================

/**
 * Interpret implied volatility skew.
 *
 * Skew = IV of 25-delta put / IV of 25-delta call.
 * Steep skew means downside protection is expensive (fear premium).
 * Flat skew means complacency.
 *
 * @param {number} skew - Put/call IV skew ratio
 * @returns {{ level: string, text: string }}
 */
export function interpretSkew(skew) {
  if (skew == null) return { level: 'unknown', text: '' };

  if (skew > 1.5) {
    return {
      level: 'steep',
      text: `Downside protection is expensive (skew: ${skew.toFixed(2)}) — market participants heavily hedged. Contrarian bullish signal if overdone. Put sellers collecting rich premium.`,
    };
  }

  if (skew > 1.2) {
    return {
      level: 'elevated',
      text: `Skew is elevated (${skew.toFixed(2)}) — moderate fear premium in puts. Downside hedging demand is above-normal. Typical in uncertain macro environments.`,
    };
  }

  if (skew > 1.0) {
    return {
      level: 'normal',
      text: `Normal skew (${skew.toFixed(2)}) — standard put premium over calls. Healthy hedging activity without panic.`,
    };
  }

  if (skew > 0.8) {
    return {
      level: 'flat',
      text: `No fear premium — complacency (skew: ${skew.toFixed(2)}). Cheap puts may offer asymmetric protection. Call IV matching or exceeding put IV is unusual.`,
    };
  }

  return {
    level: 'inverted',
    text: `Inverted skew (${skew.toFixed(2)}) — calls more expensive than puts. Rare. Usually signals a short squeeze or extreme upside speculation. Puts are historically cheap here.`,
  };
}

// ============================================================================
// TERM STRUCTURE INTERPRETATION
// ============================================================================

/**
 * Interpret the volatility term structure.
 *
 * Normal (contango): far-dated vol > near-dated vol. No imminent event risk.
 * Inverted (backwardation): near-dated vol > far-dated vol. Market pricing a specific near-term event.
 *
 * @param {number} slope - Term structure slope (positive = normal, negative = inverted)
 * @param {boolean} isInverted - Whether the curve is inverted
 * @returns {string}
 */
export function interpretTermStructure(slope, isInverted) {
  if (slope == null) return '';

  if (isInverted || slope < 0) {
    const magnitude = Math.abs(slope);
    if (magnitude > 5) {
      return `Term structure sharply inverted — near-term event risk being aggressively priced. Market expects something specific and imminent. Historically associated with binary events (earnings, FOMC, geopolitical). Straddle buyers are paying up for near-dated vol.`;
    }
    return `Term structure inverted — near-term event risk being priced. Market expects elevated volatility in the short term relative to long term. This typically resolves after the catalyst passes.`;
  }

  if (slope > 5) {
    return `Steep normal contango — far-dated vol significantly above near-dated. No imminent event risk priced. Near-dated options are cheap relative to far-dated. Calendar spreads favor selling back-month vol.`;
  }

  if (slope > 2) {
    return `Normal contango — no imminent event risk priced. Far-dated vol is moderately expensive relative to near-dated. Healthy term structure indicating steady risk expectations.`;
  }

  return `Flat term structure — near-dated and far-dated vol are similar. Market is not differentiating between time horizons. Could precede either an event risk being priced in or a broad vol repricing.`;
}

// ============================================================================
// COLOR HELPERS
// ============================================================================

/**
 * Color for dealer gamma regime.
 * @param {'LONG_GAMMA'|'SHORT_GAMMA'|'NEUTRAL'} regime
 * @returns {string} CSS color
 */
export function regimeColor(regime) {
  switch (regime) {
    case 'LONG_GAMMA':  return '#4caf50';  // green -- dampening, stable
    case 'SHORT_GAMMA': return '#f44336';  // red -- amplifying, volatile
    case 'NEUTRAL':     return '#ff9800';  // amber -- transition zone
    default:            return '#888888';
  }
}

/**
 * Color for a GEX value on a gradient.
 * Red (negative/short gamma) through amber (neutral) to green (positive/long gamma).
 *
 * @param {number} value - GEX value
 * @returns {string} CSS color
 */
export function gexColor(value) {
  if (value == null) return '#888888';

  // Normalize to [-1, 1] range using $5B as the extreme
  const normalized = Math.max(-1, Math.min(1, value / 5e9));

  if (normalized > 0.5)  return '#00c853';  // strong positive
  if (normalized > 0.1)  return '#4caf50';  // positive
  if (normalized > -0.1) return '#ff9800';  // neutral
  if (normalized > -0.5) return '#f44336';  // negative
  return '#b71c1c';                          // strong negative
}

/**
 * Generic threshold-based coloring.
 * Takes a value and an array of {threshold, color} objects sorted by threshold ascending.
 * Returns the color of the highest threshold that the value exceeds.
 *
 * @param {number} value
 * @param {Array<{threshold: number, color: string}>} thresholds - Sorted ascending
 * @param {string} [defaultColor='#888888'] - Color when below all thresholds
 * @returns {string} CSS color
 */
export function levelToColor(value, thresholds, defaultColor = '#888888') {
  if (value == null || !thresholds || thresholds.length === 0) return defaultColor;

  let color = defaultColor;
  for (const t of thresholds) {
    if (value >= t.threshold) {
      color = t.color;
    } else {
      break;
    }
  }
  return color;
}
