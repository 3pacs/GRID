// Strict number reading for values that come from an API (#959).
//
// `Number()` coerces blank strings, false, [] and [0] to 0, so it must never be applied to an
// unchecked shape. Only a finite JSON number, or a string that is a plain decimal after trimming
// (the API may send numeric strings), is a value; null, undefined, booleans, arrays, objects,
// BigInt, NaN, +/-Infinity and strings such as "0x10", "1,5" or "NaN" are not.
const PLAIN_DECIMAL = /^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$/;

export function finiteNumberOrNull(value) {
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    if (typeof value === 'string') {
        const t = value.trim();
        if (!PLAIN_DECIMAL.test(t)) return null;
        const n = Number(t);
        return Number.isFinite(n) ? n : null;
    }
    return null;
}
