// Recovery policy for persisted/materialization reads. Mutations and streams
// keep their existing policy; never replay a mutation as a read retry.
export const READ_DEADLINE_MS = 15_000;

export function safeRequestMessage(error) {
    if (error?.code === 'timeout') return 'The request timed out after 15 seconds. Please retry.';
    if (error?.code === 'cancelled') return 'The request was cancelled. Please retry.';
    if (error?.code === 'invalid_response' || error?.message === 'Invalid JSON response') return 'The server sent an unreadable response. Please retry.';
    const status = error?.status;
    if (status === 401) return 'Your session has expired. Please sign in again.';
    if (status === 403) return 'You are not authorised to view this data.';
    if (status === 404) return 'This data is unavailable on the server.';
    if (status === 524 || status === 504 || status === 408) return `The server timed out (status ${status}). Please retry.`;
    if (status >= 500) return `The server is unavailable (status ${status}). Please retry.`;
    if (status >= 400) return `The request failed (status ${status}). Please retry.`;
    if (error?.code === 'http') return 'The server could not return this data. Please retry.';
    return 'The server could not be reached. Please retry.';
}

export function requestFailure(code, status = 0) {
    const error = { error: true, status, code };
    return { ...error, message: safeRequestMessage(error) };
}

export function readErrorCode(result) {
    if (!result || typeof result !== 'object' || !Object.hasOwn(result, 'error')) return null;
    // Analytic success envelopes omit error. Backend failures use str(exc),
    // which can be empty. Null/false/other field types are malformed, not success.
    if (typeof result.error === 'string') return 'http';
    if (result.error === true) {
        return ['http', 'network', 'timeout', 'cancelled', 'invalid_response'].includes(result.code) ? result.code : 'http';
    }
    return 'invalid_response';
}

export function isResearchRunRecord(result) {
    // Only snapshots/research/latest uses this record contract: its persisted
    // lifecycle data always carries nullable diagnostic metadata (autoresearch).
    return typeof result?.run_id === 'string' && result.run_id.length > 0
        && ['started', 'running', 'ok', 'failed', 'timeout', 'abandoned'].includes(result.status)
        && (!Object.hasOwn(result, 'error') || result.error === null || typeof result.error === 'string');
}

export function requireRead(result) {
    const code = readErrorCode(result);
    if (code) {
        const error = new Error(safeRequestMessage({ ...result, code }));
        error.status = result.status;
        error.code = code;
        throw error;
    }
    return result;
}

export function requireListPayload(result, key) {
    requireRead(result);
    if (!Array.isArray(result?.[key])) {
        throw Object.assign(new Error(safeRequestMessage({ code: 'invalid_response' })), { code: 'invalid_response' });
    }
    return result;
}

export function requireCountPayload(result) {
    requireRead(result);
    if (!Number.isSafeInteger(result?.count) || result.count < 0) {
        throw Object.assign(new Error(safeRequestMessage({ code: 'invalid_response' })), { code: 'invalid_response' });
    }
    return result;
}

// Race even an uncooperative fetch/body/mock against cancellation. Abort alone
// is insufficient when a response parser or an old adapter ignores its signal.
export function raceCancellation(operation, signal) {
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
        const abort = () => { cleanup(); reject(signal.reason); };
        const cleanup = () => signal.removeEventListener('abort', abort);
        signal.addEventListener('abort', abort, { once: true });
        Promise.resolve(operation).then(
            value => { cleanup(); resolve(value); },
            error => { cleanup(); reject(error); },
        );
    });
}

export function createReadScope({ force = false, timeoutMs = READ_DEADLINE_MS } = {}) {
    const controller = new AbortController();
    const timer = timeoutMs == null ? null : setTimeout(() => controller.abort(requestFailure('timeout')), timeoutMs);
    return {
        signal: controller.signal,
        options: { signal: controller.signal, gridForce: force },
        run: operation => raceCancellation(operation, controller.signal),
        cancel: () => { clearTimeout(timer); controller.abort(requestFailure('cancelled')); },
        finish: () => { clearTimeout(timer); controller.abort(requestFailure('cancelled')); },
    };
}
