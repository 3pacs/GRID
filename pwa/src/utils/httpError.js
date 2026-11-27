// Readable text for a failed HTTP response (#959).
//
// A gateway in front of the API (Cloudflare 524 "A timeout occurred", 502, 503, ...) answers with a whole HTML
// page. The fetch wrapper used that page as the error message, so views printed raw markup
// ("<!DOCTYPE html> <!--[if lt IE 7]> ...") where an error belongs. An HTML or XML body is never shown: the
// status says what happened. Any other body (plain text, which the API sometimes uses) is kept as before.
const GATEWAY_TIMEOUT = new Set([504, 522, 523, 524]);
const GATEWAY_DOWN = new Set([502, 503, 521, 530]);

export function looksLikeMarkup(body, contentType = '') {
    if (/(text\/html|application\/xhtml|xml)/i.test(String(contentType || ''))) return true;
    return /^\s*(<!doctype\s|<html[\s>]|<\?xml|<!--)/i.test(String(body || ''));
}

export function httpErrorMessage(status, statusText, body, contentType = '') {
    if (looksLikeMarkup(body, contentType)) {
        if (GATEWAY_TIMEOUT.has(status)) {
            return `The server took too long to answer (HTTP ${status}). The request may still be running; try again shortly.`;
        }
        if (GATEWAY_DOWN.has(status)) {
            return `The server is unavailable right now (HTTP ${status}). Try again shortly.`;
        }
        return `The server returned an error page instead of data (HTTP ${status}).`;
    }
    if (body) return body;
    return statusText || `HTTP ${status}`;
}
