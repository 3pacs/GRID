export const AUTH_TOKEN_KEY = 'grid_token';
export const AUTH_ROLE_KEY = 'grid_role';
export const AUTH_USERNAME_KEY = 'grid_username';

export function getStoredToken() {
    return localStorage.getItem(AUTH_TOKEN_KEY);
}

export function getStoredRole() {
    return localStorage.getItem(AUTH_ROLE_KEY) || 'admin';
}

export function getStoredUsername() {
    return localStorage.getItem(AUTH_USERNAME_KEY) || 'operator';
}

export function writeAuthSession(token, role = 'admin', username = 'operator') {
    localStorage.setItem(AUTH_TOKEN_KEY, token);
    localStorage.setItem(AUTH_ROLE_KEY, role);
    localStorage.setItem(AUTH_USERNAME_KEY, username);
}

export function clearAuthSession() {
    localStorage.removeItem(AUTH_TOKEN_KEY);
    localStorage.removeItem(AUTH_ROLE_KEY);
    localStorage.removeItem(AUTH_USERNAME_KEY);
}

// Decode the JWT payload (role/username) without verifying — for UI gating
// only. The token is the source of truth; localStorage values are a fallback.
function decodeTokenPayload() {
    try {
        const t = getStoredToken();
        if (!t) return null;
        const part = t.split('.')[1];
        if (!part) return null;
        const json = atob(part.replace(/-/g, '+').replace(/_/g, '/'));
        return JSON.parse(json);
    } catch {
        return null;
    }
}

export function getCurrentRole() {
    const p = decodeTokenPayload();
    return (p && p.role) || getStoredRole();
}

export function getCurrentUsername() {
    const p = decodeTokenPayload();
    return (p && p.sub) || getStoredUsername();
}

// Roles that get the simple, tailored shell (big text, just their two pages,
// none of the operator cockpit). Gate explicitly so any unexpected/admin role
// falls through to the full GRID app rather than getting locked into dad mode.
const SIMPLE_ROLES = new Set(['contributor']);

export function isSimpleUser() {
    return SIMPLE_ROLES.has(getCurrentRole());
}
