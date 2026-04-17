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
