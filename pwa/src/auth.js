/**
 * Auth helpers for GRID PWA.
 */

import { api } from './api.js';

export function isAuthenticated() {
    return !!api.token;
}

export async function checkAuth() {
    if (!api.token) return false;
    try {
        const result = await api.verify();
        return result.valid;
    } catch {
        return false;
    }
}

export function clearAuth() {
    api.token = null;
    window.location.hash = '#/login';
}
