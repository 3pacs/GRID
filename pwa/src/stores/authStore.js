/**
 * Auth store slice — token, role, login/logout.
 */
import { create } from 'zustand';
import {
    clearAuthSession,
    getStoredRole,
    getStoredToken,
    getStoredUsername,
    writeAuthSession,
} from '../authSession.js';

const useAuthStore = create((set) => ({
    token: getStoredToken() || null,
    isAuthenticated: !!getStoredToken(),
    userRole: getStoredRole(),
    username: getStoredUsername(),

    setAuth: (token, role = 'admin', username = 'operator') => {
        writeAuthSession(token, role, username);
        set({ token, isAuthenticated: true, userRole: role, username });
    },

    clearAuth: () => {
        clearAuthSession();
        set({ token: null, isAuthenticated: false, userRole: 'admin', username: 'operator' });
    },
}));

export default useAuthStore;
