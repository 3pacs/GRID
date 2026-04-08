/**
 * Auth store slice — token, role, login/logout.
 */
import { create } from 'zustand';

const useAuthStore = create((set) => ({
    token: localStorage.getItem('grid_token') || null,
    isAuthenticated: !!localStorage.getItem('grid_token'),
    userRole: localStorage.getItem('grid_role') || 'admin',
    username: localStorage.getItem('grid_username') || 'operator',

    setAuth: (token, role = 'admin', username = 'operator') => {
        localStorage.setItem('grid_token', token);
        localStorage.setItem('grid_role', role);
        localStorage.setItem('grid_username', username);
        set({ token, isAuthenticated: true, userRole: role, username });
    },

    clearAuth: () => {
        localStorage.removeItem('grid_token');
        localStorage.removeItem('grid_role');
        localStorage.removeItem('grid_username');
        set({ token: null, isAuthenticated: false, userRole: 'admin', username: 'operator' });
    },
}));

export default useAuthStore;
