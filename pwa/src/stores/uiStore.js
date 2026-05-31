/**
 * UI store slice — theme, active view, loading, errors, notifications.
 */
import { create } from 'zustand';

const useUiStore = create((set) => ({
    theme: localStorage.getItem('grid_theme') || 'dark',
    activeView: 'ten-year',
    loading: {},
    errors: {},
    notifications: [],

    setTheme: (name) => {
        localStorage.setItem('grid_theme', name);
        set({ theme: name });
    },

    setActiveView: (view) => set({ activeView: view }),

    setLoading: (key, value) => set(state => ({
        loading: { ...state.loading, [key]: value },
    })),

    setError: (key, error) => set(state => ({
        errors: { ...state.errors, [key]: error },
    })),

    addNotification: (type, message) => {
        const id = Date.now();
        set(state => ({
            notifications: [...state.notifications, { id, type, message }].slice(-5),
        }));
        setTimeout(() => {
            set(state => ({
                notifications: state.notifications.filter(n => n.id !== id),
            }));
        }, 5000);
    },

    removeNotification: (id) => set(state => ({
        notifications: state.notifications.filter(n => n.id !== id),
    })),
}));

export default useUiStore;
