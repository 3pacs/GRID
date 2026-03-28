import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    root: '.',
    plugins: [
        react(),
    ],
    build: {
        outDir: '../pwa_dist',
        emptyOutDir: true,
        chunkSizeWarningLimit: 1000,
        rollupOptions: {
            output: {
                manualChunks: {
                    d3: ['d3'],
                    vendor: ['react', 'react-dom', 'zustand'],
                },
            },
        },
    },
    server: {
        proxy: {
            '/api': 'http://localhost:8000',
            '/ws': { target: 'ws://localhost:8000', ws: true }
        }
    },
    test: {
        globals: true,
        environment: 'jsdom',
        setupFiles: ['./src/test-setup.js'],
    },
})
