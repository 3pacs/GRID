import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { readFileSync, writeFileSync } from 'fs'
import { resolve } from 'path'

// Stamp the service worker with a build hash so browsers pick up new versions.
function swVersionPlugin() {
    return {
        name: 'sw-version-stamp',
        closeBundle() {
            const swPath = resolve(__dirname, '../pwa_dist/service-worker.js');
            try {
                const stamp = `grid-${Date.now()}`;
                let content = readFileSync(swPath, 'utf-8');
                content = content.replace(/grid-v1/g, stamp);
                writeFileSync(swPath, content, 'utf-8');
                console.log(`[sw-version] stamped service worker → ${stamp}`);
            } catch { /* service worker may not exist in dev */ }
        },
    };
}

export default defineConfig({
    root: '.',
    plugins: [
        react(),
        swVersionPlugin(),
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
