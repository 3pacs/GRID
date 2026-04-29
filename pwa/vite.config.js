import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { readFileSync, writeFileSync } from 'fs'
import { resolve } from 'path'

const apiProxyTarget = process.env.GRID_API_PROXY_TARGET || 'http://127.0.0.1:8000';
const wsProxyTarget = process.env.GRID_WS_PROXY_TARGET || 'ws://127.0.0.1:8000';

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
        chunkSizeWarningLimit: 1200,
        rollupOptions: {
            onwarn(warning, warn) {
                if (
                    warning.message?.includes('"spawn" is not exported by "__vite-browser-external"') &&
                    warning.message?.includes('@loaders.gl/worker-utils')
                ) {
                    return;
                }
                warn(warning);
            },
            output: {
                manualChunks(id) {
                    if (!id.includes('node_modules')) return;
                    if (id.includes('/d3')) return 'd3';
                    if (id.includes('maplibre-gl')) return 'maplibre';
                    if (id.includes('@deck.gl/layers') || id.includes('@deck.gl/aggregation-layers')) return 'deck-layers';
                    if (id.includes('@deck.gl/core')) return 'deck-core';
                    if (id.includes('@deck.gl/react') || id.includes('deck.gl')) return 'deck-react';
                    if (id.includes('@loaders.gl')) return 'loaders';
                    if (id.includes('@luma.gl')) return 'luma';
                    if (id.includes('@math.gl')) return 'mathgl';
                    if (id.includes('mjolnir.js') || id.includes('probe.gl')) return 'interaction';
                    if (
                        id.includes('/react/') ||
                        id.includes('/react-dom/') ||
                        id.includes('/zustand/')
                    ) {
                        return 'vendor';
                    }
                },
            },
        },
    },
    server: {
        proxy: {
            '/api': {
                target: apiProxyTarget,
                changeOrigin: true,
            },
            '/ws': {
                target: wsProxyTarget,
                ws: true,
                changeOrigin: true,
            },
        }
    },
    test: {
        globals: true,
        environment: 'jsdom',
        setupFiles: ['./src/test-setup.js'],
    },
})
