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
                manualChunks(id) {
                    if (id.includes('node_modules/react-dom') || id.includes('node_modules/react/') || id.includes('node_modules/zustand')) return 'vendor';
                    if (id.includes('node_modules/d3')) return 'd3';
                    if (id.includes('node_modules/uplot')) return 'viz-uplot';
                    if (id.includes('node_modules/echarts')) return 'viz-echarts';
                    if (id.includes('node_modules/sigma') || id.includes('node_modules/graphology')) return 'viz-sigma';
                    if (id.includes('node_modules/@finos/perspective')) return 'viz-perspective';
                    if (id.includes('node_modules/regl-scatterplot') || id.includes('node_modules/regl/')) return 'viz-regl';
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
