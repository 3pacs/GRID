import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    root: '.',
    base: './',
    plugins: [
        react(),
    ],
    build: {
        outDir: 'dist',
        emptyOutDir: true,
        rollupOptions: {
            output: {
                manualChunks(id) {
                    if (id.includes('node_modules')) {
                        if (id.includes('three') || id.includes('@react-three')) return 'three-stack';
                        if (id.includes('d3')) return 'd3-stack';
                    }
                    return null;
                },
            },
        },
    },
    server: {
        proxy: {
            '/api': 'http://localhost:8000',
        },
    },
})
