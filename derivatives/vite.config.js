import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    root: '.',
    base: '/derivatives/',
    plugins: [
        react(),
    ],
    build: {
        outDir: '../derivatives_dist',
        emptyOutDir: true,
    },
    server: {
        proxy: {
            '/api': 'http://localhost:8000',
        },
    },
})
