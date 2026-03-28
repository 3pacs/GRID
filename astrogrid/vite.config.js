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
    },
    server: {
        proxy: {
            '/api': 'http://localhost:8000',
        },
    },
})
