import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
    root: '.',
    base: '/astrogrid/',
    plugins: [
        react(),
    ],
    build: {
        outDir: '../astrogrid_dist',
        emptyOutDir: true,
    },
    server: {
        proxy: {
            '/api': 'http://localhost:8000',
        },
    },
})
