import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

const target = process.env.NEXO_WEB_PROXY_TARGET || 'http://127.0.0.1:8080'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': { target, changeOrigin: true },
      '/ws': { target, changeOrigin: true, ws: true },
    },
  },
  test: { include: ['src/**/*.test.ts'], environment: 'node' },
})
