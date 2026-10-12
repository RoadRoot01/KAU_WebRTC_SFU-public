import { defineConfig } from 'vite';
import basicSsl from '@vitejs/plugin-basic-ssl';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const configDirectory = dirname(fileURLToPath(import.meta.url));

// PORT는 backend/signaling port이며 browser-facing Vite port와는 별개다.
// 기본값은 server.ts와 동일한 3000이다.
// 이상값(NaN, 범위 밖)은 기본 3000으로 되돌려 잘못된 proxy target을 방지.
const parsedPort = Number(process.env.PORT ?? 3000);
const signalingPort =
  Number.isInteger(parsedPort) && parsedPort > 0 && parsedPort < 65536
    ? parsedPort
    : 3000;
const signalingTarget = `ws://127.0.0.1:${signalingPort}`;
const healthTarget = `http://127.0.0.1:${signalingPort}`;
const proxy = {
  '/ws': {
    target: signalingTarget,
    ws: true
  },
  '/health': {
    target: healthTarget
  }
};

export default defineConfig({
  plugins: [
    basicSsl()
  ],
  server: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    proxy
  },
  // Production dist도 dev와 같은 HTTPS browser endpoint를 사용한다.
  preview: {
    host: '0.0.0.0',
    port: 5173,
    strictPort: true,
    proxy
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        index: resolve(configDirectory, 'index.html'),
        statsVisualizer: resolve(configDirectory, 'stats-visualizer.html')
      },
      output: {
        manualChunks(id) {
          if (id.includes('/node_modules/chart.js/')) {
            return 'chartjs';
          }
        }
      }
    }
  }
});
