import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true
      }
    }
  },
  resolve: {
    // Mermaid depends on KaTeX and the docs view uses it directly, and without
    // this the two resolve to different module instances -- two 260 KB copies
    // of the same library in the output.
    dedupe: ["katex"]
  },
  // Escape non-ASCII characters in the bundle. KaTeX's lexer has a regex range
  // that starts at U+202A, a bidi control character, and written raw it trips
  // Trojan Source scanners on every commit of dist/.
  esbuild: {
    charset: "ascii"
  },
  build: {
    outDir: "dist"
  }
});

