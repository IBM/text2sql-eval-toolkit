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
  build: {
    outDir: "dist"
  }
});

