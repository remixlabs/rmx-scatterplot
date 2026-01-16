import { defineConfig } from "vite";

// https://vitejs.dev/config/
export default defineConfig({
  build: {
    lib: {
      entry: "src/rmx-scatterplot.js",
      formats: ["es"],
      fileName: () => "rmx-scatterplot",
    },
    rollupOptions: {
      output: {
        entryFileNames: "rmx-scatterplot.js",
      },
    },
  },
  server: {
    proxy: {
      // anything that starts with /parquet will be forwarded to agt.files.remix.app
      "/parquet": {
        target: "https://agt.files.remix.app",
        changeOrigin: true,
        secure: true,
        rewrite: (path) => path.replace(/^\/parquet/, ""),
      },
    },
  },
});
