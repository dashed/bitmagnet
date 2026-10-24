import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  base: "/app/",
  plugins: [react()],
  build: {
    cssCodeSplit: true,
    reportCompressedSize: true,
    target: "es2022",
  },
  preview: {
    host: "127.0.0.1",
    port: 4174,
  },
  server: {
    host: "127.0.0.1",
    port: 5174,
  },
  test: {
    css: true,
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
  },
});
