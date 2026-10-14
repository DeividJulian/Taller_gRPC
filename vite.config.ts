import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  root: "web",
  plugins: [react()],
  build: {
    outDir: "../dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    // En desarrollo, las llamadas gRPC-Web se reenvían al servidor Node
    proxy: {
      "/chat.v1.ChatService": "http://localhost:8080",
    },
  },
});
