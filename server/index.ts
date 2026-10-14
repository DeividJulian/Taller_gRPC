/**
 * Punto de entrada del servidor.
 *
 * UN SOLO PUERTO atiende dos protocolos (requisito de las plataformas cloud,
 * que exponen únicamente el puerto de la variable PORT):
 *
 *   • HTTP/2 sin TLS (h2c)  → gRPC nativo, incluido el STREAMING BIDIRECCIONAL
 *                              (cliente CLI, grpcurl, otros microservicios)
 *   • HTTP/1.1              → gRPC-Web desde el navegador + archivos estáticos
 *
 * ¿Cómo distingue uno del otro? Toda conexión HTTP/2 "prior knowledge" empieza
 * con el prefacio "PRI * HTTP/2.0". Miramos los primeros bytes y entregamos el
 * socket al servidor correspondiente. El TLS lo termina el balanceador de la nube.
 */
import http from "node:http";
import http2 from "node:http2";
import net from "node:net";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import type { ConnectRouter } from "@connectrpc/connect";
import { ChatService } from "../gen/chat/v1/chat_pb.js";
import { ChatHub, MAX_ATTACHMENT_BYTES } from "./hub.js";
import { createChatService } from "./chat-service.js";
import { serveStatic } from "./static.js";

const PORT = Number(process.env.PORT ?? 8080);
const HOST = process.env.HOST ?? "0.0.0.0";

const hub = new ChatHub();

const routes = (router: ConnectRouter) => router.service(ChatService, createChatService(hub));

// El adaptador de Connect entiende los protocolos gRPC, gRPC-Web y Connect.
const handler = connectNodeAdapter({
  routes,
  readMaxBytes: MAX_ATTACHMENT_BYTES + 64 * 1024,
  // Todo lo que NO sea una llamada RPC (/, /assets/..., /healthz) cae aquí
  fallback: (req, res) => {
    if (req.url === "/healthz") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "ok", ...hub.stats() }));
      return;
    }
    serveStatic(req, res as unknown as Parameters<typeof serveStatic>[1]);
  },
});

const h1Server = http.createServer(handler);
const h2Server = http2.createServer(handler);

// Los streams de chat son de larga duración: desactivamos timeouts de Node
h1Server.requestTimeout = 0;
h1Server.headersTimeout = 60_000;
h1Server.keepAliveTimeout = 65_000;
h2Server.setTimeout(0);

const HTTP2_PREFACE = "PRI * HTTP/2.0";

const server = net.createServer((socket) => {
  socket.once("data", (chunk) => {
    socket.pause();
    socket.unshift(chunk); // devolvemos los bytes leídos para que el servidor real los procese
    const isHttp2 = chunk.subarray(0, HTTP2_PREFACE.length).toString("latin1") === HTTP2_PREFACE;
    (isHttp2 ? h2Server : h1Server).emit("connection", socket);
    process.nextTick(() => socket.resume());
  });
  socket.on("error", () => socket.destroy());
});

// Latido cada 15 s para que los proxies de la nube no cierren los streams inactivos
setInterval(() => hub.heartbeat(), 15_000).unref();

server.listen(PORT, HOST, () => {
  console.log(`🚀 gRPC Chat escuchando en http://${HOST}:${PORT}`);
  console.log(`   • gRPC nativo (HTTP/2 h2c) y gRPC-Web (HTTP/1.1) en el mismo puerto`);
});

const shutdown = () => {
  console.log("Cerrando servidor...");
  server.close();
  h2Server.close();
  h1Server.close();
  setTimeout(() => process.exit(0), 1000).unref();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
