/**
 * Prueba de extremo a extremo (útil para demostrar el taller y validar el despliegue).
 *
 *   npm run test:e2e                                   (contra localhost:8080)
 *   CHAT_URL=https://TU-SERVICIO.a.run.app npm run test:e2e
 *
 * Verifica:
 *  1. Unario por gRPC nativo (ListChannels)
 *  2. Server-streaming por gRPC-Web, como lo hace el navegador (Subscribe)
 *  3. Streaming bidireccional por gRPC nativo (Chat)
 *  4. Que un mensaje enviado por el stream bidireccional llega al suscriptor web
 *  5. Subida y descarga de un adjunto binario
 */
import { randomUUID } from "node:crypto";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport, createGrpcWebTransport } from "@connectrpc/connect-node";
import { ChatService, ClientEventSchema, type ClientEvent } from "../gen/chat/v1/chat_pb.js";
import { AsyncQueue } from "../server/async-queue.js";

const url = process.env.CHAT_URL ?? "http://localhost:8080";
const grpc = createClient(ChatService, createGrpcTransport({ baseUrl: url }));
const grpcWeb = createClient(ChatService, createGrpcWebTransport({ baseUrl: url, httpVersion: "1.1" }));

const ok = (msg: string) => console.log(`  ✅ ${msg}`);
const timeout = <T>(p: Promise<T>, ms: number, what: string) =>
  Promise.race([p, new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`timeout: ${what}`)), ms))]);

async function main() {
  console.log(`Probando ${url}\n`);

  // 1. Unario
  const { channels } = await grpc.listChannels({});
  if (!channels.length) throw new Error("sin canales");
  ok(`ListChannels (gRPC/HTTP2): ${channels.map((c) => "#" + c.name).join(" ")}`);

  // 2. Server-streaming por gRPC-Web (HTTP/1.1, igual que el navegador)
  const webUser = { id: randomUUID(), name: "tester-web", color: "#6366f1" };
  const abort = new AbortController();
  const webStream = grpcWeb.subscribe({ channelId: "general", user: webUser }, { signal: abort.signal });
  const webIt = webStream[Symbol.asyncIterator]();
  const firstWeb = await timeout(webIt.next(), 5000, "subscribe");
  if (firstWeb.value?.event.case !== "heartbeat") throw new Error("subscribe no confirmó");
  ok("Subscribe (gRPC-Web/HTTP1.1) abierto");

  // 3. Bidireccional
  const cliUser = { id: randomUUID(), name: "tester-cli", color: "#10b981" };
  const out = new AsyncQueue<ClientEvent>();
  out.push(create(ClientEventSchema, { event: { case: "join", value: { channelId: "general", user: cliUser } } }));
  const bidi = grpc.chat(out)[Symbol.asyncIterator]();
  const text = `hola desde el stream bidireccional ${Date.now()}`;
  out.push(create(ClientEventSchema, { event: { case: "typing", value: { isTyping: true } } }));
  out.push(create(ClientEventSchema, { event: { case: "send", value: { text } } }));

  // El propio stream bidi debe devolvernos nuestro mensaje (eco del broadcast)
  for (;;) {
    const r = await timeout(bidi.next(), 5000, "bidi echo");
    if (r.done) throw new Error("bidi cerrado");
    if (r.value.event.case === "message" && r.value.event.value.text === text) break;
  }
  ok("Chat (bidireccional) envió y recibió en la misma conexión");

  // 4. El suscriptor web debe recibir typing + mensaje
  let sawTyping = false;
  for (;;) {
    const r = await timeout(webIt.next(), 5000, "web recibe");
    if (r.done) throw new Error("subscribe cerrado");
    if (r.value.event.case === "typing" && r.value.event.value.user?.name === "tester-cli") sawTyping = true;
    if (r.value.event.case === "message" && r.value.event.value.text === text) break;
  }
  ok(`El navegador recibió el mensaje en tiempo real${sawTyping ? " (y el indicador de escritura)" : ""}`);

  // 5. Adjuntos
  const bytes = new TextEncoder().encode("contenido de prueba 📎");
  const att = await grpcWeb.uploadAttachment({ fileName: "prueba.txt", mimeType: "text/plain", data: bytes });
  await grpcWeb.sendMessage({ channelId: "general", author: webUser, text: "adjunto", attachmentId: att.id });
  const dl = await grpcWeb.downloadAttachment({ id: att.id });
  if (new TextDecoder().decode(dl.data) !== "contenido de prueba 📎") throw new Error("adjunto corrupto");
  ok(`Upload/Download de adjunto binario (${att.size} bytes)`);

  // 6. Long-polling (respaldo cuando la red retiene los streams)
  const pollUser = { id: randomUUID(), name: "tester-poll", color: "#f59e0b" };
  const sessionId = randomUUID();
  const p1 = await grpcWeb.pollEvents({ channelId: "general", user: pollUser, sessionId });
  if (p1.events[0]?.event.case !== "heartbeat") throw new Error("poll no confirmó");
  const pending = grpcWeb.pollEvents({ channelId: "general", user: pollUser, sessionId });
  const pollText = `hola por long-polling ${Date.now()}`;
  setTimeout(() => grpcWeb.sendMessage({ channelId: "general", author: webUser, text: pollText }), 300);
  const p2 = await timeout(pending, 25000, "poll espera");
  if (!p2.events.some((e) => e.event.case === "message" && e.event.value.text === pollText)) {
    throw new Error("poll no recibió el mensaje");
  }
  ok("PollEvents (long-polling unario de respaldo) recibió el mensaje");

  out.close();
  abort.abort();
  console.log("\n🎉 Todo funciona.");
  process.exit(0);
}

main().catch((e) => {
  console.error("\n❌", e.message ?? e);
  process.exit(1);
});
