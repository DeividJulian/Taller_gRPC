/**
 * Cliente de terminal con STREAMING BIDIRECCIONAL REAL (gRPC nativo sobre HTTP/2).
 *
 * Uso:
 *   npm run cli -- --url http://localhost:8080 --name Deivid --channel general
 *   npm run cli -- --url https://TU-SERVICIO.a.run.app --name Deivid
 *
 * Mientras escribes, el stream de SALIDA envía tus mensajes y el indicador de
 * "escribiendo"; al mismo tiempo el stream de ENTRADA muestra lo que publican
 * los demás (desde la web o desde otras terminales). Una sola conexión, dos sentidos.
 */
import readline from "node:readline";
import { randomUUID } from "node:crypto";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ChatService, ClientEventSchema, type ClientEvent } from "../gen/chat/v1/chat_pb.js";
import { AsyncQueue } from "../server/async-queue.js";

function arg(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i > -1 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const url = arg("url", process.env.CHAT_URL ?? "http://localhost:8080");
const name = arg("name", `cli-${Math.floor(Math.random() * 1000)}`);
const channel = arg("channel", "general");
const user = { id: randomUUID(), name, color: "#10b981" };

// Transporte gRPC NATIVO: HTTP/2 obligatorio (h2c en local, TLS en la nube)
const transport = createGrpcTransport({ baseUrl: url });
const client = createClient(ChatService, transport);

const c = {
  dim: (s: string) => `\x1b[2m${s}\x1b[0m`,
  bold: (s: string) => `\x1b[1m${s}\x1b[0m`,
  cyan: (s: string) => `\x1b[36m${s}\x1b[0m`,
  green: (s: string) => `\x1b[32m${s}\x1b[0m`,
  yellow: (s: string) => `\x1b[33m${s}\x1b[0m`,
};

// Cola de SALIDA: todo lo que pongamos aquí viaja al servidor por el stream
const outgoing = new AsyncQueue<ClientEvent>();
outgoing.push(create(ClientEventSchema, { event: { case: "join", value: { channelId: channel, user } } }));

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: c.dim("> ") });

function print(line: string) {
  readline.clearLine(process.stdout, 0);
  readline.cursorTo(process.stdout, 0);
  console.log(line);
  rl.prompt(true);
}

// Indicador "escribiendo..." a partir de las pulsaciones
let typing = false;
let typingTimer: NodeJS.Timeout | undefined;
process.stdin.on("keypress", (_s, key) => {
  if (key?.name === "return") return;
  if (!typing) {
    typing = true;
    outgoing.push(create(ClientEventSchema, { event: { case: "typing", value: { isTyping: true } } }));
  }
  clearTimeout(typingTimer);
  typingTimer = setTimeout(() => {
    typing = false;
    outgoing.push(create(ClientEventSchema, { event: { case: "typing", value: { isTyping: false } } }));
  }, 2000);
});

rl.on("line", (line) => {
  const text = line.trim();
  clearTimeout(typingTimer);
  typing = false;
  if (text === "/salir" || text === "/quit") return rl.close();
  if (text) outgoing.push(create(ClientEventSchema, { event: { case: "send", value: { text } } }));
  rl.prompt();
});

rl.on("close", () => {
  outgoing.close(); // cerramos nuestro lado del stream → el servidor nos saca del canal
  setTimeout(() => process.exit(0), 300);
});

console.log(c.bold(`\n💬 gRPC Chat CLI — conectando a ${url} como ${name} en #${channel}`));
console.log(c.dim("Escribe y presiona Enter. /salir para terminar.\n"));

const typingNow = new Set<string>();

try {
  // ★ Llamada bidireccional: le pasamos un iterable (salida) y recibimos otro (entrada)
  for await (const ev of client.chat(outgoing)) {
    switch (ev.event.case) {
      case "message": {
        const m = ev.event.value;
        const time = new Date(Number(m.sentAt)).toLocaleTimeString();
        if (m.system) print(c.dim(`   · ${m.text}`));
        else {
          const who = m.author?.id === user.id ? c.green(m.author?.name ?? "") : c.cyan(m.author?.name ?? "");
          const file = m.attachment ? c.yellow(` 📎 ${m.attachment.fileName}`) : "";
          print(`${c.dim(time)} ${c.bold(who)}: ${m.text}${file}`);
        }
        break;
      }
      case "typing": {
        const t = ev.event.value;
        if (t.isTyping) typingNow.add(t.user?.name ?? "");
        else typingNow.delete(t.user?.name ?? "");
        if (typingNow.size) print(c.dim(`   ✎ ${[...typingNow].join(", ")} está escribiendo...`));
        break;
      }
      case "presence":
        print(c.dim(`   👥 En línea: ${ev.event.value.users.map((u) => u.name).join(", ")}`));
        break;
    }
  }
  print(c.dim("Stream cerrado por el servidor."));
} catch (err) {
  console.error("\n❌ Error de conexión:", (err as Error).message);
  console.error("   ¿El servidor está activo? En la nube, ¿activaste HTTP/2 de extremo a extremo?");
}
process.exit(0);
