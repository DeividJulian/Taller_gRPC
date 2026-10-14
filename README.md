# 💬 gRPC Chat — Taller gRPC (Programación Orientada a la Web · UCC)

**Caso de estudio 5:** Sistema de chat colaborativo y mensajería instantánea estilo Slack/Discord.
Usuarios conectados a canales de texto que envían mensajes, comparten archivos adjuntos y ven
indicadores de escritura en tiempo real, usando **gRPC con streaming bidireccional** sobre una sola
conexión **HTTP/2**, desplegado en la **nube**.

---

## Índice

1. [Qué se construyó](#1-qué-se-construyó)
2. [Arquitectura](#2-arquitectura)
3. [Requisitos previos](#3-requisitos-previos)
4. [Paso a paso: cómo se construyó](#4-paso-a-paso-cómo-se-construyó)
5. [Ejecutar en local](#5-ejecutar-en-local)
6. [Desplegar en la nube](#6-desplegar-en-la-nube)
7. [Guion para la demostración](#7-guion-para-la-demostración)
8. [Relación con la teoría del curso](#8-relación-con-la-teoría-del-curso)
9. [Problemas comunes](#9-problemas-comunes)

---

## 1. Qué se construyó

| Componente | Tecnología | Tipo de RPC gRPC |
|---|---|---|
| **Contrato** `proto/chat/v1/chat.proto` | Protocol Buffers v3 | Define los 8 RPC |
| **Servidor** `server/` | Node.js + TypeScript + Connect (`@connectrpc/connect-node`) | Atiende gRPC nativo **y** gRPC-Web en el mismo puerto |
| **Cliente web** `web/` | React + Vite + `@connectrpc/connect-web` | gRPC-Web: unarios + **server-streaming** |
| **Cliente terminal** `cli/` | Node.js + `@connectrpc/connect-node` | gRPC nativo: **streaming bidireccional** |
| **Prueba E2E** `scripts/e2e-test.ts` | Node.js | Verifica los tres tipos de RPC |

Funcionalidades: 4 canales (#general, #grpc, #proyectos, #random), historial, lista de usuarios en
línea (presencia), indicador "está escribiendo...", adjuntos binarios (imágenes con vista previa, hasta
3 MB), reconexión automática y diseño responsive (móvil).

### Los RPC del contrato

```proto
service ChatService {
  rpc Chat(stream ClientEvent) returns (stream ServerEvent);   // ★ bidireccional
  rpc Subscribe(SubscribeRequest) returns (stream ServerEvent); // server-streaming
  rpc PollEvents(...)         returns (...);                    // respaldo: long-polling
  rpc ListChannels(...)       returns (...);                    // unarios
  rpc GetHistory(...)         returns (...);
  rpc SendMessage(...)        returns (ChatMessage);
  rpc SetTyping(...)          returns (...);
  rpc UploadAttachment(...)   returns (Attachment);             // bytes binarios
  rpc DownloadAttachment(...) returns (...);
}
```

### ⚠️ Punto clave (para explicarlo al profesor)

Los **navegadores no pueden hacer gRPC nativo**: JavaScript no tiene acceso a las tramas HTTP/2 ni a
los *trailers* que gRPC necesita. Por eso existe **gRPC-Web** (diapositiva 3 del curso), que soporta
llamadas **unarias y server-streaming**, pero **no streaming bidireccional**.

Solución de este proyecto:

- El **navegador** recibe en tiempo real con `Subscribe` (server-streaming) y envía con RPC unarios.
- El **cliente de terminal** usa el RPC `Chat` con **streaming bidireccional real**: envía y recibe a la
  vez sobre una sola conexión HTTP/2.
- Ambos comparten el mismo *hub*, así que **la terminal y la web chatean entre sí**.
- **Respaldo automático (long-polling):** algunos equipos (por ejemplo, PCs de laboratorio con
  antivirus o software de seguridad) retienen las respuestas HTTP que no terminan, y el stream nunca
  llega al navegador. La app lo detecta en 8 s y cambia sola al RPC unario `PollEvents`, que espera
  hasta 20 s a que haya eventos y los devuelve en lote. El indicador inferior muestra
  "Long-polling activo" en lugar de "Stream activo", y ambos modos chatean entre sí.

Además, en lugar de un proxy **Envoy** separado (que es lo que se ve en clase para traducir gRPC ↔
gRPC-Web), el servidor usa **Connect**, que habla gRPC, gRPC-Web y Connect de forma nativa. Así se
despliega **un solo contenedor** en la nube.

---

## 2. Arquitectura

```
                         ┌──────────────── NUBE (Cloud Run / Render) ───────────────┐
                         │                                                           │
 ┌─────────────┐  HTTPS  │   ┌──────────┐      ┌──────────────────────────────────┐ │
 │ Navegador   │─────────┼──▶│Balanceador│─────▶│  Contenedor Node.js (PORT 8080)  │ │
 │ React       │gRPC-Web │   │  (TLS)    │      │                                  │ │
 │ (Subscribe  │         │   └──────────┘      │  net.Server: mira los 1ros bytes │ │
 │  + unarios) │         │         ▲            │   "PRI * HTTP/2.0" → servidor h2c│ │
 └─────────────┘         │         │            │   otro           → servidor HTTP/1│ │
                         │         │            │            │                     │ │
 ┌─────────────┐  HTTP/2 │         │            │  connectNodeAdapter (gRPC,       │ │
 │ CLI Node    │─────────┼─────────┘            │  gRPC-Web, Connect)              │ │
 │ (Chat bidi) │ gRPC    │                      │            │                     │ │
 └─────────────┘         │                      │      ┌─────▼─────┐   dist/ (web) │ │
                         │                      │      │  ChatHub  │  archivos     │ │
                         │                      │      │ canales,  │  estáticos    │ │
                         │                      │      │ presencia,│               │ │
                         │                      │      │ adjuntos  │               │ │
                         │                      │      └───────────┘               │ │
                         │                      └──────────────────────────────────┘ │
                         └───────────────────────────────────────────────────────────┘
```

**¿Por qué un solo puerto?** Las plataformas cloud solo exponen el puerto de la variable `PORT`. Todo
cliente HTTP/2 sin TLS (h2c) empieza la conexión con el prefacio `PRI * HTTP/2.0`; el servidor lee esos
bytes y entrega el socket al servidor HTTP/2 o al HTTP/1.1 según corresponda (ver `server/index.ts`).

**Fan-out en el Hub:** cada conexión abierta (stream) tiene una cola (`AsyncQueue`). Cuando alguien
envía un mensaje, el `ChatHub` lo empuja a la cola de cada conexión del canal, y cada stream lo
entrega a su cliente con `yield`.

### Estructura de carpetas

```
grpc-chat/
├── proto/chat/v1/chat.proto   ← CONTRATO (fuente única de verdad)
├── buf.yaml / buf.gen.yaml    ← configuración de Buf (lint + generación de código)
├── gen/chat/v1/chat_pb.ts     ← código GENERADO desde el .proto (no editar a mano)
├── server/
│   ├── index.ts               ← arranque, un puerto para HTTP/1.1 + HTTP/2
│   ├── chat-service.ts        ← implementación de cada RPC
│   ├── hub.ts                 ← estado: canales, historial, presencia, adjuntos
│   ├── async-queue.ts         ← cola asíncrona para los streams
│   └── static.ts              ← sirve el frontend compilado
├── web/                       ← cliente React (gRPC-Web)
│   ├── index.html
│   └── src/ (App.tsx, api.ts, styles.css, main.tsx)
├── cli/chat-cli.ts            ← cliente de terminal (bidireccional)
├── scripts/e2e-test.ts        ← prueba de extremo a extremo
├── Dockerfile                 ← imagen para la nube
├── deploy-cloudrun.sh         ← despliegue en Google Cloud Run
└── render.yaml                ← despliegue alternativo en Render
```

---

## 3. Requisitos previos

1. **Node.js 20 o superior** → `node -v` (recomendado 22 LTS: https://nodejs.org)
2. **Git** → para subir el proyecto a GitHub (requerido por Render).
3. Un editor (VS Code).
4. Para la nube, **una** de estas opciones:
   - **Google Cloud Run** (recomendado: soporta HTTP/2 de extremo a extremo → el streaming
     bidireccional funciona también en la nube). Necesita una cuenta de Google Cloud con
     facturación activa; para un taller como este normalmente queda dentro del nivel gratuito.
     Instala la CLI: https://cloud.google.com/sdk/docs/install
   - **Render** (gratis, sin tarjeta). La web funciona completa; el cliente bidireccional se
     demuestra en local (ver sección 6.2).

---

## 4. Paso a paso: cómo se construyó

> Esta sección explica cada fase para que puedas sustentarla. El código ya está completo en el zip.

### Paso 1 — Crear el proyecto e instalar dependencias

```bash
mkdir grpc-chat && cd grpc-chat
npm init -y
npm install @bufbuild/protobuf @connectrpc/connect @connectrpc/connect-node @connectrpc/connect-web react react-dom tsx
npm install -D @bufbuild/buf @bufbuild/protoc-gen-es typescript vite @vitejs/plugin-react @types/node @types/react @types/react-dom
```

- `@bufbuild/buf`: herramienta para validar (*lint*) el `.proto` y generar código.
- `@bufbuild/protoc-gen-es`: plugin que genera TypeScript a partir del `.proto`.
- `@connectrpc/*`: implementación de gRPC / gRPC-Web para Node y navegador.

### Paso 2 — Diseñar el contrato primero (Contract-First)

Se escribe `proto/chat/v1/chat.proto` **antes** que cualquier código. Decisiones importantes:

- `ClientEvent` y `ServerEvent` usan `oneof`: un mismo stream transporta distintos tipos de eventos
  (unirse, mensaje, escribiendo, presencia, latido).
- `bytes data` en los adjuntos: Protobuf transmite binario directo, sin convertir a Base64 como haría
  JSON (≈33 % menos datos).
- `int64 sent_at`: en TypeScript se genera como `bigint`.

### Paso 3 — Generar el código

```bash
npx buf lint       # valida el estilo del contrato
npx buf generate   # crea gen/chat/v1/chat_pb.ts
```

`buf.gen.yaml` le dice a Buf que use `protoc-gen-es` con salida TypeScript. El archivo generado
contiene los tipos de todos los mensajes (`ChatMessage`, `User`...) y el descriptor `ChatService`, que
usan tanto el servidor como los clientes. **Si el contrato cambia, el compilador marca los errores en
ambos lados** (tipado end-to-end).

### Paso 4 — El Hub (estado compartido) — `server/hub.ts`

Guarda canales, historial (200 mensajes por canal), adjuntos (en memoria) y las conexiones activas.
Métodos clave: `join()` registra un stream y avisa la presencia; `postMessage()` guarda y difunde;
`setTyping()` difunde el indicador a todos **menos** a quien escribe; `heartbeat()` envía un latido cada
15 s para que los proxies de la nube no cierren los streams inactivos.

### Paso 5 — Implementar los RPC — `server/chat-service.ts`

**Bidireccional (`chat`)** — el corazón del taller:

```ts
async *chat(requests, ctx) {
  const first = await it.next();             // 1. el primer evento debe ser "join"
  const { queue, leave } = hub.join(channelId, user);
  for (const m of history) yield ...;         // 2. enviamos el historial
  void (async () => {                         // 3. LECTOR: lo que el cliente envía
    for (;;) { const r = await it.next(); if (r.done) break; handle(r.value); }
  })();
  for await (const ev of queue) yield ev;     // 4. ESCRITOR: lo que el cliente recibe
}
```

Lectura y escritura corren **en paralelo** sobre la misma llamada: eso es streaming bidireccional.

**Server-streaming (`subscribe`)**: igual, pero solo escribe. Lo usa el navegador.

**Unarios**: validan entradas y lanzan `ConnectError` con códigos gRPC estándar (`NOT_FOUND`,
`INVALID_ARGUMENT`, `RESOURCE_EXHAUSTED`).

### Paso 6 — Servidor de un solo puerto — `server/index.ts`

1. `connectNodeAdapter` registra el servicio y un `fallback` que sirve el frontend y `/healthz`.
2. Se crean un servidor `http` (HTTP/1.1) y uno `http2` (h2c) con el mismo handler.
3. Un `net.Server` escucha en `PORT`, detecta el prefacio HTTP/2 y entrega cada conexión al servidor
   correcto.

### Paso 7 — Cliente web — `web/src/`

- `api.ts` crea el transporte `createGrpcWebTransport` con formato **binario** Protobuf.
- `App.tsx`:
  - `listChannels` (unario) al iniciar.
  - `subscribe` (server-streaming) por canal, con **reconexión automática** y *backoff*.
  - `getHistory` (unario) que se fusiona sin duplicados con lo que llega por el stream.
  - `setTyping` al escribir (se apaga tras 2,5 s sin teclear).
  - Adjuntos: `File → Uint8Array → uploadAttachment → sendMessage`; descarga con
    `downloadAttachment → Blob → URL`.

### Paso 8 — Cliente de terminal bidireccional — `cli/chat-cli.ts`

Crea una cola de salida (`AsyncQueue<ClientEvent>`) y llama `client.chat(cola)`. Cada línea escrita
se empuja a la cola (sale al servidor) mientras el `for await` imprime lo que llega. Las pulsaciones
del teclado generan eventos `typing`.

### Paso 9 — Contenedor — `Dockerfile`

Build en dos etapas: la primera compila el frontend con Vite; la segunda solo instala dependencias de
producción y copia `dist/`, `gen/` y `server/`. Escucha en `$PORT`.

---

## 5. Ejecutar en local

```bash
# 1. Instalar dependencias
npm install

# 2. (Opcional) regenerar el código desde el .proto
npm run generate

# 3. Compilar el frontend
npm run build

# 4. Iniciar el servidor
npm start
#   🚀 gRPC Chat escuchando en http://0.0.0.0:8080
```

Abre **http://localhost:8080** en dos navegadores distintos (o una ventana normal y otra de incógnito),
entra con nombres distintos y chatea.

**Terminal (bidireccional)** — en otra consola:

```bash
npm run cli -- --name Deivid --channel general
```

**Prueba automática:**

```bash
npm run test:e2e
```

Resultado esperado:

```
  ✅ ListChannels (gRPC/HTTP2): #general #grpc #proyectos #random
  ✅ Subscribe (gRPC-Web/HTTP1.1) abierto
  ✅ Chat (bidireccional) envió y recibió en la misma conexión
  ✅ El navegador recibió el mensaje en tiempo real (y el indicador de escritura)
  ✅ Upload/Download de adjunto binario (24 bytes)
  ✅ PollEvents (long-polling unario de respaldo) recibió el mensaje
🎉 Todo funciona.
```

**Modo desarrollo** (recarga en caliente): `npm run dev:server` en una consola y `npm run dev:web` en
otra; abre http://localhost:5173 (Vite reenvía las llamadas gRPC al puerto 8080).

---

## 6. Desplegar en la nube

### 6.1 Opción A — Google Cloud Run (recomendada)

Cloud Run permite **HTTP/2 de extremo a extremo** (`--use-http2`), así que el streaming bidireccional
funciona también contra la URL pública.

1. Crea un proyecto en https://console.cloud.google.com y activa la facturación.
2. Instala la CLI de Google Cloud e inicia sesión:
   ```bash
   gcloud auth login
   ```
3. Desde la carpeta del proyecto ejecuta el script:
   ```bash
   ./deploy-cloudrun.sh TU_PROJECT_ID us-central1
   ```
   En Windows (PowerShell), ejecuta los comandos equivalentes:
   ```powershell
   gcloud config set project TU_PROJECT_ID
   gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com
   gcloud run deploy grpc-chat --source . --region us-central1 --allow-unauthenticated `
     --use-http2 --max-instances 1 --session-affinity --timeout 3600 --memory 512Mi
   ```
4. Al terminar, verás la URL (`https://grpc-chat-xxxxx.us-central1.run.app`). Pruébala:
   ```bash
   CHAT_URL=https://grpc-chat-xxxxx.us-central1.run.app npm run test:e2e
   npm run cli -- --url https://grpc-chat-xxxxx.us-central1.run.app --name Deivid
   ```
   (En PowerShell: `$env:CHAT_URL="https://..."; npm run test:e2e`)

**Por qué cada parámetro:**

| Parámetro | Motivo |
|---|---|
| `--use-http2` | Cloud Run reenvía HTTP/2 (h2c) al contenedor → gRPC nativo y bidireccional |
| `--max-instances 1` | El estado vive en memoria: todos deben llegar a la misma instancia |
| `--session-affinity` | Mantiene a cada cliente en la misma instancia |
| `--timeout 3600` | Los streams pueden durar hasta 60 min; luego el cliente web se reconecta solo |
| `--allow-unauthenticated` | Acceso público para la demo |

### 6.2 Opción B — Render (gratis, sin tarjeta)

1. Sube el proyecto a un repositorio de GitHub:
   ```bash
   git init && git add . && git commit -m "Taller gRPC: chat colaborativo"
   git branch -M main
   git remote add origin https://github.com/TU_USUARIO/grpc-chat.git
   git push -u origin main
   ```
2. En https://dashboard.render.com → **New → Blueprint** → elige el repositorio (Render lee
   `render.yaml`). También puedes usar **New → Web Service**, runtime **Docker**, plan **Free**.
3. Espera el build y abre la URL `https://grpc-chat-xxxx.onrender.com`.

En Render la **aplicación web funciona completa** (gRPC-Web sobre HTTPS). El proxy de Render habla
HTTP/1.1 con el contenedor, así que el **cliente bidireccional** debes demostrarlo en local
(`npm start` + `npm run cli`) o usar Cloud Run. El plan gratuito "duerme" tras unos minutos sin
tráfico: la primera carga puede tardar ~1 minuto.

---

## 7. Guion para la demostración

1. **Contrato**: muestra `chat.proto` y explica `stream ClientEvent` ↔ `stream ServerEvent`.
2. **Generación**: `npx buf generate` y muestra que `gen/chat_pb.ts` tiene los tipos.
3. **Nube**: abre la URL pública en el portátil y en el celular con nombres distintos.
4. **Tiempo real**: escribe en uno → aparece "está escribiendo..." en el otro → envía el mensaje.
5. **Adjuntos**: sube una imagen y muestra la vista previa en el otro dispositivo.
6. **Bidireccional**: en la terminal, `npm run cli -- --url <URL> --name Terminal`; escribe desde la
   terminal y responde desde la web.
7. **DevTools → Network**: muestra las peticiones `chat.v1.ChatService/Subscribe` con
   `content-type: application/grpc-web+proto` (binario, no JSON).
8. **Prueba E2E** contra la URL pública.

---

## 8. Relación con la teoría del curso

| Concepto (diapositivas) | Dónde se aplica |
|---|---|
| **Contract-First / SSOT** | `chat.proto` se diseñó antes del código y genera los tipos de servidor y clientes |
| **Generación de código** | `buf generate` → `gen/chat/v1/chat_pb.ts` |
| **Tipado end-to-end** | Servidor y clientes importan los mismos tipos generados; un cambio en el contrato rompe la compilación en ambos lados |
| **Falsa seguridad de TypeScript** | Protobuf **parsea** los bytes en runtime con el esquema (no es un `as User`); además el servidor valida y sanea entradas (`sanitizeUser`, límites de tamaño) |
| **Protobuf binario** | Mensajes y adjuntos viajan como bytes; el cliente web usa `useBinaryFormat: true` |
| **HTTP/2 multiplexación** | El CLI usa una sola conexión TCP para el stream y cualquier otra llamada |
| **Streaming bidireccional** | RPC `Chat` (cliente de terminal) |
| **Resiliencia** | Si la red retiene el stream, el cliente cambia a long-polling unario (`PollEvents`) sin perder funcionalidad |
| **gRPC-Web + proxy** | Connect cumple el papel de Envoy dentro del mismo proceso |
| **Funciones remotas vs. recursos REST** | El cliente llama `chatClient.sendMessage(...)` como si fuera un método local |

---

## 9. Problemas comunes

| Síntoma | Solución |
|---|---|
| `Frontend no compilado` al abrir la web | Ejecuta `npm run build` antes de `npm start` |
| Abajo dice **"Long-polling activo"** en vez de "Stream activo" | Algo en ese equipo (antivirus, software del laboratorio, extensión o proxy) retiene los streams HTTP. La app ya cambió sola al modo de respaldo y funciona igual. Para ver el streaming en la demo, usa otro equipo, una ventana de incógnito u otro navegador, o la URL de la nube (HTTPS) |
| El CLI contra la nube da error de conexión | En Cloud Run, verifica que activaste `--use-http2`. En Render el bidireccional no está disponible (usa local) |
| Mensajes no llegan entre usuarios en la nube | Asegura `--max-instances 1` (estado en memoria) |
| "Reconectando..." cada cierto tiempo | Normal: el proxy de la nube cierra streams largos y el cliente se reconecta solo |
| `EADDRINUSE :8080` | Otro proceso usa el puerto: `PORT=3000 npm start` (PowerShell: `$env:PORT=3000; npm start`) |
| Los mensajes se borran al reiniciar | El historial vive en memoria (decisión de diseño del taller). Para persistir, se podría conectar PostgreSQL en `hub.ts` |
| `./deploy-cloudrun.sh: Permission denied` | `chmod +x deploy-cloudrun.sh` (o ejecuta los comandos manualmente) |
