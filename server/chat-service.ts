import { create } from "@bufbuild/protobuf";
import { Code, ConnectError, type ServiceImpl } from "@connectrpc/connect";
import {
  ChatService,
  DownloadAttachmentResponseSchema,
  GetHistoryResponseSchema,
  ListChannelsResponseSchema,
  PollEventsResponseSchema,
  ServerEventSchema,
  TypingResponseSchema,
  type ClientEvent,
  type ServerEvent,
  type User,
} from "../gen/chat/v1/chat_pb.js";
import { ChatHub, MAX_ATTACHMENT_BYTES } from "./hub.js";
import { AsyncQueue } from "./async-queue.js";

/**
 * Implementación del servicio definido en chat.proto.
 * Cada método corresponde a un `rpc` del contrato.
 */
export function createChatService(hub: ChatHub): ServiceImpl<typeof ChatService> {
  const requireChannel = (id: string) => {
    if (!hub.hasChannel(id)) throw new ConnectError(`El canal "${id}" no existe`, Code.NotFound);
  };
  const requireUser = (u: User | undefined): User => {
    if (!u || !u.id || !u.name.trim()) throw new ConnectError("Se requiere un usuario con id y nombre", Code.InvalidArgument);
    return u;
  };

  // Sesiones de long-polling: cada una es un suscriptor "virtual" del Hub cuya
  // cola se vacía en cada llamada a PollEvents. Si el cliente deja de consultar
  // durante 30 s, la sesión expira y el usuario sale del canal.
  const POLL_WAIT_MS = 20_000;
  const SESSION_TTL_MS = 30_000;
  const pollSessions = new Map<
    string,
    { channelId: string; userId: string; queue: AsyncQueue<ServerEvent>; leave: () => void; expiry: NodeJS.Timeout }
  >();
  const closeSession = (id: string) => {
    const s = pollSessions.get(id);
    if (!s) return;
    clearTimeout(s.expiry);
    pollSessions.delete(id);
    s.leave();
    console.log(`[poll] - sesión ${id.slice(0, 8)} cerrada`);
  };

  return {
    // ---------------------------------------------------------------------
    // ★ STREAMING BIDIRECCIONAL ★
    // `requests` es un iterable asíncrono con lo que ENVÍA el cliente y lo que
    // hacemos `yield` es lo que RECIBE. Ambas direcciones fluyen a la vez,
    // sobre la misma conexión HTTP/2.
    // ---------------------------------------------------------------------
    async *chat(requests, ctx) {
      const it = requests[Symbol.asyncIterator]();

      // 1) El primer evento debe ser JoinChannel
      const first = await it.next();
      if (first.done || first.value.event.case !== "join") {
        throw new ConnectError("El primer evento del stream debe ser 'join'", Code.InvalidArgument);
      }
      const { channelId } = first.value.event.value;
      const user = requireUser(first.value.event.value.user);
      requireChannel(channelId);

      // 2) Enviamos el historial reciente al cliente que se acaba de unir
      const history = hub.getHistory(channelId, 50);
      const { queue, leave } = hub.join(channelId, user);
      for (const m of history) {
        yield create(ServerEventSchema, { event: { case: "message", value: m } });
      }

      // 3) Tarea LECTORA: consume en paralelo lo que el cliente envía
      const handle = (ev: ClientEvent) => {
        switch (ev.event.case) {
          case "send":
            if (ev.event.value.text.trim()) hub.postMessage(channelId, user, ev.event.value.text);
            break;
          case "typing":
            hub.setTyping(channelId, user, ev.event.value.isTyping);
            break;
        }
      };
      void (async () => {
        try {
          for (;;) {
            const r = await it.next();
            if (r.done) break; // el cliente cerró su lado del stream
            handle(r.value);
          }
        } catch {
          /* conexión interrumpida */
        } finally {
          leave();
        }
      })();
      ctx.signal.addEventListener("abort", leave);

      // 4) Tarea ESCRITORA: todo lo que llega al canal se reenvía al cliente
      try {
        for await (const ev of queue) yield ev;
      } finally {
        leave();
      }
    },

    // ---------------------------------------------------------------------
    // Server-streaming (lo usa el navegador mediante gRPC-Web)
    // ---------------------------------------------------------------------
    async *subscribe(req, ctx) {
      requireChannel(req.channelId);
      const user = requireUser(req.user);
      // Pide a proxies/antivirus que NO almacenen en buffer ni transformen la respuesta
      ctx.responseHeader.set("cache-control", "no-cache, no-transform");
      ctx.responseHeader.set("x-accel-buffering", "no");
      const { queue, leave } = hub.join(req.channelId, user);
      console.log(`[stream] + ${user.name} abrió Subscribe en #${req.channelId}`);
      ctx.signal.addEventListener("abort", () => {
        console.log(`[stream] - ${user.name} cerró Subscribe en #${req.channelId}`);
        leave();
      });
      // primer evento inmediato: confirma la conexión al cliente
      yield create(ServerEventSchema, { event: { case: "heartbeat", value: { serverTime: BigInt(Date.now()) } } });
      try {
        for await (const ev of queue) yield ev as ServerEvent;
      } finally {
        leave();
      }
    },

    // ---------------------------------------------------------------------
    // Long-polling (respaldo): unario que "espera" eventos
    // ---------------------------------------------------------------------
    async pollEvents(req, ctx) {
      requireChannel(req.channelId);
      const user = requireUser(req.user);
      if (!req.sessionId) throw new ConnectError("Falta session_id", Code.InvalidArgument);

      let session = pollSessions.get(req.sessionId);
      // si la sesión era de otro canal/usuario (cambió de canal), se reemplaza
      if (session && (session.channelId !== req.channelId || session.userId !== user.id)) {
        closeSession(req.sessionId);
        session = undefined;
      }
      if (!session) {
        const { queue, leave } = hub.join(req.channelId, user);
        session = { channelId: req.channelId, userId: user.id, queue, leave, expiry: setTimeout(() => {}, 0) };
        pollSessions.set(req.sessionId, session);
        console.log(`[poll] + ${user.name} usa long-polling en #${req.channelId}`);
        // primera respuesta inmediata: confirma la conexión
        const events = [
          create(ServerEventSchema, { event: { case: "heartbeat", value: { serverTime: BigInt(Date.now()) } } }),
          ...(await queue.drain(0)),
        ];
        session.expiry = setTimeout(() => closeSession(req.sessionId), SESSION_TTL_MS + POLL_WAIT_MS);
        return create(PollEventsResponseSchema, { events });
      }

      clearTimeout(session.expiry);
      const events = await session.queue.drain(POLL_WAIT_MS, ctx.signal);
      const s = session;
      s.expiry = setTimeout(() => closeSession(req.sessionId), SESSION_TTL_MS + POLL_WAIT_MS);
      return create(PollEventsResponseSchema, { events });
    },

    // ---------------------------------------------------------------------
    // Unarios
    // ---------------------------------------------------------------------
    listChannels() {
      return create(ListChannelsResponseSchema, { channels: hub.channels });
    },

    getHistory(req) {
      requireChannel(req.channelId);
      return create(GetHistoryResponseSchema, { messages: hub.getHistory(req.channelId, req.limit) });
    },

    sendMessage(req) {
      requireChannel(req.channelId);
      const author = requireUser(req.author);
      if (!req.text.trim() && !req.attachmentId) {
        throw new ConnectError("El mensaje está vacío", Code.InvalidArgument);
      }
      if (req.attachmentId && !hub.getAttachment(req.attachmentId)) {
        throw new ConnectError("El adjunto no existe o expiró", Code.NotFound);
      }
      return hub.postMessage(req.channelId, author, req.text, req.attachmentId || undefined);
    },

    setTyping(req) {
      requireChannel(req.channelId);
      hub.setTyping(req.channelId, requireUser(req.user), req.isTyping);
      return create(TypingResponseSchema, {});
    },

    uploadAttachment(req) {
      if (req.data.byteLength === 0) throw new ConnectError("Archivo vacío", Code.InvalidArgument);
      if (req.data.byteLength > MAX_ATTACHMENT_BYTES) {
        throw new ConnectError(`El archivo supera ${MAX_ATTACHMENT_BYTES / 1024 / 1024} MB`, Code.ResourceExhausted);
      }
      return hub.saveAttachment(req.fileName, req.mimeType, req.data);
    },

    downloadAttachment(req) {
      const found = hub.getAttachment(req.id);
      if (!found) throw new ConnectError("Adjunto no encontrado", Code.NotFound);
      return create(DownloadAttachmentResponseSchema, { attachment: found.meta, data: found.data });
    },
  };
}
