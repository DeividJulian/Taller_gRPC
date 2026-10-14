import { randomUUID } from "node:crypto";
import { create } from "@bufbuild/protobuf";
import {
  AttachmentSchema,
  ChannelSchema,
  ChatMessageSchema,
  ServerEventSchema,
  UserSchema,
  type Attachment,
  type Channel,
  type ChatMessage,
  type ServerEvent,
  type User,
} from "../gen/chat/v1/chat_pb.js";
import { AsyncQueue } from "./async-queue.js";

/** Un suscriptor = una conexión abierta (stream) de un cliente en un canal. */
interface Subscriber {
  user: User;
  queue: AsyncQueue<ServerEvent>;
}

const MAX_HISTORY = 200; // mensajes guardados por canal
export const MAX_ATTACHMENT_BYTES = 3 * 1024 * 1024; // 3 MB
const MAX_ATTACHMENTS = 100; // en memoria

/**
 * ChatHub: el "corazón" del sistema. Guarda canales, historial, adjuntos y
 * la lista de conexiones activas. Cuando llega un evento lo difunde
 * (fan-out) a todos los streams abiertos de ese canal.
 *
 * Nota: el estado vive en memoria. En la nube se despliega con UNA sola
 * instancia (max-instances=1) para que todos compartan el mismo Hub.
 */
export class ChatHub {
  readonly channels: Channel[] = [
    create(ChannelSchema, { id: "general", name: "general", description: "Conversación general del curso" }),
    create(ChannelSchema, { id: "grpc", name: "grpc", description: "Dudas sobre gRPC, Protobuf y HTTP/2" }),
    create(ChannelSchema, { id: "proyectos", name: "proyectos", description: "Avances de los proyectos de la materia" }),
    create(ChannelSchema, { id: "random", name: "random", description: "Off-topic, memes y demás" }),
  ];

  private history = new Map<string, ChatMessage[]>();
  private subscribers = new Map<string, Set<Subscriber>>();
  private attachments = new Map<string, { meta: Attachment; data: Uint8Array }>();

  hasChannel(id: string): boolean {
    return this.channels.some((c) => c.id === id);
  }

  getHistory(channelId: string, limit = 50): ChatMessage[] {
    const all = this.history.get(channelId) ?? [];
    return all.slice(-Math.max(1, Math.min(limit || 50, MAX_HISTORY)));
  }

  /** Registra una conexión. Devuelve la cola de eventos y una función para salir. */
  join(channelId: string, user: User): { queue: AsyncQueue<ServerEvent>; leave: () => void } {
    const sub: Subscriber = { user: sanitizeUser(user), queue: new AsyncQueue<ServerEvent>() };
    let set = this.subscribers.get(channelId);
    if (!set) this.subscribers.set(channelId, (set = new Set()));

    const firstConnection = !this.onlineUsers(channelId).some((u) => u.id === sub.user.id);
    set.add(sub);
    if (firstConnection) this.postSystem(channelId, `${sub.user.name} se unió a #${channelId}`);
    this.broadcastPresence(channelId);

    let left = false;
    const leave = () => {
      if (left) return;
      left = true;
      set!.delete(sub);
      sub.queue.close();
      const stillOnline = this.onlineUsers(channelId).some((u) => u.id === sub.user.id);
      if (!stillOnline) {
        this.setTyping(channelId, sub.user, false);
        this.postSystem(channelId, `${sub.user.name} salió de #${channelId}`);
      }
      this.broadcastPresence(channelId);
    };
    return { queue: sub.queue, leave };
  }

  /** Publica un mensaje de usuario y lo difunde a todo el canal. */
  postMessage(channelId: string, author: User, text: string, attachmentId?: string): ChatMessage {
    const attachment = attachmentId ? this.attachments.get(attachmentId)?.meta : undefined;
    const msg = create(ChatMessageSchema, {
      id: randomUUID(),
      channelId,
      author: sanitizeUser(author),
      text: text.slice(0, 4000),
      attachment,
      sentAt: BigInt(Date.now()),
    });
    this.store(msg);
    // al enviar un mensaje, el autor deja de "estar escribiendo"
    this.setTyping(channelId, msg.author!, false);
    this.broadcast(channelId, create(ServerEventSchema, { event: { case: "message", value: msg } }));
    return msg;
  }

  setTyping(channelId: string, user: User, isTyping: boolean): void {
    this.broadcast(
      channelId,
      create(ServerEventSchema, {
        event: { case: "typing", value: { channelId, user: sanitizeUser(user), isTyping } },
      }),
      user.id, // no se lo enviamos de vuelta a quien escribe
    );
  }

  saveAttachment(fileName: string, mimeType: string, data: Uint8Array): Attachment {
    const meta = create(AttachmentSchema, {
      id: randomUUID(),
      fileName: fileName.slice(0, 200) || "archivo",
      mimeType: mimeType || "application/octet-stream",
      size: BigInt(data.byteLength),
    });
    this.attachments.set(meta.id, { meta, data });
    // política simple de memoria: eliminar el más antiguo
    if (this.attachments.size > MAX_ATTACHMENTS) {
      const oldest = this.attachments.keys().next().value!;
      this.attachments.delete(oldest);
    }
    return meta;
  }

  getAttachment(id: string) {
    return this.attachments.get(id);
  }

  /** Envía un latido a todas las conexiones (evita que los proxies cierren el stream). */
  heartbeat(): void {
    const ev = create(ServerEventSchema, {
      event: { case: "heartbeat", value: { serverTime: BigInt(Date.now()) } },
    });
    for (const set of this.subscribers.values()) for (const s of set) s.queue.push(ev);
  }

  stats() {
    let connections = 0;
    for (const set of this.subscribers.values()) connections += set.size;
    return { connections, attachments: this.attachments.size };
  }

  // ---------------------------------------------------------------- privados

  private onlineUsers(channelId: string): User[] {
    const unique = new Map<string, User>();
    for (const s of this.subscribers.get(channelId) ?? []) unique.set(s.user.id, s.user);
    return [...unique.values()];
  }

  private broadcastPresence(channelId: string): void {
    this.broadcast(
      channelId,
      create(ServerEventSchema, {
        event: { case: "presence", value: { channelId, users: this.onlineUsers(channelId) } },
      }),
    );
  }

  private postSystem(channelId: string, text: string): void {
    const msg = create(ChatMessageSchema, {
      id: randomUUID(),
      channelId,
      author: create(UserSchema, { id: "system", name: "Sistema", color: "#8b8b9a" }),
      text,
      sentAt: BigInt(Date.now()),
      system: true,
    });
    this.store(msg);
    this.broadcast(channelId, create(ServerEventSchema, { event: { case: "message", value: msg } }));
  }

  private store(msg: ChatMessage): void {
    let list = this.history.get(msg.channelId);
    if (!list) this.history.set(msg.channelId, (list = []));
    list.push(msg);
    if (list.length > MAX_HISTORY) list.splice(0, list.length - MAX_HISTORY);
  }

  private broadcast(channelId: string, event: ServerEvent, exceptUserId?: string): void {
    for (const s of this.subscribers.get(channelId) ?? []) {
      if (exceptUserId && s.user.id === exceptUserId) continue;
      s.queue.push(event);
    }
  }
}

function sanitizeUser(u: User): User {
  return create(UserSchema, {
    id: (u.id || randomUUID()).slice(0, 64),
    name: (u.name || "Anónimo").trim().slice(0, 32) || "Anónimo",
    color: /^#[0-9a-fA-F]{6}$/.test(u.color) ? u.color : "#6366f1",
  });
}
