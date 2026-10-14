import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ConnectError } from "@connectrpc/connect";
import type { Attachment, Channel, ChatMessage, ServerEvent, User } from "../../gen/chat/v1/chat_pb";
import { chatClient } from "./api";

const COLORS = ["#6366f1", "#ec4899", "#f59e0b", "#10b981", "#06b6d4", "#ef4444", "#8b5cf6", "#84cc16"];
const MAX_FILE_MB = 3;

type Status = "connecting" | "online" | "polling" | "reconnecting";

// ============================================================================
//  Raíz: pide el nombre y luego muestra el chat
// ============================================================================
export function App() {
  const [user, setUser] = useState<User | null>(() => {
    try {
      const saved = localStorage.getItem("grpc-chat-user");
      return saved ? (JSON.parse(saved) as User) : null;
    } catch {
      return null;
    }
  });

  if (!user) {
    return (
      <Login
        onLogin={(u) => {
          try {
            localStorage.setItem("grpc-chat-user", JSON.stringify(u));
          } catch {
            /* sin almacenamiento: no pasa nada */
          }
          setUser(u);
        }}
      />
    );
  }
  return (
    <ChatApp
      user={user}
      onLogout={() => {
        try {
          localStorage.removeItem("grpc-chat-user");
        } catch {
          /* ignore */
        }
        setUser(null);
      }}
    />
  );
}

// ============================================================================
//  Pantalla de ingreso
// ============================================================================
function Login({ onLogin }: { onLogin: (u: User) => void }) {
  const [name, setName] = useState("");
  const [color, setColor] = useState(COLORS[0]);
  return (
    <div className="login">
      <form
        className="login-card"
        onSubmit={(e) => {
          e.preventDefault();
          if (!name.trim()) return;
          onLogin({ id: crypto.randomUUID(), name: name.trim(), color } as User);
        }}
      >
        <div className="logo">💬</div>
        <h1>gRPC Chat</h1>
        <p className="muted">Mensajería en tiempo real con Protocol Buffers sobre HTTP/2</p>
        <label>
          Tu nombre
          <input autoFocus maxLength={32} value={name} onChange={(e) => setName(e.target.value)} placeholder="Ej. Deivid" />
        </label>
        <div className="colors">
          {COLORS.map((c) => (
            <button
              type="button"
              key={c}
              className={`swatch ${c === color ? "active" : ""}`}
              style={{ background: c }}
              onClick={() => setColor(c)}
              aria-label={`color ${c}`}
            />
          ))}
        </div>
        <button className="primary" disabled={!name.trim()}>
          Entrar
        </button>
      </form>
    </div>
  );
}

// ============================================================================
//  Aplicación principal
// ============================================================================
function ChatApp({ user, onLogout }: { user: User; onLogout: () => void }) {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [channelId, setChannelId] = useState("general");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [online, setOnline] = useState<User[]>([]);
  const [typingUsers, setTypingUsers] = useState<Record<string, string>>({});
  const [status, setStatus] = useState<Status>("connecting");
  const [error, setError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const transportMode = useRef<"stream" | "poll">("stream");

  // 1) Unario: lista de canales
  useEffect(() => {
    chatClient
      .listChannels({})
      .then((r) => setChannels(r.channels))
      .catch((e) => setError(ConnectError.from(e).rawMessage));
  }, []);

  // 2) Tiempo real: suscripción al canal activo.
  //    Modo normal  → Subscribe (server-streaming por gRPC-Web).
  //    Modo respaldo → PollEvents (long-polling unario) si algo en la red
  //    retiene los streams. Una vez detectado, se recuerda en la sesión.
  useEffect(() => {
    const abort = new AbortController();
    setMessages([]);
    setOnline([]);
    setTypingUsers({});
    setStatus("connecting");

    const merge = (incoming: ChatMessage[]) =>
      setMessages((prev) => {
        const map = new Map(prev.map((m) => [m.id, m]));
        for (const m of incoming) map.set(m.id, m);
        return [...map.values()].sort((a, b) => Number(a.sentAt - b.sentAt));
      });

    const handleEvent = (ev: ServerEvent) => {
      switch (ev.event.case) {
        case "message":
          merge([ev.event.value]);
          break;
        case "presence":
          setOnline(ev.event.value.users);
          break;
        case "typing": {
          const t = ev.event.value;
          setTypingUsers((prev) => {
            const next = { ...prev };
            if (t.isTyping && t.user) next[t.user.id] = t.user.name;
            else if (t.user) delete next[t.user.id];
            return next;
          });
          break;
        }
      }
    };

    const onConnected = (mode: "stream" | "poll") => {
      setStatus(mode === "stream" ? "online" : "polling");
      setError(null);
      chatClient.getHistory({ channelId, limit: 100 }).then((r) => merge(r.messages)).catch(() => {});
    };

    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

    // ---- Modo normal: server-streaming ----------------------------------
    const runStream = async (): Promise<"stalled" | "ended" | "aborted"> => {
      const attemptAbort = new AbortController();
      const onOuterAbort = () => attemptAbort.abort();
      abort.signal.addEventListener("abort", onOuterAbort);
      let stalled = false;
      // VIGILANTE: el servidor envía un latido apenas abre el stream.
      // Si en 8 s no llega nada, algo entre el navegador y el servidor está
      // reteniendo la respuesta (antivirus, proxy, software del laboratorio...).
      const watchdog = window.setTimeout(() => {
        stalled = true;
        attemptAbort.abort();
      }, 8000);
      try {
        let first = true;
        for await (const ev of chatClient.subscribe({ channelId, user }, { signal: attemptAbort.signal })) {
          if (first) {
            first = false;
            window.clearTimeout(watchdog);
            onConnected("stream");
          }
          handleEvent(ev);
        }
        return "ended";
      } catch (e) {
        if (abort.signal.aborted) return "aborted";
        if (stalled) return "stalled";
        console.error("[gRPC] Subscribe falló:", e);
        setError(ConnectError.from(e).rawMessage);
        return "ended";
      } finally {
        window.clearTimeout(watchdog);
        abort.signal.removeEventListener("abort", onOuterAbort);
      }
    };

    // ---- Modo respaldo: long-polling unario -----------------------------
    const runPolling = async () => {
      const sessionId = crypto.randomUUID();
      let first = true;
      let failures = 0;
      while (!abort.signal.aborted) {
        try {
          const res = await chatClient.pollEvents({ channelId, user, sessionId }, { signal: abort.signal });
          failures = 0;
          if (first) {
            first = false;
            onConnected("poll");
          }
          res.events.forEach(handleEvent);
        } catch (e) {
          if (abort.signal.aborted) return;
          failures++;
          setStatus("reconnecting");
          setError(ConnectError.from(e).rawMessage);
          first = true;
          await sleep(Math.min(1000 * 2 ** failures, 10_000));
        }
      }
    };

    (async () => {
      let attempt = 0;
      while (!abort.signal.aborted) {
        if (transportMode.current === "poll") {
          await runPolling();
          return;
        }
        const result = await runStream();
        if (result === "aborted") return;
        if (result === "stalled") {
          console.warn("[gRPC] El stream no entregó datos en 8 s → cambiando a long-polling");
          transportMode.current = "poll";
          continue;
        }
        // el stream terminó (timeout del proxy, red caída...) → reconectar con backoff
        setStatus("reconnecting");
        attempt++;
        await sleep(Math.min(1000 * 2 ** attempt, 10_000));
      }
    })();

    return () => abort.abort();
  }, [channelId, user]);

  const channel = channels.find((c) => c.id === channelId);
  const typingNames = Object.entries(typingUsers)
    .filter(([id]) => id !== user.id)
    .map(([, n]) => n);

  return (
    <div className="app">
      <aside className={`sidebar ${sidebarOpen ? "open" : ""}`}>
        <div className="workspace">
          <span className="logo-sm">💬</span>
          <div>
            <strong>gRPC Chat</strong>
            <div className="muted small">Programación Web · UCC</div>
          </div>
        </div>

        <div className="section-title">Canales</div>
        <nav>
          {channels.map((c) => (
            <button
              key={c.id}
              className={`channel ${c.id === channelId ? "active" : ""}`}
              onClick={() => {
                setChannelId(c.id);
                setSidebarOpen(false);
              }}
            >
              <span className="hash">#</span> {c.name}
            </button>
          ))}
        </nav>

        <div className="section-title">En línea — {online.length}</div>
        <ul className="online">
          {online.map((u) => (
            <li key={u.id}>
              <Avatar user={u} size={24} />
              <span>
                {u.name}
                {u.id === user.id && <span className="muted"> (tú)</span>}
              </span>
              <span className="dot" />
            </li>
          ))}
        </ul>

        <div className="me">
          <Avatar user={user} size={32} />
          <div className="me-name">
            <strong>{user.name}</strong>
            <StatusBadge status={status} />
          </div>
          <button className="ghost" onClick={onLogout} title="Cambiar usuario">
            ⎋
          </button>
        </div>
      </aside>
      {sidebarOpen && <div className="backdrop" onClick={() => setSidebarOpen(false)} />}

      <main className="main">
        <header className="topbar">
          <button className="ghost menu" onClick={() => setSidebarOpen(true)} aria-label="Menú">
            ☰
          </button>
          <div>
            <h2>
              <span className="hash">#</span> {channel?.name ?? channelId}
            </h2>
            <div className="muted small">{channel?.description}</div>
          </div>
          <div className="proto-chip" title="Protocolo usado por el navegador">
            {status === "polling" ? "gRPC-Web · long-polling" : "gRPC-Web · streaming"}
          </div>
        </header>

        {error && status !== "online" && status !== "polling" && <div className="error-bar">⚠ {error}</div>}

        <MessageList messages={messages} me={user} />

        <div className="typing">
          {typingNames.length > 0 && (
            <>
              <span className="dots">
                <i />
                <i />
                <i />
              </span>
              {typingNames.length === 1
                ? `${typingNames[0]} está escribiendo...`
                : `${typingNames.slice(0, 3).join(", ")} están escribiendo...`}
            </>
          )}
        </div>

        <Composer channelId={channelId} channelName={channel?.name ?? channelId} user={user} disabled={status !== "online" && status !== "polling"} onError={setError} />
      </main>
    </div>
  );
}

// ============================================================================
//  Lista de mensajes
// ============================================================================
function MessageList({ messages, me }: { messages: ChatMessage[]; me: User }) {
  const ref = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  useEffect(() => {
    const el = ref.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  }, [messages]);

  return (
    <div
      className="messages"
      ref={ref}
      onScroll={(e) => {
        const el = e.currentTarget;
        stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
      }}
    >
      {messages.length === 0 && <div className="empty">Aún no hay mensajes. ¡Escribe el primero! 👋</div>}
      {messages.map((m, i) => {
        if (m.system) {
          return (
            <div key={m.id} className="system">
              {m.text} · {formatTime(m.sentAt)}
            </div>
          );
        }
        const prev = messages[i - 1];
        const grouped =
          prev && !prev.system && prev.author?.id === m.author?.id && Number(m.sentAt - prev.sentAt) < 5 * 60_000;
        return (
          <div key={m.id} className={`msg ${grouped ? "grouped" : ""} ${m.author?.id === me.id ? "mine" : ""}`}>
            <div className="msg-avatar">{!grouped && m.author && <Avatar user={m.author} size={38} />}</div>
            <div className="msg-body">
              {!grouped && (
                <div className="msg-head">
                  <strong style={{ color: m.author?.color }}>{m.author?.name}</strong>
                  <span className="muted small">{formatTime(m.sentAt)}</span>
                </div>
              )}
              {m.text && <div className="msg-text">{m.text}</div>}
              {m.attachment && <AttachmentView attachment={m.attachment} />}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ============================================================================
//  Adjuntos: se descargan por un RPC unario (bytes Protobuf → Blob)
// ============================================================================
const blobCache = new Map<string, string>();

async function loadAttachment(id: string): Promise<string> {
  const cached = blobCache.get(id);
  if (cached) return cached;
  const res = await chatClient.downloadAttachment({ id });
  const url = URL.createObjectURL(
    new Blob([res.data as Uint8Array<ArrayBuffer>], { type: res.attachment?.mimeType || "application/octet-stream" }),
  );
  blobCache.set(id, url);
  return url;
}

function AttachmentView({ attachment }: { attachment: Attachment }) {
  const isImage = attachment.mimeType.startsWith("image/");
  const [src, setSrc] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!isImage) return;
    loadAttachment(attachment.id).then(setSrc).catch(() => setFailed(true));
  }, [attachment.id, isImage]);

  const download = async () => {
    try {
      const url = await loadAttachment(attachment.id);
      const a = document.createElement("a");
      a.href = url;
      a.download = attachment.fileName;
      a.click();
    } catch {
      setFailed(true);
    }
  };

  if (isImage && src) {
    return (
      <button className="image-att" onClick={download} title="Descargar">
        <img src={src} alt={attachment.fileName} />
      </button>
    );
  }
  return (
    <button className="file-att" onClick={download} disabled={failed}>
      <span className="file-icon">📎</span>
      <span>
        <strong>{attachment.fileName}</strong>
        <span className="muted small">{failed ? "No disponible" : formatSize(attachment.size)}</span>
      </span>
      <span className="dl">⬇</span>
    </button>
  );
}

// ============================================================================
//  Caja de texto: envía mensajes, adjuntos e indicador de escritura
// ============================================================================
function Composer({
  channelId,
  channelName,
  user,
  disabled,
  onError,
}: {
  channelId: string;
  channelName: string;
  user: User;
  disabled: boolean;
  onError: (e: string) => void;
}) {
  const [text, setText] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [sending, setSending] = useState(false);
  const typingRef = useRef(false);
  const timerRef = useRef<number | undefined>(undefined);
  const fileInput = useRef<HTMLInputElement>(null);

  const setTyping = useCallback(
    (isTyping: boolean) => {
      if (typingRef.current === isTyping) return;
      typingRef.current = isTyping;
      chatClient.setTyping({ channelId, user, isTyping }).catch(() => {});
    },
    [channelId, user],
  );

  // al cambiar de canal, dejamos de "escribir" en el anterior
  useEffect(() => () => setTyping(false), [setTyping]);

  const onChange = (value: string) => {
    setText(value);
    setTyping(value.length > 0);
    window.clearTimeout(timerRef.current);
    timerRef.current = window.setTimeout(() => setTyping(false), 2500);
  };

  const send = async () => {
    if (sending || (!text.trim() && !file)) return;
    setSending(true);
    try {
      let attachmentId = "";
      if (file) {
        if (file.size > MAX_FILE_MB * 1024 * 1024) throw new Error(`El archivo supera ${MAX_FILE_MB} MB`);
        const data = new Uint8Array(await file.arrayBuffer());
        const att = await chatClient.uploadAttachment({
          fileName: file.name,
          mimeType: file.type || "application/octet-stream",
          data,
        });
        attachmentId = att.id;
      }
      await chatClient.sendMessage({ channelId, author: user, text: text.trim(), attachmentId });
      setText("");
      setFile(null);
      window.clearTimeout(timerRef.current);
      typingRef.current = false; // el servidor ya limpia el "escribiendo" al recibir el mensaje
    } catch (e) {
      onError(e instanceof Error ? ConnectError.from(e).rawMessage : String(e));
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="composer-wrap">
      {file && (
        <div className="pending-file">
          📎 {file.name} <span className="muted">({formatSize(BigInt(file.size))})</span>
          <button className="ghost" onClick={() => setFile(null)}>
            ✕
          </button>
        </div>
      )}
      <form
        className="composer"
        onSubmit={(e) => {
          e.preventDefault();
          void send();
        }}
      >
        <button type="button" className="ghost attach" onClick={() => fileInput.current?.click()} title="Adjuntar archivo">
          ＋
        </button>
        <input
          type="file"
          hidden
          ref={fileInput}
          onChange={(e) => {
            setFile(e.target.files?.[0] ?? null);
            e.target.value = "";
          }}
        />
        <input
          className="text"
          value={text}
          onChange={(e) => onChange(e.target.value)}
          placeholder={disabled ? "Conectando..." : `Enviar mensaje a #${channelName}`}
          disabled={disabled}
          maxLength={4000}
        />
        <button className="primary send" disabled={disabled || sending || (!text.trim() && !file)}>
          {sending ? "..." : "Enviar"}
        </button>
      </form>
    </div>
  );
}

// ============================================================================
//  Utilidades visuales
// ============================================================================
function Avatar({ user, size }: { user: User; size: number }) {
  const initials = useMemo(
    () =>
      user.name
        .split(/\s+/)
        .map((p) => p[0])
        .join("")
        .slice(0, 2)
        .toUpperCase(),
    [user.name],
  );
  return (
    <span className="avatar" style={{ background: user.color, width: size, height: size, fontSize: size * 0.4 }}>
      {initials}
    </span>
  );
}

function StatusBadge({ status }: { status: Status }) {
  const label = {
    connecting: "Conectando...",
    online: "Stream activo",
    polling: "Long-polling activo",
    reconnecting: "Reconectando...",
  }[status];
  return <span className={`status ${status}`}>● {label}</span>;
}

function formatTime(ms: bigint) {
  return new Date(Number(ms)).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatSize(bytes: bigint) {
  const n = Number(bytes);
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}
