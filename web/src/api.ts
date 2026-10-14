/**
 * Cliente gRPC del navegador.
 *
 * Los navegadores NO pueden hablar gRPC nativo (no exponen tramas HTTP/2 ni
 * trailers a JavaScript). Por eso usamos gRPC-Web: el mismo contrato .proto,
 * mensajes binarios Protobuf, pero encapsulados de forma que fetch() los pueda
 * enviar. gRPC-Web soporta llamadas unarias y server-streaming; el streaming
 * bidireccional lo demuestra el cliente de terminal (cli/chat-cli.ts).
 */
import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { ChatService } from "../../gen/chat/v1/chat_pb";

const baseUrl = (import.meta.env.VITE_API_URL as string | undefined) || window.location.origin;

const transport = createGrpcWebTransport({
  baseUrl,
  useBinaryFormat: true, // Protobuf binario (no JSON)
});

export const chatClient = createClient(ChatService, transport);
