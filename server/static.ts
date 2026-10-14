/**
 * Servidor de archivos estáticos mínimo para el frontend compilado (carpeta dist/).
 * Funciona tanto con peticiones HTTP/1.1 como HTTP/2.
 */
import { createReadStream, existsSync, statSync } from "node:fs";
import { extname, join, normalize, resolve } from "node:path";

/** Tipos mínimos compatibles con peticiones HTTP/1.1 y HTTP/2 */
type Req = { url?: string };
type Res = NodeJS.WritableStream & {
  writeHead(status: number, headers?: Record<string, string>): unknown;
  end(chunk?: string): unknown;
};

const DIST = resolve(process.env.STATIC_DIR ?? "dist");

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json",
  ".woff2": "font/woff2",
  ".txt": "text/plain; charset=utf-8",
};

export function serveStatic(req: Req, res: Res): void {
  const urlPath = decodeURIComponent((req.url ?? "/").split("?")[0]);
  let file = normalize(join(DIST, urlPath));
  if (!file.startsWith(DIST)) {
    res.writeHead(403);
    res.end();
    return;
  }
  if (!existsSync(file) || statSync(file).isDirectory()) {
    file = join(DIST, "index.html"); // SPA fallback
  }
  if (!existsSync(file)) {
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    res.end("Frontend no compilado. Ejecuta: npm run build");
    return;
  }
  const isAsset = urlPath.startsWith("/assets/");
  res.writeHead(200, {
    "content-type": MIME[extname(file)] ?? "application/octet-stream",
    "cache-control": isAsset ? "public, max-age=31536000, immutable" : "no-cache",
  });
  createReadStream(file).pipe(res);
}
