// The test server for the static export: serves out/ under the base path, refuses HEAD, and answers
// the first request of its life with a 503, as a sleeping host would. The site and the tests must
// handle both.
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize } from "node:path";

const ROOT = new URL("../out/", import.meta.url).pathname;
const BASE = process.env.FRICTION_BASE_PATH ?? "/friction";
const PORT = Number(process.env.PORT ?? 4173);
const TYPES = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".json": "application/json",
  ".woff2": "font/woff2", ".svg": "image/svg+xml", ".txt": "text/plain", ".ico": "image/x-icon" };
let first = true;

createServer(async (req, res) => {
  if (req.method === "HEAD") { res.writeHead(405, { allow: "GET" }); res.end(); return; }
  if (first) { first = false; res.writeHead(503, { "retry-after": "1" }); res.end("waking up"); return; }
  let path = decodeURIComponent((req.url ?? "/").split("?")[0]);
  if (!path.startsWith(BASE)) { res.writeHead(404); res.end(); return; }
  path = normalize(path.slice(BASE.length) || "/");
  let file = join(ROOT, path);
  try {
    if ((await stat(file)).isDirectory()) file = join(file, "index.html");
    const body = await readFile(file);
    res.writeHead(200, { "content-type": TYPES[extname(file)] ?? "application/octet-stream" });
    res.end(body);
  } catch {
    res.writeHead(404); res.end("not found");
  }
}).listen(PORT, () => console.log(`test server on http://localhost:${PORT}${BASE}/`));
