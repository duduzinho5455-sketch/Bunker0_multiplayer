/**
 * Bunker 9 - servidor multiplayer WebSocket
 * Compatível com a ponte de rede já presente no index.html.
 * Requer Node.js 18+.
 */
"use strict";

const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const { URL } = require("node:url");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const ROOT = __dirname;
const MAX_PLAYERS = Math.max(2, Math.min(32, Number(process.env.MAX_PLAYERS || 16)));
const MAX_ROOM_LENGTH = 40;
const MAX_PAYLOAD = 1024 * 1024;

const rooms = new Map();
const clients = new Set();

function send(ws, data) {
  if (ws.readyState !== WebSocket.OPEN) return false;
  try { ws.send(JSON.stringify(data)); return true; } catch { return false; }
}
function validRoom(s) {
  return typeof s === "string" && s.length > 0 &&
    s.length <= MAX_ROOM_LENGTH && /^[a-zA-Z0-9_-]+$/.test(s);
}
function getRoom(name) {
  let room = rooms.get(name);
  if (!room) {
    room = new Map();
    rooms.set(name, room);
  }
  return room;
}
function broadcast(room, data, except) {
  for (const client of room.values()) {
    if (client !== except) send(client.ws, data);
  }
}
function leave(client) {
  if (!client || client.closed) return;
  client.closed = true;
  clients.delete(client);
  const room = rooms.get(client.room);
  if (!room) return;
  room.delete(client.id);
  broadcast(room, { t: "left", peer: client.id });
  if (room.size === 0) rooms.delete(client.room);
}
function jsonError(ws, message) {
  send(ws, { t: "err", message });
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  if (u.pathname === "/health" || u.pathname === "/healthz") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, service: "bunker9-multiplayer", rooms: rooms.size, players: clients.size }));
    return;
  }
  let pathname = decodeURIComponent(u.pathname);
  if (pathname === "/") pathname = "/index.html";
  const file = path.resolve(ROOT, "." + pathname);
  if (!file.startsWith(ROOT + path.sep) && file !== path.join(ROOT, "index.html")) {
    res.writeHead(403); res.end("Forbidden"); return;
  }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { "content-type": "text/plain; charset=utf-8" }); res.end("Arquivo não encontrado"); return; }
    const ext = path.extname(file).toLowerCase();
    const types = { ".html":"text/html; charset=utf-8", ".js":"text/javascript; charset=utf-8",
      ".css":"text/css; charset=utf-8", ".json":"application/json; charset=utf-8",
      ".webmanifest":"application/manifest+json", ".png":"image/png", ".jpg":"image/jpeg",
      ".jpeg":"image/jpeg", ".webp":"image/webp", ".svg":"image/svg+xml", ".mp3":"audio/mpeg",
      ".ogg":"audio/ogg", ".wav":"audio/wav", ".glb":"model/gltf-binary", ".gltf":"model/gltf+json" };
    res.writeHead(200, { "content-type": types[ext] || "application/octet-stream",
      "cache-control": ext === ".html" ? "no-cache" : "public, max-age=3600",
      "x-content-type-options": "nosniff" });
    res.end(data);
  });
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD, perMessageDeflate: false });
server.on("upgrade", (req, socket, head) => {
  let u;
  try { u = new URL(req.url, `http://${req.headers.host || "localhost"}`); }
  catch { socket.destroy(); return; }
  if (u.pathname !== "/ws") { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit("connection", ws, req, u));
});

wss.on("connection", (ws, req, u) => {
  const roomName = u.searchParams.get("room") || "";
  const id = u.searchParams.get("id") || "";
  if (!validRoom(roomName) || !/^[a-zA-Z0-9_-]{6,40}$/.test(id)) {
    jsonError(ws, "Sala ou identificador inválido."); ws.close(1008, "invalid room"); return;
  }
  const room = getRoom(roomName);
  if (room.has(id)) {
    jsonError(ws, "Este jogador já está conectado à sala."); ws.close(1008, "duplicate id"); return;
  }
  if (room.size >= MAX_PLAYERS) {
    jsonError(ws, `Sala cheia (limite de ${MAX_PLAYERS} jogadores).`); ws.close(1013, "room full"); return;
  }

  const client = { ws, room: roomName, id, presence: null, closed: false, alive: true };
  // Envia a lista atual antes de anunciar o novo participante.
  const peers = [...room.values()].map(p => ({ peer: p.id, presence: p.presence }));
  room.set(id, client);
  clients.add(client);
  send(ws, { t: "snap", peers });
  broadcast(room, { t: "up", peer: id, presence: null }, client);

  ws.on("pong", () => { client.alive = true; });
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { jsonError(ws, "Mensagem inválida."); return; }
    if (!msg || msg.t !== "pres" || !msg.d || typeof msg.d !== "object" || Array.isArray(msg.d)) return;
    // Não permitir que um cliente injete campos de controle do protocolo.
    const presence = msg.d;
    if (JSON.stringify(presence).length > MAX_PAYLOAD - 1024) {
      jsonError(ws, "Estado do jogador muito grande."); return;
    }
    client.presence = presence;
    broadcast(room, { t: "up", peer: id, presence }, client);
  });
  ws.on("close", () => leave(client));
  ws.on("error", () => leave(client));
});

const heartbeat = setInterval(() => {
  for (const client of clients) {
    if (!client.alive) { try { client.ws.terminate(); } catch {} leave(client); continue; }
    client.alive = false;
    try { client.ws.ping(); } catch { leave(client); }
  }
}, 25000);
heartbeat.unref();

server.listen(PORT, HOST, () => {
  console.log(`Bunker 9 multiplayer ativo em http://${HOST}:${PORT}`);
  console.log(`Limite por sala: ${MAX_PLAYERS} jogadores`);
});
server.on("close", () => clearInterval(heartbeat));

process.on("SIGINT", () => { for (const c of clients) try { c.ws.close(1001, "server shutdown"); } catch {} server.close(() => process.exit(0)); });
process.on("SIGTERM", () => { for (const c of clients) try { c.ws.close(1001, "server shutdown"); } catch {} server.close(() => process.exit(0)); });
