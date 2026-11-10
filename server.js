'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const ROOT = __dirname;
const rooms = new Map();

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function broadcast(room, data, except) {
  for (const client of room.values()) {
    if (client.ws !== except) send(client.ws, data);
  }
}
function finite(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
function bounded(value, min, max, fallback = 0) {
  return Math.max(min, Math.min(max, finite(value, fallback)));
}
function cleanEvent(event) {
  if (!event || typeof event !== 'object') return null;
  const q = Math.max(0, Math.floor(finite(event.q, 0)));
  if (!q) return null;
  const key = String(event.k || '');
  if (key === 's') {
    const point = Array.isArray(event.e) ? event.e.slice(0, 3).map(v => bounded(v, -10000, 10000)) : [];
    if (point.length !== 3) return null;
    return { k: 's', q, w: Math.floor(bounded(event.w, 0, 100)), e: point };
  }
  if (key === 'h') {
    return { k: 'h', q, t: String(event.t || '').slice(0, 64), d: Math.floor(bounded(event.d, 0, 500)), h: event.h ? 1 : 0 };
  }
  if (key === 'g') {
    return {
      k: 'g', q,
      x: bounded(event.x, -10000, 10000), y: bounded(event.y, -10000, 10000), z: bounded(event.z, -10000, 10000),
      vx: bounded(event.vx, -1000, 1000), vy: bounded(event.vy, -1000, 1000), vz: bounded(event.vz, -1000, 1000)
    };
  }
  if (key === 'c') return { k: 'c', q, m: Math.floor(bounded(event.m, 0, 100)) };
  if (key === 'x') return { k: 'x', q, b: String(event.b || '').slice(0, 64), w: Math.floor(bounded(event.w, 0, 100)), h: event.h ? 1 : 0 };
  return null;
}
function cleanPresence(data) {
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  return {
    n: String(data.n || 'Jogador').replace(/[<>]/g, '').slice(0, 20),
    c: Math.floor(bounded(data.c, 0, 100)),
    x: bounded(data.x, -10000, 10000),
    y: bounded(data.y, -10000, 10000),
    z: bounded(data.z, -10000, 10000),
    r: bounded(data.r, -1000, 1000),
    w: Math.floor(bounded(data.w, 0, 100)),
    p: bounded(data.p, -10, 10),
    k: data.k ? 1 : 0,
    a: Number(data.a) === 0 ? 0 : 1,
    K: Math.floor(bounded(data.K, 0, 1000000)),
    D: Math.floor(bounded(data.D, 0, 1000000)),
    e: Array.isArray(data.e) ? data.e.slice(-10).map(cleanEvent).filter(Boolean) : []
  };
}

const server = http.createServer((req, res) => {
  let pathname;
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch {
    res.writeHead(400); res.end('Bad Request'); return;
  }
  if (pathname === '/healthz') {
    const players = [...rooms.values()].reduce((total, room) => total + room.size, 0);
    const body = JSON.stringify({ status: 'ok', rooms: rooms.size, players });
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(body); return;
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { Allow: 'GET, HEAD' }); res.end('Method Not Allowed'); return;
  }
  if (pathname !== '/' && pathname !== '/index.html') {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Not found'); return;
  }
  fs.readFile(path.join(ROOT, 'index.html'), (error, data) => {
    if (error) { res.writeHead(500); res.end('Game file unavailable'); return; }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache, no-store, must-revalidate',
      'X-Content-Type-Options': 'nosniff'
    });
    if (req.method === 'HEAD') res.end(); else res.end(data);
  });
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024, perMessageDeflate: false });
server.on('upgrade', (req, socket, head) => {
  let url;
  try { url = new URL(req.url, `http://${req.headers.host || 'localhost'}`); }
  catch { socket.destroy(); return; }
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, url));
});

wss.on('connection', (ws, url) => {
  const roomName = (url.searchParams.get('room') || 'geral').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32) || 'geral';
  const peer = (url.searchParams.get('id') || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
  if (!peer) { ws.close(1008, 'ID de jogador inválido'); return; }

  if (!rooms.has(roomName)) rooms.set(roomName, new Map());
  const room = rooms.get(roomName);
  const previous = room.get(peer);
  if (previous && previous.ws !== ws) {
    try { previous.ws.close(4001, 'Sessão substituída'); } catch {}
  }
  const client = { ws, peer, presence: null, alive: true };
  room.set(peer, client);

  // O snapshot contém apenas jogadores que já enviaram presença válida.
  send(ws, {
    t: 'snap',
    peers: [...room.values()]
      .filter(other => other !== client && other.presence)
      .map(other => ({ peer: other.peer, presence: other.presence }))
  });

  ws.on('pong', () => { client.alive = true; });
  ws.on('message', raw => {
    let message;
    try { message = JSON.parse(raw.toString()); } catch { return; }
    if (!message || message.t !== 'pres') return;
    const presence = cleanPresence(message.d);
    if (!presence) return;
    client.presence = presence;
    broadcast(room, { t: 'up', peer, presence }, ws);
  });

  ws.on('close', () => {
    // Uma conexão velha nunca pode apagar a sessão nova do mesmo jogador.
    const current = room.get(peer);
    if (current && current.ws === ws) {
      room.delete(peer);
      broadcast(room, { t: 'left', peer });
    }
    if (room.size === 0) rooms.delete(roomName);
  });
  ws.on('error', () => {});
});

// Mantém conexões ociosas vivas e remove clientes que perderam a rede.
const heartbeat = setInterval(() => {
  for (const room of rooms.values()) {
    for (const client of room.values()) {
      if (!client.alive) {
        try { client.ws.terminate(); } catch {}
        continue;
      }
      client.alive = false;
      try { client.ws.ping(); } catch { try { client.ws.terminate(); } catch {} }
    }
  }
}, 25000);

server.on('close', () => clearInterval(heartbeat));
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Bunker 9 multiplayer ativo na porta ${PORT}`);
});
