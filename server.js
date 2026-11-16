// Schattenjagd – Spiel-Server (ohne Zusatzpakete)
// Liefert die Spielseite aus und leitet die Spielerdaten in Räumen weiter.
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const PAGE = path.join(__dirname, 'public', 'index.html');
const MAX_MSG = 16 * 1024;        // Bytes pro Nachricht
const MAX_PRESENCE = 6 * 1024;    // Bytes pro Spielerzustand
const MAX_ROOMS_PER_CLIENT = 4;
const MAX_CLIENTS = 400;

let page = fs.readFileSync(PAGE);
const rooms = new Map();   // name -> Map(id -> client)
const clients = new Set();

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok ' + clients.size); return; }
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache', 'x-content-type-options': 'nosniff' });
    res.end(page); return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('nicht gefunden');
});

// ---------- WebSocket (RFC 6455, minimal) ----------
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if ((req.url || '').split('?')[0] !== '/ws' || !key || clients.size >= MAX_CLIENTS) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, id: null, rooms: new Set(), buf: Buffer.alloc(0), frag: [], alive: true, closed: false };
  clients.add(c);
  socket.on('data', (d) => onData(c, d));
  socket.on('close', () => drop(c));
  socket.on('error', () => drop(c));
});

function sendFrame(c, opcode, payload) {
  if (c.closed) return;
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  head[0] = 0x80 | opcode;
  try { c.socket.write(Buffer.concat([head, payload])); } catch (e) { drop(c); }
}
const send = (c, obj) => sendFrame(c, 1, Buffer.from(JSON.stringify(obj)));

function onData(c, d) {
  c.buf = Buffer.concat([c.buf, d]);
  if (c.buf.length > MAX_MSG * 4) { drop(c); return; }
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; const big = c.buf.readBigUInt64BE(2); if (big > BigInt(MAX_MSG)) { drop(c); return; } len = Number(big); off = 10; }
    if (!masked || len > MAX_MSG) { drop(c); return; }
    if (c.buf.length < off + 4 + len) return;
    const mask = c.buf.slice(off, off + 4), data = Buffer.from(c.buf.slice(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = c.buf.slice(off + 4 + len);
    c.alive = true;
    if (op === 8) { sendFrame(c, 8, Buffer.alloc(0)); drop(c); return; }
    if (op === 9) { sendFrame(c, 10, data); continue; }
    if (op === 10) continue;
    if (op === 0 || op === 1) {
      c.frag.push(data);
      if (!fin) { if (c.frag.reduce((a, b) => a + b.length, 0) > MAX_MSG) { drop(c); return; } continue; }
      const text = Buffer.concat(c.frag).toString('utf8'); c.frag = [];
      let msg; try { msg = JSON.parse(text); } catch (e) { continue; }
      handle(c, msg);
    }
  }
}

// ---------- Räume ----------
const okName = (s) => typeof s === 'string' && /^[a-z0-9_][a-z0-9_.-]{0,47}$/.test(s);
const okId = (s) => typeof s === 'string' && /^[a-z0-9]{6,16}$/.test(s);

function handle(c, m) {
  if (!m || typeof m !== 'object') return;
  if (m.t === 'hello') {
    if (c.id || !okId(m.id)) return;
    // gleiche ID schon verbunden (Wiederverbinden) -> alte Verbindung schliessen
    for (const o of clients) if (o !== c && o.id === m.id) drop(o);
    c.id = m.id; send(c, { t: 'hello', id: c.id }); return;
  }
  if (!c.id) return;
  if (m.t === 'join') {
    if (!okName(m.room) || c.rooms.has(m.room)) return;
    if (c.rooms.size >= MAX_ROOMS_PER_CLIENT) { send(c, { t: 'err', room: m.room, code: 'limit_reached' }); return; }
    let r = rooms.get(m.room); if (!r) { r = new Map(); rooms.set(m.room, r); }
    c.rooms.add(m.room);
    const pres = presenceOf(m.p);
    r.set(c.id, { c, p: pres });
    const list = []; for (const [id, e] of r) if (id !== c.id) list.push({ peer: id, p: e.p });
    send(c, { t: 'joined', room: m.room, peers: list });
    broadcast(m.room, c.id, { t: 'p', room: m.room, peer: c.id, p: pres });
    return;
  }
  if (m.t === 'leave') { leave(c, m.room); return; }
  if (m.t === 'p') {
    const r = rooms.get(m.room); if (!r || !r.has(c.id)) return;
    const pres = presenceOf(m.p); if (pres === null) return;
    r.get(c.id).p = pres;
    broadcast(m.room, c.id, { t: 'p', room: m.room, peer: c.id, p: pres });
    return;
  }
  if (m.t === 'hb') { send(c, { t: 'hb' }); }
}
function presenceOf(p) {
  if (!p || typeof p !== 'object' || Array.isArray(p)) return {};
  return JSON.stringify(p).length > MAX_PRESENCE ? null : p;
}
function broadcast(room, fromId, obj) {
  const r = rooms.get(room); if (!r) return;
  const payload = Buffer.from(JSON.stringify(obj));
  for (const [id, e] of r) if (id !== fromId) sendFrame(e.c, 1, payload);
}
function leave(c, room) {
  if (!c.rooms.has(room)) return;
  c.rooms.delete(room);
  const r = rooms.get(room);
  if (r && r.get(c.id) && r.get(c.id).c === c) {
    r.delete(c.id);
    broadcast(room, c.id, { t: 'left', room, peer: c.id });
    if (!r.size) rooms.delete(room);
  }
}
function drop(c) {
  if (c.closed) return;
  for (const room of [...c.rooms]) leave(c, room);
  c.closed = true; clients.delete(c);
  try { c.socket.destroy(); } catch (e) { }
}
// tote Verbindungen aufräumen
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { drop(c); continue; }
    c.alive = false; sendFrame(c, 9, Buffer.alloc(0));
  }
}, 20000);

server.listen(PORT, () => console.log('Schattenjagd läuft auf Port ' + PORT));
