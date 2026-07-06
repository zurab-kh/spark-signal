"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const rooms = new Map();
const LOG_FILE = path.join(__dirname, "signal.log");
const LOG_RING = [];
const LOG_MAX = 200;
const HOST_GRACE_MS = 10 * 60 * 1000;
const VOLT_FLUSH_MS = 16;

// Оптимизация: логи пишем асинхронно, батчами (не блокируем event loop)
let logBuf = [];
let logFlushTimer = null;
function slog(event, detail = "") {
  const line = `[${new Date().toISOString()}] ${event}${detail ? " " + detail : ""}`;
  console.log(line);
  LOG_RING.push(line);
  if (LOG_RING.length > LOG_MAX) LOG_RING.shift();
  // буферизуем запись в файл — флуш раз в 2 секунды
  logBuf.push(line);
  if (!logFlushTimer) {
    logFlushTimer = setTimeout(() => {
      if (logBuf.length) {
        try { fs.appendFileSync(LOG_FILE, logBuf.join("\n") + "\n"); } catch {}
        logBuf = [];
      }
      logFlushTimer = null;
    }, 2000);
  }
}

// Тихие события (не логируем — для производительности при активной игре)
const QUIET_EVENTS = new Set(["game:relay", "ws:ping", "volt"]);

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function sendRaw(peer, raw) {
  if (peer && peer.readyState === WebSocket.OPEN) peer.send(raw);
}

function hostOpen(r) {
  return r.host && r.host.readyState === WebSocket.OPEN;
}

function guestOpen(r) {
  return r.guest && r.guest.readyState === WebSocket.OPEN;
}

function hostInGrace(r) {
  return !r.host && r.hostAwayAt && Date.now() - r.hostAwayAt < HOST_GRACE_MS;
}

function roomJoinable(r) {
  if (!r) return false;
  if (hostOpen(r)) return true;
  return hostInGrace(r);
}

function linkPeers(r, room) {
  if (!hostOpen(r) || !guestOpen(r)) return false;
  send(r.guest, { type: "linked" });
  send(r.host, { type: "linked" });
  slog("room:linked", room);
  return true;
}

function cleanupRoom(roomId) {
  const r = rooms.get(roomId);
  if (r?.voltFlushTimer) clearTimeout(r.voltFlushTimer);
  rooms.delete(roomId);
  slog("room:deleted", roomId);
}

function summarizeSignal(payload) {
  if (!payload) return "";
  if (payload.ready) return "ready";
  if (payload.sdp) return `sdp:${payload.sdp.type}(${payload.sdp.sdp?.length || 0}b)`;
  if (payload.candidate) return "candidate";
  return JSON.stringify(payload).slice(0, 80);
}

function rawToString(raw) {
  return typeof raw === "string" ? raw : Buffer.isBuffer(raw) ? raw.toString("utf8") : String(raw);
}

function isCompactVolt(raw) {
  const s = rawToString(raw);
  return s.length > 3 && s[0] === "v" && s[1] === "|";
}

function scheduleVoltFlush(r) {
  if (r.voltFlushTimer) return;
  r.voltFlushTimer = setTimeout(() => {
    r.voltFlushTimer = null;
    const pending = r.voltPending;
    r.voltPending = null;
    if (pending && guestOpen(r)) sendRaw(r.guest, pending);
  }, VOLT_FLUSH_MS);
}

function queueVolt(r, raw) {
  r.voltPending = raw;
  scheduleVoltFlush(r);
}

function relayGame(r, ws, payload) {
  const peer = ws._role === "host" ? r.guest : r.host;
  const pt = payload?.type;
  if (ws._role === "host" && pt === "v" && guestOpen(r)) {
    const m = Math.round((payload.m || 0) * 1000);
    const h = payload.h ?? payload.holder ?? 1;
    const t = payload.t ?? 0;
    queueVolt(r, `v|${m}|${h}|${t}`);
    return;
  }
  if (peer && peer.readyState === WebSocket.OPEN) {
    // Оптимизация: предсериализуем только один раз, без лишнего логирования
    peer.send(JSON.stringify({ type: "game", from: ws._role, payload }));
    // логируем только важные (не heat/pass — они идут десятками в секунду)
    if (pt !== "heat" && pt !== "pass") slog("game:relay", `${ws._role} room=${ws._room} ${pt || "?"}`);
  }
}

const server = http.createServer((req, res) => {
  const url = req.url || "/";
  if (url === "/logs") {
    // Оптимизация: только кольцевой буфер, без чтения файла (быстро)
    const body = LOG_RING.join("\n") + "\n";
    res.writeHead(200, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
    res.end(body);
    return;
  }
  if (url === "/rooms") {
    const info = [...rooms.entries()].map(([id, r]) => ({
      id,
      hasHost: hostOpen(r),
      hostAway: hostInGrace(r),
      hasGuest: guestOpen(r),
      pending: r.pending?.length || 0,
      voltPending: !!r.voltPending,
      ageSec: Math.round((Date.now() - r.created) / 1000)
    }));
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify(info, null, 2));
    return;
  }
  if (url === "/queue") {
    const info = [...matchQueue.entries()].map(([stake, q]) => ({ stake, waiting: q.length }));
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify(info));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
  res.end("hotwire signal ok\n");
});

const wss = new WebSocket.Server({ server, path: "/ws", perMessageDeflate: false });

const GAME_QUIET = new Set(["v", "volt", "sfx"]);

// ── Матчмейкинг-очередь ──
// matchQueue: Map<stake, Array<{ws, name}>>
const matchQueue = new Map();
const MATCH_TIMEOUT_MS = 30000;

function genMatchRoomId() {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "mm";
  for (let i = 0; i < 6; i++) id += chars[Math.floor(Math.random() * chars.length)];
  return id;
}

function tryMatchmake(ws, stake, name) {
  let queue = matchQueue.get(stake);
  if (!queue) { queue = []; matchQueue.set(stake, queue); }

  // ищем соперника (первый в очереди, не мы сами)
  const opponent = queue.find((e) => e.ws !== ws && e.ws.readyState === WebSocket.OPEN);
  if (opponent) {
    // убираем соперника из очереди
    matchQueue.set(stake, queue.filter((e) => e !== opponent));
    // создаём комнату и связываем
    const room = genMatchRoomId();
    rooms.set(room, { host: opponent.ws, created: Date.now(), pending: [], hostAwayAt: null, voltPending: null, voltFlushTimer: null });
    opponent.ws._room = room;
    opponent.ws._role = "host";
    send(opponent.ws, { type: "matched", room, role: "host", opponent: name || "Rival" });
    // guest
    ws._room = room;
    ws._role = "guest";
    rooms.get(room).guest = ws;
    send(ws, { type: "matched", room, role: "guest", opponent: opponent.name || "Rival" });
    linkPeers(rooms.get(room), room);
    slog("match:paired", `${stake} ${room}`);
    return true;
  }
  // нет соперника — встаём в очередь
  queue.push({ ws, name: name || "Player", joinedAt: Date.now() });
  ws._matchStake = stake;
  send(ws, { type: "searching" });
  slog("match:queued", `${stake} (${queue.length} waiting)`);
  return false;
}

function removeFromQueue(ws) {
  if (!ws._matchStake) return;
  const queue = matchQueue.get(ws._matchStake);
  if (queue) {
    matchQueue.set(ws._matchStake, queue.filter((e) => e.ws !== ws));
    if (matchQueue.get(ws._matchStake)?.length === 0) matchQueue.delete(ws._matchStake);
  }
  ws._matchStake = null;
}

// периодическая чистка зависших в очереди
setInterval(() => {
  const now = Date.now();
  for (const [stake, queue] of matchQueue) {
    const fresh = queue.filter((e) => {
      if (now - e.joinedAt > MATCH_TIMEOUT_MS) {
        if (e.ws.readyState === WebSocket.OPEN) send(e.ws, { type: "match-timeout" });
        return false;
      }
      return e.ws.readyState === WebSocket.OPEN;
    });
    if (fresh.length === 0) matchQueue.delete(stake);
    else matchQueue.set(stake, fresh);
  }
}, 5000);

wss.on("connection", (ws, req) => {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress || "?";
  slog("ws:connect", String(ip));
  if (ws._socket?.setNoDelay) ws._socket.setNoDelay(true);

  ws.on("message", (raw) => {
    const rawStr = rawToString(raw);

    // compact ball/paddle: "b|..." или "p|..." → relay без JSON parse
    if (rawStr.length > 2 && (rawStr[0] === 'b' || rawStr[0] === 'p') && rawStr[1] === '|') {
      const r = rooms.get(ws._room);
      if (!r) return;
      const peer = ws._role === "host" ? r.guest : r.host;
      if (peer && peer.readyState === WebSocket.OPEN) peer.send(rawStr);
      return;
    }

    if (isCompactVolt(raw)) {
      const r = rooms.get(ws._room);
      if (!r || ws._role !== "host") return;
      if (guestOpen(r)) queueVolt(r, rawStr);
      return;
    }

    let msg;
    try { msg = JSON.parse(rawToString(raw)); } catch {
      slog("ws:bad-json", String(raw).slice(0, 60));
      return;
    }

    if (msg.type === "ping") {
      send(ws, { type: "pong", t: msg.t });
      return;
    }

    // ── Матчмейкинг ──
    if (msg.type === "matchmake") {
      const stake = String(msg.stake || "10");
      const name = String(msg.name || "Player");
      tryMatchmake(ws, stake, name);
      return;
    }
    if (msg.type === "cancel-matchmake") {
      removeFromQueue(ws);
      send(ws, { type: "match-cancelled" });
      return;
    }

    if (msg.type === "create") {
      const room = String(msg.room || "").toLowerCase();
      if (!room || room.length < 4) { send(ws, { type: "error", msg: "bad room" }); slog("create:bad", room); return; }
      if (rooms.has(room)) { send(ws, { type: "error", msg: "room busy" }); slog("create:busy", room); return; }
      rooms.set(room, { host: ws, created: Date.now(), pending: [], hostAwayAt: null, voltPending: null, voltFlushTimer: null });
      ws._room = room;
      ws._role = "host";
      send(ws, { type: "created", room });
      slog("room:created", room);
      return;
    }

    if (msg.type === "join") {
      const room = String(msg.room || "").toLowerCase();
      const r = rooms.get(room);
      if (!roomJoinable(r)) {
        send(ws, { type: "error", msg: "room not found" });
        slog("join:notfound", room);
        return;
      }
      if (guestOpen(r)) { send(ws, { type: "error", msg: "room full" }); slog("join:full", room); return; }
      r.guest = ws;
      ws._room = room;
      ws._role = "guest";
      send(ws, { type: "joined", room });
      if (hostOpen(r)) {
        send(r.host, { type: "guest-joined" });
        linkPeers(r, room);
      } else {
        send(ws, { type: "host-away" });
        slog("room:guest-wait-host", room);
      }
      return;
    }

    if (msg.type === "rejoin") {
      const room = String(msg.room || "").toLowerCase();
      const as = msg.as === "guest" ? "guest" : "host";
      const r = rooms.get(room);
      if (!r) {
        send(ws, { type: "error", msg: "room not found" });
        slog("rejoin:notfound", `${as} ${room}`);
        return;
      }

      if (as === "host") {
        if (hostOpen(r)) {
          send(ws, { type: "error", msg: "room busy" });
          slog("rejoin:host-busy", room);
          return;
        }
        if (!hostOpen(r) && r.hostAwayAt && Date.now() - r.hostAwayAt >= HOST_GRACE_MS) {
          send(ws, { type: "error", msg: "room expired" });
          cleanupRoom(room);
          slog("rejoin:host-expired", room);
          return;
        }
        r.host = ws;
        r.hostAwayAt = null;
        ws._room = room;
        ws._role = "host";
        send(ws, { type: "rejoined", room, role: "host" });
        if (guestOpen(r)) {
          send(r.guest, { type: "host-reconnected" });
          linkPeers(r, room);
          slog("room:host-rejoin-linked", room);
        } else {
          slog("room:host-rejoin-waiting", room);
        }
        return;
      }

      if (guestOpen(r)) {
        send(ws, { type: "error", msg: "room full" });
        slog("rejoin:guest-full", room);
        return;
      }
      if (!roomJoinable(r)) {
        send(ws, { type: "error", msg: "room not found" });
        slog("rejoin:guest-noroom", room);
        return;
      }
      r.guest = ws;
      ws._room = room;
      ws._role = "guest";
      const hostAway = !hostOpen(r);
      send(ws, { type: "rejoined", room, role: "guest", hostAway });
      if (hostOpen(r)) {
        send(r.host, { type: "guest-joined" });
        linkPeers(r, room);
        slog("room:guest-rejoin-linked", room);
      } else {
        send(ws, { type: "host-away" });
        slog("room:guest-rejoin-host-away", room);
      }
      return;
    }

    if (msg.type === "game") {
      const r = rooms.get(ws._room);
      if (!r) { slog("game:no-room", ws._room || "?"); return; }
      relayGame(r, ws, msg.payload);
      return;
    }

    if (msg.type === "signal") {
      const r = rooms.get(ws._room);
      if (!r) { slog("signal:no-room", ws._room || "?"); return; }
      const peer = ws._role === "host" ? r.guest : r.host;
      const packet = { type: "signal", from: ws._role, payload: msg.payload };
      const summary = summarizeSignal(msg.payload);
      if (peer && peer.readyState === WebSocket.OPEN) {
        send(peer, packet);
        slog("signal:relay", `${ws._role}->${ws._role === "host" ? "guest" : "host"} room=${ws._room} ${summary}`);
      } else if (ws._role === "host") {
        r.pending.push({ from: ws._role, payload: msg.payload });
        if (r.pending.length > 8) r.pending.shift();
        slog("signal:buffer", `room=${ws._room} ${summary} q=${r.pending.length}`);
      } else {
        slog("signal:drop", `guest->host room=${ws._room} host=${r.host?.readyState} ${summary}`);
      }
    }
  });

  ws.on("close", () => {
    const roomId = ws._room;
    slog("ws:close", `${ws._role || "?"} room=${roomId || "-"}`);
    removeFromQueue(ws);   // убрать из очереди матчмейкинга
    if (!roomId) return;
    const r = rooms.get(roomId);
    if (!r) return;

    if (ws._role === "host") {
      r.host = null;
      r.hostAwayAt = Date.now();
      if (guestOpen(r)) {
        send(r.guest, { type: "host-away" });
        slog("room:host-away", roomId);
      } else {
        slog("room:host-away-empty", roomId);
      }
    } else {
      r.guest = null;
      if (hostOpen(r)) send(r.host, { type: "peer-left" });
    }
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [id, r] of rooms) {
    if (now - r.created > 2 * 60 * 60 * 1000) cleanupRoom(id);
    else if (!hostOpen(r) && r.hostAwayAt && now - r.hostAwayAt > HOST_GRACE_MS && !guestOpen(r)) {
      cleanupRoom(id);
    }
  }
}, 60000);

const PORT = Number(process.env.PORT) || 8787;
server.listen(PORT, "0.0.0.0", () => slog("server:start", `port=${PORT} log=${LOG_FILE}`));