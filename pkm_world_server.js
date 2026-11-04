/**
 * ============================================================
 * PKM WORLD SERVER — "thế giới" Pokémon theo lớp (module phụ cho server.js)
 * ============================================================
 * Có 1 "Sảnh chung" (__hall) và các phòng bên trong. Có 2 chế độ (biến môi trường
 * WORLD_MODE, hoặc sửa thẳng mảng ROOMS bên dưới):
 *   - guild (mặc định): 3 hội quán Đất / Nước / Lửa
 *   - class           : mỗi lớp 1 phòng, danh sách lớp lấy từ WORLD_CLASSES
 *                       (mặc định "1,2,3,4,5"), học sinh tự vào đúng lớp của mình
 * Học sinh tự do đổi phòng. Server chỉ giữ: ai đang ở phòng nào, đứng ở đâu,
 * đang quay mặt hướng nào, Pokémon đại diện (id), trạng thái (rảnh/đang đấu).
 *
 * KHÔNG đụng tới logic thách đấu / trận đấu trong server.js. Thách đấu vẫn
 * đi qua "challenge:send" như cũ (client gọi PkmPresence.sendChallenge).
 *
 * CÁCH GẮN: trong server.js, ngay TRƯỚC dòng "const PORT = ...", thêm:
 *     require("./pkm_world_server")(io, { onlineUsers, statusOf });
 *
 * TẢI: vị trí không phát tức thì mà gom lại, mỗi WORLD_TICK_MS (mặc định
 * 150ms) gửi 1 gói nhỏ cho phòng đó, chỉ gồm những người vừa di chuyển.
 * Chỉnh được bằng biến môi trường:
 *     WORLD_TICK_MS (mặc định 150)   WORLD_ROOM_MAX (mặc định 150 người/phòng)
 *     WORLD_MODE (guild | class)     WORLD_CLASSES (vd "1,2,3,4,5")
 * ============================================================
 */
"use strict";

module.exports = function attachWorld(io, ctx) {
  const { onlineUsers, statusOf } = ctx;

  // PHẢI khớp với pkm_world.js (client)
  const WORLD_W = 960, WORLD_H = 640;
  const MIN_X = 24, MAX_X = WORLD_W - 24, MIN_Y = 72, MAX_Y = WORLD_H - 24;

  const HALL = "__hall";

  // ---------- CẤU HÌNH PHÒNG ----------
  const MODE = (process.env.WORLD_MODE || "guild").toLowerCase();
  const THEMES = ["earth", "water", "fire"];   // earth = Đất, water = Nước, fire = Lửa
  const ROOMS = MODE === "class"
    ? (process.env.WORLD_CLASSES || "1,2,3,4,5").split(",").map(s => s.trim()).filter(Boolean)
        .map((k, i) => ({ key: k, label: "Lớp " + k, theme: THEMES[i % 3] }))
    : [
        { key: "dat",  label: "Hội quán Đất",  theme: "earth" },
        { key: "nuoc", label: "Hội quán Nước", theme: "water" },
        { key: "lua",  label: "Hội quán Lửa",  theme: "fire" },
      ];
  const ROOM_BY_KEY = new Map(ROOMS.map(r => [r.key, r]));
  const TICK_MS = Number(process.env.WORLD_TICK_MS) || 150;
  const ROOM_MAX = Number(process.env.WORLD_ROOM_MAX) || 150;
  const STATUS_CHECK_MS = 1000;
  const MOVE_MIN_GAP_MS = 40;       // bỏ bớt gói di chuyển gửi quá dày
  const EMOTE_COOLDOWN_MS = 800;
  const EMOTES = ["👋", "❤️", "😄", "👍", "❓", "⚔️"];
  const MAX_PKM_ID = 2000;

  const worlds = new Map();   // roomKey -> Map(playerId -> member)
  const memberOf = new Map(); // playerId -> member
  let nextSid = 1;            // id số ngắn để gói di chuyển nhẹ

  // ---------- tiện ích ----------
  function str(v, max, fallback = "") {
    const s = typeof v === "string" ? v.trim().slice(0, max) : "";
    return s || fallback;
  }
  function num(v, lo, hi, dflt) {
    const n = Number(v);
    return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt;
  }
  function safe(socket, ev, fn) {
    socket.on(ev, function (...args) {
      try { return fn.apply(this, args); }
      catch (e) { console.error(`[world ${ev}]`, e); }
    });
  }
  const sockRoom = key => "w:" + key;
  const pub = m => ({
    sid: m.sid, playerId: m.playerId, name: m.name, className: m.className,
    pkmId: m.pkmId, x: Math.round(m.x), y: Math.round(m.y), dir: m.dir, status: m.lastStatus,
  });
  const sizeOf = key => (worlds.get(key) ? worlds.get(key).size : 0);
  const roomList = () => [
    { key: HALL, label: "Sảnh chung", theme: "hall", count: sizeOf(HALL) },
    ...ROOMS.map(r => ({ key: r.key, label: r.label, theme: r.theme, count: sizeOf(r.key) })),
  ];
  // Chế độ "class": học sinh vào thẳng phòng lớp mình (nếu khớp). Còn lại vào Sảnh.
  function defaultRoom(user) {
    if (MODE !== "class") return HALL;
    const c = (user.className || "").trim().toLowerCase();
    const r = c && ROOMS.find(x => x.key.toLowerCase() === c || x.label.toLowerCase() === c);
    return r ? r.key : HALL;
  }

  function removeMember(m, notify = true) {
    const w = worlds.get(m.room);
    if (w) { w.delete(m.playerId); if (w.size === 0) worlds.delete(m.room); }
    if (memberOf.get(m.playerId) === m) memberOf.delete(m.playerId);
    const sock = io.sockets.sockets.get(m.socketId);
    if (sock) sock.leave(sockRoom(m.room));
    if (notify) io.to(sockRoom(m.room)).emit("world:leave", { sid: m.sid });
  }

  // ---------- socket ----------
  io.on("connection", socket => {

    // Vào 1 phòng (rời phòng cũ nếu có). ack({ ok, room, you, members })
    safe(socket, "world:join", (d, ack) => {
      const reply = typeof ack === "function" ? ack : () => {};
      d = d || {};
      const pid = socket.data.playerId;
      const user = pid && onlineUsers.get(pid);
      // Chưa gửi presence:hello (hoặc socket này đã bị thay) -> client thử lại sau
      if (!user || user.socketId !== socket.id) return reply({ ok: false, reason: "no_hello" });

      const key = str(d.room, 40) || defaultRoom(user);
      if (key !== HALL && !ROOM_BY_KEY.has(key)) return reply({ ok: false, reason: "no_room" });

      const old = memberOf.get(pid);
      const target = worlds.get(key);
      if (target && target.size >= ROOM_MAX && !(old && old.room === key)) return reply({ ok: false, reason: "full" });
      if (old) removeMember(old);

      const m = {
        sid: nextSid++, playerId: pid, name: user.name, className: user.className,
        pkmId: num(d.pkmId, 0, MAX_PKM_ID, 0),
        x: num(d.x, MIN_X, MAX_X, WORLD_W / 2),
        y: num(d.y, MIN_Y, MAX_Y, WORLD_H - 130),
        dir: 1, dirty: false, room: key, socketId: socket.id,
        lastStatus: statusOf(user), lastMoveAt: 0, lastEmoteAt: 0,
      };
      let w = worlds.get(key);
      if (!w) { w = new Map(); worlds.set(key, w); }
      w.set(pid, m);
      memberOf.set(pid, m);
      socket.join(sockRoom(key));
      socket.to(sockRoom(key)).emit("world:enter", pub(m));
      reply({ ok: true, room: key, you: pub(m), members: [...w.values()].map(pub), rooms: roomList() });
    });

    safe(socket, "world:leave", () => {
      const m = memberOf.get(socket.data.playerId);
      if (m && m.socketId === socket.id) removeMember(m);
    });

    // Di chuyển: chỉ ghi nhận, việc phát đi do vòng tick lo
    safe(socket, "world:move", d => {
      const m = memberOf.get(socket.data.playerId);
      if (!m || m.socketId !== socket.id || !d) return;
      const now = Date.now();
      if (now - m.lastMoveAt < MOVE_MIN_GAP_MS) return;
      m.lastMoveAt = now;
      m.x = num(d.x, MIN_X, MAX_X, m.x);
      m.y = num(d.y, MIN_Y, MAX_Y, m.y);
      m.dir = d.dir === -1 ? -1 : 1;
      m.dirty = true;
    });

    safe(socket, "world:emote", d => {
      const m = memberOf.get(socket.data.playerId);
      if (!m || m.socketId !== socket.id || !d || !EMOTES.includes(d.t)) return;
      const now = Date.now();
      if (now - m.lastEmoteAt < EMOTE_COOLDOWN_MS) return;
      m.lastEmoteAt = now;
      io.to(sockRoom(m.room)).emit("world:emote", { sid: m.sid, t: d.t });
    });

    safe(socket, "world:avatar", d => {
      const m = memberOf.get(socket.data.playerId);
      if (!m || m.socketId !== socket.id || !d) return;
      m.pkmId = num(d.pkmId, 0, MAX_PKM_ID, m.pkmId);
      io.to(sockRoom(m.room)).emit("world:avatar", { sid: m.sid, pkmId: m.pkmId });
    });

    // Danh sách phòng + số người đang ở trong thế giới của mỗi phòng
    safe(socket, "world:rooms", (d, ack) => {
      if (typeof ack === "function") ack({ rooms: roomList() });
    });

    socket.on("disconnect", () => {
      try {
        const m = memberOf.get(socket.data.playerId);
        if (m && m.socketId === socket.id) removeMember(m);
      } catch (e) { console.error("[world disconnect]", e); }
    });
  });

  // ---------- vòng tick: gom vị trí, gửi theo từng phòng ----------
  setInterval(() => {
    try {
      for (const [key, w] of worlds) {
        const moved = [];
        for (const m of w.values()) {
          if (!m.dirty) continue;
          m.dirty = false;
          moved.push([m.sid, Math.round(m.x), Math.round(m.y), m.dir]);
        }
        if (moved.length) io.to(sockRoom(key)).volatile.emit("world:tick", moved);
      }
    } catch (e) { console.error("[world tick]", e); }
  }, TICK_MS);

  // ---------- 1 giây/lần: cập nhật trạng thái (đang đấu...) + dọn người đã offline ----------
  setInterval(() => {
    try {
      for (const m of [...memberOf.values()]) {
        const u = onlineUsers.get(m.playerId);
        if (!u) { removeMember(m); continue; }
        const st = statusOf(u);
        if (st !== m.lastStatus) {
          m.lastStatus = st;
          io.to(sockRoom(m.room)).emit("world:status", { sid: m.sid, status: st });
        }
      }
    } catch (e) { console.error("[world status]", e); }
  }, STATUS_CHECK_MS);
};
