/**
 * ============================================================
 * PKM BATTLE SERVER v2 — server giữ quyền TÍNH TOÁN THẬT
 * ============================================================
 * Vai trò server:
 *   1. Ghép trận (hàng đợi, timeout 2 phút)
 *   2. Quy đổi chỉ số công bằng khi cả 2 bên nộp đội hình
 *   3. Giữ trạng thái máu/sát thương THẬT của cả trận (nguồn sự thật
 *      duy nhất) — tính sát thương, xử lý AOE, xác định thắng/thua
 *   4. Đồng bộ lại toàn bộ trạng thái khi 1 bên rớt mạng nối lại
 *
 * KHÔNG làm: không biết nội dung câu hỏi/từ vựng — client tự chấm
 * đúng/sai 1 câu hỏi rồi báo kết quả (đúng/sai) lên, server tin và
 * dùng kết quả đó để tính đòn đánh.
 *
 * Chạy: node server.js
 * Deploy: Render.com -> New Web Service -> Start Command: node server.js
 * ============================================================
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// ===================== CẤU HÌNH (chỉnh tay được) =====================
const MAX_TEAM_SIZE = 3;
const QUEUE_TIMEOUT_MS = 2 * 60 * 1000;
const RECONNECT_GRACE_MS = 60 * 1000;
const TEAM_SELECT_TIMEOUT_MS = 45 * 1000;
const ANSWER_TIMEOUT_MS = 25 * 1000; // mỗi vòng chờ tối đa 25s/bên
const CHALLENGE_TIMEOUT_MS = 10 * 1000; // 10s không phản hồi lời thách đấu = coi như từ chối

// Tổng chỉ số công bằng khi ĐỦ 3 con (2 hoặc 1 con sẽ nhân theo tỉ lệ N/3)
// -> khởi điểm để ước lượng ~15 câu/bên, CHỈNH TAY SAU KHI TEST THỬ.
const FAIR_TOTALS = { hp: 3000, dmg: 260, def: 150 };

app.get("/", (req, res) => res.send("PKM Battle server is running."));
app.get("/health", (req, res) => res.json({ ok: true, time: Date.now() }));

// Số liệu hiển thị ở màn chờ ghép trận: bao nhiêu người đang chờ (kèm
// tên), và đang có những cặp nào đấu với nhau. Chỉ hiện TÊN, không lộ
// playerId/roomId nội bộ.
app.get("/stats", (req, res) => {
  res.json({
    waiting: waitingQueue.length,
    waitingNames: waitingQueue.map(e => e.name || "Ẩn danh"),
    activeMatches: [...rooms.values()].map(r => ({
      a: r.players[0]?.name || "Ẩn danh",
      b: r.players[1]?.name || "Ẩn danh",
    })),
    time: Date.now(),
  });
});

// ===================== BẢNG KHẮC HỆ (nhúng sẵn, không gọi mạng ngoài) =====================
const TYPE_CHART = {
  normal: [],
  fire: ["grass", "ice", "bug", "steel"],
  water: ["fire", "ground", "rock"],
  electric: ["water", "flying"],
  grass: ["water", "ground", "rock"],
  ice: ["grass", "ground", "flying", "dragon"],
  fighting: ["normal", "ice", "rock", "dark", "steel"],
  poison: ["grass", "fairy"],
  ground: ["fire", "electric", "poison", "rock", "steel"],
  flying: ["grass", "fighting", "bug"],
  psychic: ["fighting", "poison"],
  bug: ["grass", "psychic", "dark"],
  rock: ["fire", "ice", "flying", "bug"],
  ghost: ["psychic", "ghost"],
  dragon: ["dragon"],
  dark: ["psychic", "ghost"],
  steel: ["ice", "rock", "fairy"],
  fairy: ["fighting", "dragon", "dark"],
};
function isSuperEffective(attackerType, defenderType) {
  const list = TYPE_CHART[attackerType];
  return !!(list && list.includes(defenderType));
}

// ===================== CÔNG THỨC SÁT THƯƠNG (copy nguyên từ pkm_battle.js) =====================
function computeDamage(attacker, defender, isAOE) {
  const typeBonus = isSuperEffective(attacker.type, defender.type) ? 1.1 : 1.0;
  if (!isAOE) {
    return Math.max(15, Math.floor((attacker.atk * 1.8) / (1 + defender.def / 100) * typeBonus) * 2);
  }
  return Math.max(20, Math.floor((attacker.sAtk * 1.2) / (1 + defender.def / 100) * typeBonus) * 2);
}

// ===================== QUY ĐỔI CHỈ SỐ CÔNG BẰNG =====================
function normalizeTeam(rawUnits) {
  const n = rawUnits.length;
  const scale = n / MAX_TEAM_SIZE;
  const targetHP = FAIR_TOTALS.hp * scale;
  const targetDMG = FAIR_TOTALS.dmg * scale;
  const targetDEF = FAIR_TOTALS.def * scale;

  const sumHP = rawUnits.reduce((s, u) => s + (u.hp || 0), 0) || 1;
  const sumDMG = rawUnits.reduce((s, u) => s + (u.atk || 0) + (u.sAtk || 0), 0) || 1;
  const sumDEF = rawUnits.reduce((s, u) => s + (u.def || 0), 0) || 1;

  return rawUnits.map(u => {
    const hp = Math.max(1, Math.round(targetHP * ((u.hp || 0) / sumHP)));
    const dmgTotal = (u.atk || 0) + (u.sAtk || 0);
    const dmgShare = targetDMG * (dmgTotal / sumDMG);
    const ratio = dmgTotal > 0 ? (u.atk || 0) / dmgTotal : 0.5;
    return {
      id: u.id,
      name: u.name || u.id,
      type: u.type || "normal",
      hp, maxHp: hp,
      atk: Math.max(1, Math.round(dmgShare * ratio)),
      sAtk: Math.max(1, Math.round(dmgShare * (1 - ratio))),
      def: Math.max(1, Math.round(targetDEF * ((u.def || 0) / sumDEF))),
      alive: true,
    };
  });
}

// ===================== TRẠNG THÁI TRONG RAM =====================
let waitingQueue = [];
const rooms = new Map();

// Khu vực chờ (lobby): playerId -> thông tin + trạng thái hiện tại.
// status: 'idle' (rảnh, có thể bị thách đấu) | 'searching' (đang xếp
// hàng ghép ngẫu nhiên) | 'in_match' (đang thi đấu, không thể thách đấu).
// forcedAcceptNext: true nếu người này vừa từ chối/lờ đi 1 lời mời trước
// đó -> lời mời TIẾP THEO từ BẤT KỲ AI sẽ được tự động chấp nhận ngay.
// queuedChallengeFrom: nếu người này đang bận (in_match) mà bị 1 người có
// vé "forcedAccept" nhắm tới, lời mời đó được giữ lại, tự ghép ngay khi rảnh.
const onlineUsers = new Map();

// Lời mời đang chờ phản hồi: toPlayerId -> { fromPlayerId, timer }
const pendingChallenges = new Map();

function broadcastLobby() {
  const list = [...onlineUsers.values()].map(u => ({
    playerId: u.playerId, name: u.name, className: u.className, status: u.status,
  }));
  io.emit("lobby:update", { users: list });
}

function clearPendingChallengeFor(playerId) {
  const p = pendingChallenges.get(playerId);
  if (p) { clearTimeout(p.timer); pendingChallenges.delete(playerId); }
}

// Nếu người này vừa rảnh (idle) mà đang có 1 lời mời bị "giữ lại" từ lúc
// bận -> tự ghép trận ngay, không cần hỏi lại (đúng luật "vé bắt buộc nhận").
function tryConsumeQueuedChallenge(playerId) {
  const target = onlineUsers.get(playerId);
  if (!target || !target.queuedChallengeFrom) return;
  const fromId = target.queuedChallengeFrom;
  target.queuedChallengeFrom = null;
  target.forcedAcceptNext = false;
  const fromEntry = onlineUsers.get(fromId);
  if (!fromEntry || fromEntry.status === "in_match") return; // người mời đã rời/đang bận -> bỏ qua
  removeFromQueue(fromEntry.socketId);
  removeFromQueue(target.socketId);
  createRoom(fromEntry, target);
}

// Tạo phòng trực tiếp cho 2 người cụ thể (dùng chung cho: ghép ngẫu nhiên
// FIFO, thách đấu được chấp nhận, và vé "bắt buộc nhận" được tiêu thụ).
function createRoom(a, b) {
  const unitsAllowed = Math.min(MAX_TEAM_SIZE, a.ownedCount || MAX_TEAM_SIZE, b.ownedCount || MAX_TEAM_SIZE);
  const roomId = makeRoomId();
  const room = {
    roomId, unitsAllowed,
    players: [
      { playerId: a.playerId, socketId: a.socketId, name: a.name, rawTeam: null },
      { playerId: b.playerId, socketId: b.socketId, name: b.name, rawTeam: null },
    ],
    teams: {}, turnCounter: 0, ended: false,
    pendingActions: {}, reconnectTimers: {}, teamSelectTimeout: null, answerTimer: null,
  };
  rooms.set(roomId, room);

  const sockA = io.sockets.sockets.get(a.socketId);
  const sockB = io.sockets.sockets.get(b.socketId);
  if (sockA) { sockA.join(roomId); sockA.data.roomId = roomId; sockA.data.playerId = a.playerId; }
  if (sockB) { sockB.join(roomId); sockB.data.roomId = roomId; sockB.data.playerId = b.playerId; }

  io.to(a.socketId).emit("match:found", { roomId, unitsAllowed, opponentId: b.playerId });
  io.to(b.socketId).emit("match:found", { roomId, unitsAllowed, opponentId: a.playerId });

  [a.playerId, b.playerId].forEach(pid => {
    const u = onlineUsers.get(pid);
    if (u) { u.status = "in_match"; u.queuedChallengeFrom = null; }
  });
  broadcastLobby();

  room.teamSelectTimeout = setTimeout(() => {
    if (!room.players.every(p => p.rawTeam)) {
      io.to(roomId).emit("match:cancelled", { reason: "team_select_timeout" });
      rooms.delete(roomId);
      [a.playerId, b.playerId].forEach(pid => {
        const u = onlineUsers.get(pid);
        if (u) u.status = "idle";
      });
      broadcastLobby();
    }
  }, TEAM_SELECT_TIMEOUT_MS);

  return room;
}

function makeRoomId() {
  return "room_" + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}
function clearQueueEntry(entry) { if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle); }
function removeFromQueue(socketId) {
  const idx = waitingQueue.findIndex(e => e.socketId === socketId);
  if (idx !== -1) { clearQueueEntry(waitingQueue[idx]); waitingQueue.splice(idx, 1); }
}
function otherPlayer(room, playerId) { return room.players.find(p => p.playerId !== playerId); }
function getPlayer(room, playerId) { return room.players.find(p => p.playerId === playerId); }

function clearRoomTimers(room) {
  if (room.teamSelectTimeout) clearTimeout(room.teamSelectTimeout);
  if (room.answerTimer) clearTimeout(room.answerTimer);
  Object.values(room.reconnectTimers || {}).forEach(t => clearTimeout(t));
}

function endRoom(room, winnerId, reason) {
  if (room.ended) return;
  room.ended = true;
  clearRoomTimers(room);
  io.to(room.roomId).emit("battle:end", { winnerId, reason });
  rooms.delete(room.roomId);

  room.players.forEach(p => {
    const u = onlineUsers.get(p.playerId);
    if (u) u.status = "idle";
  });
  broadcastLobby();
  room.players.forEach(p => tryConsumeQueuedChallenge(p.playerId));
}

function publicState(room) {
  const [pidA, pidB] = room.players.map(p => p.playerId);
  return {
    turnCounter: room.turnCounter,
    isAOE: currentIsAOE(room),
    primaryId: primaryPlayerId(room), // ai là bên CHÍNH lượt hiện tại (được ra chưởng)
    teams: { [pidA]: room.teams[pidA].units, [pidB]: room.teams[pidB].units },
    activeIdx: { [pidA]: room.teams[pidA].activeIdx, [pidB]: room.teams[pidB].activeIdx },
  };
}
function broadcastState(room) { io.to(room.roomId).emit("state:update", publicState(room)); }

function advanceActiveIdx(team) {
  while (team.activeIdx < team.units.length && !team.units[team.activeIdx].alive) team.activeIdx++;
}

function applyAttack(room, attackerId, defenderId, isAOE) {
  const action = room.pendingActions[attackerId];
  if (!action || !action.correct) return;
  const atkTeam = room.teams[attackerId];
  const defTeam = room.teams[defenderId];
  const attacker = atkTeam.units[atkTeam.activeIdx];
  if (!attacker || !attacker.alive) return;

  if (isAOE) {
    defTeam.units.forEach(u => {
      if (!u.alive) return;
      u.hp = Math.max(0, u.hp - computeDamage(attacker, u, true));
      if (u.hp <= 0) u.alive = false;
    });
  } else {
    const target = defTeam.units[defTeam.activeIdx];
    if (target && target.alive) {
      target.hp = Math.max(0, target.hp - computeDamage(attacker, target, false));
      if (target.hp <= 0) target.alive = false;
    }
  }
  advanceActiveIdx(defTeam);
}

function checkWinner(room) {
  const [pidA, pidB] = room.players.map(p => p.playerId);
  const aDead = room.teams[pidA].units.every(u => !u.alive);
  const bDead = room.teams[pidB].units.every(u => !u.alive);
  if (aDead && bDead) return "draw";
  if (aDead) return pidB;
  if (bDead) return pidA;
  return null;
}

// ===================== LUÂN PHIÊN CHÍNH/PHỤ =====================
// Mỗi LƯỢT chỉ 1 bên ra chưởng ("chính"), bên kia ("phụ") vẫn được hỏi
// nhưng câu trả lời không ảnh hưởng -> chỉ 1 hiệu ứng chạy/lượt, tránh lag.
// 1 VÒNG = 2 lượt (mỗi bên làm chính đúng 1 lần) -> đổi thường/AOE mỗi vòng,
// giữ đúng tinh thần "chẵn thường, lẻ AOE" như bản solo.
function primaryPlayerId(room) {
  const idx = room.turnCounter % 2; // 0 -> players[0] chính, 1 -> players[1] chính
  return room.players[idx].playerId;
}
function currentIsAOE(room) {
  const round = Math.floor(room.turnCounter / 2);
  return round % 2 === 1;
}

function scheduleAnswerTimeout(room) {
  if (room.answerTimer) clearTimeout(room.answerTimer);
  room.pendingActions = {};
  // Hết 25s mà bên nào chưa trả lời -> tự tính người đó là SAI, rồi xử lý lượt luôn.
  room.answerTimer = setTimeout(() => resolveTurn(room), ANSWER_TIMEOUT_MS);
}

// Lượt chỉ được xử lý khi CẢ 2 bên đã trả lời (hoặc hết 25s) — nhưng chỉ
// câu trả lời của bên CHÍNH mới quyết định đòn đánh; bên PHỤ chỉ là điều
// kiện để "khoá" lượt cho đồng bộ nhịp, không ảnh hưởng kết quả.
function resolveTurn(room) {
  if (room.ended) return;
  if (room.answerTimer) clearTimeout(room.answerTimer);

  const primaryId = primaryPlayerId(room);
  const [pidA, pidB] = room.players.map(p => p.playerId);
  if (!room.pendingActions[pidA]) room.pendingActions[pidA] = { correct: false };
  if (!room.pendingActions[pidB]) room.pendingActions[pidB] = { correct: false };

  const isAOE = currentIsAOE(room);
  const defenderId = otherPlayer(room, primaryId).playerId;
  applyAttack(room, primaryId, defenderId, isAOE); // tự kiểm tra pendingActions[primaryId].correct bên trong

  room.turnCounter += 1;
  room.pendingActions = {};

  const winner = checkWinner(room);
  broadcastState(room);

  if (winner) {
    endRoom(room, winner === "draw" ? null : winner, winner === "draw" ? "draw" : "hp_zero");
  } else {
    scheduleAnswerTimeout(room);
  }
}

// Hết 10s không phản hồi (hoặc từ chối tay) -> gắn vé "bắt buộc nhận lần
// sau" cho người bị mời, báo cho người mời biết bị từ chối.
function resolveChallengeTimeout(toPlayerId) {
  const pending = pendingChallenges.get(toPlayerId);
  if (!pending) return;
  pendingChallenges.delete(toPlayerId);
  const target = onlineUsers.get(toPlayerId);
  if (target) target.forcedAcceptNext = true;
  const fromEntry = onlineUsers.get(pending.fromPlayerId);
  if (fromEntry) io.to(fromEntry.socketId).emit("challenge:declined", { toPlayerId });
  broadcastLobby();
}

// ===================== SOCKET.IO =====================
io.on("connection", socket => {

  // ---------- KHU VỰC CHỜ (lobby) ----------
  // Gọi ngay khi vào trang online — đăng ký hiện diện, hiện trong danh
  // sách "ai đang online" để người khác thách đấu được.
  socket.on("lobby:join", ({ playerId, name, className, ownedCount }) => {
    if (!playerId) return;
    socket.data.playerId = playerId;
    const safeName = (typeof name === "string" && name.trim()) ? name.trim().slice(0, 30) : "Ẩn danh";
    const safeClass = (typeof className === "string" && className.trim()) ? className.trim().slice(0, 20) : null;
    const safeOwned = Math.max(1, Math.min(MAX_TEAM_SIZE, parseInt(ownedCount, 10) || 1));
    const existing = onlineUsers.get(playerId);
    onlineUsers.set(playerId, {
      playerId, socketId: socket.id, name: safeName, className: safeClass, ownedCount: safeOwned,
      status: existing?.status === "in_match" ? "in_match" : "idle",
      forcedAcceptNext: existing?.forcedAcceptNext || false,
      queuedChallengeFrom: existing?.queuedChallengeFrom || null,
    });
    broadcastLobby();
  });

  socket.on("queue:join", ({ playerId, ownedCount, name }) => {
    if (!playerId) return;
    removeFromQueue(socket.id);
    const safeOwned = Math.max(1, Math.min(MAX_TEAM_SIZE, parseInt(ownedCount, 10) || 1));
    const safeName = (typeof name === "string" && name.trim()) ? name.trim().slice(0, 30) : "Ẩn danh";
    socket.data.playerId = playerId;

    const existing = onlineUsers.get(playerId);
    onlineUsers.set(playerId, {
      ...(existing || {}), playerId, socketId: socket.id, name: safeName, ownedCount: safeOwned,
      className: existing?.className || null, status: "searching",
      forcedAcceptNext: existing?.forcedAcceptNext || false,
      queuedChallengeFrom: existing?.queuedChallengeFrom || null,
    });
    broadcastLobby();

    const entry = { socketId: socket.id, playerId, ownedCount: safeOwned, name: safeName, timeoutHandle: null };
    waitingQueue.push(entry);

    if (waitingQueue.length >= 2) {
      const a = waitingQueue.shift();
      const b = waitingQueue.shift();
      clearQueueEntry(a); clearQueueEntry(b);
      createRoom(a, b);
    } else {
      entry.timeoutHandle = setTimeout(() => {
        removeFromQueue(socket.id);
        const u = onlineUsers.get(playerId);
        if (u) { u.status = "idle"; broadcastLobby(); }
        io.to(socket.id).emit("queue:timeout");
      }, QUEUE_TIMEOUT_MS);
    }
  });

  socket.on("queue:cancel", () => {
    removeFromQueue(socket.id);
    const playerId = socket.data.playerId;
    const u = playerId && onlineUsers.get(playerId);
    if (u && u.status === "searching") { u.status = "idle"; broadcastLobby(); }
  });

  // ---------- THÁCH ĐẤU TRỰC TIẾP ----------
  socket.on("challenge:send", ({ toPlayerId, message }) => {
    const fromId = socket.data.playerId;
    if (!fromId || !toPlayerId || fromId === toPlayerId) return;
    const from = onlineUsers.get(fromId);
    const target = onlineUsers.get(toPlayerId);
    if (!from) return;
    if (!target) { io.to(from.socketId).emit("challenge:error", { reason: "offline" }); return; }

    const safeMsg = (typeof message === "string" ? message : "").slice(0, 200);

    // Vé "bắt buộc nhận" đang có hiệu lực với người này
    if (target.forcedAcceptNext) {
      if (target.status === "idle") {
        target.forcedAcceptNext = false;
        removeFromQueue(from.socketId); removeFromQueue(target.socketId);
        createRoom(from, target);
      } else {
        // Đang bận (in_match) -> giữ lại lời mời, tự ghép ngay khi họ rảnh
        target.queuedChallengeFrom = fromId;
        io.to(from.socketId).emit("challenge:queued", { toPlayerId });
      }
      return;
    }

    if (target.status !== "idle") {
      io.to(from.socketId).emit("challenge:error", { reason: "busy" });
      return;
    }
    if (pendingChallenges.has(toPlayerId)) {
      io.to(from.socketId).emit("challenge:error", { reason: "already_pending" });
      return;
    }

    const timer = setTimeout(() => resolveChallengeTimeout(toPlayerId), CHALLENGE_TIMEOUT_MS);
    pendingChallenges.set(toPlayerId, { fromPlayerId: fromId, timer });
    io.to(target.socketId).emit("challenge:incoming", { fromPlayerId: fromId, fromName: from.name, message: safeMsg });
    io.to(from.socketId).emit("challenge:sent", { toPlayerId });
  });

  socket.on("challenge:accept", ({ fromPlayerId }) => {
    const myId = socket.data.playerId;
    if (!myId) return;
    const pending = pendingChallenges.get(myId);
    if (!pending || pending.fromPlayerId !== fromPlayerId) return; // lời mời đã hết hạn/không khớp
    clearPendingChallengeFor(myId);
    const fromEntry = onlineUsers.get(fromPlayerId);
    const myEntry = onlineUsers.get(myId);
    if (!fromEntry || !myEntry) return;
    removeFromQueue(fromEntry.socketId); removeFromQueue(myEntry.socketId);
    createRoom(fromEntry, myEntry);
  });

  socket.on("challenge:decline", () => {
    const myId = socket.data.playerId;
    if (!myId || !pendingChallenges.has(myId)) return;
    resolveChallengeTimeout(myId); // từ chối tay = giống hệt hết giờ (bị gắn vé bắt buộc nhận lần sau)
  });

  socket.on("team:submit", ({ roomId, team }) => {
    const room = rooms.get(roomId);
    if (!room) return;
    const me = getPlayer(room, socket.data.playerId);
    if (!me || !Array.isArray(team)) return;
    me.rawTeam = team.slice(0, room.unitsAllowed);

    if (room.players.every(p => p.rawTeam)) {
      if (room.teamSelectTimeout) clearTimeout(room.teamSelectTimeout);
      room.players.forEach(p => {
        room.teams[p.playerId] = { units: normalizeTeam(p.rawTeam), activeIdx: 0 };
      });
      const [pidA, pidB] = room.players.map(p => p.playerId);
      io.to(room.players[0].socketId).emit("battle:start", {
        myTeam: room.teams[pidA].units, oppTeam: room.teams[pidB].units,
      });
      io.to(room.players[1].socketId).emit("battle:start", {
        myTeam: room.teams[pidB].units, oppTeam: room.teams[pidA].units,
      });
      broadcastState(room);
      scheduleAnswerTimeout(room);
    }
  });

  socket.on("answer:submit", ({ roomId, correct, turnCounter }) => {
    const room = rooms.get(roomId);
    if (!room || room.ended) return;
    if (turnCounter !== room.turnCounter) return; // trả lời trễ của lượt cũ -> bỏ qua
    const pid = socket.data.playerId;
    if (!pid || room.pendingActions[pid]) return; // đã gửi rồi -> bỏ qua lần gửi thêm

    // Ghi nhận câu trả lời của người này (dù CHÍNH hay PHỤ). Kết quả đòn
    // đánh CHỈ phụ thuộc câu trả lời của bên CHÍNH (xử lý trong resolveTurn) —
    // nhưng lượt chỉ được xử lý khi CẢ 2 đã trả lời (hoặc hết 25s).
    room.pendingActions[pid] = { correct: !!correct };

    const [pidA, pidB] = room.players.map(p => p.playerId);
    if (room.pendingActions[pidA] && room.pendingActions[pidB]) {
      resolveTurn(room);
    }
  });

  socket.on("room:rejoin", ({ roomId, playerId }) => {
    const room = rooms.get(roomId);
    if (!room) return socket.emit("room:rejoin_failed");
    const p = getPlayer(room, playerId);
    if (!p) return socket.emit("room:rejoin_failed");

    p.socketId = socket.id;
    socket.data.playerId = playerId;
    socket.data.roomId = roomId;
    socket.join(roomId);

    if (room.reconnectTimers[playerId]) {
      clearTimeout(room.reconnectTimers[playerId]);
      delete room.reconnectTimers[playerId];
    }

    const uu = onlineUsers.get(playerId);
    if (uu) uu.socketId = socket.id; // cập nhật socket mới sau khi rejoin

    socket.emit("room:rejoin_ok", { unitsAllowed: room.unitsAllowed, ...publicState(room) });
    const opp = otherPlayer(room, playerId);
    if (opp) io.to(opp.socketId).emit("opponent:reconnected");
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);
    const roomId = socket.data.roomId;
    const playerId = socket.data.playerId;

    if (!roomId) {
      // Không ở trong trận nào -> dọn khỏi khu vực chờ ngay lập tức
      if (playerId) {
        onlineUsers.delete(playerId);
        clearPendingChallengeFor(playerId);
        broadcastLobby();
      }
      return;
    }
    if (!playerId) return;
    const room = rooms.get(roomId);
    if (!room || room.ended) return;

    const opp = otherPlayer(room, playerId);
    if (opp) io.to(opp.socketId).emit("opponent:disconnected");

    room.reconnectTimers[playerId] = setTimeout(() => {
      if (!room.ended) {
        const stillOpp = otherPlayer(room, playerId);
        endRoom(room, stillOpp ? stillOpp.playerId : null, "opponent_disconnected");
      }
    }, RECONNECT_GRACE_MS);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("PKM battle server (authoritative) listening on port " + PORT));
