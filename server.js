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

// Tổng chỉ số công bằng khi ĐỦ 3 con (2 hoặc 1 con sẽ nhân theo tỉ lệ N/3)
// -> khởi điểm để ước lượng ~15 câu/bên, CHỈNH TAY SAU KHI TEST THỬ.
const FAIR_TOTALS = { hp: 3000, dmg: 260, def: 150 };

app.get("/", (req, res) => res.send("PKM Battle server is running."));
app.get("/health", (req, res) => res.json({ ok: true, time: Date.now() }));

// Số liệu hiển thị ở màn chờ ghép trận: bao nhiêu người đang chờ, bao
// nhiêu trận đang diễn ra. Không có gì nhạy cảm, cho phép gọi tự do.
app.get("/stats", (req, res) => {
  res.json({
    waiting: waitingQueue.length,
    activeMatches: rooms.size,
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
  room.answerTimer = setTimeout(() => resolveTurn(room, primaryPlayerId(room), false), ANSWER_TIMEOUT_MS);
}

// primaryAnswerCorrect: kết quả câu trả lời của bên CHÍNH lượt này (bắt buộc).
function resolveTurn(room, primaryId, primaryCorrect) {
  if (room.ended) return;
  if (room.answerTimer) clearTimeout(room.answerTimer);

  const isAOE = currentIsAOE(room);
  const defenderId = otherPlayer(room, primaryId).playerId;

  room.pendingActions = { [primaryId]: { correct: !!primaryCorrect } };
  applyAttack(room, primaryId, defenderId, isAOE);

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

// ===================== SOCKET.IO =====================
io.on("connection", socket => {

  socket.on("queue:join", ({ playerId, ownedCount }) => {
    if (!playerId) return;
    removeFromQueue(socket.id);
    const safeOwned = Math.max(1, Math.min(MAX_TEAM_SIZE, parseInt(ownedCount, 10) || 1));
    const entry = { socketId: socket.id, playerId, ownedCount: safeOwned, timeoutHandle: null };
    waitingQueue.push(entry);
    socket.data.playerId = playerId;

    if (waitingQueue.length >= 2) {
      const a = waitingQueue.shift();
      const b = waitingQueue.shift();
      clearQueueEntry(a); clearQueueEntry(b);

      const unitsAllowed = Math.min(MAX_TEAM_SIZE, a.ownedCount, b.ownedCount);
      const roomId = makeRoomId();
      const room = {
        roomId, unitsAllowed,
        players: [
          { playerId: a.playerId, socketId: a.socketId, rawTeam: null },
          { playerId: b.playerId, socketId: b.socketId, rawTeam: null },
        ],
        teams: {}, turnCounter: 0, ended: false,
        pendingActions: {}, reconnectTimers: {}, teamSelectTimeout: null, answerTimer: null,
      };
      rooms.set(roomId, room);

      const sockA = io.sockets.sockets.get(a.socketId);
      const sockB = io.sockets.sockets.get(b.socketId);
      if (sockA) { sockA.join(roomId); sockA.data.roomId = roomId; }
      if (sockB) { sockB.join(roomId); sockB.data.roomId = roomId; }

      io.to(a.socketId).emit("match:found", { roomId, unitsAllowed, opponentId: b.playerId });
      io.to(b.socketId).emit("match:found", { roomId, unitsAllowed, opponentId: a.playerId });

      room.teamSelectTimeout = setTimeout(() => {
        if (!room.players.every(p => p.rawTeam)) {
          io.to(roomId).emit("match:cancelled", { reason: "team_select_timeout" });
          rooms.delete(roomId);
        }
      }, TEAM_SELECT_TIMEOUT_MS);
    } else {
      entry.timeoutHandle = setTimeout(() => {
        removeFromQueue(socket.id);
        io.to(socket.id).emit("queue:timeout");
      }, QUEUE_TIMEOUT_MS);
    }
  });

  socket.on("queue:cancel", () => removeFromQueue(socket.id));

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
    if (!pid) return;

    const primaryId = primaryPlayerId(room);
    if (pid !== primaryId) return; // bên PHỤ gửi lên -> không ảnh hưởng, bỏ qua luôn
    resolveTurn(room, primaryId, !!correct);
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

    socket.emit("room:rejoin_ok", { unitsAllowed: room.unitsAllowed, ...publicState(room) });
    const opp = otherPlayer(room, playerId);
    if (opp) io.to(opp.socketId).emit("opponent:reconnected");
  });

  socket.on("disconnect", () => {
    removeFromQueue(socket.id);
    const roomId = socket.data.roomId;
    const playerId = socket.data.playerId;
    if (!roomId || !playerId) return;
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
