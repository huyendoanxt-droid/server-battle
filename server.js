/**
 * ============================================================
 * PKM SERVER v3 — hiện diện toàn app + thách đấu + trận PvP
 * ============================================================
 * Vai trò:
 *   1. HIỆN DIỆN (presence): mọi trang của game kết nối và báo "tôi online".
 *      Định danh cố định = "lớp|tên" (do client gửi). Mất kết nối chỉ bị coi
 *      là offline sau PRESENCE_GRACE_MS (chuyển trang không bị nhấp nháy,
 *      không còn "tên ma" nằm lại server).
 *   2. THÁCH ĐẤU: lời mời + luật "từ chối/lờ 1 lần -> lời mời kế tiếp từ
 *      BẤT KỲ AI bị tự động nhận". Người đang ở trang bận (làm bài kiểm
 *      tra, đấu với máy...) không bị làm phiền; lời mời bị giữ lại có hạn.
 *   3. GHÉP NGẪU NHIÊN (hàng đợi, timeout 2 phút).
 *   4. TRẬN PvP: server giữ máu/damage THẬT, luân phiên chính/phụ.
 *   5. Chuyển tiếp thông báo kết bạn (danh sách bạn nằm ở Firestore,
 *      server KHÔNG lưu).
 *
 * TẢI SERVER: server chỉ phát thay đổi của TỪNG NGƯỜI (presence:delta) và
 * chỉ cho những socket đang mở bảng Online (room "watchers"). Không còn
 * phát cả danh sách cho tất cả mọi người.
 *
 * Chạy: node server.js   |   Render: Start Command = node server.js
 * ============================================================
 */
"use strict";

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

// Lưới an toàn cuối: 1 lỗi bất ngờ không được làm sập cả server.
process.on("uncaughtException", err => console.error("[uncaughtException]", err));
process.on("unhandledRejection", err => console.error("[unhandledRejection]", err));

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// ===================== CẤU HÌNH (chỉnh tay được) =====================
const MAX_TEAM_SIZE = 3;
const QUEUE_TIMEOUT_MS = 2 * 60 * 1000;         // chờ ghép ngẫu nhiên tối đa
const RECONNECT_GRACE_MS = 60 * 1000;           // rớt mạng giữa trận: chờ nối lại
const TEAM_SELECT_TIMEOUT_MS = 60 * 1000;       // thời gian chọn đội hình sau khi ghép
const ANSWER_TIMEOUT_MS = 25 * 1000;            // mỗi lượt chờ tối đa/bên
const CHALLENGE_TIMEOUT_MS = Number(process.env.CHALLENGE_TIMEOUT_MS) || 10 * 1000; // 10s không phản hồi = từ chối
const CHALLENGE_COOLDOWN_MS = 2000;             // chống bấm gửi lời mời liên tục
const PRESENCE_GRACE_MS = Number(process.env.PRESENCE_GRACE_MS) || 10 * 1000; // mất kết nối bao lâu thì coi là offline
const QUEUED_CHALLENGE_TTL_MS = 2 * 60 * 1000;  // lời mời giữ lại cho người đang bận
const LIST_LIMIT = 150;                         // tối đa số người trả về mỗi lần hỏi
const CHAT_LIMIT_PER_FRIEND = 3;                // tối đa số tin nhắn GỬI cho 1 người/phiên

// Hàng rào đầu tiên, KHÔNG phải chặn tuyệt đối — học sinh vẫn có thể né bằng
// cách viết cách chữ, viết tắt khác, chèn ký tự... Chấp nhận đây là bộ lọc cơ bản.
// LƯU Ý: đã bỏ những từ ngắn trùng với từ tiếng Việt bình thường khi gõ KHÔNG
// DẤU (vd "cac"="các", "lon"="lớn", "di"="đi", "buoi"="buổi") để tránh chặn
// nhầm câu nói vô hại — chỉ giữ từ có dấu đầy đủ (ít trùng) hoặc từ lóng
// mạng không trùng nghĩa nào khác. Bạn tự thêm/bớt cho khớp thực tế lớp mình.
const BANNED_WORDS = ["đm", "đmm", "đệch", "đệt", "đéo", "địt", "đjt", "cút", "vcl", "clgt", "đĩ", "lồn", "buồi", "cặc"];
function filterProfanity(text) {
  let out = text;
  BANNED_WORDS.forEach(w => {
    const re = new RegExp(w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
    out = out.replace(re, m => "*".repeat(m.length));
  });
  return out;
}

// dmgNormal và dmgAoe là 2 QUỸ ĐỘC LẬP (giống bản offline: atk và sAtk là
// 2 chỉ số tách biệt). Áp dụng cho ĐỦ 3 con; 2 hoặc 1 con nhân theo N/3.
const FAIR_TOTALS = { hp: 3000, dmgNormal: 260, dmgAoe: 260, def: 150 };

// ===================== TIỆN ÍCH =====================
function str(v, max, fallback = "") {
  const s = typeof v === "string" ? v.trim().slice(0, max) : "";
  return s || fallback;
}
function num(v, lo, hi, dflt) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt;
}
// Bọc mọi handler: lỗi trong 1 sự kiện chỉ bị ghi log, không làm sập tiến trình.
function on(socket, ev, fn) {
  socket.on(ev, function (...args) {
    try { return fn.apply(this, args); }
    catch (e) { console.error(`[handler ${ev}]`, e); }
  });
}

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
  const targetDmgNormal = FAIR_TOTALS.dmgNormal * scale;
  const targetDmgAoe = FAIR_TOTALS.dmgAoe * scale;
  const targetDEF = FAIR_TOTALS.def * scale;

  const sumHP = rawUnits.reduce((s, u) => s + (u.hp || 0), 0) || 1;
  const sumAtk = rawUnits.reduce((s, u) => s + (u.atk || 0), 0) || 1;
  const sumSAtk = rawUnits.reduce((s, u) => s + (u.sAtk || 0), 0) || 1;
  const sumDEF = rawUnits.reduce((s, u) => s + (u.def || 0), 0) || 1;

  return rawUnits.map(u => {
    const hp = Math.max(1, Math.round(targetHP * ((u.hp || 0) / sumHP)));
    return {
      id: u.id,
      name: u.name || String(u.id),
      type: u.type || "normal",
      hp, maxHp: hp,
      atk: Math.max(1, Math.round(targetDmgNormal * ((u.atk || 0) / sumAtk))),
      sAtk: Math.max(1, Math.round(targetDmgAoe * ((u.sAtk || 0) / sumSAtk))),
      def: Math.max(1, Math.round(targetDEF * ((u.def || 0) / sumDEF))),
      alive: true,
    };
  });
}

// Làm sạch dữ liệu 1 Pokémon do client gửi (tránh NaN/undefined làm hỏng phép tính)
function sanitizeUnit(u) {
  if (!u || typeof u !== "object") return null;
  return {
    id: num(u.id, 0, 1e6, 0),
    name: str(u.name, 30, "Pokémon"),
    type: Object.prototype.hasOwnProperty.call(TYPE_CHART, u.type) ? u.type : "normal",
    hp: num(u.hp, 1, 1e7, 20),
    atk: num(u.atk, 0, 1e7, 20),
    def: num(u.def, 0, 1e7, 15),
    sAtk: num(u.sAtk, 0, 1e7, 20),
  };
}

// ===================== TRẠNG THÁI TRONG RAM =====================
const waitingQueue = [];      // { playerId, socketId, timeoutHandle }
const rooms = new Map();      // roomId -> room
const onlineUsers = new Map(); // playerId -> user
const pendingChallenges = new Map(); // toPlayerId -> { fromId, timer }

/**
 * user = {
 *   playerId, name, className, ownedCount,
 *   socketId, connected, offlineTimer, disconnectedAt,
 *   busy        : đang ở trang bận (test, đấu máy...) — do client báo
 *   searching   : đang xếp hàng ghép ngẫu nhiên
 *   inRoomId    : đang trong phòng đấu (select hoặc battle)
 *   forcedAcceptNext : vừa từ chối/lờ 1 lời mời -> lời mời kế tiếp bị ép nhận
 *   queuedChallenge  : { fromId, fromName, timer } lời mời giữ lại (người đang bận)
 *   lastChallengeAt
 * }
 * Trạng thái hiển thị được SUY RA từ các cờ trên (không lưu riêng) nên không
 * thể bị "kẹt" sai như trước.
 */
function statusOf(u) {
  if (u.inRoomId && rooms.has(u.inRoomId)) return "in_match";
  if (u.searching) return "searching";
  if (u.busy) return "busy";
  return "idle";
}
function publicUser(u) {
  return { playerId: u.playerId, name: u.name, className: u.className, status: statusOf(u) };
}
function notifyPresence(u) { io.to("watchers").emit("presence:delta", publicUser(u)); }
function notifyRemoved(playerId) { io.to("watchers").emit("presence:delta", { playerId, removed: true }); }
function getMe(socket) {
  const u = onlineUsers.get(socket.data.playerId);
  return u && u.socketId === socket.id ? u : null;
}
function emitTo(u, ev, data) { if (u && u.socketId) io.to(u.socketId).emit(ev, data); }

function makeRoomId() {
  return "room_" + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

// ===================== HÀNG ĐỢI NGẪU NHIÊN =====================
function leaveQueue(playerId) {
  const idx = waitingQueue.findIndex(e => e.playerId === playerId);
  if (idx !== -1) {
    clearTimeout(waitingQueue[idx].timeoutHandle);
    waitingQueue.splice(idx, 1);
  }
  const u = onlineUsers.get(playerId);
  if (u && u.searching) { u.searching = false; notifyPresence(u); }
}
function leaveQueueBySocket(socketId) {
  const e = waitingQueue.find(x => x.socketId === socketId);
  if (e) leaveQueue(e.playerId);
}
function validForMatch(u, socketId) {
  return u && u.connected && (!socketId || u.socketId === socketId)
    && !(u.inRoomId && rooms.has(u.inRoomId));
}
function tryPairQueue() {
  while (waitingQueue.length >= 2) {
    const a = waitingQueue.shift();
    clearTimeout(a.timeoutHandle);
    const ua = onlineUsers.get(a.playerId);
    if (!validForMatch(ua, a.socketId)) { if (ua) { ua.searching = false; } continue; }
    const bIdx = waitingQueue.findIndex(e => e.playerId !== a.playerId && validForMatch(onlineUsers.get(e.playerId), e.socketId));
    if (bIdx === -1) { waitingQueue.unshift(a); break; }
    const [b] = waitingQueue.splice(bIdx, 1);
    clearTimeout(b.timeoutHandle);
    createRoom(ua, onlineUsers.get(b.playerId));
  }
}

// ===================== PHÒNG ĐẤU =====================
function getPlayer(room, playerId) { return room.players.find(p => p.playerId === playerId); }
function otherPlayer(room, playerId) { return room.players.find(p => p.playerId !== playerId); }

function clearRoomTimers(room) {
  if (room.teamSelectTimeout) clearTimeout(room.teamSelectTimeout);
  if (room.answerTimer) clearTimeout(room.answerTimer);
  Object.values(room.reconnectTimers || {}).forEach(t => clearTimeout(t));
}

// Trả người chơi về trạng thái tự do sau khi phòng đóng. Ai đã rớt mạng thì dọn luôn.
function releasePlayers(room) {
  room.players.forEach(p => {
    const u = onlineUsers.get(p.playerId);
    if (!u) return;
    if (u.inRoomId === room.roomId) u.inRoomId = null;
    if (!u.connected) removeUser(p.playerId);
    else notifyPresence(u);
  });
}

function createRoom(uA, uB) {
  if (!uA || !uB || uA.playerId === uB.playerId) return null;
  if (!validForMatch(uA) || !validForMatch(uB)) return null;

  leaveQueue(uA.playerId);
  leaveQueue(uB.playerId);
  const unitsAllowed = Math.min(MAX_TEAM_SIZE, uA.ownedCount || MAX_TEAM_SIZE, uB.ownedCount || MAX_TEAM_SIZE);
  const roomId = makeRoomId();
  const room = {
    roomId, unitsAllowed,
    phase: "select", // select -> battle -> ended
    players: [uA, uB].map(u => ({ playerId: u.playerId, socketId: u.socketId, name: u.name, rawTeam: null })),
    teams: {}, turnCounter: 0, ended: false,
    pendingActions: {}, reconnectTimers: {}, teamSelectTimeout: null, answerTimer: null,
  };
  rooms.set(roomId, room);

  [[uA, uB], [uB, uA]].forEach(([me, opp]) => {
    me.inRoomId = roomId;
    me.searching = false;
    if (me.queuedChallenge) { clearTimeout(me.queuedChallenge.timer); me.queuedChallenge = null; }
    const sock = io.sockets.sockets.get(me.socketId);
    if (sock) { sock.join(roomId); sock.data.roomId = roomId; sock.data.playerId = me.playerId; }
    emitTo(me, "match:found", { roomId, unitsAllowed, opponentId: opp.playerId, opponentName: opp.name });
    notifyPresence(me);
  });

  room.teamSelectTimeout = setTimeout(() => {
    if (!room.ended && room.phase === "select") cancelRoom(room, "team_select_timeout");
  }, TEAM_SELECT_TIMEOUT_MS);
  return room;
}

// Huỷ phòng khi chưa vào trận (chưa chọn xong đội hình / có người bỏ đi)
function cancelRoom(room, reason) {
  if (room.ended) return;
  room.ended = true;
  room.phase = "ended";
  clearRoomTimers(room);
  io.to(room.roomId).emit("match:cancelled", { reason });
  rooms.delete(room.roomId);
  releasePlayers(room);
}

function endRoom(room, winnerId, reason) {
  if (room.ended) return;
  room.ended = true;
  room.phase = "ended";
  clearRoomTimers(room);
  io.to(room.roomId).emit("battle:end", { winnerId, reason });
  rooms.delete(room.roomId);
  releasePlayers(room);
  room.players.forEach(p => tryConsumeQueuedChallenge(p.playerId));
}

function publicState(room) {
  const [pidA, pidB] = room.players.map(p => p.playerId);
  const tA = room.teams[pidA], tB = room.teams[pidB];
  if (!tA || !tB) return null;
  return {
    turnCounter: room.turnCounter,
    isAOE: currentIsAOE(room),
    primaryId: primaryPlayerId(room),
    teams: { [pidA]: tA.units, [pidB]: tB.units },
    activeIdx: { [pidA]: tA.activeIdx, [pidB]: tB.activeIdx },
  };
}
function broadcastState(room) {
  const st = publicState(room);
  if (st) io.to(room.roomId).emit("state:update", st);
}

// ===================== LOGIC TRẬN (server giữ nguồn sự thật) =====================
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

// 1 VÒNG = 2 LƯỢT (mỗi bên làm CHÍNH 1 lần); mỗi vòng đổi thường/AOE.
function primaryPlayerId(room) { return room.players[room.turnCounter % 2].playerId; }
function currentIsAOE(room) { return Math.floor(room.turnCounter / 2) % 2 === 1; }

function scheduleAnswerTimeout(room) {
  if (room.answerTimer) clearTimeout(room.answerTimer);
  room.pendingActions = {};
  room.answerTimer = setTimeout(() => {
    try { resolveTurn(room); } catch (e) { console.error("[resolveTurn timeout]", e); }
  }, ANSWER_TIMEOUT_MS);
}

// Lượt chỉ được xử lý khi CẢ 2 bên đã trả lời (hoặc hết 25s). Chỉ câu trả lời
// của bên CHÍNH quyết định đòn đánh; bên PHỤ chỉ để đồng bộ nhịp.
function resolveTurn(room) {
  if (room.ended || room.phase !== "battle") return;
  if (room.answerTimer) clearTimeout(room.answerTimer);

  const primaryId = primaryPlayerId(room);
  const [pidA, pidB] = room.players.map(p => p.playerId);
  if (!room.pendingActions[pidA]) room.pendingActions[pidA] = { correct: false };
  if (!room.pendingActions[pidB]) room.pendingActions[pidB] = { correct: false };

  const defenderId = otherPlayer(room, primaryId).playerId;
  applyAttack(room, primaryId, defenderId, currentIsAOE(room));

  room.turnCounter += 1;
  room.pendingActions = {};

  const winner = checkWinner(room);
  broadcastState(room);
  if (winner) endRoom(room, winner === "draw" ? null : winner, winner === "draw" ? "draw" : "hp_zero");
  else scheduleAnswerTimeout(room);
}

// ===================== HIỆN DIỆN =====================
function registerUser(socket, d) {
  d = d || {};
  const playerId = str(d.playerId, 80);
  if (!playerId) return null;

  const existing = onlineUsers.get(playerId);
  if (existing) {
    if (existing.offlineTimer) clearTimeout(existing.offlineTimer);
    // Cùng 1 học sinh mở nơi khác: kết nối mới thay kết nối cũ
    if (existing.socketId && existing.socketId !== socket.id) {
      const old = io.sockets.sockets.get(existing.socketId);
      if (old) { old.data.replaced = true; old.emit("presence:replaced"); }
    }
  }

  const u = {
    playerId,
    name: str(d.name, 30, "Ẩn danh"),
    className: str(d.className, 20) || null,
    ownedCount: num(d.ownedCount, 1, MAX_TEAM_SIZE, 1),
    busy: !!d.busy,
    socketId: socket.id, connected: true, offlineTimer: null, disconnectedAt: 0,
    searching: existing ? existing.searching : false,
    inRoomId: existing ? existing.inRoomId : null,
    forcedAcceptNext: existing ? existing.forcedAcceptNext : false,
    queuedChallenge: existing ? existing.queuedChallenge : null,
    lastChallengeAt: existing ? existing.lastChallengeAt : 0,
    chatCounts: existing ? existing.chatCounts : {}, // reset về 0 khi rời hẳn (removeUser) rồi vào lại
  };
  if (u.inRoomId && !rooms.has(u.inRoomId)) u.inRoomId = null;
  onlineUsers.set(playerId, u);
  socket.data.playerId = playerId;
  delete socket.data.replaced;

  // Nếu đang trong phòng: cập nhật socket mới cho phòng để tin nhắn đến đúng nơi
  if (u.inRoomId) {
    const room = rooms.get(u.inRoomId);
    const p = room && getPlayer(room, playerId);
    if (p) p.socketId = socket.id;
  }

  notifyPresence(u);
  tryConsumeQueuedChallenge(playerId);
  return u;
}

// Xoá hẳn 1 người khỏi danh sách online và dọn mọi thứ liên quan.
function removeUser(playerId) {
  const u = onlineUsers.get(playerId);
  if (!u) return;
  if (u.offlineTimer) clearTimeout(u.offlineTimer);
  if (u.queuedChallenge) clearTimeout(u.queuedChallenge.timer);
  onlineUsers.delete(playerId);
  leaveQueue(playerId);
  clearPendingChallengeFor(playerId);
  // Lời mời do người này gửi đi mà chưa được trả lời -> huỷ
  for (const [toId, p] of pendingChallenges) {
    if (p.fromId === playerId) {
      clearTimeout(p.timer);
      pendingChallenges.delete(toId);
      emitTo(onlineUsers.get(toId), "challenge:cancelled", { fromPlayerId: playerId });
    }
  }
  notifyRemoved(playerId);
}

function removeUserIfStillOffline(playerId, socketId) {
  const u = onlineUsers.get(playerId);
  if (!u || u.connected || u.socketId !== socketId) return; // đã nối lại -> giữ
  if (u.inRoomId && rooms.has(u.inRoomId)) return;          // còn trong trận -> phòng tự dọn khi kết thúc
  removeUser(playerId);
}

// Quét định kỳ: lưới an toàn cuối cùng chống "tên ma".
setInterval(() => {
  const now = Date.now();
  for (const [pid, u] of onlineUsers) {
    if (u.inRoomId && !rooms.has(u.inRoomId)) u.inRoomId = null;
    if (!u.connected && !(u.inRoomId && rooms.has(u.inRoomId)) && now - u.disconnectedAt > PRESENCE_GRACE_MS * 3) {
      removeUser(pid);
    }
  }
}, 30 * 1000);

// ===================== THÁCH ĐẤU =====================
function clearPendingChallengeFor(playerId) {
  const p = pendingChallenges.get(playerId);
  if (p) { clearTimeout(p.timer); pendingChallenges.delete(playerId); }
}

// 10s không phản hồi (hoặc từ chối tay): gắn vé "bắt buộc nhận lần sau".
function resolveChallengeTimeout(toPlayerId) {
  const pending = pendingChallenges.get(toPlayerId);
  if (!pending) return;
  clearTimeout(pending.timer);
  pendingChallenges.delete(toPlayerId);
  const target = onlineUsers.get(toPlayerId);
  if (target) target.forcedAcceptNext = true;
  emitTo(onlineUsers.get(pending.fromId), "challenge:declined", { toPlayerId });
}

// Người bị ép nhận đang bận -> giữ lời mời lại (có hạn), vào trận ngay khi rảnh.
function queueChallenge(target, from) {
  if (target.queuedChallenge) clearTimeout(target.queuedChallenge.timer);
  const targetId = target.playerId;
  target.queuedChallenge = {
    fromId: from.playerId, fromName: from.name,
    timer: setTimeout(() => {
      const t = onlineUsers.get(targetId);
      if (!t || !t.queuedChallenge) return;
      const fromId = t.queuedChallenge.fromId;
      t.queuedChallenge = null;
      emitTo(onlineUsers.get(fromId), "challenge:expired", { toPlayerId: targetId });
    }, QUEUED_CHALLENGE_TTL_MS),
  };
}

function tryConsumeQueuedChallenge(playerId) {
  const target = onlineUsers.get(playerId);
  if (!target || !target.queuedChallenge || !target.connected) return;
  const st = statusOf(target);
  if (st !== "idle" && st !== "searching") return; // vẫn bận -> giữ tiếp
  const q = target.queuedChallenge;
  const from = onlineUsers.get(q.fromId);
  clearTimeout(q.timer);
  target.queuedChallenge = null;
  if (!from || !validForMatch(from)) return; // người mời đã đi/đang bận trận khác
  target.forcedAcceptNext = false;
  emitTo(target, "challenge:forced", { fromName: from.name });
  createRoom(from, target);
}

// ===================== HTTP =====================
app.get("/", (req, res) => res.send("PKM server is running."));
app.get("/health", (req, res) => res.json({ ok: true, time: Date.now() }));
app.get("/stats", (req, res) => {
  const online = [...onlineUsers.values()].filter(u => u.connected).length;
  res.json({
    online,
    waiting: waitingQueue.length,
    activeMatches: [...rooms.values()].filter(r => r.phase === "battle").length,
    time: Date.now(),
  });
});

// ===================== SOCKET.IO =====================
io.on("connection", socket => {

  // ---------- HIỆN DIỆN ----------
  on(socket, "presence:hello", d => { registerUser(socket, d); });

  on(socket, "presence:status", d => {
    const me = getMe(socket);
    if (!me) return;
    me.busy = !!(d && d.busy);
    notifyPresence(me);
    tryConsumeQueuedChallenge(me.playerId);
  });

  // Nút thoát chủ động: báo offline ngay, không chờ hết thời gian ân hạn.
  on(socket, "presence:bye", () => {
    const me = getMe(socket);
    if (!me) return;
    if (me.inRoomId && rooms.has(me.inRoomId)) return; // đang trong trận -> để cơ chế nối lại xử lý
    removeUser(me.playerId);
  });

  on(socket, "panel:open", () => { socket.join("watchers"); });
  on(socket, "panel:close", () => { socket.leave("watchers"); });

  // Danh sách người online: scope 'class' (cùng lớp) hoặc 'all'
  on(socket, "presence:list", (d, ack) => {
    if (typeof ack !== "function") return;
    d = d || {};
    const me = getMe(socket);
    const cls = d.scope === "class" ? (str(d.className, 20) || (me && me.className)) : null;
    const users = [];
    for (const u of onlineUsers.values()) {
      if (!u.connected) continue;
      if (d.scope === "class" && (!cls || u.className !== cls)) continue;
      users.push(publicUser(u));
      if (users.length >= LIST_LIMIT) break;
    }
    ack({ users });
  });

  // Hỏi trạng thái của 1 nhóm người cụ thể (danh sách bạn bè)
  on(socket, "presence:query", (d, ack) => {
    if (typeof ack !== "function") return;
    const ids = Array.isArray(d && d.ids) ? d.ids.slice(0, 200) : [];
    const users = [];
    ids.forEach(id => {
      const u = onlineUsers.get(str(id, 80));
      if (u && u.connected) users.push(publicUser(u));
    });
    ack({ users });
  });

  // ---------- NHẮN TIN: chỉ giữa bạn bè (client tự đảm bảo), tối đa
  // CHAT_LIMIT_PER_FRIEND tin GỬI cho mỗi người/phiên, không lưu trữ. ----------
  on(socket, "chat:send", d => {
    const me = getMe(socket);
    if (!me || !d) return;
    const toId = str(d.toPlayerId, 80);
    const text = str(d.text, 300);
    if (!toId || !text || toId === me.playerId) return;

    const target = onlineUsers.get(toId);
    if (!target || !target.connected) return emitTo(me, "chat:error", { toPlayerId: toId, reason: "offline" });

    me.chatCounts = me.chatCounts || {};
    const used = me.chatCounts[toId] || 0;
    if (used >= CHAT_LIMIT_PER_FRIEND) return emitTo(me, "chat:error", { toPlayerId: toId, reason: "limit" });

    const clean = filterProfanity(text);
    me.chatCounts[toId] = used + 1;
    const remaining = CHAT_LIMIT_PER_FRIEND - me.chatCounts[toId];
    emitTo(target, "chat:message", { fromPlayerId: me.playerId, fromName: me.name, text: clean });
    emitTo(me, "chat:sent", { toPlayerId: toId, text: clean, remaining });
  });

  // ---------- BẠN BÈ: chỉ chuyển tiếp thông báo (dữ liệu nằm ở Firestore) ----------
  on(socket, "friend:notify", d => {
    const me = getMe(socket);
    if (!me || !d) return;
    const t = onlineUsers.get(str(d.toPlayerId, 80));
    if (!t || !t.connected) return;
    emitTo(t, "friend:notify", {
      fromPlayerId: me.playerId, fromName: me.name, fromClass: me.className,
      kind: d.kind === "accepted" ? "accepted" : "request",
    });
  });

  // ---------- GHÉP NGẪU NHIÊN ----------
  on(socket, "queue:join", () => {
    const me = getMe(socket);
    if (!me || statusOf(me) === "in_match") return;
    leaveQueue(me.playerId);
    me.searching = true;
    notifyPresence(me);

    const entry = { playerId: me.playerId, socketId: socket.id, timeoutHandle: null };
    entry.timeoutHandle = setTimeout(() => {
      const idx = waitingQueue.indexOf(entry);
      if (idx === -1) return;
      waitingQueue.splice(idx, 1);
      const u = onlineUsers.get(entry.playerId);
      if (u) { u.searching = false; notifyPresence(u); }
      io.to(entry.socketId).emit("queue:timeout");
    }, QUEUE_TIMEOUT_MS);
    waitingQueue.push(entry);
    tryPairQueue();
  });

  on(socket, "queue:cancel", () => {
    const me = getMe(socket);
    if (me) leaveQueue(me.playerId);
  });

  // ---------- THÁCH ĐẤU TRỰC TIẾP ----------
  on(socket, "challenge:send", d => {
    const me = getMe(socket);
    if (!me || !d) return;
    const toId = str(d.toPlayerId, 80);
    if (!toId || toId === me.playerId) return;

    const now = Date.now();
    if (now - me.lastChallengeAt < CHALLENGE_COOLDOWN_MS) return emitTo(me, "challenge:error", { reason: "too_fast" });
    me.lastChallengeAt = now;

    const target = onlineUsers.get(toId);
    if (!target || !target.connected) return emitTo(me, "challenge:error", { reason: "offline" });
    if (statusOf(me) === "in_match") return emitTo(me, "challenge:error", { reason: "self_busy" });

    const message = str(d.message, 200);
    const ts = statusOf(target);

    // Vé "bắt buộc nhận" đang có hiệu lực với người này
    if (target.forcedAcceptNext) {
      if (ts === "idle" || ts === "searching") {
        target.forcedAcceptNext = false;
        emitTo(target, "challenge:forced", { fromName: me.name });
        createRoom(me, target);
      } else {
        queueChallenge(target, me); // đang bận -> giữ lại có hạn
        emitTo(me, "challenge:queued", { toPlayerId: toId });
      }
      return;
    }

    if (ts !== "idle") return emitTo(me, "challenge:error", { reason: "busy" });
    if (pendingChallenges.has(toId)) return emitTo(me, "challenge:error", { reason: "already_pending" });

    const timer = setTimeout(() => resolveChallengeTimeout(toId), CHALLENGE_TIMEOUT_MS);
    pendingChallenges.set(toId, { fromId: me.playerId, timer });
    emitTo(target, "challenge:incoming", {
      fromPlayerId: me.playerId, fromName: me.name, fromClass: me.className, message,
    });
    emitTo(me, "challenge:sent", { toPlayerId: toId });
  });

  on(socket, "challenge:accept", d => {
    const me = getMe(socket);
    if (!me || !d) return;
    const fromId = str(d.fromPlayerId, 80);
    const pending = pendingChallenges.get(me.playerId);
    if (!pending || pending.fromId !== fromId) return; // đã hết hạn / không khớp
    clearPendingChallengeFor(me.playerId);
    const from = onlineUsers.get(fromId);
    if (!from || !from.connected) return emitTo(me, "challenge:error", { reason: "offline" });
    if (!createRoom(from, me)) emitTo(me, "challenge:error", { reason: "busy" });
  });

  on(socket, "challenge:decline", () => {
    const me = getMe(socket);
    if (me && pendingChallenges.has(me.playerId)) resolveChallengeTimeout(me.playerId);
  });

  // ---------- NỘP ĐỘI HÌNH -> SERVER QUY ĐỔI CHỈ SỐ ----------
  on(socket, "team:submit", d => {
    if (!d) return;
    const room = rooms.get(d.roomId);
    if (!room || room.ended || room.phase !== "select") return;
    const me = getPlayer(room, socket.data.playerId);
    if (!me || !Array.isArray(d.team)) return;

    const clean = d.team.slice(0, room.unitsAllowed).map(sanitizeUnit).filter(Boolean);
    if (clean.length !== room.unitsAllowed) return; // bắt buộc đủ số con để công bằng
    me.rawTeam = clean;

    if (room.players.every(p => p.rawTeam)) {
      if (room.teamSelectTimeout) clearTimeout(room.teamSelectTimeout);
      room.phase = "battle";
      room.players.forEach(p => { room.teams[p.playerId] = { units: normalizeTeam(p.rawTeam), activeIdx: 0 }; });
      const [pA, pB] = room.players;
      io.to(pA.socketId).emit("battle:start", { myTeam: room.teams[pA.playerId].units, oppTeam: room.teams[pB.playerId].units });
      io.to(pB.socketId).emit("battle:start", { myTeam: room.teams[pB.playerId].units, oppTeam: room.teams[pA.playerId].units });
      broadcastState(room);
      scheduleAnswerTimeout(room);
    }
  });

  // ---------- TRẢ LỜI CÂU HỎI (chỉ gửi đúng/sai) ----------
  on(socket, "answer:submit", d => {
    if (!d) return;
    const room = rooms.get(d.roomId);
    if (!room || room.ended || room.phase !== "battle") return;
    if (d.turnCounter !== room.turnCounter) return; // trả lời trễ của lượt cũ
    const pid = socket.data.playerId;
    if (!pid || !getPlayer(room, pid) || room.pendingActions[pid]) return;

    room.pendingActions[pid] = { correct: !!d.correct };
    const [pidA, pidB] = room.players.map(p => p.playerId);
    if (room.pendingActions[pidA] && room.pendingActions[pidB]) resolveTurn(room);
  });

  // ---------- NỐI LẠI PHÒNG (chuyển trang / rớt mạng) ----------
  on(socket, "room:rejoin", d => {
    d = d || {};
    const room = rooms.get(d.roomId);
    const playerId = str(d.playerId, 80);
    const p = room && !room.ended && getPlayer(room, playerId);
    if (!p) return socket.emit("room:rejoin_failed");

    p.socketId = socket.id;
    socket.data.playerId = playerId;
    socket.data.roomId = room.roomId;
    socket.join(room.roomId);

    if (room.reconnectTimers[playerId]) {
      clearTimeout(room.reconnectTimers[playerId]);
      delete room.reconnectTimers[playerId];
    }
    const u = onlineUsers.get(playerId);
    if (u) {
      u.socketId = socket.id; u.connected = true; u.inRoomId = room.roomId;
      if (u.offlineTimer) { clearTimeout(u.offlineTimer); u.offlineTimer = null; }
    }

    const opp = otherPlayer(room, playerId);
    if (room.phase === "select") {
      // Đang ở giai đoạn chọn đội hình: chưa có máu/lượt để gửi
      socket.emit("room:rejoin_ok", {
        phase: "select", roomId: room.roomId, unitsAllowed: room.unitsAllowed,
        opponentId: opp && opp.playerId, opponentName: opp && opp.name, alreadySubmitted: !!p.rawTeam,
      });
    } else {
      const st = publicState(room);
      if (!st) return socket.emit("room:rejoin_failed");
      socket.emit("room:rejoin_ok", { phase: "battle", roomId: room.roomId, unitsAllowed: room.unitsAllowed, ...st });
      if (opp) emitTo(onlineUsers.get(opp.playerId) || { socketId: opp.socketId }, "opponent:reconnected");
    }
  });

  // ---------- MẤT KẾT NỐI ----------
  socket.on("disconnect", () => {
    try {
      leaveQueueBySocket(socket.id);
      if (socket.data.replaced) return; // đã có kết nối mới thay thế
      const me = getMe(socket);
      if (!me) return;

      me.connected = false;
      me.disconnectedAt = Date.now();

      // Đang trong phòng: giữ chỗ RECONNECT_GRACE_MS chờ nối lại
      const room = me.inRoomId && rooms.get(me.inRoomId);
      if (room && !room.ended) {
        if (room.phase === "battle") {
          const opp = otherPlayer(room, me.playerId);
          if (opp) io.to(opp.socketId).emit("opponent:disconnected");
        }
        room.reconnectTimers[me.playerId] = setTimeout(() => {
          try {
            if (room.ended) return;
            const stillOpp = otherPlayer(room, me.playerId);
            if (room.phase === "select") cancelRoom(room, "opponent_left");
            else endRoom(room, stillOpp ? stillOpp.playerId : null, "opponent_disconnected");
          } catch (e) { console.error("[reconnect timer]", e); }
        }, RECONNECT_GRACE_MS);
      }

      // Hết PRESENCE_GRACE_MS mà không nối lại -> offline thật sự
      me.offlineTimer = setTimeout(() => removeUserIfStillOffline(me.playerId, socket.id), PRESENCE_GRACE_MS);
    } catch (e) {
      console.error("[disconnect]", e);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("PKM server v3 listening on port " + PORT));
