// ろくぶつけ オンライン用サーバー（Cloudflare Workers + Durable Objects）
//
//   GET  /api/rooms              … 参加できる部屋の一覧
//   POST /api/rooms {name, pass} … 部屋をつくる（部屋名とパスコード）
//   GET  /api/rooms/:id/ws?pass=&nick=  … WebSocketで部屋に入る
//
// 部屋は最大6人。6人そろったら全員に start を送る。
// 試合中は「部屋を作った人（slot 0 = ホスト）」が試合を動かし、
// サーバーはホスト⇔ほかの人のメッセージを中継するだけ。

const MAX = 6;

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (req.method === "OPTIONS") return cors(new Response(null, { status: 204 }));

    const lobby = env.LOBBY.get(env.LOBBY.idFromName("main"));

    if (url.pathname === "/api/rooms" && req.method === "GET") {
      return cors(await lobby.fetch("https://lobby/list"));
    }

    if (url.pathname === "/api/rooms" && req.method === "POST") {
      let body = {};
      try { body = await req.json(); } catch (e) {}
      const name = clean(body.name, 16);
      const pass = String(body.pass || "").trim().slice(0, 16);
      if (!name) return cors(json({ error: "部屋名を入れてください" }, 400));
      if (pass.length < 1) return cors(json({ error: "パスコードを入れてください" }, 400));
      const id = randomId();
      const room = env.ROOM.get(env.ROOM.idFromName(id));
      await room.fetch("https://room/init", {
        method: "POST",
        body: JSON.stringify({ id, name, passHash: await sha(pass) }),
      });
      return cors(json({ id, name }));
    }

    const m = url.pathname.match(/^\/api\/rooms\/([a-z0-9]{8})\/ws$/);
    if (m) {
      const room = env.ROOM.get(env.ROOM.idFromName(m[1]));
      return room.fetch(req);
    }

    if (url.pathname === "/" || url.pathname === "/api") return cors(json({ ok: true, name: "rokubutsuke-online" }));
    return cors(json({ error: "not found" }, 404));
  },
};

// ===== 部屋 =====
export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/init") {
      const d = await req.json();
      await this.state.storage.put("meta", {
        id: d.id, name: d.name, passHash: d.passHash, status: "waiting", created: Date.now(),
      });
      return new Response("ok");
    }

    if (req.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket", { status: 426 });
    }

    const meta = await this.state.storage.get("meta");
    const pass = url.searchParams.get("pass") || "";
    const nick = clean(url.searchParams.get("nick"), 10) || "プレイヤー";

    if (!meta || meta.status === "closed") return reject("部屋が見つかりません");
    if ((await sha(pass)) !== meta.passHash) return reject("パスコードがちがいます");
    if (meta.status !== "waiting") return reject("この部屋はもう試合を始めています");

    const members = this.members();
    if (members.length >= MAX) return reject("この部屋は満員です");
    // ホスト（部屋を作った人）がいない部屋には入れない
    if (members.length > 0 && !members.some((x) => x.slot === 0)) return reject("この部屋は閉じています");

    const used = new Set(members.map((x) => x.slot));
    let slot = 0;
    while (used.has(slot)) slot++;

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ slot, nick });
    server.send(JSON.stringify({ t: "welcome", slot, room: meta.name, id: meta.id }));

    this.sendAll({ t: "members", members: this.members() });
    if (this.members().length >= MAX) await this.start();
    else await this.report();

    return new Response(null, { status: 101, webSocket: client });
  }

  members(except) {
    const list = [];
    for (const ws of this.state.getWebSockets()) {
      if (ws === except) continue;
      const a = ws.deserializeAttachment();
      if (a && !a.gone) list.push({ slot: a.slot, nick: a.nick });
    }
    return list.sort((a, b) => a.slot - b.slot);
  }

  sockets(except) {
    return this.state.getWebSockets().filter((ws) => {
      if (ws === except) return false;
      const a = ws.deserializeAttachment();
      return a && !a.gone;
    });
  }

  sendAll(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.sockets(except)) {
      try { ws.send(s); } catch (e) {}
    }
  }

  async start() {
    const meta = await this.state.storage.get("meta");
    if (!meta || meta.status !== "waiting") return;
    meta.status = "playing";
    await this.state.storage.put("meta", meta);
    this.sendAll({ t: "start", members: this.members() });
    await this.report();
  }

  async webSocketMessage(ws, msg) {
    let d;
    try { d = JSON.parse(msg); } catch (e) { return; }
    const me = ws.deserializeAttachment();
    if (!me || me.gone) return;

    // ホスト → ほかの全員（試合の様子・できごと）
    if (d.t === "snap" || d.t === "ev") {
      if (me.slot !== 0) return;
      this.sendAll(d, ws);
      return;
    }
    // ほかの人 → ホスト（操作）
    if (d.t === "in") {
      d.s = me.slot;
      const host = this.sockets().find((s) => s.deserializeAttachment().slot === 0);
      if (host) { try { host.send(JSON.stringify(d)); } catch (e) {} }
      return;
    }
    // ホストだけ：足りない分をCPUにして今すぐ始める
    if (d.t === "forcestart" && me.slot === 0) {
      await this.start();
      return;
    }
    if (d.t === "ping") {
      try { ws.send(JSON.stringify({ t: "pong" })); } catch (e) {}
    }
  }

  async webSocketClose(ws) { await this.left(ws); }
  async webSocketError(ws) { await this.left(ws); }

  async left(ws) {
    const me = ws.deserializeAttachment();
    if (!me || me.gone) return;
    ws.serializeAttachment({ ...me, gone: true });
    try { ws.close(1000, "bye"); } catch (e) {}

    if (me.slot === 0) {
      // ホストが抜けたら部屋を閉じる
      this.sendAll({ t: "closed", msg: "部屋を作った人が抜けたので、部屋を閉じました" }, ws);
      for (const s of this.sockets(ws)) { try { s.close(1000, "closed"); } catch (e) {} }
      const meta = await this.state.storage.get("meta");
      if (meta) { meta.status = "closed"; await this.state.storage.put("meta", meta); }
      await this.report(ws, true);
      return;
    }
    this.sendAll({ t: "left", s: me.slot, members: this.members(ws) }, ws);
    await this.report(ws);
  }

  async report(except, closed) {
    const meta = await this.state.storage.get("meta");
    if (!meta) return;
    const count = this.members(except).length;
    const lobby = this.env.LOBBY.get(this.env.LOBBY.idFromName("main"));
    await lobby.fetch("https://lobby/update", {
      method: "POST",
      body: JSON.stringify({
        id: meta.id, name: meta.name, count,
        status: closed || count === 0 ? "closed" : meta.status,
      }),
    });
  }
}

// ===== 部屋の一覧（ロビー） =====
export class Lobby {
  constructor(state) {
    this.state = state;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const rooms = (await this.state.storage.get("rooms")) || {};
    const now = Date.now();

    if (url.pathname === "/update") {
      const d = await req.json();
      if (d.status === "closed" || d.count === 0) delete rooms[d.id];
      else rooms[d.id] = { id: d.id, name: d.name, count: d.count, status: d.status, updated: now };
      // 古い部屋を片付ける
      for (const k of Object.keys(rooms)) if (now - rooms[k].updated > 3 * 3600 * 1000) delete rooms[k];
      await this.state.storage.put("rooms", rooms);
      return new Response("ok");
    }

    if (url.pathname === "/list") {
      const list = Object.values(rooms)
        .filter((r) => now - r.updated < 3 * 3600 * 1000)
        .sort((a, b) => (a.status === b.status ? b.updated - a.updated : a.status === "waiting" ? -1 : 1))
        .slice(0, 30)
        .map((r) => ({ id: r.id, name: r.name, count: r.count, status: r.status }));
      return json({ rooms: list });
    }
    return new Response("not found", { status: 404 });
  }
}

// ===== 小道具 =====
function reject(msg) {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  server.accept();
  server.send(JSON.stringify({ t: "error", msg }));
  server.close(4000, "rejected");
  return new Response(null, { status: 101, webSocket: client });
}

function clean(s, max) {
  return String(s || "").replace(/[\u0000-\u001f<>"\\]/g, "").trim().slice(0, max);
}

function randomId() {
  const c = "abcdefghijkmnpqrstuvwxyz23456789";
  const a = new Uint8Array(8);
  crypto.getRandomValues(a);
  return Array.from(a, (x) => c[x % c.length]).join("");
}

async function sha(s) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("roku:" + s));
  return Array.from(new Uint8Array(d), (b) => b.toString(16).padStart(2, "0")).join("");
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function cors(res) {
  const h = new Headers(res.headers);
  h.set("Access-Control-Allow-Origin", "*");
  h.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  h.set("Access-Control-Allow-Headers", "Content-Type");
  return new Response(res.body, { status: res.status, headers: h, webSocket: res.webSocket });
}
