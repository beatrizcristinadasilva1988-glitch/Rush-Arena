'use strict';
/* Arena Rush — servidor online (retransmissor de salas) v3
   - serve o index.html e o WebSocket em /ws na mesma porta
   - aceita QUALQUER modo de Boss (Morte, Zeus, Loki e os futuros): sem lista fixa
   - snapshots e controles são repassados SEM reprocessar (menos CPU = menos lag)
   - TCP_NODELAY ligado e compressão desligada (compressão só atrasa jogo em tempo real)
   - responde ping/pong para o jogo mostrar o ping e para o servidor não dormir no meio da partida */
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'friends.json');
const INDEX = path.join(__dirname, 'index.html');

/* ---------- amigos (salvo em arquivo; no Render grátis o disco some ao reiniciar) ---------- */
let FR = { names: {}, friends: {}, inc: {}, out: {} };
try { FR = Object.assign(FR, JSON.parse(fs.readFileSync(DATA, 'utf8'))); } catch (e) {}
let saveT = null;
const saveFr = () => { clearTimeout(saveT); saveT = setTimeout(() => { try { fs.writeFileSync(DATA, JSON.stringify(FR)); } catch (e) {} }, 2000); };
const has = (o, k, v) => !!(o[k] && o[k].includes(v));
const add = (o, k, v) => { const a = o[k] || (o[k] = []); if (!a.includes(v)) a.push(v); };
const del = (o, k, v) => { if (o[k]) o[k] = o[k].filter(x => x !== v); };

const K = n => String(n == null ? '' : n).trim().toLowerCase().slice(0, 12);
const cleanName = n => String(n == null ? '' : n).replace(/[<>&"'`]/g, '').trim().slice(0, 12) || 'Jogador';
const int = (v, a, b, d) => { v = Math.trunc(+v); return Number.isFinite(v) ? Math.max(a, Math.min(b, v)) : d; };
const cleanProf = p => { p = p && typeof p === 'object' ? p : {}; return { ci: int(p.ci, 0, 999, 0), hr: int(p.hr, 0, 999, 0), ou: int(p.ou, 0, 999, 0), wk: int(p.wk, 0, 999, 0), hd: int(p.hd, 0, 999, 0), g: int(p.g, 0, 1, 0) }; };

/* ---------- conexões ---------- */
const C = new Map(), ON = new Map(), R = new Map();
let nid = 0;
const tx = (c, o) => { if (c && c.ws.readyState === 1) { try { c.ws.send(typeof o === 'string' ? o : JSON.stringify(o)); } catch (e) {} } };

function pushFl(k) {
  const c = ON.get(k); if (!c) return;
  const nm = x => FR.names[x] || x;
  tx(c, { t: 'fl',
    friends: (FR.friends[k] || []).map(x => ({ n: nm(x), k: x, on: ON.has(x) })),
    inc: (FR.inc[k] || []).map(x => ({ n: nm(x), k: x })),
    out: (FR.out[k] || []).map(nm) });
}
const notifyFriends = k => { for (const f of (FR.friends[k] || [])) pushFl(f); };
function befriend(a, b) {
  del(FR.inc, a, b); del(FR.out, a, b); del(FR.inc, b, a); del(FR.out, b, a);
  add(FR.friends, a, b); add(FR.friends, b, a); saveFr(); pushFl(a); pushFl(b);
  tx(ON.get(a), { t: 'msg', m: (FR.names[b] || b) + ' agora é seu amigo!' });
  tx(ON.get(b), { t: 'msg', m: (FR.names[a] || a) + ' agora é seu amigo!' });
}

/* ---------- salas ---------- */
function newCode() { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; for (;;) { let s = ''; for (let i = 0; i < 4; i++) s += A[Math.random() * A.length | 0]; if (!R.has(s)) return s; } }
const maxPl = r => r.coop ? r.z : r.z * 2;
function assignTeams(r) { r.pl.forEach((id, i) => { const c = C.get(id); if (c) c.team = r.coop ? 0 : i % 2; }); }
function view(r) {
  return { code: r.code, host: r.host, mode: r.mode, mapI: r.mapI, coop: r.coop, z: r.z, dif: r.dif, live: r.live,
    players: r.pl.map(id => { const c = C.get(id) || {}; return { id, name: c.name, team: c.team | 0, prof: c.prof }; }) };
}
function bcast(r, o) { const s = JSON.stringify(o); for (const id of r.pl) tx(C.get(id), s); }
function leaveRoom(c) {
  const r = c.room; if (!r) return; c.room = null;
  if (r.host === c.id) {
    for (const id of r.pl) { if (id === c.id) continue; const o = C.get(id); if (o) { o.room = null; tx(o, { t: 'closed' }); } }
    R.delete(r.code); return;
  }
  r.pl = r.pl.filter(id => id !== c.id);
  if (r.live) tx(C.get(r.host), { t: 'left', id: c.id });
  else { assignTeams(r); bcast(r, { t: 'room', ...view(r) }); }
}
function dropClient(c) {
  if (c.gone) return; c.gone = true;
  leaveRoom(c); C.delete(c.id);
  if (c.k && ON.get(c.k) === c) { ON.delete(c.k); notifyFriends(c.k); }
}
function reg(c, name, prof) {
  const nm = cleanName(name), k = K(nm);
  if (c.k === k) { if (prof) c.prof = cleanProf(prof); return; }
  if (c.k && ON.get(c.k) === c) ON.delete(c.k);
  const old = ON.get(k);
  if (old && old !== c) { tx(old, { t: 'dup' }); dropClient(old); try { old.ws.close(); } catch (e) {} }
  c.name = nm; c.k = k; c.prof = cleanProf(prof); ON.set(k, c);
  FR.names[k] = nm; saveFr();
  pushFl(k); notifyFriends(k);
}

/* ---------- mensagens ---------- */
function onMsg(c, m) {
  switch (m.t) {
    case 'hello': reg(c, m.name, m.prof); tx(c, { t: 'you', id: c.id }); pushFl(c.k); break;
    case 'ping': tx(c, '{"t":"pong","c":' + (+m.c || 0) + '}'); break;
    case 'create': {
      reg(c, m.name || c.name, m.prof); leaveRoom(c);
      const r = { code: newCode(), host: c.id, pl: [c.id], mode: 'm3', mapI: 0, coop: false, z: 3, dif: 1, live: false };
      R.set(r.code, r); c.room = r; assignTeams(r); tx(c, { t: 'room', ...view(r) }); break;
    }
    case 'join': {
      reg(c, m.name || c.name, m.prof);
      const r = R.get(String(m.code || '').toUpperCase().trim());
      if (!r) return tx(c, { t: 'err', m: 'Sala não encontrada' });
      if (c.room === r) return tx(c, { t: 'room', ...view(r) });
      if (r.live) return tx(c, { t: 'err', m: 'A partida já começou' });
      if (r.pl.length >= maxPl(r)) return tx(c, { t: 'err', m: 'Sala cheia' });
      leaveRoom(c); r.pl.push(c.id); c.room = r; assignTeams(r); bcast(r, { t: 'room', ...view(r) }); break;
    }
    case 'cfg': {
      const r = c.room; if (!r || r.host !== c.id || r.live) return;
      const mode = /^\w{1,12}$/.test(m.mode) ? m.mode : r.mode;
      const coop = !!m.coop, z = int(m.z, 1, coop ? 4 : 5, 3), mapI = int(m.mapI, 0, 40, 0), dif = int(m.dif == null ? 1 : m.dif, 0, 3, 1);
      if (r.pl.length > (coop ? z : z * 2)) { tx(c, { t: 'err', m: 'Tem jogadores demais na sala para esse modo' }); return tx(c, { t: 'room', ...view(r) }); }
      Object.assign(r, { mode, coop, z, mapI, dif }); assignTeams(r); bcast(r, { t: 'room', ...view(r) }); break;
    }
    case 'prof': { c.prof = cleanProf(m.prof); const r = c.room; if (r && !r.live) bcast(r, { t: 'room', ...view(r) }); break; }
    case 'start': {
      const r = c.room; if (!r || r.host !== c.id || r.live) return;
      r.live = true; assignTeams(r); bcast(r, { t: 'start', ...view(r) }); break;
    }
    case 'over': { const r = c.room; if (!r || r.host !== c.id || !r.live) return; r.live = false; assignTeams(r); bcast(r, { t: 'over', ...view(r) }); break; }
    case 'leave': leaveRoom(c); break;
    /* amigos */
    case 'freq': {
      const to = K(m.to);
      if (!to || to === c.k) return tx(c, { t: 'msg', m: 'Nick inválido' });
      if (!FR.names[to]) return tx(c, { t: 'msg', m: 'Jogador não encontrado. Ele precisa ter entrado no online ao menos uma vez.' });
      if (has(FR.friends, c.k, to)) return tx(c, { t: 'msg', m: 'Vocês já são amigos' });
      if (has(FR.inc, c.k, to)) return befriend(c.k, to);
      add(FR.out, c.k, to); add(FR.inc, to, c.k); saveFr(); pushFl(c.k); pushFl(to);
      tx(c, { t: 'msg', m: 'Pedido enviado para ' + FR.names[to] });
      tx(ON.get(to), { t: 'msg', m: c.name + ' quer ser seu amigo' }); break;
    }
    case 'facc': { const f = K(m.from); if (has(FR.inc, c.k, f)) befriend(c.k, f); break; }
    case 'fdec': { const f = K(m.from); del(FR.inc, c.k, f); del(FR.out, f, c.k); saveFr(); pushFl(c.k); pushFl(f); break; }
    case 'fdel': { const f = K(m.nick); del(FR.friends, c.k, f); del(FR.friends, f, c.k); saveFr(); pushFl(c.k); pushFl(f); break; }
    case 'finv': {
      const to = K(m.to), t = ON.get(to);
      if (!c.room || !t || !has(FR.friends, c.k, to)) return;
      tx(t, { t: 'invite', from: c.name, code: c.room.code, mode: c.room.mode }); break;
    }
  }
}

/* ---------- http + websocket ---------- */
const srv = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (u === '/' || u === '/index.html') {
    return fs.readFile(INDEX, (e, b) => {
      if (e) { res.writeHead(404); return res.end('index.html não encontrado'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(b);
    });
  }
  res.writeHead(404); res.end('404');
});
const wss = new WebSocketServer({ server: srv, path: '/ws', perMessageDeflate: false, maxPayload: 1 << 20 });
wss.on('connection', (ws, req) => {
  try { req.socket.setNoDelay(true); } catch (e) {}
  const c = { id: ++nid, ws, name: 'Jogador', k: '', prof: cleanProf(), room: null, team: 0, alive: true, win: 0, cnt: 0 };
  C.set(c.id, c);
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', (data, isBin) => {
    if (isBin) return; c.alive = true;
    const now = Date.now(); if (now - c.win > 1000) { c.win = now; c.cnt = 0; } if (++c.cnt > 400) return;
    const raw = data.toString();
    try {
      /* caminho rápido: sem JSON.parse */
      if (raw.startsWith('{"t":"snap"')) {
        const r = c.room; if (!r || !r.live || r.host !== c.id) return;
        for (const id of r.pl) if (id !== c.id) tx(C.get(id), raw); return;
      }
      if (raw.startsWith('{"t":"in","d":')) {
        const r = c.room; if (!r || !r.live || r.host === c.id) return;
        tx(C.get(r.host), '{"t":"in","id":' + c.id + ',"d":' + raw.slice(14)); return;
      }
      const m = JSON.parse(raw); if (m && typeof m === 'object') onMsg(c, m);
    } catch (e) {}
  });
  ws.on('close', () => dropClient(c));
  ws.on('error', () => {});
});
setInterval(() => { for (const c of C.values()) { if (!c.alive) { try { c.ws.terminate(); } catch (e) {} continue; } c.alive = false; try { c.ws.ping(); } catch (e) {} } }, 25000);
srv.listen(PORT, '0.0.0.0', () => console.log('Arena Rush online na porta ' + PORT));
