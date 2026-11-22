// Servidor do Arena Rush — sem dependências (só precisa do Node 18+)
// Serve o jogo (pasta public) e faz as salas online por WebSocket em /ws
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const PUB = fs.existsSync(path.join(__dirname, 'public')) ? path.join(__dirname, 'public') : __dirname; // aceita index.html na pasta public ou ao lado do server.js
const BLOQ = ['server.js', 'package.json', 'friends.json'];
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.json': 'application/json', '.png': 'image/png', '.mp3': 'audio/mpeg', '.ico': 'image/x-icon' };

const server = http.createServer((req, res) => {
  let u = decodeURIComponent((req.url || '/').split('?')[0]);
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  if (u === '/') u = '/index.html';
  const f = path.normalize(path.join(PUB, u));
  if (!f.startsWith(PUB) || BLOQ.includes(path.basename(f))) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end('não encontrado'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(d);
  });
});

// ---------- WebSocket mínimo (RFC 6455) ----------
function frame(op, payload) {
  const b = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const n = b.length;
  const h = n < 126 ? Buffer.from([0x80 | op, n]) : n < 65536 ? Buffer.from([0x80 | op, 126, n >> 8, n & 255]) :
    (() => { const x = Buffer.alloc(10); x[0] = 0x80 | op; x[1] = 127; x.writeBigUInt64BE(BigInt(n), 2); return x })();
  return Buffer.concat([h, b]);
}
class Sock {
  constructor(s) { this.s = s; this.buf = Buffer.alloc(0); this.frag = []; this.alive = true; this.onmsg = null; this.onclose = null;
    s.on('data', d => { this.buf = Buffer.concat([this.buf, d]); this.parse() });
    s.on('close', () => this.close()); s.on('error', () => this.close()); }
  parse() {
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = b[0] & 0x80, op = b[0] & 15, masked = b[1] & 0x80; let len = b[1] & 127, o = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); o = 4 }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); o = 10 }
      if (len > 1e6) return this.close();
      const need = o + (masked ? 4 : 0) + len; if (b.length < need) return;
      let p = b.subarray(o + (masked ? 4 : 0), need);
      if (masked) { const k = b.subarray(o, o + 4); p = Buffer.from(p); for (let i = 0; i < p.length; i++) p[i] ^= k[i & 3] }
      this.buf = b.subarray(need);
      if (op === 8) return this.close();
      if (op === 9) { this.raw(frame(10, p)); continue }
      if (op === 10) continue;
      this.frag.push(p);
      if (fin) { const m = Buffer.concat(this.frag).toString(); this.frag = []; if (this.onmsg) this.onmsg(m) }
    }
  }
  raw(x) { if (this.alive) try { this.s.write(x) } catch (e) {} }
  send(o) { this.raw(frame(1, typeof o === 'string' ? o : JSON.stringify(o))) }
  ping() { this.raw(frame(9, '')) }
  close() { if (!this.alive) return; this.alive = false; try { this.s.destroy() } catch (e) {} if (this.onclose) this.onclose() }
}
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.url || '').split('?')[0] !== '/ws') return socket.destroy();
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  socket.setNoDelay(true);
  conn(new Sock(socket));
});

// ---------- salas ----------
const rooms = new Map(); let nid = 1;
const code = () => { const A = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; let c; do { c = ''; for (let i = 0; i < 4; i++) c += A[Math.random() * A.length | 0] } while (rooms.has(c)); return c };
const pub = r => ({ t: 'room', code: r.code, host: r.host, mode: r.mode, mapI: r.mapI, z: r.z, state: r.state, coop: !!r.coop, dif: r.dif == null ? 1 : r.dif, players: r.players.map(p => ({ id: p.id, name: p.name, prof: p.prof, team: p.team })) });
const bcast = (r, o, except) => { const s = JSON.stringify(o); for (const p of r.players) if (p.id !== except) p.sock.send(s) };
const cap = r => r.coop ? r.z : r.z * 2;
function assignTeams(r) { r.players.forEach((p, i) => p.team = r.coop ? 0 : i % 2) }

// ---------- amigos (salvos em friends.json; no Render grátis pode zerar ao reiniciar) ----------
const DBF = path.join(__dirname, 'friends.json');
let DB = { friends: {}, pend: {}, names: {} };
try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(DBF, 'utf8'))) } catch (e) {}
const saveDB = () => { try { fs.writeFileSync(DBF, JSON.stringify(DB)) } catch (e) {} };
const nk = s => String(s || '').trim().slice(0, 12).toLowerCase();
const fr = n => DB.friends[n] || (DB.friends[n] = []);
const pd = n => DB.pend[n] || (DB.pend[n] = []);
const byNick = new Map();
const flFor = u => ({ t: 'fl',
  friends: fr(u.nick).map(k => ({ k, n: DB.names[k] || k, on: byNick.has(k) })),
  inc: pd(u.nick).map(k => ({ k, n: DB.names[k] || k })),
  out: Object.keys(DB.pend).filter(t => DB.pend[t].includes(u.nick)).map(t => DB.names[t] || t) });
const pushFl = k => { const u = byNick.get(k); if (u) u.sock.send(flFor(u)) };
const notifyFriends = u => { for (const k of fr(u.nick)) pushFl(k) };
function ident(me) {
  const k = nk(me.name); if (!k) return;
  if (k !== me.nick) {
    if (me.nick && byNick.get(me.nick) === me) { byNick.delete(me.nick); notifyFriends(me) }
    const old = byNick.get(k); if (old && old !== me) old.sock.close();
    me.nick = k; byNick.set(k, me);
  }
  DB.names[k] = me.name; saveDB(); me.sock.send(flFor(me)); notifyFriends(me);
}
function acceptFriend(me, f) {
  DB.pend[me.nick] = pd(me.nick).filter(x => x !== f); DB.pend[f] = pd(f).filter(x => x !== me.nick);
  if (!fr(me.nick).includes(f)) fr(me.nick).push(f); if (!fr(f).includes(me.nick)) fr(f).push(me.nick);
  saveDB(); pushFl(me.nick); pushFl(f); const o = byNick.get(f); if (o) o.sock.send({ t: 'msg', m: me.name + ' aceitou seu pedido!' });
}

function conn(sock) {
  const me = { id: nid++, sock, name: 'Jogador', nick: '', prof: null, room: null, team: 0 };
  const timer = setInterval(() => sock.ping(), 25000);
  const leave = () => {
    const r = me.room; if (!r) return; me.room = null;
    if (r.host === me.id) { bcast(r, { t: 'closed' }, me.id); rooms.delete(r.code); for (const p of r.players) p.room = null; return }
    r.players = r.players.filter(p => p.id !== me.id); assignTeams(r);
    const h = r.players.find(p => p.id === r.host); if (h) h.sock.send({ t: 'left', id: me.id });
    bcast(r, pub(r));
  };
  sock.onclose = () => { clearInterval(timer); leave(); if (me.nick && byNick.get(me.nick) === me) { byNick.delete(me.nick); notifyFriends(me) } };
  sock.onmsg = raw => {
    let m; try { m = JSON.parse(raw) } catch (e) { return }
    const r = me.room, isHost = r && r.host === me.id;
    switch (m.t) {
      case 'create': {
        if (r) leave();
        me.name = String(m.name || 'Jogador').slice(0, 12); me.prof = m.prof || null; ident(me);
        const c = code(), room = { code: c, host: me.id, mode: 'm3', mapI: 0, z: 3, state: 'lobby', players: [me] };
        me.room = room; assignTeams(room); rooms.set(c, room);
        sock.send({ t: 'you', id: me.id }); sock.send(pub(room)); break }
      case 'join': {
        if (r) leave();
        const room = rooms.get(String(m.code || '').toUpperCase().trim());
        if (!room) return sock.send({ t: 'err', m: 'Sala não encontrada' });
        if (room.state !== 'lobby') return sock.send({ t: 'err', m: 'A partida já começou' });
        if (room.players.length >= cap(room)) return sock.send({ t: 'err', m: 'Sala cheia' });
        me.name = String(m.name || 'Jogador').slice(0, 12); me.prof = m.prof || null; ident(me);
        room.players.push(me); me.room = room; assignTeams(room);
        sock.send({ t: 'you', id: me.id }); bcast(room, pub(room)); break }
      case 'prof': if (r) { me.prof = m.prof; if (m.name) me.name = String(m.name).slice(0, 12); bcast(r, pub(r)) } break;
      case 'cfg': if (isHost && r.state === 'lobby') {
        if (!/^[a-z0-9]{1,10}$/.test(String(m.mode))) return;
        r.mode = m.mode; r.coop = !!m.coop; r.z = Math.max(1, Math.min(r.coop ? 4 : 3, m.z | 0 || 3)); r.mapI = Math.max(0, Math.min(20, m.mapI | 0)); r.dif = Math.max(0, Math.min(3, m.dif == null ? 1 : m.dif | 0));
        while (r.players.length > cap(r)) { const p = r.players.pop(); p.room = null; p.sock.send({ t: 'err', m: 'Você foi removido (sala menor)' }); p.sock.send({ t: 'closed' }) }
        assignTeams(r); bcast(r, pub(r)) } break;
      case 'start': if (isHost && r.state === 'lobby') {
        assignTeams(r); r.state = 'play'; bcast(r, Object.assign(pub(r), { t: 'start' })) } break;
      case 'in': if (r && !isHost) { const h = r.players.find(p => p.id === r.host); if (h) h.sock.send({ t: 'in', id: me.id, d: m.d }) } break;
      case 'snap': if (isHost) bcast(r, { t: 'snap', d: m.d }, me.id); break;
      case 'over': if (isHost) { r.state = 'lobby'; bcast(r, Object.assign(pub(r), { t: 'over' })) } break;
      case 'leave': leave(); break;
      case 'hello': me.name = String(m.name || 'Jogador').slice(0, 12); if (m.prof) me.prof = m.prof; ident(me); sock.send({ t: 'you', id: me.id }); break;
      // ----- amigos -----
      case 'freq': { if (!me.nick) return; const t = nk(m.to);
        if (!t || t === me.nick) return sock.send({ t: 'msg', m: 'Nick inválido' });
        if (fr(me.nick).includes(t)) return sock.send({ t: 'msg', m: 'Vocês já são amigos' });
        if (pd(me.nick).includes(t)) { acceptFriend(me, t); break }
        if (!pd(t).includes(me.nick)) pd(t).push(me.nick); saveDB();
        sock.send({ t: 'msg', m: 'Pedido enviado para ' + (DB.names[t] || t) }); pushFl(me.nick);
        const o = byNick.get(t); if (o) { o.sock.send({ t: 'msg', m: me.name + ' quer ser seu amigo!' }); pushFl(t) } break }
      case 'facc': { const f = nk(m.from); if (me.nick && pd(me.nick).includes(f)) acceptFriend(me, f); break }
      case 'fdec': { const f = nk(m.from); DB.pend[me.nick] = pd(me.nick).filter(x => x !== f); saveDB(); pushFl(me.nick); pushFl(f); break }
      case 'fdel': { const f = nk(m.nick); DB.friends[me.nick] = fr(me.nick).filter(x => x !== f); DB.friends[f] = fr(f).filter(x => x !== me.nick); saveDB(); pushFl(me.nick); pushFl(f); break }
      case 'finv': { const t = nk(m.to);
        if (!r || r.state !== 'lobby') return sock.send({ t: 'msg', m: 'Crie uma sala primeiro' });
        if (!fr(me.nick).includes(t)) return sock.send({ t: 'msg', m: 'Essa pessoa não é sua amiga' });
        const o = byNick.get(t); if (!o) return sock.send({ t: 'msg', m: 'Seu amigo está offline' });
        o.sock.send({ t: 'invite', from: me.name, code: r.code, mode: r.mode }); sock.send({ t: 'msg', m: 'Convite enviado para ' + o.name }); break }
    }
  };
}
server.listen(PORT, () => console.log('Arena Rush online na porta ' + PORT));
