// Servidor do Arena Rush — sem dependências (só precisa do Node 18+)
// Serve o jogo (pasta public) e faz as salas online por WebSocket em /ws
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3000;
const PUB = fs.existsSync(path.join(__dirname, 'public')) ? path.join(__dirname, 'public') : __dirname; // aceita index.html na pasta public ou ao lado do server.js
const BLOQ = ['server.js', 'package.json'];
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
const pub = r => ({ t: 'room', code: r.code, host: r.host, mode: r.mode, mapI: r.mapI, z: r.z, state: r.state, players: r.players.map(p => ({ id: p.id, name: p.name, prof: p.prof, team: p.team })) });
const bcast = (r, o, except) => { const s = JSON.stringify(o); for (const p of r.players) if (p.id !== except) p.sock.send(s) };
function assignTeams(r) { r.players.forEach((p, i) => p.team = i % 2) }

function conn(sock) {
  const me = { id: nid++, sock, name: 'Jogador', prof: null, room: null, team: 0 };
  const timer = setInterval(() => sock.ping(), 25000);
  const leave = () => {
    const r = me.room; if (!r) return; me.room = null;
    if (r.host === me.id) { bcast(r, { t: 'closed' }, me.id); rooms.delete(r.code); for (const p of r.players) p.room = null; return }
    r.players = r.players.filter(p => p.id !== me.id); assignTeams(r);
    const h = r.players.find(p => p.id === r.host); if (h) h.sock.send({ t: 'left', id: me.id });
    bcast(r, pub(r));
  };
  sock.onclose = () => { clearInterval(timer); leave() };
  sock.onmsg = raw => {
    let m; try { m = JSON.parse(raw) } catch (e) { return }
    const r = me.room, isHost = r && r.host === me.id;
    switch (m.t) {
      case 'create': {
        if (r) leave();
        me.name = String(m.name || 'Jogador').slice(0, 12); me.prof = m.prof || null;
        const c = code(), room = { code: c, host: me.id, mode: 'm3', mapI: 0, z: 3, state: 'lobby', players: [me] };
        me.room = room; assignTeams(room); rooms.set(c, room);
        sock.send({ t: 'you', id: me.id }); sock.send(pub(room)); break }
      case 'join': {
        if (r) leave();
        const room = rooms.get(String(m.code || '').toUpperCase().trim());
        if (!room) return sock.send({ t: 'err', m: 'Sala não encontrada' });
        if (room.state !== 'lobby') return sock.send({ t: 'err', m: 'A partida já começou' });
        if (room.players.length >= room.z * 2) return sock.send({ t: 'err', m: 'Sala cheia' });
        me.name = String(m.name || 'Jogador').slice(0, 12); me.prof = m.prof || null;
        room.players.push(me); me.room = room; assignTeams(room);
        sock.send({ t: 'you', id: me.id }); bcast(room, pub(room)); break }
      case 'prof': if (r) { me.prof = m.prof; if (m.name) me.name = String(m.name).slice(0, 12); bcast(r, pub(r)) } break;
      case 'cfg': if (isHost && r.state === 'lobby') {
        if (!['m1', 'm2', 'm3'].includes(m.mode)) return;
        r.mode = m.mode; r.z = +m.mode[1]; r.mapI = Math.max(0, Math.min(20, m.mapI | 0));
        while (r.players.length > r.z * 2) { const p = r.players.pop(); p.room = null; p.sock.send({ t: 'err', m: 'Você foi removido (sala menor)' }); p.sock.send({ t: 'closed' }) }
        assignTeams(r); bcast(r, pub(r)) } break;
      case 'start': if (isHost && r.state === 'lobby') {
        assignTeams(r); r.state = 'play'; bcast(r, Object.assign(pub(r), { t: 'start' })) } break;
      case 'in': if (r && !isHost) { const h = r.players.find(p => p.id === r.host); if (h) h.sock.send({ t: 'in', id: me.id, d: m.d }) } break;
      case 'snap': if (isHost) bcast(r, { t: 'snap', d: m.d }, me.id); break;
      case 'over': if (isHost) { r.state = 'lobby'; bcast(r, Object.assign(pub(r), { t: 'over' })) } break;
      case 'leave': leave(); break;
    }
  };
}
server.listen(PORT, () => console.log('Arena Rush online na porta ' + PORT));
