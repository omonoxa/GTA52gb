// RtcLinks: the WebRTC data-channel transport of the peer-to-peer overlay (wasm/MULTIPLAYER_PLAN.md, contract C5). Classic script: runs on the page (the real thing), in a worker
// (the proxy) and in Node (module.exports, for tests; pass opts.RTCPeerConnection there).
//
// WHERE RTCPeerConnection LIVES (decided by reading, not assumed): Chrome does not expose RTCPeerConnection in dedicated workers (W3C webrtc-pc issue #3131 asks for it, nothing
// shipped). The hub that decides who to link to (net_worker.js) is a worker, so the page's main thread owns every RTCPeerConnection and talks to the hub over one MessageChannel,
// binary messages as transferables:
//     hub (net_worker.js) --RtcLinks.create({port})--> RtcLinks.Proxy --MessagePort--> RtcLinks.serve(port) on the page --> RtcLinks (RTCPeerConnection)
// The Proxy has the same methods as RtcLinks. If a future Chrome does expose RTCPeerConnection in workers, RtcLinks.create() returns the real class there and the page bridge stays
// idle. The page does nothing but move bytes (its main thread is otherwise idle: the engine runs in workers), so no timing logic lives there that background-tab timer throttling
// could hurt: the hub (a worker, unthrottled) drives RtcLinks.tick() once a second (pings, liveness, timeouts).
//
// Per peer: one RTCPeerConnection with two PRE-NEGOTIATED data channels (id 0 "u": unordered, maxRetransmits 0 = datagrams; id 1 "r": reliable, ordered). Pre-negotiated, so both
// sides create both channels and glare can never leave two channel pairs behind. Perfect negotiation: either side may call connect(); the HIGHER pid is polite (rolls back its own
// offer), so the LOWER pid's offer wins a glare. Trickle ICE. iceServers default [] = host candidates only (LAN / same machine).
//   NOTE for local tests: Chrome hides host addresses behind mDNS names (x.local); two tabs on one machine normally resolve them, headless runs may not. If links never come up,
//   start Chrome with --disable-features=WebRtcHideLocalIpsWithMdns.
//
// Envelope on every link (all integers big endian):  [u8 kind][u8 ttl][u24 src][u24 dst][payload]
//   kind 1 engine datagram, 2 ghost batch, 3 gossip (JSON), 4 signalling relayed for a peer (JSON of the {type,sdp|candidate} object), 5 ping/pong/bye (link local, never relayed)
//   RtcLinks consumes kind 5 (RTT: u8 0 ping | 1 pong, u32 id, f64 t; u8 2 = bye, this link only; u8 3 = bye, leaving: the tab closes) and kind 4 addressed to this peer. Everything else addressed to this peer (dst == pid or 0 =
//   "the neighbour") goes to onMessage(fromPid, envelope). A message with dst != pid is relayed: ttl is decremented and it is sent to dst when a direct link exists (or to
//   opts.route(dst) when given); when it cannot be forwarded here it is handed up to onMessage unchanged (the hub knows the overlay's routes) and dropped when ttl is 0.
//   RtcLinks.pack / RtcLinks.parse build and read the envelope; callers always send the full envelope: send(pid, envelope, reliable).
//
// Signalling: opts.signaling = { send(toPid, obj), onmessage(fromPid, obj) } (RtcLinks assigns onmessage). obj = {type:'offer'|'answer', sdp} | {type:'ice', candidate} | {type:'reject'}.
// When a direct link to the target is open the object travels over it as kind 4 (reliable channel) instead of through signaling.send.
//
// Link life cycle callbacks: onLinkUp(pid) when both channels are open; onLinkDown(pid) when an up link ends for any reason (closed by either side, silence, ICE failure, our own
// disconnect); onLinkFailed(pid) when a link never came up (timeout, rejected, ICE failed before open). A failed or down link is forgotten: the caller decides whether to connect again.
// An up link that goes 'disconnected' first gets ICE restarts (the data channels survive them) before it is declared down.
(function (root) {
'use strict';

const K = { DATAGRAM: 1, GHOST: 2, GOSSIP: 3, SIGNAL: 4, PING: 5 };
const BYE = 'the other side left';		// the reason onLinkDown gets when the other side said it is leaving (bye 3: close(), its tab closed); a link it only dropped says bye 2
const HDR = 8;
const MAX_BUFFERED = 256 * 1024;		// an unordered channel with this much queued drops new datagrams (they would only arrive late)
const MAX_BUFFERED_R = 1024 * 1024;		// an ordered channel with this much queued is stalled (a dead path the consent check has not caught yet): send() refuses instead of growing the queue without bound (SCALE_RESEARCH R7)
const te = new TextEncoder(), td = new TextDecoder();
const clock = () => (typeof performance !== 'undefined' ? performance.now() : Date.now());
const noop = () => {};

function pack(kind, ttl, src, dst, payload) {
	const n = payload ? payload.length : 0;
	const u8 = new Uint8Array(HDR + n);
	u8[0] = kind; u8[1] = ttl;
	u8[2] = src >>> 16; u8[3] = src >>> 8; u8[4] = src;
	u8[5] = dst >>> 16; u8[6] = dst >>> 8; u8[7] = dst;
	if (n) u8.set(payload, HDR);
	return u8;
}
function parse(u8) {
	if (!u8 || u8.length < HDR) return null;
	return { kind: u8[0], ttl: u8[1], src: (u8[2] << 16) | (u8[3] << 8) | u8[4], dst: (u8[5] << 16) | (u8[6] << 8) | u8[7] };
}
// ---- compact offers: a published offer is ~900 bytes of SDP, of which only the ICE user/password, the fingerprint and the candidates differ from offer to offer. The rest is a fixed template, so an
// offer travels as "ufrag|pwd|fingerprint|cand;cand;..." (~250 bytes) and the joiner rebuilds the SDP. An SDP that does not match the template is not compacted (compactOffer -> null): the
// full text travels instead, so a browser with a different template costs bandwidth, never correctness.
const OFFER_REST = ['v=0', 's=-', 't=0 0', 'a=group:BUNDLE 0', 'a=extmap-allow-mixed', 'a=msid-semantic: WMS', 'm=application 9 UDP/DTLS/SCTP webrtc-datachannel', 'c=IN IP4 0.0.0.0', 'a=ice-options:trickle', 'a=setup:actpass', 'a=mid:0', 'a=sctp-port:5000', 'a=max-message-size:262144'];
let offerMiss = null;			// the first non-matching SDP (diagnostics: how to fix the template)
function compactOffer(sdp) {
	const rest = [], cands = [];
	let u = '', pw = '', fp = '';
	for (const l of sdp.split(/\r?\n/)) {
		if (!l) continue;
		if (l.startsWith('a=ice-ufrag:')) u = l.slice(12);
		else if (l.startsWith('a=ice-pwd:')) pw = l.slice(10);
		else if (l.startsWith('a=fingerprint:sha-256 ')) fp = l.slice(22);
		else if (l.startsWith('a=candidate:')) {
			const t = l.slice(12).split(' ');			// foundation component protocol priority ip port typ type [raddr R rport P] ...
			if (t.length < 8 || t[1] !== '1' || t[6] !== 'typ' || (t[2] !== 'udp' && t[2] !== 'UDP')) continue;		// a tcp or odd candidate: dropped (the udp ones are what a data channel uses)
			const ra = t.indexOf('raddr'), rp = t.indexOf('rport');
			cands.push([t[0], t[3], t[4], t[5], t[7]].concat(ra > 0 && rp > 0 ? [t[ra + 1], t[rp + 1]] : []).join(','));
		}
		else if (l === 'a=end-of-candidates' || l.startsWith('o=')) continue;
		else rest.push(l);
	}
	if (!u || !pw || !fp || rest.join('\n') !== OFFER_REST.join('\n') || /[|;]/.test(u + pw)) { if (offerMiss === null) offerMiss = rest.join(' / '); return null; }
	return u + '|' + pw + '|' + fp + '|' + cands.join(';');
}
function expandOffer(c) {
	const a = c.split('|');
	if (a.length < 4) return null;
	const lines = ['v=0', 'o=- ' + Math.floor(Math.random() * 1e15) + ' 2 IN IP4 127.0.0.1'].concat(OFFER_REST.slice(1, 8), ['a=ice-ufrag:' + a[0], 'a=ice-pwd:' + a[1], 'a=ice-options:trickle', 'a=fingerprint:sha-256 ' + a[2], 'a=setup:actpass', 'a=mid:0', 'a=sctp-port:5000', 'a=max-message-size:262144']);
	if (a[3]) for (const e of a[3].split(';')) {
		const f = e.split(',');
		lines.push('a=candidate:' + f[0] + ' 1 udp ' + f[1] + ' ' + f[2] + ' ' + f[3] + ' typ ' + f[4] + (f.length >= 7 ? ' raddr ' + f[5] + ' rport ' + f[6] : '') + ' generation 0');
	}
	return lines.join('\r\n') + '\r\n';
}
const fingerprint = (sdp) => { const m = /a=fingerprint:\S+ (\S+)/.exec(sdp || ''); return m ? m[1] : ''; };

class RtcLinks {
	constructor(o) {
		this.pid = o.pid >>> 0;
		this.signaling = o.signaling || null;
		this.iceServers = o.iceServers || [];
		this.cbMessage = o.onMessage || noop;
		this.cbUp = o.onLinkUp || noop;
		this.cbDown = o.onLinkDown || noop;
		this.cbFailed = o.onLinkFailed || noop;
		this.cbStall = o.onLinkStall || noop;						// (pid, stalled): see stallMs
		this.route = o.route || null;
		this.accept = o.accept || null;							// accept(pid) -> bool: may an unknown peer connect to us (default: yes, up to maxLinks)
		this.maxLinks = o.maxLinks || 64;
		this.connectTimeoutMs = o.connectTimeoutMs || 20000;		// 8 s was tried (T: 120 bots): 10x the failed attempts (1,100-1,600 vs 70-150 per run) and no more links; real ICE usually needs 1-3 s but a loaded or slow machine more
		this.pingMs = o.pingMs === undefined ? 2000 : o.pingMs;
		this.deadMs = o.deadMs || 30000;						// an up link that has delivered nothing for this long is dead (pings keep a quiet link alive). 30 s, was 12: a page busy loading or in a background tab can pause its pings that long while the connection itself is fine (the browser's own ICE checks find a dead one: 'disconnected', then restarts) (2026-11-03, always linked)
		this.graceMs = o.graceMs || 4000;						// 'disconnected' for this long: ICE restart
		// An open link that has delivered nothing for stallMs (it gets a ping every pingMs) or whose connection is 'disconnected' is STALLED: still up (its ICE restart may
		// bring it back), but the hub routes around it (ok(pid), net_worker.js sendEnv) instead of sending into it until it is given up (2026-11-07: a player froze that long).
		this.stallMs = o.stallMs || 5000;
		this.maxRestarts = o.maxRestarts === undefined ? 4 : o.maxRestarts;		// 4, was 2: a restart keeps the link (and the player) where a new link would start over
		this.RTC = o.RTCPeerConnection || root.RTCPeerConnection;
		this.log = o.log || noop;
		this.peers = new Map();
		this.prepared = new Map();								// offer id -> peer: offers made ahead of time (greeters) that wait for an answer
		this.early = new Map();									// ICE candidates that arrived before their offer: pid -> {t, list}
		this.totals = { bytesIn: 0, bytesOut: 0, msgsIn: 0, msgsOut: 0, forwarded: 0, droppedBusy: 0, droppedReliable: 0, droppedTtl: 0, up: 0, down: 0, failed: 0 };
		this.closed = false;
		if (!this.RTC) throw new Error('RtcLinks: no RTCPeerConnection in this context (use RtcLinks.create with a port to the page)');
		if (this.signaling) this.signaling.onmessage = (from, obj) => this._onSignal(from >>> 0, obj);
		const auto = o.autoTick === undefined ? 1000 : o.autoTick;
		this._timer = auto > 0 ? setInterval(() => this.tick(), auto) : null;
	}

	// ---- public API (contract C5) ---------------------------------------------------------------------------------------------------------------------
	connect(pid) {
		pid >>>= 0;
		if (this.closed || pid === this.pid || pid === 0 || pid > 0xFFFFFF || this.peers.has(pid)) return false;
		if (this.peers.size >= this.maxLinks) { this.totals.failed++; Promise.resolve().then(() => this.cbFailed(pid)); return false; }
		this._newPeer(pid, true);
		return true;
	}
	disconnect(pid) {
		const p = this.peers.get(pid >>> 0);
		if (p) this._drop(p, 'disconnect', true);
	}
	send(pid, u8, reliable) {
		const p = this.peers.get(pid >>> 0);
		if (!p || p.state !== 'open') return false;
		const ch = reliable ? p.r : p.u;
		if (ch.readyState !== 'open') return false;
		if (!reliable && ch.bufferedAmount > MAX_BUFFERED) { this.totals.droppedBusy++; return false; }
		if (reliable && ch.bufferedAmount > MAX_BUFFERED_R) { this.totals.droppedReliable++; return false; }
		try { ch.send(u8); } catch (e) { return false; }
		p.bytesOut += u8.length; p.msgsOut++; this.totals.bytesOut += u8.length; this.totals.msgsOut++;
		return true;
	}
	has(pid) { const p = this.peers.get(pid >>> 0); return !!p && p.state === 'open'; }
	ok(pid) { const p = this.peers.get(pid >>> 0); return !!p && p.state === 'open' && !p.stalled; }		// up and delivering (stallMs)
	list() { const r = []; for (const p of this.peers.values()) if (p.state === 'open') r.push(p.pid); return r; }
	stats() {
		const t = clock(), peers = {};
		let open = 0, pending = 0;
		for (const p of this.peers.values()) {
			if (p.state === 'open') open++; else pending++;
			peers[p.pid] = { state: p.state, upMs: p.upAt ? Math.round(t - p.upAt) : 0, rtt: +p.rtt.toFixed(1), rttMin: +(p.rttMin === Infinity ? 0 : p.rttMin).toFixed(1), bytesIn: p.bytesIn, bytesOut: p.bytesOut, msgsIn: p.msgsIn, msgsOut: p.msgsOut, forwarded: p.forwarded, restarts: p.restarts };
		}
		const kinds = { host: 0, srflx: 0, prflx: 0, relay: 0, unknown: 0, v6: 0 };
		for (const p of this.peers.values()) {
			if (p.state !== 'open') continue;
			const k = p.kind ? p.kind.split('/') : null;
			if (!k) { kinds.unknown++; continue; }
			const worst = (k[0] === 'relay' || k[1] === 'relay') ? 'relay' : (k[0] === 'srflx' || k[1] === 'srflx') ? 'srflx' : (k[0] === 'prflx' || k[1] === 'prflx') ? 'prflx' : (k[0] === 'host' && k[1] === 'host') ? 'host' : 'unknown';
			kinds[worst]++;
			if (p.v6) kinds.v6++;
		}
		return { pid: this.pid, links: open, pending, prepared: this.prepared.size, kinds, peers, totals: Object.assign({}, this.totals) };
	}
	// Which candidate types the selected path uses (diagnostics): resolves to {local, remote, protocol} or null.
	async details(pid) {
		const p = this.peers.get(pid >>> 0);
		if (!p) return null;
		const rep = await p.pc.getStats();
		let pair = null;
		rep.forEach((v) => { if (v.type === 'transport' && v.selectedCandidatePairId) pair = rep.get(v.selectedCandidatePairId); });
		if (!pair) rep.forEach((v) => { if (v.type === 'candidate-pair' && v.nominated && v.state === 'succeeded') pair = v; });
		if (!pair) return null;
		const l = rep.get(pair.localCandidateId), r = rep.get(pair.remoteCandidateId);
		return { local: l && l.candidateType, remote: r && r.candidateType, protocol: l && l.protocol, v6: !!((l && l.address && l.address.indexOf(':') >= 0) || (r && r.address && r.address.indexOf(':') >= 0)) };
	}
	ping(pid) { const p = this.peers.get(pid >>> 0); if (p && p.state === 'open') this._ping(p); }
	// Housekeeping, once a second: pings, silent links, connect timeouts, ICE restarts. The caller drives it (autoTick or the hub through the proxy).
	tick() {
		if (this.closed) return;
		const t = clock();
		for (const p of Array.from(this.peers.values())) {
			if (p.closing) continue;
			if (p.state === 'connecting') {
				if (t - p.startedAt > this.connectTimeoutMs) this._drop(p, 'connect timeout', false);
				continue;
			}
			if (this.pingMs > 0 && t - p.lastPing >= this.pingMs) this._ping(p);
			if (t - p.lastRecv > this.deadMs) { this._drop(p, 'silent for ' + Math.round((t - p.lastRecv) / 1000) + ' s', false); continue; }
			const stalled = !!p.disconnectedAt || t - p.lastRecv > this.stallMs;
			if (stalled !== !!p.stalled) { p.stalled = stalled; this.log('[rtc] link to ' + p.pid + (stalled ? ' stalled' : ' recovered')); this.cbStall(p.pid, stalled); }
			if (p.disconnectedAt && t - p.disconnectedAt >= this.graceMs) {
				if (p.restarts >= this.maxRestarts) this._drop(p, 'ICE did not recover', false);
				else this._restart(p);
			}
		}
		for (const [pid, e] of this.early) if (t - e.t > 30000) this.early.delete(pid);
		for (const [id, p] of this.prepared) if (t - p.startedAt > 150000) { this.prepared.delete(id); p.closing = true; this._closePc(p); }		// nobody took it
	}
	close() {
		this.closed = true;
		clearInterval(this._timer);
		for (const p of Array.from(this.peers.values())) this._drop(p, 'close', true);
		this.early.clear();
	}

	// ---- peers ------------------------------------------------------------------------------------------------------------------------------------------
	_newPeer(pid, initiator, opts) {
		const o2 = opts || {};		// noTrickle: every ICE candidate is inside the SDP (nothing to signal later); prepared: an offer made ahead of time for a peer not known yet (pid 0)
		const pc = new this.RTC({ iceServers: this.iceServers });
		const t = clock();
		const p = {
			pid, pc, u: null, r: null, state: 'connecting', polite: this.pid > pid, mayOffer: initiator && !o2.prepared, noTrickle: !!o2.noTrickle, makingOffer: false, ignoreOffer: false,
			queue: [], remoteSet: false, remoteFp: '', chain: Promise.resolve(), startedAt: t, upAt: 0, lastRecv: t, lastPing: 0, pingId: 0,
			rtt: 0, rttMin: Infinity, bytesIn: 0, bytesOut: 0, msgsIn: 0, msgsOut: 0, forwarded: 0, restarts: 0, disconnectedAt: 0, closing: false, wasUp: false,
		};
		const early = this.early.get(pid);
		if (early) { p.queue = early.list; this.early.delete(pid); }
		if (!o2.prepared) this.peers.set(pid, p);
		p.u = pc.createDataChannel('u', { negotiated: true, id: 0, ordered: false, maxRetransmits: 0 });
		p.r = pc.createDataChannel('r', { negotiated: true, id: 1, ordered: true });
		for (const ch of [p.u, p.r]) {
			ch.binaryType = 'arraybuffer';
			ch.onopen = () => this._check(p);
			ch.onclose = () => { if (!p.closing && this.peers.get(p.pid) === p) this._drop(p, 'channel ' + ch.label + ' closed', false); };
			ch.onmessage = (ev) => this._recv(p, ev.data);
		}
		pc.onicecandidate = (ev) => {
			if (ev.candidate && !p.closing && !p.noTrickle) this._signal(p.pid, { type: 'ice', candidate: ev.candidate.toJSON ? ev.candidate.toJSON() : ev.candidate });
		};
		pc.onnegotiationneeded = async () => {
			if (!p.mayOffer || p.closing) return;
			try {
				p.makingOffer = true;
				await pc.setLocalDescription();
				if (!p.closing) this._signal(p.pid, { type: 'offer', sdp: pc.localDescription.sdp });
			} catch (err) { this.log('[rtc] offer to ' + pid + ' failed: ' + err); } finally { p.makingOffer = false; }
		};
		pc.onconnectionstatechange = () => this._pcState(p);
		return p;
	}
	_check(p) {
		if (p.closing || p.state === 'open' || p.u.readyState !== 'open' || p.r.readyState !== 'open') return;
		p.state = 'open'; p.wasUp = true; p.upAt = clock(); p.lastRecv = p.upAt; p.disconnectedAt = 0;
		this.totals.up++;
		this._ping(p);
		this.cbUp(p.pid);
		this._kind(p);
	}
	// how the link was made (host = same network, srflx / prflx = hole punched through the routers, relay = a TURN server; there is none configured, so relay stays 0): a count the first real players
	// will tell us the truth with. Async, diagnostics only.
	_kind(p) {
		this.details(p.pid).then((d) => { if (d && this.peers.get(p.pid) === p) { p.kind = (d.local || '?') + '/' + (d.remote || '?'); p.v6 = !!d.v6; } }, () => {});
	}
	_pcState(p) {
		if (p.closing || this.peers.get(p.pid) !== p) return;
		const s = p.pc.connectionState;
		if (s === 'connected') { p.disconnectedAt = 0; p.restarts = 0; }		// recovered: the next blip gets its restarts again (they counted over the link's whole life)
		else if (s === 'disconnected') { if (!p.disconnectedAt) p.disconnectedAt = clock(); }
		else if (s === 'failed') {
			if (p.wasUp && p.restarts < this.maxRestarts) this._restart(p);
			else this._drop(p, 'ICE failed', false);
		}
		else if (s === 'closed') this._drop(p, 'closed', false);
	}
	_restart(p) {
		p.restarts++; p.disconnectedAt = clock(); p.mayOffer = true;
		this.log('[rtc] ICE restart ' + p.restarts + ' for ' + p.pid);
		try { p.pc.restartIce(); } catch (e) { this._drop(p, 'restartIce failed: ' + e, false); }
	}
	// Forget the peer. bye: tell the other side first (reliable channel) and close a moment later so the message gets out.
	_drop(p, why, bye) {
		if (p.closing) return;
		p.closing = true;
		if (this.peers.get(p.pid) === p) this.peers.delete(p.pid);
		if (bye && p.state === 'open' && p.r.readyState === 'open') { try { p.r.send(pack(K.PING, 0, this.pid, p.pid, Uint8Array.of(why === 'close' ? 3 : 2))); } catch (e) { /* closing anyway */ } setTimeout(() => this._closePc(p), 150); }
		else this._closePc(p);
		if (p.wasUp) { this.totals.down++; this.log('[rtc] link to ' + p.pid + ' down: ' + why); this.cbDown(p.pid, why); }		// why: BYE when the other side closed (its tab closed: it left), else the network
		else if (p.pid && !(bye && why === 'disconnect') && why !== 'close') { this.totals.failed++; this.log('[rtc] link to ' + p.pid + ' failed: ' + why); this.cbFailed(p.pid, why); }
	}
	_closePc(p) {
		for (const ch of [p.u, p.r]) { if (ch) { ch.onopen = ch.onclose = ch.onmessage = null; try { ch.close(); } catch (e) { /* already closed */ } } }
		p.pc.onicecandidate = p.pc.onnegotiationneeded = p.pc.onconnectionstatechange = null;
		try { p.pc.close(); } catch (e) { /* already closed */ }
	}

	// ---- messages ---------------------------------------------------------------------------------------------------------------------------------------
	_recv(p, data) {
		if (!(data instanceof ArrayBuffer) || data.byteLength < HDR) return;
		const u8 = new Uint8Array(data);
		p.lastRecv = clock(); p.bytesIn += u8.length; p.msgsIn++; this.totals.bytesIn += u8.length; this.totals.msgsIn++;
		const kind = u8[0], ttl = u8[1];
		const src = (u8[2] << 16) | (u8[3] << 8) | u8[4], dst = (u8[5] << 16) | (u8[6] << 8) | u8[7];
		if (kind === K.PING) { this._onPing(p, u8); return; }
		if (dst !== 0 && dst !== this.pid) { this._relay(p, u8, kind, ttl, dst); return; }
		if (kind === K.SIGNAL) {
			let obj = null;
			try { obj = JSON.parse(td.decode(u8.subarray(HDR))); } catch (e) { return; }
			this._onSignal(src, obj);
			return;
		}
		this.cbMessage(p.pid, u8);
	}
	_relay(p, u8, kind, ttl, dst) {
		if (ttl === 0) { this.totals.droppedTtl++; return; }
		let next = this.has(dst) ? dst : (this.route ? this.route(dst) : null);
		if (next !== null && next !== undefined && next !== p.pid && next !== this.pid) {
			const c = u8.slice();
			c[1] = ttl - 1;
			if (this.send(next, c, kind === K.SIGNAL)) { p.forwarded++; this.totals.forwarded++; return; }
		}
		this.cbMessage(p.pid, u8);		// no route here: the owner of the overlay may have one
	}
	_ping(p) {
		const b = new Uint8Array(13), dv = new DataView(b.buffer);
		p.lastPing = clock();
		dv.setUint8(0, 0); dv.setUint32(1, ++p.pingId >>> 0); dv.setFloat64(5, p.lastPing);
		this.send(p.pid, pack(K.PING, 0, this.pid, p.pid, b), false);
	}
	_onPing(p, u8) {
		const type = u8[HDR];
		if (type === 2) { this._drop(p, 'closed by the other side', false); return; }
		if (type === 3) { this._drop(p, BYE, false); return; }
		if (u8.length < HDR + 13) return;
		if (type === 0) {
			const b = u8.slice(HDR);
			b[0] = 1;
			this.send(p.pid, pack(K.PING, 0, this.pid, p.pid, b), false);
		} else if (type === 1) {
			const t = new DataView(u8.buffer, u8.byteOffset + HDR, 13).getFloat64(5);
			const rtt = clock() - t;
			if (rtt >= 0 && rtt < 60000) { p.rtt = p.rtt ? p.rtt * 0.8 + rtt * 0.2 : rtt; if (rtt < p.rttMin) p.rttMin = rtt; }
		}
	}

	// ---- offer pool: a handshake in two requests ---------------------------------------------------------------------------------------------------------
	// A greeter prepares offers ahead of time (prepareOffer: complete SDP, candidates included, published on the rendezvous). A joiner takes one (answerOffer: its complete answer)
	// and posts the answer; the greeter applies it (completeOffer) and the two connect directly: no trickle, no polling by the joiner.
	_gathered(pc, ms) {
		return new Promise((ok) => {
			if (pc.iceGatheringState === 'complete') { ok(); return; }
			const t = setTimeout(done, ms || 3000);
			function done() { clearTimeout(t); pc.removeEventListener('icegatheringstatechange', chk); ok(); }
			function chk() { if (pc.iceGatheringState === 'complete') done(); }
			pc.addEventListener('icegatheringstatechange', chk);
		});
	}
	async prepareOffer() {
		if (this.closed) return null;
		const p = this._newPeer(0, false, { noTrickle: true, prepared: true });
		try {
			await p.pc.setLocalDescription(await p.pc.createOffer());
			await this._gathered(p.pc);
		} catch (err) { p.closing = true; this._closePc(p); this.log('[rtc] prepareOffer failed: ' + err); return null; }
		const id = Math.random().toString(36).slice(2, 10) + (this.prepared.size).toString(36);
		this.prepared.set(id, p);
		this.log('[rtc] prepared offer ' + id);
		const sdp = p.pc.localDescription.sdp, c = compactOffer(sdp);
		return c ? { id, c } : { id, sdp };
	}
	async completeOffer(id, pid, answerSdp) {
		const p = this.prepared.get(id);
		if (!p) return false;
		this.prepared.delete(id);
		pid >>>= 0;
		// Glare: two peers without links (both greeters) answered each other's offers at the same time, so each holds a half-open answer to the other.
		// Both sides keep the connection offered by the lower pid: the lower pid drops its answer and takes this one, the higher pid refuses this one below.
		const old = this.peers.get(pid);
		if (old && old.entryAnswer && old.state !== 'open' && this.pid < pid) { old.closing = true; this._closePc(old); this.peers.delete(pid); }
		if (this.closed || !pid || pid === this.pid || this.peers.has(pid) || this.peers.size >= this.maxLinks) { p.closing = true; this._closePc(p); return false; }
		p.pid = pid; p.polite = this.pid > pid; p.startedAt = clock();
		this.peers.set(pid, p);
		try { await p.pc.setRemoteDescription({ type: 'answer', sdp: answerSdp }); p.remoteSet = true; p.remoteFp = fingerprint(answerSdp); } catch (err) { this._drop(p, 'answer rejected: ' + err, false); return false; }
		return true;
	}
	async answerOffer(pid, offer) {
		pid >>>= 0;
		const offerSdp = typeof offer === 'string' && offer.indexOf('v=0') === 0 ? offer : expandOffer(offer);
		if (!offerSdp) return null;
		if (this.closed || !pid || pid === this.pid || this.peers.has(pid) || this.peers.size >= this.maxLinks) return null;
		const p = this._newPeer(pid, false, { noTrickle: true });
		p.entryAnswer = true;		// see the glare rule in completeOffer
		try {
			await p.pc.setRemoteDescription({ type: 'offer', sdp: offerSdp });
			p.remoteSet = true; p.remoteFp = fingerprint(offerSdp);
			await p.pc.setLocalDescription(await p.pc.createAnswer());
			await this._gathered(p.pc);
		} catch (err) { this._drop(p, 'answer failed: ' + err, false); return null; }
		if (p.closing) return null;		// dropped while gathering (glare: the other side's offer won)
		return p.pc.localDescription.sdp;
	}

	// ---- signalling -------------------------------------------------------------------------------------------------------------------------------------
	_signal(to, obj) {
		if (this.has(to)) { this.send(to, pack(K.SIGNAL, 3, this.pid, to, te.encode(JSON.stringify(obj))), true); return; }
		if (this.signaling && this.signaling.send) this.signaling.send(to, obj);
	}
	_onSignal(from, obj) {
		if (this.closed || !obj || from === this.pid || from === 0) return;
		let p = this.peers.get(from);
		const type = obj.type;
		if (type === 'reject') { if (p && p.state !== 'open') this._drop(p, 'rejected', false); return; }
		if (type === 'offer') {
			const fp = fingerprint(obj.sdp);
			if (p && p.remoteFp && fp && fp !== p.remoteFp) { this._drop(p, 'remote restarted', false); p = null; }		// same pid, fresh peer connection on the other side
			if (!p) {
				if (this.peers.size >= this.maxLinks || (this.accept && !this.accept(from))) { this._signal(from, { type: 'reject' }); return; }
				p = this._newPeer(from, false);
			}
		} else if (type === 'ice') {
			if (!p) {
				if (!obj.candidate) return;
				let e = this.early.get(from);
				if (!e) { e = { t: clock(), list: [] }; this.early.set(from, e); }
				if (e.list.length < 64) e.list.push(obj.candidate);
				return;
			}
		} else if (type !== 'answer' || !p) return;
		const q = p;
		q.chain = q.chain.then(() => this._apply(q, obj)).catch((err) => { if (!q.closing) this.log('[rtc] signal from ' + from + ' (' + type + '): ' + err); });
	}
	async _apply(p, obj) {
		if (p.closing) return;
		const pc = p.pc;
		if (obj.type === 'offer') {
			const collision = p.makingOffer || pc.signalingState !== 'stable';
			p.ignoreOffer = !p.polite && collision;
			if (p.ignoreOffer) return;						// we are the lower pid: our own offer wins
			await pc.setRemoteDescription({ type: 'offer', sdp: obj.sdp });		// the polite side rolls its own offer back implicitly
			p.remoteSet = true; p.remoteFp = fingerprint(obj.sdp);
			await this._flush(p);
			await pc.setLocalDescription();
			this._signal(p.pid, { type: 'answer', sdp: pc.localDescription.sdp });
		} else if (obj.type === 'answer') {
			if (pc.signalingState !== 'have-local-offer') return;		// stale
			await pc.setRemoteDescription({ type: 'answer', sdp: obj.sdp });
			p.remoteSet = true; p.remoteFp = fingerprint(obj.sdp);
			await this._flush(p);
		} else if (obj.type === 'ice') {
			if (p.ignoreOffer || !obj.candidate) return;
			if (!p.remoteSet) { if (p.queue.length < 64) p.queue.push(obj.candidate); return; }
			await pc.addIceCandidate(obj.candidate);
		}
	}
	async _flush(p) {
		const list = p.queue;
		p.queue = [];
		for (const c of list) { try { await p.pc.addIceCandidate(c); } catch (e) { /* a candidate of an ignored or older negotiation */ } }
	}
}

// ---- hub side: the same API over a MessagePort to RtcLinks.serve() on the page ----------------------------------------------------------------------------
class RtcLinksProxy {
	constructor(o) {
		this.pid = o.pid >>> 0;
		this.port = o.port;
		this.signaling = o.signaling || { send: noop, onmessage: null };
		this.cbMessage = o.onMessage || noop;
		this.cbUp = o.onLinkUp || noop;
		this.cbDown = o.onLinkDown || noop;
		this.cbFailed = o.onLinkFailed || noop;
		this.cbNat = o.onNat || noop;
		this.onApp = null;			// the page's app messages (text and voice chat, chat.js / voice.js): set by the hub
		this.upSet = new Set();
		this.stallSet = new Set();		// up but stalled (RtcLinks stallMs): ok() is false for these
		this.lastStats = null;
		this._rid = 0; this._rpcs = new Map();
		this.signaling.onmessage = (from, obj) => this.port.postMessage({ t: 'sigin', from, obj });
		this.port.onmessage = (ev) => this._on(ev.data);
	}
	_on(m) {
		if (!m) return;
		switch (m.t) {
			case 'msg': this.cbMessage(m.from, new Uint8Array(m.buf)); break;
			case 'up': this.upSet.add(m.pid); this.cbUp(m.pid); break;
			case 'down': this.upSet.delete(m.pid); this.stallSet.delete(m.pid); this.cbDown(m.pid, m.why); break;
			case 'stall': if (m.on) this.stallSet.add(m.pid); else this.stallSet.delete(m.pid); break;
			case 'failed': this.cbFailed(m.pid, m.why); break;
			case 'nat': this.cbNat(m); break;
			case 'sigout': this.signaling.send(m.to, m.obj); break;
			case 'stats': this.lastStats = m.s; break;
			case 'rpc': { const f = this._rpcs.get(m.rid); if (f) { this._rpcs.delete(m.rid); f(m.v); } break; }
			case 'app': if (this.onApp) this.onApp(m.obj); break;
		}
	}
	_rpc(m) {		// a call to the page's RtcLinks that resolves with its value (null after 10 s)
		const rid = ++this._rid;
		return new Promise((ok) => {
			this._rpcs.set(rid, ok);
			setTimeout(() => { if (this._rpcs.delete(rid)) ok(null); }, 10000);
			this.port.postMessage(Object.assign({ rid }, m));
		});
	}
	prepareOffer() { return this._rpc({ t: 'prep' }); }
	completeOffer(id, pid, sdp) { return this._rpc({ t: 'complete', id, pid: pid >>> 0, sdp }); }
	answerOffer(pid, sdp) { return this._rpc({ t: 'answer', pid: pid >>> 0, sdp }); }
	connect(pid) { this.port.postMessage({ t: 'connect', pid: pid >>> 0 }); return true; }
	disconnect(pid) { this.port.postMessage({ t: 'disconnect', pid: pid >>> 0 }); }
	// The envelope's buffer is TRANSFERRED to the page (the caller's array is detached) unless the view covers only part of it, in which case it is copied.
	send(pid, u8, reliable) {
		pid >>>= 0;
		if (!this.upSet.has(pid)) return false;
		let buf = u8.buffer;
		if (u8.byteOffset !== 0 || u8.byteLength !== buf.byteLength || (typeof SharedArrayBuffer !== 'undefined' && buf instanceof SharedArrayBuffer)) buf = u8.slice().buffer;
		this.port.postMessage({ t: 'send', pid, reliable: !!reliable, buf }, [buf]);
		return true;
	}
	has(pid) { return this.upSet.has(pid >>> 0); }
	ok(pid) { pid >>>= 0; return this.upSet.has(pid) && !this.stallSet.has(pid); }
	list() { return Array.from(this.upSet); }
	ping(pid) { this.port.postMessage({ t: 'ping', pid: pid >>> 0 }); }
	tick() { this.port.postMessage({ t: 'tick' }); }			// the page runs RtcLinks.tick() and answers with its stats
	// The page's stats of the last tick (RTT, bytes per link); links = what is up right now.
	stats() {
		const s = this.lastStats ? Object.assign({}, this.lastStats) : { pid: this.pid, links: 0, pending: 0, peers: {}, totals: {} };
		s.links = this.upSet.size;
		return s;
	}
	hubInfo(info) { this.port.postMessage({ t: 'hub', info }); }	// diagnostics for the page (window.__rtc, see index.html)
	app(obj, transfer) { this.port.postMessage({ t: 'app', obj }, transfer || []); }		// hub -> page app message (text and voice chat)
	close() { this.port.postMessage({ t: 'close' }); this.upSet.clear(); }
}

// ---- page side: owns the RTCPeerConnections, serves one hub through the port --------------------------------------------------------------------------------
// opts: { pid, iceServers, maxLinks, connectTimeoutMs, onHub(info), onApp(obj), log }. Returns the RtcLinks, with toHub(obj, transfer) for the page's app messages (text and voice chat).
function serve(port, opts) {
	const o = opts || {};
	const signaling = { send: (to, obj) => port.postMessage({ t: 'sigout', to, obj }), onmessage: null };
	const links = new RtcLinks({
		pid: o.pid, signaling, iceServers: o.iceServers, maxLinks: o.maxLinks, connectTimeoutMs: o.connectTimeoutMs, autoTick: 0, pingMs: o.pingMs, log: o.log,
		RTCPeerConnection: o.RTCPeerConnection,
		onMessage: (from, u8) => {
			const buf = (u8.byteOffset === 0 && u8.byteLength === u8.buffer.byteLength) ? u8.buffer : u8.slice().buffer;
			port.postMessage({ t: 'msg', from, buf }, [buf]);
		},
		onLinkUp: (pid) => port.postMessage({ t: 'up', pid }),
		onLinkDown: (pid, why) => port.postMessage({ t: 'down', pid, why }),
		onLinkFailed: (pid, why) => port.postMessage({ t: 'failed', pid, why }),
		onLinkStall: (pid, on) => port.postMessage({ t: 'stall', pid, on }),
	});
	// router type and IPv6, once per session, told to the hub (nat.js): needs two STUN servers on one socket
	const NP = root.NatProbe, RTCc = o.RTCPeerConnection || root.RTCPeerConnection;
	const stun = [].concat.apply([], (o.iceServers || []).map((s) => s && s.urls)).filter((u) => typeof u === 'string' && /^stuns?:/.test(u));
	if (NP && RTCc && o.natProbe !== false && stun.length >= 2) {
		NP.probe(RTCc, stun.slice(0, 2), { ms: 4000 }).then((r) => port.postMessage({ t: 'nat', nat: r.nat, srflx: r.srflx, ms: r.ms, v6: false }), () => {});
	}
	port.onmessage = (ev) => {
		const m = ev.data;
		if (!m) return;
		switch (m.t) {
			case 'connect': links.connect(m.pid); break;
			case 'disconnect': links.disconnect(m.pid); break;
			case 'send': links.send(m.pid, new Uint8Array(m.buf), m.reliable); break;
			case 'ping': links.ping(m.pid); break;
			case 'tick': links.tick(); port.postMessage({ t: 'stats', s: links.stats() }); break;
			case 'prep': links.prepareOffer().then((v) => port.postMessage({ t: 'rpc', rid: m.rid, v }), () => port.postMessage({ t: 'rpc', rid: m.rid, v: null })); break;
			case 'complete': links.completeOffer(m.id, m.pid, m.sdp).then((v) => port.postMessage({ t: 'rpc', rid: m.rid, v }), () => port.postMessage({ t: 'rpc', rid: m.rid, v: false })); break;
			case 'answer': links.answerOffer(m.pid, m.sdp).then((v) => port.postMessage({ t: 'rpc', rid: m.rid, v }), () => port.postMessage({ t: 'rpc', rid: m.rid, v: null })); break;
			case 'sigin': if (signaling.onmessage) signaling.onmessage(m.from, m.obj); break;
			case 'hub': if (o.onHub) o.onHub(m.info); break;
			case 'app': if (o.onApp) o.onApp(m.obj); break;
			case 'close': links.close(); break;
		}
	};
	links.toHub = (obj, transfer) => port.postMessage({ t: 'app', obj }, transfer || []);
	return links;
}

// In a worker without RTCPeerConnection (every Chrome today) the proxy over `port`; where RTCPeerConnection exists, the real class.
// ---- links through the relay instead of WebRTC (MULTIPLAYER_PLAN step 4: Node drones have no RTCPeerConnection; also a fallback for pairs that cannot connect) ----
// opts.relayLinks = the relay's base URL (ws://127.0.0.1:8791/). One WebSocket to net_relay.mjs /mesh?pid=N carries every link of this hub:
//   text {t:'link', to} -> the relay hands {t:'link', from} to `to`, which accepts with {t:'linkok', to}; both sides then report the link up. {t:'unlink', to} ends it,
//   the relay sends {t:'gone', pid} when a hub's socket closes and {t:'nolink', pid} when `to` is not there.
//   binary [u32 to][envelope] -> the relay delivers [u32 from][envelope].
// Same interface as RtcLinks / RtcLinksProxy; the rendezvous offer calls have nothing to do (the relay is the meeting point).
class RelayLinks {
	constructor(o) {
		this.pid = o.pid >>> 0;
		this.cbMessage = o.onMessage || noop;
		this.cbUp = o.onLinkUp || noop;
		this.cbDown = o.onLinkDown || noop;
		this.cbFailed = o.onLinkFailed || noop;
		this.log = o.log || noop;
		this.upSet = new Set();
		this.pending = new Set();
		this.out = [];		// text sent before the socket opened
		this.bytes = { in: 0, out: 0 };
		const url = o.relayLinks.replace(/\/?$/, '/') + 'mesh?pid=' + this.pid;
		this.ws = new WebSocket(url);
		this.ws.binaryType = 'arraybuffer';
		this.ws.onopen = () => { for (const t of this.out) this.ws.send(t); this.out = []; };
		this.ws.onclose = () => { for (const p of Array.from(this.upSet)) this._down(p); };
		this.ws.onmessage = (ev) => {
			if (typeof ev.data === 'string') { let m = null; try { m = JSON.parse(ev.data); } catch (e) { return; } this._on(m); return; }
			const u8 = new Uint8Array(ev.data);
			if (u8.length < 4) return;
			const from = ((u8[0] << 24) | (u8[1] << 16) | (u8[2] << 8) | u8[3]) >>> 0;
			this.bytes.in += u8.length;
			if (this.upSet.has(from)) this.cbMessage(from, u8.subarray(4));
		};
	}
	_text(obj) { const t = JSON.stringify(obj); if (this.ws.readyState === 1) this.ws.send(t); else this.out.push(t); }
	_up(p) { this.pending.delete(p); if (!this.upSet.has(p)) { this.upSet.add(p); this.cbUp(p); } }
	_down(p) { this.pending.delete(p); if (this.upSet.delete(p)) this.cbDown(p); }
	_on(m) {
		const p = (m.from ?? m.pid) >>> 0;
		switch (m.t) {
			case 'link': this._text({ t: 'linkok', to: p }); this._up(p); break;		// always accepted: the overlay above decides how many links to keep
			case 'linkok': this._up(p); break;
			case 'unlink': case 'gone': this._down(p); break;
			case 'nolink': if (this.pending.delete(p)) this.cbFailed(p, 'absent'); break;
		}
	}
	connect(pid) { pid >>>= 0; if (pid === this.pid || this.upSet.has(pid)) return true; this.pending.add(pid); this._text({ t: 'link', to: pid }); return true; }
	disconnect(pid) { pid >>>= 0; this._text({ t: 'unlink', to: pid }); this._down(pid); }
	send(pid, u8) {
		pid >>>= 0;
		if (!this.upSet.has(pid) || this.ws.readyState !== 1) return false;
		const b = new Uint8Array(4 + u8.length);
		b[0] = pid >>> 24; b[1] = (pid >>> 16) & 255; b[2] = (pid >>> 8) & 255; b[3] = pid & 255;
		b.set(u8, 4);
		this.ws.send(b);
		this.bytes.out += b.length;
		return true;
	}
	has(pid) { return this.upSet.has(pid >>> 0); }
	ok(pid) { return this.has(pid); }
	list() { return Array.from(this.upSet); }
	prepareOffer() { return Promise.resolve(null); }
	completeOffer() { return Promise.resolve(false); }
	answerOffer() { return Promise.resolve(null); }
	ping() {}
	tick() {}
	stats() { return { pid: this.pid, links: this.upSet.size, pending: this.pending.size, peers: {}, totals: { bytesIn: this.bytes.in, bytesOut: this.bytes.out } }; }
	hubInfo() {}
	app() {}
	close() { try { this.ws.close(); } catch (e) { /* closed */ } this.upSet.clear(); }
}

function create(opts) {
	if (opts.relayLinks) return new RelayLinks(opts);
	if (root.RTCPeerConnection || opts.RTCPeerConnection) return new RtcLinks(opts);
	if (!opts.port) throw new Error('RtcLinks.create: this context has no RTCPeerConnection and no MessagePort to the page was given');
	return new RtcLinksProxy(opts);
}

RtcLinks.KIND = K;
RtcLinks.HEADER = HDR;
RtcLinks.pack = pack;
RtcLinks.compactOffer = compactOffer;
RtcLinks.expandOffer = expandOffer;
RtcLinks.offerMiss = () => offerMiss;
RtcLinks.parse = parse;
RtcLinks.Proxy = RtcLinksProxy;
RtcLinks.Relay = RelayLinks;
RtcLinks.serve = serve;
RtcLinks.create = create;
RtcLinks.BYE = BYE;
root.RtcLinks = RtcLinks;
if (typeof module !== 'undefined' && module.exports) module.exports = { RtcLinks };
})(typeof globalThis !== 'undefined' ? globalThis : (typeof self !== 'undefined' ? self : this));
