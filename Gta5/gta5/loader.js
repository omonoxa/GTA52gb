// Worker that hosts the engine ("main runtime thread" of the Emscripten module). importScripts('game.js') runs the static constructors
// and main() here; the loop blocks this worker, which is fine (nothing else runs on it). game.js also runs in every pthread worker.
'use strict';
let started = false;
// The page's second message, {choice: {args, netUrl, rtc, keepThreads}}: what the player chose (index.html launchEngine). main() waits for it (run dependency 'choice' below): the engine arguments and the
// network mode are start-up switches, and everything slow (the download and compilation of game.wasm, the io worker's prefetch, the GPU worker's pipelines) already runs while the player decides.
let choice = null, applyChoice = null;
self.onmessage = (ev) => {
	if (ev.data && ev.data.choice) { choice = ev.data.choice; if (applyChoice) applyChoice(); return; }
	if (started) return;
	started = true;
	const m = ev.data;
	// ?cores=N (debug): pretend to be a machine with N logical cores; the engine sizes its worker pools from it
	if (m.cores) Object.defineProperty(navigator, 'hardwareConcurrency', { value: m.cores });
	// The GPU worker must be up before the engine blocks this thread: Chrome fetches the script of a worker nested in a worker through
	// the parent's event loop. It idles until the engine sends its start message (wgpu_backend.cpp: wgpu_start_worker).
	const gq = new URLSearchParams();		// the GPU worker reads its options from its own URL
	if (m.slots) gq.set('slots', m.slots);
	if (m.syncPipelines) gq.set('syncpipelines', '1');
	if (m.remoteLog) gq.set('log', '1');		// ?log=1 on the page: its log lines go to the dev server (off by default)
	if (m.lowMemory) gq.set('low', '1');
	if (m.noNanGuard) gq.set('nanguard', '0');
	if (m.jsProf) gq.set('jsprof', m.jsProf);
	if (m.opStats) gq.set('opstats', '1');		// ?opstats=1: time per GPU command type (wgpu_worker.js)		// ?jsprof=N: the GPU worker samples itself for 10 s from N s after the world is shown (wgpu_worker.js jsProfile)		// ?nanguard=0: pixel shaders keep NaN outputs (wgpu_worker.js guardOutputs; diagnostics)		// the low-memory profile: its own pipeline seed (wgpu_worker.js loadRecipes)
	if (m.noPack) gq.set('nopack', '1');		// ?nopack=1: shaders one by one instead of from the packs (diagnostics)
	if (m.shotRt) gq.set('shotrt', '1');		// ?shotrt=1 (with ?shot=N): every render target of one frame after the world is shown goes to the dev server (diagnosis)
	if (m.dbgPs) { gq.set('dbgps', m.dbgPs); if (m.dbgPsN) gq.set('dbgpsn', String(m.dbgPsN)); }		// ?dbgps=<pixel shader name part>: the worker logs those draws (diagnosis)
	if (m.gpuLimits) gq.set('limits', m.gpuLimits);		// ?limits=name:value,...: a smaller device (diagnosis of weaker GPUs)
	const B = m.base || '';		// the page's URL prefix for everything it loads (index.html BASE): /b/<build> on the PHP host, empty otherwise
	const gpu = new Worker(B + '/wgpu_worker.js' + (gq.toString() ? '?' + gq : ''));
	const io = new Worker(B + '/io_worker.js');
	// The net worker too, for the same reason: created now, while this thread's event loop runs, and used only if the player picks a network mode. Created at the choice
	// instead, its script raced main(): on the live site, with the game data downloading, it once missed and never ran (no hub, no players: 2026-10-25).
	const net = new Worker(B + '/net_worker.js');
	let netLoaded = false, netLoadedWait = null;
	net.onmessage = (e) => { if (e.data && e.data.loaded) { netLoaded = true; if (netLoadedWait) netLoadedWait(); } };
	net.onerror = (e) => { const t = '[loader] net worker error: ' + e.message + ' (' + e.filename + ':' + e.lineno + ')'; console.error(t); if (self.__log) { self.__log(t); self.__logFlush(); } };		// __log arrives with game.js
	io.postMessage({ init: true, base: self.location.origin + '/data/', noStore: !!m.noStore, record: !!m.record, trace: !!m.trace, log: !!m.remoteLog, noHints: !!m.noHints, bootset: m.lowMemory ? 'bootset_low.json' : 'bootset.json' });		// starts the prefetch of the boot read set at once		// HTTP reads of all engine threads (platform/file/httpfs_wasm.cpp); same reason to create it up front
	let loaded = 0;
	io.onmessage = (e) => { if (e.data.loaded && ++loaded === 2) start(); };
	gpu.onmessage = (e) => {
		if (e.data.loaded && ++loaded === 2) start();
	};
	// An overloaded host answers 503/500/429 (2026-10-06): retried up to 8 times with growing, jittered waits (~20 s in all) instead of compiling an error page.
	const fetchWasm = async () => {
		for (let attempt = 0; ; attempt++) {
			let res = null, err = null;
			try { res = await fetch(B + '/game.wasm'); } catch (e) { err = e; }
			if (res && res.ok) return res;
			if (res && res.status === 404 && B) { self.postMessage({ staleBuild: 'game.wasm' }); throw new Error('HTTP 404 for game.wasm: this page is of an older build'); }		// index.php build_marker: the page reloads
			if (attempt >= 7) { if (res) throw new Error('HTTP ' + res.status + ' for game.wasm'); throw err; }
			bc0.postMessage({ label: 'The server is busy, retrying (' + (attempt + 1) + ')' });
			await new Promise((r) => setTimeout(r, Math.min(5000, 300 * 2 ** attempt) * (0.5 + Math.random())));
		}
	};
	// To the page directly (its worker.onmessage): this thread runs main() and never returns to its event loop, and a BroadcastChannel here would keep every message (prejs.js postOnce)
	const bc0 = { postMessage: (msg) => self.postMessage({ engineMsg: { ch: 'game-progress', msg } }) };
	// Every engine thread (a worker running game.js, created by emscripten in this thread) gets its own MessagePort to the page before anything else (prejs.js takes it):
	// see prejs.js postOnce for why not a BroadcastChannel.
	const NativeWorker = self.Worker;
	self.Worker = function (url, opts) {
		const w = new NativeWorker(url, opts);
		if (String(url).includes('game.js')) {
			const ch = new MessageChannel();
			self.postMessage({ enginePort: ch.port1 }, [ch.port1]);
			w.postMessage({ wasmPagePort: ch.port2 }, [ch.port2]);
		}
		return w;
	};
	self.Worker.prototype = NativeWorker.prototype;
	// The engine's arguments and the network setup of the choice: arguments in place, the net worker created (and its script loaded: Chrome fetches the script of a worker nested in a worker through this
	// thread's event loop, which main() will block) and given the page's MessagePort of the WebRTC hub.
	const applyNow = async () => {
		const c = choice, M = self.Module;
		M.arguments.length = 0;
		for (const a of c.args) M.arguments.push(a);
		M.netUrl = c.netUrl || '';
		M.netRtc = !!c.rtc;
		M.netPid = c.rtc ? c.rtc.pid : 0;
		M.keepThreads = c.keepThreads | 0;
		// A network mode runs the engine's network threads (ipc_wasm.cpp group "net": up to 16) and, once in a session, more (HasherThread, VoiceChat Worker: 46 threads at 2 cores in
		// a local test session), which the worker pool is not sized for (build_full.py POOL_DEFAULT: 28 at
		// <= 2 cores, the threads of the game without them). A thread that finds no free worker cannot start while main() blocks this thread (a new worker needs this event loop),
		// and the network start waits for its send thread: at 2 cores (?low=1, a Chromebook) every multiplayer start hung there. Add 24 workers now, before main() runs (52 at 2 cores).
		if ((M.keepThreads & 2) && typeof PThread !== 'undefined' && PThread.allocateUnusedWorker) {
			const loads = [];
			for (let i = 0; i < 24; i++) {
				PThread.allocateUnusedWorker();
				loads.push(PThread.loadWasmModuleToWorker(PThread.unusedWorkers[PThread.unusedWorkers.length - 1]));
			}
			await Promise.all(loads);
			self.__log('[loader] network mode: 24 more pthread workers (' + PThread.unusedWorkers.length + ' free)');
		}
		if (c.netUrl) {
			const t0 = Date.now();
			const got = netLoaded || await new Promise((ok) => { const t = setTimeout(() => ok(false), 120000); netLoadedWait = () => { clearTimeout(t); ok(true); }; });
			self.__log('[loader] net worker script ' + (got ? 'loaded' + (Date.now() - t0 > 100 ? ' after ' + (Date.now() - t0) + ' ms more' : '') : 'did not report within 2 minutes') + ', rtc ' + (c.rtc ? 'pid ' + c.rtc.pid : 'none'));
			if (c.rtc) net.postMessage({ rtcInit: Object.assign({}, c.rtc, { url: c.netUrl }) }, [c.rtc.port]);		// the net worker becomes the hub of the WebRTC overlay; the page's MessagePort (RtcLinks.serve) goes with it
			M.netWorker = net;
			await new Promise((ok) => setTimeout(ok, 300));		// main() blocks this thread's event loop for good: let the transferred port and the message reach the net worker first
		} else net.terminate();		// no network mode: the idle net worker goes
		console.log('loader: starting engine: ' + c.args.join(' ') + ' (cores ' + m.cores + ')');		// the line also tells the log that the player has chosen
	};
	const start = () => {
		const bc = bc0;
		self.Module = {
			...(m.rscMB ? { gameResourceMB: m.rscMB } : {}),		// the streaming heap size (index.html rscMB), read by the engine's heap set-up (rage/base/src/system/main.h)
			// Download game.wasm ourselves to report progress (0-20 %); emscripten hands the compiled module on to the pthread workers.
			instantiateWasm: (imports, done) => {
				// Streaming compilation: the module compiles while it downloads (no compile wait after the download, no 2 x 63 MB buffer copy, and Chrome can cache the
				// compiled code of a fetched response for the next visit). Progress is counted on a clone of the response. Any failure falls back to download-then-compile.
				const buffered = async () => {
					const res = await fetchWasm();
					const total = +(res.headers.get('X-Uncompressed-Length') || res.headers.get('Content-Length')) || 0;
					const reader = res.body.getReader(), chunks = [];
					let got = 0;
					for (;;) {
						const { done: end, value } = await reader.read();
						if (end) break;
						chunks.push(value);
						got += value.length;
						bc.postMessage({ pct: total ? 20 * got / total : 0, label: 'Downloading the engine (' + (got / 1048576 | 0) + ' MB)' });
					}
					const bytes = new Uint8Array(got);
					let o = 0;
					for (const c of chunks) { bytes.set(c, o); o += c.length; }
					bc.postMessage({ pct: 20, label: 'Compiling the engine' });
					return WebAssembly.instantiate(bytes, imports);
				};
				// Streaming compilation runs in the io worker (io_worker.js compileWasm), which hands the compiled module back. Not here: Chrome's code cache serializes the
				// compiled code again and again as functions get optimized in play, and those copies belong to the thread that compiled; this thread runs main() and never
				// collects its garbage, so they piled up (renderer native memory 1.7 GB -> 0.37 GB at 4 minutes in the low profile, 2026-10-26). The io worker has an event loop.
				const streaming = () => new Promise((resolve, reject) => {
					const on = (e) => {
						if (!e.data || !(e.data.wasmModule || e.data.wasmError)) return;
						io.removeEventListener('message', on);
						if (e.data.wasmModule) resolve(e.data.wasmModule); else reject(new Error(e.data.wasmError));
					};
					io.addEventListener('message', on);
					io.postMessage({ compileWasm: new URL(B + '/game.wasm', self.location.href).href });
				}).then((module) => WebAssembly.instantiate(module, imports).then((instance) => ({ instance, module })));
				(async () => {
					let r;
					try { r = await streaming(); }
					catch (e) { console.warn('streaming instantiation failed (' + e + '): downloading first'); r = await buffered(); }
					bc.postMessage({ pct: 22, label: 'Starting the engine' });
					done(r.instance, r.module);
				})().catch((e) => { bc.postMessage({ error: 'engine download failed: ' + e }); });
				return {};
			},
			canvas: m.canvas,
			arguments: [],		// filled by applyNow when the choice arrives: emscripten keeps this very array and main() reads it
			wgpuWorker: gpu,
			ioWorker: io,
			netWorker: null,		// created by applyNow for a network mode
			netUrl: '',
			netRtc: false,		// netsocket_wasm.cpp tells the net worker this tab is a peer-to-peer peer (address 10.x.y.z from netPid)
			netPid: 0,
			fullConsole: !!m.fullConsole,		// ?console=1: every engine line in the browser console (default: errors, warnings, the port's lines)
			remoteLog: !!m.remoteLog,		// ?log=1: every engine line to the dev server's log (prejs.js; default: none)
			wgpuShaderUrl: B + '/shaders',
			wgpuShotEvery: m.shotEvery,
			keepThreads: 0,		// bit mask of engine thread groups that are NOT skipped (1 replay, 2 net, 8 idle), see ipc_wasm.cpp; set by applyNow
			mainScriptUrlOrBlob: B + '/game.js' + ((m.fullConsole || m.remoteLog) ? '?' + [m.fullConsole ? 'console=1' : '', m.remoteLog ? 'log=1' : ''].filter(Boolean).join('&') : ''),		// pthreads read both from it, and take game.wasm's folder from it
			locateFile: (p) => B + '/' + p,
			onExit: (code) => { console.error('EXIT ' + code); self.__log('EXIT ' + code); self.__logFlush(); if (code) self.__crash('exit', 'EXIT ' + code); },
			onAbort: (what) => { console.error('ABORT ' + what); self.__log('ABORT ' + what); self.__logFlush(); self.__crash('abort', 'ABORT ' + what); },
			// Savegames and settings kept in IndexedDB by io_worker.js (platform/file/userdata_wasm.cpp sends them): put them back into the in-memory /userdata before the engine looks at it. The run
			// dependency holds main() until they are in (or 8 s, so a broken IndexedDB cannot keep the game from starting). Only /userdata/ paths are accepted.
			preRun: [() => {
				// main() waits for the player's choice. Before that the engine has done nothing but compile.
				if (typeof addRunDependency !== 'function') return;
				addRunDependency('choice');
				applyChoice = () => { applyChoice = null; applyNow().then(() => removeRunDependency('choice'), (e) => { console.error('loader: choice failed: ' + e); removeRunDependency('choice'); }); };
				if (choice) applyChoice();
			}, () => {
				if (typeof FS === 'undefined' || typeof addRunDependency !== 'function' || typeof indexedDB === 'undefined') return;
				addRunDependency('userdata');
				let finished = false;
				const finish = (note) => { if (finished) return; finished = true; self.__log('[loader] userdata: ' + note); self.__logFlush(); removeRunDependency('userdata'); };
				setTimeout(() => finish('timed out'), 8000);
				try {
					const r = indexedDB.open('gta5-userdata', 1);
					r.onupgradeneeded = () => r.result.createObjectStore('files');
					r.onerror = () => finish('IndexedDB unavailable: ' + r.error);
					r.onsuccess = () => {
						let n = 0, bytes = 0;
						try {
							const req = r.result.transaction('files', 'readonly').objectStore('files').openCursor();
							req.onerror = () => finish('read failed: ' + req.error);
							req.onsuccess = () => {
								const c = req.result;
								if (!c) { finish('restored ' + n + ' file(s), ' + bytes + ' bytes'); return; }
								const path = String(c.key);
								try {
									if (path.startsWith('userdata/index.html') && c.value && c.value.data) {
										FS.mkdirTree(path.slice(0, path.lastIndexOf('/')));
										FS.writeFile(path, c.value.data);
										FS.utime(path, c.value.mtime, c.value.mtime);
										n++; bytes += c.value.data.length;
									}
								} catch (e) { self.__log('[loader] userdata: ' + path + ' not restored: ' + e); }
								c.continue();
							};
						} catch (e) { finish('read failed: ' + e); }
					};
				} catch (e) { finish('failed: ' + e); }
			}],
		};
		try { importScripts(B + '/game.js'); } catch (e) { if (B) self.postMessage({ staleBuild: 'game.js' }); throw e; }		// a 404: this page is of an older build (index.php build_marker)
	};
	gpu.onerror = (e) => console.error('gpu worker failed to load: ' + e.message);
};
