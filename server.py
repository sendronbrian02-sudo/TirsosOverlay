#!/usr/bin/env python3
"""
TirsosOverlay MMR Cache Server
Cache tracker.gg API pour réponses quasi-instantanées.
"""
import json, time, threading, os, gzip, hashlib, secrets
try:
    import brotli as _brotli
    _BROTLI_OK = True
except ImportError:
    _BROTLI_OK = False
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request, urllib.error, urllib.parse

# ── Config ────────────────────────────────────────────────────────
PORT       = int(os.environ.get("PORT", 8080))
API_KEY    = os.environ.get("RLS_API_KEY", secrets.token_hex(24))
CACHE_TTL  = 45   # secondes avant expiration du cache
MAX_CACHE  = 500  # max entrées en mémoire

print(f"[BOOT] API_KEY={API_KEY}", flush=True)

# ── Mapping playlist SOS → playlistId tracker.gg ───────────────────────
_PLAYLIST_MAP = {
    "1":  "10",
    "11": "10",
    "10": "11",
    "13": "13",
    "27": "27",
    "28": "28",
    "29": "29",
    "30": "30",
    "34": "34",
}

# ── Cache en mémoire thread-safe ──────────────────────────────────
_cache: dict = {}   # key -> {"ts": float, "data": dict}
_lock  = threading.Lock()

def _cache_get(key: str):
    with _lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < CACHE_TTL:
            return entry["data"]
    return None

def _cache_set(key: str, data: dict):
    with _lock:
        if len(_cache) >= MAX_CACHE:
            oldest = min(_cache, key=lambda k: _cache[k]["ts"])
            del _cache[oldest]
        _cache[key] = {"ts": time.time(), "data": data}

# ── Headers tracker.gg (méthode Overwolf) ────────────────────────
_HEADERS_OW = {
    "User-Agent":      "overwolf-plugin/1.0",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "x-platform":      "overwolf",
    "Origin":          "https://overwolf.com",
    "Referer":         "https://overwolf.com/",
    "DNT":             "1",
}
_HEADERS_BR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer":         "https://rocketleague.tracker.network/",
    "Origin":          "https://rocketleague.tracker.network",
}

def _fetch_tracker(player: str, pid: str) -> dict | None:
    """Fetch depuis tracker.gg avec fallback headers."""
    encoded = urllib.parse.quote(player.strip(), safe="")
    url = (f"https://api.tracker.gg/api/v2/rocket-league/standard"
           f"/profile/epic/{encoded}")
    for hdrs in [_HEADERS_OW, _HEADERS_BR]:
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read()
                enc = resp.info().get("Content-Encoding", "")
                status = resp.status
            print(f"[FETCH] HTTP {status} len={len(raw)} enc={enc!r}", flush=True)
            # Décompression auto selon encoding
            if enc == "br" or enc == "brotli":
                if _BROTLI_OK:
                    try: raw = _brotli.decompress(raw)
                    except Exception as _be:
                        print(f"[WARN] brotli fail: {_be}", flush=True)
                else:
                    print("[WARN] brotli reçu mais module absent", flush=True)
            elif enc == "gzip" or (len(raw) > 1
                                   and raw[0] == 0x1f and raw[1] == 0x8b):
                try: raw = gzip.decompress(raw)
                except Exception: pass
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("latin-1")
            if not text.strip():
                print(f"[FETCH WARN] Réponse vide de tracker.gg", flush=True)
                continue
            try:
                data = json.loads(text)
            except Exception as je:
                print(f"[FETCH WARN] JSON parse error: {je} — raw[:200]={text[:200]!r}", flush=True)
                continue
            segs = data.get("data", {}).get("segments", [])
            if not segs:
                continue
            # Extrait MMR + rank par playlist
            result = {"player": player, "ts": time.time(), "playlists": {}}
            for seg in segs:
                attrs = seg.get("attributes", {})
                pid_seg = str(attrs.get("playlistId", ""))
                if not pid_seg or pid_seg == "None":
                    continue
                stats = seg.get("stats", {})
                mmr_obj  = stats.get("rating", {})
                rank_obj = stats.get("tier",   {})
                mmr  = mmr_obj.get("value") if isinstance(mmr_obj, dict) else None
                rank = rank_obj.get("metadata", {}).get("name") if isinstance(rank_obj, dict) else None
                if mmr is not None:
                    result["playlists"][pid_seg] = {
                        "mmr":  int(round(float(mmr))),
                        "rank": rank or "",
                    }
            return result
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {"error": "not_found", "player": player}
            if e.code == 429:
                time.sleep(3)
        except Exception as e:
            print(f"[FETCH ERR] {e}", flush=True)
    return None

def _search_tracker(player: str) -> str | None:
    """Cherche le nom exact sur tracker.gg search."""
    pname = player.strip()
    encoded = urllib.parse.quote(pname, safe="")
    url = (f"https://api.tracker.gg/api/v2/rocket-league/standard"
           f"/search?platform=epic&query={encoded}&autocomplete=true")
    for hdrs in [_HEADERS_OW, _HEADERS_BR]:
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=8) as resp:
                raw = resp.read()
                enc = resp.info().get("Content-Encoding", "")
            if enc == "gzip" or (len(raw) > 1 and raw[0] == 0x1f and raw[1] == 0x8b):
                try: raw = gzip.decompress(raw)
                except Exception: pass
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("latin-1")
            data = json.loads(text)
            results = data.get("data", [])
            pname_l = pname.lower()
            for r in results:
                found = r.get("platformUserIdentifier", "")
                if pname_l in found.lower() or found.lower() in pname_l:
                    return found
            if results:
                return results[0].get("platformUserIdentifier")
        except Exception:
            pass
    return None

# ── Prefetch en arrière-plan (post-match) ────────────────────────
def _bg_prefetch(player: str, pid: str):
    """Rafraîchit le cache en arrière-plan sans bloquer la réponse."""
    def _run():
        # Attend 2s pour laisser tracker.gg se mettre à jour
        time.sleep(2)
        key = f"{player.lower()}:{pid}"
        data = _fetch_tracker(player, pid)
        if data and "playlists" in data:
            _cache_set(key, data)
            avail_pids = list(data["playlists"].keys())
            print(f"[PREFETCH OK] {player!r} pid={pid} pids_cached={avail_pids} "
                  f"mmr={data['playlists'].get(pid,{}).get('mmr')}", flush=True)
    threading.Thread(target=_run, daemon=True).start()

# ── HTTP Handler ──────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # Silence le log verbeux par défaut

    def _send_json(self, code: int, obj: dict):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self) -> bool:
        auth = self.headers.get("X-API-Key", "")
        return auth == API_KEY

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))

        # ── Health check ─────────────────────────────────────────
        if parsed.path in ("/", "/health"):
            self._send_json(200, {"status": "ok", "cache": len(_cache)})
            return

        # ── GET /mmr?player=X&pid=11&prefetch=1 ──────────────────
        if parsed.path == "/mmr":
            if not self._check_auth():
                self._send_json(401, {"error": "unauthorized"})
                return
            # Décode le nom proprement (urllib donne déjà str, mais on force UTF-8)
            player = urllib.parse.unquote(params.get("player", ""), encoding="utf-8").strip()
            pid = params.get("pid", "11").strip()  # Déjà converti par l overlay
            prefetch = params.get("prefetch", "0") == "1"
            if not player:
                self._send_json(400, {"error": "missing player"})
                return

            key = f"{player.lower()}:{pid}"

            # 1. Cache hit → réponse immédiate
            cached = _cache_get(key)
            if cached:
                pl = cached.get("playlists", {}).get(pid)
                if pl:
                    self._send_json(200, {
                        "cached": True,
                        "player": cached.get("player", player),
                        "pid":    pid,
                        "mmr":    pl["mmr"],
                        "rank":   pl["rank"],
                        "ts":     cached.get("ts"),
                    })
                    # Lance un prefetch silencieux si demandé (post-match)
                    if prefetch:
                        _bg_prefetch(player, pid)
                    return

            # 2. Cache miss → fetch synchrone (première fois ~300-500ms)
            print(f"[MISS] {player!r} pid={pid}", flush=True)
            # Essaie d'abord avec le nom tel quel
            data = _fetch_tracker(player, pid)
            # 404 → cherche le bon nom
            if data and data.get("error") == "not_found":
                exact = _search_tracker(player)
                if exact and exact != player:
                    data = _fetch_tracker(exact, pid)

            if not data or "playlists" not in data:
                self._send_json(404, {
                    "cached": False, "player": player,
                    "error":  "not_found_or_rate_limited"
                })
                return

            _cache_set(key, data)
            avail = list(data["playlists"].keys())
            print(f"[CACHE SET] {player!r} pids={avail}", flush=True)
            pl = data["playlists"].get(pid)
            if not pl:
                self._send_json(404, {
                    "cached": False, "player": player, "pid": pid,
                    "error": "playlist_not_found",
                    "available_pids": list(data["playlists"].keys()),
                })
                return

            self._send_json(200, {
                "cached": False,
                "player": data.get("player", player),
                "pid":    pid,
                "mmr":    pl["mmr"],
                "rank":   pl["rank"],
                "ts":     data["ts"],
            })
            return

        self._send_json(404, {"error": "not found"})

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers", "X-API-Key")
        self.end_headers()

# ── Entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"[START] TirsosOverlay MMR Cache Server — port {PORT}", flush=True)
    print(f"[START] Cache TTL={CACHE_TTL}s  MaxEntries={MAX_CACHE}", flush=True)
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.serve_forever()
