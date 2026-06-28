"""
http_server.py — HTTP server + Web Dashboard for Bolt KV Store.

Serves:
  GET  /              → Web dashboard (HTML UI)
  GET  /api           → API info (JSON)
  GET  /api/get?key=  → get a key
  POST /api/put       → {"key", "value", "ttl"}
  DELETE /api/delete?key= → delete a key
  GET  /api/keys      → list all keys
  GET  /api/stats     → engine stats
"""

import asyncio
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path

from engine import KVEngine
from wal import WAL

wal    = WAL(wal_dir=Path("./wal_data"))
engine = KVEngine(wal=wal)
wal.replay(engine)

PORT = int(os.environ.get("PORT", 8080))

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Bolt KV Store — Live Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  * { box-sizing: border-box; }
  body {
    margin: 0; font-family: -apple-system, 'Segoe UI', Roboto, sans-serif;
    background: #0f1117; color: #e6e6e6; min-height: 100vh;
  }
  header {
    padding: 2rem 1.5rem 1rem; text-align: center;
    border-bottom: 1px solid #23262f;
  }
  header h1 { margin: 0; font-size: 1.8rem; }
  header h1 span { color: #6ee7b7; }
  header p { color: #9ca3af; margin: 0.4rem 0 0; font-size: 0.9rem; }
  .badges { display: flex; gap: 8px; justify-content: center; margin-top: 0.8rem; flex-wrap: wrap; }
  .badge { font-size: 0.75rem; background: #1a1d27; border: 1px solid #2a2e3a; padding: 4px 10px; border-radius: 20px; color: #9ca3af; }
  main { max-width: 920px; margin: 0 auto; padding: 1.5rem; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  @media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }
  .card {
    background: #161922; border: 1px solid #23262f; border-radius: 12px; padding: 1.2rem;
  }
  .card h2 { margin: 0 0 0.8rem; font-size: 1rem; color: #d1d5db; display: flex; align-items: center; gap: 6px; }
  input, textarea {
    width: 100%; background: #0f1117; border: 1px solid #2a2e3a; border-radius: 8px;
    color: #e6e6e6; padding: 10px 12px; font-size: 0.9rem; margin-bottom: 8px; font-family: inherit;
  }
  input:focus, textarea:focus { outline: none; border-color: #6ee7b7; }
  button {
    background: #6ee7b7; color: #0f1117; border: none; border-radius: 8px;
    padding: 10px 18px; font-weight: 600; cursor: pointer; font-size: 0.85rem;
    transition: opacity 0.15s;
  }
  button:hover { opacity: 0.85; }
  button.secondary { background: #2a2e3a; color: #e6e6e6; }
  .row { display: flex; gap: 8px; }
  .row input { margin-bottom: 0; }
  #result {
    background: #0f1117; border: 1px solid #2a2e3a; border-radius: 8px;
    padding: 12px; font-family: 'SF Mono', Consolas, monospace; font-size: 0.82rem;
    white-space: pre-wrap; min-height: 60px; color: #9ca3af; margin-top: 10px;
  }
  #result.success { border-color: #6ee7b7; color: #6ee7b7; }
  #result.error { border-color: #f87171; color: #f87171; }
  .keys-list { max-height: 260px; overflow-y: auto; }
  .key-item {
    display: flex; justify-content: space-between; align-items: center;
    padding: 8px 10px; border-radius: 6px; background: #0f1117; margin-bottom: 6px;
    font-family: monospace; font-size: 0.85rem; cursor: pointer; border: 1px solid transparent;
  }
  .key-item:hover { border-color: #6ee7b7; }
  .key-item button { padding: 4px 10px; font-size: 0.7rem; }
  .stats-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  .stat-box { background: #0f1117; border-radius: 8px; padding: 10px; text-align: center; }
  .stat-box .val { font-size: 1.4rem; font-weight: 700; color: #6ee7b7; }
  .stat-box .lbl { font-size: 0.7rem; color: #9ca3af; margin-top: 2px; }
  footer { text-align: center; padding: 2rem; color: #6b7280; font-size: 0.8rem; }
  footer a { color: #6ee7b7; text-decoration: none; }
  .empty { color: #6b7280; text-align: center; padding: 1rem; font-size: 0.85rem; }
</style>
</head>
<body>

<header>
  <h1>&#9889; Bolt<span> KV Store</span></h1>
  <p>Distributed key-value store built from scratch in Python — live demo</p>
  <div class="badges">
    <span class="badge">In-memory storage</span>
    <span class="badge">WAL durability</span>
    <span class="badge">Consistent hashing</span>
    <span class="badge">Raft consensus</span>
    <span class="badge">LRU cache</span>
  </div>
</header>

<main>
  <div class="grid">
    <div class="card">
      <h2>&#128190; Store a value (PUT)</h2>
      <input id="put-key" placeholder="key (e.g. name)">
      <input id="put-value" placeholder="value (e.g. Abhishek)">
      <input id="put-ttl" placeholder="TTL in seconds (optional)">
      <button onclick="doPut()">Store</button>
    </div>

    <div class="card">
      <h2>&#128269; Retrieve a value (GET)</h2>
      <input id="get-key" placeholder="key to look up">
      <button onclick="doGet()">Get Value</button>
      <button class="secondary" onclick="doDelete()" style="margin-left:6px">Delete</button>
    </div>
  </div>

  <div class="card" style="margin-bottom:16px">
    <h2>&#128203; Result</h2>
    <div id="result">Try storing or retrieving a value above...</div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>&#128209; All keys <button class="secondary" style="float:right;padding:4px 10px" onclick="loadKeys()">Refresh</button></h2>
      <div class="keys-list" id="keys-list"><div class="empty">Loading...</div></div>
    </div>

    <div class="card">
      <h2>&#128202; Database stats</h2>
      <div class="stats-grid" id="stats-grid">
        <div class="stat-box"><div class="val">-</div><div class="lbl">Total Keys</div></div>
        <div class="stat-box"><div class="val">-</div><div class="lbl">With TTL</div></div>
      </div>
    </div>
  </div>
</main>

<footer>
  Built by Abhishek Sharma &middot;
  <a href="https://github.com/Abhishek0905Sharma/bolt-kv" target="_blank">View source on GitHub</a>
</footer>

<script>
const API = '/api';

function showResult(text, type) {
  const el = document.getElementById('result');
  el.textContent = text;
  el.className = type || '';
}

async function doPut() {
  const key = document.getElementById('put-key').value.trim();
  const value = document.getElementById('put-value').value.trim();
  const ttl = document.getElementById('put-ttl').value.trim();
  if (!key || !value) { showResult('Please enter both key and value', 'error'); return; }
  try {
    const body = { key, value };
    if (ttl) body.ttl = parseFloat(ttl);
    const res = await fetch(API + '/put', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const data = await res.json();
    showResult('Stored successfully:\\n' + JSON.stringify(data, null, 2), 'success');
    loadKeys(); loadStats();
    document.getElementById('put-key').value = '';
    document.getElementById('put-value').value = '';
    document.getElementById('put-ttl').value = '';
  } catch (e) { showResult('Error: ' + e.message, 'error'); }
}

async function doGet() {
  const key = document.getElementById('get-key').value.trim();
  if (!key) { showResult('Please enter a key', 'error'); return; }
  try {
    const res = await fetch(API + '/get?key=' + encodeURIComponent(key));
    const data = await res.json();
    if (data.exists) showResult('Found:\\n' + JSON.stringify(data, null, 2), 'success');
    else showResult('Key not found:\\n' + JSON.stringify(data, null, 2), 'error');
  } catch (e) { showResult('Error: ' + e.message, 'error'); }
}

async function doDelete() {
  const key = document.getElementById('get-key').value.trim();
  if (!key) { showResult('Please enter a key to delete', 'error'); return; }
  try {
    const res = await fetch(API + '/delete?key=' + encodeURIComponent(key), { method: 'DELETE' });
    const data = await res.json();
    showResult('Delete result:\\n' + JSON.stringify(data, null, 2), data.ok ? 'success' : 'error');
    loadKeys(); loadStats();
  } catch (e) { showResult('Error: ' + e.message, 'error'); }
}

async function loadKeys() {
  const list = document.getElementById('keys-list');
  try {
    const res = await fetch(API + '/keys');
    const data = await res.json();
    if (!data.keys || data.keys.length === 0) {
      list.innerHTML = '<div class="empty">No keys stored yet</div>';
      return;
    }
    list.innerHTML = data.keys.map(k =>
      `<div class="key-item">
        <span onclick="quickGet('${k}')" style="cursor:pointer;flex:1">${k}</span>
        <button onclick="quickDelete('${k}')">Delete</button>
      </div>`
    ).join('');
  } catch (e) { list.innerHTML = '<div class="empty">Error loading keys</div>'; }
}

async function quickGet(key) {
  document.getElementById('get-key').value = key;
  doGet();
}

async function quickDelete(key) {
  try {
    await fetch(API + '/delete?key=' + encodeURIComponent(key), { method: 'DELETE' });
    loadKeys(); loadStats();
    showResult('Deleted key: ' + key, 'success');
  } catch (e) {}
}

async function loadStats() {
  try {
    const res = await fetch(API + '/stats');
    const data = await res.json();
    document.getElementById('stats-grid').innerHTML = `
      <div class="stat-box"><div class="val">${data.total_keys ?? 0}</div><div class="lbl">Total Keys</div></div>
      <div class="stat-box"><div class="val">${data.keys_with_ttl ?? 0}</div><div class="lbl">With TTL</div></div>
    `;
  } catch (e) {}
}

loadKeys();
loadStats();
setInterval(() => { loadKeys(); loadStats(); }, 5000);
</script>
</body>
</html>
"""


class BoltHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def send_json(self, data, status=200):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html, status=200):
        body = html.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/" or parsed.path == "/dashboard":
            self.send_html(DASHBOARD_HTML)

        elif parsed.path == "/api" or parsed.path == "/api/":
            stats = engine.stats()
            self.send_json({
                "status": "ok",
                "name": "Bolt KV Store",
                "description": "Distributed key-value store built from scratch in Python",
                "github": "https://github.com/Abhishek0905Sharma/bolt-kv",
                "dashboard": "/",
                "features": [
                    "In-memory storage with TTL expiry",
                    "Write-Ahead Log (WAL) for crash durability",
                    "Consistent hashing for sharding",
                    "Raft consensus for leader election",
                    "LRU cache layer"
                ],
                "stats": stats,
                "endpoints": {
                    "GET /": "web dashboard",
                    "GET /api": "this info",
                    "GET /api/get?key=<key>": "get a value",
                    "POST /api/put": "set a value {key, value, ttl?}",
                    "DELETE /api/delete?key=<key>": "delete a key",
                    "GET /api/keys": "list all keys",
                    "GET /api/stats": "engine statistics"
                }
            })

        elif parsed.path == "/api/get":
            key = params.get("key", [None])[0]
            if not key:
                self.send_json({"error": "key parameter required"}, 400)
                return
            value = engine.get(key)
            if value is None:
                self.send_json({"key": key, "value": None, "exists": False})
            else:
                self.send_json({"key": key, "value": value, "exists": True})

        elif parsed.path == "/api/keys":
            keys = engine.keys()
            self.send_json({"keys": keys, "count": len(keys)})

        elif parsed.path == "/api/stats":
            self.send_json(engine.stats())

        else:
            self.send_json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/put":
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length)
            try:
                data  = json.loads(body)
                key   = data.get("key")
                value = data.get("value")
                ttl   = data.get("ttl")
                if not key or value is None:
                    self.send_json({"error": "key and value required"}, 400)
                    return
                engine.put(key, str(value), ttl=ttl)
                self.send_json({"ok": True, "key": key, "value": value})
            except json.JSONDecodeError:
                self.send_json({"error": "invalid JSON"}, 400)
        else:
            self.send_json({"error": "not found"}, 404)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/api/delete":
            key = params.get("key", [None])[0]
            if not key:
                self.send_json({"error": "key parameter required"}, 400)
                return
            deleted = engine.delete(key)
            self.send_json({"ok": deleted, "key": key})
        else:
            self.send_json({"error": "not found"}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


def main():
    engine.put("demo:name", "Abhishek")
    engine.put("demo:project", "Bolt KV Store")
    engine.put("demo:built_with", "Python asyncio")
    engine.put("demo:features", "Raft + consistent hashing + LRU cache")

    server = HTTPServer(("0.0.0.0", PORT), BoltHandler)
    print(f"Bolt running on port {PORT}")
    print(f"Dashboard: http://localhost:{PORT}/")
    server.serve_forever()


if __name__ == "__main__":
    main()