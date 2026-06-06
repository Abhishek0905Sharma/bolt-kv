"""
http_server.py — HTTP REST API wrapper for Bolt KV Store.
Exposes the KV store over HTTP so Railway can give us a public URL.

Endpoints:
  GET  /              → health check + stats
  GET  /get?key=name  → get a key
  POST /put           → {"key": "name", "value": "Abhishek"}
  DELETE /delete?key=name → delete a key
  GET  /keys          → list all keys
"""

import asyncio
import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading

from engine import KVEngine
from wal import WAL
from pathlib import Path

# Global engine
wal    = WAL(wal_dir=Path("./wal_data"))
engine = KVEngine(wal=wal)
wal.replay(engine)

PORT = int(os.environ.get("PORT", 8080))


class BoltHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # suppress default logging

    def send_json(self, data, status=200):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/" or parsed.path == "/health":
            stats = engine.stats()
            self.send_json({
                "status": "ok",
                "name": "Bolt KV Store",
                "description": "Distributed key-value store built from scratch in Python",
                "github": "https://github.com/Abhishek0905Sharma/bolt-kv",
                "features": [
                    "In-memory storage with TTL expiry",
                    "Write-Ahead Log (WAL) for crash durability",
                    "Consistent hashing for sharding",
                    "Raft consensus for leader election",
                    "LRU cache layer"
                ],
                "stats": stats,
                "endpoints": {
                    "GET /": "health check",
                    "GET /get?key=<key>": "get a value",
                    "POST /put": "set a value {key, value, ttl?}",
                    "DELETE /delete?key=<key>": "delete a key",
                    "GET /keys": "list all keys",
                    "GET /stats": "engine statistics"
                }
            })

        elif parsed.path == "/get":
            key = params.get("key", [None])[0]
            if not key:
                self.send_json({"error": "key parameter required"}, 400)
                return
            value = engine.get(key)
            if value is None:
                self.send_json({"key": key, "value": None, "exists": False})
            else:
                self.send_json({"key": key, "value": value, "exists": True})

        elif parsed.path == "/keys":
            keys = engine.keys()
            self.send_json({"keys": keys, "count": len(keys)})

        elif parsed.path == "/stats":
            self.send_json(engine.stats())

        else:
            self.send_json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/put":
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

        if parsed.path == "/delete":
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
    # Seed some demo data
    engine.put("demo:name", "Abhishek")
    engine.put("demo:project", "Bolt KV Store")
    engine.put("demo:built_with", "Python asyncio")
    engine.put("demo:features", "Raft + consistent hashing + LRU cache")

    server = HTTPServer(("0.0.0.0", PORT), BoltHandler)
    print(f"Bolt HTTP API running on port {PORT}")
    print(f"Visit: http://localhost:{PORT}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
