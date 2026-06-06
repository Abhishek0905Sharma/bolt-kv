"""
raft_node.py — Raft KV node.
Key fix: election runs as a background task, never blocking the protocol handler.
"""

import asyncio
import argparse
import logging
import json
from pathlib import Path
from typing import Optional

from engine import KVEngine
from wal import WAL
from raft import RaftNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("kvstore.raft_node")

HOST      = "127.0.0.1"
ALL_PORTS = [6379, 6380, 6381]


class CombinedProtocol(asyncio.Protocol):
    def __init__(self, engine: KVEngine, raft: RaftNode):
        self._engine    = engine
        self._raft      = raft
        self._transport = None
        self._buffer    = b""

    def connection_made(self, transport):
        self._transport = transport

    def data_received(self, data: bytes):
        self._buffer += data
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            raw = line.decode("utf-8", errors="replace").strip()
            if raw.startswith("RAFT_RPC "):
                # Schedule Raft RPC as a new task — never blocks protocol handler
                asyncio.ensure_future(self._handle_raft(raw[9:], self._transport))
            elif raw:
                self._handle_kv(raw)

    def connection_lost(self, exc):
        pass

    async def _handle_raft(self, json_str: str, transport):
        try:
            msg      = json.loads(json_str)
            msg_type = msg.get("type")
            if msg_type == "request_vote":
                resp = await self._raft.handle_request_vote(msg)
            elif msg_type == "append_entries":
                resp = await self._raft.handle_append_entries(msg)
            else:
                resp = {"error": "unknown"}
            transport.write(json.dumps(resp).encode() + b"\n")
        except Exception as e:
            try:
                transport.write(json.dumps({"error": str(e)}).encode() + b"\n")
            except Exception:
                pass

    def _handle_kv(self, raw: str):
        parts = raw.split()
        if not parts:
            return
        cmd  = parts[0].upper()
        args = parts[1:]
        try:
            if   cmd == "PING":    self._send("+PONG")
            elif cmd == "QUIT":    self._send("+BYE"); self._transport.close()
            elif cmd == "PUT":     self._cmd_put(args)
            elif cmd == "GET":     self._cmd_get(args)
            elif cmd == "DELETE":  self._cmd_delete(args)
            elif cmd == "EXISTS":  self._cmd_exists(args)
            elif cmd == "KEYS":    self._cmd_keys()
            elif cmd == "FLUSH":   self._cmd_flush()
            elif cmd == "STATS":   self._cmd_stats()
            else: self._send(f"-ERR unknown command '{cmd}'")
        except Exception as e:
            self._send(f"-ERR {e}")

    def _cmd_put(self, args):
        if not self._raft.is_leader():
            s = self._raft.status()
            self._send(f"-ERR not leader. Leader: {s['leader'] or 'election in progress'}")
            return
        if len(args) < 2:
            self._send("-ERR PUT <key> <value> [TTL <sec>]"); return
        key = args[0]
        ttl = None
        if len(args) >= 4 and args[-2].upper() == "TTL":
            try:    ttl = float(args[-1]); value = " ".join(args[1:-2])
            except: self._send("-ERR TTL must be a number"); return
        else:
            value = " ".join(args[1:])
        cmd = f"PUT {key} {value}" + (f" TTL {ttl}" if ttl else "")
        asyncio.ensure_future(self._raft.submit(cmd))
        self._send("+OK")

    def _cmd_delete(self, args):
        if not self._raft.is_leader():
            self._send("-ERR not leader"); return
        asyncio.ensure_future(self._raft.submit(f"DELETE {args[0]}"))
        self._send(":1")

    def _cmd_get(self, args):
        v = self._engine.get(args[0])
        self._send("$NIL" if v is None else f"${v}")

    def _cmd_exists(self, args):
        self._send(f":{1 if self._engine.exists(args[0]) else 0}")

    def _cmd_keys(self):
        keys = self._engine.keys()
        self._send("$EMPTY" if not keys else "$" + ",".join(keys))

    def _cmd_flush(self):
        self._send(f"+OK deleted {self._engine.flush()} keys")

    def _cmd_stats(self):
        lines  = [f"{k}={v}" for k, v in self._engine.stats().items()]
        lines += [f"raft_{k}={v}" for k, v in self._raft.status().items()]
        self._send("$" + " | ".join(lines))

    def _send(self, msg: str):
        self._transport.write((msg + "\n").encode("utf-8"))


def make_apply_callback(engine: KVEngine):
    async def apply(command: str):
        parts = command.split()
        if not parts: return
        cmd = parts[0].upper()
        if cmd == "PUT" and len(parts) >= 3:
            key = parts[1]
            ttl = None
            if len(parts) >= 5 and parts[-2].upper() == "TTL":
                try:    ttl = float(parts[-1]); value = " ".join(parts[2:-2])
                except: value = " ".join(parts[2:])
            else:
                value = " ".join(parts[2:])
            engine.put(key, value, ttl=ttl)
        elif cmd == "DELETE" and len(parts) >= 2:
            engine.delete(parts[1])
        elif cmd == "FLUSH":
            engine.flush()
    return apply


async def run(node_id: str, host: str, port: int):
    wal_dir = Path(f"./wal_raft_{node_id}")
    wal     = WAL(wal_dir=wal_dir)
    engine  = KVEngine(wal=wal)
    wal.replay(engine)

    peers = [(HOST, p) for p in ALL_PORTS if p != port]
    raft  = RaftNode(node_id=node_id, peers=peers,
                     apply_callback=make_apply_callback(engine))

    # Cancel the auto-started timer from __init__
    if raft._election_task and not raft._election_task.done():
        raft._election_task.cancel()
        await asyncio.sleep(0)   # let cancellation propagate

    server = await asyncio.get_running_loop().create_server(
        lambda: CombinedProtocol(engine, raft), host, port,
    )

    logger.info("Node %s listening on %s:%d | peers=%s", node_id, host, port, peers)
    logger.info("Node %s waiting 6s for all nodes to be ready...", node_id)

    async with server:
        await asyncio.sleep(6)          # all nodes start within this window
        await asyncio.sleep(0)          # flush event loop
        raft._reset_timer()             # NOW start elections
        logger.info("Node %s election timer armed!", node_id)
        await server.serve_forever()

    engine.stop()
    wal.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id",   required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    print(f"Starting Raft Node {args.id} on port {args.port}")
    print("─" * 50)
    asyncio.run(run(args.id, args.host, args.port))


if __name__ == "__main__":
    main()
