"""
server.py — asyncio TCP server for the KV store.

Protocol (Redis RESP-inspired, simplified):
  Request:  <COMMAND> [args...]\n
  Response: +OK\n           (success, no value)
            $<value>\n      (success with value)
            -ERR <msg>\n    (error)
            :1\n / :0\n     (integer response, e.g. EXISTS, DELETE)

Commands:
  PUT <key> <value> [TTL <seconds>]
  GET <key>
  DELETE <key>
  EXISTS <key>
  KEYS
  FLUSH
  STATS
  PING
  QUIT

Design decisions:
- asyncio: handles thousands of concurrent connections with one thread.
  No thread-per-connection overhead. GIL is not a bottleneck here because
  we're I/O-bound (waiting on network reads/writes).
- The engine itself uses threading.RLock because the cleanup thread also
  runs concurrently. asyncio + threads interact safely here because engine
  calls are fast (microseconds) and never block the event loop long.
"""

import asyncio
import logging
import signal
from typing import Optional

from engine import KVEngine
from wal import WAL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("kvstore.server")

HOST = "127.0.0.1"
PORT = 6379  # same default as Redis for easy comparison


class KVProtocol(asyncio.Protocol):
    def __init__(self, engine: KVEngine):
        self._engine = engine
        self._transport: Optional[asyncio.Transport] = None
        self._buffer = b""
        self._peer = None

    def connection_made(self, transport: asyncio.Transport):
        self._transport = transport
        self._peer = transport.get_extra_info("peername")
        logger.debug("Client connected: %s", self._peer)

    def data_received(self, data: bytes):
        self._buffer += data
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            self._handle_command(line.decode("utf-8", errors="replace").strip())

    def connection_lost(self, exc):
        logger.debug("Client disconnected: %s", self._peer)

    # ------------------------------------------------------------------ #
    # Command dispatch                                                     #
    # ------------------------------------------------------------------ #

    def _handle_command(self, raw: str):
        if not raw:
            return
        parts = raw.split()
        if not parts:
            return
        cmd = parts[0].upper()
        args = parts[1:]

        try:
            if cmd == "PING":
                self._send("+PONG")
            elif cmd == "QUIT":
                self._send("+BYE")
                self._transport.close()
            elif cmd == "PUT":
                self._cmd_put(args)
            elif cmd == "GET":
                self._cmd_get(args)
            elif cmd == "DELETE":
                self._cmd_delete(args)
            elif cmd == "EXISTS":
                self._cmd_exists(args)
            elif cmd == "KEYS":
                self._cmd_keys()
            elif cmd == "FLUSH":
                self._cmd_flush()
            elif cmd == "STATS":
                self._cmd_stats()
            else:
                self._send(f"-ERR unknown command '{cmd}'")
        except Exception as e:
            logger.exception("Error handling command: %s", raw)
            self._send(f"-ERR internal error: {e}")

    def _cmd_put(self, args):
        # PUT <key> <value> [TTL <seconds>]
        if len(args) < 2:
            self._send("-ERR usage: PUT <key> <value> [TTL <seconds>]")
            return
        key = args[0]
        # Value may contain spaces — join everything except optional TTL trailer
        ttl = None
        if len(args) >= 4 and args[-2].upper() == "TTL":
            try:
                ttl = float(args[-1])
                value = " ".join(args[1:-2])
            except ValueError:
                self._send("-ERR TTL must be a number")
                return
        else:
            value = " ".join(args[1:])
        self._engine.put(key, value, ttl=ttl)
        self._send("+OK")

    def _cmd_get(self, args):
        if len(args) != 1:
            self._send("-ERR usage: GET <key>")
            return
        value = self._engine.get(args[0])
        if value is None:
            self._send("$NIL")
        else:
            self._send(f"${value}")

    def _cmd_delete(self, args):
        if len(args) != 1:
            self._send("-ERR usage: DELETE <key>")
            return
        deleted = self._engine.delete(args[0])
        self._send(f":{1 if deleted else 0}")

    def _cmd_exists(self, args):
        if len(args) != 1:
            self._send("-ERR usage: EXISTS <key>")
            return
        self._send(f":{1 if self._engine.exists(args[0]) else 0}")

    def _cmd_keys(self):
        keys = self._engine.keys()
        if not keys:
            self._send("$EMPTY")
        else:
            self._send("$" + ",".join(keys))

    def _cmd_flush(self):
        count = self._engine.flush()
        self._send(f"+OK deleted {count} keys")

    def _cmd_stats(self):
        stats = self._engine.stats()
        lines = [f"{k}={v}" for k, v in stats.items()]
        self._send("$" + " | ".join(lines))

    # ------------------------------------------------------------------ #
    # Transport helpers                                                    #
    # ------------------------------------------------------------------ #

    def _send(self, msg: str):
        self._transport.write((msg + "\n").encode("utf-8"))


# ------------------------------------------------------------------ #
# Server bootstrap                                                     #
# ------------------------------------------------------------------ #

async def main():
    wal = WAL()
    engine = KVEngine(wal=wal)

    replayed = wal.replay(engine)
    logger.info("Restored %d keys from WAL", replayed)

    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: KVProtocol(engine),
        HOST,
        PORT,
    )

    logger.info("KVStore listening on %s:%d", HOST, PORT)
    logger.info("Commands: PING, PUT, GET, DELETE, EXISTS, KEYS, FLUSH, STATS, QUIT")
    logger.info("Press Ctrl+C to stop")

    async with server:
        await server.serve_forever()

    logger.info("Shutting down...")
    engine.stop()
    wal.close()


if __name__ == "__main__":
    asyncio.run(main())
