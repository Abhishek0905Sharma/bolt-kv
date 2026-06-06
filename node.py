"""
node.py — Launch a single KV node on a specific port.

Usage:
  python node.py --port 6379 --id A
  python node.py --port 6380 --id B
  python node.py --port 6381 --id C

Each node is a fully independent KV server with its own WAL directory.
In Phase 3 we'll add replication between them. For now each is standalone.

Run one node per terminal window.
"""

import asyncio
import argparse
import logging
import signal
from pathlib import Path
from typing import Optional

from engine import KVEngine
from wal import WAL
from server import KVProtocol   # reuse exact same protocol from Phase 1

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] node-%(node_id)s: %(message)s",
)


def make_logger(node_id: str):
    import logging
    logger = logging.getLogger(f"kvstore.node.{node_id}")
    return logger


async def run_node(node_id: str, host: str, port: int):
    logger = make_logger(node_id)

    # Each node gets its own WAL directory so they don't share state
    wal_dir = Path(f"./wal_node_{node_id}")
    wal = WAL(wal_dir=wal_dir)
    engine = KVEngine(wal=wal)

    replayed = wal.replay(engine)
    logger.info(f"Node {node_id} restored {replayed} keys from WAL")

    loop = asyncio.get_running_loop()
    server = await loop.create_server(
        lambda: KVProtocol(engine),
        host,
        port,
    )

    logger.info(f"Node {node_id} listening on {host}:{port}")
    logger.info("Press Ctrl+C to stop this node")

    async with server:
        await server.serve_forever()

    logger.info(f"Node {node_id} shutting down")
    engine.stop()
    wal.close()


def main():
    parser = argparse.ArgumentParser(description="Run a single KV store node")
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on")
    parser.add_argument("--id",   type=str, default="A",  help="Node ID (A, B, C ...)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind")
    args = parser.parse_args()

    print(f"Starting Node {args.id} on {args.host}:{args.port}")
    print(f"WAL directory: ./wal_node_{args.id}")
    print("─" * 40)

    asyncio.run(run_node(args.id, args.host, args.port))


if __name__ == "__main__":
    main()
