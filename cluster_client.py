"""
cluster_client.py — Smart client that routes keys to the correct node
                    using consistent hashing.

Usage (interactive REPL):
  python cluster_client.py

Usage (single command):
  python cluster_client.py GET name
  python cluster_client.py PUT city Hyderabad

How it works:
  1. On startup, builds a hash ring with Node A, B, C.
  2. For every command, hashes the key to find the responsible node.
  3. Opens a TCP connection to that specific node and sends the command.
  4. The user never needs to know which node owns which key.

This is exactly how Redis Cluster's smart client works.
"""

import socket
import sys
import hashlib
import bisect

try:
    import readline
except ImportError:
    pass  # Windows — fine without it

# ------------------------------------------------------------------ #
# Cluster configuration                                               #
# ------------------------------------------------------------------ #

CLUSTER_NODES = [
    {"id": "A", "host": "127.0.0.1", "port": 6379},
    {"id": "B", "host": "127.0.0.1", "port": 6380},
    {"id": "C", "host": "127.0.0.1", "port": 6381},
]

TIMEOUT = 5
VIRTUAL_NODES = 150

# ------------------------------------------------------------------ #
# Embedded mini hash ring (no import needed, self-contained)          #
# ------------------------------------------------------------------ #

class MiniRing:
    def __init__(self):
        self._ring = {}
        self._keys = []
        self._nodes = {}

    def add(self, node):
        nid = node["id"]
        self._nodes[nid] = node
        for i in range(VIRTUAL_NODES):
            h = self._h(f"{nid}:vnode:{i}")
            self._ring[h] = nid
            bisect.insort(self._keys, h)

    def get(self, key: str) -> dict:
        if not self._ring:
            raise RuntimeError("No nodes in ring")
        h = self._h(key)
        idx = bisect.bisect(self._keys, h) % len(self._keys)
        nid = self._ring[self._keys[idx]]
        return self._nodes[nid]

    def _h(self, s):
        return int(hashlib.md5(s.encode()).hexdigest(), 16) % (2**32)


# Build the ring once at startup
_ring = MiniRing()
for _n in CLUSTER_NODES:
    _ring.add(_n)

def _find_leader():
    for n in CLUSTER_NODES:
        try:
            sock = connect(n)
            resp = send(sock, "STATS")
            sock.close()
            if "raft_leader=" in resp:
                leader_id = resp.split("raft_leader=")[1].split("|")[0].strip()
                for node in CLUSTER_NODES:
                    if node["id"] == leader_id:
                        return node
        except Exception:
            continue
    return None

# ------------------------------------------------------------------ #
# Connection + protocol                                               #
# ------------------------------------------------------------------ #

def connect(node: dict) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.connect((node["host"], node["port"]))
    return sock


def send(sock: socket.socket, command: str) -> str:
    sock.sendall((command.strip() + "\n").encode())
    resp = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        resp += chunk
        if b"\n" in resp:
            break
    return resp.decode().strip()


def route_and_send(command: str) -> tuple[str, str]:
    """
    Extract the key from the command, find the right node, send it.
    Returns (response, node_id).
    """
    parts = command.strip().split()
    cmd = parts[0].upper() if parts else ""

    # Commands that need routing by key
    if cmd in ("GET", "PUT", "DELETE", "EXISTS") and len(parts) >= 2:
        key = parts[1]
        if cmd in ("PUT", "DELETE"):
            leader = _find_leader()
            node = leader if leader else _ring.get(key)
        else:
            node = _ring.get(key)
    elif cmd in ("KEYS", "STATS", "FLUSH", "PING"):
        # Broadcast to all nodes and merge
        return broadcast(command), "ALL"
    else:
        node = CLUSTER_NODES[0]  # fallback

    try:
        sock = connect(node)
        resp = send(sock, command)
        sock.close()
        return resp, node["id"]
    except ConnectionRefusedError:
        return f"-ERR Node {node['id']} ({node['port']}) is not running", node["id"]


def broadcast(command: str) -> str:
    """Send command to all nodes and merge results."""
    cmd = command.strip().upper()
    results = []
    for node in CLUSTER_NODES:
        try:
            sock = connect(node)
            resp = send(sock, command)
            sock.close()
            results.append((node["id"], resp))
        except ConnectionRefusedError:
            results.append((node["id"], "-ERR offline"))

    # Merge KEYS responses
    if cmd == "KEYS":
        all_keys = []
        for _, resp in results:
            if resp.startswith("$") and resp[1:] not in ("EMPTY", "NIL"):
                all_keys.extend(resp[1:].split(","))
        if not all_keys:
            return "$EMPTY"
        return "$" + ",".join(all_keys)

    # Merge STATS
    if cmd == "STATS":
        merged = []
        for nid, resp in results:
            merged.append(f"Node {nid}: {resp[1:] if resp.startswith('$') else resp}")
        return "$" + " || ".join(merged)

    # For PING/FLUSH just show all responses
    lines = [f"Node {nid}: {resp}" for nid, resp in results]
    return "$" + " | ".join(lines)

# ------------------------------------------------------------------ #
# Pretty output                                                        #
# ------------------------------------------------------------------ #

NODE_COLORS = {"A": "\033[34m", "B": "\033[32m", "C": "\033[33m", "ALL": "\033[35m"}
RESET = "\033[0m"

def pretty(response: str, node_id: str) -> str:
    nc = NODE_COLORS.get(node_id, "")
    tag = f" {nc}[Node {node_id}]{RESET}" if node_id else ""

    if response.startswith("+"):
        return f"\033[32m{response[1:]}\033[0m{tag}"
    elif response.startswith("-ERR"):
        return f"\033[31m{response[1:]}\033[0m{tag}"
    elif response.startswith("$"):
        val = response[1:]
        if val == "NIL":
            return f"\033[90m(nil)\033[0m{tag}"
        if val == "EMPTY":
            return f"\033[90m(empty)\033[0m{tag}"
        if "||" in val:
            lines = [f"  \033[33m{p.strip()}\033[0m" for p in val.split("||")]
            return "\n".join(lines)
        if "|" in val and "Node" in val:
            lines = [f"  \033[90m{p.strip()}\033[0m" for p in val.split("|")]
            return "\n".join(lines)
        if "," in val:
            keys = val.split(",")
            return "\n".join(f"  \033[36m{i+1}) {k}\033[0m" for i, k in enumerate(keys)) + tag
        return f"\033[36m\"{val}\"\033[0m{tag}"
    elif response.startswith(":"):
        return f"\033[33m(integer) {response[1:]}\033[0m{tag}"
    return response + tag


HELP = """
Commands:
  PUT <key> <value> [TTL <seconds>]   — auto-routed to correct node
  GET <key>                           — fetched from correct node
  DELETE <key>                        — deleted from correct node
  EXISTS <key>                        — checked on correct node
  KEYS                                — merged from all nodes
  STATS                               — stats from all nodes
  PING                                — ping all nodes
  FLUSH                               — flush all nodes
  ROUTE <key>                         — show which node owns a key
  NODES                               — list all nodes
"""

# ------------------------------------------------------------------ #
# REPL                                                                 #
# ------------------------------------------------------------------ #

def repl():
    print("\033[1mBolt Cluster Client\033[0m — connected to 3-node cluster")
    print("Keys are automatically routed to the correct node.\n")

    while True:
        try:
            line = input("\033[90mbolt-cluster>\033[0m ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not line:
            continue

        parts = line.split()
        cmd = parts[0].upper()

        if cmd in ("HELP", "?"):
            print(HELP)
            continue

        if cmd in ("EXIT", "QUIT"):
            break

        if cmd == "NODES":
            for n in CLUSTER_NODES:
                print(f"  Node {n['id']} → {n['host']}:{n['port']}")
            continue

        if cmd == "ROUTE" and len(parts) >= 2:
            key = parts[1]
            node = _ring.get(key)
            print(f"  Key \033[36m\"{key}\"\033[0m → \033[1mNode {node['id']}\033[0m (port {node['port']})")
            continue

        resp, node_id = route_and_send(line)
        print(pretty(resp, node_id))


def single_command(args):
    command = " ".join(args)
    resp, node_id = route_and_send(command)
    print(pretty(resp, node_id))


def main():
    if len(sys.argv) > 1:
        single_command(sys.argv[1:])
    else:
        repl()


if __name__ == "__main__":
    main()
