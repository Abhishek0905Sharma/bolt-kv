"""
ring.py — Consistent hash ring for distributing keys across nodes.

How it works:
  - Imagine a circle numbered 0 to 2^32.
  - Each node gets placed at multiple points on this circle (virtual nodes).
  - A key is hashed to a point on the circle, then travels clockwise
    until it hits a node — that node owns the key.

Why virtual nodes?
  Without them, nodes can cluster unevenly on the ring causing one node
  to own 70% of keys while another owns 10%. Virtual nodes (150 per real
  node by default) spread the load evenly across the ring.

Why consistent hashing over modulo?
  With modulo hashing (key % num_nodes), adding or removing ONE node
  reshuffles almost ALL keys. With consistent hashing, only (1/N) of
  keys move — just the ones that belonged to the affected node.
  At 1M keys and 10 nodes, that's ~100k moves instead of ~1M.
"""

import hashlib
import bisect
from typing import Optional


class HashRing:
    def __init__(self, virtual_nodes: int = 150):
        self._virtual_nodes = virtual_nodes
        self._ring: dict[int, str] = {}   # hash position -> node_id
        self._sorted_keys: list[int] = [] # sorted list of positions
        self._nodes: dict[str, dict] = {} # node_id -> node info

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def add_node(self, node_id: str, host: str, port: int):
        """Add a real node — creates virtual_nodes points on the ring."""
        self._nodes[node_id] = {"host": host, "port": port, "id": node_id}
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node_id}:vnode:{i}")
            self._ring[key] = node_id
            bisect.insort(self._sorted_keys, key)

    def remove_node(self, node_id: str):
        """Remove a node and all its virtual points from the ring."""
        if node_id not in self._nodes:
            return
        for i in range(self._virtual_nodes):
            key = self._hash(f"{node_id}:vnode:{i}")
            del self._ring[key]
            idx = bisect.bisect_left(self._sorted_keys, key)
            if idx < len(self._sorted_keys) and self._sorted_keys[idx] == key:
                self._sorted_keys.pop(idx)
        del self._nodes[node_id]

    def get_node(self, key: str) -> Optional[dict]:
        """
        Return the node responsible for this key.
        Finds the first virtual node clockwise from hash(key).
        """
        if not self._ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect(self._sorted_keys, h)
        if idx == len(self._sorted_keys):
            idx = 0  # wrap around the ring
        node_id = self._ring[self._sorted_keys[idx]]
        return self._nodes[node_id]

    def get_nodes(self) -> list[dict]:
        """Return all real nodes."""
        return list(self._nodes.values())

    def node_count(self) -> int:
        return len(self._nodes)

    def distribution(self) -> dict[str, int]:
        """
        Show how many virtual nodes each real node owns.
        In a balanced ring these should all be close to virtual_nodes.
        """
        counts: dict[str, int] = {nid: 0 for nid in self._nodes}
        for node_id in self._ring.values():
            counts[node_id] += 1
        return counts

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _hash(self, key: str) -> int:
        """MD5 hash truncated to 32 bits — fast and uniform enough."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)
