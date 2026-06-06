"""
lru_cache.py — LRU (Least Recently Used) cache layer for the KV engine.

Why LRU cache?
  Disk/WAL writes are slow (~1-5k ops/sec with fsync).
  An in-memory cache serves hot reads in microseconds.
  LRU evicts the least recently used key when capacity is reached,
  keeping the most frequently accessed data in memory.

Implementation:
  OrderedDict gives O(1) move-to-end and O(1) popitem(last=False).
  This is the classic LRU implementation used in production systems.
  Python's functools.lru_cache uses the same approach internally.

Design decision — write-through vs write-back:
  We use WRITE-THROUGH: every PUT updates both cache and engine.
  Write-back (cache only, flush later) is faster but risks data loss on crash.
  Since we already have a WAL for durability, write-through is the right choice.

Google interview question this answers:
  "How would you add caching to your KV store?"
  "What eviction policy would you use and why?"
  "What's the difference between write-through and write-back caching?"
"""

from collections import OrderedDict
from typing import Optional, Any
import threading
import logging

logger = logging.getLogger("kvstore.lru_cache")


class LRUCache:
    def __init__(self, capacity: int = 1000):
        """
        capacity: maximum number of keys to hold in cache.
        Default 1000 — tune based on available memory.
        At ~100 bytes per entry, 1000 entries = ~100KB.
        """
        self._capacity  = capacity
        self._cache: OrderedDict = OrderedDict()
        self._lock      = threading.RLock()
        self._hits      = 0
        self._misses    = 0
        self._evictions = 0
        logger.info("LRUCache initialised (capacity=%d)", capacity)

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[Any]:
        """
        O(1) cache lookup.
        On hit: move key to end (most recently used).
        On miss: return None — caller must fetch from engine.
        """
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            self._misses += 1
            return None

    def put(self, key: str, value: Any):
        """
        O(1) cache insert.
        If at capacity, evict the least recently used key (front of OrderedDict).
        """
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            if len(self._cache) > self._capacity:
                evicted_key, _ = self._cache.popitem(last=False)
                self._evictions += 1
                logger.debug("Evicted key: %s", evicted_key)

    def delete(self, key: str):
        """Remove key from cache on DELETE."""
        with self._lock:
            self._cache.pop(key, None)

    def flush(self):
        """Clear entire cache on FLUSH."""
        with self._lock:
            self._cache.clear()

    def stats(self) -> dict:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            return {
                "cache_size":     len(self._cache),
                "cache_capacity": self._capacity,
                "cache_hits":     self._hits,
                "cache_misses":   self._misses,
                "cache_hit_rate": f"{hit_rate:.1f}%",
                "cache_evictions":self._evictions,
            }

    def __len__(self):
        return len(self._cache)


class CachedKVEngine:
    """
    Wraps KVEngine with an LRU cache layer.
    Drop-in replacement — same API as KVEngine.

    Read path:  GET → check cache → hit? return. miss? read engine, populate cache.
    Write path: PUT → write engine (WAL) → update cache (write-through).
    Delete path: DELETE → delete from engine → invalidate cache.
    """

    def __init__(self, engine, capacity: int = 1000):
        self._engine = engine
        self._cache  = LRUCache(capacity=capacity)

    def get(self, key: str) -> Optional[Any]:
        # Check cache first
        value = self._cache.get(key)
        if value is not None:
            return value
        # Cache miss — read from engine and populate cache
        value = self._engine.get(key)
        if value is not None:
            self._cache.put(key, value)
        return value

    def put(self, key: str, value: Any, ttl: Optional[float] = None):
        # Write-through: engine first (WAL), then cache
        self._engine.put(key, value, ttl=ttl)
        self._cache.put(key, value)

    def delete(self, key: str) -> bool:
        deleted = self._engine.delete(key)
        if deleted:
            self._cache.delete(key)
        return deleted

    def exists(self, key: str) -> bool:
        return self._engine.exists(key)

    def keys(self) -> list:
        return self._engine.keys()

    def flush(self) -> int:
        count = self._engine.flush()
        self._cache.flush()
        return count

    def stats(self) -> dict:
        engine_stats = self._engine.stats()
        cache_stats  = self._cache.stats()
        return {**engine_stats, **cache_stats}

    def stop(self):
        self._engine.stop()
