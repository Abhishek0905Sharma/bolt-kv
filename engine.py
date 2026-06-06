"""
engine.py — In-memory KV storage engine with TTL expiry.

Design decisions:
- dict for O(1) average-case GET/PUT/DELETE
- Lazy expiry on GET (no background thread scanning all keys)
- Active expiry via background thread every CLEANUP_INTERVAL seconds
  so memory doesn't grow unboundedly with expired keys
- Thread-safe via RLock (reentrant so cleanup thread and request thread
  can both call _is_expired without deadlock)
"""

import time
import threading
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

CLEANUP_INTERVAL = 5  # seconds between active expiry sweeps


class KVEngine:
    def __init__(self, wal=None):
        self._store: dict[str, Any] = {}
        self._expiry: dict[str, float] = {}  # key -> absolute expiry timestamp
        self._lock = threading.RLock()
        self._wal = wal  # injected; may be None for in-memory-only use

        self._stop_event = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop, daemon=True, name="kvstore-cleanup"
        )
        self._cleanup_thread.start()
        logger.info("KVEngine started")

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if self._is_expired(key):
                self._delete_internal(key)
                return None
            return self._store.get(key)

    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """
        Set key=value. Optional ttl in seconds.
        WAL write happens before in-memory update (write-ahead guarantee).
        """
        with self._lock:
            if self._wal:
                self._wal.append("PUT", key, value, ttl)
            self._store[key] = value
            if ttl is not None:
                self._expiry[key] = time.monotonic() + ttl
            else:
                self._expiry.pop(key, None)  # clear any previous TTL

    def delete(self, key: str) -> bool:
        with self._lock:
            if key not in self._store:
                return False
            if self._wal:
                self._wal.append("DELETE", key, None, None)
            self._delete_internal(key)
            return True

    def exists(self, key: str) -> bool:
        with self._lock:
            if self._is_expired(key):
                self._delete_internal(key)
                return False
            return key in self._store

    def keys(self) -> list[str]:
        """Return all non-expired keys."""
        with self._lock:
            now = time.monotonic()
            return [
                k for k in self._store
                if k not in self._expiry or self._expiry[k] > now
            ]

    def flush(self) -> int:
        """Delete all keys. Returns count of deleted keys."""
        with self._lock:
            count = len(self._store)
            if self._wal:
                self._wal.append("FLUSH", "", None, None)
            self._store.clear()
            self._expiry.clear()
            return count

    def stats(self) -> dict:
        with self._lock:
            now = time.monotonic()
            expired = sum(1 for k, exp in self._expiry.items() if exp <= now)
            return {
                "total_keys": len(self._store),
                "keys_with_ttl": len(self._expiry),
                "expired_pending_cleanup": expired,
            }

    def stop(self):
        self._stop_event.set()
        self._cleanup_thread.join(timeout=2)
        logger.info("KVEngine stopped")

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _is_expired(self, key: str) -> bool:
        exp = self._expiry.get(key)
        return exp is not None and time.monotonic() > exp

    def _delete_internal(self, key: str):
        self._store.pop(key, None)
        self._expiry.pop(key, None)

    def _cleanup_loop(self):
        """Background thread: actively evict expired keys every N seconds."""
        while not self._stop_event.wait(CLEANUP_INTERVAL):
            with self._lock:
                now = time.monotonic()
                expired_keys = [k for k, exp in self._expiry.items() if exp <= now]
                for k in expired_keys:
                    self._delete_internal(k)
            if expired_keys:
                logger.debug("Cleanup evicted %d expired keys", len(expired_keys))
