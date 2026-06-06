"""
tests.py — Unit tests for KVEngine and WAL.

Run with:  python -m pytest tests.py -v
       or: python tests.py
"""

import time
import tempfile
import unittest
from pathlib import Path

from engine import KVEngine
from wal import WAL


class TestKVEngine(unittest.TestCase):

    def setUp(self):
        self.engine = KVEngine(wal=None)  # no WAL for unit tests

    def tearDown(self):
        self.engine.stop()

    def test_put_and_get(self):
        self.engine.put("name", "Alice")
        self.assertEqual(self.engine.get("name"), "Alice")

    def test_get_missing_key(self):
        self.assertIsNone(self.engine.get("nonexistent"))

    def test_delete(self):
        self.engine.put("k", "v")
        self.assertTrue(self.engine.delete("k"))
        self.assertIsNone(self.engine.get("k"))

    def test_delete_missing(self):
        self.assertFalse(self.engine.delete("ghost"))

    def test_exists(self):
        self.engine.put("x", "1")
        self.assertTrue(self.engine.exists("x"))
        self.assertFalse(self.engine.exists("y"))

    def test_ttl_expiry_lazy(self):
        self.engine.put("temp", "value", ttl=0.05)  # 50ms TTL
        self.assertEqual(self.engine.get("temp"), "value")
        time.sleep(0.1)
        self.assertIsNone(self.engine.get("temp"))  # lazy expiry on GET

    def test_ttl_key_not_in_keys(self):
        self.engine.put("short", "lived", ttl=0.05)
        time.sleep(0.1)
        self.assertNotIn("short", self.engine.keys())

    def test_flush(self):
        self.engine.put("a", "1")
        self.engine.put("b", "2")
        count = self.engine.flush()
        self.assertEqual(count, 2)
        self.assertEqual(self.engine.keys(), [])

    def test_overwrite(self):
        self.engine.put("k", "old")
        self.engine.put("k", "new")
        self.assertEqual(self.engine.get("k"), "new")

    def test_ttl_cleared_on_overwrite(self):
        self.engine.put("k", "v", ttl=0.05)
        self.engine.put("k", "v2")  # no TTL this time
        time.sleep(0.1)
        self.assertEqual(self.engine.get("k"), "v2")  # should NOT have expired

    def test_stats(self):
        self.engine.put("a", "1", ttl=60)
        self.engine.put("b", "2")
        stats = self.engine.stats()
        self.assertEqual(stats["total_keys"], 2)
        self.assertEqual(stats["keys_with_ttl"], 1)


class TestWAL(unittest.TestCase):

    def test_replay_restores_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = Path(tmpdir)

            # Write some entries
            wal = WAL(wal_dir=wal_path)
            engine1 = KVEngine(wal=wal)
            engine1.put("city", "Hyderabad")
            engine1.put("lang", "Python")
            engine1.delete("lang")
            engine1.stop()
            wal.close()

            # Replay into a fresh engine
            wal2 = WAL(wal_dir=wal_path)
            engine2 = KVEngine(wal=None)
            replayed = wal2.replay(engine2)

            self.assertGreaterEqual(replayed, 2)
            self.assertEqual(engine2.get("city"), "Hyderabad")
            self.assertIsNone(engine2.get("lang"))  # was deleted
            engine2.stop()
            wal2.close()

    def test_flush_replayed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = Path(tmpdir)
            wal = WAL(wal_dir=wal_path)
            engine1 = KVEngine(wal=wal)
            engine1.put("a", "1")
            engine1.flush()
            engine1.stop()
            wal.close()

            wal2 = WAL(wal_dir=wal_path)
            engine2 = KVEngine(wal=None)
            wal2.replay(engine2)
            self.assertEqual(engine2.keys(), [])
            engine2.stop()
            wal2.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
