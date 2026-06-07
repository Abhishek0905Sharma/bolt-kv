# Bolt — Distributed Key-Value Store
**Live API:** https://abhi-bolt-kv-production.up.railway.app


A Redis-inspired distributed KV store built from scratch in Python.
Demonstrates distributed systems fundamentals: consistent hashing,
Raft consensus, WAL durability, LRU caching, and benchmarking.

**Built to target Google L3/L4 SWE roles.**

## Quick Start

### Single node
```bash
python server.py
python client.py
kvstore> PUT name Abhishek
kvstore> GET name
```

### 3-node cluster with Raft
```bash
# Terminal 1, 2, 3:
python raft_node.py --id A --port 6379
python raft_node.py --id B --port 6380
python raft_node.py --id C --port 6381

# Terminal 4:
python cluster_client.py
bolt-cluster> PUT name Abhishek   # routes to Raft leader
bolt-cluster> STATS               # shows raft_state=leader on one node
```

### Docker (one command)
```bash
docker-compose up --build
python cluster_client.py
```

## Benchmark Results

| Benchmark | Throughput | p50 | p99 |
|---|---|---|---|
| Sequential WRITEs | 937 ops/sec | 1.01ms | 1.76ms |
| Sequential READs | 5,199 ops/sec | 0.17ms | 0.36ms |
| HOT READs (LRU cache) | 4,813 ops/sec | 0.19ms | 0.41ms |
| MIXED (80/20) | 2,537 ops/sec | 0.21ms | 1.23ms |
| CONCURRENT (10 clients) | 1,025 ops/sec | 9.56ms | 26.08ms |

Run benchmarks: `python benchmark.py --ops 1000`

## Architecture

```
cluster_client.py  ──TCP──►  raft_node.py (asyncio)
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
                 engine.py    raft.py      lru_cache.py
                    │
                 wal.py  ──► segment_0000.wal
```

## Project Structure

| File | Purpose |
|---|---|
| `engine.py` | In-memory KV engine, TTL expiry |
| `wal.py` | Write-Ahead Log, crash durability |
| `server.py` | Single-node asyncio TCP server |
| `raft.py` | Raft consensus (leader election + replication) |
| `raft_node.py` | Raft-enabled node server |
| `ring.py` | Consistent hash ring (150 virtual nodes) |
| `lru_cache.py` | LRU cache layer (write-through) |
| `cluster_client.py` | Smart client with leader detection |
| `benchmark.py` | Throughput + latency benchmarks |
| `Dockerfile` | Container image |
| `docker-compose.yml` | 3-node cluster orchestration |

## Architecture Decision Records

### ADR-1: asyncio over threads
asyncio handles thousands of concurrent connections with one thread.
No thread-per-connection overhead. Engine uses threading.RLock for
the background cleanup thread.

### ADR-2: WAL with fsync on every write
Guarantees no data loss on crash. Trade-off: ~1k writes/sec vs ~100k
without fsync. Group commit would improve throughput at cost of a small
data-loss window.

### ADR-3: Consistent hashing over modulo
With modulo (key % n), removing one node reshuffles ~100% of keys.
With consistent hashing, only ~1/n keys move. At 1M keys and 10 nodes,
that's 100k moves vs 1M.

### ADR-4: Raft over Paxos
Raft is designed for understandability. Same safety guarantees as
Paxos but with clearer separation of leader election, log replication,
and safety. Easier to implement correctly.

### ADR-5: Write-through LRU cache
Write-back (cache only, flush later) is faster but risks data loss.
Since WAL already handles durability, write-through is correct.
Cache capacity: 1000 keys (~100KB at 100 bytes/entry).

### ADR-6: JSON for WAL encoding
Human-readable — `cat segment_0000.wal` shows all mutations directly.
Trade-off: ~2-3x larger than msgpack. Acceptable for development;
switch to msgpack for production.

## Roadmap

- [x] Phase 1: Single-node KV store (engine, WAL, TCP server)
- [x] Phase 2: Consistent hashing + 3-node cluster
- [x] Phase 3: Raft leader election + log replication
- [x] Phase 4: LRU cache + benchmarking
- [x] Phase 5: Docker + GCP deployment guide
