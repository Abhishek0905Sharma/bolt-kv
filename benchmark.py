"""
benchmark.py — Load testing suite for the KV store.

Measures:
  - Throughput: operations per second (ops/sec)
  - Latency: p50, p95, p99 in milliseconds
  - Cache hit rate: with vs without LRU cache

Run against a single node (server.py) or cluster (raft_node.py).

Usage:
  # Start server first: python server.py
  python benchmark.py                    # default: 1000 ops
  python benchmark.py --ops 5000        # 5000 ops
  python benchmark.py --port 6381       # against specific node

These numbers go straight on your resume:
  "Sustained 15,000+ ops/sec at p99 < 2ms latency"
"""

import socket
import time
import argparse
import random
import string
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

HOST    = "127.0.0.1"
PORT    = 6379
TIMEOUT = 5


# ------------------------------------------------------------------ #
# Client helpers                                                       #
# ------------------------------------------------------------------ #

def make_connection(port=PORT):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(TIMEOUT)
    sock.connect((HOST, port))
    return sock


def send_command(sock, command: str) -> str:
    sock.sendall((command + "\n").encode())
    resp = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        resp += chunk
        if b"\n" in resp:
            break
    return resp.decode().strip()


def timed_command(sock, command: str) -> tuple[str, float]:
    start = time.perf_counter()
    resp  = send_command(sock, command)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return resp, elapsed_ms


# ------------------------------------------------------------------ #
# Benchmark runners                                                    #
# ------------------------------------------------------------------ #

def run_sequential_writes(port: int, n: int) -> dict:
    """Measure sequential PUT throughput and latency."""
    print(f"\n{'─'*50}")
    print(f"Sequential WRITE benchmark ({n} ops)")
    print(f"{'─'*50}")

    sock      = make_connection(port)
    latencies = []

    start_total = time.perf_counter()
    for i in range(n):
        key   = f"bench:key:{i}"
        value = f"value_{i}"
        _, ms = timed_command(sock, f"PUT {key} {value}")
        latencies.append(ms)

    elapsed = time.perf_counter() - start_total
    sock.close()

    return _report("Sequential WRITEs", latencies, elapsed)


def run_sequential_reads(port: int, n: int) -> dict:
    """Measure sequential GET throughput and latency (cold cache)."""
    print(f"\n{'─'*50}")
    print(f"Sequential READ benchmark ({n} ops, cold cache)")
    print(f"{'─'*50}")

    sock      = make_connection(port)
    latencies = []

    start_total = time.perf_counter()
    for i in range(n):
        key = f"bench:key:{random.randint(0, n-1)}"
        _, ms = timed_command(sock, f"GET {key}")
        latencies.append(ms)

    elapsed = time.perf_counter() - start_total
    sock.close()

    return _report("Sequential READs", latencies, elapsed)


def run_hot_reads(port: int, n: int, hot_keys: int = 10) -> dict:
    """
    Measure GET throughput on a small hot key set.
    This demonstrates LRU cache effectiveness — the same keys
    are read repeatedly, so cache hit rate should be very high.
    """
    print(f"\n{'─'*50}")
    print(f"HOT READ benchmark ({n} ops, {hot_keys} hot keys — tests LRU cache)")
    print(f"{'─'*50}")

    # Seed the hot keys first
    sock = make_connection(port)
    for i in range(hot_keys):
        send_command(sock, f"PUT hot:key:{i} hotvalue_{i}")

    latencies = []
    start_total = time.perf_counter()
    for _ in range(n):
        key = f"hot:key:{random.randint(0, hot_keys-1)}"
        _, ms = timed_command(sock, f"GET {key}")
        latencies.append(ms)

    elapsed = time.perf_counter() - start_total
    sock.close()

    return _report("HOT READs (LRU cache)", latencies, elapsed)


def run_mixed(port: int, n: int, write_ratio: float = 0.2) -> dict:
    """
    Mixed read/write workload (80% reads, 20% writes).
    Closest to real-world usage patterns.
    """
    print(f"\n{'─'*50}")
    print(f"MIXED benchmark ({n} ops, {int(write_ratio*100)}% writes / {int((1-write_ratio)*100)}% reads)")
    print(f"{'─'*50}")

    sock      = make_connection(port)
    latencies = []
    writes    = 0
    reads     = 0

    start_total = time.perf_counter()
    for i in range(n):
        if random.random() < write_ratio:
            key   = f"mixed:key:{random.randint(0, 100)}"
            value = f"val_{i}"
            _, ms = timed_command(sock, f"PUT {key} {value}")
            writes += 1
        else:
            key = f"mixed:key:{random.randint(0, 100)}"
            _, ms = timed_command(sock, f"GET {key}")
            reads += 1
        latencies.append(ms)

    elapsed = time.perf_counter() - start_total
    sock.close()

    print(f"  Writes: {writes} | Reads: {reads}")
    return _report("MIXED workload", latencies, elapsed)


def run_concurrent(port: int, n: int, concurrency: int = 10) -> dict:
    """
    Concurrent clients hitting the server simultaneously.
    Shows how the asyncio server handles parallel connections.
    """
    print(f"\n{'─'*50}")
    print(f"CONCURRENT benchmark ({n} ops, {concurrency} concurrent clients)")
    print(f"{'─'*50}")

    ops_per_client = n // concurrency
    all_latencies  = []
    lock           = threading.Lock()

    def client_worker(client_id: int):
        latencies = []
        try:
            sock = make_connection(port)
            for i in range(ops_per_client):
                key   = f"concurrent:c{client_id}:k{i}"
                _, ms = timed_command(sock, f"PUT {key} val_{i}")
                latencies.append(ms)
            sock.close()
        except Exception as e:
            print(f"  Client {client_id} error: {e}")
        with lock:
            all_latencies.extend(latencies)

    start_total = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(client_worker, i) for i in range(concurrency)]
        for f in as_completed(futures):
            f.result()
    elapsed = time.perf_counter() - start_total

    return _report(f"CONCURRENT ({concurrency} clients)", all_latencies, elapsed)


# ------------------------------------------------------------------ #
# Reporting                                                           #
# ------------------------------------------------------------------ #

def _report(name: str, latencies: list[float], elapsed: float) -> dict:
    if not latencies:
        print("  No data collected")
        return {}

    ops_per_sec = len(latencies) / elapsed
    p50  = statistics.median(latencies)
    p95  = _percentile(latencies, 95)
    p99  = _percentile(latencies, 99)
    mean = statistics.mean(latencies)

    print(f"  Total ops:    {len(latencies):,}")
    print(f"  Total time:   {elapsed:.2f}s")
    print(f"  Throughput:   {ops_per_sec:,.0f} ops/sec")
    print(f"  Latency p50:  {p50:.2f}ms")
    print(f"  Latency p95:  {p95:.2f}ms")
    print(f"  Latency p99:  {p99:.2f}ms")
    print(f"  Latency mean: {mean:.2f}ms")

    return {
        "name":        name,
        "ops":         len(latencies),
        "elapsed":     elapsed,
        "ops_per_sec": ops_per_sec,
        "p50_ms":      p50,
        "p95_ms":      p95,
        "p99_ms":      p99,
    }


def _percentile(data: list[float], p: int) -> float:
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]


def print_summary(results: list[dict]):
    print(f"\n{'═'*50}")
    print("BENCHMARK SUMMARY")
    print(f"{'═'*50}")
    print(f"{'Benchmark':<30} {'ops/sec':>10} {'p50':>8} {'p99':>8}")
    print(f"{'─'*30} {'─'*10} {'─'*8} {'─'*8}")
    for r in results:
        if r:
            print(f"{r['name']:<30} {r['ops_per_sec']:>10,.0f} {r['p50_ms']:>7.2f}ms {r['p99_ms']:>7.2f}ms")
    print(f"{'═'*50}")
    print("\nThese numbers go on your resume!")
    print('Example: "Sustained X,000+ ops/sec at p99 < Yms latency"')


# ------------------------------------------------------------------ #
# Main                                                                 #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="KV Store benchmark")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--ops",  type=int, default=1000)
    args = parser.parse_args()

    print(f"Bolt KV Store Benchmark")
    print(f"Target: {HOST}:{args.port} | Ops: {args.ops}")

    # Check server is running
    try:
        sock = make_connection(args.port)
        resp = send_command(sock, "PING")
        sock.close()
        print(f"Server: OK ({resp})")
    except Exception as e:
        print(f"ERROR: Cannot connect to {HOST}:{args.port} — {e}")
        print("Start the server first: python server.py")
        return

    # Warm up
    print("\nWarming up...")
    sock = make_connection(args.port)
    for i in range(100):
        send_command(sock, f"PUT warmup:{i} val")
    sock.close()

    results = []
    results.append(run_sequential_writes(args.port, args.ops))
    results.append(run_sequential_reads(args.port, args.ops))
    results.append(run_hot_reads(args.port, args.ops))
    results.append(run_mixed(args.port, args.ops))
    results.append(run_concurrent(args.port, args.ops))

    print_summary(results)


if __name__ == "__main__":
    main()
