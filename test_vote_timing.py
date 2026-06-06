"""
Run this while all 3 raft_nodes are running.
Simulates exactly what happens during election - sends vote to B and C simultaneously.
"""
import asyncio
import json
import time

async def send_vote(port):
    start = time.time()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection('127.0.0.1', port), timeout=2
        )
        msg = json.dumps({"type":"request_vote","term":9998,"candidate_id":"A",
                         "last_log_index":-1,"last_log_term":0})
        writer.write(("RAFT_RPC " + msg + "\n").encode())
        await writer.drain()
        resp = await asyncio.wait_for(reader.readline(), timeout=2)
        elapsed = time.time() - start
        result = json.loads(resp.decode().strip())
        print(f"Port {port}: {result} in {elapsed*1000:.0f}ms")
        writer.close()
        return result
    except Exception as e:
        elapsed = time.time() - start
        print(f"Port {port}: FAILED after {elapsed*1000:.0f}ms - {e}")
        return None

async def main():
    print("Testing vote request timing to ports 6380 and 6381...")
    results = await asyncio.gather(send_vote(6380), send_vote(6381))
    votes = sum(1 for r in results if r and r.get("vote_granted"))
    print(f"\nVotes received: {votes}/2 (need 1 to win with self-vote)")

asyncio.run(main())
