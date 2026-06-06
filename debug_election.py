"""
debug_election.py - Simulates exactly what happens during a Raft election
Run this while all 3 raft_nodes are running to see the actual vote responses.
"""
import asyncio
import json

HOST = "127.0.0.1"
PORTS = [6379, 6380, 6381]

async def send_vote_request(port, term):
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(HOST, port), timeout=2
        )
        msg = {
            "type": "request_vote",
            "term": term,
            "candidate_id": "DEBUG",
            "last_log_index": -1,
            "last_log_term": 0
        }
        writer.write(("RAFT_RPC " + json.dumps(msg) + "\n").encode())
        await writer.drain()
        resp_line = await asyncio.wait_for(reader.readline(), timeout=2)
        writer.close()
        resp = json.loads(resp_line.decode().strip())
        print(f"Port {port} response: {resp}")
        print(f"  vote_granted = {resp.get('vote_granted')}")
        print(f"  term = {resp.get('term')}")
        return resp
    except Exception as e:
        print(f"Port {port} ERROR: {e}")
        return None

async def main():
    print("Sending vote requests to all nodes...")
    print("="*50)
    results = await asyncio.gather(*[send_vote_request(p, 9999) for p in PORTS])
    votes = sum(1 for r in results if r and r.get("vote_granted"))
    print("="*50)
    print(f"Total votes granted: {votes}/3")
    majority = 2
    print(f"Majority needed: {majority}")
    print(f"Would win election: {votes >= majority}")

asyncio.run(main())
