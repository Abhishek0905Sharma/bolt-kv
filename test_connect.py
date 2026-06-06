"""
test_connect.py - Test if nodes can reach each other.
Run this while all 3 raft_nodes are running.
"""
import asyncio
import json

async def test():
    ports = [6379, 6380, 6381]
    for port in ports:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', port), timeout=2
            )
            # Send a PING
            writer.write(b'PING\n')
            await writer.drain()
            resp = await asyncio.wait_for(reader.readline(), timeout=2)
            print(f'Port {port}: OK - {resp.decode().strip()}')
            writer.close()
            
            # Now test RAFT_RPC
            reader2, writer2 = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', port), timeout=2
            )
            msg = json.dumps({'type':'request_vote','term':99,'candidate_id':'TEST',
                            'last_log_index':-1,'last_log_term':0})
            writer2.write(('RAFT_RPC ' + msg + '\n').encode())
            await writer2.drain()
            resp2 = await asyncio.wait_for(reader2.readline(), timeout=2)
            print(f'Port {port} RAFT_RPC: {resp2.decode().strip()}')
            writer2.close()
        except Exception as e:
            print(f'Port {port}: FAILED - {e}')

asyncio.run(test())
