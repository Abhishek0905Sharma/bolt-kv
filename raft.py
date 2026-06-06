"""
raft.py — Simplified Raft with extensive debug logging to diagnose election issues.
"""

import asyncio
import random
import logging
import json
import time
from enum import Enum
from typing import Optional

logger = logging.getLogger("kvstore.raft")

ELECTION_TIMEOUT_MIN = 2.0
ELECTION_TIMEOUT_MAX = 4.0
HEARTBEAT_INTERVAL   = 0.5
HOST = "127.0.0.1"


class RaftState(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


class LogEntry:
    def __init__(self, term: int, command: str):
        self.term    = term
        self.command = command

    def __repr__(self):
        return f"LogEntry(term={self.term}, cmd={self.command!r})"


class RaftNode:
    def __init__(self, node_id: str, peers: list, apply_callback):
        self.node_id      = node_id
        self.peers        = peers
        self._apply       = apply_callback
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: list[LogEntry] = []
        self.commit_index = -1
        self.last_applied = -1
        self.next_index:  dict = {}
        self.match_index: dict = {}
        self.state     = RaftState.FOLLOWER
        self.leader_id: Optional[str] = None
        self._election_task:  Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        logger.info("[%s] RaftNode initialised, peers=%s", node_id, peers)

    async def submit(self, command: str) -> bool:
        if self.state != RaftState.LEADER:
            return False
        entry = LogEntry(self.current_term, command)
        self.log.append(entry)
        idx = len(self.log) - 1
        committed = await self._replicate_and_commit(idx)
        if committed:
            await self._apply_up_to(idx)
        return committed

    def is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def status(self) -> dict:
        return {
            "node_id":      self.node_id,
            "state":        self.state.value,
            "term":         self.current_term,
            "leader":       self.leader_id,
            "log_length":   len(self.log),
            "commit_index": self.commit_index,
        }

    async def handle_request_vote(self, msg: dict) -> dict:
        term         = msg["term"]
        candidate_id = msg["candidate_id"]
        last_log_idx = msg["last_log_index"]
        last_log_term= msg["last_log_term"]

        if term > self.current_term:
            self._become_follower(term)

        vote_granted = False
        if (term >= self.current_term
                and (self.voted_for is None or self.voted_for == candidate_id)
                and self._candidate_log_ok(last_log_idx, last_log_term)):
            self.voted_for = candidate_id
            vote_granted   = True
            self._reset_timer()
            logger.info("[%s] GRANTED vote to %s term=%d", self.node_id, candidate_id, term)
        else:
            logger.info("[%s] DENIED vote to %s term=%d (my_term=%d voted_for=%s)",
                        self.node_id, candidate_id, term, self.current_term, self.voted_for)

        return {"term": self.current_term, "vote_granted": vote_granted}

    async def handle_append_entries(self, msg: dict) -> dict:
        term          = msg["term"]
        leader_id     = msg["leader_id"]
        entries       = msg.get("entries", [])
        leader_commit = msg.get("leader_commit", -1)

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        if term > self.current_term or self.state == RaftState.CANDIDATE:
            self._become_follower(term)

        self.leader_id = leader_id
        self._reset_timer()

        for e in entries:
            self.log.append(LogEntry(e["term"], e["command"]))

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            await self._apply_up_to(self.commit_index)

        return {"term": self.current_term, "success": True}

    def _reset_timer(self):
        if self._election_task and not self._election_task.done():
            self._election_task.cancel()
        try:
            loop = asyncio.get_running_loop()
            self._election_task = loop.create_task(self._election_timeout())
        except RuntimeError:
            pass

    async def _election_timeout(self):
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        await asyncio.sleep(timeout)
        if self.state != RaftState.LEADER:
            # Run election as independent task so it doesn't block
            # incoming vote requests from other nodes (deadlock prevention)
            asyncio.ensure_future(self._start_election())

    async def _start_election(self):
        self.current_term += 1
        self.state         = RaftState.CANDIDATE
        self.voted_for     = self.node_id
        self.leader_id     = None
        logger.info("[%s] === Starting election term=%d peers=%s ===",
                    self.node_id, self.current_term, self.peers)
        # Cancel election timer for duration of this election
        # We'll reset it only if we don't win
        if self._election_task and not self._election_task.done():
            self._election_task.cancel()

        last_idx  = len(self.log) - 1
        last_term = self.log[last_idx].term if self.log else 0

        msg = {
            "type":           "request_vote",
            "term":           self.current_term,
            "candidate_id":   self.node_id,
            "last_log_index": last_idx,
            "last_log_term":  last_term,
        }

        # Send to each peer individually with full logging
        vote_count = 1  # self vote
        for peer in self.peers:
            if self.state != RaftState.CANDIDATE:
                logger.info("[%s] No longer candidate, stopping election", self.node_id)
                return
            logger.info("[%s] Sending vote request to %s", self.node_id, peer)
            resp = await self._send_rpc(peer, msg)
            logger.info("[%s] Vote response from %s: %s", self.node_id, peer, resp)
            if resp is None:
                logger.warning("[%s] No response from %s", self.node_id, peer)
                continue
            if resp.get("term", 0) > self.current_term:
                logger.info("[%s] Higher term seen, stepping down", self.node_id)
                self._become_follower(resp["term"])
                return
            if resp.get("vote_granted"):
                vote_count += 1
                logger.info("[%s] Vote count now %d/%d", self.node_id, vote_count, len(self.peers)+1)

        majority = (len(self.peers) + 1) // 2 + 1
        logger.info("[%s] Election done: %d votes, need %d, state=%s",
                    self.node_id, vote_count, majority, self.state.value)
        if vote_count >= majority and self.state == RaftState.CANDIDATE:
            await self._become_leader()
        elif self.state == RaftState.CANDIDATE:
            # Lost election — start new timer to try again
            self._reset_timer()

    async def _become_leader(self):
        self.state     = RaftState.LEADER
        self.leader_id = self.node_id
        logger.info("[%s] *** BECAME LEADER term=%d ***", self.node_id, self.current_term)
        for peer in self.peers:
            self.next_index[peer]  = len(self.log)
            self.match_index[peer] = -1
        if self._election_task and not self._election_task.done():
            self._election_task.cancel()
        self._heartbeat_task = asyncio.get_running_loop().create_task(
            self._heartbeat_loop()
        )

    def _become_follower(self, term: int):
        logger.info("[%s] Becoming FOLLOWER term=%d", self.node_id, term)
        self.state        = RaftState.FOLLOWER
        self.current_term = term
        self.voted_for    = None
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._reset_timer()

    async def _heartbeat_loop(self):
        while self.state == RaftState.LEADER:
            await self._send_heartbeats()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _send_heartbeats(self):
        msg = {
            "type":          "append_entries",
            "term":          self.current_term,
            "leader_id":     self.node_id,
            "entries":       [],
            "leader_commit": self.commit_index,
        }
        await asyncio.gather(*[self._send_rpc(p, msg) for p in self.peers],
                             return_exceptions=True)

    async def _replicate_and_commit(self, idx: int) -> bool:
        entry = self.log[idx]
        msg = {
            "type":          "append_entries",
            "term":          self.current_term,
            "leader_id":     self.node_id,
            "entries":       [{"term": entry.term, "command": entry.command}],
            "leader_commit": self.commit_index,
        }
        responses = await asyncio.gather(
            *[self._send_rpc(p, msg) for p in self.peers],
            return_exceptions=True
        )
        acks = 1
        for resp in responses:
            if not isinstance(resp, Exception) and resp and resp.get("success"):
                acks += 1
        majority = (len(self.peers) + 1) // 2 + 1
        if acks >= majority:
            self.commit_index = idx
            return True
        return False

    async def _apply_up_to(self, idx: int):
        while self.last_applied < idx:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            await self._apply(entry.command)

    async def _send_rpc(self, peer: tuple, msg: dict) -> Optional[dict]:
        host, port = peer
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=1.5
            )
            data = ("RAFT_RPC " + json.dumps(msg) + "\n").encode()
            writer.write(data)
            await writer.drain()
            resp_line = await asyncio.wait_for(reader.readline(), timeout=1.5)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return json.loads(resp_line.decode().strip())
        except Exception as e:
            logger.warning("[%s] _send_rpc to %s:%d failed: %s",
                           self.node_id, host, port, e)
            return None

    def _candidate_log_ok(self, last_idx: int, last_term: int) -> bool:
        my_last_term = self.log[-1].term if self.log else 0
        my_last_idx  = len(self.log) - 1
        if last_term != my_last_term:
            return last_term > my_last_term
        return last_idx >= my_last_idx
