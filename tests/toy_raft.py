"""A toy Raft leader election, written like an ordinary asyncio program.

This is the canonical DST demo (turmoil and madsim both use it): nodes talk
JSON-over-streams through the simulated network, persist term/votedFor to
their host disks (with fsync, as Raft requires), and the torture suite in
test_raft_demo.py throws partitions, crashes, restarts, and resets at them.

The election-safety invariant: at most one leader per term. ``buggy=True``
plants the classic bug — granting votes without checking votedFor — which
lets two candidates win the same term under the right interleaving.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import simloom
from simloom import Host, World

PORT = 4000
RPC_TIMEOUT = 0.25
HEARTBEAT_EVERY = 0.05


class RaftNode:
    def __init__(
        self,
        host: Host,
        name: str,
        peers: list[str],
        leaders_by_term: dict[int, set[str]],
        *,
        buggy: bool,
    ) -> None:
        self.host = host
        self.name = name
        self.peers = peers
        self.leaders_by_term = leaders_by_term
        self.buggy = buggy
        self.role = "follower"
        self.heard = asyncio.Event()
        # Raft requires term/votedFor to be durable before acting on them.
        if host.disk.exists("raft-state"):
            state = json.loads(host.disk.read("raft-state"))
            self.term = int(state["term"])
            self.voted_for = state["voted_for"]
        else:
            self.term = 0
            self.voted_for: str | None = None

    def _persist(self) -> None:
        self.host.disk.write(
            "raft-state",
            json.dumps({"term": self.term, "voted_for": self.voted_for}).encode(),
        )
        self.host.disk.fsync("raft-state")

    # --- the node's main loop ---

    async def run(self) -> None:
        await asyncio.start_server(self.handle, f"{self.name}.raft", PORT)
        while True:
            timeout = 0.15 + simloom.draw(f"raft.timeout.{self.name}", 100) * 0.003
            self.heard.clear()
            try:
                async with asyncio.timeout(timeout):
                    await self.heard.wait()
            except TimeoutError:
                if self.role != "leader":
                    await self.campaign()

    # --- RPC server side ---

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            with contextlib.suppress(ConnectionResetError, json.JSONDecodeError):
                line = await reader.readline()
                if line:
                    response = self.on_rpc(json.loads(line))
                    writer.write((json.dumps(response) + "\n").encode())
                    await writer.drain()
        finally:
            writer.close()

    def on_rpc(self, request: dict[str, Any]) -> dict[str, Any]:
        if request["term"] > self.term:
            self.term = int(request["term"])
            self.voted_for = None
            self.role = "follower"
            self._persist()
        if request["type"] == "vote":
            if self.buggy:
                # THE PLANTED BUG: votedFor is never consulted, so one node
                # can grant two candidates the same term.
                granted = request["term"] >= self.term
            else:
                granted = request["term"] == self.term and self.voted_for in (
                    None,
                    request["from"],
                )
            if granted:
                self.voted_for = request["from"]
                self._persist()
                self.heard.set()
            return {"granted": granted, "term": self.term}
        # heartbeat
        if request["term"] >= self.term:
            if request["from"] != self.name:
                self.role = "follower"
            self.heard.set()
            return {"ok": True, "term": self.term}
        return {"ok": False, "term": self.term}

    # --- RPC client side ---

    async def rpc(self, peer: str, message: dict[str, Any]) -> dict[str, Any] | None:
        writer = None
        try:
            async with asyncio.timeout(RPC_TIMEOUT):
                reader, writer = await asyncio.open_connection(f"{peer}.raft", PORT)
                writer.write((json.dumps(message) + "\n").encode())
                await writer.drain()
                line = await reader.readline()
                return json.loads(line) if line else None
        except (TimeoutError, OSError, json.JSONDecodeError):
            simloom.reached("raft.rpc_failed")
            return None
        finally:
            if writer is not None:
                writer.close()

    # --- elections ---

    async def campaign(self) -> None:
        self.term += 1
        self.role = "candidate"
        self.voted_for = self.name
        self._persist()
        term = self.term
        request = {"type": "vote", "term": term, "from": self.name}
        replies = await asyncio.gather(*(self.rpc(p, request) for p in self.peers))
        votes = 1
        for reply in replies:
            if reply is None:
                continue
            if reply.get("term", 0) > self.term:
                self.term = int(reply["term"])
                self.role = "follower"
                self.voted_for = None
                self._persist()
                return
            if reply.get("granted"):
                votes += 1
        majority = (len(self.peers) + 1) // 2 + 1
        if self.role == "candidate" and self.term == term and votes >= majority:
            self.role = "leader"
            self.leaders_by_term.setdefault(term, set()).add(self.name)
            simloom.reached("raft.leader_elected")
            asyncio.get_running_loop().create_task(self.lead(term))

    async def lead(self, term: int) -> None:
        while self.role == "leader" and self.term == term:
            message = {"type": "heartbeat", "term": term, "from": self.name}
            await asyncio.gather(*(self.rpc(p, message) for p in self.peers))
            await asyncio.sleep(HEARTBEAT_EVERY)


async def chaos(world: World, hosts: list[Host], *, rounds: int, allow_crash: bool) -> None:
    """Tape-driven torture: partitions, heals, crashes, restarts, resets."""
    for _ in range(rounds):
        await world.sleep(0.3 + simloom.draw("chaos.delay", 50) * 0.01)
        action = simloom.draw("chaos.action", 10)
        if action < 4:
            k = 1 + simloom.draw("chaos.minority", 2)
            picked = sorted({simloom.draw("chaos.victim", len(hosts)) for _ in range(k)})
            minority = [hosts[i] for i in picked]
            majority = [h for h in hosts if h not in minority]
            world.net.partition(minority, majority)
            simloom.reached("chaos.partition")
        elif action < 6:
            world.net.heal()
        elif action < 8 and allow_crash:
            victim = hosts[simloom.draw("chaos.crash_victim", len(hosts))]
            if not victim.crashed:
                victim.crash()
                simloom.reached("chaos.crash")
        else:
            crashed = [h for h in hosts if h.crashed]
            if crashed:
                crashed[0].restart()
                simloom.reached("chaos.restart")
            else:
                a = hosts[simloom.draw("chaos.reset_a", len(hosts))]
                b = hosts[simloom.draw("chaos.reset_b", len(hosts))]
                if a is not b:
                    world.net.reset_connections(a, b)
                    simloom.reached("chaos.reset")
    # Let the cluster converge: full connectivity, everyone running.
    world.net.heal()
    for host in hosts:
        if host.crashed:
            host.restart()
    await world.sleep(3)


def raft_world(*, buggy: bool, nodes: int = 5, rounds: int = 6, allow_crash: bool = True) -> Any:
    """Build a `main(world)` for simloom.run: a Raft cluster under torture.

    Returns the leaders-per-term map; election safety holds iff every term
    has at most one leader.
    """

    async def main(world: World) -> dict[int, list[str]]:
        leaders_by_term: dict[int, set[str]] = {}
        names = [f"n{i}" for i in range(nodes)]
        world.net.set_loss(5)

        def entry(name: str) -> Any:
            host = world.host(name)
            peers = [p for p in names if p != name]

            def factory() -> Any:
                node = RaftNode(host, name, peers, leaders_by_term, buggy=buggy)
                return node.run()

            return factory

        for name in names:
            world.host(name).spawn(entry(name))

        await world.sleep(2.0)  # first elections
        await chaos(world, world.hosts, rounds=rounds, allow_crash=allow_crash)
        return {term: sorted(who) for term, who in sorted(leaders_by_term.items())}

    return main
