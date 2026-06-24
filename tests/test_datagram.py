"""Phase I — datagram (UDP) transport: real loss, reorder, and duplication —
the faults a UDP protocol must actually tolerate, unlike the stream model which
only ever delays."""

from __future__ import annotations

import asyncio

import simloom


class Sink(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.received: list[bytes] = []

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.received.append(data)


async def _send(world: simloom.World, n: int, sink: Sink) -> None:
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(lambda: sink, local_addr=("srv", 9000))
    client, _ = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol, remote_addr=("srv", 9000)
    )
    for i in range(n):
        client.sendto(str(i).encode())
    await world.sleep(2.0)


class TestDelivery:
    def test_clean_is_in_order_and_exactly_once(self) -> None:
        async def main(world: simloom.World) -> list[str]:
            sink = Sink()
            await _send(world, 8, sink)
            return [d.decode() for d in sink.received]

        # deterministic, in order, no loss/dup across many seeds
        for seed in range(20):
            assert simloom.run(main, seed=seed).value == [str(i) for i in range(8)]

    def test_loss_drops_packets(self) -> None:
        async def main(world: simloom.World) -> int:
            world.net.set_datagram_loss(50)
            sink = Sink()
            await _send(world, 12, sink)
            return len(sink.received)

        counts = {simloom.run(main, seed=s).value for s in range(10)}
        assert min(counts) < 12  # some seeds lose packets
        assert max(counts) <= 12  # never invents packets

    def test_duplication_delivers_extras(self) -> None:
        async def main(world: simloom.World) -> int:
            world.net.set_datagram_duplication(60)
            sink = Sink()
            await _send(world, 12, sink)
            return len(sink.received)

        assert any(simloom.run(main, seed=s).value > 12 for s in range(10))

    def test_reorder_shuffles(self) -> None:
        async def main(world: simloom.World) -> list[str]:
            world.net.set_datagram_reorder(60)
            sink = Sink()
            await _send(world, 8, sink)
            return [d.decode() for d in sink.received]

        ordered = [str(i) for i in range(8)]
        assert any(simloom.run(main, seed=s).value != ordered for s in range(10))

    def test_no_listener_drops_silently(self) -> None:
        async def main(world: simloom.World) -> str:
            loop = asyncio.get_running_loop()
            client, _ = await loop.create_datagram_endpoint(
                asyncio.DatagramProtocol, remote_addr=("10.0.0.50", 1234)
            )
            client.sendto(b"into the void")  # a valid address, but nothing bound there
            await world.sleep(0.5)
            return "ok"

        assert simloom.run(main, seed=0).value == "ok"  # no crash, packet vanishes

    def test_connected_endpoint_and_replies(self) -> None:
        async def main(world: simloom.World) -> list[bytes]:
            loop = asyncio.get_running_loop()
            replies: list[bytes] = []

            class Server(asyncio.DatagramProtocol):
                def connection_made(self, transport: asyncio.BaseTransport) -> None:
                    self.t = transport

                def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
                    self.t.sendto(b"r:" + data, addr)  # type: ignore[attr-defined]

            class Client(asyncio.DatagramProtocol):
                def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
                    replies.append(data)

            await loop.create_datagram_endpoint(Server, local_addr=("srv", 7000))
            client, _ = await loop.create_datagram_endpoint(Client, remote_addr=("srv", 7000))
            client.sendto(b"hi")  # no addr needed: connected
            await world.sleep(0.5)
            return replies

        assert simloom.run(main, seed=0).value == [b"r:hi"]


class TestGate:
    """A UDP client that assumes in-order exactly-once delivery is found buggy
    under loss/reorder/dup; the failure replays. A correct client survives."""

    @staticmethod
    def naive_sequence_check() -> object:
        async def main(world: simloom.World) -> None:
            world.net.set_datagram_loss(20)
            world.net.set_datagram_duplication(20)
            world.net.set_datagram_reorder(20)
            sink = Sink()
            await _send(world, 10, sink)
            seq = [int(d) for d in sink.received]
            # the bug: assume every packet arrives once, in order
            assert seq == list(range(10)), f"out-of-spec delivery: {seq}"

        return main

    def test_naive_client_is_found_buggy_and_replays(self) -> None:
        exploration = simloom.explore(self.naive_sequence_check(), runs=50)
        assert exploration.failed
        failure = exploration.first_failure
        assert failure is not None
        replay = simloom.replay(self.naive_sequence_check(), tape=failure, raise_on_error=False)
        assert replay.outcome == "error"
        assert replay.digest == failure.digest

    def test_correct_client_tolerates_the_faults(self) -> None:
        async def main(world: simloom.World) -> None:
            world.net.set_datagram_loss(20)
            world.net.set_datagram_duplication(20)
            world.net.set_datagram_reorder(20)
            sink = Sink()
            await _send(world, 10, sink)
            # correct: dedup + ignore order + tolerate loss (a real protocol)
            seen = {int(d) for d in sink.received}
            assert seen <= set(range(10))  # never invents, order-independent

        assert not simloom.explore(main, runs=50).failed
