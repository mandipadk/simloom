"""Stand-in services: in-sim fakes for the dependencies real applications talk
to, so you test *your* logic against a realistic, fault-injected dependency
without standing one up.

The first is **sim-redis**: a RESP (REdis Serialization Protocol) server good
enough that an unmodified ``redis.asyncio.Redis`` client speaks to it —
``SET``/``GET``/``DEL``, and the ``WATCH``/``MULTI``/``EXEC`` optimistic-locking
transaction (a watched key changing aborts the EXEC). It runs as an ordinary
in-sim server, so every ``world.net`` fault — latency, loss, partitions,
resets — applies to the wire between the client and it.
"""

from __future__ import annotations

import asyncio


def _simple(text: bytes) -> bytes:
    return b"+" + text + b"\r\n"


def _error(text: bytes) -> bytes:
    return b"-" + text + b"\r\n"


def _integer(value: int) -> bytes:
    return b":" + str(value).encode() + b"\r\n"


def _bulk(data: bytes | None) -> bytes:
    if data is None:
        return b"$-1\r\n"
    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


def _array(items: list[bytes] | None) -> bytes:
    if items is None:
        return b"*-1\r\n"
    return b"*" + str(len(items)).encode() + b"\r\n" + b"".join(items)


async def _read_command(reader: asyncio.StreamReader) -> list[bytes] | None:
    """Read one RESP command (an array of bulk strings), or None on EOF/reset."""
    try:
        line = await reader.readline()
        if not line:
            return None
        if not line.startswith(b"*"):  # inline command
            return line.split()
        count = int(line[1:-2])
        args: list[bytes] = []
        for _ in range(count):
            header = await reader.readline()
            length = int(header[1:-2])
            args.append(await reader.readexactly(length))
            await reader.readexactly(2)  # trailing CRLF
        return args
    except (asyncio.IncompleteReadError, ConnectionError, ValueError):
        return None


class _Connection:
    __slots__ = ("queue", "watched")

    def __init__(self) -> None:
        self.watched: dict[bytes, int] = {}  # key -> version observed at WATCH
        self.queue: list[list[bytes]] | None = None  # commands buffered inside MULTI


class SimRedis:
    """An in-sim RESP server. Point an unmodified ``redis.asyncio.Redis`` at the
    host/port it serves on (via ``world.run_service``)."""

    def __init__(self) -> None:
        self._store: dict[bytes, bytes] = {}
        self._version: dict[bytes, int] = {}

    def _bump(self, key: bytes) -> None:
        self._version[key] = self._version.get(key, 0) + 1

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = _Connection()
        while True:
            command = await _read_command(reader)
            if command is None:
                break
            name = command[0].upper()
            if conn.queue is not None and name not in (b"EXEC", b"DISCARD", b"MULTI", b"WATCH"):
                conn.queue.append(command)
                writer.write(_simple(b"QUEUED"))
            else:
                writer.write(self._dispatch(conn, name, command[1:]))
            try:
                await writer.drain()
            except ConnectionError:
                break
        writer.close()

    def _dispatch(self, conn: _Connection, name: bytes, args: list[bytes]) -> bytes:
        if name == b"HELLO":  # protocol handshake — answer in RESP2
            return _array(
                [
                    _bulk(b"server"),
                    _bulk(b"redis"),
                    _bulk(b"version"),
                    _bulk(b"7.4.0"),
                    _bulk(b"proto"),
                    _integer(2),
                    _bulk(b"id"),
                    _integer(1),
                    _bulk(b"mode"),
                    _bulk(b"standalone"),
                    _bulk(b"role"),
                    _bulk(b"master"),
                    _bulk(b"modules"),
                    _array([]),
                ]
            )
        if name in (b"CLIENT", b"AUTH", b"COMMAND", b"CONFIG"):
            return _simple(b"OK")  # handshake/no-op commands
        if name == b"PING":
            return _bulk(args[0]) if args else _simple(b"PONG")
        if name == b"SET":
            self._store[args[0]] = args[1]
            self._bump(args[0])
            return _simple(b"OK")
        if name == b"GET":
            return _bulk(self._store.get(args[0]))
        if name == b"DEL":
            removed = 0
            for key in args:
                if key in self._store:
                    del self._store[key]
                    self._bump(key)
                    removed += 1
            return _integer(removed)
        if name == b"EXISTS":
            return _integer(sum(1 for key in args if key in self._store))
        if name in (b"INCR", b"INCRBY", b"DECR", b"DECRBY"):
            step = int(args[1]) if name in (b"INCRBY", b"DECRBY") else 1
            if name in (b"DECR", b"DECRBY"):
                step = -step
            current = int(self._store.get(args[0], b"0")) + step
            self._store[args[0]] = str(current).encode()
            self._bump(args[0])
            return _integer(current)
        if name == b"WATCH":
            for key in args:
                conn.watched[key] = self._version.get(key, 0)
            return _simple(b"OK")
        if name == b"UNWATCH":
            conn.watched.clear()
            return _simple(b"OK")
        if name == b"MULTI":
            conn.queue = []
            return _simple(b"OK")
        if name == b"DISCARD":
            conn.queue = None
            conn.watched.clear()
            return _simple(b"OK")
        if name == b"EXEC":
            return self._exec(conn)
        return _error(b"ERR unknown command '" + name + b"'")

    def _exec(self, conn: _Connection) -> bytes:
        if conn.queue is None:
            return _error(b"ERR EXEC without MULTI")
        queued = conn.queue
        watched = conn.watched
        conn.queue = None
        conn.watched = {}
        # Optimistic lock: if any watched key changed since WATCH, abort (nil).
        if any(self._version.get(key, 0) != version for key, version in watched.items()):
            return _array(None)
        return _array([self._dispatch(conn, cmd[0].upper(), cmd[1:]) for cmd in queued])
