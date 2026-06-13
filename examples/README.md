# simloom examples

Both examples are also imported by the test suite, so they are exercised in
CI on every push.

## `toy_raft.py` — the canonical DST demo

A Raft leader election written as an ordinary asyncio program: JSON-RPC over
`asyncio.start_server`/`open_connection`, term and votedFor persisted to the
host disk with fsync. The torture script throws partitions, crashes,
restarts, and connection resets at a five-node cluster.

```sh
uv run python examples/toy_raft.py
```

The `buggy=True` variant plants the classic double-vote bug (votedFor never
consulted); exploration finds seeds where **two leaders win the same term**,
and every one of them replays exactly. The correct variant survives.

## `bpo42130.py` — a real, historical CPython race

[bpo-42130 / python/cpython#86296](https://github.com/python/cpython/issues/86296):
pre-3.12 `asyncio.wait_for` could *swallow a delivered cancellation* when the
inner future completed in the same event-loop window as the cancel. The race
needed an exact wall-clock collision in production; under simloom the
timeout boundary is a first-class scheduling choice, so exploration finds the
interleaving from a seed and replays it byte-for-byte. The modern stdlib
implementation survives the identical torture.

```sh
uv run python examples/bpo42130.py
```
