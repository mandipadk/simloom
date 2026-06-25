"""Phase I — TLS in the simulation: a memory-BIO SSL transport over SimTransport.

The handshake runs in-process over the simulated wire; the run is deterministic
and replays byte-for-byte even though OpenSSL's RNG is not seeded (the event log
records scheduling structure, not ciphertext). The gate: an unmodified aiohttp
HTTPS client hits an unmodified aiohttp HTTPS server in-sim over a pinned cert."""

from __future__ import annotations

import asyncio
import shutil
import ssl
import subprocess

import pytest

import simloom


@pytest.fixture(scope="session")
def certs(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str]:
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available to generate a test certificate")
    directory = tmp_path_factory.mktemp("tls")
    cert = directory / "cert.pem"
    key = directory / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "3650",
            "-nodes",
            "-subj",
            "/CN=svc.local",
            "-addext",
            "subjectAltName=DNS:svc.local",
        ],
        check=True,
        capture_output=True,
    )
    return str(cert), str(key)


def _server_ctx(cert: str, key: str) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    return ctx


def _client_ctx(cert: str) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(cert)
    return ctx


class TestAsyncioStreamsTLS:
    def test_handshake_and_echo(self, certs: tuple[str, str]) -> None:
        cert, key = certs

        async def main(world: simloom.World) -> bytes:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                line = await reader.readline()
                writer.write(b"secure:" + line)
                await writer.drain()
                writer.close()

            server = await asyncio.start_server(
                handle, "svc.local", 443, ssl=_server_ctx(cert, key)
            )
            async with server:
                reader, writer = await asyncio.open_connection(
                    "svc.local", 443, ssl=_client_ctx(cert), server_hostname="svc.local"
                )
                writer.write(b"ping\n")
                await writer.drain()
                reply = await asyncio.wait_for(reader.readline(), timeout=5.0)
                writer.close()
                return reply

        result = simloom.run(main, seed=0)
        assert result.value == b"secure:ping\n"

    def test_is_deterministic_and_replayable(self, certs: tuple[str, str]) -> None:
        cert, key = certs

        async def main(world: simloom.World) -> int:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                await reader.readline()
                writer.write(b"ok\n")
                await writer.drain()
                writer.close()

            server = await asyncio.start_server(
                handle, "svc.local", 443, ssl=_server_ctx(cert, key)
            )
            async with server:
                reader, writer = await asyncio.open_connection(
                    "svc.local", 443, ssl=_client_ctx(cert), server_hostname="svc.local"
                )
                writer.write(b"go\n")
                await writer.drain()
                await reader.readline()
                writer.close()
                return 1

        a = simloom.run(main, seed=0)
        b = simloom.run(main, seed=0)
        replay = simloom.replay(main, tape=a)
        assert a.digest == b.digest == replay.digest

    def test_untrusted_cert_is_rejected(self, certs: tuple[str, str]) -> None:
        cert, key = certs

        async def main(world: simloom.World) -> None:
            async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                writer.close()

            server = await asyncio.start_server(
                handle, "svc.local", 443, ssl=_server_ctx(cert, key)
            )
            async with server:
                # A default context does NOT trust our self-signed cert.
                await asyncio.open_connection(
                    "svc.local", 443, ssl=ssl.create_default_context(), server_hostname="svc.local"
                )

        result = simloom.run(main, seed=0, raise_on_error=False)
        assert isinstance(result.error, ssl.SSLError)


class TestAiohttpGate:
    """The gate: an unmodified aiohttp HTTPS client hits an unmodified aiohttp
    HTTPS server in-sim over a pinned cert, replayable byte-for-byte."""

    def test_aiohttp_https_in_sim(self, certs: tuple[str, str]) -> None:
        pytest.importorskip("aiohttp")
        import aiohttp
        from aiohttp import web

        cert, key = certs

        async def main(world: simloom.World) -> dict[str, object]:
            async def handler(request: web.Request) -> web.Response:
                return web.json_response({"hello": request.query.get("name", "world")})

            app = web.Application()
            app.router.add_get("/greet", handler)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "svc.local", 443, ssl_context=_server_ctx(cert, key))
            await site.start()

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://svc.local:443/greet?name=simloom", ssl=_client_ctx(cert)
                ) as resp:
                    payload = {"status": resp.status, "body": await resp.json()}
            await runner.cleanup()
            return payload

        result = simloom.run(main, seed=0)
        assert result.value == {"status": 200, "body": {"hello": "simloom"}}
        # byte-for-byte replay
        assert simloom.replay(main, tape=result).digest == result.digest
        assert simloom.run(main, seed=0).digest == result.digest
