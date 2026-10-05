"""Dockuri Fast-IPC Worker Daemon for Filesystem Connector (DOCK-STD-001).

Listens on Unix Domain Socket, executes procedures with zero cold-start.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

SOCKET_PATH = os.getenv("DOCKURI_SOCKET", "/tmp/dockuri_fs.sock")


def handle_ping() -> dict[str, Any]:
    return {
        "status": "pong",
        "runtime": f"python-{sys.version.split()[0]}",
        "app": "urirun-connector-fs",
        "pid": os.getpid(),
    }


def read_text_proc(path: str) -> dict[str, Any]:
    p = Path(path).resolve()
    if not p.is_file():
        raise FileNotFoundError(f"File not found: {p}")
    # Read text
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    return {"content": content, "size": len(content)}


def sha256_proc(path: str) -> dict[str, Any]:
    p = Path(path).resolve()
    if not p.is_file():
        raise FileNotFoundError(f"File not found: {p}")
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return {"sha256": h.hexdigest(), "size": p.stat().st_size}


REGISTRY = {
    "fs.read_text": read_text_proc,
    "fs.sha256": sha256_proc,
}


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while line := await reader.readline():
            t0 = time.perf_counter()
            req = json.loads(line.decode("utf-8").strip())
            req_id = req.get("id", "unknown")
            proc = req.get("proc")
            args = req.get("args", {})

            if proc == "__ping__":
                res = handle_ping()
                err = None
            elif proc in REGISTRY:
                try:
                    res = REGISTRY[proc](**args)
                    err = None
                except Exception as e:
                    res = None
                    err = str(e)
            else:
                res = None
                err = f"Procedure not found: {proc}"

            t1 = time.perf_counter()
            resp = {
                "id": req_id,
                "ok": err is None,
                "result": res,
                "error": err,
                "metrics": {"duration_us": int((t1 - t0) * 1_000_000)},
            }
            writer.write(json.dumps(resp).encode("utf-8") + b"\n")
            await writer.drain()
    except asyncio.IncompleteReadError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()


async def run_server() -> None:
    if os.path.exists(SOCKET_PATH):
        try:
            os.remove(SOCKET_PATH)
        except OSError:
            pass

    server = await asyncio.start_unix_server(handle_client, path=SOCKET_PATH)
    os.chmod(SOCKET_PATH, 0o660)
    print(f"✓ Dockuri FS Worker ready on UDS: {SOCKET_PATH} (PID {os.getpid()})")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    async with server:
        await stop_event.wait()

    if os.path.exists(SOCKET_PATH):
        try:
            os.remove(SOCKET_PATH)
        except OSError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        pass
