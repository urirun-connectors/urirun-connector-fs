import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
import pytest

@pytest.fixture(scope="module")
def fs_worker(tmp_path_factory):
    sock_dir = tmp_path_factory.mktemp("fs_socks")
    sock_path = str(sock_dir / "fs.sock")
    env = os.environ.copy()
    env["DOCKURI_SOCKET"] = sock_path
    repo_root = Path(__file__).parent.parent.resolve()
    env["PYTHONPATH"] = str(repo_root)

    proc = subprocess.Popen([sys.executable, "-m", "urirun_connector_fs.dockuri_worker"], env=env, cwd=str(repo_root), stderr=subprocess.PIPE)
    for _ in range(50):
        if os.path.exists(sock_path):
            break
        time.sleep(0.02)
    else:
        err = proc.stderr.read().decode() if proc.stderr else ""
        proc.kill()
        raise TimeoutError(f"FS worker failed to initialize socket: {err}")

    yield sock_path

    proc.terminate()
    proc.wait(timeout=2.0)
    if os.path.exists(sock_path):
        os.remove(sock_path)


def test_dockuri_ping_and_file_read(fs_worker, tmp_path):
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(fs_worker)

    # 1. Ping
    t0 = time.perf_counter()
    client.sendall(json.dumps({"id": "p1", "proc": "__ping__", "args": {}}).encode() + b"\n")
    raw = client.recv(4096)
    dur_ms = (time.perf_counter() - t0) * 1000.0
    res = json.loads(raw.decode().strip())
    assert res["ok"] is True
    assert res["result"]["status"] == "pong"
    assert dur_ms < 2.0
    print(f"\n✓ FS Ping latency: {dur_ms:.3f} ms")

    # 2. Read text file
    sample_file = tmp_path / "test.txt"
    sample_file.write_text("Hello Dockuri FS Connector!", encoding="utf-8")

    t0 = time.perf_counter()
    msg = json.dumps({"id": "r1", "proc": "fs.read_text", "args": {"path": str(sample_file)}}).encode() + b"\n"
    client.sendall(msg)
    raw = client.recv(4096)
    dur_ms = (time.perf_counter() - t0) * 1000.0
    res = json.loads(raw.decode().strip())
    assert res["ok"] is True
    assert res["result"]["content"] == "Hello Dockuri FS Connector!"
    assert dur_ms < 2.0
    print(f"✓ FS Read Text latency: {dur_ms:.3f} ms")

    client.close()
