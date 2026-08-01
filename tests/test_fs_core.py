"""Offline tests for the fs connector: sha256 dedup, perceptual (optional), move."""
import base64
import os
import pytest
import urirun_connector_fs.core as c


def test_bindings_valid():
    b = c.urirun_bindings()
    assert set(b["bindings"]) == {
        "fs://host/archive/command/unpack-b64",
        "fs://host/duplicates/query/find",
        "fs://host/duplicates/command/move",
        "fs://host/file/query/read-b64",
        "fs://host/file/command/write-b64",
        "fs://host/file/command/delete",
    }


def test_sha256_finds_exact_duplicates(tmp_path):
    (tmp_path / "a.txt").write_text("INVOICE-123")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "a_copy.txt").write_text("INVOICE-123")   # identical
    (tmp_path / "other.txt").write_text("DIFFERENT")
    r = c.find(root=str(tmp_path), mode="sha256")
    assert r["ok"] and r["duplicateGroups"] == 1
    assert r["extraFiles"] == 1
    grp = r["groups"][0]
    assert grp["count"] == 2
    assert {os.path.basename(p) for p in grp["files"]} == {"a.txt", "a_copy.txt"}


def test_extensions_and_minsize_filter(tmp_path):
    (tmp_path / "a.pdf").write_bytes(b"x" * 2000)
    (tmp_path / "b.pdf").write_bytes(b"x" * 2000)   # dup
    (tmp_path / "c.txt").write_bytes(b"x" * 2000)   # filtered out by extension
    (tmp_path / "tiny.pdf").write_bytes(b"x")        # filtered out by min_size
    r = c.find(root=str(tmp_path), mode="sha256", extensions=[".pdf"], min_size=100)
    assert r["duplicateGroups"] == 1 and r["groups"][0]["count"] == 2


def test_reclaimable_bytes(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"y" * 500)
    (tmp_path / "b.bin").write_bytes(b"y" * 500)
    (tmp_path / "d.bin").write_bytes(b"y" * 500)     # 3 copies → reclaim 2*500
    r = c.find(root=str(tmp_path), mode="sha256")
    assert r["reclaimableBytes"] == 1000


def test_move_dry_run_does_not_touch_files(tmp_path):
    (tmp_path / "a.txt").write_text("same")
    (tmp_path / "b.txt").write_text("same")
    r = c.move(root=str(tmp_path), mode="sha256", dry_run=True)
    assert r["ok"] and r["movedCount"] == 1
    assert (tmp_path / "b.txt").exists()             # nothing moved on a dry run
    assert not (tmp_path / "_duplicates").exists()


def test_move_quarantines_extras(tmp_path):
    (tmp_path / "a.txt").write_text("same")
    (tmp_path / "b.txt").write_text("same")
    r = c.move(root=str(tmp_path), mode="sha256", dry_run=False)
    assert r["movedCount"] == 1
    assert (tmp_path / "a.txt").exists()             # keeper stays
    assert (tmp_path / "_duplicates").exists()       # extra quarantined


def test_read_and_write_b64_round_trip(tmp_path):
    payload = b"%PDF-1.4\nsmall invoice\n"
    encoded = base64.b64encode(payload).decode("ascii")
    target = tmp_path / "Downloads" / "scan.pdf"

    written = c.write_b64(path=str(target), bytes_b64=encoded)
    assert written["ok"] is True
    assert written["path"] == str(target)
    assert target.read_bytes() == payload

    read = c.read_b64(path=str(target))
    assert read["ok"] is True
    assert read["bytes_b64"] == encoded
    assert base64.b64decode(read["bytes_b64"]) == payload


def test_write_b64_does_not_overwrite_by_default(tmp_path):
    target = tmp_path / "scan.pdf"
    target.write_bytes(b"first")

    written = c.write_b64(path=str(target), bytes_b64=base64.b64encode(b"second").decode("ascii"))

    assert written["ok"] is True
    assert written["renamed"] is True
    assert target.read_bytes() == b"first"
    assert os.path.basename(written["path"]) == "scan_1.pdf"
    assert (tmp_path / "scan_1.pdf").read_bytes() == b"second"


def test_perceptual_mode_needs_images_or_reports_cleanly(tmp_path):
    pytest.importorskip("PIL")
    pytest.importorskip("imagehash")
    from PIL import Image
    img = Image.new("RGB", (64, 64), "white")
    img.save(tmp_path / "x.png")
    img.save(tmp_path / "x_copy.png")
    r = c.find(root=str(tmp_path), mode="perceptual", threshold=5)
    assert r["ok"] and r["duplicateGroups"] == 1


# --------------------------------------------------------------------------- #
# REVERSIBILITY: the fs connector ADOPTS the engine contract (mutation returns
# `inverse`). Drive a real flow with the connector-agnostic ReversibleProcess and
# prove files on disk return to their prior state — write⟂restore/delete, delete⟂write.
# --------------------------------------------------------------------------- #
import sys, pathlib  # noqa: E402
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "urirun/adapters/python"))


def test_write_returns_delete_inverse_for_new_file(tmp_path):
    p = tmp_path / "new.txt"
    r = c.write_b64(path=str(p), bytes_b64=base64.b64encode(b"hi").decode(), overwrite=True)
    assert r["ok"] and r["inverse"]["uri"].endswith("file/command/delete")
    assert r["inverse"]["args"]["path"] == str(p)


def test_overwrite_returns_restore_inverse(tmp_path):
    p = tmp_path / "f.txt"
    p.write_bytes(b"OLD")
    r = c.write_b64(path=str(p), bytes_b64=base64.b64encode(b"NEW").decode(), overwrite=True)
    assert r["inverse"]["uri"].endswith("file/command/write-b64")
    assert base64.b64decode(r["inverse"]["args"]["bytes_b64"]) == b"OLD"   # restores the prior bytes


def test_end_to_end_flow_rollback_restores_disk(tmp_path):
    from urirun.node.reversible import (CallableTransport, ReversibleProcess,
                                        Transition, Action, ledger_from_execution, path_of)
    a, b = tmp_path / "a.txt", tmp_path / "b.txt"
    a.write_bytes(b"A0")                                    # a pre-exists; b is new

    # transport routing fs://host/file/command/* to the real connector handlers, in-process
    def call(uri, payload):
        p = path_of(uri)
        if p == "file/command/write-b64":
            return c.write_b64(**payload)
        if p == "file/command/delete":
            return c.delete(**payload)
        return {"ok": False, "error": f"route {p}"}
    proc = ReversibleProcess(CallableTransport(call))

    # a "flow" that mutates the disk, collecting each step's inverse (as execute_flow would)
    steps = [
        ("s1", "fs://host/file/command/write-b64",
         {"path": str(a), "bytes_b64": base64.b64encode(b"A1").decode(), "overwrite": True}),
        ("s2", "fs://host/file/command/write-b64",
         {"path": str(b), "bytes_b64": base64.b64encode(b"B1").decode(), "overwrite": True}),
    ]
    timeline, results = [], {}
    for sid, uri, args in steps:
        res = call(uri, args)
        timeline.append({"id": sid, "uri": uri, "ok": True})
        results[sid] = {"ok": True, "result": {"value": res}}
    assert a.read_bytes() == b"A1" and b.read_bytes() == b"B1"   # flow mutated the disk

    ledger = ledger_from_execution({"ok": True, "timeline": timeline, "results": results})
    assert [path_of(t.inverse.uri) for t in ledger] == ["file/command/write-b64", "file/command/delete"]

    rb = proc.rollback_flow(None, ledger)                  # LIFO: delete b, restore a
    assert rb["ok"]
    assert a.read_bytes() == b"A0"                          # a restored to its prior content
    assert not b.exists()                                  # b (created by the flow) removed
