"""Tests for copy_object and stream_sha256 on InMemoryStorageAdapter (Phase 6a step 3)."""
from __future__ import annotations

import hashlib
import pytest

from app.config import NodeConfig
from app.storage.adapter import InMemoryStorageAdapter

CHUNK = 1024 * 1024  # 1 MiB


def make_adapter(*node_ids: str) -> InMemoryStorageAdapter:
    configs = [
        NodeConfig(id=nid, name=nid, endpoint_url=f"http://fake-{nid}:9000")
        for nid in node_ids
    ]
    return InMemoryStorageAdapter(configs)


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# copy_object tests
# ---------------------------------------------------------------------------

def test_copy_returns_correct_byte_count():
    adapter = make_adapter("src", "tgt")
    data = b"hello world"
    adapter.put_object("src", "obj1", data)
    n = adapter.copy_object("src", "tgt", "obj1")
    assert n == len(data)


def test_copy_source_and_target_hashes_match():
    adapter = make_adapter("src", "tgt")
    data = b"x" * 500
    adapter.put_object("src", "obj1", data)
    adapter.copy_object("src", "tgt", "obj1")

    src_hash = adapter.stream_sha256("src", "obj1")
    tgt_hash = adapter.stream_sha256("tgt", "obj1")
    assert src_hash == tgt_hash


def test_copy_source_unchanged():
    adapter = make_adapter("src", "tgt")
    original = b"original data"
    adapter.put_object("src", "obj1", original)
    adapter.copy_object("src", "tgt", "obj1")
    assert adapter.get_object("src", "obj1") == original


def test_fenced_source_raises_and_writes_nothing():
    adapter = make_adapter("src", "tgt")
    adapter.put_object("src", "obj1", b"data")
    adapter.fence_node("src")
    with pytest.raises(ConnectionError):
        adapter.copy_object("src", "tgt", "obj1")
    # target must have nothing
    assert "obj1" not in adapter._storage.get("tgt", {})


def test_fenced_target_raises():
    adapter = make_adapter("src", "tgt")
    adapter.put_object("src", "obj1", b"data")
    adapter.fence_node("tgt")
    with pytest.raises(ConnectionError):
        adapter.copy_object("src", "tgt", "obj1")


def test_interrupted_copy_leaves_no_target_object():
    adapter = make_adapter("src", "tgt")
    # 3 MiB object, fail after 1 MiB
    data = b"A" * (3 * CHUNK)
    adapter.put_object("src", "bigobj", data)
    adapter.set_fail_after_bytes("tgt", CHUNK)

    with pytest.raises(ConnectionError):
        adapter.copy_object("src", "tgt", "bigobj")

    # No partial object on target
    assert "bigobj" not in adapter._storage.get("tgt", {})


def test_multi_chunk_copy_works():
    adapter = make_adapter("src", "tgt")
    # 2.5 MiB — spans 3 chunks
    data = b"B" * int(2.5 * CHUNK)
    adapter.put_object("src", "bigobj", data)
    n = adapter.copy_object("src", "tgt", "bigobj")
    assert n == len(data)
    assert adapter.get_object("tgt", "bigobj") == data


def test_copy_overwrites_existing_target():
    adapter = make_adapter("src", "tgt")
    adapter.put_object("src", "obj1", b"new data")
    adapter.put_object("tgt", "obj1", b"old data")
    adapter.copy_object("src", "tgt", "obj1")
    assert adapter.get_object("tgt", "obj1") == b"new data"


# ---------------------------------------------------------------------------
# stream_sha256 tests
# ---------------------------------------------------------------------------

def test_stream_sha256_stable():
    adapter = make_adapter("src")
    data = b"stable" * 100
    adapter.put_object("src", "obj1", data)
    h1 = adapter.stream_sha256("src", "obj1")
    h2 = adapter.stream_sha256("src", "obj1")
    assert h1 == h2
    assert h1 == sha256_of(data)


def test_stream_sha256_fenced_raises():
    adapter = make_adapter("src")
    adapter.put_object("src", "obj1", b"data")
    adapter.fence_node("src")
    with pytest.raises(ConnectionError):
        adapter.stream_sha256("src", "obj1")
