import hashlib
import io
import math
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from app.config import AppConfig, NodeConfig
from app.db import create_connection, run_migrations, sync_nodes
from app.main import create_app, get_adapter, get_db_session
from app.storage.adapter import InMemoryStorageAdapter


@pytest.fixture
def test_setup(tmp_path):
    """Sets up an isolated SQLite database and InMemoryStorageAdapter for testing."""
    nodes = [
        NodeConfig(
            id="node-1",
            name="Node 1",
            endpoint_url="http://127.0.0.1:9001",
            virtual_capacity_bytes=5000,
        ),
        NodeConfig(
            id="node-2",
            name="Node 2",
            endpoint_url="http://127.0.0.1:9002",
            virtual_capacity_bytes=10000,
        ),
        NodeConfig(
            id="node-3",
            name="Node 3",
            endpoint_url="http://127.0.0.1:9003",
            virtual_capacity_bytes=7000,
        ),
    ]
    cfg = AppConfig(nodes=nodes)
    db_file = tmp_path / "test_metadata.db"
    conn = create_connection(db_file)
    run_migrations(conn)
    sync_nodes(conn, nodes)

    adapter = InMemoryStorageAdapter(nodes)
    app = create_app(cfg=cfg, adapter=adapter, db_conn=conn)

    # Dependency overrides to ensure isolation per test
    def override_get_db():
        yield conn

    def override_get_adapter():
        return adapter

    app.dependency_overrides[get_db_session] = override_get_db
    app.dependency_overrides[get_adapter] = override_get_adapter

    client = TestClient(app)
    return {
        "client": client,
        "conn": conn,
        "adapter": adapter,
        "nodes": nodes,
    }


def test_checksum_correctness_and_upload_download(test_setup):
    client = test_setup["client"]
    adapter = test_setup["adapter"]
    conn = test_setup["conn"]

    content = b"The quick brown fox jumps over the lazy dog"
    expected_sha256 = hashlib.sha256(content).hexdigest()

    response = client.post(
        "/objects",
        files={"file": ("fox.txt", io.BytesIO(content), "text/plain")},
        data={"criticality": "4", "required_replica_count": "2"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["checksum"] == expected_sha256
    assert data["checksum_sha256"] == expected_sha256
    assert data["object_name"] == "fox.txt"
    assert data["size_bytes"] == len(content)
    assert data["criticality"] == 4
    assert data["required_replica_count"] == 2
    object_id = data["object_id"]
    origin_node = data["origin_node_id"]

    # Verify adapter stores under object_id as key, NEVER the untrusted filename
    stored_bytes = adapter.get_object(origin_node, object_id)
    assert stored_bytes == content
    assert object_id in adapter._storage[origin_node]
    assert "fox.txt" not in adapter._storage[origin_node]

    # Verify download endpoint returns identical bytes and content disposition
    download_resp = client.get(f"/objects/{object_id}/download")
    assert download_resp.status_code == 200
    assert download_resp.content == content
    assert 'filename="fox.txt"' in download_resp.headers.get("content-disposition", "")

    # Verify SHA-256 of downloaded content matches expected
    download_sha256 = hashlib.sha256(download_resp.content).hexdigest()
    assert download_sha256 == expected_sha256


def test_quota_rejection(test_setup):
    client = test_setup["client"]
    adapter = test_setup["adapter"]

    # Node capacities: node-1: 5000, node-2: 10000, node-3: 7000
    # Fence node-1 and node-3 to test quota enforcement strictly on node-2
    adapter.fence_node("node-1")
    adapter.fence_node("node-3")

    # Initial free on node-2 is 10000.
    # Upload 6000 bytes -> Should succeed (remaining 4000)
    data_6000 = b"A" * 6000
    resp1 = client.post(
        "/objects",
        files={"file": ("chunk1.bin", io.BytesIO(data_6000), "application/octet-stream")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    assert resp1.status_code == 200
    assert resp1.json()["origin_node_id"] == "node-2"

    # Attempt to upload 4500 bytes -> 4500 > 4000 free, must be rejected with 507
    data_4500 = b"B" * 4500
    resp2 = client.post(
        "/objects",
        files={"file": ("chunk2.bin", io.BytesIO(data_4500), "application/octet-stream")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    assert resp2.status_code == 507
    assert "quota exceeded" in resp2.json()["detail"].lower()

    # Upload 3500 bytes -> 3500 <= 4000 free, should succeed (remaining 500)
    data_3500 = b"C" * 3500
    resp3 = client.post(
        "/objects",
        files={"file": ("chunk3.bin", io.BytesIO(data_3500), "application/octet-stream")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    assert resp3.status_code == 200

    # Attempt to upload 600 bytes -> 600 > 500 free, must be rejected with 507
    data_600 = b"D" * 600
    resp4 = client.post(
        "/objects",
        files={"file": ("chunk4.bin", io.BytesIO(data_600), "application/octet-stream")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    assert resp4.status_code == 507


def test_filename_validation(test_setup):
    client = test_setup["client"]

    invalid_names = [
        "../etc/passwd",
        "../../shadow",
        "/etc/hosts",
        "nested/path/file.txt",
        "dir\\winfile.txt",
        "file\0null.txt",
        "file\nnewline.txt",
        "",
        "   ",
        ".",
        "..",
        "a" * 256 + ".txt",
    ]

    for bad_name in invalid_names:
        response = client.post(
            "/objects",
            files={"file": (bad_name, io.BytesIO(b"data"), "text/plain")},
            data={"criticality": "2", "required_replica_count": "1"},
        )
        assert response.status_code == 400, f"Expected 400 for {bad_name!r}, got {response.status_code}: {response.text}"

    # Valid filenames must succeed
    valid_names = [
        "document.pdf",
        "image_2026-10-28.png",
        "archive.tar.gz",
        "my-dataset-v1.parquet",
    ]
    for good_name in valid_names:
        response = client.post(
            "/objects",
            files={"file": (good_name, io.BytesIO(b"valid content"), "text/plain")},
            data={"criticality": "1", "required_replica_count": "1"},
        )
        assert response.status_code == 200, f"Expected 200 for '{good_name}'"
        assert response.json()["object_name"] == good_name


def test_access_normalization(test_setup):
    client = test_setup["client"]

    # Upload 3 objects
    r1 = client.post(
        "/objects",
        files={"file": ("obj1.txt", io.BytesIO(b"111"), "text/plain")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    r2 = client.post(
        "/objects",
        files={"file": ("obj2.txt", io.BytesIO(b"222"), "text/plain")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    r3 = client.post(
        "/objects",
        files={"file": ("obj3.txt", io.BytesIO(b"333"), "text/plain")},
        data={"criticality": "3", "required_replica_count": "1"},
    )

    id1 = r1.json()["object_id"]
    id2 = r2.json()["object_id"]
    id3 = r3.json()["object_id"]

    # Initially, all access counts are 0, normalized frequency is 0.0
    objs = client.get("/objects").json()["items"]
    by_id = {o["object_id"]: o for o in objs}
    assert by_id[id1]["access_count"] == 0
    assert by_id[id1]["access_frequency_normalized"] == 0.0
    assert by_id[id2]["access_frequency_normalized"] == 0.0
    assert by_id[id3]["access_frequency_normalized"] == 0.0

    # Download obj1 3 times (count=3)
    for _ in range(3):
        res = client.get(f"/objects/{id1}/download")
        assert res.status_code == 200

    # Download obj2 1 time (count=1)
    res = client.get(f"/objects/{id2}/download")
    assert res.status_code == 200

    # obj3 has count=0
    # Max count is 3
    # obj1: log(1 + 3) / log(1 + 3) = 1.0
    # obj2: log(1 + 1) / log(1 + 3) = log(2) / log(4) = 0.5
    # obj3: log(1 + 0) / log(1 + 3) = 0.0
    objs = client.get("/objects").json()["items"]
    by_id = {o["object_id"]: o for o in objs}
    assert by_id[id1]["access_count"] == 3
    assert pytest.approx(by_id[id1]["access_frequency_normalized"], 1e-4) == 1.0

    assert by_id[id2]["access_count"] == 1
    assert pytest.approx(by_id[id2]["access_frequency_normalized"], 1e-4) == 0.5

    assert by_id[id3]["access_count"] == 0
    assert by_id[id3]["access_frequency_normalized"] == 0.0

    # Verify last_access_at is populated
    assert by_id[id1]["last_access_at"] is not None
    assert by_id[id2]["last_access_at"] is not None
    assert by_id[id3]["last_access_at"] is None


def test_pagination(test_setup):
    client = test_setup["client"]

    # Upload 7 objects
    created_ids = []
    for i in range(7):
        resp = client.post(
            "/objects",
            files={"file": (f"file_{i}.txt", io.BytesIO(f"content {i}".encode()), "text/plain")},
            data={"criticality": "2", "required_replica_count": "1"},
        )
        assert resp.status_code == 200
        created_ids.append(resp.json()["object_id"])

    # Page 1: limit=3, offset=0
    p1 = client.get("/objects?limit=3&offset=0").json()
    assert p1["total"] == 7
    assert p1["limit"] == 3
    assert p1["offset"] == 0
    assert len(p1["items"]) == 3
    p1_ids = [item["object_id"] for item in p1["items"]]

    # Page 2: limit=3, offset=3
    p2 = client.get("/objects?limit=3&offset=3").json()
    assert p2["total"] == 7
    assert len(p2["items"]) == 3
    p2_ids = [item["object_id"] for item in p2["items"]]
    assert set(p1_ids).isdisjoint(set(p2_ids))

    # Page 3: limit=3, offset=6
    p3 = client.get("/objects?limit=3&offset=6").json()
    assert p3["total"] == 7
    assert len(p3["items"]) == 1
    p3_ids = [item["object_id"] for item in p3["items"]]
    assert p3_ids[0] not in p1_ids and p3_ids[0] not in p2_ids

    # Verify replica state is populated for items
    for item in p1["items"]:
        assert len(item["replicas"]) >= 1
        rep = item["replicas"][0]
        assert rep["verified"] is True
        assert rep["is_authoritative"] is True
        assert rep["status"] == "valid"
        assert rep["node_id"] in ["node-1", "node-2", "node-3"]


def test_initial_placement_and_get_nodes_capacity(test_setup):
    client = test_setup["client"]
    adapter = test_setup["adapter"]

    # Initial capacities: node-1: 5000, node-2: 10000, node-3: 7000
    # Node-2 has the most free virtual capacity.
    resp = client.post(
        "/objects",
        files={"file": ("place1.txt", io.BytesIO(b"x" * 1000), "text/plain")},
        data={"criticality": "5", "required_replica_count": "2"},
    )
    assert resp.status_code == 200
    assert resp.json()["origin_node_id"] == "node-2"

    # GET /nodes should reflect node-2 free_bytes = 10000 - 1000 = 9000
    nodes_resp = client.get("/nodes")
    assert nodes_resp.status_code == 200
    nodes = {n["node_id"]: n for n in nodes_resp.json()}
    assert nodes["node-2"]["virtual_capacity_bytes"] == 10000
    assert nodes["node-2"]["free_bytes"] == 9000
    assert nodes["node-1"]["free_bytes"] == 5000
    assert nodes["node-3"]["free_bytes"] == 7000

    # Fence node-2, now highest free among reachable is node-3 (7000)
    adapter.fence_node("node-2")
    resp2 = client.post(
        "/objects",
        files={"file": ("place2.txt", io.BytesIO(b"y" * 500), "text/plain")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    assert resp2.status_code == 200
    assert resp2.json()["origin_node_id"] == "node-3"


def test_download_unavailable_when_node_fenced(test_setup):
    client = test_setup["client"]
    adapter = test_setup["adapter"]

    # Upload an object to node-2
    resp = client.post(
        "/objects",
        files={"file": ("sample.txt", io.BytesIO(b"hello world"), "text/plain")},
        data={"criticality": "3", "required_replica_count": "1"},
    )
    assert resp.status_code == 200
    obj_id = resp.json()["object_id"]
    node_id = resp.json()["origin_node_id"]

    # Fence the storage node where the replica is hosted
    adapter.fence_node(node_id)

    # Attempting to download should return 503 Service Unavailable
    down_resp = client.get(f"/objects/{obj_id}/download")
    assert down_resp.status_code == 503

    # Unfence node; download should succeed
    adapter.unfence_node(node_id)
    down_resp2 = client.get(f"/objects/{obj_id}/download")
    assert down_resp2.status_code == 200
    assert down_resp2.content == b"hello world"


def test_download_nonexistent_returns_404(test_setup):
    client = test_setup["client"]
    res = client.get("/objects/non-existent-id/download")
    assert res.status_code == 404


def test_criticality_and_replica_count_validation(test_setup):
    client = test_setup["client"]

    # Invalid criticality (< 1 or > 5)
    for crit in ["0", "6", "-1", "abc"]:
        resp = client.post(
            "/objects",
            files={"file": ("crit.txt", io.BytesIO(b"data"), "text/plain")},
            data={"criticality": crit, "required_replica_count": "2"},
        )
        assert resp.status_code == 400

    # Invalid required_replica_count (< 1 or > 3)
    for rep in ["0", "4", "-1", "xyz"]:
        resp = client.post(
            "/objects",
            files={"file": ("rep.txt", io.BytesIO(b"data"), "text/plain")},
            data={"criticality": "3", "required_replica_count": rep},
        )
        assert resp.status_code == 400
