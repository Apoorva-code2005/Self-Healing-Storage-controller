"""
Unit tests for Phase 5 — Priority Engine and Destination Selection.

Covers all items from PRD Section 18 (unit tests):
  - Priority normalisation and weight validation (config.py already validates; tested here too)
  - Deterministic tie-breaking
  - Size-aware score with gamma=0 equals base score
  - Redundancy vulnerability mapping including Section 9 examples plus edge cases
  - Destination feasibility under virtual-capacity, risk, load, and reachability constraints

Also covers the GET /priority endpoint via the test FastAPI client.
"""
from __future__ import annotations

import io
import math
import pytest

from app.config import AppConfig, NodeConfig, WeightsConfig
from app.db import create_connection, run_migrations, sync_nodes
from app.main import create_app, get_adapter, get_db_session
from app.services.placement import is_feasible, select_destinations
from app.services.priority import (
    _access_frequency_term,
    _criticality_term,
    _redundancy_vulnerability,
    count_healthy_verified_replicas,
    rank_objects,
    score_object,
)
from app.storage.adapter import InMemoryStorageAdapter
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

W1, W2, W3, W4 = 0.40, 0.20, 0.25, 0.15  # defaults from PRD Section 24


def _node(
    node_id: str,
    status: str = "healthy",
    risk: float = 0.0,
    load: float = 0.0,
    free: int = 100_000,
    v_cap: int = 100_000,
) -> dict:
    return {
        "node_id": node_id,
        "status": status,
        "risk_score": risk,
        "load_metric": load,
        "free_bytes": free,
        "virtual_capacity_bytes": v_cap,
    }


def _replica(node_id: str, verified: bool = True, status: str = "valid", auth: bool = True) -> dict:
    return {
        "node_id": node_id,
        "verified": verified,
        "status": status,
        "is_authoritative": auth,
    }


@pytest.fixture
def api_setup(tmp_path):
    """Isolated FastAPI + in-memory storage for endpoint tests."""
    nodes = [
        NodeConfig(id="node-1", name="N1", endpoint_url="http://127.0.0.1:9001", virtual_capacity_bytes=50_000),
        NodeConfig(id="node-2", name="N2", endpoint_url="http://127.0.0.1:9002", virtual_capacity_bytes=50_000),
        NodeConfig(id="node-3", name="N3", endpoint_url="http://127.0.0.1:9003", virtual_capacity_bytes=50_000),
    ]
    cfg = AppConfig(nodes=nodes)
    db_file = tmp_path / "test.db"
    conn = create_connection(db_file)
    run_migrations(conn)
    sync_nodes(conn, nodes)

    adapter = InMemoryStorageAdapter(nodes)
    app = create_app(cfg=cfg, adapter=adapter, db_conn=conn)

    def override_db():
        yield conn

    def override_adapter():
        return adapter

    app.dependency_overrides[get_db_session] = override_db
    app.dependency_overrides[get_adapter] = override_adapter

    return {"client": TestClient(app), "conn": conn, "adapter": adapter, "nodes": nodes}


# ===========================================================================
# 1. Individual normalisation term tests
# ===========================================================================

class TestCriticalityTerm:
    def test_min_criticality(self):
        assert _criticality_term(1) == pytest.approx(0.0)

    def test_max_criticality(self):
        assert _criticality_term(5) == pytest.approx(1.0)

    def test_mid_criticality(self):
        assert _criticality_term(3) == pytest.approx(0.5)

    def test_formula_level2(self):
        assert _criticality_term(2) == pytest.approx(0.25)

    def test_formula_level4(self):
        assert _criticality_term(4) == pytest.approx(0.75)


class TestAccessFrequencyTerm:
    def test_zero_max_count_returns_zero(self):
        assert _access_frequency_term(0, 0) == 0.0
        assert _access_frequency_term(5, 0) == 0.0

    def test_max_count_normalises_to_one(self):
        # log(1+n)/log(1+n) = 1.0
        assert _access_frequency_term(10, 10) == pytest.approx(1.0)

    def test_zero_access_returns_zero(self):
        assert _access_frequency_term(0, 10) == pytest.approx(0.0)

    def test_half_way(self):
        # log(1+1)/log(1+3) = log(2)/log(4) ≈ 0.5
        val = _access_frequency_term(1, 3)
        assert val == pytest.approx(math.log(2) / math.log(4), rel=1e-6)


# ===========================================================================
# 2. Redundancy vulnerability — PRD Section 9 examples and edge cases
# ===========================================================================

class TestRedundancyVulnerability:
    # PRD Section 9 explicit examples
    def test_r1_h0_gives_1(self):
        assert _redundancy_vulnerability(0, 1) == pytest.approx(1.0)

    def test_r2_h1_gives_0_5(self):
        assert _redundancy_vulnerability(1, 2) == pytest.approx(0.5)

    def test_r3_h1_gives_0_667(self):
        assert _redundancy_vulnerability(1, 3) == pytest.approx(2 / 3, rel=1e-6)

    def test_h_ge_r_gives_zero(self):
        assert _redundancy_vulnerability(2, 2) == pytest.approx(0.0)
        assert _redundancy_vulnerability(5, 3) == pytest.approx(0.0)

    # Edge: r = 0 must return None (object excluded)
    def test_r0_excluded(self):
        assert _redundancy_vulnerability(0, 0) is None
        assert _redundancy_vulnerability(3, 0) is None

    def test_r3_h2_gives_0_333(self):
        assert _redundancy_vulnerability(2, 3) == pytest.approx(1 / 3, rel=1e-6)


# ===========================================================================
# 3. Healthy replica counter
# ===========================================================================

class TestHealthyReplicaCounter:
    def test_empty_replicas(self):
        assert count_healthy_verified_replicas([], ["node-1"]) == 0

    def test_verified_on_healthy_node(self):
        reps = [_replica("node-1")]
        assert count_healthy_verified_replicas(reps, ["node-1"]) == 1

    def test_verified_but_node_not_healthy(self):
        reps = [_replica("node-1")]
        assert count_healthy_verified_replicas(reps, ["node-2"]) == 0

    def test_unverified_excluded(self):
        reps = [{"node_id": "node-1", "verified": False, "status": "valid"}]
        assert count_healthy_verified_replicas(reps, ["node-1"]) == 0

    def test_invalid_status_excluded(self):
        reps = [{"node_id": "node-1", "verified": True, "status": "invalid"}]
        assert count_healthy_verified_replicas(reps, ["node-1"]) == 0

    def test_multiple_replicas(self):
        reps = [
            _replica("node-1"),
            _replica("node-2"),
            {"node_id": "node-3", "verified": True, "status": "lost"},
        ]
        assert count_healthy_verified_replicas(reps, ["node-1", "node-2"]) == 2


# ===========================================================================
# 4. score_object — single object scoring
# ===========================================================================

class TestScoreObject:
    def test_excluded_when_r_is_zero(self):
        result = score_object(
            criticality=5, access_count=10, max_access_count=10,
            node_risk_score=0.9, healthy_verified_replicas=0,
            required_replica_count=0, size_bytes=100,
            w1=W1, w2=W2, w3=W3, w4=W4,
        )
        assert result is None

    def test_gamma_zero_equals_base(self):
        """Fundamental PRD requirement: gamma=0 must reproduce base priority."""
        base = score_object(
            criticality=4, access_count=3, max_access_count=10,
            node_risk_score=0.6, healthy_verified_replicas=1,
            required_replica_count=2, size_bytes=1024,
            w1=W1, w2=W2, w3=W3, w4=W4, gamma=0.0,
        )
        assert base is not None
        assert base["priority"] == pytest.approx(base["priority_rate"], rel=1e-9)

    def test_gamma_nonzero_reduces_large_objects(self):
        """Larger object should have lower priority_rate with gamma > 0."""
        large = score_object(
            criticality=5, access_count=0, max_access_count=0,
            node_risk_score=0.8, healthy_verified_replicas=0,
            required_replica_count=1, size_bytes=10_000_000,
            w1=W1, w2=W2, w3=W3, w4=W4, gamma=1.0,
        )
        small = score_object(
            criticality=5, access_count=0, max_access_count=0,
            node_risk_score=0.8, healthy_verified_replicas=0,
            required_replica_count=1, size_bytes=100,
            w1=W1, w2=W2, w3=W3, w4=W4, gamma=1.0,
        )
        assert large is not None and small is not None
        assert large["priority"] == pytest.approx(small["priority"], rel=1e-9)
        assert large["priority_rate"] < small["priority_rate"]

    def test_min_size_bytes_clamped_to_1(self):
        """size_bytes=0 must not cause division by zero."""
        result = score_object(
            criticality=3, access_count=0, max_access_count=0,
            node_risk_score=0.5, healthy_verified_replicas=1,
            required_replica_count=2, size_bytes=0,
            w1=W1, w2=W2, w3=W3, w4=W4, gamma=0.5,
        )
        assert result is not None
        assert math.isfinite(result["priority_rate"])

    def test_all_terms_present(self):
        result = score_object(
            criticality=2, access_count=1, max_access_count=4,
            node_risk_score=0.3, healthy_verified_replicas=1,
            required_replica_count=2, size_bytes=512,
            w1=W1, w2=W2, w3=W3, w4=W4,
        )
        assert result is not None
        for key in ("priority", "priority_rate", "c_term", "a_term", "pf_term", "r_term",
                    "healthy_replicas", "gamma"):
            assert key in result

    def test_known_values(self):
        """Hand-computed example using default weights."""
        # C = (3-1)/4 = 0.5
        # A = log(1+1)/log(1+1) = 1.0  (max_count=1, access_count=1)
        # Pf = 0.0
        # R = 1 - min(2,2)/2 = 0.0
        # Priority = 0.40*0.5 + 0.20*1.0 + 0.25*0.0 + 0.15*0.0 = 0.40
        result = score_object(
            criticality=3, access_count=1, max_access_count=1,
            node_risk_score=0.0, healthy_verified_replicas=2,
            required_replica_count=2, size_bytes=1,
            w1=W1, w2=W2, w3=W3, w4=W4,
        )
        assert result is not None
        assert result["c_term"] == pytest.approx(0.5)
        assert result["a_term"] == pytest.approx(1.0)
        assert result["pf_term"] == pytest.approx(0.0)
        assert result["r_term"] == pytest.approx(0.0)
        expected = 0.40 * 0.5 + 0.20 * 1.0 + 0.25 * 0.0 + 0.15 * 0.0
        assert result["priority"] == pytest.approx(expected, rel=1e-9)


# ===========================================================================
# 5. rank_objects — multi-object ranking and tie-breaking
# ===========================================================================

def _make_obj(
    oid: str,
    criticality: int = 3,
    access_count: int = 0,
    size_bytes: int = 100,
    required_replica_count: int = 2,
    replicas=None,
    node_risk: float = 0.0,
) -> dict:
    if replicas is None:
        replicas = []
    return {
        "object_id": oid,
        "criticality": criticality,
        "access_count": access_count,
        "size_bytes": size_bytes,
        "required_replica_count": required_replica_count,
        "replicas": replicas,
        "_node_risk_score": node_risk,
    }


class TestRankObjects:
    def test_empty_list(self):
        result = rank_objects([], healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)
        assert result == []

    def test_excluded_r0_absent(self):
        objs = [_make_obj("a", required_replica_count=0)]
        result = rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)
        assert result == []

    def test_higher_criticality_ranks_first(self):
        objs = [
            _make_obj("low", criticality=1),
            _make_obj("high", criticality=5),
        ]
        ranked = rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)
        assert ranked[0]["object_id"] == "high"
        assert ranked[0]["rank"] == 1

    def test_rank_field_starts_at_one(self):
        objs = [_make_obj("a"), _make_obj("b")]
        ranked = rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)
        ranks = [r["rank"] for r in ranked]
        assert ranks == sorted(ranks)
        assert ranks[0] == 1

    def test_deterministic_tie_breaking_by_healthy_replicas_then_id(self):
        """When priority is equal, fewer healthy replicas wins, then smaller object_id."""
        # All same criticality, no access, no risk, same r=1, no healthy replicas
        # => identical priority, same healthy_replicas => tie-break by object_id
        objs = [
            _make_obj("z-obj", criticality=3, required_replica_count=1),
            _make_obj("a-obj", criticality=3, required_replica_count=1),
        ]
        ranked = rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)
        # object_id 'a-obj' < 'z-obj'
        assert ranked[0]["object_id"] == "a-obj"

    def test_fewer_healthy_replicas_ranks_higher_on_tie(self):
        """With equal priority score, fewer healthy replicas means higher urgency."""
        healthy = ["node-1"]
        objs = [
            _make_obj("with-replica", criticality=3, replicas=[_replica("node-1")]),
            _make_obj("no-replica", criticality=3, replicas=[]),
        ]
        ranked = rank_objects(objs, healthy_node_ids=healthy, w1=W1, w2=W2, w3=W3, w4=W4)
        # no-replica has h=0 => higher R, so it ranks first
        assert ranked[0]["object_id"] == "no-replica"

    def test_gamma_zero_equals_base_in_rank(self):
        """rank_objects with gamma=0 must produce same order as base priority."""
        objs = [_make_obj("a", criticality=5, size_bytes=100_000), _make_obj("b", criticality=1)]
        rank_base = rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4, gamma=0.0)
        rank_rate = rank_objects(
            objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4, gamma=0.0, use_rate=True
        )
        assert [r["object_id"] for r in rank_base] == [r["object_id"] for r in rank_rate]
        for rb, rr in zip(rank_base, rank_rate):
            assert rb["priority"] == pytest.approx(rr["priority_rate"], rel=1e-9)

    def test_gamma_inverts_order_for_large_vs_small(self):
        """gamma=1 should flip ranking when size is the only difference."""
        # Large object: same criticality, same everything, but huge size
        large = _make_obj("large", criticality=5, size_bytes=1_000_000, required_replica_count=1)
        small = _make_obj("small", criticality=5, size_bytes=1, required_replica_count=1)
        # With gamma=0 both have equal priority (same criticality etc.)
        ranked_0 = rank_objects(
            [large, small], healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4, gamma=0.0, use_rate=True
        )
        # Both equal => tie-break by id
        assert ranked_0[0]["object_id"] == "large"  # 'l' > ... no wait 'l' < 's' alphabetically
        # Actually 'large' < 'small', so 'large' wins tie-break
        # With gamma=1, small should rank first
        ranked_1 = rank_objects(
            [large, small], healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4, gamma=1.0, use_rate=True
        )
        assert ranked_1[0]["object_id"] == "small"

    def test_score_components_present_in_output(self):
        objs = [_make_obj("x")]
        ranked = rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)
        for key in ("priority", "priority_rate", "c_term", "a_term", "pf_term", "r_term",
                    "healthy_replicas", "rank"):
            assert key in ranked[0], f"missing key: {key}"

    def test_deterministic_repeated_calls(self):
        """Same inputs => same output on repeated calls."""
        objs = [_make_obj(f"obj-{i}", criticality=(i % 5) + 1) for i in range(10)]
        r1 = [o["object_id"] for o in rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)]
        r2 = [o["object_id"] for o in rank_objects(objs, healthy_node_ids=[], w1=W1, w2=W2, w3=W3, w4=W4)]
        assert r1 == r2


# ===========================================================================
# 6. Weight validation (PRD FR-06: non-negative weights required)
# ===========================================================================

class TestWeightValidation:
    def test_negative_w1_raises(self):
        with pytest.raises(Exception):
            WeightsConfig(w1_criticality=-0.1, w2_access_frequency=0.2,
                          w3_node_risk=0.25, w4_redundancy=0.15)

    def test_negative_w2_raises(self):
        with pytest.raises(Exception):
            WeightsConfig(w1_criticality=0.4, w2_access_frequency=-0.01,
                          w3_node_risk=0.25, w4_redundancy=0.15)

    def test_zero_weights_allowed(self):
        """Weights of 0 are non-negative and must be accepted."""
        cfg = WeightsConfig(w1_criticality=0.0, w2_access_frequency=0.0,
                            w3_node_risk=0.0, w4_redundancy=0.0)
        assert cfg.w1_criticality == 0.0

    def test_default_weights_are_valid(self):
        cfg = WeightsConfig()
        assert cfg.w1_criticality == pytest.approx(0.40)
        assert cfg.w2_access_frequency == pytest.approx(0.20)
        assert cfg.w3_node_risk == pytest.approx(0.25)
        assert cfg.w4_redundancy == pytest.approx(0.15)


# ===========================================================================
# 7. Destination selection (FR-07)
# ===========================================================================

class TestSelectDestinations:
    def _base_candidates(self):
        return [
            _node("node-1", status="healthy", risk=0.1, load=0.3, free=50_000),
            _node("node-2", status="healthy", risk=0.2, load=0.4, free=80_000),
            _node("node-3", status="healthy", risk=0.3, load=0.5, free=30_000),
        ]

    def test_source_excluded(self):
        candidates = self._base_candidates()
        result = select_destinations(
            source_node_id="node-1",
            object_size_bytes=1000,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=0.9,
            max_destination_load=0.9,
            reserve_margin_bytes=0,
        )
        assert all(n["node_id"] != "node-1" for n in result)

    def test_degraded_node_excluded(self):
        candidates = [
            _node("node-1", status="healthy"),
            _node("node-2", status="degraded"),
        ]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=100,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=0.9,
            max_destination_load=0.9,
            reserve_margin_bytes=0,
        )
        assert all(n["node_id"] != "node-2" for n in result)

    def test_high_risk_excluded(self):
        candidates = [
            _node("node-1", risk=0.3),
            _node("node-2", risk=0.9),
        ]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=100,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=0.5,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
        )
        assert all(n["node_id"] != "node-2" for n in result)
        assert any(n["node_id"] == "node-1" for n in result)

    def test_high_load_excluded(self):
        candidates = [
            _node("node-1", load=0.3),
            _node("node-2", load=0.95),
        ]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=100,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=0.8,
            reserve_margin_bytes=0,
        )
        assert all(n["node_id"] != "node-2" for n in result)

    def test_insufficient_capacity_excluded(self):
        candidates = [
            _node("node-1", free=1000),
            _node("node-2", free=500),
        ]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=800,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
        )
        # node-2 free=500 < 800 => excluded; node-1 free=1000 >= 800 => ok
        assert all(n["node_id"] != "node-2" for n in result)
        assert any(n["node_id"] == "node-1" for n in result)

    def test_reserve_margin_reduces_effective_capacity(self):
        """Effective capacity = free - reserve_margin. Must satisfy >= object size."""
        candidates = [_node("node-1", free=1000)]
        # reserve=500, size=600 => 1000-500=500 < 600 => excluded
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=600,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=500,
        )
        assert result == []

        # reserve=100, size=600 => 1000-100=900 >= 600 => ok
        result2 = select_destinations(
            source_node_id="other",
            object_size_bytes=600,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=100,
        )
        assert len(result2) == 1

    def test_already_has_replica_excluded(self):
        """Node that already holds a verified valid replica must be excluded (spread)."""
        existing = [_replica("node-1")]
        candidates = [_node("node-1"), _node("node-2")]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=100,
            existing_replicas=existing,
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
        )
        assert all(n["node_id"] != "node-1" for n in result)

    def test_returns_sorted_by_free_bytes_descending(self):
        candidates = [
            _node("node-a", free=1000),
            _node("node-b", free=5000),
            _node("node-c", free=3000),
        ]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=100,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
            needed=3,
        )
        frees = [n["free_bytes"] for n in result]
        assert frees == sorted(frees, reverse=True)

    def test_needed_limits_results(self):
        candidates = [_node(f"node-{i}", free=1000 * i) for i in range(1, 5)]
        result = select_destinations(
            source_node_id="other",
            object_size_bytes=10,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
            needed=2,
        )
        assert len(result) <= 2

    def test_no_feasible_returns_empty(self):
        result = select_destinations(
            source_node_id="node-1",
            object_size_bytes=999_999,
            existing_replicas=[],
            candidate_nodes=[_node("node-1", free=100)],
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
        )
        assert result == []

    def test_is_feasible_convenience_wrapper(self):
        candidates = [_node("node-2", free=10_000)]
        assert is_feasible(
            source_node_id="node-1",
            object_size_bytes=500,
            existing_replicas=[],
            candidate_nodes=candidates,
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
        )
        assert not is_feasible(
            source_node_id="node-1",
            object_size_bytes=500,
            existing_replicas=[],
            candidate_nodes=[],
            max_destination_risk=1.0,
            max_destination_load=1.0,
            reserve_margin_bytes=0,
        )


# ===========================================================================
# 8. GET /priority endpoint integration tests
# ===========================================================================

class TestPriorityEndpoint:
    def _upload(self, client, filename="test.txt", content=b"hello", criticality="3", rrc="2"):
        return client.post(
            "/objects",
            files={"file": (filename, io.BytesIO(content), "text/plain")},
            data={"criticality": criticality, "required_replica_count": rrc},
        )

    def test_empty_returns_empty_items(self, api_setup):
        resp = api_setup["client"].get("/priority")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["total"] == 0

    def test_returns_ranked_items_with_score_fields(self, api_setup):
        client = api_setup["client"]
        self._upload(client, "a.txt", b"abc", criticality="5", rrc="2")
        self._upload(client, "b.txt", b"xyz", criticality="1", rrc="2")

        resp = client.get("/priority")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 2

        # Higher criticality must rank first
        assert items[0]["criticality"] == 5
        assert items[0]["rank"] == 1

        # Check score fields present
        for field in ("priority", "priority_rate", "c_term", "a_term", "pf_term", "r_term"):
            assert field in items[0], f"missing: {field}"

    def test_gamma_zero_equals_base_priority(self, api_setup):
        """Endpoint: gamma=0 must return priority_rate == priority."""
        client = api_setup["client"]
        self._upload(client, "file.bin", b"X" * 1000, criticality="4", rrc="1")

        resp = client.get("/priority?gamma=0.0")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["priority"] == pytest.approx(item["priority_rate"], rel=1e-9)

    def test_weights_returned_in_response(self, api_setup):
        resp = api_setup["client"].get("/priority")
        data = resp.json()
        assert "weights" in data
        w = data["weights"]
        assert "w1_criticality" in w
        assert "w4_redundancy" in w

    def test_pagination(self, api_setup):
        client = api_setup["client"]
        for i in range(5):
            self._upload(client, f"f{i}.txt", f"data{i}".encode(), criticality=str((i % 5) + 1))

        p1 = client.get("/priority?limit=3&offset=0").json()
        p2 = client.get("/priority?limit=3&offset=3").json()
        assert p1["total"] == 5
        assert len(p1["items"]) == 3
        assert len(p2["items"]) == 2
        ids1 = {i["object_id"] for i in p1["items"]}
        ids2 = {i["object_id"] for i in p2["items"]}
        assert ids1.isdisjoint(ids2)

    def test_unknown_node_id_returns_404(self, api_setup):
        resp = api_setup["client"].get("/priority?node_id=does-not-exist")
        assert resp.status_code == 404

    def test_node_id_filter(self, api_setup):
        client = api_setup["client"]
        # Upload to all nodes (placement picks best free node)
        self._upload(client, "g1.txt", b"G1", criticality="5")
        resp = client.get("/priority?node_id=node-1")
        # Either 0 or 1 items, but must succeed
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            authoritative_nodes = [
                r["node_id"] for r in item["replicas"] if r.get("is_authoritative")
            ]
            assert "node-1" in authoritative_nodes

    def test_ranking_is_deterministic_across_calls(self, api_setup):
        client = api_setup["client"]
        for i in range(4):
            self._upload(client, f"det{i}.txt", f"data{i}".encode(), criticality=str(i + 1))

        r1 = [o["object_id"] for o in client.get("/priority").json()["items"]]
        r2 = [o["object_id"] for o in client.get("/priority").json()["items"]]
        assert r1 == r2
