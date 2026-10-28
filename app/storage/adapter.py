from __future__ import annotations

import time
from typing import Dict, List, Optional, Set
import boto3
from botocore.config import Config
from pydantic import BaseModel, Field

from app.config import NodeConfig


class NodeReachability(BaseModel):
    node_id: str
    endpoint_url: str
    reachable: bool
    fenced: bool = False
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    virtual_capacity_bytes: int = 0
    free_bytes: int = 0
    status: str = "healthy"
    risk_score: float = 0.0


class StorageAdapter:
    """S3 storage adapter wrapping boto3 clients for multi-node storage.

    Provides per-node connectivity checks, bucket management, object storage,
    and a software fencing switch required for fault injection experiments per PRD Section 7.1.
    """

    def __init__(self, node_configs: List[NodeConfig], connect_timeout: float = 2.0):
        self._configs: Dict[str, NodeConfig] = {n.id: n for n in node_configs}
        self._clients: Dict[str, any] = {}
        self._fenced_nodes: Set[str] = set()
        self._connect_timeout = connect_timeout

        for node in node_configs:
            self._init_client(node)

    def _init_client(self, node: NodeConfig) -> None:
        client_config = Config(
            connect_timeout=self._connect_timeout,
            read_timeout=self._connect_timeout,
            retries={"max_attempts": 1},
        )
        self._clients[node.id] = boto3.client(
            "s3",
            endpoint_url=node.endpoint_url,
            aws_access_key_id=node.access_key,
            aws_secret_access_key=node.secret_key,
            region_name=node.region,
            config=client_config,
        )

    def fence_node(self, node_id: str) -> None:
        """Fences a node to simulate hard failure or network isolation."""
        if node_id not in self._configs:
            raise KeyError(f"Unknown node ID: {node_id}")
        self._fenced_nodes.add(node_id)

    def unfence_node(self, node_id: str) -> None:
        """Removes fencing from a node."""
        self._fenced_nodes.discard(node_id)

    def is_fenced(self, node_id: str) -> bool:
        return node_id in self._fenced_nodes

    def get_client(self, node_id: str) -> any:
        """Returns the boto3 client for a node if not fenced."""
        if node_id not in self._configs:
            raise KeyError(f"Unknown node ID: {node_id}")
        if self.is_fenced(node_id):
            raise ConnectionError(f"Node '{node_id}' is fenced (simulated failure).")
        return self._clients[node_id]

    def check_node_connectivity(self, node_id: str) -> NodeReachability:
        """Performs a per-node reachability check using S3 list_buckets."""
        if node_id not in self._configs:
            return NodeReachability(
                node_id=node_id,
                endpoint_url="unknown",
                reachable=False,
                error=f"Unknown node ID: {node_id}",
            )

        node = self._configs[node_id]
        if self.is_fenced(node_id):
            return NodeReachability(
                node_id=node.id,
                endpoint_url=node.endpoint_url,
                reachable=False,
                fenced=True,
                error="Node is fenced (simulated failure)",
                virtual_capacity_bytes=node.virtual_capacity_bytes,
                free_bytes=node.virtual_capacity_bytes,
                status="fenced",
                risk_score=1.0,
            )

        client = self._clients[node.id]
        t0 = time.perf_counter()
        try:
            client.list_buckets()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return NodeReachability(
                node_id=node.id,
                endpoint_url=node.endpoint_url,
                reachable=True,
                fenced=False,
                latency_ms=round(elapsed_ms, 2),
                error=None,
                virtual_capacity_bytes=node.virtual_capacity_bytes,
                free_bytes=node.virtual_capacity_bytes,
                status="healthy",
                risk_score=0.0,
            )
        except Exception as exc:
            return NodeReachability(
                node_id=node.id,
                endpoint_url=node.endpoint_url,
                reachable=False,
                fenced=False,
                latency_ms=None,
                error=str(exc),
                virtual_capacity_bytes=node.virtual_capacity_bytes,
                free_bytes=node.virtual_capacity_bytes,
                status="failed",
                risk_score=1.0,
            )

    def check_all_nodes(self) -> List[NodeReachability]:
        """Checks reachability for all registered storage nodes."""
        return [self.check_node_connectivity(node_id) for node_id in self._configs]

    def ensure_bucket(self, node_id: str, bucket: str = "objects") -> None:
        """Ensures the destination S3 bucket exists on the node."""
        client = self.get_client(node_id)
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            try:
                client.create_bucket(Bucket=bucket)
            except Exception:
                pass

    def put_object(self, node_id: str, key: str, data: bytes, bucket: str = "objects") -> None:
        """Stores object bytes under key on the specified node."""
        self.ensure_bucket(node_id, bucket=bucket)
        client = self.get_client(node_id)
        client.put_object(Bucket=bucket, Key=key, Body=data)

    def get_object(self, node_id: str, key: str, bucket: str = "objects") -> bytes:
        """Fetches object bytes under key from the specified node."""
        client = self.get_client(node_id)
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    def delete_object(self, node_id: str, key: str, bucket: str = "objects") -> None:
        """Deletes an object under key from the specified node."""
        client = self.get_client(node_id)
        client.delete_object(Bucket=bucket, Key=key)


class InMemoryStorageAdapter(StorageAdapter):
    """In-memory storage adapter for unit testing without Docker or external S3 services."""

    def __init__(self, node_configs: List[NodeConfig]):
        self._configs: Dict[str, NodeConfig] = {n.id: n for n in node_configs}
        self._clients: Dict[str, any] = {}
        self._fenced_nodes: Set[str] = set()
        self._unreachable_nodes: Set[str] = set()
        self._storage: Dict[str, Dict[str, bytes]] = {n.id: {} for n in node_configs}
        self._latencies: Dict[str, float] = {n.id: 1.0 for n in node_configs}
        self._errors: Dict[str, str] = {}
        self._connect_timeout = 2.0

    def set_node_reachability(self, node_id: str, reachable: bool, error: Optional[str] = None) -> None:
        if reachable:
            self._unreachable_nodes.discard(node_id)
            self._errors.pop(node_id, None)
        else:
            self._unreachable_nodes.add(node_id)
            if error:
                self._errors[node_id] = error

    def check_node_connectivity(self, node_id: str) -> NodeReachability:
        if node_id not in self._configs:
            return NodeReachability(
                node_id=node_id,
                endpoint_url="unknown",
                reachable=False,
                error=f"Unknown node ID: {node_id}",
            )
        node = self._configs[node_id]
        if self.is_fenced(node_id):
            return NodeReachability(
                node_id=node.id,
                endpoint_url=node.endpoint_url,
                reachable=False,
                fenced=True,
                error="Node is fenced (simulated failure)",
                virtual_capacity_bytes=node.virtual_capacity_bytes,
                free_bytes=node.virtual_capacity_bytes,
                status="fenced",
                risk_score=1.0,
            )
        if node_id in self._unreachable_nodes:
            return NodeReachability(
                node_id=node.id,
                endpoint_url=node.endpoint_url,
                reachable=False,
                fenced=False,
                error=self._errors.get(node_id, "Connection refused"),
                virtual_capacity_bytes=node.virtual_capacity_bytes,
                free_bytes=node.virtual_capacity_bytes,
                status="failed",
                risk_score=1.0,
            )
        return NodeReachability(
            node_id=node.id,
            endpoint_url=node.endpoint_url,
            reachable=True,
            fenced=False,
            latency_ms=self._latencies.get(node_id, 1.0),
            error=None,
            virtual_capacity_bytes=node.virtual_capacity_bytes,
            free_bytes=node.virtual_capacity_bytes,
            status="healthy",
            risk_score=0.0,
        )

    def put_object(self, node_id: str, key: str, data: bytes, bucket: str = "objects") -> None:
        if node_id not in self._configs:
            raise KeyError(f"Unknown node ID: {node_id}")
        if self.is_fenced(node_id):
            raise ConnectionError(f"Node '{node_id}' is fenced (simulated failure)")
        if node_id in self._unreachable_nodes:
            raise ConnectionError(f"Node '{node_id}' is unreachable: {self._errors.get(node_id, 'Connection refused')}")
        if node_id not in self._storage:
            self._storage[node_id] = {}
        self._storage[node_id][key] = data

    def get_object(self, node_id: str, key: str, bucket: str = "objects") -> bytes:
        if node_id not in self._configs:
            raise KeyError(f"Unknown node ID: {node_id}")
        if self.is_fenced(node_id):
            raise ConnectionError(f"Node '{node_id}' is fenced (simulated failure)")
        if node_id in self._unreachable_nodes:
            raise ConnectionError(f"Node '{node_id}' is unreachable: {self._errors.get(node_id, 'Connection refused')}")
        if node_id not in self._storage or key not in self._storage[node_id]:
            raise KeyError(f"Object '{key}' not found on node '{node_id}'")
        return self._storage[node_id][key]

    def delete_object(self, node_id: str, key: str, bucket: str = "objects") -> None:
        if node_id not in self._configs:
            raise KeyError(f"Unknown node ID: {node_id}")
        if self.is_fenced(node_id):
            raise ConnectionError(f"Node '{node_id}' is fenced (simulated failure)")
        if node_id in self._unreachable_nodes:
            raise ConnectionError(f"Node '{node_id}' is unreachable: {self._errors.get(node_id, 'Connection refused')}")
        if node_id in self._storage:
            self._storage[node_id].pop(key, None)
