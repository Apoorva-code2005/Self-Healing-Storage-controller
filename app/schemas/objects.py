from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class ReplicaResponse(BaseModel):
    replica_id: str
    object_id: str
    node_id: str
    checksum_sha256: str
    verified: bool
    is_authoritative: bool
    status: str
    created_at: str
    verified_at: Optional[str] = None


class ObjectResponse(BaseModel):
    object_id: str
    object_name: str
    size_bytes: int
    criticality: int
    access_count: int
    access_frequency_normalized: float
    origin_node_id: Optional[str] = None
    checksum_sha256: str
    required_replica_count: int
    status: str
    created_at: str
    updated_at: str
    last_access_at: Optional[str] = None
    replicas: List[ReplicaResponse] = Field(default_factory=list)


class UploadResponse(BaseModel):
    object_id: str
    checksum: str
    checksum_sha256: str
    object_name: str
    size_bytes: int
    criticality: int
    required_replica_count: int
    origin_node_id: str
    status: str = "ok"


class ObjectListResponse(BaseModel):
    items: List[ObjectResponse]
    total: int
    limit: int
    offset: int
