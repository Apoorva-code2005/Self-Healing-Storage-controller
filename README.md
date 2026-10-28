# Self-Healing Object Storage (Phase 3)

Implementation for the prototype self-healing storage system based on `docs/PRD.md`.

## Features Implemented

### Phase 1: Foundation
- **Repo Skeleton**: Structured according to PRD Section 20.
- **Independent Storage Nodes**: Docker Compose configuration running 3 independent MinIO/Silo instances with dedicated volumes.
- **Storage Adapter**: `boto3` client wrapper with per-node connectivity verification and fencing controls (Section 7.1).
- **FastAPI Controller**: `GET /health` and `GET /nodes`.
- **Automated Tests**: Unit and integration tests for configuration and reachability.

### Phase 2: Metadata Store, Virtual Capacity, & Object Lifecycle
- **SQLite Metadata Store**: WAL mode and busy timeout with versioned migrations for `Node`, `Object`, and `Replica` tables (no ORM migration library).
- **Virtual Capacity Accounting**: Controller-enforced per-node virtual quotas (`free_bytes = virtual_capacity_bytes - sum(replica sizes)`). Rejects writes that exceed quota with HTTP 507.
- **Initial Placement**: Automatically selects reachable node with the highest free virtual capacity.
- **Object Upload (`POST /objects`)**: Streams multipart upload, computes SHA-256 hash while streaming, validates filenames (blocks traversal, control chars), uses `object_id` as storage key, and creates authoritative verified replica records.
- **Object Download (`GET /objects/{object_id}/download`)**: Streams content from healthy reachable replica, updates last access time, increments `access_count`, and normalizes access frequency using `log(1 + count) / log(1 + max_count)`.
- **Paginated Objects List (`GET /objects`)**: Returns object metadata and replica states with `limit`, `offset`, and `total`.
- **Node Capacity Reporting (`GET /nodes`)**: Now includes `virtual_capacity_bytes`, `free_bytes`, node `status`, and `risk_score`.

### Phase 3: Telemetry, Baseline Risk Detection, & Failure Simulation
- **Telemetry Ingestion & Storage**: Records observed adapter connectivity/latency and synthetic telemetry (`latency_ms`, `error_rate`, `load_metric`, `reachable`, `source_type`) in SQLite `telemetry` table.
- **Deterministic Scenario Clock**: `app/services/scenario_clock.py` provides fixed 1s tick simulation without real sleeping.
- **Scenario Configurations**: Declarative synthetic scenarios in `config/scenarios/` with deterministic PRNG seeds covering `true_alarm`, `false_alarm`, and `missed` failure predictions.
- **Baseline Risk Detector**: Evaluates latency and error rate against thresholds (from `config/default.yaml`), flags anomalies, computes risk scores in `[0, 1]`, and records explanations in `predictions` table.
- **Failure Injection Engine (`POST /simulate-failure`)**: Executes named scenarios using adapter software fencing for hard failure; protected outside `ENV=development`.
- **Predictions API (`GET /predictions`)**: Lists latest predictions and anomaly flags with trigger explanations.
- **Structured JSON Logging**: Emits clean structured JSON logs for telemetry, anomaly, and failure injection events (no secrets).

---

## Setup & Run Instructions

### 1. Environment Setup
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
```

### 2. Start Storage Services (MinIO / Silo)
*(Do not run via automated tools per instructions; run in terminal directly)*
```bash
# Pull and start 3 independent storage nodes in background
docker compose up -d

# Check status of containers
docker compose ps
```

MinIO S3 API Endpoints:
- Node 1: `http://127.0.0.1:9001` (Console: `http://127.0.0.1:9091`)
- Node 2: `http://127.0.0.1:9002` (Console: `http://127.0.0.1:9092`)
- Node 3: `http://127.0.0.1:9003` (Console: `http://127.0.0.1:9093`)

Default Dev Credentials: `minioadmin` / `minioadmin`

### 3. Run Tests
```bash
pytest -v
```

### 4. Start the Application Server
```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

### 5. Verify Endpoints & cURL Examples

#### Health check
```bash
curl http://127.0.0.1:8000/health
```

#### Storage nodes status (with virtual capacity, status & risk score)
```bash
curl http://127.0.0.1:8000/nodes
```

#### Upload an Object (POST /objects)
```bash
curl -X POST http://127.0.0.1:8000/objects \
  -F "file=@example.txt" \
  -F "criticality=4" \
  -F "required_replica_count=2"
```

#### Download an Object (GET /objects/{object_id}/download)
```bash
curl -OJ http://127.0.0.1:8000/objects/<object_id>/download
```

#### Simulate Failure (POST /simulate-failure)
```bash
# Trigger a named scenario (e.g. true_alarm, false_alarm, missed_prediction)
curl -X POST http://127.0.0.1:8000/simulate-failure \
  -H "Content-Type: application/json" \
  -d '{"scenario_name": "true_alarm"}'
```

#### Query Predictions & Anomaly Explanations (GET /predictions)
```bash
curl "http://127.0.0.1:8000/predictions?limit=10"
```

### 6. Teardown Storage Services
```bash
docker compose down -v
```
