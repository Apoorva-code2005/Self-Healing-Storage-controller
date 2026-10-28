# Self-Healing Cloud Storage: Product Requirements Document (PRD)

**A Predictive, Priority-Driven Data Migration and Recovery Framework**

*Version 1.1 | 28 October 2026 | Academic prototype*

*Prepared as an implementation-ready specification for an academic software prototype. Revised from v1.0 after review.*

## 1. Document Purpose

This PRD defines the product goals, user needs, functional requirements, architecture, data model, APIs, safety rules, implementation phases, testing strategy, and evaluation plan for a college project that automatically identifies risky storage nodes, prioritizes important data, migrates it to healthy storage, restores redundancy, and verifies integrity.

The document is intended to be used as the primary specification in Google Antigravity or another AI-assisted development environment. Implement the project incrementally and validate each phase; do not generate the entire system in one unreviewed step.

**Version 1.1** resolves five logic gaps found in review of v1.0 (failure semantics, telemetry bridge, virtual capacity, priority formula, experiment lead time) and adds implementation decisions that were previously unspecified. Section 1.1 lists every change.

### 1.1 Changes in v1.1

| Change | What was fixed | Sections |
|---|---|---|
| Failure semantics | Reactive recovery was undefined: a hard-failed node cannot be migrated from. Added degraded vs. hard-failure states, evacuation vs. re-replication, and a mixed starting replica policy. | 7.3, 8 (FR-08, FR-10, FR-11), 9, 16 |
| Telemetry bridge | Backblaze SMART model could not score MinIO nodes. Added replay of held-out real drive time series as node telemetry. | 10.1, 10.5, FR-03 |
| Virtual capacity | Containers share one host disk, so reported free space is meaningless. Added controller-enforced virtual quotas. | 7.4, FR-01, 11 |
| Priority formula | Node risk term is constant within a node; size was ignored. Documented the quirk and added a size-aware variant (Mode C+). | 9, FR-06, 16.1 |
| Experiment lead time | Predictive modes only win if prediction precedes failure. Added lead-time parameter, false-alarm and missed-prediction scenarios, and a lead-time sweep. | 16.2, FR-11 |
| Implementation decisions | Added job runner, SQLite WAL, per-object locking, storage adapter, dashboard framework, MinIO version check, scope tiers. | 7.5, 19, 21 |
| Document fixes | Repository tree rewritten as a proper tree; static contents list; data model de-duplicated. | Contents, 11, 20 |

## 2. Product Summary

Product name: Self-Healing Cloud Storage.

Core idea: move from reactive recovery after a failure to risk-aware recovery before a predicted failure. When a node's estimated failure risk crosses a configurable threshold, the controller ranks objects using criticality, access frequency, node risk, and redundancy vulnerability; selects a suitable destination; copies data there (evacuation); verifies the copy with SHA-256; restores the configured replica count; and records the outcome. If a node hard-fails before or during evacuation, the controller falls back to re-replication from surviving copies.

Target delivery: a reproducible software prototype running on a development laptop with Docker Compose, Python, an S3-compatible object store (MinIO by default), FastAPI, scikit-learn, and a dashboard. Physical hardware and a paid cloud account are not required.

## 3. Problem Statement

Traditional reactive recovery may begin only after a disk or node failure. During detection and repair, data can be unavailable or have fewer healthy copies than required, and objects stored with a single copy are lost outright. A rebuild that treats all objects equally may spend time copying large, rarely accessed files while critical, frequently accessed objects remain exposed.

The project addresses this by combining telemetry monitoring, risk estimation, priority-based migration, destination selection, redundancy restoration, and checksum verification in a closed-loop controller.

## 4. Goals and Non-Goals

### 4.1 Goals

- Demonstrate a working multi-node object-storage prototype.
- Collect, simulate, or replay node-health telemetry and expose node status.
- Train and evaluate a failure-risk model using Backblaze Drive Stats data when practical, and use held-out real drive histories to drive node telemetry in replay scenarios.
- Support configurable priority scoring for objects on risky nodes, including a size-aware variant.
- Choose destinations using virtual capacity, health/risk, load, and replica-placement constraints.
- Automate migration, integrity verification, metadata updates, and redundancy restoration.
- Inject controlled faults (including false alarms and missed predictions) and compare recovery strategies.
- Provide observable logs, APIs, dashboards, and reproducible experiment results.

### 4.2 Non-goals

- Production-grade replacement for Ceph, MinIO distributed deployments, or a managed cloud storage provider.
- Guaranteeing prediction of every real-world disk failure.
- Using synthetic telemetry as proof of real-world predictive accuracy.
- Building a production multi-region service, billing system, enterprise IAM, or disaster-recovery service.
- Deleting original data before a destination copy and configured redundancy are verified.

## 5. Users and Use Cases

### Primary user: project developer / evaluator

- Start and inspect storage nodes.
- Upload test files and assign criticality and replica-count metadata.
- View node health and failure-risk estimates.
- Trigger a simulated fault and observe automated recovery.
- Inspect why a file received its priority score.
- Confirm destination selection, checksums, replica count, and audit logs.
- Run identical scenarios against baseline and proposed recovery modes.

### Secondary user: storage administrator (simulated)

- Adjust thresholds, priority weights, reserve capacity, virtual quotas, and replica policy.
- Review alerts, failed migrations, capacity constraints, lost objects, and experiment reports.

## 6. Product Scope and MVP

The minimum viable product (MVP) consists of three independently managed S3-compatible storage instances (MinIO by default), a Python controller, a metadata store, synthetic fault injection, a baseline failure-risk estimator, a priority engine, safe file migration, SHA-256 verification, replica tracking, REST APIs, and a simple dashboard.

For the first version, use Docker Compose and a local metadata database (SQLite in WAL mode). A later phase may add PostgreSQL, Prometheus/Grafana, real SMART telemetry, or a more sophisticated model. The storage instances in this prototype are separate endpoints coordinated by the controller; do not describe them as a production distributed cluster.

### 6.1 Scope tiers (new in v1.1)

| Tier | Contents | Phases |
|---|---|---|
| Tier 1: core | Three nodes, metadata, telemetry and fault injection, baseline threshold risk score, priority engine, placement, migration, verification, re-replication, basic dashboard, Modes A/B/C experiments. | 1, 2, 3, 5, 6, 7 (basic), 8 |
| Tier 2: extended | Backblaze data preparation, Random Forest model, telemetry replay scenarios, Mode C+ size-aware ablation, lead-time sweep. | 4, 8 |
| Tier 3: stretch | Isolation Forest, XGBoost/LSTM, PostgreSQL, Prometheus/Grafana, richer dashboard. | 7, optional |

Finish Tier 1 end to end before starting Tier 2. A complete Tier 1 with honest experiments is a better project than a half-working Tier 3.

## 7. System Architecture

Logical pipeline: Telemetry Collector → Anomaly Detection / Failure Predictor → Recovery Decision Controller → Priority Engine → Destination Selector → Migration Worker → Checksum Verifier → Redundancy Manager → Metadata and Audit Log → Dashboard.

### 7.1 Components

- **Storage nodes:** three S3-compatible services (MinIO by default), each with its own persistent volume and API endpoint.
- **Storage adapter:** a thin interface over an S3 client (for example boto3) so the object store can be swapped without touching controller logic. Includes a fencing switch used by fault injection.
- **Telemetry collector:** gathers reachable/unreachable status, latency, error counts, capacity, load, and synthetic, replayed, or observed disk-health features.
- **Prediction service:** returns a risk score and model/version metadata; supports a simple threshold baseline and an optional trained classifier.
- **Priority engine:** normalizes object metadata and calculates a transparent, configurable score.
- **Destination selector:** filters out unhealthy nodes and checks virtual capacity, reserve margin, load, and replica-placement constraints.
- **Migration worker:** copies object bytes to the target, retries transient errors safely, and reports progress.
- **Integrity verifier:** compares source and destination SHA-256 checksums.
- **Redundancy manager:** tracks valid replicas and creates missing copies where feasible.
- **Metadata store:** records objects, nodes, replicas, migration jobs, predictions, scenarios, and experiment results.
- **Dashboard:** displays node health, risk, ranked objects, migration status, integrity, and evaluation metrics.

### 7.2 Architecture constraints

- Every migration must be idempotent or detect an already completed copy.
- The source copy must remain until the destination copy is verified and the required protection policy is satisfied.
- A node that is unreachable or exceeds configured risk/load limits must not be selected as a destination.
- If no valid destination exists, record a blocked/partial recovery state and alert; never report success.
- Use UTC timestamps internally and document units for all telemetry fields.

### 7.3 Failure model and recovery semantics (new in v1.1)

The prototype distinguishes two node conditions and two recovery operations. These definitions are normative; every service, test, and metric must use them.

| Term | Definition | Consequence |
|---|---|---|
| Degraded (at-risk) node | Risk score at or above threshold, or anomaly flagged. Node is still reachable and readable. | Replicas on it are readable but do not count as healthy. Recovery is evacuation. |
| Hard-failed node | Node is unreachable or fenced. All data on it is inaccessible. | Replicas on it are lost for recovery purposes. Recovery is re-replication from surviving replicas. |
| Evacuation (Modes B, C, C+) | Copy an object from a degraded node to a healthy target, verify it, and register a new replica. The source replica is retained in the MVP. | Increases healthy verified replicas. |
| Re-replication (all modes) | Copy an object from a surviving verified replica on a reachable node to a healthy target to restore the replica target. | Only possible if at least one verified replica survives. |

**Healthy verified replica:** a replica with verified = true on a node that is reachable and not flagged degraded. Replica counts used for redundancy vulnerability, data-at-risk, and recovery status are always counts of healthy verified replicas.

**Lost object:** an object with zero verified replicas on reachable nodes after a hard failure. Lost objects are recorded, reported in every experiment, and never counted as recovered. In particular, an object with required_replica_count = 1 stored on a node that hard-fails before evacuation completes is lost in every mode.

**Mixed replica policy:** test datasets must contain objects with different required replica counts (default distribution in Section 24). A dataset where every object has the same replica count makes redundancy vulnerability constant and removes the difference between modes.

**Fallback during predictive recovery:** if a node hard-fails while an evacuation is in progress or before it starts (a missed or late prediction), the controller switches the remaining objects on that node to re-replication, using the same ordering rule as the active mode.

### 7.4 Virtual capacity and failure domains (new in v1.1)

Containers on one laptop report the capacity of the same host disk, so native free-space figures are not meaningful per node. Each node therefore has a virtual capacity quota defined in configuration. The controller computes free_bytes as virtual_capacity_bytes minus the sum of sizes of replicas registered on the node, and rejects any write that would exceed the quota. Capacity-pressure scenarios (FR-11) change quotas or pre-fill nodes through the controller.

All three nodes share one host and one physical disk. Node-level separation is therefore a simplified, logical failure-domain policy. Reports must state this and must not claim production-grade reliability.

### 7.5 Implementation decisions (new in v1.1)

- **Language and API:** Python 3.11 or later, FastAPI, Pydantic for schemas, pytest for tests.
- **Object-store client:** boto3 (or equivalent S3 client) behind the storage adapter. Phase 1 must verify that the pinned MinIO image pulls and runs, and record the exact tag and digest. If it cannot be used, substitute another S3-compatible store (for example Garage or SeaweedFS) and record the change.
- **Metadata store:** SQLite with WAL mode and a busy timeout; a single controller process writes. Use migrations (for example Alembic) or a versioned schema script.
- **Job runner:** a database-backed job queue consumed by an asyncio background worker inside the controller process. Concurrency limit and simulated bandwidth cap are configurable. API requests create jobs and return immediately.
- **Locking:** one active job per object, enforced by a lease row (job ID, owner, expiry) in the database. Expired leases are reclaimed during reconciliation.
- **Scenario clock:** experiments run on a scenario clock with a fixed tick (default 1 s) and a configurable simulated bandwidth, so lead time and recovery time are comparable across runs. Wall-clock timings are recorded alongside.
- **Dashboard:** Streamlit reading the REST API, unless the team prefers another framework. Keep it read-mostly.

## 8. Functional Requirements

### FR-01: Storage node management

The system shall configure at least three storage endpoints, test connectivity, report node status, and track free capacity against a configured virtual quota (Section 7.4). Each node shall have a stable node ID and independent persistent data volume.

### FR-02: Object upload and metadata

The system shall upload/download test objects and store object ID, name, origin node, size in bytes, criticality, access count or normalized access frequency, required replica count, replica locations, checksum, creation time, last access time, and status. Replica count and authoritative location are derived from Replica records, not stored redundantly on the object.

### FR-03: Telemetry ingestion

The system shall accept real, synthetic, or replayed telemetry with timestamp, node ID, latency, error rate/count, temperature if available, disk-health attributes if available, capacity, load, and reachability. Each field must be labelled with a source type: observed, simulated, or replayed. Replayed fields come from historical Backblaze records of a held-out drive (Section 10.5) and must carry the drive identifier and original record date.

### FR-04: Anomaly detection

The system shall provide a baseline threshold or rolling-statistics detector. Isolation Forest may be added as an optional unsupervised detector. The system shall log the signal that triggered an anomaly.

### FR-05: Failure-risk prediction

The system shall provide a baseline risk score and an optional supervised model trained on Backblaze Drive Stats. The prediction target and horizon must be explicitly defined. The model shall be evaluated on a time-aware or drive-aware holdout to reduce leakage. Report precision, recall, F1, false-positive rate, and confusion matrix; prioritize recall where missing a failure is costly, while documenting false alarms.

### FR-06: Priority scoring

When risk crosses the configured threshold, the system shall rank objects on the affected node using a configurable score. Base formula: Priority(d) = w1*C(d) + w2*A(d) + w3*Pf(node) + w4*R(d). C is normalized criticality; A is normalized access frequency; Pf is node failure risk; R is redundancy vulnerability (Section 9). Normalize all terms to [0,1], validate non-negative weights, and document whether weights sum to 1. These weights are tunable experimental parameters, not assumed optimal values.

The system shall also support a size-aware variant, PriorityRate(d) = Priority(d) / max(size_bytes(d), 1)^gamma, with gamma configurable in [0,1]. Gamma = 0 reproduces the base score exactly.

### FR-07: Destination selection

The system shall exclude source, risky, and unreachable nodes, require target free virtual capacity >= object size + reserve margin, respect configured maximum risk and load, and attempt to spread replicas across separate nodes. State that node-level separation is a simplified failure-domain policy (Section 7.4).

### FR-08: Migration

The system shall copy ranked objects to selected targets as jobs of type evacuate or re-replicate, expose job states (queued, running, verifying, succeeded, failed, blocked), retry safe transient failures, record bytes transferred and elapsed time, and avoid marking a migration successful before verification. Re-replication jobs read from a surviving verified replica, never from a hard-failed node.

### FR-09: Integrity verification

The system shall calculate SHA-256 for the source and destination byte streams and compare them. A mismatch must mark verification as failed, preserve the source, avoid registering the unverified target as a verified replica, and raise an alert.

### FR-10: Redundancy restoration

The system shall maintain a configurable replica target per object and record the locations of verified copies. It shall create missing replicas where capacity and policy permit. Recovery status must distinguish three conditions: evacuation complete, redundancy fully restored (healthy verified replicas >= required), and object lost (Section 7.3).

### FR-11: Failure injection

The system shall support controlled scenarios including node unavailable, latency spike, error-rate increase, low capacity, and simultaneous risky nodes. Each scenario file shall define a prediction outcome so that experiments cover all three cases:

| Prediction outcome | Scenario behaviour | Purpose |
|---|---|---|
| True alarm | Risk crosses threshold, then the node hard-fails after lead_time_s seconds. | Measures benefit of early action. |
| False alarm | Risk crosses threshold but the node never fails. | Measures wasted bytes and unnecessary movement. |
| Missed prediction | Node hard-fails without a prior threshold crossing. | Shows that predictive modes degrade to reactive behaviour, not worse. |

Scenarios must be labelled synthetic or replay, and repeatable using a seed or saved scenario file. A node is failed by fencing it in the storage adapter; stopping the container is optional and secondary. Failure injection is disabled outside development mode.

### FR-12: Audit and observability

The system shall record timestamp, trigger reason, model score, score components, destination decision, job type, migration state, checksum result, healthy verified replica count, failure reason, and final outcome. Secrets must not be logged.

### FR-13: Evaluation

The system shall run the recovery modes defined in Section 16.1 (A, B, C, and optionally C+) on identical saved scenarios. It shall export comparable results to CSV/JSON and report repeated-run statistics.

### FR-14: Dashboard

The dashboard shall show node status, telemetry trend, risk score, alerts, ranked files and score breakdown, destination choices, migration job state, checksum result, replicas, lost objects, and baseline comparison charts.

### FR-15: Concurrency and locking (new in v1.1)

The system shall allow at most one active job per object, using a database lease with expiry. Concurrent job count and simulated bandwidth shall be configurable. After a controller restart, expired leases are reclaimed and in-progress jobs are reconciled against persistent metadata and actual object-store state.

## 9. Priority Algorithm Specification

Inputs are normalized to [0,1].

- **Criticality C(d):** assigned by a user or policy on a 1 to 5 scale and mapped as (level - 1) / 4.
- **Access frequency A(d):** documented normalization such as log scaling, log(1 + count) / log(1 + max_count), or min-max scaling over the evaluation set.
- **Node failure risk Pf(node):** shared by all objects on the same source node.
- **Redundancy vulnerability R(d):** with r = required replica count and h = healthy verified replicas, R(d) = 1 - min(h, r) / r. Examples: r=1, h=0 gives 1.0; r=2, h=1 gives 0.5; r=3, h=1 gives 0.667; h >= r gives 0. If r = 0 the object is excluded.

**Known property of the formula.** Because Pf(node) is identical for every object on a node, the w3 term never changes the ranking among objects on that node. It affects ordering only when several risky nodes are processed together, and it matters for the trigger decision. This is expected, not a bug. Reports should say so and may include an ablation with w3 = 0.

**Size blindness and Mode C+.** The base score ignores object size, so one very large critical object can delay many small critical objects. The size-aware variant divides the score by size raised to gamma. Evaluate gamma in {0, 0.5, 1} and report the effect on critical-data recovery time and total bytes at risk.

Initial example weights for experiments only: w1=0.40, w2=0.20, w3=0.25, w4=0.15. Keep weights configurable in a configuration file or database. Compare more than one weight setting where time permits.

Sort in descending priority. Use deterministic tie-breakers (fewer healthy verified replicas, then smaller object ID) so runs are reproducible. Destination feasibility is a hard constraint, not a component that can be overridden by a high priority score.

## 10. Data and ML Plan

### 10.1 Data sources

- Backblaze Hard Drive Stats public dataset for real drive telemetry and failure labels.
- Synthetic node telemetry for repeatable prototype faults and for fields not represented in the dataset.
- Replayed Backblaze drive histories as node telemetry in replay scenarios (Section 10.5).
- Synthetic object metadata for criticality, access frequency, size, and required replica count.

### 10.2 Data preparation

- Inspect actual columns and schema before coding; do not assume every SMART attribute is present.
- Define a target label and horizon, and exclude identifiers/timestamps or features that leak the target.
- Handle missing values and severe class imbalance; compare class weighting before using SMOTE, and apply resampling only inside training folds.
- Split by time and/or drive identity as appropriate. Do not randomly mix near-duplicate records from the same drive across train and test without justification.
- Reserve a set of drives (both failing and healthy) as a replay pool that is excluded from training and tuning.
- Save preprocessing steps and model version together with the model.

### 10.3 Model plan

Start with transparent statistical thresholds on commonly reported SMART attributes (verify which are present in the data you downloaded). Then evaluate Random Forest as the first supervised model. Isolation Forest is optional for anomaly detection. XGBoost or LSTM are stretch goals only if time and dataset quality justify them.

### 10.4 Claims and limitations

A risk score based on synthetic telemetry is a simulation signal, not a validated real-world failure probability. If a calibrated failure probability for a stated horizon is claimed, the target, horizon, calibration procedure, and test results must support that claim.

### 10.5 Telemetry replay bridge (new in v1.1)

The Backblaze model scores drive-level SMART features, but the prototype nodes are containers that do not expose SMART data. To connect the two honestly, each node in a replay scenario is assigned one held-out drive's daily history. The telemetry collector emits that drive's records, one per scenario tick (or per fixed number of ticks, using a documented time-compression factor), labelled source_type = replayed with the drive serial and original date. The predictor scores each emitted record.

- A failing drive's recorded failure date defines the hard-failure time of its node. Lead time then emerges from when the model's score first crosses the threshold, and is reported as a measured distribution over replay scenarios.
- Healthy drives provide false-alarm and no-failure cases.
- Latency, error rate, load, and capacity for replay nodes remain simulated and labelled as such.
- Replay results demonstrate the controller on realistic risk signals. They do not prove that the prototype predicts failures of the container nodes.
- Replay drives must come from the held-out pool (Section 10.2) so that results are not inflated by training leakage.

## 11. Data Model

### Node

node_id (primary key), name, endpoint, status (healthy, degraded, failed, fenced), virtual_capacity_bytes, free_bytes (derived), load_metric, risk_score, risk_updated_at, telemetry_source, last_seen_at.

### Object

object_id (primary key), object_name, size_bytes, criticality, access_count, access_frequency_normalized, origin_node_id (informational), checksum_sha256, required_replica_count, status (ok, at_risk, restoring, lost), created_at, updated_at. Replica count and authoritative location are derived from Replica.

### Replica

replica_id, object_id, node_id, checksum_sha256, verified, is_authoritative, status (pending, valid, invalid, lost), created_at, verified_at.

### Telemetry

telemetry_id, node_id, timestamp_utc, latency_ms, error_rate, temperature_c nullable, capacity_free_bytes, load_metric, reachable, source_type (observed, simulated, replayed), replay_drive_serial nullable, replay_source_date nullable.

### Prediction

prediction_id, node_id, timestamp_utc, risk_score, model_name, model_version, target_horizon, anomaly_flag, explanation.

### MigrationJob

job_id, job_type (evacuate, re_replicate), object_id, source_node_id, target_node_id, priority_score, status, bytes_transferred, started_at, finished_at, source_checksum, target_checksum, verification_result, error_message.

### ObjectLease

object_id (primary key), job_id, owner, acquired_at, expires_at.

### Scenario

scenario_id, name, scenario_type (synthetic, replay), prediction_outcome (true_alarm, false_alarm, missed), lead_time_s nullable, seed, node_faults_json, replay_drive_map_json nullable, initial_dataset_json.

### ExperimentRun

run_id, scenario_id, recovery_mode, random_seed, start_time, end_time, metrics_json, config_snapshot_json, software_version.

## 12. API Requirements

Use FastAPI with input validation and documented error responses. Endpoint names may be adjusted, but the following capabilities are required.

- GET /health — application liveness.
- GET /nodes — node status, virtual capacity and current risk.
- GET /objects — object metadata and replica state.
- POST /objects — upload an object with metadata.
- GET /predictions — latest predictions and anomaly flags.
- POST /simulate-failure — trigger a named, bounded test scenario; disabled or protected outside development mode.
- POST /recovery/{node_id} — request recovery for a node (evacuation or re-replication as appropriate).
- GET /recovery/status — active and recent migration jobs, including lost objects.
- GET /experiments — experiment runs and summary metrics.
- GET /metrics — Prometheus-compatible metrics if Prometheus integration is enabled.

Use clear HTTP status codes, request schemas, structured errors, pagination for lists, and a request/job ID for long-running recovery operations. Do not block a web request for a long migration; create a job and return its ID.

## 13. Non-Functional Requirements

- **Reliability:** failed or interrupted migration must not corrupt authoritative metadata or delete the source.
- **Integrity:** every completed migration must have a successful checksum comparison.
- **Observability:** all important decisions and state changes must be auditable.
- **Reproducibility:** experiments must record configuration, random seed, model version, scenario, and storage image versions.
- **Maintainability:** modular Python packages, type hints for public interfaces, meaningful logs, and documented setup.
- **Portability:** local development on macOS/Linux with Docker Desktop or Docker Engine; document platform-specific issues.
- **Performance:** record measured latency and throughput; avoid claiming targets before benchmarking.
- **Security:** local-only development defaults, secrets in environment variables, no real credentials in Git, and destructive operations disabled by default.
- **Accessibility/usability:** dashboard labels should be clear, units visible, and status not conveyed by colour alone.

## 14. Safety, Failure Handling, and Security

- Never remove the last verified copy of an object.
- Do not treat a timeout as proof that a copy failed; check the target state before retrying.
- If checksum mismatch occurs, preserve source, quarantine or mark the target copy invalid, and report failure.
- If destination capacity is insufficient, mark the object blocked and continue only where policy permits.
- Never read from a hard-failed or fenced node during re-replication; if no verified replica survives, mark the object lost and alert.
- If the controller restarts, reconcile in-progress jobs and expired leases from persistent metadata and object storage.
- Use a bounded retry policy with backoff; avoid retry storms.
- Use development credentials only for local testing and keep MinIO console/API ports bound to localhost where possible.
- Validate uploaded file names and metadata; do not use untrusted file names as arbitrary filesystem paths.
- Failure injection must require explicit development mode or a deliberate operator action.
- Do not automatically delete source copies in the MVP; retention cleanup is out of scope unless separately reviewed and tested.

## 15. Observability and Dashboard Metrics

- Node reachable status and last-seen time.
- Virtual free capacity, load, latency, error rate, and telemetry source type.
- Risk score, configured threshold, model version, and prediction timestamp.
- Object priority rank and component-level score breakdown.
- Migration queue length, active jobs, bytes moved, job duration, retries, and failures.
- Checksum verification pass/fail, and replica target versus healthy verified replica count.
- Lost objects, including lost critical objects.
- Recovery metrics: MTTR, critical-data recovery time, availability, data-at-risk duration, migration overhead, wasted bytes from false alarms, realized lead time, prediction recall, false-alarm rate, integrity success rate.

## 16. Experimental Evaluation

### 16.1 Recovery modes

| Mode | Trigger | Behaviour |
|---|---|---|
| A: reactive FIFO | Hard failure of a node. | Re-replicate from surviving replicas in FIFO order. Single-replica objects on the failed node are lost. |
| B: predictive FIFO / seeded random | Risk threshold crossing. | Evacuate in FIFO or seeded-random order; fall back to re-replication if the node hard-fails first. |
| C: predictive priority | Same threshold as B. | Evacuate in descending base Priority(d) order; same fallback. |
| C+: predictive size-aware (optional ablation) | Same threshold as B. | Evacuate in descending PriorityRate(d) order for gamma in {0.5, 1}; same fallback. |

### 16.2 Experimental controls

- Use identical initial objects, node virtual capacities, scenario schedules, simulated bandwidth, and clock across modes.
- Run multiple trials with fixed seeds for repeatability and report mean plus spread (standard deviation or confidence interval where appropriate).
- Include varied file sizes, capacity pressure, single and simultaneous node faults, mixed replica policies, and different priority distributions.
- Include all three prediction outcomes (true alarm, false alarm, missed) so the comparison is not limited to cases where prediction helps.
- Sweep lead_time_s over several values, including values too short to evacuate everything, and report where each mode's advantage appears and disappears.
- In replay scenarios, report lead times as measured on held-out drives rather than as chosen parameters.
- Separate ML evaluation from storage-recovery evaluation. A good recovery result does not prove model accuracy, and high model accuracy alone does not prove faster recovery.

### 16.3 Metrics and definitions

- **MTTR:** define precisely whether it measures time to restore all required replicas or time until the service is healthy again.
- **Critical-data recovery time:** time from trigger or failure, explicitly stated, until critical objects have healthy verified replicas >= required.
- **Objects and bytes lost:** count and bytes of objects with zero verified replicas on reachable nodes, reported overall and for critical objects.
- **Availability:** successful service time divided by total measured time, using a stated availability probe.
- **Data-at-risk window:** duration in which healthy verified copies are below the required policy.
- **Migration overhead:** bytes moved and elapsed migration time; include wasted bytes moved in false-alarm scenarios.
- **Integrity success rate:** verified successful copies divided by copies that reached verification.
- **Prediction recall and false-alarm rate:** compute only against correctly labelled held-out outcomes.

## 17. Acceptance Criteria

- A fresh developer can follow README instructions to launch the prototype.
- At least three storage endpoints can be checked and used independently, and the pinned storage image version is recorded.
- A file can be uploaded, metadata stored, and a checksum recorded.
- Virtual quotas are enforced: a write that would exceed a node's quota is rejected.
- A simulated high-risk event triggers the configured recovery decision.
- The priority engine produces deterministic scores and rankings for fixed inputs, including the size-aware variant.
- Destination selection rejects unreachable, over-risk, overloaded, or insufficient-capacity nodes according to configuration.
- A successful migration has matching SHA-256 checksums and updated replica metadata.
- A checksum mismatch, unavailable destination, or interrupted copy is not marked successful.
- After a hard failure, objects with a surviving verified replica are re-replicated, and objects without one are recorded as lost and never reported as recovered.
- The dashboard or API exposes prediction, ranking, migration, verification, lost objects, and recovery state.
- All recovery modes can be run on the same saved scenario, including true-alarm, false-alarm, and missed-prediction cases, and results exported.
- Automated tests cover scoring, destination selection, checksum verification, retries, locking, and source-retention safety.
- The report distinguishes measured results, simulated results, replayed results, assumptions, and limitations.

## 18. Testing Strategy

### Unit tests

- Priority normalization, weight validation, deterministic tie-breaking, size-aware score with gamma = 0 equal to base score.
- Redundancy vulnerability mapping, including the examples in Section 9 and the h > r and r = 0 edge cases.
- Destination feasibility under virtual-capacity, risk, load, and reachability constraints.
- SHA-256 match and mismatch behaviour.
- Metadata updates only after successful verification.
- Object lease acquisition, expiry, and refusal of a second concurrent job.

### Integration tests

- Upload and download files across storage endpoints.
- Simulated risky node causes an evacuation job.
- Hard-failed node triggers re-replication from a surviving replica; a single-replica object on it is recorded lost.
- Node hard-fails mid-evacuation and the controller falls back to re-replication.
- Target becomes unavailable during migration.
- Controller restarts while jobs are pending and leases are held.
- No destination has sufficient virtual capacity.
- Redundancy restoration succeeds or reports a clear blocked state.
- False-alarm scenario completes without data loss and reports wasted bytes.

### ML tests

- Data split prevents leakage according to the chosen strategy, and replay drives are absent from training data.
- Metrics are calculated from held-out predictions.
- Model artifact loads with its preprocessing pipeline and version.

### End-to-end test

Upload objects with mixed replica policies to Node A, trigger a reproducible true-alarm scenario, observe priority ranking, evacuate the highest-priority object to Node B, verify its checksum, restore the configured replica count, hard-fail Node A after the lead time, and compare results against the reactive and other predictive modes.

## 19. Implementation Roadmap

### Phase 1: Foundation (Tier 1)

- Create repository, virtual environment, configuration, Docker Compose, and health endpoint.
- Verify that the pinned MinIO image pulls and runs; record tag and digest. Choose a fallback S3-compatible store if it does not.
- Launch three storage instances and verify connectivity through the storage adapter.

### Phase 2: Object metadata (Tier 1)

- Implement upload/download, SQLite (WAL) metadata, checksum creation, criticality, access metadata, required replica count, and virtual-capacity accounting.

### Phase 3: Failure simulation and monitoring (Tier 1)

- Implement telemetry schema with source types, synthetic scenarios, scenario clock, fencing, node health, and structured logs.

### Phase 4: Prediction (Tier 2)

- Download and inspect Backblaze data, define target/horizon, reserve the replay pool, build baseline and Random Forest model, evaluate leakage-safe splits, and implement the replay bridge.
- A baseline threshold risk score on synthetic telemetry is sufficient to complete Tier 1 before this phase.

### Phase 5: Priority and placement (Tier 1)

- Implement score configuration, ranking, size-aware variant, destination feasibility checks, and explanation of score components.

### Phase 6: Recovery (Tier 1)

- Implement job-based evacuation and re-replication, object leases, checksum verification, safe retries, replica tracking, lost-object handling, and recovery reconciliation.

### Phase 7: Dashboard and observability (Tier 1 basic, Tier 3 extras)

- Expose APIs and display node health, risk, priorities, jobs, integrity, lost objects, and recovery metrics.

### Phase 8: Experiments and report (Tier 1, with Tier 2 additions)

- Run Modes A, B, and C on true-alarm, false-alarm, and missed scenarios; add C+, the lead-time sweep, and replay scenarios when Tier 2 is available. Export results, analyze trade-offs, and document limitations.

Each phase must end with tests and a brief explanation of files changed before moving on.

## 20. Recommended Repository Structure

```text
self-healing-storage/
├── app/
│   ├── main.py
│   ├── api/
│   ├── services/
│   │   ├── telemetry.py
│   │   ├── predictor.py
│   │   ├── priority.py
│   │   ├── placement.py
│   │   ├── migration.py
│   │   ├── integrity.py
│   │   ├── redundancy.py
│   │   ├── leases.py
│   │   └── scenario_clock.py
│   ├── storage/            # storage adapter (S3 client, fencing)
│   ├── db/
│   ├── schemas/
│   └── config.py
├── config/
│   ├── default.yaml        # thresholds, weights, quotas, replica policy
│   └── scenarios/          # saved synthetic and replay scenarios
├── dashboard/
├── data/
├── models/
├── scripts/
│   ├── prepare_backblaze.py
│   ├── train_model.py
│   ├── build_replay_pool.py
│   ├── inject_failure.py
│   └── run_experiments.py
├── docs/
│   └── PRD.md
├── tests/
├── logs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## 21. Risks and Mitigations

- **Backblaze data is large:** begin with a small date range or sampled subset for development; document the sample and use a suitable held-out evaluation.
- **Rare failure labels:** use appropriate metrics, class weighting, and careful validation; do not rely on accuracy alone.
- **False alarms cause unnecessary movement:** tune thresholds and measure bytes moved and false-alarm rate.
- **No healthy destination:** block the migration, alert, and retain source data.
- **Prototype nodes do not represent independent physical failure domains:** document this limitation and avoid production-reliability claims.
- **Storage image availability:** the MinIO community image or its features may change. Pin and record the tested version, and keep logic behind the storage adapter so another S3-compatible store can replace it.
- **Replay results overstated:** replayed real drive histories drive simulated nodes; present them as controller behaviour on realistic signals, not as proof of predicting container failures.
- **Unfair mode comparison:** if lead time is always long, predictive modes always win. Include false-alarm, missed, and short-lead-time scenarios.
- **SQLite write contention:** use WAL mode, a single writer process, and short transactions.
- **Scope creep:** finish Tier 1 before Tier 2 and Tier 3.
- **Agent-generated code may be incomplete:** require tests, inspect diffs, and review destructive or security-sensitive operations.

## 22. Deliverables

- Source repository with modular, documented code.
- Docker Compose configuration for storage services.
- Metadata schema and sample test objects with a mixed replica policy.
- Backblaze data preparation, replay-pool builder, and model-training scripts.
- Prediction model artifact and evaluation report, if trained successfully.
- Priority engine (base and size-aware) and destination selector.
- Migration controller with integrity, redundancy, and lost-object handling.
- Failure-injection scripts and saved scenarios, including false-alarm and missed-prediction cases.
- REST API and dashboard.
- Automated test suite and reproducible experiment outputs.
- Final project report, architecture diagram, flowchart, presentation, and limitations section.

## 23. Instructions for Google Antigravity

Place a Markdown copy of this PRD in the workspace at docs/PRD.md (agents read Markdown more reliably than .docx) and use it as the source of truth. First inspect the current workspace. Produce a short implementation plan and a checklist mapping each phase to files and tests. Implement only Phase 1 initially, run the relevant tests, explain each created file and any commands executed, and wait for review before starting Phase 2.

Do not invent results, silently replace requirements, or claim integrations work without running them. Ask before destructive operations, large downloads, changing credentials, deleting files, or running commands that affect data outside the project folder. Keep secrets out of source control. If a requirement cannot be implemented in the current environment, explain the limitation and provide a safe alternative. Treat Sections 7.3 and 7.4 as normative definitions.

### Initial Antigravity prompt

Read docs/PRD.md in this workspace and treat it as the source of truth. Do not build everything at once. Start by summarizing the architecture, the failure model in Section 7.3, assumptions, and risks, and propose Phase 1 tasks. Then implement only Phase 1: repository structure, Python environment files, configuration, Docker Compose for three independent S3-compatible storage endpoints (MinIO by default, after verifying the pinned image pulls and recording its tag), a storage adapter with a connectivity check, and a FastAPI health endpoint. Add setup instructions and basic tests. Run what you can, report actual results and blockers, explain every created file, and wait for my approval before Phase 2.

## 24. Assumptions and Open Decisions

- The first implementation is local and educational, not production deployment.
- Storage instances are independently coordinated by the project controller; node capacity is a virtual quota.
- SQLite (WAL) is acceptable for MVP metadata; PostgreSQL can be introduced later.
- Initial prediction may use synthetic telemetry for pipeline testing, but model claims must be based on held-out Backblaze evaluation.
- The prediction horizon, threshold, replica target, reserve capacity, virtual quotas, simulated bandwidth, lead time, and priority weights must be configurable.
- Rack/zone placement is out of scope unless the environment models those failure domains.
- Any final reported performance improvement must come from measured, repeatable experiments.

Suggested starting defaults, all tunable and none validated as optimal:

| Parameter | Starting value |
|---|---|
| Required replica count distribution | 30% of objects at 1, 50% at 2, 20% at 3 |
| Criticality scale | 1 to 5, mapped to [0,1] as (level - 1) / 4 |

| Risk threshold | 0.7 (baseline score in [0,1]) |
| Reserve margin | 10% of node virtual capacity |
| Scenario tick | 1 second |
| Simulated bandwidth cap | configurable per scenario, for example 10 MB/s |
| Lead times for sweep | 30 s, 120 s, 600 s, plus a value shorter than total evacuation time |
| Size-aware gamma | 0, 0.5, 1 |
| Trials per scenario | at least 10 with distinct seeds |

## 25. Glossary

- **Object:** a file and its metadata stored by an object-storage service.
- **Node:** one independently managed storage endpoint in the prototype.
- **Degraded node:** reachable but at risk; its replicas are readable but not counted as healthy.
- **Hard-failed node:** unreachable or fenced; its data is inaccessible.
- **Evacuation:** copying data off a degraded node to a healthy one.
- **Re-replication:** creating a new replica from a surviving verified copy.
- **Telemetry:** time-stamped measurements describing node or disk behaviour; labelled observed, simulated, or replayed.
- **Failure probability / risk score:** model output associated with a precisely defined target and horizon; not automatically calibrated probability.
- **Priority score:** relative urgency assigned to an object for migration.
- **Replica:** a verified copy of an object stored on a node.
- **Healthy verified replica:** a verified replica on a node that is reachable and not degraded.
- **Lost object:** an object with zero verified replicas on reachable nodes.
- **Lead time:** time between the risk threshold crossing and the node's hard failure.
- **Checksum:** digest used to detect whether copied bytes differ from the source.
- **MTTR:** mean time to recovery, calculated according to the explicitly stated recovery endpoint.
- **Data-at-risk window:** duration during which the configured protection level is not met.
