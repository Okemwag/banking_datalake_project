# Banking Lakehouse Implementation Plan

This file is the execution tracker for the project. Update statuses, owners, dates, and notes as work moves from scaffold to fully validated platform.

## Status Legend

- `[x]` Done
- `[~]` In progress
- `[ ]` Not started
- `[!]` Blocked

## Working Rules

1. Every meaningful code change should map to a line item in this file.
2. When a task starts, change it to `[~]` and add a short note.
3. When a task finishes, change it to `[x]` and link the validating artifact if one exists.
4. If scope changes, update this file first so implementation stays aligned with the project narrative.
5. Keep this file outcome-oriented. Track shipped behavior, not just file creation.

## Project End-State

The project is complete when all of the following are true:

- Local dev stack boots successfully with one command.
- Bronze, Silver, and Gold flows run end to end on MinIO-backed Delta tables.
- Kafka ingestion, raw file landing, and backfill flows are demonstrated.
- Schema drift, late arrivals, duplicates, corrupt records, and retry behavior are exercised and documented.
- Airflow DAGs orchestrate the pipeline reliably.
- Trino and dbt query the curated layers successfully.
- Data quality checks, optimization tasks, and observability signals run on schedule.
- Tests cover the critical edge cases credibly.
- Architecture docs, ADRs, and runbooks are complete enough for reviewer walkthroughs.

## Current Snapshot

### Completed

- [x] Repository scaffold created for `infra`, `ingestion`, `lakehouse`, `transforms`, `quality`, `optimization`, `orchestration`, `observability`, `catalog`, `tests`, `scripts`, `config`, and `docs`
- [x] Base Delta runtime and Spark session builder added
- [x] Bronze writer implemented for transactions
- [x] Silver processor implemented with cleaning, conforming, deduplication, UTC normalization, and late-arrival flags
- [x] Gold mart builder implemented for customer and account metrics
- [x] Airflow DAG skeletons added for Bronze, Silver, Gold, dbt, DQ, optimization, and backfill flows
- [x] dbt project scaffold added with staging, intermediate, marts, macros, and snapshot structure
- [x] Data quality framework scaffold added with expectation suites and validation helper
- [x] Optimization utility scaffold added for compaction, Z-order, vacuum, bloom filter config, and partition inspection
- [x] Observability scaffold added for metrics, lineage payloads, and dashboard assets
- [x] Catalog YAMLs, ADRs, runbooks, and starter tests added
- [x] Python syntax validation completed via `python3 -m compileall ...`

### Open Immediate Issue

- [~] Fix local Docker build and dependency compatibility for Airflow and Spark images
  Notes: Airflow resolver backtracked on `dbt-*` and `pyspark`; constraints and version pinning have been tightened, but the stack still needs a full rebuild verification.

## Phase 1: Local Platform Bring-Up

### 1.1 Container Build Stability

- [~] Rebuild `airflow-init`, `airflow-webserver`, and `airflow-scheduler` successfully
  Acceptance: `docker compose build` completes without resolver or timeout failures.
- [ ] Rebuild `spark` image successfully
  Acceptance: image installs project dependencies and starts without import errors.
- [ ] Verify `docker compose up` starts Postgres, MinIO, Kafka, Hive Metastore, Trino, Spark, and Airflow cleanly
  Acceptance: all core services are healthy and reachable on documented ports.

### 1.2 Bootstrap Validation

- [ ] Confirm `.env.example` matches the actual runtime expectations
  Acceptance: no missing env vars during first boot.
- [ ] Run `python3 scripts/create_tables.py` successfully against the local stack
  Acceptance: `bronze`, `silver`, `gold`, and `ops` databases are created in the metastore.
- [ ] Verify Trino can see the Delta catalog and schemas
  Acceptance: `SHOW SCHEMAS FROM delta_lake` and `SHOW TABLES` return expected results.

## Phase 2: Bronze Layer Completion

### 2.1 Streaming and Raw Landing

- [ ] Validate Kafka topic seeding for `transactions`, `customers`, and `events`
  Acceptance: messages are produced and raw files land in MinIO under `raw/<dataset>/ingest_date=.../run_id=.../`.
- [ ] Ensure Bronze ingestion works repeatedly without duplicate file path collisions
  Acceptance: multiple runs generate unique raw paths and append-only Bronze writes.
- [ ] Add operational metadata for source record count, source object path, and ingestion duration
  Acceptance: metrics are queryable from `ops.pipeline_metrics` or equivalent tracking table.

### 2.2 Bronze Data Robustness

- [ ] Validate schema evolution on new upstream columns
  Acceptance: a drift payload lands without failing the Bronze write, and new columns are observable.
- [ ] Validate corrupt record quarantine to `_bad/bronze/...`
  Acceptance: malformed records are isolated and good records still land.
- [ ] Add explicit Bronze write tests for append-only behavior
  Acceptance: tests prove reruns append rather than overwrite.

## Phase 3: Silver Layer Hardening

### 3.1 Canonicalization

- [ ] Verify type casting and standardization on real Spark dataframes
  Acceptance: `amount`, `status`, `payment_channel`, and timestamps are typed as intended.
- [ ] Verify UTC normalization for mixed source time zones
  Acceptance: all downstream timestamps are stored in UTC.
- [ ] Promote customer and event Silver models, not just transaction Silver
  Acceptance: `silver.customers_clean` and `silver.events_clean` exist with canonical schemas.

### 3.2 Business Correctness

- [ ] Validate deduplication using natural keys under duplicate inputs
  Acceptance: only the newest logical record survives in Silver.
- [ ] Validate late-arriving record handling beyond the watermark
  Acceptance: records older than the threshold merge correctly and are flagged.
- [ ] Expand quarantine reasons for invalid domain states
  Acceptance: missing keys, invalid amount, invalid timestamp, and unsupported values are distinguishable.
- [ ] Add Silver integration tests using fixtures and Spark jobs
  Acceptance: edge cases are verified through runnable tests, not just helper function assertions.

## Phase 4: Gold Layer Completion

### 4.1 Current Gold Assets

- [ ] Validate `gold.daily_customer_metrics` incremental merge behavior
  Acceptance: reruns update only affected grains.
- [ ] Validate `gold.account_risk_metrics` incremental merge behavior
  Acceptance: impacted dates and accounts are merged without full rewrites.

### 4.2 Additional Domain Marts

- [ ] Implement finance-facing Gold marts aligned with dbt outputs
  Acceptance: revenue and customer finance marts are queryable from Spark and Trino.
- [ ] Implement risk-facing Gold marts aligned with dbt outputs
  Acceptance: fraud and account risk marts are queryable from Spark and Trino.
- [ ] Add ops mart for pipeline monitoring and reconciliation
  Acceptance: lag, quarantine rate, duplicate rate, and row throughput are queryable.

## Phase 5: dbt Query Layer

### 5.1 Connectivity

- [ ] Validate dbt profile connectivity to Trino
  Acceptance: `dbt debug` passes.
- [ ] Validate `dbt run` end to end against local Delta-backed tables
  Acceptance: staging, intermediate, and mart models build successfully.
- [ ] Validate `dbt test` end to end
  Acceptance: generic and singular tests run and fail only on real rule violations.

### 5.2 Modeling Depth

- [ ] Add model docs and column descriptions in dbt YAML
  Acceptance: `dbt docs generate` includes useful metadata.
- [ ] Add source freshness checks where relevant
  Acceptance: freshness can be run from orchestration and surfaces stale inputs.
- [ ] Validate snapshot behavior for customer dimension history
  Acceptance: snapshot creates SCD2-style history across changes.

## Phase 6: Airflow Orchestration

### 6.1 DAG Runtime Validation

- [ ] Verify all DAGs import cleanly in Airflow
  Acceptance: no broken DAGs in the UI.
- [ ] Trigger Bronze, Silver, Gold, dbt, DQ, optimization, and backfill DAGs successfully
  Acceptance: tasks execute in order with expected outputs.
- [ ] Wire cross-DAG dependencies or event-driven triggers where needed
  Acceptance: dbt and DQ run after the correct upstream datasets are ready.

### 6.2 Operational Behavior

- [ ] Add retries, timeouts, and idempotency protections to task boundaries
  Acceptance: rerunning failed tasks does not corrupt Delta state.
- [ ] Validate SLA callbacks and alert hooks
  Acceptance: a simulated failure emits an alert.
- [ ] Replace placeholder Bash steps with stronger operator usage where needed
  Acceptance: important flows are expressed as Python or custom operators rather than shell glue.

## Phase 7: Data Quality Maturity

### 7.1 Rule Coverage

- [ ] Expand Bronze, Silver, and Gold expectation suites beyond starter checks
  Acceptance: suites cover keys, accepted values, non-negative amounts, and critical completeness rules.
- [ ] Add baseline profiling output and drift comparison workflow
  Acceptance: a baseline profile can be captured and compared to current data.
- [ ] Classify checks by severity (`blocker`, `warn`, `info`)
  Acceptance: orchestration behavior differs correctly by severity.

### 7.2 Quarantine Operations

- [ ] Validate quarantine replay flow via `scripts/reprocess_quarantine.py`
  Acceptance: replayed records promote successfully without duplicate Gold facts.
- [ ] Track quarantine volume and reasons in observability outputs
  Acceptance: quarantine trends are visible in metrics and dashboards.

## Phase 8: Optimization and Performance

### 8.1 Delta Table Maintenance

- [ ] Validate `OPTIMIZE` / compaction behavior on Silver and Gold tables
  Acceptance: file counts reduce after repeated writes.
- [ ] Validate Z-order execution on high-filter columns
  Acceptance: Delta maintenance commands run without errors on the chosen tables.
- [ ] Validate `VACUUM` retention policy
  Acceptance: old files are removed only beyond the configured retention window.

### 8.2 Physical Design

- [ ] Review partition strategy under realistic data volume
  Acceptance: ingestion-date partitioning remains stable and avoids explosion.
- [ ] Evaluate bloom filter table properties on high-cardinality keys
  Acceptance: table properties are applied and documented.
- [ ] Add small-file monitoring and skew alerts
  Acceptance: partition monitor outputs are operationally useful.

## Phase 9: Observability and Lineage

### 9.1 Metrics

- [ ] Persist meaningful pipeline metrics per layer and per run
  Acceptance: row counts, quarantine counts, lag, and durations are queryable.
- [ ] Persist data quality outcome metrics
  Acceptance: pass/fail trends are queryable over time.

### 9.2 Lineage

- [ ] Emit OpenLineage-style events from Bronze, Silver, and Gold jobs
  Acceptance: event payloads reflect correct input/output dataset names.
- [ ] Validate Marquez or DataHub integration path
  Acceptance: lineage can be pushed to a target service or mocked endpoint.

### 9.3 Dashboards

- [ ] Turn placeholder Grafana JSON into real dashboards
  Acceptance: dashboards plot operational metrics from a concrete source.
- [ ] Add dashboard setup notes
  Acceptance: a reviewer can reproduce the dashboards locally.

## Phase 10: Metadata and Governance

### 10.1 Catalog Completeness

- [ ] Add schema YAMLs for all Bronze, Silver, and Gold datasets
  Acceptance: every production-facing table has owner, description, and column metadata.
- [ ] Expand classification tags to include field-level sensitivity coverage
  Acceptance: PII, financial, and sensitive columns are consistently marked.

### 10.2 Governance Hooks

- [ ] Document how catalog YAMLs would feed DataHub or OpenMetadata
  Acceptance: integration path is explicit and believable.
- [ ] Add data ownership map by domain
  Acceptance: finance, risk, and platform tables have named ownership boundaries.

## Phase 11: Testing Strategy

### 11.1 Unit Tests

- [ ] Expand unit tests for cleaner, conformer, quarantine, metrics, and runtime config
  Acceptance: critical pure logic is covered.

### 11.2 Integration Tests

- [ ] Add Spark-backed integration tests for Bronze to Silver promotion
  Acceptance: tests run on temporary Delta locations and assert actual row outcomes.
- [ ] Add Spark-backed integration tests for Silver to Gold promotion
  Acceptance: tests validate incremental merge behavior.
- [ ] Add end-to-end DQ quarantine tests
  Acceptance: malformed and invalid records route correctly without blocking good data.

### 11.3 CI Readiness

- [ ] Add a repeatable test command path for local and CI execution
  Acceptance: `make test` runs the intended suite consistently.
- [ ] Separate fast unit checks from slower integration checks
  Acceptance: contributors can choose a fast feedback path.

## Phase 12: Documentation and Reviewer Experience

### 12.1 Core Docs

- [ ] Expand `README.md` from scaffold to full operator guide
  Acceptance: setup, architecture, flow, and demo steps are complete.
- [ ] Add architecture diagram assets or Mermaid diagrams
  Acceptance: the data flow is visually clear.
- [ ] Validate ADRs reflect the final implementation rather than just intent
  Acceptance: ADRs stay accurate after build decisions settle.

### 12.2 Demo Narrative

- [ ] Write a walkthrough that demonstrates the edge cases intentionally
  Acceptance: reviewer can trigger schema drift, corrupt records, duplicates, and late arrivals.
- [ ] Prepare a concise interview demo path
  Acceptance: the project can be explained and shown in 10 to 15 minutes.

## Phase 13: Optional Cloud Lift

### 13.1 Terraform Depth

- [ ] Flesh out Terraform modules for storage, networking, Kafka, and Spark
  Acceptance: modules are more than placeholders and reflect a realistic cloud deployment path.
- [ ] Add provider-specific notes for AWS, GCP, or Azure
  Acceptance: one provider path is fully documented and internally consistent.

### 13.2 Environment Promotion

- [ ] Validate config separation across `dev`, `staging`, and `prod`
  Acceptance: overrides are meaningful and support deployment promotion.
- [ ] Document what changes between local MinIO and cloud object storage
  Acceptance: a reviewer can understand the migration path.

## Definition of Done Checklist

- [ ] `make dev` works on a clean machine
- [ ] `python3 scripts/create_tables.py` succeeds
- [ ] `python3 scripts/seed_kafka.py ...` lands raw data and Bronze rows
- [ ] Bronze to Silver pipeline runs successfully
- [ ] Silver to Gold pipeline runs successfully
- [ ] `dbt run` succeeds
- [ ] `dbt test` succeeds
- [ ] Airflow DAGs import and execute successfully
- [ ] DQ checks produce visible outcomes
- [ ] Optimization tasks run successfully
- [ ] Metrics and lineage outputs are visible
- [ ] Tests cover the stated edge cases
- [ ] Docs support a reviewer from setup through demo

## Change Log

### 2026-04-04

- [x] Initial tracking plan created and aligned to the current scaffold state
- [x] Recorded current blocker around Airflow image dependency resolution
