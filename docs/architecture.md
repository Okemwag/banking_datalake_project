# Architecture

The platform implements a medallion architecture on object storage with Delta Lake tables shared between Spark and Trino.

## Flow

1. Source systems publish events through Kafka or are pulled through batch connectors.
2. Bronze stores raw, append-only records in Delta with ingestion metadata preserved.
3. Silver applies canonical typing, UTC normalization, deduplication, conforming, and late-arrival merge logic.
4. Gold publishes business-ready marts optimized for finance, risk, and operations use cases.
5. dbt models and tests run on top of the query layer.

## Operational Guardrails

- Corrupt records are routed to `_bad/` Delta paths and never block the happy path.
- Schema drift is recorded and absorbed with Delta schema evolution.
- Gold writes are incremental and only recompute impacted dates.
- Partitioning uses ingestion dates to avoid partition explosion on high-cardinality event dimensions.
- Optimization jobs compact small files, vacuum old snapshots, and surface skew risks.

