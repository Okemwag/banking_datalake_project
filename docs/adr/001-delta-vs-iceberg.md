# ADR 001: Delta Lake vs Iceberg

## Decision

Use Delta Lake as the primary table format for this project.

## Rationale

- Native `MERGE INTO` support matches the late-arrival and deduplication requirements.
- Delta schema evolution and time travel are central to the operational scenarios this project demonstrates.
- The optimization layer can express compaction, Z-ordering, and retention controls in a single toolchain.

## Tradeoffs

- Multi-engine interoperability is stronger with Iceberg in some environments.
- Delta-specific optimization commands vary across runtimes and distributions.

