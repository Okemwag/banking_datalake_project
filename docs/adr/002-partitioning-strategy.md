# ADR 002: Partitioning Strategy

Partition curated tables by ingestion date rather than event date.

This keeps partition counts stable under late-arriving data, avoids high-cardinality blowups, and preserves fast object listing behavior in cloud storage. Event dates remain regular columns and are used for clustering and filtering rather than physical partitioning.

