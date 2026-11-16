# Milestone 3 implementation notes

- This is an overlay. It does not replace Milestone 1 or Milestone 2 files.
- Milestone 2 continues writing clean event JSON to `payment-events-processed`.
- `src/postgres_sink.py` is a separate Spark Structured Streaming application that consumes that topic.
- Spark writes micro-batches to a PostgreSQL staging table using JDBC.
- A psycopg2 transaction merges staging rows into `payment_events` with `ON CONFLICT (event_id) DO NOTHING` and deletes the staging rows for that batch.
- The staging write is outside the merge transaction. If it is replayed, duplicate staging rows are collapsed with `DISTINCT ON (event_id)`.
- The target table primary key provides idempotency across micro-batches and restarts.
- Analytics views aggregate event counts and amounts by UTC day, payment rail, status, and sender bank.
- This does not provide cross-system exactly-once semantics. Kafka/Spark delivery can replay; target inserts are idempotent by event_id.
