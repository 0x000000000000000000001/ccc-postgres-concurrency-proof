DROP SCHEMA IF EXISTS atomic_cte CASCADE;
CREATE SCHEMA atomic_cte;

CREATE TABLE atomic_cte.events (
    sequence_number BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL
);
