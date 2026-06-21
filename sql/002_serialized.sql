DROP SCHEMA IF EXISTS serialized CASCADE;
CREATE SCHEMA serialized;

CREATE TABLE serialized.metadata (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    current_sequence_number BIGINT NOT NULL DEFAULT 0
);

INSERT INTO serialized.metadata (id, current_sequence_number)
VALUES (TRUE, 0);

CREATE TABLE serialized.events (
    sequence_number BIGINT PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL
);
