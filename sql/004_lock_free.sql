DROP SCHEMA IF EXISTS lock_free CASCADE;
CREATE SCHEMA lock_free;

CREATE UNLOGGED TABLE lock_free.append_intents (
    intent_id UUID PRIMARY KEY,
    lock_key BIGINT NOT NULL UNIQUE,
    context JSONPATH NOT NULL,
    events JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE lock_free.events (
    sequence_number BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX idx_lock_free_events_payload ON lock_free.events USING GIN (payload);
