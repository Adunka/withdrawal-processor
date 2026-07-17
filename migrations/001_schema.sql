-- sluice schema, migration 001.
--
-- Design notes are in the README; the short version of what this file
-- guarantees on its own, before any application code runs:
--
--   * one row per operation_id, forever (PRIMARY KEY = the idempotency root)
--   * amounts are NUMERIC(38,0) minimal units - integers with headroom,
--     no floating point within a mile of money
--   * state transitions outside the whitelist are REJECTED BY THE DATABASE,
--     terminal rows are immutable, even against a fat-fingered psql session
--   * this trigger mirrors TRANSITIONS in sluice/states.py; if you change
--     one, change both (tests/pg walks the full matrix to catch drift)

BEGIN;

CREATE TYPE op_state AS ENUM (
    'requested', 'validated', 'signing', 'signed',
    'broadcasting', 'broadcast', 'confirmed', 'failed', 'rejected'
);

CREATE TABLE operations (
    operation_id     uuid PRIMARY KEY,                -- minted by the client
    request_hash     bytea NOT NULL,                  -- sha256(canonical payload)
    state            op_state NOT NULL DEFAULT 'requested',
    to_address       text NOT NULL,
    asset            text NOT NULL,
    amount_units     numeric(38,0) NOT NULL CHECK (amount_units > 0),

    -- transaction artefacts; txid is durable BEFORE any broadcast attempt
    unsigned_tx      jsonb,
    txid             text,
    tx_expiration    timestamptz,
    signed_tx        jsonb,

    attempts         int NOT NULL DEFAULT 0,

    -- claim bookkeeping: lease says who *probably* owns it, fencing_token
    -- says who *actually* does
    claimed_by       text,
    lease_expires_at timestamptz,
    fencing_token    bigint NOT NULL DEFAULT 0,

    last_error       text,
    failure_reason   text,

    broadcast_at     timestamptz,
    included_block   bigint,
    confirmed_at     timestamptz,

    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- The claim scan only ever looks at live rows; keep the index that small.
CREATE INDEX operations_claim_scan
    ON operations (created_at)
    WHERE state NOT IN ('confirmed', 'failed', 'rejected');

-- A txid belongs to at most one operation, ever.
CREATE UNIQUE INDEX operations_txid_unique
    ON operations (txid)
    WHERE txid IS NOT NULL;

CREATE TABLE operation_events (
    event_id      bigserial PRIMARY KEY,
    operation_id  uuid NOT NULL REFERENCES operations (operation_id),
    from_state    op_state,
    to_state      op_state NOT NULL,
    worker_id     text,
    fencing_token bigint NOT NULL DEFAULT 0,
    detail        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX operation_events_by_op ON operation_events (operation_id, event_id);

-- ---------------------------------------------------------------------------
-- Transition guard. The application checks transitions too, but the database
-- is the party that doesn't get redeployed with a bug on a Friday.
-- ---------------------------------------------------------------------------
CREATE FUNCTION op_transition_guard() RETURNS trigger AS $$
BEGIN
    IF OLD.state IN ('confirmed', 'failed', 'rejected') THEN
        RAISE EXCEPTION 'operation % is terminal (%) and immutable',
            OLD.operation_id, OLD.state
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.state IS DISTINCT FROM OLD.state
       AND (OLD.state::text, NEW.state::text) NOT IN (
            ('requested',    'validated'),
            ('requested',    'rejected'),
            ('validated',    'signing'),
            ('validated',    'rejected'),
            ('signing',      'signed'),
            ('signing',      'failed'),
            ('signed',       'broadcasting'),
            ('signed',       'signing'),
            ('broadcasting', 'broadcast'),
            ('broadcasting', 'signing'),
            ('broadcasting', 'failed'),
            ('broadcast',    'confirmed'),
            ('broadcast',    'signing'),
            ('broadcast',    'failed')
       ) THEN
        RAISE EXCEPTION 'illegal transition % -> % on operation %',
            OLD.state, NEW.state, OLD.operation_id
            USING ERRCODE = 'check_violation';
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER operations_transition_guard
    BEFORE UPDATE ON operations
    FOR EACH ROW
    EXECUTE FUNCTION op_transition_guard();

COMMIT;
