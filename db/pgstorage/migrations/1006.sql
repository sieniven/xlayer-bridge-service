-- +migrate Up
CREATE TABLE IF NOT EXISTS sync.notification_tracker
(
    id SERIAL PRIMARY KEY,
    deposit_cnt BIGINT NOT NULL,
    network_id  INTEGER NOT NULL,
    txtype TEXT NOT NULL CHECK (txtype IN ('claimed', 'ready_for_claim')),
    is_sent BOOLEAN DEFAULT false, -- If true, Kafka notification has been sent.
    sent_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- with timezone
    message_sent bytea, -- Remember message sent, in this case, the full json string will be captured.
    CONSTRAINT notification_tracker_uidx UNIQUE (network_id, deposit_cnt)
);

-- +migrate Down
DROP TABLE IF EXISTS sync.notification_tracker;
