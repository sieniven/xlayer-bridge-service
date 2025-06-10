-- +migrate Up
CREATE TYPE NOTIFICATION_TYPE AS ENUM ('claimed', 'ready_for_claim');

CREATE TABLE sync.notification_tracker
(
    deposit_cnt BIGINT NOT NULL,
    network_id  INTEGER NOT NULL,
    txtype NOTIFICATION_TYPE NOT NULL, -- "claimed" or "ready_for_claim"
    is_sent BOOLEAN DEFAULT false, -- If true, Kafka notification has been sent.
    sent_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- with timezone
    message_sent bytea, -- Remember message sent, in this case, the full json string will be captured.
    PRIMARY KEY(network_id, deposit_cnt)
);

-- +migrate Down
DROP TABLE IF EXISTS sync.notification_tracker;
DROP TYPE IF EXISTS NOTIFICATION_TYPE;
