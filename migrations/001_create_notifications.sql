CREATE TABLE IF NOT EXISTS notifications
(
    event_id        uuid        NOT NULL,
    channel         text        NOT NULL,
    user_id         bigint        NOT NULL,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    event_timestamp timestamptz NOT NULL,
    status          text        NOT NULL DEFAULT 'pending',
    PRIMARY KEY (event_id, channel)
);
