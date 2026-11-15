ALTER TABLE notifications
    ADD COLUMN attempts int NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX idx_notifications_next_attempt
    ON notifications (next_attempt_at)
    WHERE status = 'pending'